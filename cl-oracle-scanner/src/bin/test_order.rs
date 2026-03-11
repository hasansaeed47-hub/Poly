/// test_order.rs — Force-place a single tiny BUY order to verify CLOB connectivity.
///
/// Usage (from cl-oracle-scanner/):
///   cargo build --release --bin test_order
///   # Windows:
///   set RUST_LOG=test_order=info,lag_scanner=info && target\release\test_order.exe
///   # Linux/Mac:
///   RUST_LOG=test_order=info,lag_scanner=info ./target/release/test_order
///
/// Uses the exact same market discovery as the main scanner (build_slug +
/// fetch_market_meta via Gamma API). Places a $0.50 GTC BUY on the first
/// active BTC 5m market it finds.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use tracing::{info, error};
use tracing_subscriber::EnvFilter;

use lag_scanner::feeds::{build_slug, current_window_starts, fetch_market_meta, RateLimiter};
use lag_scanner::order::{ApiCreds, ClobClient};
use lag_scanner::wallet::Wallet;

#[derive(Deserialize)]
struct MiniConfig {
    wallet: Option<WalletConfig>,
    feed:   Option<FeedConfig>,
}

#[derive(Deserialize, Default)]
struct WalletConfig {
    private_key: Option<String>,
    api_key:     Option<String>,
    api_secret:  Option<String>,
    passphrase:  Option<String>,
    api_url:     Option<String>,
    neg_risk:    Option<bool>,
}

#[derive(Deserialize, Default)]
struct FeedConfig {
    gamma_api: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let _ = dotenvy::dotenv();

    let cfg_text = std::fs::read_to_string("config.toml")
        .context("Cannot read config.toml — run from cl-oracle-scanner/ directory")?;
    let cfg: MiniConfig = toml::from_str(&cfg_text)
        .context("Invalid config.toml")?;

    let wcfg = cfg.wallet.unwrap_or_default();

    // Resolve credentials (config.toml > env vars)
    let pk = wcfg.private_key.clone()
        .or_else(|| std::env::var("PRIVATE_KEY").ok())
        .ok_or_else(|| anyhow!("No private_key in config.toml or PRIVATE_KEY env var"))?;
    let api_key = wcfg.api_key.clone()
        .or_else(|| std::env::var("CLOB_API_KEY").ok())
        .ok_or_else(|| anyhow!("No api_key — set in config.toml or CLOB_API_KEY env"))?;
    let api_secret = wcfg.api_secret.clone()
        .or_else(|| std::env::var("CLOB_API_SECRET").ok())
        .ok_or_else(|| anyhow!("No api_secret — set in config.toml or CLOB_API_SECRET env"))?;
    let passphrase = wcfg.passphrase.clone()
        .or_else(|| std::env::var("CLOB_PASSPHRASE").ok())
        .ok_or_else(|| anyhow!("No passphrase — set in config.toml or CLOB_PASSPHRASE env"))?;

    let neg_risk = wcfg.neg_risk.unwrap_or(false)
        || std::env::var("NEG_RISK").unwrap_or_default() == "true";
    let base_url = wcfg.api_url.clone()
        .or_else(|| std::env::var("CLOB_API_URL").ok())
        .unwrap_or_else(|| "https://clob.polymarket.com".into());
    let gamma_url = cfg.feed
        .and_then(|f| f.gamma_api)
        .unwrap_or_else(|| "https://gamma-api.polymarket.com".into());

    // Build wallet & client
    let wallet = Wallet::from_hex(&pk)?;
    info!("Wallet: {}", wallet.address());

    let creds = ApiCreds {
        api_key,
        api_secret: api_secret.trim().to_string(),
        api_passphrase: passphrase,
    };
    let client = ClobClient::new(&base_url, wallet, Some(creds), neg_risk);
    info!("CLOB: {}  neg_risk: {}", base_url, neg_risk);

    // -- Discover market using the same logic as the main scanner ---------------
    let http = reqwest::Client::new();
    let limiter = RateLimiter::new(200);
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let assets = ["btc", "eth", "sol", "xrp"];
    let timeframes = [5u32, 15];

    let mut token_id: Option<String> = None;
    let mut market_desc = String::new();

    info!("Discovering active markets via Gamma...");

    'outer: for asset in &assets {
        for &tf in &timeframes {
            let windows = current_window_starts(tf, now_secs);
            for ws in &windows {
                let slug = build_slug(asset, tf, *ws);
                match fetch_market_meta(&http, &gamma_url, &slug, asset, tf, &limiter).await {
                    Ok(Some(meta)) => {
                        info!("Found: {} → token_yes={}...{}", slug,
                            &meta.token_yes[..8], &meta.token_yes[meta.token_yes.len()-8..]);
                        token_id = Some(meta.token_yes.clone());
                        market_desc = slug;
                        break 'outer;
                    }
                    Ok(None) => {
                        info!("  {} — not found", slug);
                    }
                    Err(e) => {
                        info!("  {} — error: {}", slug, e);
                    }
                }
            }
        }
    }

    let token_id = token_id.ok_or_else(|| anyhow!(
        "No active up/down market found. Markets may be between windows."
    ))?;

    // -- Place test order -------------------------------------------------------
    let price = 0.50;
    let size  = 1.0;

    info!("═══════════════════════════════════════════════════════");
    info!("  TEST ORDER — FORCE TRADE");
    info!("═══════════════════════════════════════════════════════");
    info!("  Market:   {}", market_desc);
    info!("  Token:    {}...{}", &token_id[..8], &token_id[token_id.len()-8..]);
    info!("  Side:     BUY");
    info!("  Price:    {}", price);
    info!("  Size:     {} shares (${:.2} USDC)", size, size * price);
    info!("  Type:     GTC (limit)");
    info!("  neg_risk: {}", neg_risk);
    info!("═══════════════════════════════════════════════════════");

    match client.place_limit_order(&token_id, price, size, "BUY").await {
        Ok(resp) => {
            info!("Order placed successfully!");
            info!("Response: {}", serde_json::to_string_pretty(&resp).unwrap_or_default());
        }
        Err(e) => {
            error!("Order FAILED: {:#}", e);
            error!("Check: wallet funded? API creds valid? neg_risk correct?");
            return Err(e);
        }
    }

    Ok(())
}
