/// test_order.rs — Force-place a single tiny BUY order using official Polymarket SDK.
///
/// Usage (from cl-oracle-scanner/):
///   cargo build --release --bin test_order
///   # Windows:
///   set RUST_LOG=test_order=info,lag_scanner=info && target\release\test_order.exe
///   # Linux/Mac:
///   RUST_LOG=test_order=info,lag_scanner=info ./target/release/test_order
///
/// Uses the exact same market discovery as the main scanner. Places a $0.50 GTC BUY
/// on the first active BTC 5m market it finds. Authentication is handled by the SDK.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use tracing::{info, error};
use tracing_subscriber::EnvFilter;

use lag_scanner::feeds::{build_slug, current_window_starts, fetch_market_meta, RateLimiter};
use lag_scanner::order::ClobClient;
use lag_scanner::wallet::Wallet;

#[derive(Deserialize)]
struct MiniConfig {
    wallet: Option<WalletConfig>,
    feed:   Option<FeedConfig>,
}

#[derive(Deserialize, Default)]
struct WalletConfig {
    private_key: Option<String>,
    api_url:     Option<String>,
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

    // Only need private key — SDK handles API key derivation + auth
    let pk = wcfg.private_key.clone()
        .or_else(|| std::env::var("PRIVATE_KEY").ok())
        .ok_or_else(|| anyhow!("No private_key in config.toml or PRIVATE_KEY env var"))?;

    let base_url = wcfg.api_url.clone()
        .or_else(|| std::env::var("CLOB_API_URL").ok())
        .unwrap_or_else(|| "https://clob.polymarket.com".into());
    let gamma_url = cfg.feed
        .and_then(|f| f.gamma_api)
        .unwrap_or_else(|| "https://gamma-api.polymarket.com".into());

    // Build wallet & SDK client
    let wallet = Wallet::from_hex(&pk)?;
    info!("Wallet: {}", wallet.address());

    let client = ClobClient::new(&base_url, wallet);
    info!("CLOB: {}", base_url);

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
                        info!("Found: {} -> token_yes={}...{}", slug,
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

    info!("===============================================================");
    info!("  TEST ORDER — OFFICIAL POLYMARKET SDK");
    info!("===============================================================");
    info!("  Market:   {}", market_desc);
    info!("  Token:    {}...{}", &token_id[..8], &token_id[token_id.len()-8..]);
    info!("  Side:     BUY");
    info!("  Price:    {}", price);
    info!("  Size:     {} shares (${:.2} USDC)", size, size * price);
    info!("  Type:     GTC (limit)");
    info!("===============================================================");

    match client.place_limit_order(&token_id, price, size, "BUY").await {
        Ok(resp) => {
            info!("Order placed successfully!");
            info!("Response: {}", resp);
        }
        Err(e) => {
            error!("Order FAILED: {:#}", e);
            error!("Check: wallet funded? Private key correct?");
            return Err(e);
        }
    }

    Ok(())
}
