/// test_order.rs — Force-place a single tiny BUY order to verify CLOB connectivity.
///
/// Usage (from cl-oracle-scanner/):
///   cargo build --release --bin test_order
///   # Windows:
///   set RUST_LOG=test_order=debug,lag_scanner=debug && target\release\test_order.exe
///   # Linux/Mac:
///   RUST_LOG=test_order=debug,lag_scanner=debug ./target/release/test_order
///
/// Reads wallet/API creds from config.toml (or .env / env vars).
/// Places a $0.50 BUY @ 0.50 on a known active BTC 5m market.
/// The order is GTC so it will sit on the book — cancel manually if needed.

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use tracing::{info, warn, error};
use tracing_subscriber::EnvFilter;

// Re-use library modules from the main crate
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

#[derive(Deserialize)]
struct FeedConfig {
    clob_rest: Option<String>,
    gamma_api: Option<String>,
}

/// Fetch current active token IDs for a BTC 5m market from Gamma API
async fn fetch_active_token(gamma_url: &str) -> Result<(String, String)> {
    let url = format!(
        "{}/markets?slug_contains=btc&active=true&closed=false&limit=5",
        gamma_url.trim_end_matches('/')
    );
    info!("Fetching active markets from: {}", url);

    let resp: serde_json::Value = reqwest::get(&url).await?.json().await?;
    let markets = resp.as_array().ok_or_else(|| anyhow!("Gamma response is not an array"))?;

    for m in markets {
        let q = m.get("question").and_then(|v| v.as_str()).unwrap_or("");
        // Look for a short-duration (5m/15m) BTC up/down market
        if q.to_lowercase().contains("btc") && q.to_lowercase().contains("up") {
            if let Some(tokens) = m.get("tokens").and_then(|v| v.as_array()) {
                for tok in tokens {
                    let outcome = tok.get("outcome").and_then(|v| v.as_str()).unwrap_or("");
                    let tid = tok.get("token_id").and_then(|v| v.as_str()).unwrap_or("");
                    if outcome == "Yes" && !tid.is_empty() {
                        let cid = m.get("condition_id").and_then(|v| v.as_str()).unwrap_or("?");
                        info!("Found market: {} (condition={})", q, cid);
                        return Ok((tid.to_string(), q.to_string()));
                    }
                }
            }
        }
    }

    Err(anyhow!("No active BTC up/down market found on Gamma"))
}

#[tokio::main]
async fn main() -> Result<()> {
    // Init tracing
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Load .env
    let _ = dotenvy::dotenv();

    // Load config
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

    info!("CLOB base URL: {}", base_url);
    info!("neg_risk: {}", neg_risk);

    // Find an active token to trade
    let (token_id, question) = match fetch_active_token(&gamma_url).await {
        Ok(v) => v,
        Err(e) => {
            error!("Could not find active market: {}", e);
            error!("You can manually set TOKEN_ID env var and re-run");
            return Err(e);
        }
    };

    info!("═══════════════════════════════════════════════════════");
    info!("  TEST ORDER — FORCE TRADE");
    info!("═══════════════════════════════════════════════════════");
    info!("  Market:   {}", question);
    info!("  Token:    {}...{}", &token_id[..8], &token_id[token_id.len()-8..]);
    info!("  Side:     BUY");
    info!("  Price:    0.50");
    info!("  Size:     1.0 shares ($0.50 USDC)");
    info!("  Type:     GTC (limit)");
    info!("  neg_risk: {}", neg_risk);
    info!("═══════════════════════════════════════════════════════");

    // Place a tiny GTC limit buy: 1 share @ $0.50 = $0.50 USDC
    let price = 0.50;
    let size  = 1.0;

    match client.place_limit_order(&token_id, price, size, "BUY").await {
        Ok(resp) => {
            info!("Order placed successfully!");
            info!("Response: {}", serde_json::to_string_pretty(&resp).unwrap_or_default());
        }
        Err(e) => {
            error!("Order FAILED: {:#}", e);
            warn!("Check: wallet funded? API creds valid? neg_risk correct?");
            return Err(e);
        }
    }

    Ok(())
}
