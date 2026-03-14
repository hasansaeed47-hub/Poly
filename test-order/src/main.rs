/// test-order — Force-place a single tiny trade using the official Polymarket Rust SDK.
///
/// Usage:
///   Set POLYMARKET_PRIVATE_KEY (or PRIVATE_KEY) env var
///   cd test-order && cargo run --release
///
/// Places a $0.50 GTC limit BUY @ 0.50 on the first active BTC market found.

use std::str::FromStr;

use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;
use polymarket_client_sdk::clob::types::Side;
use polymarket_client_sdk::clob::{Client, Config};
use polymarket_client_sdk::types::{Decimal, U256};
use polymarket_client_sdk::{POLYGON, PRIVATE_KEY_VAR};
use rust_decimal_macros::dec;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

/// Discover an active BTC up/down market token ID via Gamma API
async fn find_active_token() -> anyhow::Result<(String, String)> {
    let http = reqwest::Client::new();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    let assets = ["btc", "eth", "sol", "xrp"];
    let timeframes = [5u64, 15];

    for asset in &assets {
        for &tf in &timeframes {
            let interval = tf * 60;
            let current_window = (now / interval) * interval;
            let next_window = current_window + interval;

            for ws in [current_window, next_window] {
                let slug = format!("{}-updown-{}m-{}", asset, tf, ws);

                let resp = http
                    .get("https://gamma-api.polymarket.com/markets")
                    .query(&[("slug", &slug)])
                    .timeout(std::time::Duration::from_secs(5))
                    .send()
                    .await;

                let resp = match resp {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                let text = match resp.text().await {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                let markets: Vec<serde_json::Value> = match serde_json::from_str(&text) {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                for m in &markets {
                    // Gamma returns outcomes/clobTokenIds as JSON-encoded strings or arrays
                    let outcomes: Option<Vec<String>> = m
                        .get("outcomes")
                        .and_then(|v: &serde_json::Value| {
                            v.as_str()
                                .and_then(|s| serde_json::from_str(s).ok())
                                .or_else(|| {
                                    v.as_array().map(|a| {
                                        a.iter()
                                            .filter_map(|x| x.as_str().map(String::from))
                                            .collect()
                                    })
                                })
                        });

                    let tokens: Option<Vec<String>> = m
                        .get("clobTokenIds")
                        .and_then(|v: &serde_json::Value| {
                            v.as_str()
                                .and_then(|s| serde_json::from_str(s).ok())
                                .or_else(|| {
                                    v.as_array().map(|a| {
                                        a.iter()
                                            .filter_map(|x| x.as_str().map(String::from))
                                            .collect()
                                    })
                                })
                        });

                    if let (Some(outcomes), Some(tokens)) = (outcomes, tokens) {
                        if outcomes.len() >= 2 && tokens.len() >= 2 {
                            let yes_idx = outcomes.iter().position(|o| {
                                o.eq_ignore_ascii_case("yes") || o.eq_ignore_ascii_case("up")
                            });
                            if let Some(idx) = yes_idx {
                                info!(
                                    "Found market: {} -> token {}...",
                                    slug,
                                    &tokens[idx][..16.min(tokens[idx].len())]
                                );
                                return Ok((tokens[idx].clone(), slug));
                            }
                        }
                    }
                }
            }
        }
    }

    anyhow::bail!("No active up/down market found")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Load .env from cl-oracle-scanner directory
    let _ = dotenvy::from_filename("../cl-oracle-scanner/.env");
    let _ = dotenvy::dotenv();

    // Get private key (support both env var names)
    let pk_str = std::env::var(PRIVATE_KEY_VAR)
        .or_else(|_| std::env::var("PRIVATE_KEY"))
        .expect("Set POLYMARKET_PRIVATE_KEY or PRIVATE_KEY env var");

    let clean_pk = pk_str.strip_prefix("0x").unwrap_or(&pk_str);
    let signer = LocalSigner::from_str(clean_pk)?.with_chain_id(Some(POLYGON));
    info!("Wallet: 0x{:x}", signer.address());

    // Build authenticated CLOB client using official SDK
    let config = Config::builder().use_server_time(true).build();
    let client = Client::new("https://clob.polymarket.com", config)?
        .authentication_builder(&signer)
        .authenticate()
        .await?;

    info!("Authenticated with CLOB API");

    // Discover market
    let (token_id, slug) = find_active_token().await?;

    info!("===============================================================");
    info!("  TEST ORDER — OFFICIAL POLYMARKET RUST SDK");
    info!("===============================================================");
    info!("  Market:   {}", slug);
    info!(
        "  Token:    {}...{}",
        &token_id[..8],
        &token_id[token_id.len() - 8..]
    );
    info!("  Side:     BUY");
    info!("  Price:    0.50");
    info!("  Size:     1 share ($0.50 USDC)");
    info!("  Type:     GTC (limit)");
    info!("===============================================================");

    // Build limit order: 1 share @ $0.50
    let token_u256 = U256::from_str(&token_id)
        .expect("token_id must be valid uint256");
    let limit_order = client
        .limit_order()
        .token_id(token_u256)
        .price(dec!(0.50))
        .size(Decimal::ONE)
        .side(Side::Buy)
        .build()
        .await?;

    info!("Order built, signing...");

    let signed_order = client.sign(&signer, limit_order).await?;

    info!("Signed, posting...");

    match client.post_order(signed_order).await {
        Ok(resp) => {
            info!("Order placed successfully!");
            info!("Response: {:?}", resp);
        }
        Err(e) => {
            error!("Order FAILED: {:#}", e);
            return Err(e.into());
        }
    }

    Ok(())
}
