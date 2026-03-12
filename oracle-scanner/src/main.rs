/// main.rs — CL Scanner
///
/// Startup sequence:
/// 1. Load config
/// 2. Discover active markets (batch REST)
/// 3. Start CL price WebSocket feed
/// 4. Start PM book WebSocket feed
/// 5. Wait for book warmup (5s gate)
/// 6. Scan loop: every 500ms
///    a. Compute signals (once per market)
///    b. Pass to all 5 runners
///    c. Check for window settlements
/// 7. Print stats every 60s

mod feeds;
mod runner;
mod signal;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use dashmap::DashMap;
use reqwest::Client;
use serde::Deserialize;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use feeds::{
    BookState, ClPrices, MarketMeta, PriceHistory, RateLimiter,
    build_slug, current_window_starts, fetch_books_batch, fetch_market_meta,
    run_cl_feed,
};
use runner::{ConfigRunner, RunnerConfig};
use signal::{compute, estimate_sigma};

// ── Config file types ─────────────────────────────────────────────────────────

#[derive(Deserialize, Debug)]
struct AppConfig {
    feed:    FeedConfig,
    scan:    ScanConfig,
    paper:   PaperConfig,
    configs: ConfigsTable,
}

#[derive(Deserialize, Debug)]
struct FeedConfig {
    assets:           Vec<String>,
    timeframes:       Vec<u32>,
    clob_rest:        String,
    clob_ws:          String,
    live_ws:          String,
    gamma_api:        String,
    book_batch_size:  usize,
    rest_throttle_ms: u64,
    book_warmup_secs: u64,
}

#[derive(Deserialize, Debug)]
struct ScanConfig {
    tick_ms:          u64,
    sigma_window_secs: f64,
    min_secs:         f64,
}

#[derive(Deserialize, Debug)]
struct PaperConfig {
    stake:          f64,
    taker_fee_rate: f64,
}

#[derive(Deserialize, Debug)]
struct ConfigsTable {
    c1: RunnerConfig,
    c2: RunnerConfig,
    c3: RunnerConfig,
    c4: RunnerConfig,
    c5: RunnerConfig,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    // Logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("cl_scanner=info".parse()?))
        .with_target(false)
        .init();

    // Load config
    let cfg_text = std::fs::read_to_string("config.toml")
        .context("cannot read config.toml")?;
    let cfg: AppConfig = toml::from_str(&cfg_text)
        .context("invalid config.toml")?;

    info!("CL Oracle Scanner starting");
    info!("Assets: {:?}", cfg.feed.assets);
    info!("Timeframes: {:?}m", cfg.feed.timeframes);

    // Create log directory
    std::fs::create_dir_all("logs").context("cannot create logs/")?;

    // Shared state
    let cl_prices:     ClPrices      = Arc::new(DashMap::new());
    let book_state:    BookState     = Arc::new(DashMap::new());
    let price_history: PriceHistory  = Arc::new(DashMap::new());
    let token_ids:     Arc<DashMap<String, ()>> = Arc::new(DashMap::new());

    // HTTP client (shared, connection pooled)
    let http = Client::builder()
        .user_agent("cl-oracle-scanner/1")
        .timeout(Duration::from_secs(10))
        .build()
        .context("HTTP client build failed")?;

    let limiter = Arc::new(RateLimiter::new(cfg.feed.rest_throttle_ms));

    // ── Discover markets ──────────────────────────────────────────────────────

    info!("Discovering active markets...");
    let mut markets: HashMap<String, MarketMeta> = HashMap::new();

    for asset in &cfg.feed.assets {
        for &tf in &cfg.feed.timeframes {
            let now = now_unix();
            for window_start in current_window_starts(tf, now) {
                let slug = build_slug(asset, tf, window_start);

                match fetch_market_meta(
                    &http, &cfg.feed.gamma_api, &slug, asset, tf, &limiter,
                ).await {
                    Ok(Some(meta)) => {
                        info!("[DISCOVER] {} YES={} NO={}",
                            slug,
                            &meta.token_yes[..8],
                            &meta.token_no[..8]
                        );
                        // Register token IDs for book WS subscription
                        token_ids.insert(meta.token_yes.clone(), ());
                        token_ids.insert(meta.token_no.clone(),  ());
                        markets.insert(slug, meta);
                    }
                    Ok(None) => debug!("[DISCOVER] {} not found", slug),
                    Err(e)   => warn!("[DISCOVER] {} error: {}", slug, e),
                }
            }
        }
    }

    info!("Discovered {} active markets", markets.len());
    if markets.is_empty() {
        warn!("No markets found — will retry on scan ticks");
    }

    // ── Start WebSocket feeds ─────────────────────────────────────────────────

    {
        let cp = cl_prices.clone();
        let ph = price_history.clone();
        let assets = cfg.feed.assets.clone();
        let ws = cfg.feed.live_ws.clone();
        tokio::spawn(async move {
            run_cl_feed(ws, assets, cp, ph).await;
        });
    }

    // Book data via REST polling only (WS book feed removed — unreliable)

    // ── Build runners ─────────────────────────────────────────────────────────

    let mut runners = vec![
        ConfigRunner::new(cfg.configs.c1.clone(), cfg.paper.stake, cfg.paper.taker_fee_rate, "logs"),
        ConfigRunner::new(cfg.configs.c2.clone(), cfg.paper.stake, cfg.paper.taker_fee_rate, "logs"),
        ConfigRunner::new(cfg.configs.c3.clone(), cfg.paper.stake, cfg.paper.taker_fee_rate, "logs"),
        ConfigRunner::new(cfg.configs.c4.clone(), cfg.paper.stake, cfg.paper.taker_fee_rate, "logs"),
        ConfigRunner::new(cfg.configs.c5.clone(), cfg.paper.stake, cfg.paper.taker_fee_rate, "logs"),
    ];

    // ── Warmup gate ───────────────────────────────────────────────────────────

    info!("Starting scan loop (REST book polling, {}s warmup)...", cfg.feed.book_warmup_secs);

    // ── Main scan loop ────────────────────────────────────────────────────────

    let tick = Duration::from_millis(cfg.scan.tick_ms);
    let mut tick_count:    u64 = 0;
    let mut last_stats_ts: f64 = now_secs();
    let mut last_discover: f64 = now_secs();

    // Track which markets have been settled (slug → settled)
    let mut settled: HashMap<String, bool> = HashMap::new();

    loop {
        tokio::time::sleep(tick).await;
        tick_count += 1;

        let now = now_secs();
        let now_u = now as u64;

        // ── Periodic market rediscovery (every 60s) ───────────────────────────
        // New windows open every 5/15 minutes — need to discover them

        if now - last_discover > 60.0 {
            last_discover = now;
            for asset in &cfg.feed.assets {
                for &tf in &cfg.feed.timeframes {
                    for window_start in current_window_starts(tf, now_u) {
                        let slug = build_slug(asset, tf, window_start);
                        if !markets.contains_key(&slug) {
                            if let Ok(Some(meta)) = fetch_market_meta(
                                &http, &cfg.feed.gamma_api, &slug, asset, tf, &limiter,
                            ).await {
                                info!("[DISCOVER] New market: {}", slug);
                                token_ids.insert(meta.token_yes.clone(), ());
                                token_ids.insert(meta.token_no.clone(),  ());
                                markets.insert(slug, meta);
                            }
                        }
                    }
                }
            }
        }

        // ── Batch book refresh (every 2s via REST) ──────────────────────────

        if tick_count % 4 == 0 {
            let all_tokens: Vec<String> = token_ids.iter().map(|e| e.key().clone()).collect();
            if !all_tokens.is_empty() {
                match fetch_books_batch(&http, &cfg.feed.clob_rest, &all_tokens, &limiter).await {
                    Ok(books) => {
                        for (tid, entry) in books {
                            book_state.insert(tid, entry);
                        }
                    }
                    Err(e) => warn!("[BOOK REST] batch failed: {}", e),
                }
            }
        }

        // ── Check settlements ─────────────────────────────────────────────────

        for (slug, meta) in &markets {
            if settled.get(slug).copied().unwrap_or(false) {
                continue;
            }
            if now_u >= meta.window_end + 5 {
                // Window closed — settle using current CL price as proxy
                // In production: listen to settlement event from PM
                let cl_settle = cl_prices
                    .get(&meta.asset)
                    .map(|v| v.1)
                    .unwrap_or(0.0);

                if cl_settle <= 0.0 {
                    continue; // no CL price yet, wait
                }

                // Determine binary outcome: YES=1.0 if cl_settle > open_price
                let outcome = if meta.open_price > 0.0 {
                    if cl_settle > meta.open_price { 1.0 } else { 0.0 }
                } else {
                    // open_price not set yet (market was just discovered)
                    continue;
                };

                info!("[SETTLE] {} cl={:.2} open={:.2} outcome={}",
                    slug, cl_settle, meta.open_price, if outcome == 1.0 { "YES" } else { "NO" });

                for runner in &mut runners {
                    runner.on_settlement(slug, outcome, now).await;
                }

                settled.insert(slug.clone(), true);
            }
        }

        // ── Signal computation + runner dispatch ──────────────────────────────

        // Simple warmup: skip first N ticks to let REST book data populate
        if tick_count < (cfg.feed.book_warmup_secs * 1000 / cfg.scan.tick_ms) {
            continue;
        }

        for (slug, meta) in &mut markets {
            if settled.get(slug.as_str()).copied().unwrap_or(false) {
                continue;
            }

            let secs_left = meta.window_end as f64 - now;
            if secs_left < cfg.scan.min_secs {
                continue;
            }

            // Get CL price
            let cl = match cl_prices.get(&meta.asset) {
                Some(v) => v.1,
                None    => continue,
            };

            // Record open price at window start (first CL price we see)
            if meta.open_price <= 0.0 {
                if now_u >= meta.window_start {
                    meta.open_price = cl;
                    info!("[OPEN] {} open_price={:.2}", slug, cl);
                }
                continue; // don't trade until open price is set
            }

            // Get book prices for YES and NO tokens
            let book_yes = match book_state.get(&meta.token_yes) {
                Some(b) => b.best_ask,
                None    => continue,
            };
            let book_no = match book_state.get(&meta.token_no) {
                Some(b) => b.best_ask,
                None    => continue,
            };

            // Estimate sigma from CL price history
            let sigma = {
                let hist = price_history.get(&meta.asset);
                match hist {
                    Some(h) => estimate_sigma(&h, cfg.scan.sigma_window_secs, now),
                    None    => 0.001,
                }
            };

            // Compute signal ONCE — shared across all runners
            let sig = match compute(
                slug, &meta.asset, meta.tf,
                meta.open_price, cl, sigma, secs_left,
                book_yes, book_no, now,
            ) {
                Some(s) => s,
                None    => continue,
            };

            if sig.best_edge > 0.05 {
                debug!(
                    "[SCAN] {} cl={:.2} fair_y={:.3} bk_y={:.3} bk_n={:.3} dev={:+.3} secs={:.0}",
                    slug, cl, sig.fair_yes, book_yes, book_no, sig.best_edge, secs_left
                );
            }

            // Dispatch to all 5 runners
            for runner in &mut runners {
                runner.on_signal(&sig, meta.window_end).await;
            }
        }

        // ── Periodic stats print (every 60s) ──────────────────────────────────

        if now - last_stats_ts >= 60.0 {
            last_stats_ts = now;
            info!("──────────────────────────────────────────────────────");
            for runner in &runners {
                runner.print_stats();
            }
            info!("open_markets={} settled={} book_entries={}",
                markets.len(), settled.len(), book_state.len()
            );
        }
    }
}
