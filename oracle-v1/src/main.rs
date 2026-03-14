/// Oracle Scanner V1 — 14th March 2026
///
/// Live execution build implementing OPT2 maker strategy:
/// - edge >= 0.20 entry filter
/// - Stop-loss when fair < entry_price
/// - 50% take-profit when PM bid >= fair_at_entry
/// - Maker chase: GTC(post_only) → chase 2 ticks @ 500ms → FAK(taker)
/// - Uses official polymarket-client-sdk for order execution
/// - Heartbeat auto-managed by SDK
///
/// Scan loop: 500ms tick
///   1. Discover markets (Gamma API)
///   2. Poll books (CLOB REST)
///   3. Compute signals (Black-Scholes)
///   4. Runner entry/exit/settlement

mod feeds;
mod signal;
mod execution;
mod runner;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use dashmap::DashMap;
use reqwest::Client;
use serde::Deserialize;
use tracing::{info, warn, error};
use tracing_subscriber::EnvFilter;

use feeds::{
    BookState, ClPrices, MarketMeta, PriceHistory, RateLimiter,
    build_slug, current_window_starts, fetch_books_batch, fetch_market_meta,
    run_cl_feed,
};
use runner::{LiveRunner, StrategyConfig};
use signal::{compute, estimate_sigma, BookData};

// ── Config ──────────────────────────────────────────────────────────────────

#[derive(Deserialize, Debug)]
struct AppConfig {
    wallet:   WalletConfig,
    feed:     FeedConfig,
    scan:     ScanConfig,
    strategy: StrategyConfigFile,
}

#[derive(Deserialize, Debug)]
struct WalletConfig {
    private_key:    String,
    funder_address: String,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct FeedConfig {
    assets:           Vec<String>,
    timeframes:       Vec<u32>,
    clob_rest:        String,
    live_ws:          String,
    gamma_api:        String,
    book_batch_size:  usize,
    rest_throttle_ms: u64,
    book_warmup_secs: u64,
    book_stale_secs:  f64,
}

#[derive(Deserialize, Debug)]
struct ScanConfig {
    tick_ms:            u64,
    sigma_window_secs:  f64,
    min_secs:           f64,
}

#[derive(Deserialize, Debug)]
struct StrategyConfigFile {
    stake:             f64,
    min_edge:          f64,
    max_secs_left:     f64,
    min_entry_price:   f64,
    max_sigma:         f64,
    stop_loss:         bool,
    take_profit:       bool,
    partial_tp_pct:    f64,
    taker_fee_rate:    f64,
    maker_fee_rate:    f64,
    maker_chase_ticks:  u32,
    chase_interval_ms:  u64,
    #[serde(default = "default_max_concurrent")]
    max_concurrent:     usize,
}

fn default_max_concurrent() -> usize { 6 }

// ── Helpers ─────────────────────────────────────────────────────────────────

fn now_secs() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64()
}

fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

// ── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("oracle_v1=info".parse()?)
                .add_directive("polymarket_client_sdk=warn".parse()?)
        )
        .with_target(false)
        .init();

    // Load config
    let cfg_text = std::fs::read_to_string("config.toml")
        .context("cannot read config.toml")?;
    let cfg: AppConfig = toml::from_str(&cfg_text)
        .context("invalid config.toml")?;

    info!("═══════════════════════════════════════════════════════════");
    info!("  Oracle Scanner V1 — OPT2 Maker — 14th March 2026");
    info!("═══════════════════════════════════════════════════════════");
    info!("Assets: {:?}", cfg.feed.assets);
    info!("Timeframes: {:?}m", cfg.feed.timeframes);
    info!("Strategy: edge>={} SL={} TP={}({}%) stake=${} chase={}ticks max_sigma={} max_pos={}",
        cfg.strategy.min_edge, cfg.strategy.stop_loss, cfg.strategy.take_profit,
        cfg.strategy.partial_tp_pct * 100.0, cfg.strategy.stake,
        cfg.strategy.maker_chase_ticks, cfg.strategy.max_sigma, cfg.strategy.max_concurrent);

    // Wallet keys: config.toml first, then env vars as fallback
    let private_key = if cfg.wallet.private_key.is_empty() {
        std::env::var("POLY_PRIVATE_KEY")
            .context("private_key empty in config.toml and POLY_PRIVATE_KEY env var not set")?
    } else {
        cfg.wallet.private_key.clone()
    };
    let funder_address = if cfg.wallet.funder_address.is_empty() {
        std::env::var("POLY_FUNDER_ADDRESS").unwrap_or_default()
    } else {
        cfg.wallet.funder_address.clone()
    };

    // Create log directory
    std::fs::create_dir_all("logs").context("cannot create logs/")?;

    // ── Initialize execution layer ──────────────────────────────────────────

    info!("Initializing execution layer...");
    let exec = execution::ExecutionLayer::new(
        &private_key,
        &funder_address,
    ).await.context("Execution layer init failed")?;
    let exec = Arc::new(exec);

    // ── Cancel stale orders on startup (V6 pattern) ───────────────────────
    info!("Cancelling any stale orders from previous run...");
    match exec.cancel_all().await {
        Ok(_)  => info!("[EXEC] All stale orders cancelled"),
        Err(e) => warn!("[EXEC] Cancel all on startup failed (may be clean): {}", e),
    }

    // ── Shared state ────────────────────────────────────────────────────────

    let cl_prices:     ClPrices     = Arc::new(DashMap::new());
    let book_state:    BookState    = Arc::new(DashMap::new());
    let price_history: PriceHistory = Arc::new(DashMap::new());
    let token_ids:     Arc<DashMap<String, ()>> = Arc::new(DashMap::new());

    let http = Client::builder()
        .user_agent("oracle-v1/1")
        .timeout(Duration::from_secs(10))
        .build()
        .context("HTTP client build failed")?;

    let limiter = Arc::new(RateLimiter::new(cfg.feed.rest_throttle_ms));

    // ── Discover markets ────────────────────────────────────────────────────

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
                            slug, &meta.token_yes[..8.min(meta.token_yes.len())],
                            &meta.token_no[..8.min(meta.token_no.len())]);
                        token_ids.insert(meta.token_yes.clone(), ());
                        token_ids.insert(meta.token_no.clone(), ());
                        markets.insert(slug, meta);
                    }
                    Ok(None) => {}
                    Err(e) => warn!("[DISCOVER] {} error: {}", slug, e),
                }
            }
        }
    }

    info!("Discovered {} active markets", markets.len());

    // ── Start CL WebSocket feed ─────────────────────────────────────────────

    {
        let cp = cl_prices.clone();
        let ph = price_history.clone();
        let assets = cfg.feed.assets.clone();
        let ws = cfg.feed.live_ws.clone();
        tokio::spawn(async move {
            run_cl_feed(ws, assets, cp, ph).await;
        });
    }

    // ── Build runner ────────────────────────────────────────────────────────

    let strat_config = StrategyConfig {
        stake:             cfg.strategy.stake,
        min_edge:          cfg.strategy.min_edge,
        max_secs_left:     cfg.strategy.max_secs_left,
        min_entry_price:   cfg.strategy.min_entry_price,
        max_sigma:         cfg.strategy.max_sigma,
        stop_loss:         cfg.strategy.stop_loss,
        take_profit:       cfg.strategy.take_profit,
        partial_tp_pct:    cfg.strategy.partial_tp_pct,
        taker_fee_rate:    cfg.strategy.taker_fee_rate,
        maker_fee_rate:    cfg.strategy.maker_fee_rate,
        maker_chase_ticks: cfg.strategy.maker_chase_ticks,
        chase_interval_ms: cfg.strategy.chase_interval_ms,
        max_concurrent:    cfg.strategy.max_concurrent,
    };

    let mut runner = LiveRunner::new(strat_config, exec.clone(), "logs");

    // ── Main scan loop ──────────────────────────────────────────────────────

    let tick = Duration::from_millis(cfg.scan.tick_ms);
    let mut tick_count:    u64 = 0;
    let mut last_stats_ts: f64 = now_secs();
    let mut last_discover: f64 = now_secs();
    let mut last_dash_ts:  f64 = 0.0;
    let mut settled: HashMap<String, bool> = HashMap::new();
    let mut cl_close_snap: HashMap<String, f64> = HashMap::new(); // V6 FIX #3

    // ── Graceful shutdown handler ──────────────────────────────────────────
    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let shutdown = shutdown.clone();
        let exec = exec.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            warn!("SIGINT/SIGTERM received — cancelling all orders and shutting down...");
            shutdown.store(true, Ordering::SeqCst);
            match exec.cancel_all().await {
                Ok(_)  => info!("[SHUTDOWN] All orders cancelled"),
                Err(e) => error!("[SHUTDOWN] Cancel all failed: {}", e),
            }
        });
    }

    info!("Starting scan loop ({}ms tick, {}s warmup)...",
        cfg.scan.tick_ms, cfg.feed.book_warmup_secs);

    // ── CL feed warmup: wait for at least one price per asset ─────────────
    info!("Waiting for CL feed...");
    for _ in 0..40u32 { // 20s max
        tokio::time::sleep(Duration::from_millis(500)).await;
        let all_have = cfg.feed.assets.iter().all(|a| cl_prices.contains_key(a.as_str()));
        if all_have {
            for asset in &cfg.feed.assets {
                if let Some(v) = cl_prices.get(asset.as_str()) {
                    info!("[CL] {} = {:.2}", asset.to_uppercase(), v.1);
                }
            }
            break;
        }
    }

    loop {
        tokio::time::sleep(tick).await;

        if shutdown.load(Ordering::SeqCst) {
            info!("Shutdown flag set — exiting scan loop");
            runner.print_stats();
            break;
        }

        tick_count += 1;

        let now = now_secs();
        let now_u = now as u64;

        // ── Periodic market rediscovery (every 60s) ─────────────────────────

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
                                token_ids.insert(meta.token_no.clone(), ());
                                markets.insert(slug, meta);
                            }
                        }
                    }
                }
            }
        }

        // ── Batch book refresh (every other tick) ───────────────────────────

        if tick_count % 2 == 0 {
            let all_tokens: Vec<String> = token_ids.iter().map(|e| e.key().clone()).collect();
            if !all_tokens.is_empty() {
                match fetch_books_batch(&http, &cfg.feed.clob_rest, &all_tokens, &limiter).await {
                    Ok(books) => {
                        for (tid, entry) in books {
                            book_state.insert(tid, entry);
                        }
                    }
                    Err(e) => warn!("[BOOK] batch failed: {}", e),
                }
            }
        }

        // ── V6 FIX #3: Record CL close snap — first CL after window end ──

        for (slug, meta) in &markets {
            if settled.get(slug.as_str()).copied().unwrap_or(false) { continue; }
            if cl_close_snap.contains_key(slug.as_str()) { continue; }
            if now_u > meta.window_end {
                let cl_now = cl_prices.get(&meta.asset).map(|v| v.1).unwrap_or(0.0);
                if cl_now > 0.0 {
                    cl_close_snap.insert(slug.clone(), cl_now);
                    info!("[CL_CLOSE] {} snap={:.2}", slug, cl_now);
                }
            }
        }

        // ── Check settlements (use CL close snap, not live CL) ───────────

        for (slug, meta) in &markets {
            if settled.get(slug.as_str()).copied().unwrap_or(false) { continue; }
            if now_u >= meta.window_end + 5 {
                let cl_settle = match cl_close_snap.get(slug.as_str()) {
                    Some(&p) => p,
                    None => continue, // Wait for close snap
                };
                if cl_settle <= 0.0 { continue; }
                if meta.open_price <= 0.0 { continue; }

                let outcome = if cl_settle > meta.open_price { 1.0 } else { 0.0 };
                info!("[SETTLE] {} cl_close={:.2} open={:.2} outcome={}",
                    slug, cl_settle, meta.open_price, if outcome == 1.0 { "YES" } else { "NO" });

                runner.on_settlement(slug, outcome, now).await;
                settled.insert(slug.clone(), true);
            }
        }

        // ── Warmup gate ─────────────────────────────────────────────────────

        if tick_count < (cfg.feed.book_warmup_secs * 1000 / cfg.scan.tick_ms) {
            continue;
        }

        // ── Signal computation + runner dispatch ────────────────────────────

        for (slug, meta) in &mut markets {
            if settled.get(slug.as_str()).copied().unwrap_or(false) { continue; }

            let secs_left = meta.window_end as f64 - now;
            if secs_left < cfg.scan.min_secs { continue; }

            let cl = match cl_prices.get(&meta.asset) {
                Some(v) => v.1,
                None    => continue,
            };

            // Set open price at window start
            if meta.open_price <= 0.0 {
                if now_u >= meta.window_start {
                    meta.open_price = cl;
                    info!("[OPEN] {} open_price={:.2}", slug, cl);
                }
                continue;
            }

            // Get book data
            let book_yes_entry = match book_state.get(&meta.token_yes) {
                Some(b) if b.best_ask > 0.0 => b.clone(),
                _ => continue,
            };
            let book_no_entry = match book_state.get(&meta.token_no) {
                Some(b) if b.best_ask > 0.0 => b.clone(),
                _ => continue,
            };

            // Skip stale books (V6 used 1s, configurable via book_stale_secs)
            if now - book_yes_entry.ts > cfg.feed.book_stale_secs
                || now - book_no_entry.ts > cfg.feed.book_stale_secs {
                continue;
            }

            let bd_yes = BookData {
                best_ask: book_yes_entry.best_ask,
                best_bid: book_yes_entry.best_bid,
                asks: book_yes_entry.asks,
                bids: book_yes_entry.bids,
            };
            let bd_no = BookData {
                best_ask: book_no_entry.best_ask,
                best_bid: book_no_entry.best_bid,
                asks: book_no_entry.asks,
                bids: book_no_entry.bids,
            };

            let sigma = {
                let hist = price_history.get(&meta.asset);
                match hist {
                    Some(h) => estimate_sigma(&h, cfg.scan.sigma_window_secs, now),
                    None    => 0.50,
                }
            };

            let sig = match compute(
                slug, &meta.asset, meta.tf,
                meta.open_price, cl, sigma, secs_left,
                &bd_yes, &bd_no, cfg.strategy.stake, now,
            ) {
                Some(s) => s,
                None    => continue,
            };

            runner.on_signal(&sig, meta.window_end, &meta.token_yes, &meta.token_no).await;
        }

        // ── Real-time dashboard (every 2s) ──────────────────────────────────

        if now - last_dash_ts >= 2.0 {
            last_dash_ts = now;
            let mut lines: Vec<String> = Vec::new();

            for (slug, meta) in &markets {
                if settled.get(slug.as_str()).copied().unwrap_or(false) { continue; }
                let secs_left = meta.window_end as f64 - now;
                if secs_left < 0.0 { continue; }

                let open_str = if meta.open_price > 0.0 {
                    format!("{:.2}", meta.open_price)
                } else { "WAIT".to_string() };

                let (cl_str, cl_val) = match cl_prices.get(&meta.asset) {
                    Some(v) => (format!("{:.2}", v.1), v.1),
                    None    => ("--".to_string(), 0.0),
                };

                let delta_str = if meta.open_price > 0.0 && cl_val > 0.0 {
                    format!("{:+.4}%", (cl_val - meta.open_price) / meta.open_price * 100.0)
                } else { "--".to_string() };

                let sigma_str = {
                    let hist = price_history.get(&meta.asset);
                    match hist {
                        Some(h) => {
                            let sig = estimate_sigma(&h, cfg.scan.sigma_window_secs, now);
                            format!("{:.1}%", sig * 100.0)
                        }
                        None => "--".to_string(),
                    }
                };

                let (pm_yes_str, pm_no_str) = {
                    let yes_book = book_state.get(&meta.token_yes);
                    let no_book  = book_state.get(&meta.token_no);
                    let yes = match &yes_book {
                        Some(b) => format!("{:.2}/{:.2}", b.best_bid, b.best_ask),
                        None    => "--/--".to_string(),
                    };
                    let no = match &no_book {
                        Some(b) => format!("{:.2}/{:.2}", b.best_bid, b.best_ask),
                        None    => "--/--".to_string(),
                    };
                    (yes, no)
                };

                lines.push(format!(
                    "  {} | open={} cl={} d={} | σ={} | YES={} NO={} | T-{:.0}s",
                    slug, open_str, cl_str, delta_str, sigma_str, pm_yes_str, pm_no_str, secs_left
                ));
            }

            if !lines.is_empty() {
                lines.sort();
                info!("─── LIVE ─────────────────────────────────────────────────────");
                for line in &lines {
                    info!("{}", line);
                }
            }
        }

        // ── Stats (every 60s) ───────────────────────────────────────────────

        if now - last_stats_ts >= 60.0 {
            last_stats_ts = now;
            info!("══════════════════════════════════════════════════════════════");
            runner.print_stats();
            info!("markets={} settled={} books={}", markets.len(), settled.len(), book_state.len());
        }
    }

    Ok(())
}
