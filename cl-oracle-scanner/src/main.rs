/// main.rs — Lag Scanner
///
/// Polymarket CL oracle lag scanner.
/// Detects mispricing between Chainlink oracle prices and Polymarket order books
/// on crypto up/down prediction markets (5m and 15m windows).
///
/// Architecture:
/// 1. Discover active markets via Gamma API
/// 2. CL price feed (WebSocket) — real-time oracle prices
/// 3. PM book feed (WebSocket + REST fallback) — order book state
/// 4. Scan loop (500ms): compute fair values, detect edge, dispatch to runners
/// 5. Per-config runners: independent paper trading with different strategies
///
/// Logging (all JSONL, one object per line):
/// - logs/scan.jsonl   — every signal computation (all data points per tick)
/// - logs/events.jsonl — market lifecycle (DISCOVER, OPEN, OPEN_MISSED, SETTLE_CAPTURE, SETTLE)
/// - logs/{name}.jsonl — trade logs per config (entries, exits, PnL)

mod feeds;
mod runner;
mod signal;

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use dashmap::DashMap;
use reqwest::Client;
use serde::Deserialize;
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

use feeds::{
    BookState, ClPrices, MarketMeta, PriceHistory, RateLimiter,
    build_slug, current_window_starts, fetch_books_batch, fetch_market_meta,
    run_book_feed, run_cl_feed,
};
use runner::{ConfigRunner, RunnerConfig};
use signal::{compute, estimate_sigma};

// -- Config file types --------------------------------------------------------

#[derive(Deserialize, Debug)]
struct AppConfig {
    feed:     FeedConfig,
    scan:     ScanConfig,
    scan_5m:  TfScanConfig,
    scan_15m: TfScanConfig,
    paper:    PaperConfig,
    configs:  HashMap<String, RunnerConfig>,
}

#[derive(Deserialize, Debug)]
struct FeedConfig {
    assets:           Vec<String>,
    timeframes:       Vec<u32>,
    clob_rest:        String,
    clob_ws:          String,
    live_ws:          String,
    gamma_api:        String,
    #[allow(dead_code)]
    book_batch_size:  usize,
    rest_throttle_ms: u64,
    book_warmup_secs: u64,
    max_open_delay:   f64,
}

#[derive(Deserialize, Debug)]
struct ScanConfig {
    tick_ms: u64,
}

#[derive(Deserialize, Debug)]
struct TfScanConfig {
    sigma_window_secs: f64,
    settle_delay_secs: u64,
}

#[derive(Deserialize, Debug)]
struct PaperConfig {
    stake:          f64,
    taker_fee_rate: f64,
}

// -- Helpers ------------------------------------------------------------------

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

fn tf_scan_config(cfg: &AppConfig, tf: u32) -> &TfScanConfig {
    if tf == 5 { &cfg.scan_5m } else { &cfg.scan_15m }
}

// -- Log writers --------------------------------------------------------------

fn open_log(path: &str) -> BufWriter<File> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|e| panic!("Cannot open {}: {}", path, e));
    BufWriter::new(file)
}

fn log_json(w: &mut BufWriter<File>, v: &serde_json::Value) {
    if let Ok(line) = serde_json::to_string(v) {
        let _ = writeln!(w, "{}", line);
    }
}

// -- Main ---------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    // Logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("lag_scanner=info".parse()?)
        )
        .with_target(false)
        .init();

    // Load config
    let cfg_text = std::fs::read_to_string("config.toml")
        .context("cannot read config.toml")?;
    let cfg: AppConfig = toml::from_str(&cfg_text)
        .context("invalid config.toml")?;

    info!("Lag Scanner starting");
    info!("Assets: {:?}", cfg.feed.assets);
    info!("Timeframes: {:?}m", cfg.feed.timeframes);
    info!("Configs: {} (5m sigma={}s, 15m sigma={}s)",
        cfg.configs.len(), cfg.scan_5m.sigma_window_secs, cfg.scan_15m.sigma_window_secs);

    // Create log directory
    std::fs::create_dir_all("logs").context("cannot create logs/")?;

    // Open log files
    let mut scan_log  = open_log("logs/scan.jsonl");
    let mut event_log = open_log("logs/events.jsonl");

    // Shared state
    let cl_prices:     ClPrices      = Arc::new(DashMap::new());
    let book_state:    BookState     = Arc::new(DashMap::new());
    let price_history: PriceHistory  = Arc::new(DashMap::new());
    let token_ids:     Arc<DashMap<String, ()>> = Arc::new(DashMap::new());
    let book_live:     Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

    // HTTP client (shared, connection pooled)
    let http = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("HTTP client build failed")?;

    let limiter = Arc::new(RateLimiter::new(cfg.feed.rest_throttle_ms));

    let fee_rate = cfg.paper.taker_fee_rate;

    // -- Discover markets -----------------------------------------------------

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
                        info!("[DISCOVER] {} YES={} NO={} ws={} we={}",
                            slug,
                            &meta.token_yes[..8.min(meta.token_yes.len())],
                            &meta.token_no[..8.min(meta.token_no.len())],
                            meta.window_start, meta.window_end,
                        );
                        log_json(&mut event_log, &serde_json::json!({
                            "event": "DISCOVER", "ts": now_secs(),
                            "slug": &slug, "asset": asset, "tf": tf,
                            "window_start": meta.window_start,
                            "window_end": meta.window_end,
                            "token_yes": &meta.token_yes,
                            "token_no": &meta.token_no,
                        }));
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

    // -- Start WebSocket feeds ------------------------------------------------

    {
        let cp = cl_prices.clone();
        let ph = price_history.clone();
        let assets = cfg.feed.assets.clone();
        let ws = cfg.feed.live_ws.clone();
        tokio::spawn(async move {
            run_cl_feed(ws, assets, cp, ph).await;
        });
    }

    {
        let bs = book_state.clone();
        let ti = token_ids.clone();
        let bl = book_live.clone();
        let ws = cfg.feed.clob_ws.clone();
        tokio::spawn(async move {
            run_book_feed(ws, ti, bs, bl).await;
        });
    }

    // -- Build runners --------------------------------------------------------

    let mut runners: Vec<ConfigRunner> = cfg.configs.values()
        .map(|rc| ConfigRunner::new(rc.clone(), cfg.paper.stake, cfg.paper.taker_fee_rate, "logs"))
        .collect();

    // Sort by name for consistent ordering
    runners.sort_by(|a, b| a.config.name.cmp(&b.config.name));

    info!("{} runners initialized:", runners.len());
    for r in &runners {
        info!("  [{}] tf={}m edge>={:.2} secs=[{:.0}..{:.0}] sl={} tp={}",
            r.config.name, r.config.tf, r.config.min_edge,
            r.config.min_secs, r.config.max_secs_left,
            r.config.stop_loss, r.config.take_profit);
    }

    // -- Warmup gate ----------------------------------------------------------

    info!("Waiting {}s for book feed warmup...", cfg.feed.book_warmup_secs);
    tokio::time::sleep(Duration::from_secs(cfg.feed.book_warmup_secs)).await;
    info!("Warmup complete — scan loop starting");

    // -- Main scan loop -------------------------------------------------------

    let tick = Duration::from_millis(cfg.scan.tick_ms);
    let mut tick_count:    u64 = 0;
    let mut last_stats_ts: f64 = now_secs();
    let mut last_discover: f64 = now_secs();

    let mut settled: HashMap<String, bool> = HashMap::new();

    let max_open_delay = cfg.feed.max_open_delay;

    loop {
        tokio::time::sleep(tick).await;
        tick_count += 1;

        let now = now_secs();
        let now_u = now as u64;

        // -- Periodic market rediscovery (every 60s) --------------------------

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
                                info!("[DISCOVER] New: {} ws={} we={}",
                                    slug, meta.window_start, meta.window_end);
                                log_json(&mut event_log, &serde_json::json!({
                                    "event": "DISCOVER", "ts": now,
                                    "slug": &slug, "asset": asset, "tf": tf,
                                    "window_start": meta.window_start,
                                    "window_end": meta.window_end,
                                    "token_yes": &meta.token_yes,
                                    "token_no": &meta.token_no,
                                }));
                                token_ids.insert(meta.token_yes.clone(), ());
                                token_ids.insert(meta.token_no.clone(),  ());
                                markets.insert(slug, meta);
                            }
                        }
                    }
                }
            }
        }

        // -- Batch book refresh (every 2s via REST as fallback) ---------------
        // REST covers newly discovered tokens that WS hasn't subscribed to yet.

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

        // -- Process each market ----------------------------------------------

        let live_since = book_live.load(Ordering::Relaxed);
        let book_ready = live_since > 0 && now_u.saturating_sub(live_since) >= cfg.feed.book_warmup_secs;

        for (slug, meta) in &mut markets {
            // Skip fully settled markets
            if settled.get(slug.as_str()).copied().unwrap_or(false) {
                continue;
            }

            // --- Settle price capture ----------------------------------------
            // Capture CL price at window_end (first opportunity).
            // Uses the CL tick closest to window_end for accurate settlement.
            if now >= meta.window_end as f64 && meta.settle_price <= 0.0
                && !meta.open_missed && meta.open_price > 0.0
            {
                if let Some(cl_entry) = cl_prices.get(&meta.asset) {
                    let (cl_ts, cl_price) = *cl_entry;
                    // Accept if CL timestamp is within 2s before window_end or after
                    if cl_ts >= meta.window_end as f64 - 2.0 && cl_price > 0.0 {
                        meta.settle_price = cl_price;
                        meta.settle_cl_ts = cl_ts;
                        let delay = now - meta.window_end as f64;
                        info!("[SETTLE_CAPTURE] {} price={:.2} open={:.2} delay={:.1}s cl_ts_delta={:.1}s",
                            slug, cl_price, meta.open_price, delay, cl_ts - meta.window_end as f64);
                        log_json(&mut event_log, &serde_json::json!({
                            "event": "SETTLE_CAPTURE", "ts": now,
                            "slug": slug, "asset": &meta.asset, "tf": meta.tf,
                            "settle_price": cl_price,
                            "open_price": meta.open_price,
                            "capture_delay_s": delay,
                            "cl_ts": cl_ts,
                            "cl_ts_delta_from_end_s": cl_ts - meta.window_end as f64,
                            "window_start": meta.window_start,
                            "window_end": meta.window_end,
                        }));
                    }
                }
            }

            // --- Execute settlement ------------------------------------------
            // Wait settle_delay after window_end, then settle using captured price.
            if meta.settle_price > 0.0 && meta.open_price > 0.0 {
                let tf_cfg = tf_scan_config(&cfg, meta.tf);
                if now_u >= meta.window_end + tf_cfg.settle_delay_secs {
                    let outcome = if meta.settle_price > meta.open_price { 1.0 } else { 0.0 };
                    let result_str = if outcome == 1.0 { "YES" } else { "NO" };

                    info!("[SETTLE] {} tf={}m open={:.2} settle={:.2} outcome={}",
                        slug, meta.tf, meta.open_price, meta.settle_price, result_str);
                    log_json(&mut event_log, &serde_json::json!({
                        "event": "SETTLE", "ts": now,
                        "slug": slug, "asset": &meta.asset, "tf": meta.tf,
                        "open_price": meta.open_price,
                        "open_cl_ts": meta.open_cl_ts,
                        "settle_price": meta.settle_price,
                        "settle_cl_ts": meta.settle_cl_ts,
                        "outcome": result_str,
                        "window_start": meta.window_start,
                        "window_end": meta.window_end,
                    }));

                    for runner in &mut runners {
                        runner.on_settlement(slug, outcome, now).await;
                    }

                    settled.insert(slug.clone(), true);
                    continue;
                }
            }

            // --- Window not started yet --------------------------------------
            if now < meta.window_start as f64 {
                continue;
            }

            // --- Capture open price ------------------------------------------
            // Use first CL tick at or after window_start.
            // Reject if we're more than max_open_delay seconds late.
            if meta.open_price <= 0.0 {
                if meta.open_missed {
                    continue;
                }

                let delay = now - meta.window_start as f64;
                if delay > max_open_delay {
                    meta.open_missed = true;
                    warn!("[OPEN] {} MISSED — {:.1}s late (max {}s), skipping window",
                        slug, delay, max_open_delay);
                    log_json(&mut event_log, &serde_json::json!({
                        "event": "OPEN_MISSED", "ts": now,
                        "slug": slug, "asset": &meta.asset, "tf": meta.tf,
                        "delay_s": delay,
                        "max_open_delay": max_open_delay,
                        "window_start": meta.window_start,
                        "window_end": meta.window_end,
                    }));
                    continue;
                }

                // First CL tick with timestamp at or after window_start
                if let Some(cl_entry) = cl_prices.get(&meta.asset) {
                    let (cl_ts, cl_price) = *cl_entry;
                    if cl_ts >= meta.window_start as f64 && cl_price > 0.0 {
                        meta.open_price = cl_price;
                        meta.open_cl_ts = cl_ts;
                        info!("[OPEN] {} tf={}m open={:.2} delay={:.1}s cl_ts={:.3}",
                            slug, meta.tf, cl_price, delay, cl_ts);
                        log_json(&mut event_log, &serde_json::json!({
                            "event": "OPEN", "ts": now,
                            "slug": slug, "asset": &meta.asset, "tf": meta.tf,
                            "open_price": cl_price,
                            "cl_ts": cl_ts,
                            "delay_s": delay,
                            "window_start": meta.window_start,
                            "window_end": meta.window_end,
                        }));
                    }
                }
                continue;
            }

            // --- Skip if window ended (waiting for settlement) ---------------
            if now >= meta.window_end as f64 {
                continue;
            }

            // --- Skip if book feed not ready ---------------------------------
            if !book_ready {
                continue;
            }

            // --- Signal computation ------------------------------------------

            let secs_left = meta.window_end as f64 - now;

            // Get CL price
            let (cl_ts, cl) = match cl_prices.get(&meta.asset) {
                Some(v) => (v.0, v.1),
                None    => continue,
            };

            // Get book prices + timestamps
            let (book_yes, book_yes_ts) = match book_state.get(&meta.token_yes) {
                Some(b) => (b.best_ask, b.ts),
                None    => continue,
            };
            let (book_no, book_no_ts) = match book_state.get(&meta.token_no) {
                Some(b) => (b.best_ask, b.ts),
                None    => continue,
            };

            // Skip if book data is stale (>30s old — likely WS dropped)
            let book_age = (now - book_yes_ts).max(now - book_no_ts);
            if book_age > 30.0 {
                debug!("[SCAN] {} book stale by {:.0}s, skipping", slug, book_age);
                continue;
            }

            // Estimate sigma (per-TF window)
            let tf_cfg = tf_scan_config(&cfg, meta.tf);
            let sigma = {
                let hist = price_history.get(&meta.asset);
                match hist {
                    Some(h) => estimate_sigma(&h, tf_cfg.sigma_window_secs, now),
                    None    => 0.001,
                }
            };

            // Compute signal ONCE — shared across all runners
            let sig = match compute(
                slug, &meta.asset, meta.tf,
                meta.open_price, cl, sigma, secs_left,
                book_yes, book_no, fee_rate, now,
            ) {
                Some(s) => s,
                None    => continue,
            };

            // --- Log to scan.jsonl (every tick, all data points) -------------
            log_json(&mut scan_log, &serde_json::json!({
                "ts": now,
                "slug": slug,
                "asset": &meta.asset,
                "tf": meta.tf,
                "cl": cl,
                "cl_ts": cl_ts,
                "cl_age_ms": ((now - cl_ts) * 1000.0).round(),
                "open": meta.open_price,
                "open_cl_ts": meta.open_cl_ts,
                "sigma": (sigma * 10000.0).round() / 10000.0,
                "secs_left": secs_left.round(),
                "fair_y": (sig.fair_yes * 10000.0).round() / 10000.0,
                "fair_n": (sig.fair_no * 10000.0).round() / 10000.0,
                "bk_y": book_yes,
                "bk_n": book_no,
                "bk_y_ts": book_yes_ts,
                "bk_n_ts": book_no_ts,
                "bk_y_age_ms": ((now - book_yes_ts) * 1000.0).round(),
                "bk_n_age_ms": ((now - book_no_ts) * 1000.0).round(),
                "edge_y": (sig.edge_yes * 10000.0).round() / 10000.0,
                "edge_n": (sig.edge_no * 10000.0).round() / 10000.0,
                "best_side": sig.best_side.map(|s| s.to_string()).unwrap_or_default(),
                "best_edge": (sig.best_edge * 10000.0).round() / 10000.0,
                "window_start": meta.window_start,
                "window_end": meta.window_end,
            }));

            // Log interesting edges at INFO
            if sig.best_edge > 0.01 {
                info!(
                    "[SCAN] {} tf={}m cl={:.2} open={:.2} fair_y={:.3} bk_y={:.3} bk_n={:.3} edge={:+.3} sig={:.4} secs={:.0}",
                    slug, meta.tf, cl, meta.open_price, sig.fair_yes, book_yes, book_no, sig.best_edge, sigma, secs_left
                );
            }

            // Dispatch to all runners (each filters by own tf)
            for runner in &mut runners {
                runner.on_signal(&sig, meta.window_end).await;
            }
        }

        // -- Flush logs periodically (every 10s) -----------------------------

        if tick_count % 20 == 0 {
            let _ = scan_log.flush();
            let _ = event_log.flush();
        }

        // -- Periodic stats print (every 60s) ---------------------------------

        if now - last_stats_ts >= 60.0 {
            last_stats_ts = now;
            info!("──────────────────────────────────────────────────────");
            info!("5m runners:");
            for runner in runners.iter().filter(|r| r.config.tf == 5) {
                runner.print_stats();
            }
            info!("15m runners:");
            for runner in runners.iter().filter(|r| r.config.tf == 15) {
                runner.print_stats();
            }
            let active = markets.values().filter(|m| !m.open_missed && m.open_price > 0.0).count();
            let missed = markets.values().filter(|m| m.open_missed).count();
            info!("markets={} active={} settled={} missed={} book={} cl={}",
                markets.len(), active, settled.len(), missed,
                book_state.len(), cl_prices.len(),
            );
        }
    }
}
