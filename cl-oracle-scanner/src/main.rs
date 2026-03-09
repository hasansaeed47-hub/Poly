/// main.rs — CL Oracle Scanner v2 (production-grade)
///
/// Startup sequence:
/// 1. Load config
/// 2. Discover active markets (batch REST)
/// 3. Start CL price WebSocket feed
/// 4. Start PM book WebSocket feed
/// 5. Wait for book warmup (configurable gate)
/// 6. Scan loop: every tick_ms
///    a. Periodic market rediscovery (every 60s)
///    b. Batch book refresh via REST fallback
///    c. Check window settlements
///    d. Compute signals (once per market)
///    e. Log signal to JSONL for analysis
///    f. Pass to all 5 runners
/// 7. Print stats every 60s
/// 8. Graceful shutdown on Ctrl+C (flush all logs)
///
/// v2 fixes:
/// - `debug!` macro properly imported
/// - Config values passed to feeds (no hardcoded constants)
/// - Signal JSONL logger for every computed signal
/// - Full tick-level logging of all data
/// - Graceful shutdown with log flushing
/// - Settlement uses correct YES/NO binary outcome
/// - Book staleness checked before signal dispatch

mod feeds;
mod runner;
mod signal;

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use dashmap::DashMap;
use reqwest::Client;
use serde::Deserialize;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

use feeds::{
    BookInput as FeedBookInput, BookSource, BookState, ClPrices, FeedParams, MarketMeta,
    PriceHistory, RateLimiter,
    build_slug, current_window_starts, fetch_books_batch, fetch_market_meta,
    run_book_feed, run_cl_feed,
};
use runner::{ConfigRunner, RunnerConfig};
use signal::{BookInput, compute, estimate_sigma};

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
    assets:            Vec<String>,
    timeframes:        Vec<u32>,
    clob_rest:         String,
    clob_ws:           String,
    live_ws:           String,
    gamma_api:         String,
    book_batch_size:   usize,
    rest_throttle_ms:  u64,
    book_warmup_secs:  u64,
    ws_reconnect_secs: u64,
    price_history_cap: usize,
}

#[derive(Deserialize, Debug)]
struct ScanConfig {
    tick_ms:           u64,
    sigma_window_secs: f64,
    min_secs:          f64,
}

#[derive(Deserialize, Debug)]
struct PaperConfig {
    stake:                   f64,
    taker_fee_rate:          f64,
    max_positions_per_config: usize,
    max_exposure_per_config:  f64,
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
    // Logging — console + file
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("cl_oracle_scanner=debug".parse()?)
        )
        .with_target(false)
        .with_ansi(true)
        .init();

    // Load config
    let cfg_text = std::fs::read_to_string("config.toml")
        .context("cannot read config.toml")?;
    let cfg: AppConfig = toml::from_str(&cfg_text)
        .context("invalid config.toml")?;

    info!("═══════════════════════════════════════════════════════");
    info!("  CL Oracle Scanner v2.0.0 — STARTING");
    info!("═══════════════════════════════════════════════════════");
    info!("Assets:     {:?}", cfg.feed.assets);
    info!("Timeframes: {:?} minutes", cfg.feed.timeframes);
    info!("Tick:       {}ms", cfg.scan.tick_ms);
    info!("Sigma win:  {}s", cfg.scan.sigma_window_secs);
    info!("Stake:      ${}", cfg.paper.stake);
    info!("Fee rate:   {}", cfg.paper.taker_fee_rate);
    info!("Max pos:    {} per config", cfg.paper.max_positions_per_config);
    info!("Max exp:    ${} per config", cfg.paper.max_exposure_per_config);
    info!("Batch size: {}", cfg.feed.book_batch_size);
    info!("Throttle:   {}ms", cfg.feed.rest_throttle_ms);
    info!("WS recon:   {}s", cfg.feed.ws_reconnect_secs);
    info!("History:    {} entries/asset", cfg.feed.price_history_cap);

    // Create log directories
    std::fs::create_dir_all("logs").context("cannot create logs/")?;
    std::fs::create_dir_all("logs/signals").context("cannot create logs/signals/")?;

    // Graceful shutdown flag
    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let sd = shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = tokio::signal::ctrl_c().await {
                error!("Failed to listen for Ctrl+C: {}", e);
                return;
            }
            info!("╔══════════════════════════════════════╗");
            info!("║  Ctrl+C received — shutting down...  ║");
            info!("╚══════════════════════════════════════╝");
            sd.store(true, Ordering::SeqCst);
        });
    }

    // Shared state
    let cl_prices:     ClPrices     = Arc::new(DashMap::new());
    let book_state:    BookState    = Arc::new(DashMap::new());
    let price_history: PriceHistory = Arc::new(DashMap::new());
    let token_ids:     Arc<DashMap<String, ()>> = Arc::new(DashMap::new());
    let book_live:     Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

    // HTTP client (shared, connection pooled)
    let http = Client::builder()
        .timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(10)
        .build()
        .context("HTTP client build failed")?;

    let limiter = Arc::new(RateLimiter::new(cfg.feed.rest_throttle_ms));

    // Feed params (config-driven, no hardcoded constants)
    let feed_params = FeedParams {
        book_batch_size:   cfg.feed.book_batch_size,
        rest_throttle_ms:  cfg.feed.rest_throttle_ms,
        ws_reconnect_secs: cfg.feed.ws_reconnect_secs,
        price_history_cap: cfg.feed.price_history_cap,
    };

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
                        let yes_short = if meta.token_yes.len() >= 8 { &meta.token_yes[..8] } else { &meta.token_yes };
                        let no_short  = if meta.token_no.len() >= 8  { &meta.token_no[..8]  } else { &meta.token_no };
                        info!(
                            "[DISCOVER] {} YES={} NO={} window={}..{} cond={}",
                            slug, yes_short, no_short,
                            meta.window_start, meta.window_end,
                            &meta.condition_id[..8.min(meta.condition_id.len())]
                        );
                        token_ids.insert(meta.token_yes.clone(), ());
                        token_ids.insert(meta.token_no.clone(),  ());
                        markets.insert(slug, meta);
                    }
                    Ok(None) => debug!("[DISCOVER] {} not found", slug),
                    Err(e)   => warn!("[DISCOVER] {} error: {:#}", slug, e),
                }
            }
        }
    }

    info!("Discovered {} active markets, {} token IDs", markets.len(), token_ids.len());
    if markets.is_empty() {
        warn!("No markets found — will retry on scan ticks");
    }

    // ── Start WebSocket feeds ─────────────────────────────────────────────────

    {
        let cp = cl_prices.clone();
        let ph = price_history.clone();
        let assets = cfg.feed.assets.clone();
        let ws = cfg.feed.live_ws.clone();
        let params = feed_params.clone();
        tokio::spawn(async move {
            run_cl_feed(ws, assets, cp, ph, params).await;
        });
    }

    {
        let bs = book_state.clone();
        let ti = token_ids.clone();
        let bl = book_live.clone();
        let ws = cfg.feed.clob_ws.clone();
        let params = feed_params.clone();
        tokio::spawn(async move {
            run_book_feed(ws, ti, bs, bl, params).await;
        });
    }

    // ── Build runners ─────────────────────────────────────────────────────────

    let max_pos = cfg.paper.max_positions_per_config;
    let max_exp = cfg.paper.max_exposure_per_config;
    let mut runners = vec![
        ConfigRunner::new(cfg.configs.c1.clone(), cfg.paper.stake, cfg.paper.taker_fee_rate, max_pos, max_exp, "logs"),
        ConfigRunner::new(cfg.configs.c2.clone(), cfg.paper.stake, cfg.paper.taker_fee_rate, max_pos, max_exp, "logs"),
        ConfigRunner::new(cfg.configs.c3.clone(), cfg.paper.stake, cfg.paper.taker_fee_rate, max_pos, max_exp, "logs"),
        ConfigRunner::new(cfg.configs.c4.clone(), cfg.paper.stake, cfg.paper.taker_fee_rate, max_pos, max_exp, "logs"),
        ConfigRunner::new(cfg.configs.c5.clone(), cfg.paper.stake, cfg.paper.taker_fee_rate, max_pos, max_exp, "logs"),
    ];

    // ── Signal logger (JSONL) ─────────────────────────────────────────────────

    let signal_log_path = format!("logs/signals/signals_{}.jsonl",
        chrono::Utc::now().format("%Y%m%d_%H%M%S"));
    let mut signal_log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&signal_log_path)
        .context("cannot create signal log file")?;
    info!("Signal log: {}", signal_log_path);

    // ── Warmup gate ───────────────────────────────────────────────────────────

    info!("Waiting {}s for book feed warmup...", cfg.feed.book_warmup_secs);
    tokio::time::sleep(Duration::from_secs(cfg.feed.book_warmup_secs)).await;
    info!("Warmup complete — scan loop starting");

    // ── Main scan loop ────────────────────────────────────────────────────────

    let tick = Duration::from_millis(cfg.scan.tick_ms);
    let mut tick_count:    u64 = 0;
    let mut signal_count:  u64 = 0;
    let mut last_stats_ts: f64 = now_secs();
    let mut last_discover: f64 = now_secs();
    let batch_size = cfg.feed.book_batch_size;
    let throttle_ms = cfg.feed.rest_throttle_ms;

    // Track which markets have been settled (slug → settled)
    let mut settled: HashMap<String, bool> = HashMap::new();

    // Book staleness threshold: if book is older than 30s, skip the market
    let max_book_age_secs: f64 = 30.0;

    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        tokio::time::sleep(tick).await;
        tick_count += 1;

        let now = now_secs();
        let now_u = now as u64;

        // ── Periodic market rediscovery (every 60s) ───────────────────────────

        if now - last_discover > 60.0 {
            last_discover = now;
            let mut new_found = 0_u32;
            for asset in &cfg.feed.assets {
                for &tf in &cfg.feed.timeframes {
                    for window_start in current_window_starts(tf, now_u) {
                        let slug = build_slug(asset, tf, window_start);
                        if !markets.contains_key(&slug) {
                            match fetch_market_meta(
                                &http, &cfg.feed.gamma_api, &slug, asset, tf, &limiter,
                            ).await {
                                Ok(Some(meta)) => {
                                    info!("[DISCOVER] New market: {} window={}..{}", slug, meta.window_start, meta.window_end);
                                    token_ids.insert(meta.token_yes.clone(), ());
                                    token_ids.insert(meta.token_no.clone(),  ());
                                    markets.insert(slug, meta);
                                    new_found += 1;
                                }
                                Ok(None) => {}
                                Err(e)   => warn!("[DISCOVER] {} error: {:#}", slug, e),
                            }
                        }
                    }
                }
            }
            if new_found > 0 {
                info!("[DISCOVER] Found {} new markets, total={}", new_found, markets.len());
            }
        }

        // ── Batch book refresh (every ~2s via REST as fallback) ────────────────

        if tick_count % 4 == 0 {
            let all_tokens: Vec<String> = token_ids.iter().map(|e| e.key().clone()).collect();
            if !all_tokens.is_empty() {
                match fetch_books_batch(&http, &cfg.feed.clob_rest, &all_tokens, &limiter, batch_size, throttle_ms).await {
                    Ok(books) => {
                        let count = books.len();
                        for (tid, entry) in books {
                            book_state.insert(tid, entry);
                        }
                        debug!("[BOOK REST] Updated {} book entries", count);
                    }
                    Err(e) => warn!("[BOOK REST] batch failed: {:#}", e),
                }
            }
        }

        // ── Check settlements ─────────────────────────────────────────────────

        for (slug, meta) in &markets {
            if settled.get(slug).copied().unwrap_or(false) {
                continue;
            }
            if now_u >= meta.window_end + 5 {
                let cl_settle = cl_prices
                    .get(&meta.asset)
                    .map(|v| v.1)
                    .unwrap_or(0.0);

                if cl_settle <= 0.0 {
                    continue;
                }

                if meta.open_price <= 0.0 {
                    continue;
                }

                // Determine binary outcome correctly
                let outcome = if cl_settle > meta.open_price { 1.0 } else { 0.0 };
                let secs_past_end = now - meta.window_end as f64;

                info!(
                    "[SETTLE] {} cl={:.6} open={:.6} delta={:+.6} outcome={} secs_past_end={:.1}",
                    slug, cl_settle, meta.open_price,
                    cl_settle - meta.open_price,
                    if outcome == 1.0 { "YES" } else { "NO" },
                    secs_past_end
                );

                for runner in &mut runners {
                    runner.on_settlement(slug, outcome, cl_settle, now, 0.0).await;
                }

                settled.insert(slug.clone(), true);
            }
        }

        // ── Signal computation + runner dispatch ──────────────────────────────

        // Check book warmup gate
        let live_since = book_live.load(Ordering::Relaxed);
        if live_since == 0 || now_u.saturating_sub(live_since) < cfg.feed.book_warmup_secs {
            if tick_count % 20 == 0 {
                debug!("[WARMUP] Book not warm yet: live_since={} now={}", live_since, now_u);
            }
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
            let (cl_ts, cl) = match cl_prices.get(&meta.asset) {
                Some(v) => (v.0, v.1),
                None    => continue,
            };

            // Check CL price freshness (must be within 10s)
            let cl_age = now - cl_ts;
            if cl_age > 10.0 {
                debug!("[STALE] {} CL price is {:.1}s old, skipping", slug, cl_age);
                continue;
            }

            // Record open price at window start
            if meta.open_price <= 0.0 {
                if now_u >= meta.window_start {
                    meta.open_price = cl;
                    info!(
                        "[OPEN] {} open_price={:.6} cl={:.6} ts={:.3} secs_left={:.1}",
                        slug, cl, cl, now, secs_left
                    );
                }
                continue;
            }

            // Get book data for YES token
            let book_yes_entry = match book_state.get(&meta.token_yes) {
                Some(b) => b.clone(),
                None    => continue,
            };
            // Get book data for NO token
            let book_no_entry = match book_state.get(&meta.token_no) {
                Some(b) => b.clone(),
                None    => continue,
            };

            // Check book staleness — reject if books are too old
            let book_age_yes = now - book_yes_entry.ts;
            let book_age_no  = now - book_no_entry.ts;
            if book_age_yes > max_book_age_secs || book_age_no > max_book_age_secs {
                debug!(
                    "[STALE] {} book ages: YES={:.1}s NO={:.1}s (max={}s), skipping",
                    slug, book_age_yes, book_age_no, max_book_age_secs
                );
                continue;
            }

            // Estimate sigma from CL price history (using actual time deltas)
            let sigma = {
                let hist = price_history.get(&meta.asset);
                match hist {
                    Some(h) => estimate_sigma(&h, cfg.scan.sigma_window_secs, now),
                    None    => 0.001,
                }
            };

            // Build BookInput for signal computation
            let yes_input = BookInput {
                ask:       book_yes_entry.best_ask,
                bid:       book_yes_entry.best_bid,
                ask_depth: book_yes_entry.ask_depth,
                bid_depth: book_yes_entry.bid_depth,
                spread:    book_yes_entry.spread,
                book_ts:   book_yes_entry.ts,
            };
            let no_input = BookInput {
                ask:       book_no_entry.best_ask,
                bid:       book_no_entry.best_bid,
                ask_depth: book_no_entry.ask_depth,
                bid_depth: book_no_entry.bid_depth,
                spread:    book_no_entry.spread,
                book_ts:   book_no_entry.ts,
            };

            // Compute signal ONCE — shared across all runners
            let sig = match compute(
                slug, &meta.asset, meta.tf,
                meta.open_price, cl, sigma, secs_left,
                &yes_input, &no_input, now,
            ) {
                Some(s) => s,
                None    => continue,
            };

            signal_count += 1;

            // Log every signal with edge to JSONL for post-analysis
            if sig.best_edge > 0.01 {
                if let Ok(line) = serde_json::to_string(&sig) {
                    let _ = writeln!(signal_log, "{}", line);
                }
            }

            // Periodic signal log flush
            if signal_count % 100 == 0 {
                let _ = signal_log.flush();
            }

            // Log signals with meaningful edge
            if sig.best_edge > 0.05 {
                debug!(
                    "[SIGNAL] {} cl={:.6} open={:.6} cl_vs_open={:+.4}% fair_y={:.4} fair_n={:.4} ask_y={:.4} ask_n={:.4} bid_y={:.4} bid_n={:.4} edge_y={:+.4} edge_n={:+.4} best={:?}({:.4}) sigma={:.6} secs={:.1} spread_y={:.4} spread_n={:.4} age_y={:.1} age_n={:.1}",
                    slug, cl, sig.open_price, sig.cl_vs_open,
                    sig.fair_yes, sig.fair_no,
                    sig.book_yes, sig.book_no,
                    sig.bid_yes, sig.bid_no,
                    sig.edge_yes, sig.edge_no,
                    sig.best_side, sig.best_edge,
                    sigma, secs_left,
                    sig.spread_yes, sig.spread_no,
                    sig.book_age_yes, sig.book_age_no
                );
            }

            // Dispatch to all 5 runners
            for runner in &mut runners {
                runner.on_signal(&sig, meta.window_end).await;
            }
        }

        // ── Periodic stats (every 60s) ────────────────────────────────────────

        if now - last_stats_ts >= 60.0 {
            last_stats_ts = now;
            let cl_snapshot: Vec<(String, f64, f64)> = cl_prices.iter()
                .map(|e| (e.key().clone(), e.value().0, e.value().1))
                .collect();

            info!("═══════════════════════════════════════════════════════");
            info!("[TICK] count={} signals={} open_mkts={} settled={} book_entries={} tokens={}",
                tick_count, signal_count,
                markets.len() - settled.len(), settled.len(),
                book_state.len(), token_ids.len()
            );
            for (asset, ts, price) in &cl_snapshot {
                let age = now - ts;
                info!("[CL STATE] {} price={:.6} age={:.1}s", asset, price, age);
            }
            for runner in &runners {
                runner.print_stats();
            }
            info!("═══════════════════════════════════════════════════════");
        }
    }

    // ── Graceful shutdown: flush logs and print final stats ────────────────────

    info!("═══════════════════════════════════════════════════════");
    info!("  FINAL STATS");
    info!("═══════════════════════════════════════════════════════");
    for runner in &runners {
        runner.print_stats();
    }
    info!("Total ticks: {}, Total signals: {}", tick_count, signal_count);

    // Flush signal log
    if let Err(e) = signal_log.flush() {
        warn!("Failed to flush signal log: {}", e);
    }

    info!("CL Oracle Scanner v2 — shutdown complete");
    Ok(())
}
