/// main.rs — CL Scanner v2.0
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
/// 8. Graceful shutdown on Ctrl-C

mod feeds;
mod runner;
mod signal;

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write as IoWrite;
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
    BookState, ClPrices, FeedHealth, FeedParams, LogWriter, MarketMeta, PriceHistory, RateLimiter,
    build_slug, current_window_starts, fetch_books_batch, fetch_market_meta,
    run_book_feed, run_cl_feed,
};
use runner::{ConfigRunner, RunnerConfig};
use signal::{compute, estimate_sigma, BookSnap, Side};

// ── Tick-level signal log ────────────────────────────────────────────────────

#[derive(serde::Serialize)]
struct TickLog {
    tick:       u64,
    ts:         f64,
    slug:       String,
    asset:      String,
    tf:         u32,
    cl_price:   f64,
    open_price: f64,
    sigma:      f64,
    secs_left:  f64,
    book_yes:   f64,
    book_no:    f64,
    bid_yes:    f64,
    bid_no:     f64,
    fair_yes:   f64,
    fair_no:    f64,
    edge_yes:   f64,
    edge_no:    f64,
    best_side:  String,
    best_edge:  f64,
    // v2: microstructure
    spread_yes:    f64,
    spread_no:     f64,
    depth_ask_yes: f64,
    depth_ask_no:  f64,
    book_age_yes:  f64,
    book_age_no:   f64,
    cl_age:        f64,
}


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
    cl_stale_secs:     f64,
    book_stale_secs:   f64,
}

#[derive(Deserialize, Debug)]
struct ScanConfig {
    tick_ms:           u64,
    sigma_window_secs: f64,
    min_secs:          f64,
}

#[derive(Deserialize, Debug)]
struct PaperConfig {
    stake: f64,
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

    info!("CL Oracle Scanner v2.0.0 starting");
    info!("Assets: {:?}", cfg.feed.assets);
    info!("Timeframes: {:?}m", cfg.feed.timeframes);
    info!("Staleness gates: CL={:.0}s Book={:.0}s", cfg.feed.cl_stale_secs, cfg.feed.book_stale_secs);

    // Build FeedParams from config
    let feed_params = FeedParams {
        book_batch_size:   cfg.feed.book_batch_size,
        rest_throttle_ms:  cfg.feed.rest_throttle_ms,
        ws_reconnect_secs: cfg.feed.ws_reconnect_secs,
        price_history_cap: cfg.feed.price_history_cap,
    };

    // Feed health counters (shared across all feeds)
    let feed_health = Arc::new(FeedHealth::new());

    // Create log directory
    std::fs::create_dir_all("logs").context("cannot create logs/")?;

    // Open JSONL log files for comprehensive data capture
    let cl_log: LogWriter = Arc::new(tokio::sync::Mutex::new(
        OpenOptions::new().create(true).append(true)
            .open("logs/cl_prices.jsonl").context("cannot open logs/cl_prices.jsonl")?
    ));
    let book_log: LogWriter = Arc::new(tokio::sync::Mutex::new(
        OpenOptions::new().create(true).append(true)
            .open("logs/books.jsonl").context("cannot open logs/books.jsonl")?
    ));
    let tick_log_file = Arc::new(tokio::sync::Mutex::new(
        OpenOptions::new().create(true).append(true)
            .open("logs/ticks.jsonl").context("cannot open logs/ticks.jsonl")?
    ));
    let mtm_log_file = Arc::new(tokio::sync::Mutex::new(
        OpenOptions::new().create(true).append(true)
            .open("logs/mtm.jsonl").context("cannot open logs/mtm.jsonl")?
    ));

    info!("Logging to: logs/cl_prices.jsonl, logs/books.jsonl, logs/ticks.jsonl, logs/mtm.jsonl");

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
                        let yes_short = &meta.token_yes[..meta.token_yes.len().min(8)];
                        let no_short  = &meta.token_no[..meta.token_no.len().min(8)];
                        info!("[DISCOVER] {} YES={} NO={}",
                            slug, yes_short, no_short
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
        let cl_log = cl_log.clone();
        let params = feed_params.clone();
        let health = feed_health.clone();
        tokio::spawn(async move {
            run_cl_feed(ws, assets, cp, ph, cl_log, params, health).await;
        });
    }

    // Book WS sender for subscribing new tokens dynamically
    let (book_sub_tx, book_sub_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<String>>();

    {
        let bs = book_state.clone();
        let ti = token_ids.clone();
        let bl = book_live.clone();
        let ws = cfg.feed.clob_ws.clone();
        let book_log = book_log.clone();
        let params = feed_params.clone();
        let health = feed_health.clone();
        tokio::spawn(async move {
            run_book_feed(ws, ti, bs, bl, book_sub_rx, book_log, params, health).await;
        });
    }

    // ── Build runners ─────────────────────────────────────────────────────────

    let min_secs = cfg.scan.min_secs;
    let mut runners = vec![
        ConfigRunner::new(cfg.configs.c1.clone(), cfg.paper.stake, min_secs, "logs"),
        ConfigRunner::new(cfg.configs.c2.clone(), cfg.paper.stake, min_secs, "logs"),
        ConfigRunner::new(cfg.configs.c3.clone(), cfg.paper.stake, min_secs, "logs"),
        ConfigRunner::new(cfg.configs.c4.clone(), cfg.paper.stake, min_secs, "logs"),
        ConfigRunner::new(cfg.configs.c5.clone(), cfg.paper.stake, min_secs, "logs"),
    ];

    // ── Warmup gate ───────────────────────────────────────────────────────────

    info!("Waiting {}s for book feed warmup...", cfg.feed.book_warmup_secs);
    tokio::time::sleep(Duration::from_secs(cfg.feed.book_warmup_secs)).await;
    info!("Warmup complete — scan loop starting");

    // ── Main scan loop ────────────────────────────────────────────────────────

    let tick = Duration::from_millis(cfg.scan.tick_ms);
    let mut tick_count:    u64 = 0;
    let mut last_stats_ts: f64 = now_secs();
    let mut last_discover: f64 = now_secs();

    // Track which markets have been settled (slug → settled)
    let mut settled: HashMap<String, bool> = HashMap::new();

    // Record CL price at window close for accurate settlement
    // slug → (cl_price_at_close, timestamp)
    let mut close_prices: HashMap<String, (f64, f64)> = HashMap::new();

    // Staleness thresholds from config
    let cl_stale_secs   = cfg.feed.cl_stale_secs;
    let book_stale_secs = cfg.feed.book_stale_secs;

    loop {
        // Graceful shutdown — check for Ctrl-C alongside tick sleep
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Ctrl-C received — shutting down gracefully");
                break;
            }
            _ = tokio::time::sleep(tick) => {}
        }

        tick_count += 1;

        let now = now_secs();
        let now_u = now as u64;

        // ── Periodic market rediscovery (every 60s) ───────────────────────────
        // New windows open every 5/15 minutes — need to discover them

        if now - last_discover > 60.0 {
            last_discover = now;
            let mut new_tokens: Vec<String> = Vec::new();
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
                                new_tokens.push(meta.token_yes.clone());
                                new_tokens.push(meta.token_no.clone());
                                markets.insert(slug, meta);
                            }
                        }
                    }
                }
            }
            // Subscribe new tokens on the book WS
            if !new_tokens.is_empty() {
                let _ = book_sub_tx.send(new_tokens);
            }

            // Cleanup settled markets to prevent memory leak
            let stale_slugs: Vec<String> = settled.keys()
                .filter(|slug| {
                    markets.get(*slug)
                        .map(|m| now_u > m.window_end + 120) // keep for 2 min after end
                        .unwrap_or(true)
                })
                .cloned()
                .collect();
            for slug in &stale_slugs {
                if let Some(meta) = markets.remove(slug) {
                    token_ids.remove(&meta.token_yes);
                    token_ids.remove(&meta.token_no);
                    book_state.remove(&meta.token_yes);
                    book_state.remove(&meta.token_no);
                }
                settled.remove(slug);
                close_prices.remove(slug);
            }
            if !stale_slugs.is_empty() {
                debug!("[CLEANUP] Removed {} stale markets", stale_slugs.len());
            }
        }

        // ── Batch book refresh (every 2s via REST as fallback) ────────────────
        // WS is primary; REST fills gaps for markets WS hasn't snapped yet

        if tick_count % 4 == 0 {
            let all_tokens: Vec<String> = token_ids.iter().map(|e| e.key().clone()).collect();
            if !all_tokens.is_empty() {
                match fetch_books_batch(&http, &cfg.feed.clob_rest, &all_tokens, &limiter, &book_log, &feed_params).await {
                    Ok(books) => {
                        for (tid, entry) in books {
                            book_state.insert(tid, entry);
                        }
                    }
                    Err(e) => warn!("[BOOK REST] batch failed: {}", e),
                }
            }
        }

        // ── Capture CL price at exact window close ────────────────────────
        // Take the LAST CL price update before or at window_end.
        // Keep overwriting until settlement fires at window_end+5.
        for (slug, meta) in &markets {
            if settled.get(slug).copied().unwrap_or(false) {
                continue;
            }
            if now_u >= meta.window_end && now_u < meta.window_end + 5 {
                if let Some(cl_ref) = cl_prices.get(&meta.asset) {
                    let cl_ts = cl_ref.0;
                    let cl_price = cl_ref.1;
                    // Only use CL prices that arrived at or before window_end
                    if cl_ts <= meta.window_end as f64 + 1.0 {
                        // Always overwrite — want the freshest pre-close price
                        let prev = close_prices.get(slug).map(|(p, _)| *p);
                        close_prices.insert(slug.clone(), (cl_price, cl_ts));
                        if prev.is_none() {
                            info!("[CLOSE_PRICE] {} cl={:.6} cl_ts={:.3}", slug, cl_price, cl_ts);
                        }
                    }
                }
            }
        }

        // ── Check settlements ─────────────────────────────────────────────────

        for (slug, meta) in &markets {
            if settled.get(slug).copied().unwrap_or(false) {
                continue;
            }
            if now_u >= meta.window_end + 5 {
                if meta.open_price <= 0.0 {
                    continue;
                }

                // Use captured close price only — no fallback to post-window CL
                let cl_settle = match close_prices.get(slug) {
                    Some((p, _)) => *p,
                    None => {
                        warn!("[SETTLE] {} no close price captured, deferring", slug);
                        continue;
                    }
                };

                if cl_settle <= 0.0 {
                    continue;
                }

                // Determine winning side
                // Polymarket up/down: YES wins if cl >= open (at or above)
                let winning_side = if cl_settle >= meta.open_price {
                    Side::Yes
                } else {
                    Side::No
                };

                info!("[SETTLE] {} cl={:.2} open={:.2} winner={}",
                    slug, cl_settle, meta.open_price, winning_side);

                for runner in &mut runners {
                    runner.on_settlement(slug, winning_side, now).await;
                }

                settled.insert(slug.clone(), true);
            }
        }

        // ── Signal computation + runner dispatch ──────────────────────────────

        // Check book warmup gate
        let live_since = book_live.load(Ordering::Relaxed);
        if live_since == 0 || now_u - live_since < cfg.feed.book_warmup_secs {
            continue; // book not warm yet
        }

        for (slug, meta) in &mut markets {
            if settled.get(slug.as_str()).copied().unwrap_or(false) {
                continue;
            }

            let secs_left = meta.window_end as f64 - now;
            if secs_left < cfg.scan.min_secs {
                continue;
            }

            // Get CL price + check staleness
            let (cl, cl_ts) = match cl_prices.get(&meta.asset) {
                Some(v) => (v.1, v.0),
                None    => continue,
            };
            let cl_age = now - cl_ts;
            if cl_age > cl_stale_secs {
                debug!("[STALE_CL] {} cl_age={:.1}s > {:.0}s", slug, cl_age, cl_stale_secs);
                continue;
            }

            // Record open price at window start (first fresh CL price we see)
            if meta.open_price <= 0.0 {
                if now_u >= meta.window_start && cl_ts >= meta.window_start as f64 {
                    meta.open_price = cl;
                    info!("[OPEN] {} open_price={:.2} cl_ts={:.0}", slug, cl, cl_ts);
                }
                continue; // don't trade until open price is set
            }

            // Get book prices for YES and NO tokens + check staleness
            let yes_book_entry = match book_state.get(&meta.token_yes) {
                Some(b) => b.clone(),
                None    => continue,
            };
            let no_book_entry = match book_state.get(&meta.token_no) {
                Some(b) => b.clone(),
                None    => continue,
            };

            let yes_book_age = now - yes_book_entry.ts;
            let no_book_age  = now - no_book_entry.ts;

            if yes_book_age > book_stale_secs {
                debug!("[STALE_BOOK] {} YES book_age={:.1}s > {:.0}s", slug, yes_book_age, book_stale_secs);
                continue;
            }
            if no_book_age > book_stale_secs {
                debug!("[STALE_BOOK] {} NO book_age={:.1}s > {:.0}s", slug, no_book_age, book_stale_secs);
                continue;
            }

            // Build BookSnap structs
            let yes_snap = BookSnap {
                best_ask:  yes_book_entry.best_ask,
                best_bid:  yes_book_entry.best_bid,
                ask_depth: yes_book_entry.ask_depth,
                bid_depth: yes_book_entry.bid_depth,
                book_age:  yes_book_age,
            };
            let no_snap = BookSnap {
                best_ask:  no_book_entry.best_ask,
                best_bid:  no_book_entry.best_bid,
                ask_depth: no_book_entry.ask_depth,
                bid_depth: no_book_entry.bid_depth,
                book_age:  no_book_age,
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
                &yes_snap, &no_snap, cl_age, now,
            ) {
                Some(s) => s,
                None    => continue,
            };

            // Log EVERY signal computation to ticks.jsonl — zero gaps
            {
                let tick_entry = TickLog {
                    tick: tick_count,
                    ts: now,
                    slug: sig.slug.clone(),
                    asset: sig.asset.clone(),
                    tf: sig.tf,
                    cl_price: sig.cl_price,
                    open_price: sig.open_price,
                    sigma: sig.sigma,
                    secs_left: sig.secs_left,
                    book_yes: sig.book_yes,
                    book_no: sig.book_no,
                    bid_yes: sig.bid_yes,
                    bid_no: sig.bid_no,
                    fair_yes: sig.fair_yes,
                    fair_no: sig.fair_no,
                    edge_yes: sig.edge_yes,
                    edge_no: sig.edge_no,
                    best_side: sig.best_side.map(|s| s.to_string()).unwrap_or_default(),
                    best_edge: sig.best_edge,
                    spread_yes: sig.spread_yes,
                    spread_no: sig.spread_no,
                    depth_ask_yes: sig.depth_ask_yes,
                    depth_ask_no: sig.depth_ask_no,
                    book_age_yes: sig.book_age_yes,
                    book_age_no: sig.book_age_no,
                    cl_age: sig.cl_age,
                };
                if let Ok(line) = serde_json::to_string(&tick_entry) {
                    let mut f = tick_log_file.lock().await;
                    let _ = writeln!(f, "{}", line);
                }
            }

            if sig.best_edge > 0.05 {
                debug!(
                    "[SCAN] {} cl={:.2} fair_y={:.3} bk_y={:.3} bk_n={:.3} edge={:+.3} secs={:.0} cl_age={:.1}s",
                    slug, cl, sig.fair_yes, sig.book_yes, sig.book_no, sig.best_edge, secs_left, cl_age
                );
            }

            // Dispatch to all 5 runners + log mark-to-market for open positions
            for runner in &mut runners {
                // Log MTM for any open positions in this slug before processing
                for mtm in runner.get_mtm_entries(&sig) {
                    if let Ok(line) = serde_json::to_string(&mtm) {
                        let mut f = mtm_log_file.lock().await;
                        let _ = writeln!(f, "{}", line);
                    }
                }
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
            info!("open_markets={} settled={} book_entries={} cl_msgs={} book_msgs={}",
                markets.len(), settled.len(), book_state.len(),
                feed_health.cl_msg_count.load(Ordering::Relaxed),
                feed_health.book_msg_count.load(Ordering::Relaxed),
            );
        }
    }

    // ── Graceful shutdown ─────────────────────────────────────────────────────

    info!("Flushing runner logs...");
    for runner in &runners {
        runner.flush().await;
    }

    info!("Final stats:");
    for runner in &runners {
        runner.print_stats();
    }

    info!("CL Oracle Scanner v2.0.0 shutdown complete");
    Ok(())
}
