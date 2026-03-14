/// Oracle Scanner V2 — Pure Data Capture (No Trading)
///
/// Captures ALL market data for offline analysis:
///   - Every signal tick (500ms) → data/signals_YYYY-MM-DD.jsonl
///   - Full order book snapshots → data/books_YYYY-MM-DD.jsonl
///   - Settlement outcomes      → data/settlements_YYYY-MM-DD.jsonl
///   - Raw CL price ticks       → data/cl_ticks_YYYY-MM-DD.jsonl
///
/// HTTP server on :8080 for file downloads:
///   GET /files          → list data files (JSON)
///   GET /download/{f}   → download a file
///   GET /status         → scanner health check
///
/// No wallet. No execution. No SDK. Pure observation.

mod feeds;
mod signal;

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::{Router, Json, extract::Path as AxumPath, response::IntoResponse};
use chrono::Utc;
use dashmap::DashMap;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use feeds::{
    BookState, ClPrices, MarketMeta, PriceHistory, RateLimiter,
    build_slug, current_window_starts, fetch_books_batch, fetch_market_meta,
    run_cl_feed,
};
use signal::{compute, estimate_sigma, fair_yes, BookData};

// ── Config ──────────────────────────────────────────────────────────────────

#[derive(Deserialize, Debug)]
struct AppConfig {
    feed:     FeedConfig,
    scan:     ScanConfig,
    #[serde(default)]
    http:     HttpConfig,
}

#[derive(Deserialize, Debug)]
struct FeedConfig {
    assets:           Vec<String>,
    timeframes:       Vec<u32>,
    clob_rest:        String,
    live_ws:          String,
    gamma_api:        String,
    #[serde(default = "default_batch_size")]
    #[allow(dead_code)]
    book_batch_size:  usize,
    #[serde(default = "default_throttle")]
    rest_throttle_ms: u64,
    #[serde(default = "default_warmup")]
    book_warmup_secs: u64,
    #[serde(default = "default_stale")]
    book_stale_secs:  f64,
}

#[derive(Deserialize, Debug)]
struct ScanConfig {
    #[serde(default = "default_tick")]
    tick_ms:            u64,
    #[serde(default = "default_sigma_window")]
    sigma_window_secs:  f64,
    #[serde(default = "default_min_secs")]
    min_secs:           f64,
    #[serde(default = "default_stake")]
    vwap_stake:         f64,
}

#[derive(Deserialize, Debug, Default)]
struct HttpConfig {
    #[serde(default = "default_port")]
    port: u16,
}

fn default_batch_size() -> usize { 20 }
fn default_throttle() -> u64 { 500 }
fn default_warmup() -> u64 { 5 }
fn default_stale() -> f64 { 3.0 }
fn default_tick() -> u64 { 500 }
fn default_sigma_window() -> f64 { 300.0 }
fn default_min_secs() -> f64 { 30.0 } // lower than v1's 60 — capture more data
fn default_stake() -> f64 { 5.0 }
fn default_port() -> u16 { 8080 }

// ── Log entry types ─────────────────────────────────────────────────────────

#[derive(Serialize)]
struct SignalLog {
    ts:           f64,
    slug:         String,
    asset:        String,
    tf:           u32,
    open_price:   f64,
    cl_price:     f64,
    pct_move:     f64,
    sigma:        f64,
    secs_left:    f64,
    // Fair values
    fair_yes:     f64,
    fair_no:      f64,
    // Book state
    bid_yes:      f64,
    ask_yes:      f64,
    bid_no:       f64,
    ask_no:       f64,
    // VWAP fills
    fill_yes:     f64,
    fill_no:      f64,
    // Edge
    edge_yes:     f64,
    edge_no:      f64,
    best_side:    String,
    best_edge:    f64,
    // Depth
    depth_yes:    f64,
    depth_no:     f64,
    // Microstructure
    cl_momentum:  f64,
    book_imbal:   f64,
    spread_yes:   f64,
    spread_no:    f64,
    // Book levels (top 5)
    asks_yes_5:   Vec<[f64; 2]>,
    bids_yes_5:   Vec<[f64; 2]>,
    asks_no_5:    Vec<[f64; 2]>,
    bids_no_5:    Vec<[f64; 2]>,
}

#[derive(Serialize)]
struct SettlementLog {
    ts:           f64,
    slug:         String,
    asset:        String,
    tf:           u32,
    open_price:   f64,
    cl_close:     f64,
    pct_move:     f64,
    outcome:      String,  // "YES" or "NO"
    window_start: u64,
    window_end:   u64,
}

#[derive(Serialize)]
struct ClTickLog {
    ts:    f64,
    asset: String,
    price: f64,
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn now_secs() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64()
}

fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn today_str() -> String {
    Utc::now().format("%Y-%m-%d").to_string()
}


fn top_n_levels(levels: &[feeds::PriceLevel], n: usize) -> Vec<[f64; 2]> {
    levels.iter().take(n).map(|l| [l.price, l.size]).collect()
}

// ── HTTP server ─────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    signal_count: Arc<AtomicU64>,
    settle_count: Arc<AtomicU64>,
    market_count: Arc<AtomicU64>,
    start_ts:     f64,
}

async fn list_files() -> impl IntoResponse {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir("data") {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.ends_with(".jsonl") {
                    files.push(serde_json::json!({
                        "name": name,
                        "size_mb": meta.len() as f64 / 1_048_576.0,
                        "modified": meta.modified().ok()
                            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                            .map(|d| d.as_secs())
                            .unwrap_or(0),
                    }));
                }
            }
        }
    }
    files.sort_by(|a, b| {
        a["name"].as_str().unwrap_or("")
            .cmp(b["name"].as_str().unwrap_or(""))
    });
    Json(serde_json::json!({ "files": files }))
}

async fn download_file(AxumPath(filename): AxumPath<String>) -> impl IntoResponse {
    // Sanitize: only allow alphanumeric, dash, underscore, dot
    let safe: String = filename.chars()
        .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_' || *c == '.')
        .collect();
    let path = format!("data/{}", safe);

    match tokio::fs::read(&path).await {
        Ok(bytes) => {
            axum::http::Response::builder()
                .header("content-type", "application/x-ndjson")
                .header("content-disposition", format!("attachment; filename=\"{}\"", safe))
                .body(axum::body::Body::from(bytes))
                .unwrap()
        }
        Err(_) => {
            axum::http::Response::builder()
                .status(404)
                .body(axum::body::Body::from("File not found"))
                .unwrap()
        }
    }
}

async fn status(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    let uptime = now_secs() - state.start_ts;
    Json(serde_json::json!({
        "status": "running",
        "uptime_secs": uptime as u64,
        "uptime_hours": format!("{:.1}", uptime / 3600.0),
        "signals_logged": state.signal_count.load(Ordering::Relaxed),
        "settlements_logged": state.settle_count.load(Ordering::Relaxed),
        "active_markets": state.market_count.load(Ordering::Relaxed),
    }))
}

// ── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("oracle_scanner_v2=info".parse()?)
        )
        .with_target(false)
        .init();

    let cfg_text = std::fs::read_to_string("config.toml")
        .context("cannot read config.toml")?;
    let cfg: AppConfig = toml::from_str(&cfg_text)
        .context("invalid config.toml")?;

    info!("═══════════════════════════════════════════════════════════");
    info!("  Oracle Scanner V2 — Data Capture Only (No Trading)");
    info!("═══════════════════════════════════════════════════════════");
    info!("Assets: {:?}", cfg.feed.assets);
    info!("Timeframes: {:?}m", cfg.feed.timeframes);
    info!("VWAP stake: ${}", cfg.scan.vwap_stake);
    info!("HTTP server: :{}", cfg.http.port);
    info!("Data dir: data/");

    // Create data directory
    std::fs::create_dir_all("data").context("cannot create data/")?;

    // ── Shared state ──────────────────────────────────────────────────────

    let cl_prices:     ClPrices     = Arc::new(DashMap::new());
    let book_state:    BookState    = Arc::new(DashMap::new());
    let price_history: PriceHistory = Arc::new(DashMap::new());
    let token_ids:     Arc<DashMap<String, ()>> = Arc::new(DashMap::new());

    let signal_count = Arc::new(AtomicU64::new(0));
    let settle_count = Arc::new(AtomicU64::new(0));
    let market_count = Arc::new(AtomicU64::new(0));

    let http = Client::builder()
        .user_agent("oracle-scanner-v2/1")
        .timeout(Duration::from_secs(10))
        .build()
        .context("HTTP client build failed")?;

    let limiter = Arc::new(RateLimiter::new(cfg.feed.rest_throttle_ms));

    // ── Start HTTP server ─────────────────────────────────────────────────

    let app_state = AppState {
        signal_count: signal_count.clone(),
        settle_count: settle_count.clone(),
        market_count: market_count.clone(),
        start_ts: now_secs(),
    };

    let app = Router::new()
        .route("/files", axum::routing::get(list_files))
        .route("/download/{filename}", axum::routing::get(download_file))
        .route("/status", axum::routing::get(status))
        .with_state(app_state);

    let port = cfg.http.port;
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
            .await
            .expect("Failed to bind HTTP server");
        info!("[HTTP] Listening on :{}", port);
        axum::serve(listener, app).await.expect("HTTP server failed");
    });

    // ── Discover markets ──────────────────────────────────────────────────

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
                        info!("[DISCOVER] {}", slug);
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
    market_count.store(markets.len() as u64, Ordering::Relaxed);

    // ── Start CL WebSocket feed ───────────────────────────────────────────

    // Also log raw CL ticks
    let cl_for_log = cl_prices.clone();
    let assets_for_log = cfg.feed.assets.clone();
    tokio::spawn(async move {
        let mut last_prices: HashMap<String, f64> = HashMap::new();
        loop {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let date = today_str();
            let path = format!("data/cl_ticks_{}.jsonl", date);

            for asset in &assets_for_log {
                if let Some(entry) = cl_for_log.get(asset.as_str()) {
                    let (ts, price) = *entry;
                    let prev = last_prices.get(asset).copied().unwrap_or(0.0);
                    if (price - prev).abs() > 0.001 {
                        let log = ClTickLog { ts, asset: asset.clone(), price };
                        if let Ok(line) = serde_json::to_string(&log) {
                            if let Ok(mut f) = std::fs::OpenOptions::new()
                                .create(true).append(true).open(&path) {
                                let _ = writeln!(f, "{}", line);
                            }
                        }
                        last_prices.insert(asset.clone(), price);
                    }
                }
            }
        }
    });

    {
        let cp = cl_prices.clone();
        let ph = price_history.clone();
        let assets = cfg.feed.assets.clone();
        let ws = cfg.feed.live_ws.clone();
        tokio::spawn(async move {
            run_cl_feed(ws, assets, cp, ph).await;
        });
    }

    // ── Main scan loop ──────────────────────────────────────────────────

    let tick = Duration::from_millis(cfg.scan.tick_ms);
    let mut tick_count:    u64 = 0;
    let mut last_stats_ts: f64 = now_secs();
    let mut last_discover: f64 = now_secs();
    let mut last_dash_ts:  f64 = now_secs();
    let mut settled: HashMap<String, bool> = HashMap::new();
    let mut cl_close_snap: HashMap<String, f64> = HashMap::new();
    let mut current_date = today_str();

    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                let mut sigterm = tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::terminate()
                ).expect("failed to register SIGTERM handler");
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => { warn!("SIGINT received — shutting down..."); }
                    _ = sigterm.recv() => { warn!("SIGTERM received — shutting down..."); }
                }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
                warn!("SIGINT received — shutting down...");
            }
            shutdown.store(true, Ordering::SeqCst);
        });
    }

    // ── CL feed warmup ──────────────────────────────────────────────────

    info!("Waiting for CL feed...");
    for _ in 0..40u32 {
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

    info!("Starting scan loop ({}ms tick)...", cfg.scan.tick_ms);

    loop {
        tokio::time::sleep(tick).await;

        if shutdown.load(Ordering::SeqCst) {
            info!("Shutdown — signals={} settlements={}",
                signal_count.load(Ordering::Relaxed),
                settle_count.load(Ordering::Relaxed));
            break;
        }

        tick_count += 1;
        let now = now_secs();
        let now_u = now as u64;

        // Date rollover check
        let date = today_str();
        if date != current_date {
            info!("[DATE] Rolled to {}", date);
            current_date = date;
        }

        // ── Periodic market rediscovery (every 60s) ─────────────────────

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
            let active = markets.iter().filter(|(s, _)| !settled.contains_key(s.as_str())).count();
            market_count.store(active as u64, Ordering::Relaxed);
        }

        // ── Batch book refresh (every other tick) ───────────────────────

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

        // ── CL close snap — first CL after window end ──────────────────

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

        // ── Settlements ────────────────────────────────────────────────

        for (slug, meta) in &markets {
            if settled.get(slug.as_str()).copied().unwrap_or(false) { continue; }
            if now_u >= meta.window_end + 5 {
                let cl_settle = match cl_close_snap.get(slug.as_str()) {
                    Some(&p) => p,
                    None => continue,
                };
                if cl_settle <= 0.0 || meta.open_price <= 0.0 { continue; }

                let outcome = if cl_settle > meta.open_price { "YES" } else { "NO" };
                let pct_move = ((cl_settle / meta.open_price) - 1.0) * 100.0;

                info!("[SETTLE] {} cl={:.2} open={:.2} d={:+.4}% → {}",
                    slug, cl_settle, meta.open_price, pct_move, outcome);

                let log = SettlementLog {
                    ts: now,
                    slug: slug.clone(),
                    asset: meta.asset.clone(),
                    tf: meta.tf,
                    open_price: meta.open_price,
                    cl_close: cl_settle,
                    pct_move,
                    outcome: outcome.to_string(),
                    window_start: meta.window_start,
                    window_end: meta.window_end,
                };

                let path = format!("data/settlements_{}.jsonl", current_date);
                if let Ok(line) = serde_json::to_string(&log) {
                    if let Ok(mut f) = open_or_create(&path) {
                        let _ = writeln!(f, "{}", line);
                    }
                }

                settle_count.fetch_add(1, Ordering::Relaxed);
                settled.insert(slug.clone(), true);
            }
        }

        // ── Warmup gate ─────────────────────────────────────────────────

        if tick_count < (cfg.feed.book_warmup_secs * 1000 / cfg.scan.tick_ms) {
            continue;
        }

        // ── Signal computation + logging (EVERY tick, EVERY market) ───

        let signal_path = format!("data/signals_{}.jsonl", current_date);

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

            // Skip stale books
            if now - book_yes_entry.ts > cfg.feed.book_stale_secs
                || now - book_no_entry.ts > cfg.feed.book_stale_secs {
                continue;
            }

            let bd_yes = BookData {
                best_ask: book_yes_entry.best_ask,
                best_bid: book_yes_entry.best_bid,
                asks: book_yes_entry.asks.clone(),
                bids: book_yes_entry.bids.clone(),
            };
            let bd_no = BookData {
                best_ask: book_no_entry.best_ask,
                best_bid: book_no_entry.best_bid,
                asks: book_no_entry.asks.clone(),
                bids: book_no_entry.bids.clone(),
            };

            let sigma = {
                let hist = price_history.get(&meta.asset);
                match hist {
                    Some(h) => estimate_sigma(&h, cfg.scan.sigma_window_secs, now),
                    None    => 0.50,
                }
            };

            let cl_momentum = {
                let hist = price_history.get(&meta.asset);
                match hist {
                    Some(h) => {
                        let cutoff = now - 30.0;
                        let old_price = h.iter()
                            .filter(|(ts, _)| *ts <= cutoff)
                            .last()
                            .map(|(_, p)| *p);
                        match old_price {
                            Some(old) if old > 0.0 => (cl - old) / old,
                            _ => 0.0,
                        }
                    }
                    None => 0.0,
                }
            };

            let sig = match compute(
                slug, &meta.asset, meta.tf,
                meta.open_price, cl, sigma, secs_left,
                &bd_yes, &bd_no, cfg.scan.vwap_stake, cl_momentum, now,
            ) {
                Some(s) => s,
                None    => continue,
            };

            let pct_move = ((cl / meta.open_price) - 1.0) * 100.0;

            // Log signal (EVERY tick)
            let log = SignalLog {
                ts: now,
                slug: slug.clone(),
                asset: meta.asset.clone(),
                tf: meta.tf,
                open_price: meta.open_price,
                cl_price: cl,
                pct_move,
                sigma,
                secs_left,
                fair_yes: sig.fair_yes,
                fair_no: sig.fair_no,
                bid_yes: sig.bid_yes,
                ask_yes: sig.book_yes,
                bid_no: sig.bid_no,
                ask_no: sig.book_no,
                fill_yes: sig.fill_yes.unwrap_or(0.0),
                fill_no: sig.fill_no.unwrap_or(0.0),
                edge_yes: sig.edge_fill_yes,
                edge_no: sig.edge_fill_no,
                best_side: sig.best_side.map(|s| s.to_string()).unwrap_or_default(),
                best_edge: sig.best_edge,
                depth_yes: sig.depth_yes,
                depth_no: sig.depth_no,
                cl_momentum,
                book_imbal: sig.book_imbal,
                spread_yes: book_yes_entry.best_ask - book_yes_entry.best_bid,
                spread_no: book_no_entry.best_ask - book_no_entry.best_bid,
                asks_yes_5: top_n_levels(&book_yes_entry.asks, 5),
                bids_yes_5: top_n_levels(&book_yes_entry.bids, 5),
                asks_no_5: top_n_levels(&book_no_entry.asks, 5),
                bids_no_5: top_n_levels(&book_no_entry.bids, 5),
            };

            if let Ok(line) = serde_json::to_string(&log) {
                if let Ok(mut f) = open_or_create(&signal_path) {
                    let _ = writeln!(f, "{}", line);
                }
            }

            signal_count.fetch_add(1, Ordering::Relaxed);
        }

        // ── Dashboard (every 60s) ────────────────────────────────────────

        if now - last_dash_ts >= 60.0 {
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

                let fy = if meta.open_price > 0.0 && cl_val > 0.0 {
                    let hist = price_history.get(&meta.asset);
                    let sigma = match hist {
                        Some(h) => estimate_sigma(&h, cfg.scan.sigma_window_secs, now),
                        None => 0.50,
                    };
                    fair_yes(cl_val, meta.open_price, sigma, secs_left)
                } else { 0.0 };

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
                    "  {} | open={} cl={} d={} | σ={} fy={:.2} | YES={} NO={} | T-{:.0}s",
                    slug, open_str, cl_str, delta_str, sigma_str, fy,
                    pm_yes_str, pm_no_str, secs_left
                ));
            }

            if !lines.is_empty() {
                lines.sort();
                info!("─── SCAN ─────────────────────────────────────────────────────");
                for line in &lines {
                    info!("{}", line);
                }
                info!("══════════════════════════════════════════════════════════════");
            }
        }

        // ── Stats (every 60s) ────────────────────────────────────────────

        if now - last_stats_ts >= 60.0 {
            last_stats_ts = now;
            let active = markets.iter().filter(|(s, _)| !settled.contains_key(s.as_str())).count();
            info!(
                "[SCANNER] signals={} settlements={} markets={} settled={} books={} | http://0.0.0.0:{}",
                signal_count.load(Ordering::Relaxed),
                settle_count.load(Ordering::Relaxed),
                active, settled.len(), book_state.len(),
                cfg.http.port
            );
        }
    }

    Ok(())
}

fn open_or_create(path: &str) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
}
