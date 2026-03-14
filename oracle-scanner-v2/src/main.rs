/// Oracle Scanner V2 -- Clean Fast Data Capture (No Trading)
///
/// Captures ALL market data with unthrottled live feeds:
///   - Every signal tick (500ms)  -> data/signals_YYYY-MM-DD.jsonl
///   - Settlement outcomes        -> data/settlements_YYYY-MM-DD.jsonl
///   - Raw CL price ticks         -> data/cl_ticks_YYYY-MM-DD.jsonl
///   - Raw BN price ticks         -> data/bn_ticks_YYYY-MM-DD.jsonl
///   - Whale/smart-money trades   -> data/whale_trades_YYYY-MM-DD.jsonl
///   - Whale wallet snapshots     -> data/whale_wallets_YYYY-MM-DD.jsonl
///
/// HTTP server on :8080 for section-wise downloads:
///   GET /files                -> list all data files
///   GET /download/{f}         -> download a single file
///   GET /sections             -> list available data sections
///   GET /section/{name}       -> download all files for a section (tar)
///   GET /status               -> scanner health + feed staleness
///
/// Feeds: CL (Chainlink RTDS WS) + BN (Binance aggTrade WS) + PM Book (REST)
/// No wallet. No execution. No throttling on WS reads.

mod feeds;
mod signal;
mod tracker;

use std::collections::{HashMap, HashSet};
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
    BnPrices, BnTradeFlow, BookState, ClPrices, MarketMeta, PriceHistory, RateLimiter,
    build_slug, current_window_starts, fetch_books_batch, fetch_market_meta,
    run_cl_feed, run_bn_feed, momentum_from_history, count_updates_in_window,
    max_gap_in_window, bn_trade_flow_stats, price_acceleration,
};
use signal::{compute, estimate_sigma, fair_yes, BookData};

// -- Config -------------------------------------------------------------------

#[derive(Deserialize, Debug)]
struct AppConfig {
    feed:     FeedConfig,
    scan:     ScanConfig,
    #[serde(default)]
    http:     HttpConfig,
    #[serde(default)]
    tracker:  TrackerConfig,
}

#[derive(Deserialize, Debug)]
struct TrackerConfig {
    #[serde(default = "default_tracker_enabled")]
    enabled:            bool,
    #[serde(default = "default_data_api")]
    data_api:           String,
    #[serde(default = "default_leaderboard_limit")]
    leaderboard_limit:  usize,
    #[serde(default = "default_min_trade_usd")]
    min_trade_usd:      f64,
    #[serde(default = "default_poll_interval")]
    poll_interval_ms:   u64,
    #[serde(default = "default_refresh_wallets")]
    refresh_wallets_s:  u64,
}

impl Default for TrackerConfig {
    fn default() -> Self {
        Self {
            enabled:           true,
            data_api:          default_data_api(),
            leaderboard_limit: default_leaderboard_limit(),
            min_trade_usd:     default_min_trade_usd(),
            poll_interval_ms:  default_poll_interval(),
            refresh_wallets_s: default_refresh_wallets(),
        }
    }
}

fn default_tracker_enabled() -> bool { true }
fn default_data_api() -> String { "https://data-api.polymarket.com".to_string() }
fn default_leaderboard_limit() -> usize { 50 }
fn default_min_trade_usd() -> f64 { 5000.0 }
fn default_poll_interval() -> u64 { 10_000 }
fn default_refresh_wallets() -> u64 { 300 }

#[derive(Deserialize, Debug)]
struct FeedConfig {
    assets:           Vec<String>,
    timeframes:       Vec<u32>,
    clob_rest:        String,
    live_ws:          String,
    gamma_api:        String,
    #[serde(default = "default_batch_size")]
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
fn default_min_secs() -> f64 { 30.0 }
fn default_stake() -> f64 { 5.0 }
fn default_port() -> u16 { 8080 }

// -- Log entry types ----------------------------------------------------------

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

    // Microstructure (original)
    cl_momentum:  f64,
    book_imbal:   f64,
    spread_yes:   f64,
    spread_no:    f64,

    // Full orderbook depth (all levels, [price, size] pairs)
    asks_yes:     Vec<[f64; 2]>,
    bids_yes:     Vec<[f64; 2]>,
    asks_no:      Vec<[f64; 2]>,
    bids_no:      Vec<[f64; 2]>,

    // --- NEW: Binance parallel capture ---
    #[serde(skip_serializing_if = "Option::is_none")]
    bn_price:         Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bn_momentum_5s:   Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bn_momentum_30s:  Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bn_momentum_60s:  Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cl_bn_spread:     Option<f64>,  // (cl - bn) / bn

    // --- NEW: Multi-timeframe CL momentum ---
    #[serde(skip_serializing_if = "Option::is_none")]
    cl_momentum_5s:   Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cl_momentum_60s:  Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cl_acceleration:  Option<f64>,  // momentum_5s - momentum_30s (positive = accelerating)

    // --- NEW: Latency / staleness ---
    #[serde(skip_serializing_if = "Option::is_none")]
    cl_feed_age_ms:   Option<f64>,  // now - cl_timestamp (ms)
    #[serde(skip_serializing_if = "Option::is_none")]
    bn_feed_age_ms:   Option<f64>,  // now - bn_timestamp (ms)
    #[serde(skip_serializing_if = "Option::is_none")]
    book_age_ms:      Option<f64>,  // now - book_timestamp (ms)
    #[serde(skip_serializing_if = "Option::is_none")]
    cl_bn_lag_ms:     Option<f64>,  // cl_timestamp - bn_timestamp (ms)

    // --- NEW: Data quality flags ---
    cl_stale:         bool,   // cl_feed_age > 10s
    bn_stale:         bool,   // bn_feed_age > 5s
    book_stale:       bool,   // book_age > 3s
    data_quality:     String, // "full", "degraded", "stale"

    // --- NEW: Vol regime ---
    #[serde(skip_serializing_if = "Option::is_none")]
    sigma_60s:        Option<f64>,  // 60s rolling sigma
    #[serde(skip_serializing_if = "Option::is_none")]
    sigma_ratio:      Option<f64>,  // sigma_60s / sigma_300s (>1 = vol expanding)

    // --- NEW: Oracle health ---
    #[serde(skip_serializing_if = "Option::is_none")]
    cl_update_count:  Option<u32>,  // CL ticks in current window
    #[serde(skip_serializing_if = "Option::is_none")]
    cl_gap_max_ms:    Option<f64>,  // max gap between CL updates in window

    // --- Arb ---
    #[serde(skip_serializing_if = "Option::is_none")]
    yes_no_arb:       Option<f64>,  // 1.0 - (best_ask_yes + best_ask_no), positive = arb exists
    #[serde(skip_serializing_if = "Option::is_none")]
    yes_no_arb_net:   Option<f64>,  // yes_no_arb minus 2x taker fee (real profitability)

    // --- NEW: Book dynamics ---
    #[serde(skip_serializing_if = "Option::is_none")]
    midpoint_yes:     Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    midpoint_no:      Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_spread_yes: Option<f64>,  // 2 * |fill - midpoint|
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_spread_no:  Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_ask_size_yes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_bid_size_yes: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_ask_size_no:  Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_bid_size_no:  Option<f64>,

    // --- Time context ---
    #[serde(skip_serializing_if = "Option::is_none")]
    hour_utc:         Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    window_elapsed_pct: Option<f64>, // how far through the window (0.0 = start, 1.0 = end)

    // --- BN microstructure (from aggTrade) ---
    #[serde(skip_serializing_if = "Option::is_none")]
    bn_trade_side:    Option<String>,  // last aggTrade: "BUY" or "SELL" (buyer was maker?)
    #[serde(skip_serializing_if = "Option::is_none")]
    bn_trade_qty:     Option<f64>,     // last aggTrade quantity
    #[serde(skip_serializing_if = "Option::is_none")]
    bn_buy_vol_5s:    Option<f64>,     // buyer-initiated volume in last 5s
    #[serde(skip_serializing_if = "Option::is_none")]
    bn_sell_vol_5s:   Option<f64>,     // seller-initiated volume in last 5s
    #[serde(skip_serializing_if = "Option::is_none")]
    bn_trade_imbal_5s: Option<f64>,    // (buy_vol - sell_vol) / total_vol, last 5s

    // --- BN-based sigma (alternative vol source for lag bot) ---
    #[serde(skip_serializing_if = "Option::is_none")]
    sigma_bn:         Option<f64>,     // 300s BN sigma (more responsive than CL)
    #[serde(skip_serializing_if = "Option::is_none")]
    sigma_bn_60s:     Option<f64>,     // 60s BN sigma
    #[serde(skip_serializing_if = "Option::is_none")]
    sigma_cl_bn_ratio: Option<f64>,    // sigma_cl / sigma_bn (>1 = CL noisier than BN)

    // --- True acceleration (2nd derivative) ---
    #[serde(skip_serializing_if = "Option::is_none")]
    cl_accel_true:    Option<f64>,     // d²price/dt² via finite differencing on CL
    #[serde(skip_serializing_if = "Option::is_none")]
    bn_accel_true:    Option<f64>,     // d²price/dt² via finite differencing on BN

    // --- Fair value dynamics ---
    #[serde(skip_serializing_if = "Option::is_none")]
    fair_yes_velocity: Option<f64>,    // delta(fair_yes) since last tick (how fast frontier moves)
    #[serde(skip_serializing_if = "Option::is_none")]
    fair_yes_prev:     Option<f64>,    // fair_yes from previous tick (for diff analysis)

    // --- PM trade flow (from live-activity, aggregated by tracker) ---
    #[serde(skip_serializing_if = "Option::is_none")]
    pm_trade_count:    Option<u32>,    // trades on this market in last poll
    #[serde(skip_serializing_if = "Option::is_none")]
    pm_buy_volume:     Option<f64>,    // USD buy volume
    #[serde(skip_serializing_if = "Option::is_none")]
    pm_sell_volume:    Option<f64>,    // USD sell volume
    #[serde(skip_serializing_if = "Option::is_none")]
    pm_trade_imbal:    Option<f64>,    // (buy-sell)/total, [-1,+1]
    #[serde(skip_serializing_if = "Option::is_none")]
    pm_avg_trade_size: Option<f64>,    // avg USD per trade
    #[serde(skip_serializing_if = "Option::is_none")]
    pm_whale_count:    Option<u32>,    // tracked wallet trades on this market
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
    outcome:      String,
    window_start: u64,
    window_end:   u64,
    // NEW: BN price at settlement for oracle integrity check
    #[serde(skip_serializing_if = "Option::is_none")]
    bn_close:     Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cl_bn_divergence: Option<f64>,  // |cl - bn| / bn at settlement
}

#[derive(Serialize)]
struct ClTickLog {
    ts:    f64,
    asset: String,
    price: f64,
}

#[derive(Serialize)]
struct BnTickLog {
    ts:    f64,
    asset: String,
    price: f64,
}

// -- Helpers ------------------------------------------------------------------

fn now_secs() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64()
}

fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn today_str() -> String {
    Utc::now().format("%Y-%m-%d").to_string()
}

fn levels_to_array(levels: &[feeds::PriceLevel]) -> Vec<[f64; 2]> {
    levels.iter().map(|l| [l.price, l.size]).collect()
}

/// Cached file handle pool — avoids reopening the same file every 500ms tick.
/// Handles are keyed by path; on date rollover the old date's handle is dropped
/// automatically when the new path is requested.
struct FileCache {
    handles: HashMap<String, std::fs::File>,
}

impl FileCache {
    fn new() -> Self { Self { handles: HashMap::new() } }

    fn get(&mut self, path: &str) -> std::io::Result<&mut std::fs::File> {
        if !self.handles.contains_key(path) {
            // Evict stale handles (old dates) when we open a new one
            // Keep at most 24 handles (6 sections x 2 dates during rollover + headroom)
            if self.handles.len() >= 24 {
                let stale: Vec<String> = self.handles.keys()
                    .filter(|k| *k != path)
                    .take(self.handles.len() - 12)
                    .cloned()
                    .collect();
                for k in stale { self.handles.remove(&k); }
            }
            let f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            self.handles.insert(path.to_string(), f);
        }
        Ok(self.handles.get_mut(path).unwrap())
    }

    /// Force close all handles (e.g. on date rollover)
    fn flush_all(&mut self) {
        self.handles.clear();
    }
}

/// Simple open-append for spawned tasks that can't share FileCache
fn open_or_create(path: &str) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
}

// -- HTTP server --------------------------------------------------------------

#[derive(Clone)]
struct AppState {
    signal_count:      Arc<AtomicU64>,
    settle_count:      Arc<AtomicU64>,
    market_count:      Arc<AtomicU64>,
    cl_tick_count:     Arc<AtomicU64>,
    bn_tick_count:     Arc<AtomicU64>,
    whale_trade_count: Arc<AtomicU64>,
    whale_wallet_count: Arc<AtomicU64>,
    start_ts:     f64,
    cl_prices:    ClPrices,
    bn_prices:    BnPrices,
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

/// List available data sections with their files
async fn list_sections() -> impl IntoResponse {
    let sections = vec!["signals", "settlements", "cl_ticks", "bn_ticks", "whale_trades", "whale_wallets"];
    let mut result: HashMap<String, Vec<serde_json::Value>> = HashMap::new();

    if let Ok(entries) = std::fs::read_dir("data") {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.ends_with(".jsonl") { continue; }
            let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
            for section in &sections {
                if name.starts_with(section) {
                    result.entry(section.to_string()).or_default().push(
                        serde_json::json!({ "name": name, "size_bytes": size })
                    );
                }
            }
        }
    }

    // Sort files within each section
    for files in result.values_mut() {
        files.sort_by(|a, b| a["name"].as_str().unwrap_or("").cmp(b["name"].as_str().unwrap_or("")));
    }

    Json(serde_json::json!({
        "sections": sections,
        "files": result,
    }))
}

/// Download all files for a section, concatenated as JSONL
async fn download_section(AxumPath(section): AxumPath<String>) -> impl IntoResponse {
    let valid = ["signals", "settlements", "cl_ticks", "bn_ticks", "whale_trades", "whale_wallets"];
    if !valid.contains(&section.as_str()) {
        return axum::http::Response::builder()
            .status(400)
            .body(axum::body::Body::from(
                format!("Invalid section '{}'. Valid: {:?}", section, valid)
            ))
            .unwrap();
    }

    let mut all_data = Vec::new();
    let mut file_names: Vec<String> = Vec::new();

    if let Ok(entries) = std::fs::read_dir("data") {
        let mut paths: Vec<_> = entries.flatten()
            .filter(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                name.starts_with(&section) && name.ends_with(".jsonl")
            })
            .collect();
        paths.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        for entry in paths {
            let name = entry.file_name().to_string_lossy().to_string();
            if let Ok(data) = std::fs::read(entry.path()) {
                all_data.extend_from_slice(&data);
                file_names.push(name);
            }
        }
    }

    let filename = format!("{}_all.jsonl", section);
    axum::http::Response::builder()
        .header("content-type", "application/x-ndjson")
        .header("content-disposition", format!("attachment; filename=\"{}\"", filename))
        .header("x-files-included", file_names.join(","))
        .header("x-total-bytes", all_data.len().to_string())
        .body(axum::body::Body::from(all_data))
        .unwrap()
}

async fn status(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    let now = now_secs();
    let uptime = now - state.start_ts;

    // Feed freshness
    let mut feed_status: HashMap<String, serde_json::Value> = HashMap::new();
    for entry in state.cl_prices.iter() {
        let (ts, price) = *entry.value();
        let age_ms = (now - ts) * 1000.0;
        feed_status.insert(format!("cl_{}", entry.key()), serde_json::json!({
            "price": price,
            "age_ms": age_ms as u64,
            "stale": age_ms > 10_000.0,
        }));
    }
    for entry in state.bn_prices.iter() {
        let (ts, price) = *entry.value();
        let age_ms = (now - ts) * 1000.0;
        feed_status.insert(format!("bn_{}", entry.key()), serde_json::json!({
            "price": price,
            "age_ms": age_ms as u64,
            "stale": age_ms > 5_000.0,
        }));
    }

    // Data files summary
    let mut total_bytes: u64 = 0;
    let mut file_count: u32 = 0;
    if let Ok(entries) = std::fs::read_dir("data") {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                if entry.file_name().to_string_lossy().ends_with(".jsonl") {
                    total_bytes += meta.len();
                    file_count += 1;
                }
            }
        }
    }

    Json(serde_json::json!({
        "status": "running",
        "uptime_secs": uptime as u64,
        "uptime_hours": format!("{:.1}", uptime / 3600.0),
        "signals_logged": state.signal_count.load(Ordering::Relaxed),
        "settlements_logged": state.settle_count.load(Ordering::Relaxed),
        "cl_ticks_logged": state.cl_tick_count.load(Ordering::Relaxed),
        "bn_ticks_logged": state.bn_tick_count.load(Ordering::Relaxed),
        "whale_trades_logged": state.whale_trade_count.load(Ordering::Relaxed),
        "whale_wallets_tracked": state.whale_wallet_count.load(Ordering::Relaxed),
        "active_markets": state.market_count.load(Ordering::Relaxed),
        "feeds": feed_status,
        "data_files": file_count,
        "data_size_mb": total_bytes as f64 / 1_048_576.0,
    }))
}

// -- Main ---------------------------------------------------------------------

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

    info!("================================================================");
    info!("  Oracle Scanner V2 -- Clean Fast Data Capture (No Trading)");
    info!("================================================================");
    info!("Assets: {:?}", cfg.feed.assets);
    info!("Timeframes: {:?}m", cfg.feed.timeframes);
    info!("Feeds: CL (RTDS WS) + BN (aggTrade WS) + Book (REST)");
    info!("Whale tracker: {} (top {}, min ${})",
        if cfg.tracker.enabled { "ON" } else { "OFF" },
        cfg.tracker.leaderboard_limit, cfg.tracker.min_trade_usd);
    info!("VWAP stake: ${}", cfg.scan.vwap_stake);
    info!("HTTP server: :{}", cfg.http.port);
    info!("Data dir: data/");

    std::fs::create_dir_all("data").context("cannot create data/")?;

    // -- Shared state ---------------------------------------------------------

    let cl_prices:     ClPrices     = Arc::new(DashMap::new());
    let bn_prices:     BnPrices     = Arc::new(DashMap::new());
    let book_state:    BookState    = Arc::new(DashMap::new());
    let price_history: PriceHistory = Arc::new(DashMap::new());
    let bn_history:    PriceHistory = Arc::new(DashMap::new());
    let bn_trades:     BnTradeFlow  = Arc::new(DashMap::new());
    let token_ids:     Arc<DashMap<String, ()>> = Arc::new(DashMap::new());

    let signal_count  = Arc::new(AtomicU64::new(0));
    let settle_count  = Arc::new(AtomicU64::new(0));
    let market_count  = Arc::new(AtomicU64::new(0));
    let cl_tick_count = Arc::new(AtomicU64::new(0));
    let bn_tick_count = Arc::new(AtomicU64::new(0));
    let whale_trade_count  = Arc::new(AtomicU64::new(0));
    let whale_wallet_count = Arc::new(AtomicU64::new(0));
    let book_fetching      = Arc::new(AtomicBool::new(false));

    let http = Client::builder()
        .user_agent("oracle-scanner-v2/2")
        .timeout(Duration::from_secs(10))
        .build()
        .context("HTTP client build failed")?;

    let limiter = Arc::new(RateLimiter::new(cfg.feed.rest_throttle_ms));

    // -- Start HTTP server ----------------------------------------------------

    let app_state = AppState {
        signal_count: signal_count.clone(),
        settle_count: settle_count.clone(),
        market_count: market_count.clone(),
        cl_tick_count: cl_tick_count.clone(),
        bn_tick_count: bn_tick_count.clone(),
        whale_trade_count: whale_trade_count.clone(),
        whale_wallet_count: whale_wallet_count.clone(),
        start_ts: now_secs(),
        cl_prices: cl_prices.clone(),
        bn_prices: bn_prices.clone(),
    };

    let app = Router::new()
        .route("/files", axum::routing::get(list_files))
        .route("/download/{filename}", axum::routing::get(download_file))
        .route("/sections", axum::routing::get(list_sections))
        .route("/section/{name}", axum::routing::get(download_section))
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

    // -- Start CL tick logger -------------------------------------------------

    {
        let cl_for_log = cl_prices.clone();
        let assets_for_log = cfg.feed.assets.clone();
        let counter = cl_tick_count.clone();
        tokio::spawn(async move {
            let mut last_prices: HashMap<String, f64> = HashMap::new();
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let date = today_str();
                let path = format!("data/cl_ticks_{}.jsonl", date);

                for asset in &assets_for_log {
                    if let Some(entry) = cl_for_log.get(asset.as_str()) {
                        let (ts, price) = *entry;
                        let prev = last_prices.get(asset).copied().unwrap_or(0.0);
                        // Relative threshold: log if price changed by >0.0001% (1 bp)
                        if prev <= 0.0 || ((price - prev) / prev).abs() > 0.000001 {
                            let log = ClTickLog { ts, asset: asset.clone(), price };
                            if let Ok(line) = serde_json::to_string(&log) {
                                if let Ok(mut f) = open_or_create(&path) {
                                    let _ = writeln!(f, "{}", line);
                                }
                            }
                            last_prices.insert(asset.clone(), price);
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        });
    }

    // -- Start BN tick logger -------------------------------------------------

    {
        let bn_for_log = bn_prices.clone();
        let assets_for_log = cfg.feed.assets.clone();
        let counter = bn_tick_count.clone();
        tokio::spawn(async move {
            let mut last_prices: HashMap<String, f64> = HashMap::new();
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let date = today_str();
                let path = format!("data/bn_ticks_{}.jsonl", date);

                for asset in &assets_for_log {
                    if let Some(entry) = bn_for_log.get(asset.as_str()) {
                        let (ts, price) = *entry;
                        let prev = last_prices.get(asset).copied().unwrap_or(0.0);
                        // Log every meaningful change (>0.01 for BTC, scales with price)
                        if prev > 0.0 && ((price - prev) / prev).abs() < 0.000001 {
                            continue;
                        }
                        let log = BnTickLog { ts, asset: asset.clone(), price };
                        if let Ok(line) = serde_json::to_string(&log) {
                            if let Ok(mut f) = open_or_create(&path) {
                                let _ = writeln!(f, "{}", line);
                            }
                        }
                        last_prices.insert(asset.clone(), price);
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    // -- Start CL WebSocket feed ----------------------------------------------

    {
        let cp = cl_prices.clone();
        let ph = price_history.clone();
        let assets = cfg.feed.assets.clone();
        let ws = cfg.feed.live_ws.clone();
        tokio::spawn(async move {
            run_cl_feed(ws, assets, cp, ph).await;
        });
    }

    // -- Start BN WebSocket feed (unthrottled aggTrade) -----------------------

    {
        let bp = bn_prices.clone();
        let bh = bn_history.clone();
        let bt = bn_trades.clone();
        let assets = cfg.feed.assets.clone();
        tokio::spawn(async move {
            run_bn_feed(assets, bp, bh, bt).await;
        });
    }

    // -- Start whale tracker --------------------------------------------------

    let active_markets_for_tracker: Arc<tokio::sync::RwLock<Vec<tracker::ActiveMarketInfo>>>
        = Arc::new(tokio::sync::RwLock::new(Vec::new()));
    let market_flow: tracker::MarketFlowState = Arc::new(DashMap::new());

    if cfg.tracker.enabled {
        let tracker_cfg = tracker::TrackerConfig {
            data_api:          cfg.tracker.data_api.clone(),
            clob_rest:         cfg.feed.clob_rest.clone(),
            leaderboard_limit: cfg.tracker.leaderboard_limit,
            min_trade_usd:     cfg.tracker.min_trade_usd,
            poll_interval_ms:  cfg.tracker.poll_interval_ms,
            refresh_wallets_s: cfg.tracker.refresh_wallets_s,
        };
        let tracker_client = http.clone();
        let tc = whale_trade_count.clone();
        let wc = whale_wallet_count.clone();
        let am = active_markets_for_tracker.clone();
        let mf = market_flow.clone();
        tokio::spawn(async move {
            tracker::run_whale_tracker(tracker_cfg, tracker_client, tc, wc, am, mf).await;
        });
        info!("[WHALE] Tracker enabled (top {}, min ${}, poll {}ms)",
            cfg.tracker.leaderboard_limit, cfg.tracker.min_trade_usd, cfg.tracker.poll_interval_ms);
    } else {
        info!("[WHALE] Tracker disabled");
    }

    // -- Main scan loop -------------------------------------------------------

    let tick = Duration::from_millis(cfg.scan.tick_ms);
    let mut tick_count:    u64 = 0;
    let mut last_stats_ts: f64 = now_secs();
    let mut last_discover: f64 = now_secs();
    let mut last_dash_ts:  f64 = now_secs();
    let mut settled: HashSet<String> = HashSet::new();
    let mut cl_close_snap: HashMap<String, f64> = HashMap::new();
    let mut current_date = today_str();
    let mut file_cache = FileCache::new();
    let mut prev_fair_yes: HashMap<String, f64> = HashMap::new(); // slug -> last fair_yes

    // Polymarket taker fee: ~2% on net winnings. For arb: you pay fee on both legs.
    const TAKER_FEE_BPS: f64 = 0.02;

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
                    _ = tokio::signal::ctrl_c() => { warn!("SIGINT received -- shutting down..."); }
                    _ = sigterm.recv() => { warn!("SIGTERM received -- shutting down..."); }
                }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
                warn!("SIGINT received -- shutting down...");
            }
            shutdown.store(true, Ordering::SeqCst);
        });
    }

    // -- Feed warmup ----------------------------------------------------------

    info!("Waiting for CL + BN feeds...");
    for _ in 0..40u32 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let cl_ready = cfg.feed.assets.iter().all(|a| cl_prices.contains_key(a.as_str()));
        let bn_ready = cfg.feed.assets.iter().all(|a| bn_prices.contains_key(a.as_str()));
        if cl_ready && bn_ready {
            for asset in &cfg.feed.assets {
                let cl_val = cl_prices.get(asset.as_str()).map(|v| v.1).unwrap_or(0.0);
                let bn_val = bn_prices.get(asset.as_str()).map(|v| v.1).unwrap_or(0.0);
                info!("[FEED] {} CL={:.2} BN={:.2} spread={:+.4}%",
                    asset.to_uppercase(), cl_val, bn_val,
                    if bn_val > 0.0 { (cl_val - bn_val) / bn_val * 100.0 } else { 0.0 }
                );
            }
            break;
        }
        if cl_ready && !bn_ready {
            // Don't block on BN -- it's supplementary
            info!("[FEED] CL ready, BN still connecting (will continue without blocking)");
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
            info!("Shutdown -- signals={} settlements={} cl={} bn={} whales={}",
                signal_count.load(Ordering::Relaxed),
                settle_count.load(Ordering::Relaxed),
                cl_tick_count.load(Ordering::Relaxed),
                bn_tick_count.load(Ordering::Relaxed),
                whale_trade_count.load(Ordering::Relaxed));
            break;
        }

        tick_count += 1;
        let now = now_secs();
        let now_u = now as u64;

        // Date rollover
        let date = today_str();
        if date != current_date {
            info!("[DATE] Rolled to {}", date);
            current_date = date;
            file_cache.flush_all();
        }

        // -- Periodic market rediscovery (every 60s) --------------------------

        if now - last_discover > 60.0 {
            last_discover = now;

            // Prune expired windows: remove markets settled >120s ago
            let expired: Vec<String> = markets.iter()
                .filter(|(_, m)| now_u > m.window_end + 120)
                .map(|(s, _)| s.clone())
                .collect();
            for slug in &expired {
                if let Some(meta) = markets.remove(slug) {
                    token_ids.remove(&meta.token_yes);
                    token_ids.remove(&meta.token_no);
                }
                settled.remove(slug);
                cl_close_snap.remove(slug);
            }
            if !expired.is_empty() {
                info!("[PRUNE] Removed {} expired markets", expired.len());
            }

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
            let active = markets.iter().filter(|(s, _)| !settled.contains(s.as_str())).count();
            market_count.store(active as u64, Ordering::Relaxed);

            // Update active markets for whale tracker
            if cfg.tracker.enabled {
                let active_list: Vec<tracker::ActiveMarketInfo> = markets.iter()
                    .filter(|(s, _)| !settled.contains(s.as_str()))
                    .map(|(_, m)| tracker::ActiveMarketInfo {
                        slug: m.slug.clone(),
                        asset: m.asset.clone(),
                        tf: m.tf,
                        condition_id: m.condition_id.clone(),
                        token_yes: m.token_yes.clone(),
                        token_no: m.token_no.clone(),
                    })
                    .collect();
                *active_markets_for_tracker.write().await = active_list;
            }
        }

        // -- Batch book refresh (every other tick, skip if previous still running)

        if tick_count % 2 == 0 && !book_fetching.load(Ordering::Relaxed) {
            let all_tokens: Vec<String> = token_ids.iter().map(|e| e.key().clone()).collect();
            if !all_tokens.is_empty() {
                let http2 = http.clone();
                let clob2 = cfg.feed.clob_rest.clone();
                let lim2 = limiter.clone();
                let bs2 = book_state.clone();
                let flag2 = book_fetching.clone();
                let batch_sz = cfg.feed.book_batch_size;
                flag2.store(true, Ordering::Relaxed);
                tokio::spawn(async move {
                    match fetch_books_batch(&http2, &clob2, &all_tokens, &lim2, batch_sz).await {
                        Ok(books) => {
                            for (tid, entry) in books {
                                bs2.insert(tid, entry);
                            }
                        }
                        Err(e) => warn!("[BOOK] batch failed: {}", e),
                    }
                    flag2.store(false, Ordering::Relaxed);
                });
            }
        }

        // -- CL close snap ----------------------------------------------------

        for (slug, meta) in &markets {
            if settled.contains(slug.as_str()) { continue; }
            if cl_close_snap.contains_key(slug.as_str()) { continue; }
            if now_u > meta.window_end {
                let cl_now = cl_prices.get(&meta.asset).map(|v| v.1).unwrap_or(0.0);
                if cl_now > 0.0 {
                    cl_close_snap.insert(slug.clone(), cl_now);
                    info!("[CL_CLOSE] {} snap={:.2}", slug, cl_now);
                }
            }
        }

        // -- Settlements ------------------------------------------------------

        for (slug, meta) in &markets {
            if settled.contains(slug.as_str()) { continue; }
            if now_u >= meta.window_end + 5 {
                let cl_settle = match cl_close_snap.get(slug.as_str()) {
                    Some(&p) => p,
                    None => continue,
                };
                if cl_settle <= 0.0 || meta.open_price <= 0.0 { continue; }

                let outcome = if cl_settle > meta.open_price { "YES" } else { "NO" };
                let pct_move = ((cl_settle / meta.open_price) - 1.0) * 100.0;

                // BN price at settlement for oracle integrity
                let bn_close = bn_prices.get(&meta.asset).map(|v| v.1);
                let cl_bn_divergence = bn_close.map(|bn| {
                    if bn > 0.0 { ((cl_settle - bn) / bn).abs() } else { 0.0 }
                });

                info!("[SETTLE] {} cl={:.2} bn={:.2} open={:.2} d={:+.4}% -> {}",
                    slug, cl_settle, bn_close.unwrap_or(0.0), meta.open_price, pct_move, outcome);

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
                    bn_close,
                    cl_bn_divergence,
                };

                let path = format!("data/settlements_{}.jsonl", current_date);
                if let Ok(line) = serde_json::to_string(&log) {
                    if let Ok(f) = file_cache.get(&path) {
                        let _ = writeln!(f, "{}", line);
                    }
                }

                settle_count.fetch_add(1, Ordering::Relaxed);
                settled.insert(slug.clone());
            }
        }

        // -- Warmup gate ------------------------------------------------------

        if tick_count < (cfg.feed.book_warmup_secs * 1000 / cfg.scan.tick_ms) {
            continue;
        }

        // -- Signal computation + logging (EVERY tick, EVERY market) ----------

        let signal_path = format!("data/signals_{}.jsonl", current_date);

        for (slug, meta) in &mut markets {
            if settled.contains(slug.as_str()) { continue; }

            let secs_left = meta.window_end as f64 - now;
            if secs_left < cfg.scan.min_secs { continue; }

            // CL price + timestamp
            let (cl_ts, cl) = match cl_prices.get(&meta.asset) {
                Some(v) => *v.value(),
                None    => continue,
            };

            // Set open price at window start.
            // Only set if we're within the grace period of window start — if we
            // joined late (crash recovery), the CL price has drifted and using
            // it as "open" corrupts fair value. Skip the market instead.
            // Grace period scales with timeframe: 5s for 5m, 30s for 60m, 120s for 240m.
            if meta.open_price <= 0.0 {
                let grace_secs = (meta.tf as u64).max(5).min(120);
                if now_u >= meta.window_start {
                    let window_age = now_u - meta.window_start;
                    if window_age <= grace_secs {
                        meta.open_price = cl;
                        info!("[OPEN] {} open_price={:.2}", slug, cl);
                    } else {
                        // Joined too late — mark as settled so we don't keep retrying
                        warn!("[OPEN] {} skipped: joined {}s late (grace={}s), CL has drifted",
                            slug, window_age, grace_secs);
                        settled.insert(slug.clone());
                    }
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

            // Book staleness (but DON'T skip -- log with flag)
            let book_age_s = (now - book_yes_entry.ts).max(now - book_no_entry.ts);
            let book_is_stale = book_age_s > cfg.feed.book_stale_secs;

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

            // -- Fetch histories once, reuse for all derived metrics --
            let cl_hist_ref = price_history.get(&meta.asset);
            let bn_hist_key = format!("bn_{}", meta.asset);
            let bn_hist_ref = bn_history.get(&bn_hist_key);

            // Sigma (300s)
            let sigma = cl_hist_ref.as_ref()
                .map(|h| estimate_sigma(h.value(), cfg.scan.sigma_window_secs, now))
                .unwrap_or(0.50);

            // Sigma (60s) for vol regime
            let sigma_60s = cl_hist_ref.as_ref()
                .map(|h| estimate_sigma(h.value(), 60.0, now));
            let sigma_ratio = sigma_60s.map(|s60| if sigma > 0.0 { s60 / sigma } else { 1.0 });

            // CL momentum (30s -- original)
            let cl_momentum = cl_hist_ref.as_ref()
                .map(|h| momentum_from_history(h.value(), now, 30.0).0)
                .unwrap_or(0.0);

            // Multi-timeframe CL momentum
            let cl_momentum_5s = cl_hist_ref.as_ref()
                .map(|h| momentum_from_history(h.value(), now, 5.0).0);
            let cl_momentum_60s = cl_hist_ref.as_ref()
                .map(|h| momentum_from_history(h.value(), now, 60.0).0);
            let cl_acceleration = match (cl_momentum_5s, Some(cl_momentum)) {
                (Some(m5), Some(m30)) => Some(m5 - m30),
                _ => None,
            };

            // Oracle health
            let cl_update_count = cl_hist_ref.as_ref()
                .map(|h| count_updates_in_window(h.value(), meta.window_start as f64, now));
            let cl_gap_max_ms = cl_hist_ref.as_ref()
                .map(|h| max_gap_in_window(h.value(), meta.window_start as f64, now) * 1000.0);

            // True acceleration (d²price/dt², 10s finite differencing)
            let cl_accel_true = cl_hist_ref.as_ref()
                .and_then(|h| price_acceleration(h.value(), now, 10.0));

            // Drop CL history lock
            drop(cl_hist_ref);

            // BN price + staleness
            let (bn_ts, bn_price) = bn_prices.get(&meta.asset)
                .map(|v| *v.value())
                .unwrap_or((0.0, 0.0));
            let bn_available = bn_price > 0.0;

            let (bn_momentum_5s, bn_momentum_30s, bn_momentum_60s) = if bn_available {
                let m5  = bn_hist_ref.as_ref().map(|h| momentum_from_history(h.value(), now, 5.0).0);
                let m30 = bn_hist_ref.as_ref().map(|h| momentum_from_history(h.value(), now, 30.0).0);
                let m60 = bn_hist_ref.as_ref().map(|h| momentum_from_history(h.value(), now, 60.0).0);
                (m5, m30, m60)
            } else { (None, None, None) };

            // BN sigma (300s + 60s) — alternative vol source for lag bot
            let sigma_bn = bn_hist_ref.as_ref()
                .map(|h| estimate_sigma(h.value(), cfg.scan.sigma_window_secs, now));
            let sigma_bn_60s = bn_hist_ref.as_ref()
                .map(|h| estimate_sigma(h.value(), 60.0, now));
            let sigma_cl_bn_ratio = sigma_bn.map(|sbn| if sbn > 0.0 { sigma / sbn } else { 1.0 });

            // BN true acceleration
            let bn_accel_true = bn_hist_ref.as_ref()
                .and_then(|h| price_acceleration(h.value(), now, 10.0));

            // Drop BN history lock
            drop(bn_hist_ref);

            // BN trade flow (side + volume imbalance in last 5s)
            let (bn_buy_vol_5s, bn_sell_vol_5s, bn_trade_imbal_5s, bn_last_side, bn_last_qty) = {
                let trades_ref = bn_trades.get(&meta.asset);
                match trades_ref {
                    Some(trades) => bn_trade_flow_stats(&trades, now, 5.0),
                    None => (0.0, 0.0, 0.0, None, None),
                }
            };
            let bn_trade_side = bn_last_side.map(|s| if s { "SELL" } else { "BUY" }.to_string());

            let cl_bn_spread = if bn_available && bn_price > 0.0 {
                Some((cl - bn_price) / bn_price)
            } else { None };

            // Latency
            let cl_feed_age_ms = Some((now - cl_ts) * 1000.0);
            let bn_feed_age_ms = if bn_available { Some((now - bn_ts) * 1000.0) } else { None };
            let book_age_ms = Some(book_age_s * 1000.0);
            let cl_bn_lag_ms = if bn_available { Some((cl_ts - bn_ts) * 1000.0) } else { None };

            // Staleness flags
            let cl_is_stale = (now - cl_ts) > 10.0;
            let bn_is_stale = !bn_available || (now - bn_ts) > 5.0;

            let data_quality = if cl_is_stale || book_is_stale {
                "stale"
            } else if bn_is_stale {
                "degraded"
            } else {
                "full"
            }.to_string();

            // Arb: YES+NO completeness
            let yes_no_arb = Some(1.0 - (book_yes_entry.best_ask + book_no_entry.best_ask));

            // Midpoints
            let mid_yes = if book_yes_entry.best_bid > 0.0 && book_yes_entry.best_ask > 0.0 {
                Some((book_yes_entry.best_bid + book_yes_entry.best_ask) / 2.0)
            } else { None };
            let mid_no = if book_no_entry.best_bid > 0.0 && book_no_entry.best_ask > 0.0 {
                Some((book_no_entry.best_bid + book_no_entry.best_ask) / 2.0)
            } else { None };

            // Signal compute
            let sig = match compute(
                slug, &meta.asset, meta.tf,
                meta.open_price, cl, sigma, secs_left,
                &bd_yes, &bd_no, cfg.scan.vwap_stake, cl_momentum, now,
            ) {
                Some(s) => s,
                None    => continue,
            };

            let pct_move = ((cl / meta.open_price) - 1.0) * 100.0;

            // Effective spread
            let effective_spread_yes = match (sig.fill_yes, mid_yes) {
                (Some(fill), Some(mid)) => Some(2.0 * (fill - mid).abs()),
                _ => None,
            };
            let effective_spread_no = match (sig.fill_no, mid_no) {
                (Some(fill), Some(mid)) => Some(2.0 * (fill - mid).abs()),
                _ => None,
            };

            // Window progress
            let window_duration = (meta.tf as f64) * 60.0;
            let elapsed = now - meta.window_start as f64;
            let window_elapsed_pct = Some((elapsed / window_duration).clamp(0.0, 1.0));

            let hour_utc = Some(Utc::now().format("%H").to_string().parse::<u32>().unwrap_or(0));

            // Fair value velocity: how fast the BS frontier is moving
            let prev_fy = prev_fair_yes.get(slug.as_str()).copied();
            let fair_yes_velocity = prev_fy.map(|pfy| sig.fair_yes - pfy);
            prev_fair_yes.insert(slug.clone(), sig.fair_yes);

            // Fee-adjusted arb
            let yes_no_arb_net = yes_no_arb.map(|arb| arb - 2.0 * TAKER_FEE_BPS);

            // Cache market flow lookup (1 DashMap hit instead of 6)
            let mf_snap = market_flow.get(&meta.condition_id).map(|f| f.clone());

            // Log signal
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
                // Full orderbook depth (all levels)
                asks_yes: levels_to_array(&book_yes_entry.asks),
                bids_yes: levels_to_array(&book_yes_entry.bids),
                asks_no: levels_to_array(&book_no_entry.asks),
                bids_no: levels_to_array(&book_no_entry.bids),
                // BN + CL cross-feed
                bn_price: if bn_available { Some(bn_price) } else { None },
                bn_momentum_5s,
                bn_momentum_30s,
                bn_momentum_60s,
                cl_bn_spread,
                cl_momentum_5s,
                cl_momentum_60s,
                cl_acceleration,
                cl_feed_age_ms,
                bn_feed_age_ms,
                book_age_ms,
                cl_bn_lag_ms,
                cl_stale: cl_is_stale,
                bn_stale: bn_is_stale,
                book_stale: book_is_stale,
                data_quality,
                sigma_60s,
                sigma_ratio,
                cl_update_count,
                cl_gap_max_ms,
                yes_no_arb,
                yes_no_arb_net,
                midpoint_yes: mid_yes,
                midpoint_no: mid_no,
                effective_spread_yes,
                effective_spread_no,
                top_ask_size_yes: book_yes_entry.asks.first().map(|l| l.size),
                top_bid_size_yes: book_yes_entry.bids.first().map(|l| l.size),
                top_ask_size_no: book_no_entry.asks.first().map(|l| l.size),
                top_bid_size_no: book_no_entry.bids.first().map(|l| l.size),
                hour_utc,
                window_elapsed_pct,
                // BN microstructure
                bn_trade_side,
                bn_trade_qty: bn_last_qty,
                bn_buy_vol_5s:    if bn_available { Some(bn_buy_vol_5s) } else { None },
                bn_sell_vol_5s:   if bn_available { Some(bn_sell_vol_5s) } else { None },
                bn_trade_imbal_5s: if bn_available { Some(bn_trade_imbal_5s) } else { None },
                // BN sigma
                sigma_bn,
                sigma_bn_60s,
                sigma_cl_bn_ratio,
                // True acceleration
                cl_accel_true,
                bn_accel_true,
                // Fair value dynamics
                fair_yes_velocity,
                fair_yes_prev: prev_fy,
                // PM trade flow (single lookup, cached)
                pm_trade_count:    mf_snap.as_ref().map(|f| f.trade_count_5m),
                pm_buy_volume:     mf_snap.as_ref().map(|f| f.buy_volume_5m),
                pm_sell_volume:    mf_snap.as_ref().map(|f| f.sell_volume_5m),
                pm_trade_imbal:    mf_snap.as_ref().map(|f| f.imbalance_5m),
                pm_avg_trade_size: mf_snap.as_ref().map(|f| f.avg_trade_size),
                pm_whale_count:    mf_snap.as_ref().map(|f| f.whale_trade_count),
            };

            if let Ok(line) = serde_json::to_string(&log) {
                if let Ok(f) = file_cache.get(&signal_path) {
                    let _ = writeln!(f, "{}", line);
                }
            }

            signal_count.fetch_add(1, Ordering::Relaxed);
        }

        // -- Dashboard (every 60s) --------------------------------------------

        if now - last_dash_ts >= 60.0 {
            last_dash_ts = now;
            let mut lines: Vec<String> = Vec::new();

            for (slug, meta) in &markets {
                if settled.contains(slug.as_str()) { continue; }
                let secs_left = meta.window_end as f64 - now;
                if secs_left < 0.0 { continue; }

                let open_str = if meta.open_price > 0.0 {
                    format!("{:.2}", meta.open_price)
                } else { "WAIT".to_string() };

                let (cl_str, cl_val) = match cl_prices.get(&meta.asset) {
                    Some(v) => (format!("{:.2}", v.1), v.1),
                    None    => ("--".to_string(), 0.0),
                };

                let bn_str = match bn_prices.get(&meta.asset) {
                    Some(v) => format!("{:.2}", v.1),
                    None    => "--".to_string(),
                };

                let delta_str = if meta.open_price > 0.0 && cl_val > 0.0 {
                    format!("{:+.4}%", (cl_val - meta.open_price) / meta.open_price * 100.0)
                } else { "--".to_string() };

                // Fetch history once for both sigma and fair_yes
                let hist_ref = price_history.get(&meta.asset);
                let sigma = hist_ref.as_ref()
                    .map(|h| estimate_sigma(h.value(), cfg.scan.sigma_window_secs, now))
                    .unwrap_or(0.50);
                drop(hist_ref);

                let sigma_str = format!("{:.1}%", sigma * 100.0);

                let fy = if meta.open_price > 0.0 && cl_val > 0.0 {
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
                    "  {} | open={} cl={} bn={} d={} | s={} fy={:.2} | YES={} NO={} | T-{:.0}s",
                    slug, open_str, cl_str, bn_str, delta_str, sigma_str, fy,
                    pm_yes_str, pm_no_str, secs_left
                ));
            }

            if !lines.is_empty() {
                lines.sort();
                info!("--- SCAN ----------------------------------------------------------");
                for line in &lines {
                    info!("{}", line);
                }
                info!("===================================================================");
            }
        }

        // -- Stats (every 60s) ------------------------------------------------

        if now - last_stats_ts >= 60.0 {
            last_stats_ts = now;
            let active = markets.iter().filter(|(s, _)| !settled.contains(s.as_str())).count();
            info!(
                "[SCANNER] signals={} settles={} cl={} bn={} whales={} markets={} books={} | http://0.0.0.0:{}",
                signal_count.load(Ordering::Relaxed),
                settle_count.load(Ordering::Relaxed),
                cl_tick_count.load(Ordering::Relaxed),
                bn_tick_count.load(Ordering::Relaxed),
                whale_trade_count.load(Ordering::Relaxed),
                active, book_state.len(),
                cfg.http.port
            );
        }
    }

    Ok(())
}
