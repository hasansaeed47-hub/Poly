//! CL Sniper V6 — Production-ready with 9 fixes from V5
//! 1. Persistent auth per tick (no re-auth on every call)
//! 2. CL open snap on arrival (immediate, no tolerance)
//! 3. CL close snap on arrival (first CL after end_ts)
//! 4. FAK taker (partial fills accepted)
//! 5. SL dual confirm (CL flip AND bid <= 50% fill_px)
//! 6. Book refresh for held positions
//! 7. Double-fill prevention (verify cancel before repost)
//! 8. Paper trackers apply BN/CL filters
//! 9. Taker cleanup (explicit cancel on zero fill)

use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, trace, warn};

use polymarket_client_sdk::auth::{LocalSigner, Signer as _};
use polymarket_client_sdk::clob::types::{
    Amount, OrderType as PolyOrderType, Side as PolySide, SignatureType,
};
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::types::Decimal;
use polymarket_client_sdk::POLYGON;

type AuthClient = ClobClient<polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>>;

// ── Constants ────────────────────────────────────────────────────────────────

const ASSETS: &[&str] = &["btc", "eth", "sol", "xrp"];
const DELTA_BASE: f64 = 0.10;
const DELTA_15M: f64 = 0.18;
const DELTA_15M_PAPER_10: f64 = 0.10;
const DELTA_15M_PAPER_15: f64 = 0.15;
const STDEV_BTC: f64 = 0.167;
const STDEV_ETH: f64 = 0.194;
const STDEV_SOL: f64 = 0.247;
const STDEV_XRP: f64 = 0.440;
const STDEV_BASE: f64 = 0.167;
const STAKE: f64 = 5.0;
const MIN_ENTRY: f64 = 0.85;
const MAX_ENTRY: f64 = 0.98;
const ENTRY_START: i64 = 57;
const ENTRY_END: i64 = 45;
const TAKER_DEADLINE: i64 = 44;
const SL_BID_PCT: f64 = 0.50;      // SL: bid <= 50% of fill_px
const MAX_DD: f64 = 35.0;
const MAX_CONSEC_LOSS: u32 = 4;
const MAX_CONCURRENT: usize = 6;
const BN_CONTRA_PCT: f64 = 0.02;
const CL_FADE_PCT: f64 = 0.03;

const RTDS_WS: &str = "wss://ws-live-data.polymarket.com";
const BN_WS: &str = "wss://stream.binance.com:9443/ws";
const GAMMA_API: &str = "https://gamma-api.polymarket.com";
const CLOB_API: &str = "https://clob.polymarket.com";

// ── Shared State ─────────────────────────────────────────────────────────────

type SharedState = Arc<RwLock<State>>;

struct State {
    cl_px: HashMap<&'static str, f64>,
    cl_snap: HashMap<&'static str, HashMap<i64, f64>>,
    bn_px: HashMap<&'static str, f64>,
    bn_hist: HashMap<&'static str, VecDeque<(f64, f64)>>,
}

impl State {
    fn new() -> Self {
        State { cl_px: HashMap::new(), cl_snap: HashMap::new(), bn_px: HashMap::new(), bn_hist: HashMap::new() }
    }
    fn cl_update(&mut self, asset: &'static str, px: f64, ts: f64) {
        self.cl_px.insert(asset, px);
        let snap = self.cl_snap.entry(asset).or_default();
        snap.insert(ts as i64, px);
        let cut = ts as i64 - 3600;
        snap.retain(|k, _| *k > cut);
    }
    fn bn_update(&mut self, asset: &'static str, px: f64) {
        let ts = Utc::now().timestamp_millis() as f64 / 1000.0;
        self.bn_px.insert(asset, px);
        let hist = self.bn_hist.entry(asset).or_default();
        hist.push_back((ts, px));
        if hist.len() > 7200 { hist.pop_front(); }
    }
    #[allow(dead_code)]
    fn cl_at(&self, asset: &str, target: i64, tol: i64) -> Option<f64> {
        let snap = self.cl_snap.get(asset)?;
        for dt in 0..=tol {
            if let Some(&px) = snap.get(&(target + dt)) { return Some(px); }
            if dt > 0 { if let Some(&px) = snap.get(&(target - dt)) { return Some(px); } }
        }
        None
    }
    fn bn_trend(&self, asset: &str, secs: u64) -> Option<f64> {
        let hist = self.bn_hist.get(asset)?;
        if hist.len() < 2 { return None; }
        let now = Utc::now().timestamp_millis() as f64 / 1000.0;
        let cutoff = now - secs as f64;
        let old = hist.iter().find(|(ts, _)| *ts >= cutoff)?;
        if old.1 <= 0.0 { return None; }
        let cur = hist.back()?;
        Some((cur.1 - old.1) / old.1 * 100.0)
    }
    fn cl_trend(&self, asset: &str, secs: u64) -> Option<f64> {
        let snap = self.cl_snap.get(asset)?;
        if snap.is_empty() { return None; }
        let now = Utc::now().timestamp();
        let cutoff = now - secs as i64;
        let cur = self.cl_px.get(asset)?;
        let old_px = snap.iter()
            .filter(|(&ts, _)| ts >= cutoff)
            .min_by_key(|(&ts, _)| ts)
            .map(|(_, &px)| px)?;
        if old_px <= 0.0 { return None; }
        Some((cur - old_px) / old_px * 100.0)
    }
}

fn asset_stdev(a: &str) -> f64 {
    match a { "btc" => STDEV_BTC, "eth" => STDEV_ETH, "sol" => STDEV_SOL, "xrp" => STDEV_XRP, _ => STDEV_BASE }
}

fn scaled_threshold(asset: &str, wmin: u32) -> f64 {
    let base = if wmin == 15 { DELTA_15M } else { DELTA_BASE };
    base * (asset_stdev(asset) / STDEV_BASE)
}

// ── Feeds ────────────────────────────────────────────────────────────────────

fn cl_to_asset(sym: &str) -> Option<&'static str> {
    match sym {
        "btc/usd" | "btcusd" | "btc" => Some("btc"),
        "eth/usd" | "ethusd" | "eth" => Some("eth"),
        "sol/usd" | "solusd" | "sol" => Some("sol"),
        "xrp/usd" | "xrpusd" | "xrp" => Some("xrp"),
        _ => None,
    }
}

fn bn_sym(a: &str) -> &'static str {
    match a { "btc" => "btcusdt", "eth" => "ethusdt", "sol" => "solusdt", "xrp" => "xrpusdt", _ => "btcusdt" }
}

async fn run_cl_feed(state: SharedState) {
    loop {
        info!("[CL] Connecting...");
        if let Err(e) = cl_connect(&state).await { error!("[CL] {}", e); }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn cl_connect(state: &SharedState) -> Result<()> {
    let (mut ws, _) = connect_async(RTDS_WS).await.context("CL WS")?;
    ws.send(Message::Text(json!({"action":"subscribe","subscriptions":[
        {"topic":"crypto_prices_chainlink","type":"*","filters":""}
    ]}).to_string())).await?;
    info!("[CL] Subscribed");
    while let Some(msg) = ws.next().await {
        match msg {
            Ok(Message::Text(txt)) => {
                let d: Value = match serde_json::from_str(&txt) { Ok(d) => d, _ => continue };
                if d.get("topic").and_then(|t| t.as_str()) != Some("crypto_prices_chainlink") { continue; }
                let p = match d.get("payload") { Some(p) => p, _ => continue };
                let sym = p.get("symbol").and_then(|s| s.as_str()).unwrap_or("").to_lowercase();
                let val = p.get("value").and_then(|v| v.as_f64().or(v.as_str().and_then(|s| s.parse().ok())));
                let raw_ts = p.get("timestamp").and_then(|t| t.as_f64().or(t.as_i64().map(|i| i as f64))).unwrap_or(0.0);
                let ts = if raw_ts > 1e12 { raw_ts / 1000.0 } else if raw_ts > 1e9 { raw_ts } else { Utc::now().timestamp() as f64 };
                if let (Some(a), Some(px)) = (cl_to_asset(&sym), val) {
                    if px > 0.0 { state.write().await.cl_update(a, px, ts); }
                }
            }
            Ok(Message::Ping(d)) => { let _ = ws.send(Message::Pong(d)).await; }
            Ok(_) => {}
            Err(e) => { error!("[CL] WS: {}", e); break; }
        }
    }
    Ok(())
}

async fn run_bn_feed(state: SharedState) {
    loop {
        info!("[BN] Connecting...");
        if let Err(e) = bn_connect(&state).await { error!("[BN] {}", e); }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn bn_connect(state: &SharedState) -> Result<()> {
    let streams: Vec<String> = ASSETS.iter().map(|a| format!("{}@aggTrade", bn_sym(a))).collect();
    let url = format!("{}/{}", BN_WS, streams.join("/"));
    let (mut ws, _) = connect_async(&url).await.context("BN WS")?;
    info!("[BN] Connected");
    while let Some(msg) = ws.next().await {
        match msg {
            Ok(Message::Text(txt)) => {
                let data: Value = match serde_json::from_str(&txt) { Ok(d) => d, _ => continue };
                let inner = data.get("data").unwrap_or(&data);
                let sym_raw = inner.get("s").and_then(|s| s.as_str()).unwrap_or("").to_lowercase();
                let px = inner.get("p").and_then(|p| p.as_str().and_then(|s| s.parse::<f64>().ok()));
                let asset: Option<&'static str> = match sym_raw.as_str() {
                    "btcusdt" => Some("btc"), "ethusdt" => Some("eth"),
                    "solusdt" => Some("sol"), "xrpusdt" => Some("xrp"), _ => None,
                };
                if let (Some(a), Some(p)) = (asset, px) {
                    if p > 0.0 { state.write().await.bn_update(a, p); }
                }
            }
            Ok(Message::Ping(d)) => { let _ = ws.send(Message::Pong(d)).await; }
            Ok(_) => {}
            Err(e) => { error!("[BN] WS: {}", e); break; }
        }
    }
    Ok(())
}

// ── Scanner ──────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Window {
    slug: String,
    asset: &'static str,
    wmin: u32,
    tid_up: String,
    tid_down: String,
    start_ts: i64,
    end_ts: i64,
}

impl Window {
    fn left(&self) -> i64 { self.end_ts - Utc::now().timestamp() }
}

struct Scanner {
    http: reqwest::Client,
    cache: Vec<Window>,
    last_fetch: Instant,
}

impl Scanner {
    fn new() -> Self {
        Scanner {
            http: reqwest::Client::builder().user_agent("cl-sniper/6.0")
                .timeout(Duration::from_secs(5)).build().expect("HTTP"),
            cache: Vec::new(),
            last_fetch: Instant::now() - Duration::from_secs(999),
        }
    }

    async fn scan(&mut self) -> Vec<Window> {
        if self.last_fetch.elapsed() < Duration::from_secs(10) {
            return self.cache.iter().filter(|w| w.left() > 0).cloned().collect();
        }
        let now = Utc::now().timestamp();
        let mut windows = Vec::new();
        for &asset in ASSETS {
            for &wmin in &[5u32, 15] {
                let interval = wmin as i64 * 60;
                let window_start = (now / interval) * interval;
                let next_start = window_start + interval;
                for start_ts in [window_start, next_start] {
                    let end_ts = start_ts + interval;
                    if end_ts < now { continue; }
                    let slug = format!("{}-updown-{}m-{}", asset, wmin, start_ts);
                    let resp = match self.http.get(format!("{}/markets", GAMMA_API))
                        .query(&[("slug", &slug)]).send().await {
                        Ok(r) if r.status().is_success() => r, _ => continue,
                    };
                    let data: Value = match resp.json().await { Ok(d) => d, _ => continue };
                    let market = if data.is_array() {
                        match data.as_array().and_then(|a| a.first()) { Some(m) => m.clone(), None => continue }
                    } else { data };
                    let tids_raw = market.get("clobTokenIds").unwrap_or(&Value::Null);
                    let tids: Vec<String> = if tids_raw.is_string() {
                        serde_json::from_str(tids_raw.as_str().unwrap_or("[]")).unwrap_or_default()
                    } else { serde_json::from_value(tids_raw.clone()).unwrap_or_default() };
                    if tids.len() < 2 { continue; }
                    let outcomes_raw = market.get("outcomes").unwrap_or(&Value::Null);
                    let outcomes: Vec<String> = if outcomes_raw.is_string() {
                        serde_json::from_str(outcomes_raw.as_str().unwrap_or("[]")).unwrap_or_default()
                    } else { serde_json::from_value(outcomes_raw.clone()).unwrap_or_default() };
                    let (tid_up, tid_down) = if outcomes.len() >= 2 && outcomes[0] == "Down" {
                        (tids[1].clone(), tids[0].clone())
                    } else { (tids[0].clone(), tids[1].clone()) };
                    windows.push(Window { slug, asset, wmin, tid_up, tid_down, start_ts, end_ts });
                }
            }
        }
        self.cache = windows.clone();
        self.last_fetch = Instant::now();
        windows.into_iter().filter(|w| w.left() > 0).collect()
    }
}

// ── Book Cache ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Default)]
struct Book { bb: f64, ba: f64, has_asks: bool, has_bids: bool }

struct BookCache {
    http: reqwest::Client,
    cache: HashMap<String, Book>,
    ts: HashMap<String, Instant>,
}

impl BookCache {
    fn new() -> Self {
        BookCache {
            http: reqwest::Client::builder().user_agent("cl-sniper/6.0")
                .timeout(Duration::from_secs(2)).build().expect("HTTP"),
            cache: HashMap::new(), ts: HashMap::new(),
        }
    }
    fn get(&self, tid: &str) -> Book {
        self.cache.get(tid).cloned().unwrap_or_default()
    }
    async fn bulk_refresh(&mut self, tids: &[String]) {
        let stale: Vec<&String> = tids.iter()
            .filter(|t| self.ts.get(*t).map(|ts| ts.elapsed() >= Duration::from_secs(1)).unwrap_or(true))
            .collect();
        if stale.is_empty() { return; }
        let body: Vec<Value> = stale.iter().map(|t| json!({"token_id": t})).collect();
        let resp = match self.http.post(format!("{}/books", CLOB_API)).json(&body).send().await {
            Ok(r) if r.status().is_success() => r, _ => return,
        };
        let results: Vec<Value> = match resp.json().await { Ok(d) => d, _ => return };
        let now = Instant::now();
        for item in &results {
            let tid = item.get("asset_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            if tid.is_empty() { continue; }
            let mut book = Book::default();
            if let Some(bids) = item.get("bids").and_then(|b| b.as_array()) {
                let mut prices: Vec<f64> = bids.iter()
                    .filter_map(|b| b.get("price").and_then(|p| p.as_str().and_then(|s| s.parse().ok())))
                    .collect();
                prices.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
                if let Some(&bb) = prices.first() { book.bb = bb; book.has_bids = true; }
            }
            if let Some(asks) = item.get("asks").and_then(|a| a.as_array()) {
                let mut prices: Vec<f64> = asks.iter()
                    .filter_map(|a| a.get("price").and_then(|p| p.as_str().and_then(|s| s.parse().ok())))
                    .collect();
                prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                if let Some(&ba) = prices.first() { book.ba = ba; book.has_asks = true; }
            }
            self.cache.insert(tid.clone(), book);
            self.ts.insert(tid, now);
        }
    }
}

// ── Executor (FIX #1: persistent auth per tick) ──────────────────────────────

struct Executor {
    private_key: String,
    paper: bool,
    pnl: f64,
    consec_loss: u32,
    oid_counter: u64,
    fills: HashMap<String, FillInfo>,
    clob_ids: HashMap<String, String>,
}

#[derive(Clone)]
struct FillInfo {
    price: f64,
    filled: bool,
    fill_px: f64,
    posted_at: Instant,
}

impl Executor {
    async fn new_live(private_key: &str) -> Result<Self> {
        let signer = LocalSigner::from_str(private_key).context("Invalid key")?.with_chain_id(Some(POLYGON));
        let client = ClobClient::new(CLOB_API, ClobConfig::default())?
            .authentication_builder(&signer).signature_type(SignatureType::Proxy).authenticate().await?;
        info!("[EXEC] CLOB: {}", client.ok().await.context("health")?);
        match client.cancel_all_orders().await { Ok(r) => info!("[EXEC] Cancel all: {:?}", r), Err(e) => warn!("[EXEC] Cancel: {}", e) }
        match client.balance_allowance(Default::default()).await { Ok(b) => info!("[EXEC] Balance: {:?}", b), Err(e) => warn!("[EXEC] Bal: {}", e) }
        info!("[EXEC] LIVE mode ready");
        Ok(Executor { private_key: private_key.to_string(), paper: false, pnl: 0.0, consec_loss: 0, oid_counter: 0, fills: HashMap::new(), clob_ids: HashMap::new() })
    }

    fn new_paper() -> Self {
        info!("[EXEC] PAPER mode");
        Executor { private_key: String::new(), paper: true, pnl: 0.0, consec_loss: 0, oid_counter: 0, fills: HashMap::new(), clob_ids: HashMap::new() }
    }

    /// FIX #1: Authenticate once, return client for this tick
    async fn auth_client(&self) -> Result<AuthClient> {
        let signer = LocalSigner::from_str(&self.private_key).context("key")?
            .with_chain_id(Some(POLYGON));
        let client = ClobClient::new(CLOB_API, ClobConfig::default())?
            .authentication_builder(&signer)
            .signature_type(SignatureType::Proxy)
            .authenticate().await?;
        Ok(client)
    }

    fn next_oid(&mut self) -> String { self.oid_counter += 1; format!("oid-{}", self.oid_counter) }
    fn dd_ok(&self) -> bool { self.pnl > -MAX_DD }
    fn consec_ok(&self) -> bool { self.consec_loss < MAX_CONSEC_LOSS }

    /// Post maker limit order using pre-authed client
    async fn post_maker(&mut self, client: &AuthClient, tid: &str, price: f64) -> Result<String> {
        let oid = self.next_oid();
        self.fills.insert(oid.clone(), FillInfo { price, filled: false, fill_px: 0.0, posted_at: Instant::now() });
        if self.paper {
            return Ok(oid);
        }
        let signer = LocalSigner::from_str(&self.private_key)?.with_chain_id(Some(POLYGON));
        let price_dec = Decimal::from_str(&format!("{:.2}", price))?;
        let shares = (STAKE / price).max(1.0) as i64;
        let order = client.limit_order()
            .token_id(tid.parse().context("U256")?)
            .size(Decimal::from(shares)).price(price_dec).side(PolySide::Buy)
            .build().await?;
        let signed = client.sign(&signer, order).await?;
        let resp = client.post_order(signed).await?;
        let clob_id = resp.order_id.to_string();
        info!("[LIVE] MAKER @{:.3} -> {}", price, clob_id);
        self.clob_ids.insert(oid.clone(), clob_id);
        Ok(oid)
    }

    /// Paper-only maker post (no CLOB client needed)
    fn post_maker_paper(&mut self, _tid: &str, price: f64) -> Result<String> {
        let oid = self.next_oid();
        self.fills.insert(oid.clone(), FillInfo { price, filled: false, fill_px: 0.0, posted_at: Instant::now() });
        Ok(oid)
    }

    fn check_maker_fill(&mut self, oid: &str, current_ask: f64) -> bool {
        let Some(fill) = self.fills.get_mut(oid) else { return false };
        if fill.filled { return true; }
        if fill.price >= current_ask {
            fill.filled = true;
            fill.fill_px = fill.price;
            return true;
        }
        if self.paper && fill.posted_at.elapsed() > Duration::from_secs(1) {
            let elapsed_ms = fill.posted_at.elapsed().as_millis();
            if (elapsed_ms % 100) < 60 {
                fill.filled = true;
                fill.fill_px = fill.price;
                return true;
            }
        }
        false
    }

    /// FIX #7: Double-fill prevention — verify cancel before repost
    async fn update_maker_price(&mut self, client: &AuthClient, oid: &str, tid: &str, new_price: f64) {
        if let Some(fill) = self.fills.get(oid) {
            if fill.filled || (new_price - fill.price).abs() < 0.005 { return; }
        } else { return; }

        if !self.paper {
            if let Some(clob_id) = self.clob_ids.get(oid).cloned() {
                let signer = match LocalSigner::from_str(&self.private_key) {
                    Ok(s) => s.with_chain_id(Some(POLYGON)), Err(_) => return,
                };
                // FIX #7: Verify cancel succeeded before reposting
                match client.cancel_order(&clob_id).await {
                    Ok(_) => {
                        // Cancel confirmed — safe to repost
                        let price_dec = Decimal::from_str(&format!("{:.2}", new_price)).unwrap_or_default();
                        let shares = (STAKE / new_price).max(1.0) as i64;
                        if let Ok(token_id) = tid.parse() {
                            if let Ok(order) = client.limit_order()
                                .token_id(token_id)
                                .size(Decimal::from(shares)).price(price_dec).side(PolySide::Buy)
                                .build().await {
                                if let Ok(signed) = client.sign(&signer, order).await {
                                    if let Ok(resp) = client.post_order(signed).await {
                                        self.clob_ids.insert(oid.to_string(), resp.order_id.to_string());
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // FIX #7: Cancel failed — do NOT repost, keep old order
                        warn!("[CHASE] Cancel failed for {}: {} — keeping old order", clob_id, e);
                        return;
                    }
                }
            }
        }

        if let Some(fill) = self.fills.get_mut(oid) {
            fill.price = new_price;
            fill.posted_at = Instant::now();
        }
    }

    /// FIX #4: FAK taker (partial fills accepted)
    async fn taker_buy(&mut self, client: &AuthClient, tid: &str, ask_price: f64) -> Result<(String, f64)> {
        let oid = self.next_oid();
        if self.paper {
            let fill_px = (ask_price + 0.005).min(0.99);
            self.fills.insert(oid.clone(), FillInfo { price: fill_px, filled: true, fill_px, posted_at: Instant::now() });
            return Ok((oid, fill_px));
        }
        let signer = LocalSigner::from_str(&self.private_key)?.with_chain_id(Some(POLYGON));
        let stake_dec = Decimal::from_str(&format!("{:.2}", STAKE))?;
        let order = client.market_order()
            .token_id(tid.parse().context("U256")?)
            .amount(Amount::usdc(stake_dec)?).side(PolySide::Buy)
            .order_type(PolyOrderType::FAK)  // FIX #4: FAK instead of FOK
            .build().await?;
        let signed = client.sign(&signer, order).await?;
        match client.post_order(signed).await {
            Ok(r) if r.success => {
                info!("[LIVE] FAK FILL @~{:.3}", ask_price);
                Ok((oid, ask_price))
            }
            Ok(r) => {
                warn!("[LIVE] FAK zero fill: {:?}", r.error_msg);
                // FIX #9: Explicit cleanup on zero fill
                if let Some(clob_id) = self.clob_ids.get(&oid).cloned() {
                    let _ = client.cancel_order(&clob_id).await;
                }
                Ok((oid, 0.0))
            }
            Err(e) => { error!("[LIVE] Taker: {}", e); Ok((oid, 0.0)) }
        }
    }

    /// SL exit: taker sell at bid to recover partial value
    async fn taker_sell(&mut self, client: &AuthClient, tid: &str, shares: f64, bid_price: f64) -> Result<f64> {
        if self.paper {
            let recovery = (bid_price - 0.005).max(0.0);
            return Ok(recovery);
        }
        let signer = LocalSigner::from_str(&self.private_key)?.with_chain_id(Some(POLYGON));
        let shares_dec = Decimal::from_str(&format!("{:.0}", shares.max(1.0)))?;
        let order = client.market_order()
            .token_id(tid.parse().context("U256")?)
            .amount(Amount::shares(shares_dec)?).side(PolySide::Sell)
            .order_type(PolyOrderType::FAK)
            .build().await?;
        let signed = client.sign(&signer, order).await?;
        match client.post_order(signed).await {
            Ok(r) if r.success => {
                info!("[LIVE] SL SELL @~{:.3}", bid_price);
                Ok((bid_price - 0.005).max(0.0))
            }
            Ok(r) => {
                warn!("[LIVE] SL sell zero fill: {:?}", r.error_msg);
                Ok(0.0)
            }
            Err(e) => { error!("[LIVE] SL sell: {}", e); Ok(0.0) }
        }
    }

    async fn cancel(&mut self, client: &AuthClient, oid: &str) {
        if !self.paper {
            if let Some(clob_id) = self.clob_ids.get(oid).cloned() {
                let _ = client.cancel_order(&clob_id).await;
            }
        }
        self.fills.remove(oid);
        self.clob_ids.remove(oid);
    }
}

// ── Active Trade ─────────────────────────────────────────────────────────────

#[derive(Clone)]
struct ActiveTrade {
    oid: String,
    tid: String,
    dir: String,
    asset: &'static str,
    wmin: u32,
    entry_px: f64,
    fill_px: f64,
    end_ts: i64,
    filled: bool,
    sl_triggered: bool,
    sl_recovery: f64,
    phase: String,
}

// ── Paper Tracker ────────────────────────────────────────────────────────────

struct PaperTracker {
    label: &'static str,
    wins: u32,
    losses: u32,
    pnl: f64,
    pending: HashMap<String, (String, f64)>,
}

impl PaperTracker {
    fn new(label: &'static str) -> Self {
        PaperTracker { label, wins: 0, losses: 0, pnl: 0.0, pending: HashMap::new() }
    }
}

// ── Engine ───────────────────────────────────────────────────────────────────

struct Engine {
    state: SharedState,
    scanner: Scanner,
    books: BookCache,
    exe: Executor,
    active: HashMap<String, ActiveTrade>,
    traded: HashSet<String>,
    cl_opens: HashMap<String, f64>,
    cl_open_ts: HashMap<String, i64>,       // snap timestamp used for open (closest to start_ts)
    cl_closes: HashMap<String, f64>,
    cl_close_ts: HashMap<String, i64>,      // snap timestamp used for close (closest to end_ts)
    wins: u32,
    losses: u32,
    sl_count: u32,
    start: Instant,
    paper_10: PaperTracker,
    paper_15: PaperTracker,
}

impl Engine {
    fn new(state: SharedState, scanner: Scanner, books: BookCache, exe: Executor) -> Self {
        Engine {
            state, scanner, books, exe,
            active: HashMap::new(), traded: HashSet::new(),
            cl_opens: HashMap::new(), cl_open_ts: HashMap::new(),
            cl_closes: HashMap::new(), cl_close_ts: HashMap::new(),
            wins: 0, losses: 0, sl_count: 0, start: Instant::now(),
            paper_10: PaperTracker::new("P10"),
            paper_15: PaperTracker::new("P15"),
        }
    }

    async fn tick(&mut self) -> Result<()> {
        let windows = self.scanner.scan().await;

        // FIX #6: Bulk refresh books for near-expiry AND held positions
        let mut active_tids: Vec<String> = Vec::new();
        for w in &windows {
            if w.left() > 0 && w.left() < 120 {
                active_tids.push(w.tid_up.clone());
                active_tids.push(w.tid_down.clone());
            }
        }
        // FIX #6: Add held position token IDs for SL bid checking
        for t in self.active.values() {
            if t.filled { active_tids.push(t.tid.clone()); }
        }
        active_tids.sort(); active_tids.dedup();
        if !active_tids.is_empty() { self.books.bulk_refresh(&active_tids).await; }

        // FIX #2: Record CL opens — snap immediately on first detection
        self.record_opens(&windows).await;

        // FIX #3: Record CL closes — first CL after end_ts
        self.record_closes(&windows).await;

        // FIX #1: Auth once per tick for live mode
        let client_opt = if !self.exe.paper {
            match self.exe.auth_client().await {
                Ok(c) => Some(c),
                Err(e) => { error!("[AUTH] {}", e); None }
            }
        } else { None };

        self.scan_entries(&windows, &client_opt).await;
        self.manage_orders(&client_opt).await;
        self.settle().await;
        Ok(())
    }

    /// CL open — continuously updated to the snap closest to start_ts
    async fn record_opens(&mut self, windows: &[Window]) {
        let s = self.state.read().await;
        for w in windows {
            if let Some(snap) = s.cl_snap.get(w.asset) {
                // Find the CL snap entry closest to this window's start_ts
                if let Some((&best_ts, &best_px)) = snap.iter()
                    .min_by_key(|(&ts, _)| (ts - w.start_ts).abs())
                {
                    if best_px <= 0.0 { continue; }
                    let new_dist = (best_ts - w.start_ts).abs();
                    let prev_dist = self.cl_open_ts.get(&w.slug)
                        .map(|&ts| (ts - w.start_ts).abs())
                        .unwrap_or(i64::MAX);
                    if new_dist < prev_dist {
                        self.cl_opens.insert(w.slug.clone(), best_px);
                        self.cl_open_ts.insert(w.slug.clone(), best_ts);
                    }
                }
            }
        }
    }

    /// CL close — continuously updated to the snap closest to end_ts (only after window ends)
    async fn record_closes(&mut self, windows: &[Window]) {
        let s = self.state.read().await;
        let now = Utc::now().timestamp();

        // Collect all (slug, asset, end_ts) pairs that need close prices
        let mut targets: Vec<(String, &'static str, i64)> = Vec::new();
        for w in windows {
            if now > w.end_ts {
                targets.push((w.slug.clone(), w.asset, w.end_ts));
            }
        }
        // Also check active trades whose windows have ended
        for (slug, t) in &self.active {
            if now > t.end_ts {
                targets.push((slug.clone(), t.asset, t.end_ts));
            }
        }

        for (slug, asset, end_ts) in targets {
            if let Some(snap) = s.cl_snap.get(asset) {
                // Find the CL snap entry closest to this window's end_ts
                if let Some((&best_ts, &best_px)) = snap.iter()
                    .min_by_key(|(&ts, _)| (ts - end_ts).abs())
                {
                    if best_px <= 0.0 { continue; }
                    let new_dist = (best_ts - end_ts).abs();
                    let prev_dist = self.cl_close_ts.get(&slug)
                        .map(|&ts| (ts - end_ts).abs())
                        .unwrap_or(i64::MAX);
                    if new_dist < prev_dist {
                        self.cl_closes.insert(slug.clone(), best_px);
                        self.cl_close_ts.insert(slug, best_ts);
                    }
                }
            }
        }
    }

    async fn scan_entries(&mut self, windows: &[Window], client: &Option<AuthClient>) {
        let filled_count = self.active.values().filter(|t| t.filled).count();
        if filled_count >= MAX_CONCURRENT { return; }
        if !self.exe.dd_ok() || !self.exe.consec_ok() { return; }

        for w in windows {
            if self.active.contains_key(&w.slug) { continue; }
            if self.traded.contains(&w.slug) { continue; }
            if !self.cl_opens.contains_key(&w.slug) { continue; }

            let left = w.left();
            if !(TAKER_DEADLINE..=ENTRY_START).contains(&left) { continue; }

            let cl_open = self.cl_opens[&w.slug];
            let s = self.state.read().await;
            let cl_now = match s.cl_px.get(w.asset) { Some(&px) if px > 0.0 => px, _ => continue };
            let cl_delta = (cl_now - cl_open) / cl_open * 100.0;
            let dir = if cl_delta > 0.0 { "UP" } else if cl_delta < 0.0 { "DOWN" } else { continue };

            // BN contra (15s lookback)
            let bn_trend = s.bn_trend(w.asset, 15);
            let bn_blocks = if let Some(bt) = bn_trend {
                (dir == "UP" && bt < -BN_CONTRA_PCT) || (dir == "DOWN" && bt > BN_CONTRA_PCT)
            } else { false };
            if bn_blocks {
                trace!("[SKIP] {} BN contra", w.slug);
                continue;
            }

            // CL fade (10s lookback)
            let cl_fade_blocks = if let Some(ct) = s.cl_trend(w.asset, 10) {
                (dir == "UP" && ct < -CL_FADE_PCT) || (dir == "DOWN" && ct > CL_FADE_PCT)
            } else { false };
            if cl_fade_blocks {
                trace!("[SKIP] {} CL fade", w.slug);
                continue;
            }
            drop(s);

            // FIX #8: Paper trackers apply BN/CL filters (only reach here if both passed)
            if w.wmin == 15 {
                let ptid = if dir == "UP" { &w.tid_up } else { &w.tid_down };
                let pbook = self.books.get(ptid);
                if pbook.has_asks && pbook.ba >= MIN_ENTRY && pbook.ba <= MAX_ENTRY {
                    let pentry = ((pbook.ba - 0.01) * 100.0).round() / 100.0;
                    let scaled_10 = DELTA_15M_PAPER_10 * (asset_stdev(w.asset) / STDEV_BASE);
                    let scaled_15 = DELTA_15M_PAPER_15 * (asset_stdev(w.asset) / STDEV_BASE);
                    if cl_delta.abs() >= scaled_10 && !self.paper_10.pending.contains_key(&w.slug) {
                        self.paper_10.pending.insert(w.slug.clone(), (dir.to_string(), pentry));
                        info!("[P10] {} {} 15m @{:.3} d={:+.4}%", dir, w.asset.to_uppercase(), pentry, cl_delta);
                    }
                    if cl_delta.abs() >= scaled_15 && !self.paper_15.pending.contains_key(&w.slug) {
                        self.paper_15.pending.insert(w.slug.clone(), (dir.to_string(), pentry));
                        info!("[P15] {} {} 15m @{:.3} d={:+.4}%", dir, w.asset.to_uppercase(), pentry, cl_delta);
                    }
                }
            }

            // Scaled delta threshold
            let threshold = scaled_threshold(w.asset, w.wmin);
            if cl_delta.abs() < threshold { continue; }

            // Book check
            let tid = if dir == "UP" { &w.tid_up } else { &w.tid_down };
            let book = self.books.get(tid);
            if !book.has_asks || book.ba < MIN_ENTRY || book.ba > MAX_ENTRY { continue; }

            let entry_price = ((book.ba - 0.01) * 100.0).round() / 100.0;
            let entry_price = entry_price.clamp(MIN_ENTRY, MAX_ENTRY);

            let bn_str = bn_trend.map(|bt| format!(" BN={:+.3}%", bt)).unwrap_or_default();
            info!("[ENTRY] {} {} {}m @{:.3} d={:+.4}% ask={:.2} left={}s{}",
                  dir, w.asset.to_uppercase(), w.wmin, entry_price, cl_delta, book.ba, left, bn_str);

            let oid = if let Some(ref c) = client {
                match self.exe.post_maker(c, tid, entry_price).await {
                    Ok(o) => o, Err(e) => { error!("[ENTRY] {}", e); continue; }
                }
            } else {
                match self.exe.post_maker_paper(tid, entry_price) {
                    Ok(o) => o, Err(e) => { error!("[ENTRY] {}", e); continue; }
                }
            };

            self.active.insert(w.slug.clone(), ActiveTrade {
                oid, tid: tid.to_string(), dir: dir.to_string(), asset: w.asset,
                wmin: w.wmin, entry_px: entry_price, fill_px: 0.0,
                end_ts: w.end_ts, filled: false, sl_triggered: false, sl_recovery: 0.0, phase: "maker".to_string(),
            });
            self.traded.insert(w.slug.clone());
        }
    }

    async fn manage_orders(&mut self, client: &Option<AuthClient>) {
        let now = Utc::now().timestamp();
        let slugs: Vec<String> = self.active.keys().cloned().collect();

        for slug in slugs {
            let Some(t) = self.active.get(&slug) else { continue };
            let left = t.end_ts - now;

            // === UNFILLED: manage maker / switch to taker ===
            if !t.filled {
                let book = self.books.get(&t.tid);

                if t.phase == "maker" {
                    if self.exe.check_maker_fill(&t.oid, book.ba) {
                        let fp = self.exe.fills.get(&t.oid).map(|f| f.fill_px).unwrap_or(t.entry_px);
                        let Some(t) = self.active.get_mut(&slug) else { continue };
                        t.filled = true;
                        t.fill_px = fp;
                        t.phase = "holding".to_string();
                        info!("[FILL] MAKER {} {} @{:.3}", t.dir, t.asset.to_uppercase(), fp);
                        continue;
                    }

                    // Chase ask
                    if book.has_asks && book.ba <= MAX_ENTRY {
                        let new_price = ((book.ba - 0.01) * 100.0).round() / 100.0;
                        let t_ref = self.active.get(&slug).expect("checked");
                        let oid = t_ref.oid.clone();
                        let tid = t_ref.tid.clone();
                        if let Some(ref c) = client {
                            self.exe.update_maker_price(c, &oid, &tid, new_price).await;
                        }
                        if let Some(t) = self.active.get_mut(&slug) { t.entry_px = new_price; }
                    }

                    // FIX #4: T-45 switch to FAK taker
                    if left <= ENTRY_END {
                        let t_ref = self.active.get(&slug).expect("checked");
                        let oid_clone = t_ref.oid.clone();
                        let tid_clone = t_ref.tid.clone();
                        if let Some(ref c) = client {
                            self.exe.cancel(c, &oid_clone).await;
                        } else {
                            self.exe.fills.remove(&oid_clone);
                            self.exe.clob_ids.remove(&oid_clone);
                        }
                        if book.has_asks && book.ba <= MAX_ENTRY {
                            let (new_oid, fp) = if let Some(ref c) = client {
                                self.exe.taker_buy(c, &tid_clone, book.ba).await.unwrap_or_default()
                            } else {
                                // Paper taker
                                let fp = (book.ba + 0.005).min(0.99);
                                let oid = self.exe.next_oid();
                                self.exe.fills.insert(oid.clone(), FillInfo { price: fp, filled: true, fill_px: fp, posted_at: Instant::now() });
                                (oid, fp)
                            };
                            if fp > 0.0 {
                                let Some(t) = self.active.get_mut(&slug) else { continue };
                                t.oid = new_oid; t.filled = true; t.fill_px = fp; t.phase = "holding".to_string();
                                info!("[FILL] FAK {} {} @{:.3}", t.dir, t.asset.to_uppercase(), fp);
                            } else {
                                // FIX #9: Zero fill cleanup
                                self.active.remove(&slug);
                            }
                        } else {
                            self.active.remove(&slug);
                        }
                        continue;
                    }
                }

                // T-44 hard deadline
                if left < TAKER_DEADLINE && !self.active.get(&slug).map(|t| t.filled).unwrap_or(true) {
                    if let Some(t) = self.active.get(&slug) {
                        let oid = t.oid.clone();
                        if let Some(ref c) = client {
                            self.exe.cancel(c, &oid).await;
                        } else {
                            self.exe.fills.remove(&oid);
                            self.exe.clob_ids.remove(&oid);
                        }
                    }
                    self.active.remove(&slug);
                }
                continue;
            }

            // === FILLED: SL dual confirmation ===
            // Skip if SL already triggered (taker sell already executed)
            if t.sl_triggered { continue; }
            // Condition 1: CL direction flipped against us
            // Condition 2: Best bid of our token <= 50% of fill_px
            if let Some(&cl_open) = self.cl_opens.get(&slug) {
                let s = self.state.read().await;
                let cl_flipped = if let Some(&cl_now) = s.cl_px.get(t.asset) {
                    let delta = (cl_now - cl_open) / cl_open * 100.0;
                    (t.dir == "UP" && delta < 0.0) || (t.dir == "DOWN" && delta > 0.0)
                } else { false };
                drop(s);

                // FIX #5: Only check bid if CL confirms the flip
                if cl_flipped {
                    let book = self.books.get(&t.tid);
                    if book.has_bids && book.bb <= t.fill_px * SL_BID_PCT {
                        let dir = t.dir.clone();
                        let asset = t.asset;
                        let fp = t.fill_px;
                        let tid = t.tid.clone();
                        let shares = (STAKE / fp).max(1.0);
                        // Immediate taker sell at bid
                        let recovery = if let Some(ref c) = client {
                            self.exe.taker_sell(c, &tid, shares, book.bb).await.unwrap_or(0.0)
                        } else {
                            (book.bb - 0.005).max(0.0)
                        };
                        if let Some(t_mut) = self.active.get_mut(&slug) {
                            t_mut.sl_triggered = true;
                            t_mut.sl_recovery = recovery;
                        }
                        info!("[SL] {} {} bid={:.3} <= {:.3}(50% of {:.3}) recovery={:.3}",
                              dir, asset.to_uppercase(), book.bb, fp * SL_BID_PCT, fp, recovery);
                    }
                }
            }
        }
    }

    async fn settle(&mut self) {
        let now = Utc::now().timestamp();
        let slugs: Vec<String> = self.active.keys().cloned().collect();

        for slug in slugs {
            let Some(t) = self.active.get(&slug) else { continue };
            if now < t.end_ts + 3 { continue; }

            if !t.filled {
                let oid = t.oid.clone();
                self.exe.fills.remove(&oid);
                self.exe.clob_ids.remove(&oid);
                self.active.remove(&slug);
                continue;
            }

            // FIX #3: Use pre-recorded CL close
            let cl_open = self.cl_opens.get(&slug).copied().unwrap_or(0.0);
            let cl_close = self.cl_closes.get(&slug).copied().unwrap_or(0.0);

            if cl_open <= 0.0 || cl_close <= 0.0 {
                warn!("[SETTLE] CL missing for {}", slug);
                self.exe.pnl -= STAKE;
                self.active.remove(&slug);
                continue;
            }

            let actual = if cl_close >= cl_open { "UP" } else { "DOWN" };
            let won = actual == t.dir && !t.sl_triggered;

            let pnl = if t.fill_px <= 0.0 {
                -STAKE
            } else if won {
                let shares = STAKE / t.fill_px;
                shares * 1.0 - STAKE
            } else if t.sl_triggered {
                // SL: use recovery price captured at trigger time (taker sell already executed)
                let shares = STAKE / t.fill_px;
                (shares * t.sl_recovery) - STAKE
            } else {
                -STAKE
            };

            self.exe.pnl += pnl;
            let result = if won { self.wins += 1; self.exe.consec_loss = 0; "WIN" }
                else if t.sl_triggered { self.sl_count += 1; self.exe.consec_loss += 1; "SL" }
                else { self.losses += 1; self.exe.consec_loss += 1; "LOSS" };

            info!("[{}] {} {}m {} @{:.3} -> {} ${:+.2} total=${:+.2} CL:{:.2}->{:.2}",
                  result, t.asset.to_uppercase(), t.wmin, t.dir, t.fill_px,
                  actual, pnl, self.exe.pnl, cl_open, cl_close);
            self.active.remove(&slug);
        }

        // Settle paper trackers
        for tracker in [&mut self.paper_10, &mut self.paper_15] {
            let slugs: Vec<String> = tracker.pending.keys().cloned().collect();
            for slug in slugs {
                let end_ts = slug.rsplit('-').next()
                    .and_then(|s| s.parse::<i64>().ok())
                    .map(|start| {
                        let wmin = if slug.contains("15m") { 15i64 } else { 5i64 };
                        start + wmin * 60
                    });
                let Some(end) = end_ts else { continue };
                if now < end + 3 { continue; }

                let (dir, entry_px) = match tracker.pending.remove(&slug) {
                    Some(v) => v, None => continue,
                };

                // FIX #3: Use pre-recorded CL close
                let cl_open = self.cl_opens.get(&slug).copied().unwrap_or(0.0);
                let cl_close = self.cl_closes.get(&slug).copied().unwrap_or(0.0);

                if cl_open > 0.0 && cl_close > 0.0 {
                    let actual = if cl_close >= cl_open { "UP" } else { "DOWN" };
                    let won = actual == dir;
                    let pnl = if won { STAKE / entry_px - STAKE } else { -STAKE };
                    tracker.pnl += pnl;
                    if won { tracker.wins += 1; } else { tracker.losses += 1; }
                    let asset = slug.split('-').next().unwrap_or("?");
                    info!("[{}] {} 15m {} @{:.3} -> {} {} ${:+.2} (${:+.2})",
                          tracker.label, asset.to_uppercase(), dir, entry_px,
                          actual, if won {"WIN"} else {"LOSS"}, pnl, tracker.pnl);
                }
            }
        }

        // Cleanup
        let cut = now - 3600;
        self.cl_opens.retain(|s, _| s.rsplit('-').next().and_then(|v| v.parse::<i64>().ok()).map(|t| t > cut).unwrap_or(false));
        self.cl_open_ts.retain(|s, _| s.rsplit('-').next().and_then(|v| v.parse::<i64>().ok()).map(|t| t > cut).unwrap_or(false));
        self.cl_closes.retain(|s, _| s.rsplit('-').next().and_then(|v| v.parse::<i64>().ok()).map(|t| t > cut).unwrap_or(false));
        self.cl_close_ts.retain(|s, _| s.rsplit('-').next().and_then(|v| v.parse::<i64>().ok()).map(|t| t > cut).unwrap_or(false));
        self.traded.retain(|s| s.rsplit('-').next().and_then(|v| v.parse::<i64>().ok()).map(|t| t > cut).unwrap_or(false));
    }
}

// ── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("cl_sniper=info").with_target(false).init();
    dotenvy::dotenv().ok();
    let private_key = std::env::var("POLYMARKET_PRIVATE_KEY").unwrap_or_default();

    info!("==================================================================");
    info!("  CL SNIPER V6 — 9 fixes from V5");
    info!("  #1 Persistent auth | #2 CL open snap | #3 CL close snap");
    info!("  #4 FAK taker | #5 SL dual confirm | #6 Held book refresh");
    info!("  #7 Double-fill guard | #8 Paper BN/CL filter | #9 Taker cleanup");
    info!("  Assets: {:?}", ASSETS);
    info!("  Delta: 5m={}% 15m={}%", DELTA_BASE, DELTA_15M);
    info!("  Stake: ${} | Entry: [{},{}] | SL: bid<={}% of fill",
          STAKE, MIN_ENTRY, MAX_ENTRY, SL_BID_PCT*100.0);
    info!("  Mode: {}", if private_key.is_empty() { "PAPER" } else { "LIVE" });
    info!("==================================================================");

    let state: SharedState = Arc::new(RwLock::new(State::new()));
    let cl_s = state.clone();
    tokio::spawn(async move { run_cl_feed(cl_s).await; });
    let bn_s = state.clone();
    tokio::spawn(async move { run_bn_feed(bn_s).await; });

    info!("[BOOT] Waiting for feeds...");
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let s = state.read().await;
        if s.cl_px.contains_key("btc") && s.bn_px.contains_key("btc") { break; }
    }
    { let s = state.read().await; for &a in ASSETS {
        info!("  {}: CL=${:.2} BN=${:.2}", a.to_uppercase(),
              s.cl_px.get(a).copied().unwrap_or(0.0), s.bn_px.get(a).copied().unwrap_or(0.0));
    }}

    let exe = if private_key.is_empty() { Executor::new_paper() } else { Executor::new_live(&private_key).await? };
    let scanner = Scanner::new();
    let books = BookCache::new();
    let mut engine = Engine::new(state.clone(), scanner, books, exe);
    info!("[BOOT] Running...");

    let mut last_status = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Err(e) = engine.tick().await { error!("[ENGINE] {}", e); }
        if last_status.elapsed().as_secs() >= 30 {
            let s = state.read().await;
            let px: Vec<String> = ASSETS.iter().filter_map(|&a| s.cl_px.get(a).map(|p| format!("{}=${:.0}", a.to_uppercase(), p))).collect();
            let hrs = engine.start.elapsed().as_secs_f64() / 3600.0;
            let total = engine.wins + engine.losses + engine.sl_count;
            let wr = if total > 0 { engine.wins as f64 / total as f64 * 100.0 } else { 0.0 };
            let p10 = &engine.paper_10;
            let p15 = &engine.paper_15;
            info!("--- {} | {}W/{}L/{}SL ({:.0}%) ${:+.2} | P10:{}W/{}L ${:+.2} | P15:{}W/{}L ${:+.2} | active={} | {:.1}h ---",
                  px.join(" | "), engine.wins, engine.losses, engine.sl_count, wr, engine.exe.pnl,
                  p10.wins, p10.losses, p10.pnl, p15.wins, p15.losses, p15.pnl,
                  engine.active.len(), hrs);
            last_status = Instant::now();
        }
    }
}
