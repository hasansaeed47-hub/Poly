/// feeds.rs — All external data feeds (v2 — production-grade)
///
/// Three feeds:
/// 1. CL price feed   : WebSocket from Polymarket live-data (chainlink topic)
/// 2. PM book feed    : WebSocket from Polymarket CLOB subscriptions
/// 3. Market discovery: Batched REST via Gamma API (slug → token IDs)
///
/// v2 fixes:
/// - Batch size and throttle passed from config (no hardcoded constants)
/// - price_change merges deltas into existing book state instead of overwriting
/// - Full book depth (ask/bid arrays) logged for analysis
/// - Configurable price history cap
/// - All WS messages logged at trace level for replay
/// - Book staleness tracking (last_update_ts per entry)

use std::collections::HashMap;
use std::io::Write as IoWrite;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, trace, warn};
use url::Url;

// ── Log types ─────────────────────────────────────────────────────────────────

/// Shared file handle for JSONL log writers
pub type LogWriter = Arc<Mutex<std::fs::File>>;

/// Serializable log entry for every CL oracle price update
#[derive(Debug, Serialize)]
pub struct ClPriceLog {
    pub asset:  String,
    pub price:  f64,
    pub ts:     f64,
    pub source: String,
}

/// Serializable log entry for every book update
#[derive(Debug, Serialize)]
pub struct BookUpdateLog {
    pub token_id:   String,
    pub best_ask:   f64,
    pub best_bid:   f64,
    pub ask_depth:  f64,
    pub bid_depth:  f64,
    pub spread:     f64,
    pub mid:        f64,
    pub ts:         f64,
    pub source:     String,
    pub event_type: String,
}

// ── Shared state types ────────────────────────────────────────────────────────

/// CL oracle prices: asset → (unix_ts, price)
pub type ClPrices = Arc<DashMap<String, (f64, f64)>>;

/// PM order book: token_id → BookEntry
pub type BookState = Arc<DashMap<String, BookEntry>>;

/// CL price history for sigma: asset → Vec<(ts, price)>
pub type PriceHistory = Arc<DashMap<String, Vec<(f64, f64)>>>;

#[derive(Debug, Clone)]
pub struct BookEntry {
    pub best_ask:  f64,
    pub best_bid:  f64,
    pub ask_depth: f64,   // total ask size at best
    pub bid_depth: f64,   // total bid size at best
    pub spread:    f64,   // best_ask - best_bid
    pub mid:       f64,   // (best_ask + best_bid) / 2
    pub ts:        f64,   // unix timestamp of last update
    pub source:    BookSource, // where this data came from
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BookSource {
    Rest,
    WsSnapshot,
    WsDelta,
}

impl std::fmt::Display for BookSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BookSource::Rest       => write!(f, "REST"),
            BookSource::WsSnapshot => write!(f, "WS_SNAP"),
            BookSource::WsDelta    => write!(f, "WS_DELTA"),
        }
    }
}

/// Market metadata fetched once at startup
#[derive(Debug, Clone)]
pub struct MarketMeta {
    pub slug:         String,
    pub asset:        String,
    pub tf:           u32,
    pub window_start: u64,
    pub window_end:   u64,
    pub token_yes:    String,
    pub token_no:     String,
    pub open_price:   f64,
    pub condition_id: String,
}

// ── Feed config (passed from main, no hardcoded constants) ────────────────────

#[derive(Debug, Clone)]
pub struct FeedParams {
    pub book_batch_size:   usize,
    pub rest_throttle_ms:  u64,
    pub ws_reconnect_secs: u64,
    pub price_history_cap: usize,
}

// ── Gamma API response types ──────────────────────────────────────────────────

#[derive(Deserialize, Debug)]
struct GammaEvent {
    markets: Vec<GammaMarket>,
}

/// Deserialize a field that may be either a JSON array or a stringified JSON array.
/// Polymarket's Gamma API sometimes returns `"[\"Up\",\"Down\"]"` instead of `["Up","Down"]`.
fn deserialize_string_or_vec<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;
    struct StringOrVec;
    impl<'de> de::Visitor<'de> for StringOrVec {
        type Value = Option<Vec<String>>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a JSON array or a stringified JSON array")
        }
        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> { Ok(None) }
        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> { Ok(None) }
        fn visit_str<E: de::Error>(self, s: &str) -> Result<Self::Value, E> {
            serde_json::from_str(s).map(Some).map_err(de::Error::custom)
        }
        fn visit_seq<A: de::SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
            let v = Vec::deserialize(de::value::SeqAccessDeserializer::new(seq))?;
            Ok(Some(v))
        }
    }
    deserializer.deserialize_any(StringOrVec)
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    condition_id:   Option<String>,
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    clob_token_ids: Option<Vec<String>>,
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    outcomes:       Option<Vec<String>>,
    start_date_iso: Option<String>,
    end_date_iso:   Option<String>,
}

// ── CLOB REST book response ───────────────────────────────────────────────────

#[derive(Deserialize, Debug)]
struct ClobBook {
    asks: Vec<ClobLevel>,
    bids: Vec<ClobLevel>,
}

#[derive(Deserialize, Debug)]
struct ClobLevel {
    price: String,
    size:  String,
}

// ── Rate limiter ──────────────────────────────────────────────────────────────

pub struct RateLimiter {
    last_call: Mutex<Instant>,
    min_gap:   Duration,
}

impl RateLimiter {
    pub fn new(throttle_ms: u64) -> Self {
        Self {
            last_call: Mutex::new(Instant::now() - Duration::from_secs(10)),
            min_gap:   Duration::from_millis(throttle_ms),
        }
    }

    pub async fn wait(&self) {
        let mut last = self.last_call.lock().await;
        let elapsed = last.elapsed();
        if elapsed < self.min_gap {
            tokio::time::sleep(self.min_gap - elapsed).await;
        }
        *last = Instant::now();
    }
}

// ── Utility ───────────────────────────────────────────────────────────────────

fn now_f64() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

// ── Market discovery ──────────────────────────────────────────────────────────

/// Build slug for a given asset, timeframe, and window start unix timestamp
pub fn build_slug(asset: &str, tf_mins: u32, window_start: u64) -> String {
    format!("{}-updown-{}m-{}", asset, tf_mins, window_start)
}

/// Compute window starts for the current time.
/// Returns all windows that are currently open or starting within 60s.
pub fn current_window_starts(tf_mins: u32, now_secs: u64) -> Vec<u64> {
    let interval = (tf_mins as u64) * 60;
    if interval == 0 {
        return vec![];
    }
    let current = (now_secs / interval) * interval;
    vec![current, current + interval]
}

/// Fetch market metadata for one slug from Gamma API
pub async fn fetch_market_meta(
    client:    &Client,
    gamma_api: &str,
    slug:      &str,
    asset:     &str,
    tf:        u32,
    limiter:   &RateLimiter,
) -> Result<Option<MarketMeta>> {
    limiter.wait().await;

    let url = format!("{}/events/slug/{}", gamma_api, slug);
    debug!("[GAMMA] GET {}", url);

    let resp = client
        .get(&url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .context("gamma API request failed")?;

    let status = resp.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        debug!("[GAMMA] {} → 404 not found", slug);
        return Ok(None);
    }
    if !status.is_success() {
        warn!("[GAMMA] {} → HTTP {}", slug, status);
        return Ok(None);
    }

    let body = resp.text().await.context("gamma API read body failed")?;
    trace!("[GAMMA] {} response: {}", slug, body);

    let event: GammaEvent = serde_json::from_str(&body)
        .context("gamma API JSON parse failed")?;

    // Find the market with YES/NO outcomes
    let market = event.markets.iter().find(|m| {
        m.outcomes
            .as_ref()
            .map(|o| o.iter().any(|x| x.eq_ignore_ascii_case("yes")))
            .unwrap_or(false)
    });

    let market = match market {
        Some(m) => m,
        None    => return Ok(None),
    };

    let tokens = market.clob_token_ids.as_ref()
        .ok_or_else(|| anyhow!("no clobTokenIds"))?;
    let outcomes = market.outcomes.as_ref()
        .ok_or_else(|| anyhow!("no outcomes"))?;

    if tokens.len() < 2 || outcomes.len() < 2 {
        warn!("[GAMMA] {} has <2 tokens or outcomes", slug);
        return Ok(None);
    }

    // Map YES/NO to token IDs
    let yes_idx = outcomes.iter()
        .position(|o| o.eq_ignore_ascii_case("yes"))
        .ok_or_else(|| anyhow!("no YES outcome"))?;
    let no_idx = outcomes.iter()
        .position(|o| o.eq_ignore_ascii_case("no"))
        .ok_or_else(|| anyhow!("no NO outcome"))?;

    let token_yes = tokens[yes_idx].clone();
    let token_no  = tokens[no_idx].clone();

    let condition_id = market.condition_id.clone().unwrap_or_default();

    // Parse window times from slug
    let window_start = slug
        .rsplit('-')
        .next()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let window_end = window_start + (tf as u64 * 60);

    info!(
        "[GAMMA] {} condition={} YES={} NO={} window={}..{}",
        slug, condition_id, token_yes, token_no, window_start, window_end
    );

    Ok(Some(MarketMeta {
        slug: slug.to_string(),
        asset: asset.to_string(),
        tf,
        window_start,
        window_end,
        token_yes,
        token_no,
        open_price: 0.0,
        condition_id,
    }))
}

// ── Batch book fetcher ────────────────────────────────────────────────────────

/// Fetch order books for a batch of token IDs via REST.
/// Uses configurable batch size and throttle instead of hardcoded constants.
pub async fn fetch_books_batch(
    client:     &Client,
    clob_rest:  &str,
    token_ids:  &[String],
    limiter:    &RateLimiter,
    batch_size: usize,
    throttle_ms: u64,
    book_log:   &LogWriter,
) -> Result<HashMap<String, BookEntry>> {
    if token_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut result = HashMap::new();
    let batch_size = if batch_size == 0 { 20 } else { batch_size };

    for chunk in token_ids.chunks(batch_size) {
        limiter.wait().await;

        let mut url = Url::parse(&format!("{}/books", clob_rest))
            .context("invalid CLOB REST URL")?;

        {
            let mut pairs = url.query_pairs_mut();
            for tid in chunk {
                pairs.append_pair("token_id", tid);
            }
        }

        debug!("[BOOK REST] fetching {} tokens: {}", chunk.len(), url.as_str());

        let resp = client
            .get(url.as_str())
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .context("CLOB batch book request failed")?;

        let status = resp.status();
        if !status.is_success() {
            warn!("[BOOK REST] HTTP {} for {} tokens", status, chunk.len());
            continue;
        }

        let body = resp.text().await.context("CLOB batch book read body failed")?;
        trace!("[BOOK REST] response: {}", body);

        let books: Vec<Option<ClobBook>> = serde_json::from_str(&body)
            .context("CLOB batch book JSON parse failed")?;

        let now = now_f64();

        for (tid, book_opt) in chunk.iter().zip(books.iter()) {
            if let Some(book) = book_opt {
                let entry = parse_clob_book(book, now, BookSource::Rest);
                if entry.best_ask > 0.0 {
                    debug!(
                        "[BOOK REST] {} ask={:.4} bid={:.4} spread={:.4} mid={:.4} ask_depth={:.2} bid_depth={:.2}",
                        &tid[..8.min(tid.len())], entry.best_ask, entry.best_bid,
                        entry.spread, entry.mid, entry.ask_depth, entry.bid_depth
                    );
                    // Write BookUpdateLog JSONL for REST source
                    {
                        let log_entry = BookUpdateLog {
                            token_id: tid.clone(),
                            best_ask: entry.best_ask, best_bid: entry.best_bid,
                            ask_depth: entry.ask_depth, bid_depth: entry.bid_depth,
                            spread: entry.spread, mid: entry.mid,
                            ts: now, source: "rest".to_string(),
                            event_type: "rest_fetch".to_string(),
                        };
                        if let Ok(line) = serde_json::to_string(&log_entry) {
                            let mut file = book_log.lock().await;
                            let _ = writeln!(file, "{}", line);
                        }
                    }
                    result.insert(tid.clone(), entry);
                }
            }
        }

        // Throttle between batch chunks (config-driven)
        if token_ids.len() > batch_size {
            tokio::time::sleep(Duration::from_millis(throttle_ms)).await;
        }
    }

    info!("[BOOK REST] fetched {}/{} tokens successfully", result.len(), token_ids.len());
    Ok(result)
}

/// Parse a ClobBook into a BookEntry with full depth info
fn parse_clob_book(book: &ClobBook, ts: f64, source: BookSource) -> BookEntry {
    let (best_ask, ask_depth) = book.asks.iter()
        .filter_map(|l| {
            let p = l.price.parse::<f64>().ok()?;
            let s = l.size.parse::<f64>().ok()?;
            Some((p, s))
        })
        .fold((f64::MAX, 0.0_f64), |(best, depth), (p, s)| {
            if p < best { (p, s) }
            else if (p - best).abs() < 1e-10 { (best, depth + s) }
            else { (best, depth) }
        });
    let best_ask = if best_ask == f64::MAX { 0.0 } else { best_ask };

    let (best_bid, bid_depth) = book.bids.iter()
        .filter_map(|l| {
            let p = l.price.parse::<f64>().ok()?;
            let s = l.size.parse::<f64>().ok()?;
            Some((p, s))
        })
        .fold((0.0_f64, 0.0_f64), |(best, depth), (p, s)| {
            if p > best { (p, s) }
            else if (p - best).abs() < 1e-10 { (best, depth + s) }
            else { (best, depth) }
        });

    let spread = if best_ask > 0.0 && best_bid > 0.0 { best_ask - best_bid } else { 0.0 };
    let mid    = if best_ask > 0.0 && best_bid > 0.0 { (best_ask + best_bid) / 2.0 } else { 0.0 };

    BookEntry { best_ask, best_bid, ask_depth, bid_depth, spread, mid, ts, source }
}

// ── CL price WebSocket feed ───────────────────────────────────────────────────

/// Maintains a CL price WebSocket connection with auto-reconnect.
pub async fn run_cl_feed(
    live_ws:       String,
    assets:        Vec<String>,
    cl_prices:     ClPrices,
    price_history: PriceHistory,
    params:        FeedParams,
    cl_log:        LogWriter,
) {
    loop {
        info!("[CL] Connecting to {}", live_ws);

        match connect_cl_feed(&live_ws, &assets, &cl_prices, &price_history, &params, &cl_log).await {
            Ok(_)  => warn!("[CL] Feed closed cleanly, reconnecting in {}s...", params.ws_reconnect_secs),
            Err(e) => error!("[CL] Feed error: {:#}, reconnecting in {}s", e, params.ws_reconnect_secs),
        }

        tokio::time::sleep(Duration::from_secs(params.ws_reconnect_secs)).await;
    }
}

async fn connect_cl_feed(
    live_ws:       &str,
    assets:        &[String],
    cl_prices:     &ClPrices,
    price_history: &PriceHistory,
    params:        &FeedParams,
    cl_log:        &LogWriter,
) -> Result<()> {
    let url = Url::parse(live_ws).context("invalid live WS URL")?;
    let (mut ws, _) = connect_async(url).await.context("CL WS connect failed")?;

    // Subscribe to chainlink topic for each asset
    for asset in assets {
        let symbol = format!("{}usd", asset.to_uppercase());
        let sub = serde_json::json!({
            "type": "subscribe",
            "channel": "crypto_prices_chainlink",
            "symbol": symbol
        });
        ws.send(Message::Text(sub.to_string())).await?;
        info!("[CL] Subscribed to {}", symbol);
    }

    info!("[CL] Feed connected, {} assets subscribed", assets.len());

    let mut msg_count: u64 = 0;
    let mut price_count: u64 = 0;

    while let Some(msg) = ws.next().await {
        let msg = msg.context("CL WS message error")?;
        msg_count += 1;

        match &msg {
            Message::Text(text) => {
                trace!("[CL RAW] {}", text);
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
                    if process_cl_message(&v, cl_prices, price_history, params.price_history_cap, cl_log).await {
                        price_count += 1;
                    }
                } else {
                    warn!("[CL] Failed to parse JSON: {}", &text[..200.min(text.len())]);
                }
            }
            Message::Ping(_) => {
                trace!("[CL] Ping received (msg #{})", msg_count);
            }
            Message::Close(frame) => {
                warn!("[CL] Close frame received: {:?}", frame);
                break;
            }
            _ => {
                trace!("[CL] Non-text message type at msg #{}", msg_count);
            }
        }

        // Periodic feed health log
        if msg_count % 1000 == 0 {
            info!("[CL] Health: {} messages, {} price updates", msg_count, price_count);
        }
    }

    info!("[CL] Feed ended after {} messages, {} price updates", msg_count, price_count);
    Ok(())
}

/// Process a CL price message. Returns true if a price was updated.
/// Writes a ClPriceLog JSONL line on every price update.
async fn process_cl_message(
    v:             &serde_json::Value,
    cl_prices:     &ClPrices,
    price_history: &PriceHistory,
    history_cap:   usize,
    cl_log:        &LogWriter,
) -> bool {
    let channel = v.get("channel").and_then(|c| c.as_str()).unwrap_or("");
    if channel != "crypto_prices_chainlink" {
        return false;
    }

    let symbol = match v.get("symbol").and_then(|s| s.as_str()) {
        Some(s) => s.to_lowercase(),
        None => {
            warn!("[CL] Message missing 'symbol' field");
            return false;
        }
    };

    // "btcusd" → "btc"
    let asset = symbol.trim_end_matches("usd").to_string();

    // Parse price — accept both string and number JSON types
    let price: f64 = if let Some(p) = v.get("price") {
        if let Some(s) = p.as_str() {
            match s.parse::<f64>() {
                Ok(p) if p > 0.0 => p,
                _ => { warn!("[CL] Invalid price string '{}' for {}", s, asset); return false; }
            }
        } else if let Some(n) = p.as_f64() {
            if n > 0.0 { n } else { return false; }
        } else {
            warn!("[CL] Unparseable price field for {}: {:?}", asset, p);
            return false;
        }
    } else {
        warn!("[CL] Message missing 'price' field for {}", asset);
        return false;
    };

    // Parse timestamp — accept both number and string
    let ts = if let Some(t) = v.get("ts") {
        t.as_f64()
            .or_else(|| t.as_str().and_then(|s| s.parse::<f64>().ok()))
            .unwrap_or_else(now_f64)
    } else {
        now_f64()
    };

    // Get previous price for delta logging
    let prev = cl_prices.get(&asset).map(|v| v.1);

    cl_prices.insert(asset.clone(), (ts, price));

    // Write JSONL log entry for every CL price update
    {
        let log_entry = ClPriceLog {
            asset: asset.clone(),
            price,
            ts,
            source: "ws_chainlink".to_string(),
        };
        if let Ok(line) = serde_json::to_string(&log_entry) {
            let mut file = cl_log.lock().await;
            let _ = writeln!(file, "{}", line);
        }
    }

    // Log every price update with delta
    match prev {
        Some(prev_price) => {
            let delta = price - prev_price;
            let pct = if prev_price > 0.0 { delta / prev_price * 100.0 } else { 0.0 };
            debug!(
                "[CL PRICE] {} {:.6} delta={:+.6} ({:+.4}%) ts={:.3}",
                asset, price, delta, pct, ts
            );
        }
        None => {
            info!("[CL PRICE] {} first_price={:.6} ts={:.3}", asset, price, ts);
        }
    }

    // Append to price history with configurable cap
    let mut hist = price_history.entry(asset).or_default();
    hist.push((ts, price));
    let cap = if history_cap == 0 { 3600 } else { history_cap };
    if hist.len() > cap {
        let drain_to = hist.len() - cap;
        hist.drain(0..drain_to);
    }

    true
}

// ── PM book WebSocket feed ────────────────────────────────────────────────────

/// Maintains a PM CLOB book WebSocket connection with auto-reconnect.
pub async fn run_book_feed(
    clob_ws:    String,
    token_ids:  Arc<DashMap<String, ()>>,
    book_state: BookState,
    book_live:  Arc<AtomicU64>,
    params:     FeedParams,
    book_log:   LogWriter,
) {
    loop {
        info!("[BOOK WS] Connecting to {}", clob_ws);

        match connect_book_feed(&clob_ws, &token_ids, &book_state, &book_live, &book_log).await {
            Ok(_)  => warn!("[BOOK WS] Feed closed cleanly, reconnecting in {}s...", params.ws_reconnect_secs),
            Err(e) => error!("[BOOK WS] Feed error: {:#}, reconnecting in {}s", e, params.ws_reconnect_secs),
        }

        // Reset live timestamp on disconnect so warmup gate re-engages
        book_live.store(0, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_secs(params.ws_reconnect_secs)).await;
    }
}

async fn connect_book_feed(
    clob_ws:    &str,
    token_ids:  &DashMap<String, ()>,
    book_state: &BookState,
    book_live:  &Arc<AtomicU64>,
    book_log:   &LogWriter,
) -> Result<()> {
    let url = Url::parse(clob_ws).context("invalid CLOB WS URL")?;
    let (mut ws, _) = connect_async(url).await.context("CLOB WS connect failed")?;

    // Subscribe all known token IDs
    let ids: Vec<String> = token_ids.iter().map(|e| e.key().clone()).collect();
    if !ids.is_empty() {
        let sub = serde_json::json!({
            "auth": {},
            "type": "subscribe",
            "markets": ids
        });
        ws.send(Message::Text(sub.to_string())).await?;
        info!("[BOOK WS] Subscribed to {} token IDs", ids.len());
    } else {
        warn!("[BOOK WS] No token IDs to subscribe — feed will idle");
    }

    // Record connect time for warmup gate
    let connect_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    book_live.store(connect_ts, Ordering::Relaxed);
    info!("[BOOK WS] Live at ts={}", connect_ts);

    let mut msg_count: u64 = 0;
    let mut snap_count: u64 = 0;
    let mut delta_count: u64 = 0;

    while let Some(msg) = ws.next().await {
        let msg = msg.context("CLOB WS message error")?;
        msg_count += 1;

        match &msg {
            Message::Text(text) => {
                trace!("[BOOK RAW] {}", text);
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
                    match process_book_message(&v, book_state, book_log).await {
                        BookUpdateResult::Snapshot => snap_count += 1,
                        BookUpdateResult::Delta    => delta_count += 1,
                        BookUpdateResult::None     => {}
                    }
                } else {
                    warn!("[BOOK WS] Failed to parse JSON: {}", &text[..200.min(text.len())]);
                }
            }
            Message::Ping(_) => {
                trace!("[BOOK WS] Ping at msg #{}", msg_count);
            }
            Message::Close(frame) => {
                warn!("[BOOK WS] Close frame: {:?}", frame);
                break;
            }
            _ => {}
        }

        // Periodic health log
        if msg_count % 1000 == 0 {
            info!(
                "[BOOK WS] Health: {} msgs, {} snapshots, {} deltas, {} book_entries",
                msg_count, snap_count, delta_count, book_state.len()
            );
        }
    }

    info!("[BOOK WS] Feed ended: {} msgs, {} snaps, {} deltas", msg_count, snap_count, delta_count);
    Ok(())
}

enum BookUpdateResult {
    Snapshot,
    Delta,
    None,
}

/// Process a book WS message.
/// - "book" events: full snapshot, overwrites entry. Writes BookUpdateLog with source "ws_book".
/// - "price_change" events: merge into existing entry. Writes BookUpdateLog with source "ws_delta".
async fn process_book_message(v: &serde_json::Value, book_state: &BookState, book_log: &LogWriter) -> BookUpdateResult {
    let event_type = v.get("event_type").and_then(|e| e.as_str()).unwrap_or("");
    let asset_id = match v.get("asset_id").and_then(|a| a.as_str()) {
        Some(id) => id.to_string(),
        None     => return BookUpdateResult::None,
    };

    let now = now_f64();
    let short_id = &asset_id[..8.min(asset_id.len())];

    match event_type {
        "book" => {
            // Full book snapshot — always overwrite completely
            let best_ask   = extract_best_ask(v);
            let best_bid   = extract_best_bid(v);
            let ask_depth  = extract_depth_at_best(v, "asks", best_ask);
            let bid_depth  = extract_depth_at_best(v, "bids", best_bid);
            let spread     = if best_ask > 0.0 && best_bid > 0.0 { best_ask - best_bid } else { 0.0 };
            let mid        = if best_ask > 0.0 && best_bid > 0.0 { (best_ask + best_bid) / 2.0 } else { 0.0 };

            if best_ask > 0.0 {
                debug!(
                    "[BOOK SNAP] {} ask={:.4} bid={:.4} spread={:.4} mid={:.4} adepth={:.1} bdepth={:.1}",
                    short_id, best_ask, best_bid, spread, mid, ask_depth, bid_depth
                );
                book_state.insert(asset_id.clone(), BookEntry {
                    best_ask, best_bid, ask_depth, bid_depth, spread, mid, ts: now,
                    source: BookSource::WsSnapshot,
                });
                // Write BookUpdateLog JSONL
                {
                    let log_entry = BookUpdateLog {
                        token_id: asset_id, best_ask, best_bid, ask_depth, bid_depth,
                        spread, mid, ts: now, source: "ws_book".to_string(),
                        event_type: "book".to_string(),
                    };
                    if let Ok(line) = serde_json::to_string(&log_entry) {
                        let mut file = book_log.lock().await;
                        let _ = writeln!(file, "{}", line);
                    }
                }
                BookUpdateResult::Snapshot
            } else {
                warn!("[BOOK SNAP] {} no valid asks in snapshot", short_id);
                BookUpdateResult::None
            }
        }
        "price_change" => {
            // Delta update — MERGE into existing state, don't blindly overwrite
            let new_ask = extract_best_ask(v);
            let new_bid = extract_best_bid(v);

            // If we have no existing entry, we need at least an ask to create one
            let existing = book_state.get(&asset_id).map(|e| e.clone());

            let (best_ask, best_bid, ask_depth, bid_depth) = match existing {
                Some(prev) => {
                    // Merge: only update sides that have data in this delta
                    let ask = if new_ask > 0.0 { new_ask } else { prev.best_ask };
                    let bid = if new_bid > 0.0 { new_bid } else { prev.best_bid };
                    let adepth = if new_ask > 0.0 {
                        extract_depth_at_best(v, "asks", new_ask)
                    } else {
                        prev.ask_depth
                    };
                    let bdepth = if new_bid > 0.0 {
                        extract_depth_at_best(v, "bids", new_bid)
                    } else {
                        prev.bid_depth
                    };
                    (ask, bid, adepth, bdepth)
                }
                None => {
                    if new_ask <= 0.0 {
                        // No existing entry and no ask in delta — cannot create entry
                        trace!("[BOOK DELTA] {} skipped — no existing entry and no asks", short_id);
                        return BookUpdateResult::None;
                    }
                    let adepth = extract_depth_at_best(v, "asks", new_ask);
                    let bdepth = extract_depth_at_best(v, "bids", new_bid);
                    (new_ask, new_bid, adepth, bdepth)
                }
            };

            let spread = if best_ask > 0.0 && best_bid > 0.0 { best_ask - best_bid } else { 0.0 };
            let mid    = if best_ask > 0.0 && best_bid > 0.0 { (best_ask + best_bid) / 2.0 } else { 0.0 };

            debug!(
                "[BOOK DELTA] {} ask={:.4} bid={:.4} spread={:.4} mid={:.4}",
                short_id, best_ask, best_bid, spread, mid
            );

            book_state.insert(asset_id.clone(), BookEntry {
                best_ask, best_bid, ask_depth, bid_depth, spread, mid, ts: now,
                source: BookSource::WsDelta,
            });
            // Write BookUpdateLog JSONL
            {
                let log_entry = BookUpdateLog {
                    token_id: asset_id, best_ask, best_bid, ask_depth, bid_depth,
                    spread, mid, ts: now, source: "ws_delta".to_string(),
                    event_type: "price_change".to_string(),
                };
                if let Ok(line) = serde_json::to_string(&log_entry) {
                    let mut file = book_log.lock().await;
                    let _ = writeln!(file, "{}", line);
                }
            }

            BookUpdateResult::Delta
        }
        _ => {
            trace!("[BOOK WS] Unknown event_type '{}' for {}", event_type, short_id);
            BookUpdateResult::None
        }
    }
}

fn extract_best_ask(v: &serde_json::Value) -> f64 {
    v.get("asks")
        .and_then(|a| a.as_array())
        .map(|asks| {
            asks.iter()
                .filter_map(|l| {
                    l.get("price")
                        .and_then(|p| p.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                })
                .reduce(f64::min)
                .unwrap_or(0.0)
        })
        .unwrap_or(0.0)
}

fn extract_best_bid(v: &serde_json::Value) -> f64 {
    v.get("bids")
        .and_then(|b| b.as_array())
        .map(|bids| {
            bids.iter()
                .filter_map(|l| {
                    l.get("price")
                        .and_then(|p| p.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                })
                .reduce(f64::max)
                .unwrap_or(0.0)
        })
        .unwrap_or(0.0)
}

/// Extract total size at the best price level for a given side
fn extract_depth_at_best(v: &serde_json::Value, side: &str, best_price: f64) -> f64 {
    if best_price <= 0.0 {
        return 0.0;
    }
    v.get(side)
        .and_then(|a| a.as_array())
        .map(|levels| {
            levels.iter()
                .filter_map(|l| {
                    let p = l.get("price")?.as_str()?.parse::<f64>().ok()?;
                    let s = l.get("size")?.as_str()?.parse::<f64>().ok()?;
                    Some((p, s))
                })
                .filter(|(p, _)| (*p - best_price).abs() < 1e-10)
                .map(|(_, s)| s)
                .sum()
        })
        .unwrap_or(0.0)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a LogWriter backed by a temp file for tests
    fn test_log_writer() -> LogWriter {
        let file = tempfile::tempfile().expect("create temp file for test log");
        Arc::new(Mutex::new(file))
    }

    #[test]
    fn build_slug_correct() {
        assert_eq!(
            build_slug("btc", 5, 1772788200),
            "btc-updown-5m-1772788200"
        );
        assert_eq!(
            build_slug("eth", 15, 1772788200),
            "eth-updown-15m-1772788200"
        );
    }

    #[test]
    fn current_window_starts_5m() {
        let starts = current_window_starts(5, 1772788230);
        assert_eq!(starts[0], 1772788200);
        assert_eq!(starts[1], 1772788500);
    }

    #[test]
    fn current_window_starts_zero_tf() {
        let starts = current_window_starts(0, 1772788230);
        assert!(starts.is_empty());
    }

    #[test]
    fn extract_best_ask_from_book() {
        let v = serde_json::json!({
            "event_type": "book",
            "asset_id": "abc123",
            "asks": [
                {"price": "0.55", "size": "10"},
                {"price": "0.60", "size": "5"},
                {"price": "0.52", "size": "3"}
            ],
            "bids": []
        });
        assert_eq!(extract_best_ask(&v), 0.52);
    }

    #[test]
    fn extract_depth_at_best_level() {
        let v = serde_json::json!({
            "asks": [
                {"price": "0.52", "size": "3"},
                {"price": "0.52", "size": "7"},
                {"price": "0.55", "size": "10"}
            ]
        });
        let depth = extract_depth_at_best(&v, "asks", 0.52);
        assert!((depth - 10.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn price_change_merges_into_existing() {
        let book_state: BookState = Arc::new(DashMap::new());
        let book_log = test_log_writer();

        // Insert initial snapshot
        book_state.insert("token1".to_string(), BookEntry {
            best_ask: 0.55, best_bid: 0.45,
            ask_depth: 10.0, bid_depth: 5.0,
            spread: 0.10, mid: 0.50,
            ts: 1000.0, source: BookSource::WsSnapshot,
        });

        // Delta with only asks (no bids) — bid should be preserved
        let delta = serde_json::json!({
            "event_type": "price_change",
            "asset_id": "token1",
            "asks": [{"price": "0.53", "size": "8"}]
        });
        process_book_message(&delta, &book_state, &book_log).await;

        let entry = book_state.get("token1").unwrap();
        assert!((entry.best_ask - 0.53).abs() < 0.001, "ask should update to 0.53");
        assert!((entry.best_bid - 0.45).abs() < 0.001, "bid should be preserved at 0.45");
    }

    #[tokio::test]
    async fn price_change_without_existing_needs_ask() {
        let book_state: BookState = Arc::new(DashMap::new());
        let book_log = test_log_writer();

        // Delta with only bids, no existing entry — should NOT create entry
        let delta = serde_json::json!({
            "event_type": "price_change",
            "asset_id": "token2",
            "bids": [{"price": "0.45", "size": "5"}]
        });
        process_book_message(&delta, &book_state, &book_log).await;
        assert!(book_state.get("token2").is_none());
    }

    #[test]
    fn batch_book_url_construction() {
        let base = "https://clob.polymarket.com";
        let mut url = Url::parse(&format!("{}/books", base)).unwrap();
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("token_id", "abc");
            pairs.append_pair("token_id", "def");
        }
        assert!(url.as_str().contains("token_id=abc"));
        assert!(url.as_str().contains("token_id=def"));
    }

    #[tokio::test]
    async fn cl_message_parses_string_price() {
        let cl_prices: ClPrices = Arc::new(DashMap::new());
        let price_history: PriceHistory = Arc::new(DashMap::new());
        let cl_log = test_log_writer();

        let msg = serde_json::json!({
            "channel": "crypto_prices_chainlink",
            "symbol": "BTCUSD",
            "price": "67321.5",
            "ts": 1234567890.123
        });
        assert!(process_cl_message(&msg, &cl_prices, &price_history, 1000, &cl_log).await);
        let entry = cl_prices.get("btc").unwrap();
        assert!((entry.1 - 67321.5).abs() < 0.01);
    }

    #[tokio::test]
    async fn cl_message_parses_number_price() {
        let cl_prices: ClPrices = Arc::new(DashMap::new());
        let price_history: PriceHistory = Arc::new(DashMap::new());
        let cl_log = test_log_writer();

        let msg = serde_json::json!({
            "channel": "crypto_prices_chainlink",
            "symbol": "ETHUSD",
            "price": 3500.25,
            "ts": 1234567890
        });
        assert!(process_cl_message(&msg, &cl_prices, &price_history, 1000, &cl_log).await);
        let entry = cl_prices.get("eth").unwrap();
        assert!((entry.1 - 3500.25).abs() < 0.01);
    }
}
