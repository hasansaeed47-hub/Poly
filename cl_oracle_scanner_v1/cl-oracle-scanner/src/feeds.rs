/// feeds.rs — All external data feeds
///
/// Three feeds:
/// 1. CL price feed   : WebSocket from Polymarket live-data (chainlink topic)
/// 2. PM book feed    : WebSocket from Polymarket CLOB subscriptions
/// 3. Market discovery: Batched REST via Gamma API (slug → token IDs)
///
/// Batching rules:
/// - Book REST requests: max batch_size token IDs per request
/// - Minimum throttle_ms between any two REST calls
/// - WebSocket feeds are event-driven — no polling

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
use tokio_tungstenite::{connect_async, tungstenite::{Message, client::IntoClientRequest}};
use tracing::{debug, error, info, trace, warn};
use url::Url;

// ── JSONL log types ─────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct ClPriceLog {
    pub asset:      String,
    pub price:      f64,
    pub prev_price: f64,
    pub delta:      f64,      // price - prev_price
    pub pct_change: f64,      // (price - prev_price) / prev_price * 100
    pub ts:         f64,
    pub source:     String,   // "ws"
}

#[derive(Debug, Serialize)]
pub struct BookUpdateLog {
    pub token_id:   String,
    pub best_ask:   f64,
    pub best_bid:   f64,
    pub spread:     f64,      // best_ask - best_bid
    pub mid:        f64,      // (best_ask + best_bid) / 2
    pub ask_depth:  f64,      // size at best ask
    pub bid_depth:  f64,      // size at best bid
    pub ts:         f64,
    pub source:     String,   // "ws_book" | "ws_delta" | "rest"
    pub event_type: String,
}

/// Shared log writer type
pub type LogWriter = Arc<Mutex<std::fs::File>>;

// ── Feed parameters (from config) ───────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FeedParams {
    pub book_batch_size:    usize,
    pub rest_throttle_ms:   u64,
    pub ws_reconnect_secs:  u64,
    pub price_history_cap:  usize,
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
    pub ask_depth: f64,    // size at best ask level
    pub bid_depth: f64,    // size at best bid level
    pub ts:        f64,
    pub source:    String, // "ws_book" | "ws_delta" | "rest"
}

/// Market metadata fetched once at startup
#[derive(Debug, Clone)]
pub struct MarketMeta {
    pub slug:         String,
    pub asset:        String,
    pub tf:           u32,
    pub window_start: u64,   // unix timestamp
    pub window_end:   u64,
    pub token_yes:    String, // clobTokenId for YES
    pub token_no:     String, // clobTokenId for NO
    pub open_price:   f64,
}

// ── Feed health counters ─────────────────────────────────────────────────────

pub struct FeedHealth {
    pub cl_msg_count:   AtomicU64,
    pub book_msg_count: AtomicU64,
}

impl FeedHealth {
    pub fn new() -> Self {
        Self {
            cl_msg_count:   AtomicU64::new(0),
            book_msg_count: AtomicU64::new(0),
        }
    }
}

// ── Gamma API response types ──────────────────────────────────────────────────

#[derive(Deserialize, Debug)]
struct GammaEvent {
    markets: Vec<GammaMarket>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    clob_token_ids: Option<Vec<String>>,
    outcomes:       Option<Vec<String>>,
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

// ── Market discovery ──────────────────────────────────────────────────────────

/// Build slug for a given asset, timeframe, and window start unix timestamp
pub fn build_slug(asset: &str, tf_mins: u32, window_start: u64) -> String {
    format!("{}-updown-{}m-{}", asset, tf_mins, window_start)
}

/// Compute window starts for the current time
/// Returns all windows that are currently open or starting within 60s
pub fn current_window_starts(tf_mins: u32, now_secs: u64) -> Vec<u64> {
    let interval = (tf_mins as u64) * 60;
    let current  = (now_secs / interval) * interval;
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
    debug!("GET {}", url);

    let resp = client
        .get(&url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .context("gamma API request failed")?;

    if resp.status() == 404 {
        return Ok(None);
    }

    let event: GammaEvent = resp
        .json()
        .await
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

    let tokens = market.clob_token_ids.as_ref().ok_or_else(|| anyhow!("no clobTokenIds"))?;
    let outcomes = market.outcomes.as_ref().ok_or_else(|| anyhow!("no outcomes"))?;

    if tokens.len() < 2 || outcomes.len() < 2 {
        return Ok(None);
    }

    let yes_idx = outcomes
        .iter()
        .position(|o| o.eq_ignore_ascii_case("yes"))
        .ok_or_else(|| anyhow!("no YES outcome"))?;
    let no_idx = outcomes
        .iter()
        .position(|o| o.eq_ignore_ascii_case("no"))
        .ok_or_else(|| anyhow!("no NO outcome"))?;

    let token_yes = tokens[yes_idx].clone();
    let token_no  = tokens[no_idx].clone();

    let window_start = slug
        .rsplit('-')
        .next()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let window_end = window_start + (tf as u64 * 60);

    Ok(Some(MarketMeta {
        slug: slug.to_string(),
        asset: asset.to_string(),
        tf,
        window_start,
        window_end,
        token_yes,
        token_no,
        open_price: 0.0,
    }))
}

// ── Batch book fetcher ────────────────────────────────────────────────────────

pub async fn fetch_books_batch(
    client:    &Client,
    clob_rest: &str,
    token_ids: &[String],
    limiter:   &RateLimiter,
    book_log:  &LogWriter,
    params:    &FeedParams,
) -> Result<HashMap<String, BookEntry>> {
    if token_ids.is_empty() {
        return Ok(HashMap::new());
    }

    limiter.wait().await;

    let mut result = HashMap::new();

    for chunk in token_ids.chunks(params.book_batch_size) {
        let mut url = Url::parse(&format!("{}/books", clob_rest))
            .context("invalid CLOB REST URL")?;

        {
            let mut pairs = url.query_pairs_mut();
            for tid in chunk {
                pairs.append_pair("token_id", tid);
            }
        }

        debug!("Batch book fetch: {} tokens", chunk.len());

        let resp = client
            .get(url.as_str())
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .context("CLOB batch book request failed")?;

        if !resp.status().is_success() {
            warn!("CLOB batch book returned {}", resp.status());
            continue;
        }

        let books: Vec<Option<ClobBook>> = resp
            .json()
            .await
            .context("CLOB batch book JSON parse failed")?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        for (tid, book_opt) in chunk.iter().zip(books.iter()) {
            if let Some(book) = book_opt {
                let (best_ask, ask_depth) = book
                    .asks
                    .iter()
                    .filter_map(|l| {
                        let p = l.price.parse::<f64>().ok()?;
                        let s = l.size.parse::<f64>().ok()?;
                        Some((p, s))
                    })
                    .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
                    .unwrap_or((0.0, 0.0));

                let (best_bid, bid_depth) = book
                    .bids
                    .iter()
                    .filter_map(|l| {
                        let p = l.price.parse::<f64>().ok()?;
                        let s = l.size.parse::<f64>().ok()?;
                        Some((p, s))
                    })
                    .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
                    .unwrap_or((0.0, 0.0));

                if best_ask > 0.0 {
                    result.insert(
                        tid.clone(),
                        BookEntry {
                            best_ask, best_bid, ask_depth, bid_depth,
                            ts: now, source: "rest".to_string(),
                        },
                    );
                    // Log REST book fetch
                    let log_entry = BookUpdateLog {
                        token_id: tid.clone(),
                        best_ask, best_bid,
                        spread: best_ask - best_bid,
                        mid: (best_ask + best_bid) / 2.0,
                        ask_depth, bid_depth,
                        ts: now,
                        source: "rest".to_string(),
                        event_type: "batch_fetch".to_string(),
                    };
                    if let Ok(line) = serde_json::to_string(&log_entry) {
                        let mut file = book_log.lock().await;
                        let _ = writeln!(file, "{}", line);
                    }
                }
            }
        }

        if token_ids.len() > params.book_batch_size {
            tokio::time::sleep(Duration::from_millis(params.rest_throttle_ms)).await;
        }
    }

    Ok(result)
}

// ── CL price WebSocket feed ───────────────────────────────────────────────────

pub async fn run_cl_feed(
    live_ws:       String,
    assets:        Vec<String>,
    cl_prices:     ClPrices,
    price_history: PriceHistory,
    cl_log:        LogWriter,
    params:        FeedParams,
    health:        Arc<FeedHealth>,
) {
    loop {
        info!("[CL] Connecting to {}", live_ws);

        match connect_cl_feed(&live_ws, &assets, &cl_prices, &price_history, &cl_log, &params, &health).await {
            Ok(_)  => warn!("[CL] Feed closed cleanly, reconnecting..."),
            Err(e) => error!("[CL] Feed error: {}, reconnecting in {}s", e, params.ws_reconnect_secs),
        }

        tokio::time::sleep(Duration::from_secs(params.ws_reconnect_secs)).await;
    }
}

async fn connect_cl_feed(
    live_ws:       &str,
    assets:        &[String],
    cl_prices:     &ClPrices,
    price_history: &PriceHistory,
    cl_log:        &LogWriter,
    params:        &FeedParams,
    health:        &Arc<FeedHealth>,
) -> Result<()> {
    let mut request = live_ws.into_client_request().context("invalid live WS URL")?;
    request.headers_mut().insert("Origin", "https://polymarket.com".parse().unwrap());
    request.headers_mut().insert("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36".parse().unwrap());
    let (mut ws, _) = connect_async(request).await.context("CL WS connect failed")?;

    // Subscribe to each asset's Chainlink feed
    // filters must be a JSON-stringified object, e.g. "{\"symbol\":\"eth/usd\"}"
    let mut subscriptions = Vec::new();
    for asset in assets {
        let symbol = format!("{}/usd", asset.to_lowercase());
        let filter = serde_json::json!({"symbol": symbol}).to_string();
        subscriptions.push(serde_json::json!({
            "topic": "crypto_prices_chainlink",
            "type": "update",
            "filters": filter
        }));
    }
    let sub = serde_json::json!({
        "action": "subscribe",
        "subscriptions": subscriptions
    });
    ws.send(Message::Text(sub.to_string())).await?;
    debug!("[CL] Subscribed to {} assets", assets.len());

    info!("[CL] Feed connected, {} assets", assets.len());

    let (mut ws_tx, mut ws_rx) = ws.split();
    let mut ping_interval = tokio::time::interval(Duration::from_secs(5));
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        trace!("[CL_RAW] {}", text);
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                            process_cl_message(&v, cl_prices, price_history, cl_log, params, health).await;
                        }
                    }
                    Some(Ok(_)) => {} // pong/ping/binary
                    Some(Err(e)) => return Err(anyhow!("CL WS message error: {}", e)),
                    None => return Ok(()),
                }
            }
            _ = ping_interval.tick() => {
                ws_tx.send(Message::Text("ping".to_string())).await.context("CL WS ping failed")?;
            }
        }
    }
}

async fn process_cl_message(
    v:             &serde_json::Value,
    cl_prices:     &ClPrices,
    price_history: &PriceHistory,
    cl_log:        &LogWriter,
    params:        &FeedParams,
    health:        &Arc<FeedHealth>,
) {
    // RTDS format: {"topic":"crypto_prices_chainlink","type":"update","payload":{"symbol":"eth/usd","timestamp":...,"value":1234.56}}
    let topic = v.get("topic").and_then(|c| c.as_str()).unwrap_or("");
    if topic != "crypto_prices_chainlink" {
        return;
    }

    let payload = match v.get("payload") {
        Some(p) => p,
        None => return,
    };

    // symbol is "eth/usd" → extract "eth"
    let symbol = match payload.get("symbol").and_then(|s| s.as_str()) {
        Some(s) => s.to_lowercase(),
        None    => return,
    };
    let asset = symbol.split('/').next().unwrap_or("").to_string();
    if asset.is_empty() {
        return;
    }

    let price: f64 = match payload.get("value").and_then(|p| p.as_f64()) {
        Some(p) if p > 0.0 => p,
        _ => return,
    };

    let ts = v
        .get("ts")
        .and_then(|t| t.as_f64())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64()
        });

    // Get previous price for delta calculation
    let prev_price = cl_prices.get(&asset).map(|v| v.1).unwrap_or(price);

    cl_prices.insert(asset.clone(), (ts, price));

    // Increment health counter + periodic log
    let count = health.cl_msg_count.fetch_add(1, Ordering::Relaxed) + 1;
    if count % 1000 == 0 {
        info!("[CL_HEALTH] {} messages processed", count);
    }

    // Log every CL price update to JSONL with delta
    {
        let delta = price - prev_price;
        let pct_change = if prev_price > 0.0 { delta / prev_price * 100.0 } else { 0.0 };
        let log_entry = ClPriceLog {
            asset: asset.clone(),
            price,
            prev_price,
            delta,
            pct_change,
            ts,
            source: "ws".to_string(),
        };
        if let Ok(line) = serde_json::to_string(&log_entry) {
            let mut file = cl_log.lock().await;
            let _ = writeln!(file, "{}", line);
        }
    }

    // Append to price history (cap from config)
    let mut hist = price_history.entry(asset).or_default();
    hist.push((ts, price));
    if hist.len() > params.price_history_cap {
        let drain_to = hist.len() - params.price_history_cap;
        hist.drain(0..drain_to);
    }
}

// ── PM book WebSocket feed ────────────────────────────────────────────────────

pub async fn run_book_feed(
    clob_ws:    String,
    token_ids:  Arc<DashMap<String, ()>>,
    book_state: BookState,
    book_live:  Arc<AtomicU64>,
    mut sub_rx: tokio::sync::mpsc::UnboundedReceiver<Vec<String>>,
    book_log:   LogWriter,
    params:     FeedParams,
    health:     Arc<FeedHealth>,
) {
    loop {
        info!("[BOOK] Connecting to {}", clob_ws);

        match connect_book_feed(&clob_ws, &token_ids, &book_state, &book_live, &mut sub_rx, &book_log, &health).await {
            Ok(_)  => warn!("[BOOK] Feed closed cleanly, reconnecting..."),
            Err(e) => error!("[BOOK] Feed error: {}, reconnecting in {}s", e, params.ws_reconnect_secs),
        }

        book_live.store(0, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_secs(params.ws_reconnect_secs)).await;
    }
}

async fn connect_book_feed(
    clob_ws:    &str,
    token_ids:  &DashMap<String, ()>,
    book_state: &BookState,
    book_live:  &Arc<AtomicU64>,
    sub_rx:     &mut tokio::sync::mpsc::UnboundedReceiver<Vec<String>>,
    book_log:   &LogWriter,
    health:     &Arc<FeedHealth>,
) -> Result<()> {
    let mut request = clob_ws.into_client_request().context("invalid CLOB WS URL")?;
    request.headers_mut().insert("Origin", "https://polymarket.com".parse().unwrap());
    request.headers_mut().insert("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36".parse().unwrap());
    let (ws, _) = connect_async(request).await.context("CLOB WS connect failed")?;
    let (mut ws_tx, mut ws_rx) = ws.split();

    // Subscribe all known token IDs using correct Polymarket CLOB WS format
    let ids: Vec<String> = token_ids.iter().map(|e| e.key().clone()).collect();
    if !ids.is_empty() {
        let sub = serde_json::json!({
            "type": "market",
            "assets_ids": ids
        });
        ws_tx.send(Message::Text(sub.to_string())).await?;
        info!("[BOOK] Subscribed to {} token IDs", ids.len());
    }

    let connect_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    book_live.store(connect_ts, Ordering::Relaxed);

    let mut ping_interval = tokio::time::interval(Duration::from_secs(5));
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            // Handle incoming WS messages
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        trace!("[BOOK_RAW] {}", text);
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                            process_book_message(&v, book_state, book_log, health).await;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {} // expected pong response
                    Some(Ok(_)) => {} // ping/binary
                    Some(Err(e)) => return Err(e.into()),
                    None => return Ok(()), // stream closed
                }
            }
            // Handle new token subscription requests
            new_tokens = sub_rx.recv() => {
                if let Some(tokens) = new_tokens {
                    if !tokens.is_empty() {
                        let sub = serde_json::json!({
                            "type": "market",
                            "assets_ids": tokens
                        });
                        ws_tx.send(Message::Text(sub.to_string())).await?;
                        info!("[BOOK] Subscribed {} new tokens", tokens.len());
                    }
                }
            }
            // Keepalive ping every 5s
            _ = ping_interval.tick() => {
                ws_tx.send(Message::Ping(vec![])).await.context("BOOK WS ping failed")?;
            }
        }
    }
}

async fn process_book_message(
    v:          &serde_json::Value,
    book_state: &BookState,
    book_log:   &LogWriter,
    health:     &Arc<FeedHealth>,
) {
    // Polymarket CLOB WS sends:
    // 1. "book" events: full snapshot with asks[] and bids[]
    // 2. "price_change" events: delta with changes[] array
    //    changes format: [{"side":"BUY"|"SELL","price":"0.55","size":"10"}]

    let event_type = v.get("event_type").and_then(|e| e.as_str()).unwrap_or("");
    let asset_id   = match v.get("asset_id").and_then(|a| a.as_str()) {
        Some(id) => id.to_string(),
        None     => return,
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();

    // Increment health counter + periodic log
    let count = health.book_msg_count.fetch_add(1, Ordering::Relaxed) + 1;
    if count % 1000 == 0 {
        info!("[BOOK_HEALTH] {} messages processed", count);
    }

    match event_type {
        "book" => {
            // Full book snapshot — safe to overwrite
            let (best_ask, ask_depth) = extract_best_ask_with_depth(v);
            let (best_bid, bid_depth) = extract_best_bid_with_depth(v);
            if best_ask > 0.0 {
                book_state.insert(asset_id.clone(), BookEntry {
                    best_ask, best_bid, ask_depth, bid_depth,
                    ts: now, source: "ws_book".to_string(),
                });
                // Log book snapshot
                let log_entry = BookUpdateLog {
                    token_id: asset_id, best_ask, best_bid,
                    spread: best_ask - best_bid,
                    mid: (best_ask + best_bid) / 2.0,
                    ask_depth, bid_depth,
                    ts: now,
                    source: "ws_book".to_string(),
                    event_type: "snapshot".to_string(),
                };
                if let Ok(line) = serde_json::to_string(&log_entry) {
                    let mut file = book_log.lock().await;
                    let _ = writeln!(file, "{}", line);
                }
            }
        }
        "price_change" => {
            // Incremental update — merge with existing state
            // PM sends changes[] with side/price/size entries
            if let Some(changes) = v.get("changes").and_then(|c| c.as_array()) {
                // Get existing entry or create default
                let mut entry = book_state
                    .get(&asset_id)
                    .map(|e| e.clone())
                    .unwrap_or(BookEntry {
                        best_ask: 0.0, best_bid: 0.0,
                        ask_depth: 0.0, bid_depth: 0.0,
                        ts: now, source: "ws_delta".to_string(),
                    });

                // Parse all changes to find new best ask/bid
                let mut sell_prices: Vec<(f64, f64)> = Vec::new(); // (price, size)
                let mut buy_prices: Vec<(f64, f64)> = Vec::new();

                for change in changes {
                    let side = change.get("side").and_then(|s| s.as_str()).unwrap_or("");
                    let price = change.get("price")
                        .and_then(|p| p.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    let size = change.get("size")
                        .and_then(|s| s.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);

                    if price <= 0.0 {
                        continue;
                    }

                    // size=0 means level removed, size>0 means level updated
                    match side {
                        "SELL" => {
                            if size > 0.0 {
                                sell_prices.push((price, size));
                            } else if (price - entry.best_ask).abs() < 1e-9 {
                                // Best ask removed — invalidate so REST refresh corrects it
                                entry.best_ask = 0.0;
                                entry.ask_depth = 0.0;
                            }
                        }
                        "BUY" => {
                            if size > 0.0 {
                                buy_prices.push((price, size));
                            } else if (price - entry.best_bid).abs() < 1e-9 {
                                // Best bid removed — invalidate so REST refresh corrects it
                                entry.best_bid = 0.0;
                                entry.bid_depth = 0.0;
                            }
                        }
                        _ => {}
                    }
                }

                // Update best ask: new minimum of existing + new sell levels
                if !sell_prices.is_empty() {
                    let (new_min, new_depth) = sell_prices.iter()
                        .copied()
                        .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
                        .unwrap();
                    if entry.best_ask <= 0.0 || new_min < entry.best_ask {
                        entry.best_ask = new_min;
                        entry.ask_depth = new_depth;
                    } else if (new_min - entry.best_ask).abs() < 1e-9 {
                        entry.ask_depth = new_depth; // same level, update depth
                    }
                }

                // Update best bid: new maximum of existing + new buy levels
                if !buy_prices.is_empty() {
                    let (new_max, new_depth) = buy_prices.iter()
                        .copied()
                        .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
                        .unwrap();
                    if new_max > entry.best_bid {
                        entry.best_bid = new_max;
                        entry.bid_depth = new_depth;
                    } else if (new_max - entry.best_bid).abs() < 1e-9 {
                        entry.bid_depth = new_depth;
                    }
                }

                entry.ts = now;
                entry.source = "ws_delta".to_string();

                // Always write back (including invalidated entries so stale prices don't persist)
                if entry.best_ask > 0.0 {
                    let log_entry = BookUpdateLog {
                        token_id: asset_id.clone(),
                        best_ask: entry.best_ask,
                        best_bid: entry.best_bid,
                        spread: entry.best_ask - entry.best_bid,
                        mid: (entry.best_ask + entry.best_bid) / 2.0,
                        ask_depth: entry.ask_depth,
                        bid_depth: entry.bid_depth,
                        ts: now,
                        source: "ws_delta".to_string(),
                        event_type: "price_change".to_string(),
                    };
                    if let Ok(line) = serde_json::to_string(&log_entry) {
                        let mut file = book_log.lock().await;
                        let _ = writeln!(file, "{}", line);
                    }
                }
                book_state.insert(asset_id, entry);
            }
        }
        _ => {}
    }
}

fn extract_best_ask_with_depth(v: &serde_json::Value) -> (f64, f64) {
    v.get("asks")
        .and_then(|a| a.as_array())
        .and_then(|asks| {
            asks.iter()
                .filter_map(|l| {
                    let p = l.get("price")?.as_str()?.parse::<f64>().ok()?;
                    let s = l.get("size")?.as_str()?.parse::<f64>().ok()?;
                    Some((p, s))
                })
                .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
        })
        .unwrap_or((0.0, 0.0))
}

fn extract_best_bid_with_depth(v: &serde_json::Value) -> (f64, f64) {
    v.get("bids")
        .and_then(|b| b.as_array())
        .and_then(|bids| {
            bids.iter()
                .filter_map(|l| {
                    let p = l.get("price")?.as_str()?.parse::<f64>().ok()?;
                    let s = l.get("size")?.as_str()?.parse::<f64>().ok()?;
                    Some((p, s))
                })
                .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
        })
        .unwrap_or((0.0, 0.0))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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
        let (ask, depth) = extract_best_ask_with_depth(&v);
        assert_eq!(ask, 0.52);
        assert_eq!(depth, 3.0);
    }

    fn test_book_log() -> LogWriter {
        let f = std::fs::OpenOptions::new()
            .create(true).write(true).truncate(true)
            .open(std::env::temp_dir().join("test_book.jsonl")).unwrap();
        Arc::new(Mutex::new(f))
    }

    fn test_health() -> Arc<FeedHealth> {
        Arc::new(FeedHealth::new())
    }

    #[tokio::test]
    async fn price_change_merges_correctly() {
        let book_state: BookState = Arc::new(DashMap::new());
        let log = test_book_log();
        let health = test_health();

        // First: full snapshot
        let snap = serde_json::json!({
            "event_type": "book",
            "asset_id": "token1",
            "asks": [{"price": "0.55", "size": "10"}],
            "bids": [{"price": "0.45", "size": "5"}]
        });
        process_book_message(&snap, &book_state, &log, &health).await;

        let entry = book_state.get("token1").unwrap();
        assert_eq!(entry.best_ask, 0.55);
        assert_eq!(entry.best_bid, 0.45);
        drop(entry);

        // Then: price_change with better ask
        let delta = serde_json::json!({
            "event_type": "price_change",
            "asset_id": "token1",
            "changes": [
                {"side": "SELL", "price": "0.52", "size": "3"}
            ]
        });
        process_book_message(&delta, &book_state, &log, &health).await;

        let entry = book_state.get("token1").unwrap();
        assert_eq!(entry.best_ask, 0.52, "ask should improve to 0.52");
        assert_eq!(entry.best_bid, 0.45, "bid should be preserved");
    }

    #[tokio::test]
    async fn price_change_bid_only_preserves_ask() {
        let book_state: BookState = Arc::new(DashMap::new());
        let log = test_book_log();
        let health = test_health();

        // Snapshot first
        let snap = serde_json::json!({
            "event_type": "book",
            "asset_id": "token2",
            "asks": [{"price": "0.60", "size": "10"}],
            "bids": [{"price": "0.40", "size": "5"}]
        });
        process_book_message(&snap, &book_state, &log, &health).await;

        // Bid-only delta
        let delta = serde_json::json!({
            "event_type": "price_change",
            "asset_id": "token2",
            "changes": [
                {"side": "BUY", "price": "0.42", "size": "8"}
            ]
        });
        process_book_message(&delta, &book_state, &log, &health).await;

        let entry = book_state.get("token2").unwrap();
        assert_eq!(entry.best_ask, 0.60, "ask should be preserved");
        assert_eq!(entry.best_bid, 0.42, "bid should improve to 0.42");
    }

    #[tokio::test]
    async fn best_ask_invalidated_on_removal() {
        let book_state: BookState = Arc::new(DashMap::new());
        let log = test_book_log();
        let health = test_health();

        // Snapshot: best_ask=0.55
        let snap = serde_json::json!({
            "event_type": "book",
            "asset_id": "token3",
            "asks": [{"price": "0.55", "size": "10"}],
            "bids": [{"price": "0.45", "size": "5"}]
        });
        process_book_message(&snap, &book_state, &log, &health).await;

        // Remove the best ask level (size=0)
        let delta = serde_json::json!({
            "event_type": "price_change",
            "asset_id": "token3",
            "changes": [
                {"side": "SELL", "price": "0.55", "size": "0"}
            ]
        });
        process_book_message(&delta, &book_state, &log, &health).await;

        // best_ask should be invalidated (0.0) — no phantom price
        let entry = book_state.get("token3");
        // Entry might be removed (best_ask=0) or absent
        match entry {
            Some(e) => assert_eq!(e.best_ask, 0.0, "best_ask should be invalidated on removal"),
            None => {} // also acceptable — entry not stored when best_ask=0
        }
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
}
