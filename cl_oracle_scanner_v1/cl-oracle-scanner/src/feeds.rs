/// feeds.rs — All external data feeds
///
/// Three feeds:
/// 1. CL price feed   : WebSocket from Polymarket live-data (chainlink topic)
/// 2. PM book feed    : WebSocket from Polymarket CLOB subscriptions
/// 3. Market discovery: Batched REST via Gamma API (slug → token IDs)
///
/// Batching rules:
/// - Book REST requests: max BOOK_BATCH_SIZE token IDs per request
/// - Minimum REST_THROTTLE_MS between any two REST calls
/// - WebSocket feeds are event-driven — no polling

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};
use url::Url;

// ── Constants ────────────────────────────────────────────────────────────────

const BOOK_BATCH_SIZE:   usize = 20;
const REST_THROTTLE_MS:  u64   = 500;
const WS_RECONNECT_SECS: u64   = 5;

// ── Shared state types ────────────────────────────────────────────────────────

/// CL oracle prices: asset → (unix_ts, price)
pub type ClPrices = Arc<DashMap<String, (f64, f64)>>;

/// PM order book: token_id → BookEntry
pub type BookState = Arc<DashMap<String, BookEntry>>;

/// CL price history for sigma: asset → Vec<(ts, price)>
pub type PriceHistory = Arc<DashMap<String, Vec<(f64, f64)>>>;

#[derive(Debug, Clone)]
pub struct BookEntry {
    pub best_ask: f64,
    pub best_bid: f64,
    pub ts:       f64,
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
    client:   &Client,
    clob_rest: &str,
    token_ids: &[String],
    limiter:   &RateLimiter,
) -> Result<HashMap<String, BookEntry>> {
    if token_ids.is_empty() {
        return Ok(HashMap::new());
    }

    limiter.wait().await;

    let mut result = HashMap::new();

    for chunk in token_ids.chunks(BOOK_BATCH_SIZE) {
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
                let best_ask = book
                    .asks
                    .iter()
                    .filter_map(|l| l.price.parse::<f64>().ok())
                    .reduce(f64::min)
                    .unwrap_or(0.0);

                let best_bid = book
                    .bids
                    .iter()
                    .filter_map(|l| l.price.parse::<f64>().ok())
                    .reduce(f64::max)
                    .unwrap_or(0.0);

                if best_ask > 0.0 {
                    result.insert(
                        tid.clone(),
                        BookEntry { best_ask, best_bid, ts: now },
                    );
                }
            }
        }

        if token_ids.len() > BOOK_BATCH_SIZE {
            tokio::time::sleep(Duration::from_millis(REST_THROTTLE_MS)).await;
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
) {
    loop {
        info!("[CL] Connecting to {}", live_ws);

        match connect_cl_feed(&live_ws, &assets, &cl_prices, &price_history).await {
            Ok(_)  => warn!("[CL] Feed closed cleanly, reconnecting..."),
            Err(e) => error!("[CL] Feed error: {}, reconnecting in {}s", e, WS_RECONNECT_SECS),
        }

        tokio::time::sleep(Duration::from_secs(WS_RECONNECT_SECS)).await;
    }
}

async fn connect_cl_feed(
    live_ws:       &str,
    assets:        &[String],
    cl_prices:     &ClPrices,
    price_history: &PriceHistory,
) -> Result<()> {
    let url = Url::parse(live_ws).context("invalid live WS URL")?;
    let (mut ws, _) = connect_async(url).await.context("CL WS connect failed")?;

    for asset in assets {
        let symbol = format!("{}usd", asset.to_uppercase());
        let sub = serde_json::json!({
            "type": "subscribe",
            "channel": "crypto_prices_chainlink",
            "symbol": symbol
        });
        ws.send(Message::Text(sub.to_string())).await?;
        debug!("[CL] Subscribed to {}", symbol);
    }

    info!("[CL] Feed connected, {} assets", assets.len());

    while let Some(msg) = ws.next().await {
        let msg = msg.context("CL WS message error")?;
        if let Message::Text(text) = msg {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                process_cl_message(&v, cl_prices, price_history);
            }
        }
    }

    Ok(())
}

fn process_cl_message(
    v:             &serde_json::Value,
    cl_prices:     &ClPrices,
    price_history: &PriceHistory,
) {
    let channel = v.get("channel").and_then(|c| c.as_str()).unwrap_or("");
    if channel != "crypto_prices_chainlink" {
        return;
    }

    let symbol = match v.get("symbol").and_then(|s| s.as_str()) {
        Some(s) => s.to_lowercase(),
        None    => return,
    };

    // "btcusd" → "btc"
    let asset = symbol.trim_end_matches("usd").to_string();

    let price_str = v.get("price").and_then(|p| p.as_str()).unwrap_or("0");
    let price: f64 = match price_str.parse() {
        Ok(p) if p > 0.0 => p,
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

    cl_prices.insert(asset.clone(), (ts, price));

    // Append to price history (cap at 1000 entries per asset)
    let mut hist = price_history.entry(asset).or_default();
    hist.push((ts, price));
    if hist.len() > 1000 {
        let drain_to = hist.len() - 1000;
        hist.drain(0..drain_to);
    }
}

// ── PM book WebSocket feed ────────────────────────────────────────────────────

pub async fn run_book_feed(
    clob_ws:    String,
    token_ids:  Arc<DashMap<String, ()>>,
    book_state: BookState,
    book_live:  Arc<std::sync::atomic::AtomicU64>,
    mut sub_rx: tokio::sync::mpsc::UnboundedReceiver<Vec<String>>,
) {
    loop {
        info!("[BOOK] Connecting to {}", clob_ws);

        match connect_book_feed(&clob_ws, &token_ids, &book_state, &book_live, &mut sub_rx).await {
            Ok(_)  => warn!("[BOOK] Feed closed cleanly, reconnecting..."),
            Err(e) => error!("[BOOK] Feed error: {}, reconnecting in {}s", e, WS_RECONNECT_SECS),
        }

        book_live.store(0, std::sync::atomic::Ordering::Relaxed);
        tokio::time::sleep(Duration::from_secs(WS_RECONNECT_SECS)).await;
    }
}

async fn connect_book_feed(
    clob_ws:    &str,
    token_ids:  &DashMap<String, ()>,
    book_state: &BookState,
    book_live:  &Arc<std::sync::atomic::AtomicU64>,
    sub_rx:     &mut tokio::sync::mpsc::UnboundedReceiver<Vec<String>>,
) -> Result<()> {
    let url = Url::parse(clob_ws).context("invalid CLOB WS URL")?;
    let (ws, _) = connect_async(url).await.context("CLOB WS connect failed")?;
    let (mut ws_tx, mut ws_rx) = ws.split();

    // Subscribe all known token IDs
    let ids: Vec<String> = token_ids.iter().map(|e| e.key().clone()).collect();
    if !ids.is_empty() {
        let sub = serde_json::json!({
            "auth": {},
            "type": "subscribe",
            "markets": ids
        });
        ws_tx.send(Message::Text(sub.to_string())).await?;
        info!("[BOOK] Subscribed to {} token IDs", ids.len());
    }

    let connect_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    book_live.store(connect_ts, std::sync::atomic::Ordering::Relaxed);

    loop {
        tokio::select! {
            // Handle incoming WS messages
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                            process_book_message(&v, book_state);
                        }
                    }
                    Some(Ok(_)) => {} // ping/pong/binary
                    Some(Err(e)) => return Err(e.into()),
                    None => return Ok(()), // stream closed
                }
            }
            // Handle new token subscription requests
            new_tokens = sub_rx.recv() => {
                if let Some(tokens) = new_tokens {
                    if !tokens.is_empty() {
                        let sub = serde_json::json!({
                            "auth": {},
                            "type": "subscribe",
                            "markets": tokens
                        });
                        ws_tx.send(Message::Text(sub.to_string())).await?;
                        info!("[BOOK] Subscribed {} new tokens", tokens.len());
                    }
                }
            }
        }
    }
}

fn process_book_message(v: &serde_json::Value, book_state: &BookState) {
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

    match event_type {
        "book" => {
            // Full book snapshot — safe to overwrite
            let best_ask = extract_best_ask(v);
            let best_bid = extract_best_bid(v);
            if best_ask > 0.0 {
                book_state.insert(asset_id, BookEntry { best_ask, best_bid, ts: now });
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
                    .unwrap_or(BookEntry { best_ask: 0.0, best_bid: 0.0, ts: now });

                // Parse all changes to find new best ask/bid
                let mut sell_prices: Vec<f64> = Vec::new();
                let mut buy_prices: Vec<f64> = Vec::new();

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
                                sell_prices.push(price);
                            }
                            // If size=0 and this was our best ask, we can't know the new best
                            // without the full book. REST fallback will correct this.
                        }
                        "BUY" => {
                            if size > 0.0 {
                                buy_prices.push(price);
                            }
                        }
                        _ => {}
                    }
                }

                // Update best ask: new minimum of existing + new sell levels
                if !sell_prices.is_empty() {
                    let new_min = sell_prices.iter().copied().reduce(f64::min).unwrap();
                    if entry.best_ask <= 0.0 || new_min < entry.best_ask {
                        entry.best_ask = new_min;
                    }
                }

                // Update best bid: new maximum of existing + new buy levels
                if !buy_prices.is_empty() {
                    let new_max = buy_prices.iter().copied().reduce(f64::max).unwrap();
                    if new_max > entry.best_bid {
                        entry.best_bid = new_max;
                    }
                }

                entry.ts = now;
                if entry.best_ask > 0.0 {
                    book_state.insert(asset_id, entry);
                }
            }
        }
        _ => {}
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
        assert_eq!(extract_best_ask(&v), 0.52);
    }

    #[test]
    fn price_change_merges_correctly() {
        let book_state: BookState = Arc::new(DashMap::new());

        // First: full snapshot
        let snap = serde_json::json!({
            "event_type": "book",
            "asset_id": "token1",
            "asks": [{"price": "0.55", "size": "10"}],
            "bids": [{"price": "0.45", "size": "5"}]
        });
        process_book_message(&snap, &book_state);

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
        process_book_message(&delta, &book_state);

        let entry = book_state.get("token1").unwrap();
        assert_eq!(entry.best_ask, 0.52, "ask should improve to 0.52");
        assert_eq!(entry.best_bid, 0.45, "bid should be preserved");
    }

    #[test]
    fn price_change_bid_only_preserves_ask() {
        let book_state: BookState = Arc::new(DashMap::new());

        // Snapshot first
        let snap = serde_json::json!({
            "event_type": "book",
            "asset_id": "token2",
            "asks": [{"price": "0.60", "size": "10"}],
            "bids": [{"price": "0.40", "size": "5"}]
        });
        process_book_message(&snap, &book_state);

        // Bid-only delta
        let delta = serde_json::json!({
            "event_type": "price_change",
            "asset_id": "token2",
            "changes": [
                {"side": "BUY", "price": "0.42", "size": "8"}
            ]
        });
        process_book_message(&delta, &book_state);

        let entry = book_state.get("token2").unwrap();
        assert_eq!(entry.best_ask, 0.60, "ask should be preserved");
        assert_eq!(entry.best_bid, 0.42, "bid should improve to 0.42");
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
