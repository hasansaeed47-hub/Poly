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

// ── Constants (overridden by config in main) ──────────────────────────────────

const BOOK_BATCH_SIZE:   usize = 20;
const REST_THROTTLE_MS:  u64   = 500;
const WS_RECONNECT_SECS: u64   = 5;

// ── Shared state types ────────────────────────────────────────────────────────

/// CL oracle prices: asset → (unix_ts, price)
pub type ClPrices = Arc<DashMap<String, (f64, f64)>>;

/// PM order book: token_id → (best_ask, best_bid, timestamp)
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
    condition_id:   Option<String>,
    clob_token_ids: Option<Vec<String>>,
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
    // Return current and next window (next may not have markets yet but safe to try)
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
        return Ok(None); // market doesn't exist yet
    }

    let event: GammaEvent = resp
        .json()
        .await
        .context("gamma API JSON parse failed")?;

    // Find the market with Up/Down outcomes
    let market = event.markets.iter().find(|m| {
        m.outcomes
            .as_ref()
            .map(|o| o.iter().any(|x| x.eq_ignore_ascii_case("up")))
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

    // Map Up/Down to token IDs
    let yes_idx = outcomes
        .iter()
        .position(|o| o.eq_ignore_ascii_case("up"))
        .ok_or_else(|| anyhow!("no Up outcome"))?;
    let no_idx = outcomes
        .iter()
        .position(|o| o.eq_ignore_ascii_case("down"))
        .ok_or_else(|| anyhow!("no Down outcome"))?;

    let token_yes = tokens[yes_idx].clone();
    let token_no  = tokens[no_idx].clone();

    // Parse window times
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
        open_price: 0.0, // filled in by CL feed at window_start
    }))
}

// ── Batch book fetcher ────────────────────────────────────────────────────────

/// Fetch order books for a batch of token IDs in a single REST request.
/// Polymarket CLOB supports ?token_id=X&token_id=Y... multi-token queries.
/// Returns map of token_id → BookEntry
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

    // Build query: /books?token_id=X&token_id=Y
    // Split into batches of BOOK_BATCH_SIZE
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

        // Response is array of books, one per token_id, same order as request
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

        // Throttle between batch chunks
        if token_ids.len() > BOOK_BATCH_SIZE {
            tokio::time::sleep(Duration::from_millis(REST_THROTTLE_MS)).await;
        }
    }

    Ok(result)
}

// ── CL price WebSocket feed ───────────────────────────────────────────────────

/// Spawn a task that maintains a CL price WebSocket connection.
/// Writes to cl_prices: asset → (ts, price)
/// Writes to price_history: asset → Vec<(ts, price)>
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

    // Subscribe to chainlink topic for each asset
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
    // Expected: {"channel":"crypto_prices_chainlink","symbol":"BTCUSD","price":"67321.5","ts":1234567890}
    let channel = v.get("channel").and_then(|c| c.as_str()).unwrap_or("");
    if channel != "crypto_prices_chainlink" {
        return;
    }

    let symbol = match v.get("symbol").and_then(|s| s.as_str()) {
        Some(s) => s.to_lowercase(),
        None    => return,
    };

    // "BTCUSD" → "btc"
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

/// Spawn a task that maintains a PM CLOB book WebSocket subscription.
/// Subscribes to all token IDs in `token_ids`.
/// Updates book_state: token_id → BookEntry
pub async fn run_book_feed(
    clob_ws:    String,
    token_ids:  Arc<DashMap<String, ()>>, // set of token IDs to subscribe
    book_state: BookState,
    book_live:  Arc<std::sync::atomic::AtomicU64>,
) {
    loop {
        info!("[BOOK] Connecting to {}", clob_ws);

        match connect_book_feed(&clob_ws, &token_ids, &book_state, &book_live).await {
            Ok(_)  => warn!("[BOOK] Feed closed cleanly, reconnecting..."),
            Err(e) => error!("[BOOK] Feed error: {}, reconnecting in {}s", e, WS_RECONNECT_SECS),
        }

        // Reset live timestamp on disconnect
        book_live.store(0, std::sync::atomic::Ordering::Relaxed);
        tokio::time::sleep(Duration::from_secs(WS_RECONNECT_SECS)).await;
    }
}

async fn connect_book_feed(
    clob_ws:    &str,
    token_ids:  &DashMap<String, ()>,
    book_state: &BookState,
    book_live:  &Arc<std::sync::atomic::AtomicU64>,
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
        info!("[BOOK] Subscribed to {} token IDs", ids.len());
    }

    // Record connect time for warmup gate
    let connect_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    book_live.store(connect_ts, std::sync::atomic::Ordering::Relaxed);

    while let Some(msg) = ws.next().await {
        let msg = msg.context("CLOB WS message error")?;
        if let Message::Text(text) = msg {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                process_book_message(&v, book_state);
            }
        }
    }

    Ok(())
}

fn process_book_message(v: &serde_json::Value, book_state: &BookState) {
    // Polymarket CLOB WS sends book snapshots and delta updates
    // Format: {"event_type":"book","asset_id":"<token_id>","asks":[...],"bids":[...]}
    // Or:     {"event_type":"price_change","asset_id":"...","changes":[...]}

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
            // Full book snapshot
            let best_ask = extract_best_ask(v);
            let best_bid = extract_best_bid(v);
            if best_ask > 0.0 {
                book_state.insert(asset_id, BookEntry { best_ask, best_bid, ts: now });
            }
        }
        "price_change" => {
            // Incremental update — recompute best from changes
            // For simplicity: treat as full update if ask/bid present
            let best_ask = extract_best_ask(v);
            let best_bid = extract_best_bid(v);
            if best_ask > 0.0 {
                book_state.insert(asset_id, BookEntry { best_ask, best_bid, ts: now });
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
        // 1772788230 = some ts that is 30s into a 5m window
        // window start should be 1772788200 (rounded down to 300s boundary)
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
    fn batch_book_url_construction() {
        // Verify we can construct multi-token URLs without panic
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
