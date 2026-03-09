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

// -- Constants ----------------------------------------------------------------

const BOOK_BATCH_SIZE:   usize = 20;
const REST_THROTTLE_MS:  u64   = 500;
const WS_RECONNECT_SECS: u64   = 5;

// -- Shared state types -------------------------------------------------------

/// CL oracle prices: asset → (unix_ts, price)
pub type ClPrices = Arc<DashMap<String, (f64, f64)>>;

/// PM order book: token_id → BookEntry (with full level tracking)
pub type BookState = Arc<DashMap<String, BookEntry>>;

/// CL price history for sigma: asset → Vec<(ts, price)>
pub type PriceHistory = Arc<DashMap<String, Vec<(f64, f64)>>>;

/// Full order book entry — maintains all levels for correct delta application.
///
/// The old code only stored best_ask/best_bid. When a `price_change` delta
/// arrived, it treated the delta levels AS the entire book, overwriting state.
/// A delta with 1 ask level would replace a 50-level book, corrupting prices.
///
/// Now: `asks` and `bids` store all known levels. Deltas update individual
/// levels (size=0 removes, size>0 inserts/updates). best_ask/best_bid are
/// recomputed from the full book after every update.
#[derive(Debug, Clone)]
pub struct BookEntry {
    pub best_ask: f64,
    pub best_bid: f64,
    pub ts:       f64,
    asks: Vec<(f64, f64)>,  // (price, size) — sorted ascending by price
    bids: Vec<(f64, f64)>,  // (price, size) — sorted descending by price
}

impl BookEntry {
    pub fn new() -> Self {
        Self { best_ask: 0.0, best_bid: 0.0, ts: 0.0, asks: Vec::new(), bids: Vec::new() }
    }

    /// Replace entire book from a full snapshot (REST or WS "book" event)
    pub fn set_snapshot(&mut self, mut asks: Vec<(f64, f64)>, mut bids: Vec<(f64, f64)>, ts: f64) {
        asks.retain(|(_, s)| *s > 0.0);
        asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        bids.retain(|(_, s)| *s > 0.0);
        bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        self.asks = asks;
        self.bids = bids;
        self.ts   = ts;
        self.recompute_best();
    }

    /// Apply a single level delta: size=0 removes the level, size>0 upserts
    pub fn apply_level(&mut self, price: f64, size: f64, is_ask: bool, ts: f64) {
        let levels = if is_ask { &mut self.asks } else { &mut self.bids };

        // Remove existing level at this price (if any)
        levels.retain(|(p, _)| (*p - price).abs() > 1e-10);

        if size > 0.0 {
            levels.push((price, size));
            if is_ask {
                levels.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            } else {
                levels.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
            }
        }

        self.ts = ts;
        self.recompute_best();
    }

    fn recompute_best(&mut self) {
        self.best_ask = self.asks.first().map(|(p, _)| *p).unwrap_or(0.0);
        self.best_bid = self.bids.first().map(|(p, _)| *p).unwrap_or(0.0);
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
}

// -- Gamma API response types -------------------------------------------------

#[derive(Deserialize, Debug)]
struct GammaEvent {
    markets: Vec<GammaMarket>,
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

/// Deserialize a field that may be either a JSON array or a stringified JSON array.
/// e.g. both `["a","b"]` and `"[\"a\",\"b\"]"` → Some(vec!["a","b"])
fn deserialize_string_or_vec<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct StringOrVec;

    impl<'de> de::Visitor<'de> for StringOrVec {
        type Value = Option<Vec<String>>;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a string or array of strings")
        }

        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_str<E: de::Error>(self, s: &str) -> Result<Self::Value, E> {
            // Try parsing as JSON array
            serde_json::from_str::<Vec<String>>(s)
                .map(Some)
                .map_err(de::Error::custom)
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut v = Vec::new();
            while let Some(item) = seq.next_element::<String>()? {
                v.push(item);
            }
            Ok(Some(v))
        }
    }

    deserializer.deserialize_any(StringOrVec)
}

// -- CLOB REST book response --------------------------------------------------

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

// -- Rate limiter -------------------------------------------------------------

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

// -- Market discovery ---------------------------------------------------------

pub fn build_slug(asset: &str, tf_mins: u32, window_start: u64) -> String {
    format!("{}-updown-{}m-{}", asset, tf_mins, window_start)
}

pub fn current_window_starts(tf_mins: u32, now_secs: u64) -> Vec<u64> {
    let interval = (tf_mins as u64) * 60;
    let current  = (now_secs / interval) * interval;
    vec![current, current + interval]
}

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

    let text = resp.text().await.context("gamma API read body failed")?;

    let event: GammaEvent = match serde_json::from_str(&text) {
        Ok(e) => e,
        Err(e) => {
            // Try parsing as array — some endpoints return [event] instead of event
            if let Ok(mut arr) = serde_json::from_str::<Vec<GammaEvent>>(&text) {
                if let Some(ev) = arr.pop() {
                    ev
                } else {
                    return Ok(None);
                }
            } else {
                warn!("gamma parse error: {}  body[..200]: {}", e, &text[..text.len().min(200)]);
                return Err(anyhow::anyhow!("gamma API JSON parse failed: {}", e));
            }
        }
    };

    // Find market with Up/Down or Yes/No outcomes
    let market = event.markets.iter().find(|m| {
        m.outcomes
            .as_ref()
            .map(|o| o.iter().any(|x| {
                x.eq_ignore_ascii_case("yes") || x.eq_ignore_ascii_case("up")
            }))
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

    // Support both Yes/No and Up/Down outcome labels
    let yes_idx = outcomes
        .iter()
        .position(|o| o.eq_ignore_ascii_case("yes") || o.eq_ignore_ascii_case("up"))
        .ok_or_else(|| anyhow!("no YES/Up outcome"))?;
    let no_idx = outcomes
        .iter()
        .position(|o| o.eq_ignore_ascii_case("no") || o.eq_ignore_ascii_case("down"))
        .ok_or_else(|| anyhow!("no NO/Down outcome"))?;

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

// -- Batch book fetcher -------------------------------------------------------

/// Fetch order books for a batch of token IDs via REST.
/// Returns full BookEntry with all levels (not just best ask/bid).
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
                let asks: Vec<(f64, f64)> = book.asks.iter()
                    .filter_map(|l| {
                        let p = l.price.parse::<f64>().ok()?;
                        let s = l.size.parse::<f64>().ok()?;
                        Some((p, s))
                    })
                    .collect();

                let bids: Vec<(f64, f64)> = book.bids.iter()
                    .filter_map(|l| {
                        let p = l.price.parse::<f64>().ok()?;
                        let s = l.size.parse::<f64>().ok()?;
                        Some((p, s))
                    })
                    .collect();

                let mut entry = BookEntry::new();
                entry.set_snapshot(asks, bids, now);

                if entry.best_ask > 0.0 {
                    result.insert(tid.clone(), entry);
                }
            }
        }

        if token_ids.len() > BOOK_BATCH_SIZE {
            tokio::time::sleep(Duration::from_millis(REST_THROTTLE_MS)).await;
        }
    }

    Ok(result)
}

// -- CL price WebSocket feed --------------------------------------------------

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

    let mut hist = price_history.entry(asset).or_default();
    hist.push((ts, price));
    if hist.len() > 1000 {
        let drain_to = hist.len() - 1000;
        hist.drain(0..drain_to);
    }
}

// -- PM book WebSocket feed ---------------------------------------------------

pub async fn run_book_feed(
    clob_ws:    String,
    token_ids:  Arc<DashMap<String, ()>>,
    book_state: BookState,
    book_live:  Arc<std::sync::atomic::AtomicU64>,
) {
    loop {
        info!("[BOOK] Connecting to {}", clob_ws);

        match connect_book_feed(&clob_ws, &token_ids, &book_state, &book_live).await {
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
) -> Result<()> {
    let url = Url::parse(clob_ws).context("invalid CLOB WS URL")?;
    let (mut ws, _) = connect_async(url).await.context("CLOB WS connect failed")?;

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
            // Full book snapshot — replace entire book
            let asks = parse_levels(v.get("asks"));
            let bids = parse_levels(v.get("bids"));

            let mut entry = BookEntry::new();
            entry.set_snapshot(asks, bids, now);

            if entry.best_ask > 0.0 {
                book_state.insert(asset_id, entry);
            }
        }
        "price_change" => {
            // Incremental delta — apply individual level changes to existing book.
            //
            // OLD BUG: treated delta levels as a full snapshot, overwriting the
            // entire book. A delta with 1 ask level would destroy a 50-level book.
            //
            // FIX: get or create the existing book, apply each change individually.
            // PM WS sends changes as: {"changes":[{"price":"0.55","size":"10","side":"SELL"},...]
            // where side "SELL" = ask, "BUY" = bid, size "0" = remove level.
            // Also handle asks/bids arrays if present (some PM WS versions).

            let mut entry = book_state
                .get(&asset_id)
                .map(|e| e.clone())
                .unwrap_or_else(BookEntry::new);

            let mut applied = false;

            // Handle "changes" array format
            if let Some(changes) = v.get("changes").and_then(|c| c.as_array()) {
                for change in changes {
                    let price = change.get("price")
                        .and_then(|p| p.as_str())
                        .and_then(|s| s.parse::<f64>().ok());
                    let size = change.get("size")
                        .and_then(|s| s.as_str())
                        .and_then(|s| s.parse::<f64>().ok());
                    let side = change.get("side")
                        .and_then(|s| s.as_str())
                        .unwrap_or("");

                    if let (Some(price), Some(size)) = (price, size) {
                        let is_ask = side.eq_ignore_ascii_case("SELL")
                                  || side.eq_ignore_ascii_case("ASK");
                        entry.apply_level(price, size, is_ask, now);
                        applied = true;
                    }
                }
            }

            // Also handle direct asks/bids arrays in delta (some formats)
            if let Some(asks) = v.get("asks").and_then(|a| a.as_array()) {
                for level in asks {
                    if let (Some(p), Some(s)) = (
                        level.get("price").and_then(|p| p.as_str()).and_then(|s| s.parse::<f64>().ok()),
                        level.get("size").and_then(|s| s.as_str()).and_then(|s| s.parse::<f64>().ok()),
                    ) {
                        entry.apply_level(p, s, true, now);
                        applied = true;
                    }
                }
            }

            if let Some(bids) = v.get("bids").and_then(|b| b.as_array()) {
                for level in bids {
                    if let (Some(p), Some(s)) = (
                        level.get("price").and_then(|p| p.as_str()).and_then(|s| s.parse::<f64>().ok()),
                        level.get("size").and_then(|s| s.as_str()).and_then(|s| s.parse::<f64>().ok()),
                    ) {
                        entry.apply_level(p, s, false, now);
                        applied = true;
                    }
                }
            }

            if applied && entry.best_ask > 0.0 {
                book_state.insert(asset_id, entry);
            }
        }
        _ => {}
    }
}

/// Parse price levels from a JSON asks/bids array
fn parse_levels(val: Option<&serde_json::Value>) -> Vec<(f64, f64)> {
    val.and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|l| {
                    let p = l.get("price")?.as_str()?.parse::<f64>().ok()?;
                    let s = l.get("size")?.as_str()?.parse::<f64>().ok()?;
                    Some((p, s))
                })
                .collect()
        })
        .unwrap_or_default()
}

// -- Tests --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_slug_correct() {
        assert_eq!(build_slug("btc", 5, 1772788200), "btc-updown-5m-1772788200");
        assert_eq!(build_slug("eth", 15, 1772788200), "eth-updown-15m-1772788200");
    }

    #[test]
    fn current_window_starts_5m() {
        let starts = current_window_starts(5, 1772788230);
        assert_eq!(starts[0], 1772788200);
        assert_eq!(starts[1], 1772788500);
    }

    #[test]
    fn book_entry_snapshot() {
        let mut entry = BookEntry::new();
        entry.set_snapshot(
            vec![(0.55, 10.0), (0.60, 5.0), (0.52, 3.0)],
            vec![(0.48, 8.0), (0.45, 20.0), (0.50, 5.0)],
            1000.0,
        );
        assert_eq!(entry.best_ask, 0.52);
        assert_eq!(entry.best_bid, 0.50);
    }

    #[test]
    fn book_entry_delta_updates() {
        let mut entry = BookEntry::new();
        entry.set_snapshot(
            vec![(0.55, 10.0), (0.52, 3.0)],
            vec![(0.48, 8.0), (0.50, 5.0)],
            1000.0,
        );
        assert_eq!(entry.best_ask, 0.52);

        // Remove best ask (size=0)
        entry.apply_level(0.52, 0.0, true, 1001.0);
        assert_eq!(entry.best_ask, 0.55, "after removing 0.52, best ask should be 0.55");

        // Add a better ask
        entry.apply_level(0.51, 5.0, true, 1002.0);
        assert_eq!(entry.best_ask, 0.51);

        // Update bid
        entry.apply_level(0.49, 10.0, false, 1003.0);
        assert_eq!(entry.best_bid, 0.50, "0.50 still best bid");

        // Remove best bid
        entry.apply_level(0.50, 0.0, false, 1004.0);
        assert_eq!(entry.best_bid, 0.49);
    }

    #[test]
    fn book_entry_delta_does_not_corrupt() {
        // Simulate the old bug: price_change with 1 ask level shouldn't destroy the book
        let mut entry = BookEntry::new();
        entry.set_snapshot(
            vec![(0.52, 3.0), (0.55, 10.0), (0.60, 20.0)],
            vec![(0.50, 5.0), (0.48, 8.0)],
            1000.0,
        );
        assert_eq!(entry.asks.len(), 3);
        assert_eq!(entry.bids.len(), 2);

        // Delta: update one ask level
        entry.apply_level(0.55, 15.0, true, 1001.0);
        assert_eq!(entry.asks.len(), 3, "should still have 3 ask levels");
        assert_eq!(entry.bids.len(), 2, "bids untouched");
        assert_eq!(entry.best_ask, 0.52, "best ask unchanged");
    }

    #[test]
    fn process_book_snapshot() {
        let book_state: BookState = Arc::new(DashMap::new());
        let v = serde_json::json!({
            "event_type": "book",
            "asset_id": "token123",
            "asks": [
                {"price": "0.55", "size": "10"},
                {"price": "0.60", "size": "5"},
                {"price": "0.52", "size": "3"}
            ],
            "bids": [
                {"price": "0.48", "size": "8"},
                {"price": "0.50", "size": "5"}
            ]
        });
        process_book_message(&v, &book_state);
        let entry = book_state.get("token123").unwrap();
        assert_eq!(entry.best_ask, 0.52);
        assert_eq!(entry.best_bid, 0.50);
    }

    #[test]
    fn process_price_change_delta() {
        let book_state: BookState = Arc::new(DashMap::new());

        // First: full snapshot
        let snap = serde_json::json!({
            "event_type": "book",
            "asset_id": "token123",
            "asks": [{"price": "0.52", "size": "3"}, {"price": "0.55", "size": "10"}],
            "bids": [{"price": "0.50", "size": "5"}, {"price": "0.48", "size": "8"}]
        });
        process_book_message(&snap, &book_state);

        // Then: price_change delta — remove best ask, add new bid
        let delta = serde_json::json!({
            "event_type": "price_change",
            "asset_id": "token123",
            "changes": [
                {"price": "0.52", "size": "0", "side": "SELL"},
                {"price": "0.49", "size": "12", "side": "BUY"}
            ]
        });
        process_book_message(&delta, &book_state);

        let entry = book_state.get("token123").unwrap();
        assert_eq!(entry.best_ask, 0.55, "best ask should now be 0.55 after removing 0.52");
        assert_eq!(entry.best_bid, 0.50, "best bid still 0.50");
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
