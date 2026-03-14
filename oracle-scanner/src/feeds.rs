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
pub struct PriceLevel {
    pub price: f64,
    pub size:  f64,
}

#[derive(Debug, Clone)]
pub struct BookEntry {
    pub asks:     Vec<PriceLevel>,  // sorted ascending by price
    pub bids:     Vec<PriceLevel>,  // sorted descending by price
    pub best_ask: f64,
    pub best_bid: f64,
    pub ts:       f64,
}

/// Walk the book and compute VWAP fill price for a given USD stake.
/// `levels` should be sorted: asks ascending, bids descending.
/// Returns (avg_fill_price, shares_filled) or None if insufficient liquidity.
pub fn vwap_fill(levels: &[PriceLevel], stake: f64) -> Option<(f64, f64)> {
    if levels.is_empty() || stake <= 0.0 {
        return None;
    }
    let mut budget = stake;
    let mut total_shares = 0.0;
    let mut total_cost   = 0.0;

    for level in levels {
        if budget <= 0.001 { break; }
        if level.price <= 0.0 || level.size <= 0.0 { continue; }

        // How many shares we can buy/sell at this level
        let max_shares = level.size;
        let shares_wanted = budget / level.price;
        let shares = shares_wanted.min(max_shares);

        let cost = shares * level.price;
        total_shares += shares;
        total_cost   += cost;
        budget       -= cost;
    }

    if total_shares <= 0.0 || total_cost <= 0.0 {
        return None;
    }

    Some((total_cost / total_shares, total_shares))
}

/// Compute VWAP sell price for a given number of shares on the bid side.
/// `bids` should be sorted descending by price.
/// Returns avg_fill_price or None if insufficient liquidity.
#[allow(dead_code)]
pub fn vwap_sell(bids: &[PriceLevel], shares_to_sell: f64) -> Option<f64> {
    if bids.is_empty() || shares_to_sell <= 0.0 {
        return None;
    }
    let mut remaining = shares_to_sell;
    let mut total_proceeds = 0.0;
    let mut total_sold     = 0.0;

    for level in bids {
        if remaining <= 0.001 { break; }
        if level.price <= 0.0 || level.size <= 0.0 { continue; }

        let shares = remaining.min(level.size);
        total_proceeds += shares * level.price;
        total_sold     += shares;
        remaining      -= shares;
    }

    if total_sold <= 0.0 {
        return None;
    }

    Some(total_proceeds / total_sold)
}

/// Market metadata fetched once at startup
#[derive(Debug, Clone)]
#[allow(dead_code)]
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
#[allow(dead_code)]
struct GammaMarket {
    condition_id:   Option<String>,
    #[serde(default, deserialize_with = "deserialize_stringified_vec")]
    clob_token_ids: Option<Vec<String>>,
    #[serde(default, deserialize_with = "deserialize_stringified_vec")]
    outcomes:       Option<Vec<String>>,
    start_date_iso: Option<String>,
    end_date_iso:   Option<String>,
}

/// Gamma API returns these fields as stringified JSON arrays, e.g. "[\"Up\", \"Down\"]"
fn deserialize_stringified_vec<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let opt: Option<serde_json::Value> = Option::deserialize(deserializer)?;
    match opt {
        None => Ok(None),
        Some(serde_json::Value::Array(arr)) => {
            // Already a real array
            let v: Vec<String> = arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect();
            Ok(Some(v))
        }
        Some(serde_json::Value::String(s)) => {
            // Stringified JSON array — parse it
            let v: Vec<String> = serde_json::from_str(&s)
                .map_err(|e| D::Error::custom(format!("bad stringified array: {}", e)))?;
            Ok(Some(v))
        }
        Some(other) => Err(D::Error::custom(format!("expected string or array, got {:?}", other))),
    }
}

// ── CLOB REST ────────────────────────────────────────────────────────────────

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
    // Only return current window — entering the NEXT window early caused duplicate positions
    vec![current]
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

    // POST /books with JSON body [{"token_id": "X"}, {"token_id": "Y"}]
    // Split into batches of BOOK_BATCH_SIZE
    let mut result = HashMap::new();

    for chunk in token_ids.chunks(BOOK_BATCH_SIZE) {
        let url = format!("{}/books", clob_rest);
        let body: Vec<serde_json::Value> = chunk.iter()
            .map(|tid| serde_json::json!({"token_id": tid}))
            .collect();

        debug!("Batch book fetch: {} tokens", chunk.len());

        let resp = client
            .post(&url)
            .json(&body)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .context("CLOB batch book request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!("CLOB batch book returned {} body={}", status, &body[..body.len().min(200)]);
            continue;
        }

        // Response is array of book objects with asset_id, bids, asks
        let books: Vec<serde_json::Value> = resp
            .json()
            .await
            .context("CLOB batch book JSON parse failed")?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        for item in &books {
            let tid = match item.get("asset_id").and_then(|v| v.as_str()) {
                Some(id) => id.to_string(),
                None => continue,
            };

            // Parse full ask depth (sorted ascending by price)
            let mut asks: Vec<PriceLevel> = item.get("asks")
                .and_then(|a| a.as_array())
                .map(|arr| arr.iter().filter_map(|l| {
                    let price = l.get("price").and_then(|p| p.as_str()).and_then(|s| s.parse::<f64>().ok())?;
                    let size  = l.get("size").and_then(|s| s.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                    Some(PriceLevel { price, size })
                }).collect())
                .unwrap_or_default();
            asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());

            // Parse full bid depth (sorted descending by price)
            let mut bids: Vec<PriceLevel> = item.get("bids")
                .and_then(|b| b.as_array())
                .map(|arr| arr.iter().filter_map(|l| {
                    let price = l.get("price").and_then(|p| p.as_str()).and_then(|s| s.parse::<f64>().ok())?;
                    let size  = l.get("size").and_then(|s| s.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                    Some(PriceLevel { price, size })
                }).collect())
                .unwrap_or_default();
            bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());

            let best_ask = asks.first().map(|l| l.price).unwrap_or(0.0);
            let best_bid = bids.first().map(|l| l.price).unwrap_or(0.0);

            if best_ask > 0.0 {
                result.insert(tid, BookEntry { asks, bids, best_ask, best_bid, ts: now });
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

    // Subscribe to chainlink topic (wildcard — all assets in one subscription)
    let sub = serde_json::json!({
        "action": "subscribe",
        "subscriptions": [{
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": ""
        }]
    });
    ws.send(Message::Text(sub.to_string())).await?;

    info!("[CL] Feed connected, watching {} assets", assets.len());

    while let Some(msg) = ws.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    process_cl_message(&v, cl_prices, price_history);
                }
            }
            Ok(Message::Ping(data)) => { let _ = ws.send(Message::Pong(data)).await; }
            Ok(_) => {}
            Err(e) => { error!("[CL] WebSocket error: {}", e); break; }
        }
    }

    Ok(())
}

fn process_cl_message(
    v:             &serde_json::Value,
    cl_prices:     &ClPrices,
    price_history: &PriceHistory,
) {
    // Format: {"topic":"crypto_prices_chainlink","payload":{"symbol":"BTC/USD","value":67321.5,"timestamp":1234567890000}}
    let topic = v.get("topic").and_then(|t| t.as_str()).unwrap_or("");
    if topic != "crypto_prices_chainlink" {
        return;
    }

    let payload = match v.get("payload") {
        Some(p) => p,
        None    => return,
    };

    let symbol = match payload.get("symbol").and_then(|s| s.as_str()) {
        Some(s) => s.to_lowercase(),
        None    => return,
    };

    // Normalize: "btc/usd" → "btc", "btcusd" → "btc", "btc" → "btc"
    let asset = symbol
        .replace("/usd", "")
        .trim_end_matches("usd")
        .to_string();

    // value can be f64 or string
    let price: f64 = payload.get("value")
        .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .unwrap_or(0.0);
    if price <= 0.0 {
        return;
    }

    // timestamp may be in milliseconds or seconds
    let raw_ts = payload.get("timestamp")
        .and_then(|t| t.as_f64().or_else(|| t.as_i64().map(|i| i as f64)))
        .unwrap_or(0.0);
    let ts = if raw_ts > 1e12 {
        raw_ts / 1000.0  // milliseconds → seconds
    } else if raw_ts > 1e9 {
        raw_ts
    } else {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64()
    };

    cl_prices.insert(asset.clone(), (ts, price));

    // Append to price history (cap at 1000 entries per asset)
    let mut hist = price_history.entry(asset).or_default();
    hist.push((ts, price));
    if hist.len() > 1000 {
        let drain_to = hist.len() - 1000;
        hist.drain(0..drain_to);
    }
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
        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0], 1772788200);
    }
}
