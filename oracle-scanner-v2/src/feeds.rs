/// feeds.rs -- External data feeds (CL + BN prices + PM book)
///
/// 1. CL price feed: WebSocket from Polymarket live-data (chainlink topic)
/// 2. BN price feed: WebSocket from Binance aggTrade (unthrottled, no rate limit)
/// 3. Book data: Batched REST via CLOB /books endpoint

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

const BOOK_BATCH_SIZE: usize = 20;
const WS_RECONNECT_SECS: u64 = 5;

// -- Shared state types -------------------------------------------------------

pub type ClPrices     = Arc<DashMap<String, (f64, f64)>>;   // asset -> (ts, price)
pub type BnPrices     = Arc<DashMap<String, (f64, f64)>>;   // asset -> (ts, price)
pub type BookState    = Arc<DashMap<String, BookEntry>>;
pub type PriceHistory = Arc<DashMap<String, Vec<(f64, f64)>>>; // key -> [(ts, price)]

#[derive(Debug, Clone)]
pub struct PriceLevel {
    pub price: f64,
    pub size:  f64,
}

#[derive(Debug, Clone)]
pub struct BookEntry {
    pub asks:     Vec<PriceLevel>,
    pub bids:     Vec<PriceLevel>,
    pub best_ask: f64,
    pub best_bid: f64,
    pub ts:       f64,
}

/// VWAP fill: walk the book for a given USD stake.
/// Returns (avg_fill_price, shares_filled) or None.
pub fn vwap_fill(levels: &[PriceLevel], stake: f64) -> Option<(f64, f64)> {
    if levels.is_empty() || stake <= 0.0 { return None; }
    let mut budget = stake;
    let mut total_shares = 0.0;
    let mut total_cost   = 0.0;

    for level in levels {
        if budget <= 0.001 { break; }
        if level.price <= 0.0 || level.size <= 0.0 { continue; }
        let shares_wanted = budget / level.price;
        let shares = shares_wanted.min(level.size);
        let cost = shares * level.price;
        total_shares += shares;
        total_cost   += cost;
        budget       -= cost;
    }

    if total_shares <= 0.0 { return None; }
    Some((total_cost / total_shares, total_shares))
}

// -- Market metadata ----------------------------------------------------------

#[derive(Debug, Clone)]
#[allow(dead_code)]
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

// -- Gamma API types ----------------------------------------------------------

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

fn deserialize_stringified_vec<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let opt: Option<serde_json::Value> = Option::deserialize(deserializer)?;
    match opt {
        None => Ok(None),
        Some(serde_json::Value::Array(arr)) => {
            let v: Vec<String> = arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect();
            Ok(Some(v))
        }
        Some(serde_json::Value::String(s)) => {
            let v: Vec<String> = serde_json::from_str(&s)
                .map_err(|e| D::Error::custom(format!("bad stringified array: {}", e)))?;
            Ok(Some(v))
        }
        Some(other) => Err(D::Error::custom(format!("expected string or array, got {:?}", other))),
    }
}

// -- Slug helpers -------------------------------------------------------------

pub fn build_slug(asset: &str, tf_mins: u32, window_start: u64) -> String {
    format!("{}-updown-{}m-{}", asset, tf_mins, window_start)
}

pub fn current_window_starts(tf_mins: u32, now_secs: u64) -> Vec<u64> {
    let interval = (tf_mins as u64) * 60;
    let current  = (now_secs / interval) * interval;
    vec![current]
}

// -- Market discovery ---------------------------------------------------------

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

    if resp.status().as_u16() == 404 {
        return Ok(None);
    }

    let event: GammaEvent = resp.json().await.context("gamma API JSON parse failed")?;

    let market = event.markets.iter().find(|m| {
        m.outcomes.as_ref()
            .map(|o| o.iter().any(|x| x.eq_ignore_ascii_case("up")))
            .unwrap_or(false)
    });

    let market = match market {
        Some(m) => m,
        None    => return Ok(None),
    };

    let tokens   = market.clob_token_ids.as_ref().ok_or_else(|| anyhow!("no clobTokenIds"))?;
    let outcomes = market.outcomes.as_ref().ok_or_else(|| anyhow!("no outcomes"))?;
    if tokens.len() < 2 || outcomes.len() < 2 { return Ok(None); }

    let yes_idx = outcomes.iter().position(|o| o.eq_ignore_ascii_case("up"))
        .ok_or_else(|| anyhow!("no Up outcome"))?;
    let no_idx = outcomes.iter().position(|o| o.eq_ignore_ascii_case("down"))
        .ok_or_else(|| anyhow!("no Down outcome"))?;

    let token_yes = tokens.get(yes_idx)
        .ok_or_else(|| anyhow!("yes_idx {} out of bounds (tokens len={})", yes_idx, tokens.len()))?
        .clone();
    let token_no = tokens.get(no_idx)
        .ok_or_else(|| anyhow!("no_idx {} out of bounds (tokens len={})", no_idx, tokens.len()))?
        .clone();

    let condition_id = market.condition_id.clone().unwrap_or_default();

    let window_start = slug.rsplit('-').next()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let window_end = window_start + (tf as u64 * 60);

    Ok(Some(MarketMeta {
        slug: slug.to_string(),
        asset: asset.to_string(),
        tf, window_start, window_end,
        token_yes, token_no,
        open_price: 0.0,
        condition_id,
    }))
}

// -- Batch book fetcher -------------------------------------------------------

pub async fn fetch_books_batch(
    client:    &Client,
    clob_rest: &str,
    token_ids: &[String],
    limiter:   &RateLimiter,
    batch_size: usize,
) -> Result<HashMap<String, BookEntry>> {
    if token_ids.is_empty() { return Ok(HashMap::new()); }
    limiter.wait().await;

    let batch = if batch_size > 0 { batch_size } else { BOOK_BATCH_SIZE };
    let mut result = HashMap::new();

    for chunk in token_ids.chunks(batch) {
        let url = format!("{}/books", clob_rest);
        let body: Vec<serde_json::Value> = chunk.iter()
            .map(|tid| serde_json::json!({"token_id": tid}))
            .collect();

        let resp = client.post(&url).json(&body)
            .timeout(Duration::from_secs(5))
            .send().await
            .context("CLOB batch book request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!("CLOB batch book returned {} body={}", status, &body[..body.len().min(200)]);
            continue;
        }

        let books: Vec<serde_json::Value> = resp.json().await
            .context("CLOB batch book JSON parse failed")?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default().as_secs_f64();

        for item in &books {
            let tid = match item.get("asset_id").and_then(|v| v.as_str()) {
                Some(id) => id.to_string(),
                None => continue,
            };

            let mut asks: Vec<PriceLevel> = item.get("asks")
                .and_then(|a| a.as_array())
                .map(|arr| arr.iter().filter_map(|l| {
                    let price = l.get("price").and_then(|p| p.as_str()).and_then(|s| s.parse::<f64>().ok())?;
                    let size  = l.get("size").and_then(|s| s.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                    Some(PriceLevel { price, size })
                }).collect())
                .unwrap_or_default();
            asks.retain(|l| l.price.is_finite() && l.size.is_finite());
            asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));

            let mut bids: Vec<PriceLevel> = item.get("bids")
                .and_then(|b| b.as_array())
                .map(|arr| arr.iter().filter_map(|l| {
                    let price = l.get("price").and_then(|p| p.as_str()).and_then(|s| s.parse::<f64>().ok())?;
                    let size  = l.get("size").and_then(|s| s.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                    Some(PriceLevel { price, size })
                }).collect())
                .unwrap_or_default();
            bids.retain(|l| l.price.is_finite() && l.size.is_finite());
            bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal));

            let best_ask = asks.first().map(|l| l.price).unwrap_or(0.0);
            let best_bid = bids.first().map(|l| l.price).unwrap_or(0.0);

            if best_ask > 0.0 {
                result.insert(tid, BookEntry { asks, bids, best_ask, best_bid, ts: now });
            }
        }

        if token_ids.len() > batch {
            tokio::time::sleep(Duration::from_millis(500)).await;
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
    _assets:       &[String],
    cl_prices:     &ClPrices,
    price_history: &PriceHistory,
) -> Result<()> {
    let url = Url::parse(live_ws).context("invalid live WS URL")?;
    let (mut ws, _) = connect_async(url.to_string()).await.context("CL WS connect failed")?;

    let sub = serde_json::json!({
        "action": "subscribe",
        "subscriptions": [{
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": ""
        }]
    });
    ws.send(Message::Text(sub.to_string().into())).await?;

    info!("[CL] Feed connected");

    let mut ping_interval = tokio::time::interval(Duration::from_secs(5));
    ping_interval.tick().await;

    loop {
        let msg = tokio::select! {
            msg = ws.next() => match msg {
                Some(m) => m,
                None => break,
            },
            _ = ping_interval.tick() => {
                let _ = ws.send(Message::Ping(vec![].into())).await;
                continue;
            }
        };

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
    let topic = v.get("topic").and_then(|t| t.as_str()).unwrap_or("");
    if topic != "crypto_prices_chainlink" { return; }

    let payload = match v.get("payload") {
        Some(p) => p,
        None    => return,
    };

    let symbol = match payload.get("symbol").and_then(|s| s.as_str()) {
        Some(s) => s.to_lowercase(),
        None    => return,
    };

    let asset = symbol.replace("/usd", "").trim_end_matches("usd").to_string();

    let price: f64 = payload.get("value")
        .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .unwrap_or(0.0);
    if price <= 0.0 { return; }

    let raw_ts = payload.get("timestamp")
        .and_then(|t| t.as_f64().or_else(|| t.as_i64().map(|i| i as f64)))
        .unwrap_or(0.0);
    let ts = if raw_ts > 1e12 { raw_ts / 1000.0 }
             else if raw_ts > 1e9 { raw_ts }
             else {
                 std::time::SystemTime::now()
                     .duration_since(std::time::UNIX_EPOCH)
                     .unwrap_or_default().as_secs_f64()
             };

    cl_prices.insert(asset.clone(), (ts, price));

    let mut hist = price_history.entry(asset).or_default();
    hist.push((ts, price));
    if hist.len() > 1000 {
        let drain_to = hist.len() - 1000;
        hist.drain(0..drain_to);
    }
}

// -- Binance aggTrade WebSocket feed ------------------------------------------

fn bn_stream_url(assets: &[String]) -> String {
    let streams: Vec<String> = assets.iter()
        .map(|a| format!("{}usdt@aggTrade", a))
        .collect();
    format!("wss://stream.binance.com:9443/stream?streams={}", streams.join("/"))
}

fn bn_symbol_to_asset(symbol: &str) -> Option<String> {
    let s = symbol.to_lowercase();
    s.strip_suffix("usdt").map(|a| a.to_string())
}

pub async fn run_bn_feed(
    assets:     Vec<String>,
    bn_prices:  BnPrices,
    bn_history: PriceHistory,
) {
    let url = bn_stream_url(&assets);
    loop {
        info!("[BN] Connecting to {}", url);
        match connect_bn_feed(&url, &bn_prices, &bn_history).await {
            Ok(_)  => warn!("[BN] Feed closed, reconnecting..."),
            Err(e) => error!("[BN] Feed error: {}, reconnecting in {}s", e, WS_RECONNECT_SECS),
        }
        tokio::time::sleep(Duration::from_secs(WS_RECONNECT_SECS)).await;
    }
}

async fn connect_bn_feed(
    url:        &str,
    bn_prices:  &BnPrices,
    bn_history: &PriceHistory,
) -> Result<()> {
    let parsed = Url::parse(url).context("invalid BN WS URL")?;
    let (mut ws, _) = connect_async(parsed.to_string()).await.context("BN WS connect failed")?;

    info!("[BN] Feed connected");

    let mut ping_interval = tokio::time::interval(Duration::from_secs(5));
    ping_interval.tick().await;

    loop {
        let msg = tokio::select! {
            msg = ws.next() => match msg {
                Some(m) => m,
                None => break,
            },
            _ = ping_interval.tick() => {
                let _ = ws.send(Message::Ping(vec![].into())).await;
                continue;
            }
        };

        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    process_bn_message(&v, bn_prices, bn_history);
                }
            }
            Ok(Message::Ping(data)) => { let _ = ws.send(Message::Pong(data)).await; }
            Ok(_) => {}
            Err(e) => { error!("[BN] WebSocket error: {}", e); break; }
        }
    }

    Ok(())
}

fn process_bn_message(
    v:          &serde_json::Value,
    bn_prices:  &BnPrices,
    bn_history: &PriceHistory,
) {
    // Combined stream: {"stream":"btcusdt@aggTrade","data":{"s":"BTCUSDT","p":"...","T":...}}
    let data = match v.get("data") {
        Some(d) => d,
        None => v,
    };

    let symbol = match data.get("s").and_then(|s| s.as_str()) {
        Some(s) => s,
        None => return,
    };

    let asset = match bn_symbol_to_asset(symbol) {
        Some(a) => a,
        None => return,
    };

    let price: f64 = data.get("p")
        .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_f64()))
        .unwrap_or(0.0);
    if price <= 0.0 { return; }

    let ts_ms = data.get("T")
        .and_then(|t| t.as_f64().or_else(|| t.as_i64().map(|i| i as f64)))
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default().as_millis() as f64
        });
    let ts = if ts_ms > 1e12 { ts_ms / 1000.0 } else { ts_ms };

    bn_prices.insert(asset.clone(), (ts, price));

    let hist_key = format!("bn_{}", asset);
    let mut hist = bn_history.entry(hist_key).or_default();
    hist.push((ts, price));
    if hist.len() > 1000 {
        let drain_to = hist.len() - 1000;
        hist.drain(0..drain_to);
    }
}

// -- Helpers for momentum/lag computation (used by main.rs) -------------------

/// Compute momentum: price change over last N seconds from a history vec.
/// Returns (pct_change, abs_change) or (0.0, 0.0) if insufficient data.
pub fn momentum_from_history(hist: &[(f64, f64)], now: f64, lookback_secs: f64) -> (f64, f64) {
    let cutoff = now - lookback_secs;
    let old = hist.iter()
        .filter(|(ts, _)| *ts <= cutoff)
        .last()
        .map(|(_, p)| *p);
    let current = hist.last().map(|(_, p)| *p);
    match (old, current) {
        (Some(o), Some(c)) if o > 0.0 => ((c - o) / o, c - o),
        _ => (0.0, 0.0),
    }
}

/// Count CL updates in a time window (for cl_update_count metric)
pub fn count_updates_in_window(hist: &[(f64, f64)], start: f64, end: f64) -> u32 {
    hist.iter().filter(|(ts, _)| *ts >= start && *ts <= end).count() as u32
}

/// Max gap between consecutive updates in a time window
pub fn max_gap_in_window(hist: &[(f64, f64)], start: f64, end: f64) -> f64 {
    let window: Vec<f64> = hist.iter()
        .filter(|(ts, _)| *ts >= start && *ts <= end)
        .map(|(ts, _)| *ts)
        .collect();
    if window.len() < 2 { return 0.0; }
    window.windows(2).map(|w| w[1] - w[0]).fold(0.0_f64, f64::max)
}

// -- Tests --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_slug_correct() {
        assert_eq!(build_slug("btc", 5, 1772788200), "btc-updown-5m-1772788200");
    }

    #[test]
    fn current_window_starts_5m() {
        let starts = current_window_starts(5, 1772788230);
        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0], 1772788200);
    }

    #[test]
    fn vwap_fill_single_level() {
        let levels = vec![PriceLevel { price: 0.50, size: 100.0 }];
        let (price, shares) = vwap_fill(&levels, 5.0).unwrap();
        assert!((price - 0.50).abs() < 0.001);
        assert!((shares - 10.0).abs() < 0.01);
    }

    #[test]
    fn bn_stream_url_correct() {
        let url = bn_stream_url(&["btc".to_string(), "eth".to_string()]);
        assert!(url.contains("btcusdt@aggTrade"));
        assert!(url.contains("ethusdt@aggTrade"));
    }

    #[test]
    fn bn_symbol_to_asset_works() {
        assert_eq!(bn_symbol_to_asset("BTCUSDT"), Some("btc".to_string()));
        assert_eq!(bn_symbol_to_asset("ETHUSDT"), Some("eth".to_string()));
        assert_eq!(bn_symbol_to_asset("INVALID"), None);
    }

    #[test]
    fn momentum_basic() {
        let hist = vec![(1.0, 100.0), (2.0, 101.0), (3.0, 102.0)];
        let (pct, _) = momentum_from_history(&hist, 3.0, 2.0);
        assert!((pct - 0.02).abs() < 0.001);
    }
}
