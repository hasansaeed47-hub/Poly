/// feeds.rs — All external data feeds
///
/// Three feeds:
/// 1. CL price feed   : WebSocket from Polymarket live-data (chainlink topic)
/// 2. PM book feed    : WebSocket from Polymarket CLOB subscriptions
/// 3. Market discovery: Batched REST via Gamma API (slug -> token IDs)

use std::collections::{HashMap, VecDeque};
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

// -- Constants ----------------------------------------------------------------

const WS_RECONNECT_SECS: u64 = 5;

// -- Shared state types -------------------------------------------------------

/// CL oracle prices: asset -> (unix_ts, price)
pub type ClPrices = Arc<DashMap<String, (f64, f64)>>;

/// CL price snapshots: asset -> HashMap<unix_ts_i64, price> (for cl_at lookups)
pub type ClSnapshots = Arc<DashMap<String, HashMap<i64, f64>>>;

/// PM order book: token_id -> BookEntry (with full level tracking)
pub type BookState = Arc<DashMap<String, BookEntry>>;

/// Binance prices: asset -> latest price
pub type BnPrices = Arc<DashMap<String, f64>>;

/// Binance price history: asset -> VecDeque<(ts, price)>
pub type BnHistory = Arc<DashMap<String, VecDeque<(f64, f64)>>>;

// -- BN/CL helper queries -----------------------------------------------------

/// Get CL price snapshot at a specific timestamp (+-3s tolerance)
pub fn cl_at(snapshots: &ClSnapshots, asset: &str, t: i64) -> Option<f64> {
    let s = snapshots.get(asset)?;
    for off in [0i64, 1, -1, 2, -2, 3, -3] {
        if let Some(&p) = s.get(&(t + off)) {
            return Some(p);
        }
    }
    None
}

/// Get Binance trend over last N seconds (% change)
pub fn bn_trend(bn_hist: &BnHistory, asset: &str, secs: u64) -> Option<f64> {
    let h = bn_hist.get(asset)?;
    if h.len() < 2 { return None; }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    let old = h.iter().find(|(t, _)| *t >= now - secs as f64)?;
    if old.1 <= 0.0 { return None; }
    Some((h.back()?.1 - old.1) / old.1 * 100.0)
}

/// Get CL trend over last N seconds (% change)
pub fn cl_trend(snapshots: &ClSnapshots, cl_prices: &ClPrices, asset: &str, secs: u64) -> Option<f64> {
    let s = snapshots.get(asset)?;
    if s.is_empty() { return None; }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    let cut = now as i64 - secs as i64;
    let cur = cl_prices.get(asset)?.1;
    let old = s.iter()
        .filter(|(t, _)| *t >= &cut)
        .min_by_key(|(t, _)| *t)
        .map(|(_, p)| *p)?;
    if old <= 0.0 { return None; }
    Some((cur - old) / old * 100.0)
}

/// Get 1-hour price range (%)
pub fn hour_range(snapshots: &ClSnapshots, asset: &str) -> f64 {
    let s = match snapshots.get(asset) { Some(s) => s, None => return 0.0 };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64() as i64;
    let cut = now - 3600;
    let prices: Vec<f64> = s.iter().filter(|(t, _)| *t > &cut).map(|(_, p)| *p).collect();
    if prices.len() < 10 { return 999.0; }
    let hi = prices.iter().cloned().fold(f64::MIN, f64::max);
    let lo = prices.iter().cloned().fold(f64::MAX, f64::min);
    if lo <= 0.0 { return 0.0; }
    (hi - lo) / lo * 100.0
}

/// Full order book entry — maintains all levels for correct delta application.
#[derive(Debug, Clone)]
pub struct BookEntry {
    pub best_ask: f64,
    pub best_bid: f64,
    pub ts:       f64,
    asks: Vec<(f64, f64)>,
    bids: Vec<(f64, f64)>,
}

impl BookEntry {
    pub fn new() -> Self {
        Self { best_ask: 0.0, best_bid: 0.0, ts: 0.0, asks: Vec::new(), bids: Vec::new() }
    }

    /// Replace entire book from a full snapshot
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

/// Market metadata — one per active window
#[derive(Debug, Clone)]
pub struct MarketMeta {
    pub asset:         String,
    pub tf:            u32,
    pub window_start:  u64,
    pub window_end:    u64,
    pub token_yes:     String,
    pub token_no:      String,
    pub open_price:    f64,
    pub open_cl_ts:    f64,
    pub open_missed:   bool,
}

// -- Gamma API response types -------------------------------------------------

#[derive(Deserialize, Debug)]
struct GammaEvent {
    markets: Vec<GammaMarket>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    clob_token_ids: Option<Vec<String>>,
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    outcomes:       Option<Vec<String>>,
}

/// Deserialize a field that may be either a JSON array or a stringified JSON array.
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

    let url = format!("{}/markets", gamma_api);
    debug!("GET {}?slug={}", url, slug);

    let resp = client
        .get(&url)
        .query(&[("slug", slug)])
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .context("gamma API request failed")?;

    if resp.status() == 404 {
        return Ok(None);
    }

    let text = resp.text().await.context("gamma API read body failed")?;

    let markets: Vec<GammaMarket> = match serde_json::from_str(&text) {
        Ok(m) => m,
        Err(_) => {
            if let Ok(event) = serde_json::from_str::<GammaEvent>(&text) {
                event.markets
            } else if let Ok(mut arr) = serde_json::from_str::<Vec<GammaEvent>>(&text) {
                arr.pop().map(|e| e.markets).unwrap_or_default()
            } else {
                warn!("gamma parse error for slug {}: body[..200]: {}", slug, &text[..text.len().min(200)]);
                return Ok(None);
            }
        }
    };

    let market = markets.iter().find(|m| {
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
        asset: asset.to_string(),
        tf,
        window_start,
        window_end,
        token_yes,
        token_no,
        open_price:   0.0,
        open_cl_ts:   0.0,
        open_missed:  false,
    }))
}

// -- Batch book fetcher -------------------------------------------------------

pub async fn fetch_books_batch(
    client:   &Client,
    clob_rest: &str,
    token_ids: &[String],
    _limiter:  &RateLimiter,
) -> Result<HashMap<String, BookEntry>> {
    if token_ids.is_empty() {
        return Ok(HashMap::new());
    }

    const CHUNK_SIZE: usize = 15;
    let url = format!("{}/books", clob_rest);
    let mut all_items: Vec<serde_json::Value> = Vec::new();

    for chunk in token_ids.chunks(CHUNK_SIZE) {
        let body: Vec<serde_json::Value> = chunk.iter()
            .map(|t| serde_json::json!({"token_id": t}))
            .collect();

        let resp = match client
            .post(&url)
            .json(&body)
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("CLOB books batch request failed: {}", e);
                continue;
            }
        };

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!("CLOB books batch returned {}: {}", status, &body[..body.len().min(200)]);
            continue;
        }

        match resp.json::<Vec<serde_json::Value>>().await {
            Ok(v) => all_items.extend(v),
            Err(e) => {
                warn!("CLOB books batch parse failed: {}", e);
                continue;
            }
        }
    }

    let items = all_items;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();

    let mut result = HashMap::new();

    for item in &items {
        let tid = match item.get("asset_id").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => continue,
        };

        let mut asks: Vec<(f64, f64)> = Vec::new();
        let mut bids: Vec<(f64, f64)> = Vec::new();

        if let Some(ask_arr) = item.get("asks").and_then(|a| a.as_array()) {
            for a in ask_arr {
                if let (Some(p), Some(s)) = (
                    a.get("price").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()),
                    a.get("size").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()),
                ) {
                    asks.push((p, s));
                }
            }
        }

        if let Some(bid_arr) = item.get("bids").and_then(|b| b.as_array()) {
            for b in bid_arr {
                if let (Some(p), Some(s)) = (
                    b.get("price").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()),
                    b.get("size").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()),
                ) {
                    bids.push((p, s));
                }
            }
        }

        let mut entry = BookEntry::new();
        entry.set_snapshot(asks, bids, now);

        if entry.best_ask > 0.0 {
            result.insert(tid, entry);
        }
    }

    Ok(result)
}

// -- CL price WebSocket feed --------------------------------------------------

pub async fn run_cl_feed(
    live_ws:      String,
    assets:       Vec<String>,
    cl_prices:    ClPrices,
    cl_snapshots: ClSnapshots,
) {
    loop {
        info!("[CL] Connecting to {}", live_ws);

        match connect_cl_feed(&live_ws, &assets, &cl_prices, &cl_snapshots).await {
            Ok(_)  => warn!("[CL] Feed closed cleanly, reconnecting..."),
            Err(e) => error!("[CL] Feed error: {}, reconnecting in {}s", e, WS_RECONNECT_SECS),
        }

        tokio::time::sleep(Duration::from_secs(WS_RECONNECT_SECS)).await;
    }
}

async fn connect_cl_feed(
    live_ws:      &str,
    assets:       &[String],
    cl_prices:    &ClPrices,
    cl_snapshots: &ClSnapshots,
) -> Result<()> {
    // Connect with 10s timeout to avoid hanging forever
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_async(live_ws),
    ).await.context("CL WS connect timed out")?.context("CL WS connect failed")?;

    let sub = serde_json::json!({
        "action": "subscribe",
        "subscriptions": [{
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": ""
        }]
    });
    ws.send(Message::Text(sub.to_string().into())).await?;
    debug!("[CL] Subscribe msg: {}", serde_json::to_string(&sub).unwrap_or_default());

    info!("[CL] Feed connected, watching {} assets: {:?}", assets.len(), assets);

    let mut ping_interval = tokio::time::interval(Duration::from_secs(5));
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut last_data = tokio::time::Instant::now();
    const STALE_TIMEOUT: Duration = Duration::from_secs(30);

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                // Check for stale feed — no price data in 30s means silent disconnect
                if last_data.elapsed() > STALE_TIMEOUT {
                    warn!("[CL] Feed stale — no price data for {:.0}s, forcing reconnect",
                        last_data.elapsed().as_secs_f64());
                    break;
                }
                ws.send(Message::Text(
                    serde_json::json!({"action":"ping"}).to_string().into()
                )).await?;
            }
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if text == "pong" {
                            continue;
                        }
                        if text.contains("payload") {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                                process_cl_message(&v, cl_prices, cl_snapshots);
                                last_data = tokio::time::Instant::now();
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = ws.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        warn!("[CL] WS close frame: {:?}", frame);
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(anyhow::anyhow!("CL WS message error: {}", e)),
                    None => break,
                }
            }
        }
    }

    Ok(())
}

fn process_cl_message(
    v:            &serde_json::Value,
    cl_prices:    &ClPrices,
    cl_snapshots: &ClSnapshots,
) {
    let topic = v.get("topic").and_then(|t| t.as_str()).unwrap_or("");
    if topic != "crypto_prices_chainlink" {
        return;
    }

    let payload = match v.get("payload") {
        Some(p) => p,
        None => return,
    };

    let symbol = match payload.get("symbol").and_then(|s| s.as_str()) {
        Some(s) => s.to_lowercase(),
        None    => return,
    };
    let asset = symbol.split('/').next().unwrap_or("").to_string();
    if asset.is_empty() {
        return;
    }

    let price = match payload.get("value").and_then(|p| p.as_f64()) {
        Some(p) if p > 0.0 => p,
        _ => {
            match payload.get("value").and_then(|p| p.as_str()).and_then(|s| s.parse::<f64>().ok()) {
                Some(p) if p > 0.0 => p,
                _ => return,
            }
        }
    };

    let ts = payload
        .get("timestamp")
        .and_then(|t| t.as_f64())
        .map(|t| if t > 1e12 { t / 1000.0 } else { t })
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64()
        });

    let is_new = !cl_prices.contains_key(&asset);
    cl_prices.insert(asset.clone(), (ts, price));
    if is_new {
        info!("[CL] First price for {}: {:.2} at ts={:.0}", asset, price, ts);
    }

    {
        let mut snap = cl_snapshots.entry(asset).or_default();
        snap.insert(ts as i64, price);
        let cutoff = ts as i64 - 7200;
        snap.retain(|k, _| *k > cutoff);
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
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_async(clob_ws),
    ).await.context("CLOB WS connect timed out")?.context("CLOB WS connect failed")?;

    let ids: Vec<String> = token_ids.iter().map(|e| e.key().clone()).collect();
    if !ids.is_empty() {
        let sub = serde_json::json!({
            "type": "market",
            "assets_ids": ids,
            "custom_feature_enabled": true
        });
        debug!("[BOOK] Subscribe msg: {} tokens", ids.len());
        ws.send(Message::Text(sub.to_string().into())).await?;
        info!("[BOOK] Subscribed to {} token IDs", ids.len());
    }

    let connect_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    book_live.store(connect_ts, std::sync::atomic::Ordering::Relaxed);

    let mut ping_interval = tokio::time::interval(Duration::from_secs(10));
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut msg_count = 0u64;
    let mut last_data = tokio::time::Instant::now();
    const BOOK_STALE_TIMEOUT: Duration = Duration::from_secs(60);

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                if last_data.elapsed() > BOOK_STALE_TIMEOUT {
                    warn!("[BOOK] Feed stale — no data for {:.0}s, forcing reconnect",
                        last_data.elapsed().as_secs_f64());
                    break;
                }
                ws.send(Message::Text("ping".into())).await?;
            }
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if text == "pong" || text == "PONG" {
                            continue;
                        }

                        last_data = tokio::time::Instant::now();
                        msg_count += 1;
                        if msg_count <= 5 {
                            debug!("[BOOK] WS msg #{}: {}", msg_count, &text[..text.len().min(500)]);
                        }

                        if text.starts_with('[') {
                            if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(&text) {
                                for v in &arr {
                                    process_book_message(v, book_state);
                                }
                            }
                        } else if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                            process_book_message(&v, book_state);
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = ws.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        warn!("[BOOK] WS close frame: {:?}", frame);
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(anyhow::anyhow!("CLOB WS message error: {}", e)),
                    None => break,
                }
            }
        }
    }

    Ok(())
}

fn process_book_message(v: &serde_json::Value, book_state: &BookState) {
    let event_type = v.get("event_type").and_then(|e| e.as_str()).unwrap_or("");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();

    match event_type {
        "best_bid_ask" => {
            let asset_id = match v.get("asset_id").and_then(|a| a.as_str()) {
                Some(id) => id.to_string(),
                None     => return,
            };
            let best_ask = v.get("best_ask").and_then(|b| b.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
            let best_bid = v.get("best_bid").and_then(|b| b.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);

            if best_ask > 0.0 {
                let mut entry = book_state
                    .get(&asset_id)
                    .map(|e| e.clone())
                    .unwrap_or_else(BookEntry::new);
                entry.best_ask = best_ask;
                if best_bid > 0.0 { entry.best_bid = best_bid; }
                entry.ts = now;
                book_state.insert(asset_id, entry);
            }
        }
        "book" => {
            let asset_id = match v.get("asset_id").and_then(|a| a.as_str()) {
                Some(id) => id.to_string(),
                None     => return,
            };

            let asks = parse_levels(v.get("asks"));
            let bids = parse_levels(v.get("bids"));

            let mut entry = BookEntry::new();
            entry.set_snapshot(asks, bids, now);

            if entry.best_ask > 0.0 {
                book_state.insert(asset_id, entry);
            }
        }
        "price_change" => {
            if let Some(changes) = v.get("price_changes").and_then(|c| c.as_array()) {
                for change in changes {
                    let asset_id = match change.get("asset_id").and_then(|a| a.as_str()) {
                        Some(id) => id.to_string(),
                        None => continue,
                    };
                    apply_price_change(&asset_id, change, book_state, now);
                }
            } else if let Some(changes) = v.get("changes").and_then(|c| c.as_array()) {
                let asset_id = match v.get("asset_id").and_then(|a| a.as_str()) {
                    Some(id) => id.to_string(),
                    None => return,
                };
                for change in changes {
                    apply_price_change(&asset_id, change, book_state, now);
                }
            }
        }
        _ => {}
    }
}

fn apply_price_change(
    asset_id: &str,
    change: &serde_json::Value,
    book_state: &BookState,
    now: f64,
) {
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

        let mut entry = book_state
            .get(asset_id)
            .map(|e| e.clone())
            .unwrap_or_else(BookEntry::new);

        entry.apply_level(price, size, is_ask, now);

        if let Some(bb) = change.get("best_bid").and_then(|b| b.as_str()).and_then(|s| s.parse::<f64>().ok()) {
            if bb > 0.0 { entry.best_bid = bb; }
        }
        if let Some(ba) = change.get("best_ask").and_then(|b| b.as_str()).and_then(|s| s.parse::<f64>().ok()) {
            if ba > 0.0 { entry.best_ask = ba; }
        }

        if entry.best_ask > 0.0 {
            book_state.insert(asset_id.to_string(), entry);
        }
    }
}

// -- Binance aggTrade WebSocket feed ------------------------------------------

pub async fn run_bn_feed(
    bn_ws:     String,
    assets:    Vec<String>,
    bn_prices: BnPrices,
    bn_hist:   BnHistory,
) {
    loop {
        info!("[BN] Connecting to {}", bn_ws);

        match connect_bn_feed(&bn_ws, &assets, &bn_prices, &bn_hist).await {
            Ok(_)  => warn!("[BN] Feed closed cleanly, reconnecting..."),
            Err(e) => error!("[BN] Feed error: {}, reconnecting in {}s", e, WS_RECONNECT_SECS),
        }

        tokio::time::sleep(Duration::from_secs(WS_RECONNECT_SECS)).await;
    }
}

fn bn_symbol(asset: &str) -> String {
    format!("{}usdt", asset)
}

fn bn_asset(sym: &str) -> Option<&'static str> {
    match sym {
        "btcusdt" | "BTCUSDT" => Some("btc"),
        "ethusdt" | "ETHUSDT" => Some("eth"),
        "solusdt" | "SOLUSDT" => Some("sol"),
        "xrpusdt" | "XRPUSDT" => Some("xrp"),
        _ => None,
    }
}

async fn connect_bn_feed(
    bn_ws:     &str,
    assets:    &[String],
    bn_prices: &BnPrices,
    bn_hist:   &BnHistory,
) -> Result<()> {
    let streams: Vec<String> = assets.iter()
        .map(|a| format!("{}@aggTrade", bn_symbol(a)))
        .collect();
    let url = format!("{}/{}", bn_ws, streams.join("/"));
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_async(&url),
    ).await.context("BN WS connect timed out")?.context("BN WS connect failed")?;
    info!("[BN] Connected, watching {} assets", assets.len());

    let mut last_data = tokio::time::Instant::now();
    const STALE_TIMEOUT: Duration = Duration::from_secs(30);

    let mut stale_check = tokio::time::interval(Duration::from_secs(5));
    stale_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = stale_check.tick() => {
                if last_data.elapsed() > STALE_TIMEOUT {
                    warn!("[BN] Feed stale — no trade data for {:.0}s, forcing reconnect",
                        last_data.elapsed().as_secs_f64());
                    break;
                }
            }
            msg = ws.next() => {
        match msg {
            Some(Ok(Message::Text(t))) => {
                let d: serde_json::Value = match serde_json::from_str(&t) {
                    Ok(d) => d,
                    _ => continue,
                };
                let inner = d.get("data").unwrap_or(&d);
                let sym = inner.get("s")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_lowercase();
                let px = inner.get("p")
                    .and_then(|p| p.as_str().and_then(|s| s.parse::<f64>().ok()));

                if let (Some(asset), Some(price)) = (bn_asset(&sym), px) {
                    if price > 0.0 {
                        last_data = tokio::time::Instant::now();
                        let is_new = !bn_prices.contains_key(asset);
                        bn_prices.insert(asset.to_string(), price);
                        if is_new {
                            info!("[BN] First price for {}: {:.2}", asset.to_uppercase(), price);
                        }

                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs_f64();
                        let mut h = bn_hist.entry(asset.to_string()).or_default();
                        h.push_back((now, price));
                        if h.len() > 14400 {
                            h.pop_front();
                        }
                    }
                }
            }
            Some(Ok(Message::Ping(data))) => {
                let _ = ws.send(Message::Pong(data)).await;
            }
            Some(Ok(Message::Close(frame))) => {
                warn!("[BN] WS close frame: {:?}", frame);
                break;
            }
            Some(Err(e)) => return Err(anyhow::anyhow!("BN WS error: {}", e)),
            Some(Ok(_)) => {}
            None => break,
        }
            }
        }
    }

    Ok(())
}

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

        entry.apply_level(0.52, 0.0, true, 1001.0);
        assert_eq!(entry.best_ask, 0.55, "after removing 0.52, best ask should be 0.55");

        entry.apply_level(0.51, 5.0, true, 1002.0);
        assert_eq!(entry.best_ask, 0.51);

        entry.apply_level(0.49, 10.0, false, 1003.0);
        assert_eq!(entry.best_bid, 0.50, "0.50 still best bid");

        entry.apply_level(0.50, 0.0, false, 1004.0);
        assert_eq!(entry.best_bid, 0.49);
    }

    #[test]
    fn book_entry_delta_does_not_corrupt() {
        let mut entry = BookEntry::new();
        entry.set_snapshot(
            vec![(0.52, 3.0), (0.55, 10.0), (0.60, 20.0)],
            vec![(0.50, 5.0), (0.48, 8.0)],
            1000.0,
        );
        assert_eq!(entry.asks.len(), 3);
        assert_eq!(entry.bids.len(), 2);

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

        let snap = serde_json::json!({
            "event_type": "book",
            "asset_id": "token123",
            "asks": [{"price": "0.52", "size": "3"}, {"price": "0.55", "size": "10"}],
            "bids": [{"price": "0.50", "size": "5"}, {"price": "0.48", "size": "8"}]
        });
        process_book_message(&snap, &book_state);

        let delta = serde_json::json!({
            "event_type": "price_change",
            "market": "0xabc",
            "timestamp": "1753314088351",
            "price_changes": [
                {"asset_id": "token123", "price": "0.52", "size": "0", "side": "SELL",
                 "hash": "abc", "best_bid": "0.50", "best_ask": "0.55"},
                {"asset_id": "token123", "price": "0.49", "size": "12", "side": "BUY",
                 "hash": "def", "best_bid": "0.50", "best_ask": "0.55"}
            ]
        });
        process_book_message(&delta, &book_state);

        let entry = book_state.get("token123").unwrap();
        assert_eq!(entry.best_ask, 0.55, "best ask should now be 0.55 after removing 0.52");
        assert_eq!(entry.best_bid, 0.50, "best bid still 0.50");
    }
}
