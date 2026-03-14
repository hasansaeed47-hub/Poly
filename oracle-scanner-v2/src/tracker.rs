/// tracker.rs -- Whale / Smart-Money Tracker
///
/// Polls Polymarket leaderboard for top crypto wallets, then polls their
/// trades on today's active short-term markets. Logs large trades ($5k+)
/// to data/whale_trades_{date}.jsonl.
///
/// Data sections:
///   - whale_trades:  individual large trades by tracked wallets
///   - whale_wallets: the leaderboard snapshot (refreshed periodically)

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

// -- Config -------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TrackerConfig {
    pub data_api:          String,
    pub clob_rest:         String,
    pub leaderboard_limit: usize,  // top N wallets to track (20-50)
    pub min_trade_usd:     f64,    // minimum trade size to log ($5000)
    pub poll_interval_ms:  u64,    // how often to poll trades (10s default)
    pub refresh_wallets_s: u64,    // how often to refresh leaderboard (300s = 5min)
}

impl Default for TrackerConfig {
    fn default() -> Self {
        Self {
            data_api:          "https://data-api.polymarket.com".to_string(),
            clob_rest:         "https://clob.polymarket.com".to_string(),
            leaderboard_limit: 50,
            min_trade_usd:     5000.0,
            poll_interval_ms:  10_000,
            refresh_wallets_s: 300,
        }
    }
}

// -- Log types ----------------------------------------------------------------

#[derive(Serialize, Clone, Debug)]
pub struct WhaleTradeLog {
    pub ts:               f64,
    pub wallet:           String,
    pub wallet_rank:      u32,       // leaderboard rank
    pub wallet_pnl:       f64,       // total PNL from leaderboard
    pub market_slug:      String,    // which market
    pub asset:            String,
    pub tf:               u32,       // timeframe in minutes
    pub side:             String,    // "BUY" or "SELL"
    pub outcome:          String,    // "YES" or "NO" (which token)
    pub price:            f64,
    pub size:             f64,       // number of shares
    pub usd_value:        f64,       // price * size
    pub trade_id:         String,
    pub condition_id:     String,
    pub token_id:         String,
    pub taker_or_maker:   String,    // "TAKER" or "MAKER"
}

#[derive(Serialize, Clone, Debug)]
pub struct WhaleWalletLog {
    pub ts:           f64,
    pub rank:         u32,
    pub address:      String,
    pub pnl:          f64,
    pub volume:       f64,
    pub markets:      u64,
    pub display_name: String,
}

// -- Leaderboard API response -------------------------------------------------

#[derive(Deserialize, Debug)]
struct LeaderboardEntry {
    #[serde(default, alias = "userAddress", alias = "proxyWallet")]
    address:      Option<String>,
    #[serde(default, alias = "proxy_wallet")]
    proxy_wallet: Option<String>,
    #[serde(default)]
    pnl:          Option<f64>,
    #[serde(default, alias = "totalVolume")]
    volume:       Option<f64>,
    #[serde(default, alias = "numMarkets")]
    markets:      Option<u64>,
    #[serde(default, alias = "displayName", alias = "username")]
    display_name: Option<String>,
}

// -- Tracked wallet -----------------------------------------------------------

#[derive(Clone, Debug)]
struct TrackedWallet {
    address: String,
    rank:    u32,
    pnl:     f64,
    volume:  f64,
    name:    String,
}

// -- Active market info passed from main --------------------------------------

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ActiveMarketInfo {
    pub slug:         String,
    pub asset:        String,
    pub tf:           u32,
    pub condition_id: String,
    pub token_yes:    String,
    pub token_no:     String,
}

// -- Tracker state ------------------------------------------------------------

pub struct WhaleTracker {
    cfg:            TrackerConfig,
    client:         Client,
    wallets:        Vec<TrackedWallet>,
    seen_set:       HashSet<String>,        // O(1) lookup for dedup
    seen_queue:     VecDeque<String>,        // FIFO order for eviction
    trade_count:    Arc<AtomicU64>,
    wallet_count:   Arc<AtomicU64>,
    last_refresh:   f64,
}

impl WhaleTracker {
    pub fn new(
        cfg: TrackerConfig,
        client: Client,
        trade_count: Arc<AtomicU64>,
        wallet_count: Arc<AtomicU64>,
    ) -> Self {
        Self {
            cfg,
            client,
            wallets: Vec::new(),
            seen_set: HashSet::new(),
            seen_queue: VecDeque::new(),
            trade_count,
            wallet_count,
            last_refresh: 0.0,
        }
    }

    /// Insert a trade ID into the dedup set with FIFO eviction.
    fn mark_seen(&mut self, id: String) {
        if self.seen_set.insert(id.clone()) {
            self.seen_queue.push_back(id);
        }
        // FIFO eviction: remove oldest entries when over capacity
        while self.seen_set.len() > 50_000 {
            if let Some(old) = self.seen_queue.pop_front() {
                self.seen_set.remove(&old);
            } else {
                break;
            }
        }
    }

    fn is_seen(&self, id: &str) -> bool {
        self.seen_set.contains(id)
    }

    fn now_secs() -> f64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64()
    }

    fn today() -> String {
        chrono::Utc::now().format("%Y-%m-%d").to_string()
    }

    /// Fetch top crypto wallets from leaderboard
    async fn refresh_wallets(&mut self) -> Result<()> {
        // Try multiple leaderboard URL patterns (API has evolved over time)
        let urls = [
            format!("{}/leaderboard?tag=crypto&limit={}", self.cfg.data_api, self.cfg.leaderboard_limit),
            format!("{}/leaderboard?category=CRYPTO&orderBy=PNL&limit={}", self.cfg.data_api, self.cfg.leaderboard_limit),
            format!("{}/v1/leaderboard?category=CRYPTO&orderBy=PNL&limit={}", self.cfg.data_api, self.cfg.leaderboard_limit),
        ];

        let mut entries: Vec<LeaderboardEntry> = Vec::new();

        for url in &urls {
            debug!("[WHALE] Trying leaderboard: {}", url);
            match self.client.get(url)
                .timeout(Duration::from_secs(10))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    // Response might be array directly or { data: [...] }
                    let text = resp.text().await.unwrap_or_default();
                    if let Ok(arr) = serde_json::from_str::<Vec<LeaderboardEntry>>(&text) {
                        entries = arr;
                        info!("[WHALE] Leaderboard loaded from {} ({} entries)", url, entries.len());
                        break;
                    }
                    // Try nested format
                    if let Ok(obj) = serde_json::from_str::<serde_json::Value>(&text) {
                        if let Some(data) = obj.get("data").or(obj.get("leaderboard")) {
                            if let Ok(arr) = serde_json::from_value::<Vec<LeaderboardEntry>>(data.clone()) {
                                entries = arr;
                                info!("[WHALE] Leaderboard loaded (nested) from {} ({} entries)", url, entries.len());
                                break;
                            }
                        }
                    }
                    debug!("[WHALE] URL {} returned success but unparseable: {}",
                        url, &text[..text.len().min(200)]);
                }
                Ok(resp) => {
                    debug!("[WHALE] URL {} returned {}", url, resp.status());
                }
                Err(e) => {
                    debug!("[WHALE] URL {} failed: {}", url, e);
                }
            }
        }

        if entries.is_empty() {
            warn!("[WHALE] No leaderboard entries from any endpoint");
            return Ok(());
        }

        // Convert to tracked wallets
        let mut new_wallets: Vec<TrackedWallet> = Vec::new();
        let now = Self::now_secs();
        let date = Self::today();
        let path = format!("data/whale_wallets_{}.jsonl", date);

        for (i, entry) in entries.iter().enumerate() {
            let addr = entry.address.as_deref()
                .or(entry.proxy_wallet.as_deref())
                .unwrap_or_default()
                .to_string();
            if addr.is_empty() { continue; }

            let wallet = TrackedWallet {
                address: addr.clone(),
                rank: (i + 1) as u32,
                pnl: entry.pnl.unwrap_or(0.0),
                volume: entry.volume.unwrap_or(0.0),
                name: entry.display_name.clone().unwrap_or_default(),
            };

            // Log wallet snapshot
            let log = WhaleWalletLog {
                ts: now,
                rank: wallet.rank,
                address: wallet.address.clone(),
                pnl: wallet.pnl,
                volume: wallet.volume,
                markets: entry.markets.unwrap_or(0),
                display_name: wallet.name.clone(),
            };
            if let Ok(line) = serde_json::to_string(&log) {
                if let Ok(mut f) = open_append(&path) {
                    let _ = writeln!(f, "{}", line);
                }
            }

            new_wallets.push(wallet);
        }

        info!("[WHALE] Tracking {} wallets (top by PNL)", new_wallets.len());
        self.wallet_count.store(new_wallets.len() as u64, Ordering::Relaxed);
        self.wallets = new_wallets;
        self.last_refresh = now;

        Ok(())
    }

    /// Poll trades for tracked wallets on active markets.
    /// Uses the public /live-activity/events/{condition_id} endpoint which
    /// returns user.address per trade — one call per market, no auth needed.
    async fn poll_trades(&mut self, active_markets: &[ActiveMarketInfo]) -> Result<Vec<WhaleTradeLog>> {
        if self.wallets.is_empty() || active_markets.is_empty() {
            return Ok(Vec::new());
        }

        let now = Self::now_secs();
        let mut all_trades: Vec<WhaleTradeLog> = Vec::new();

        let wallet_set: HashMap<String, TrackedWallet> = self.wallets.iter()
            .map(|w| (w.address.to_lowercase(), w.clone()))
            .collect();

        // Deduplicate condition_ids (multiple slugs may share same condition)
        let mut seen_conditions: HashSet<String> = HashSet::new();

        for market in active_markets {
            if !seen_conditions.insert(market.condition_id.clone()) { continue; }
            if market.condition_id.is_empty() { continue; }

            let events = self.fetch_live_activity(&market.condition_id).await;
            let events = match events {
                Ok(e) => e,
                Err(e) => {
                    debug!("[WHALE] live-activity failed for {}: {}", market.slug, e);
                    continue;
                }
            };

            for event in &events {
                // Dedup by transaction_hash (most reliable unique ID)
                let tx_hash = event.get("transaction_hash")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                if tx_hash.is_empty() { continue; }
                if self.is_seen(&tx_hash) { continue; }

                // Extract user address from nested user object
                let user_addr = event.get("user")
                    .and_then(|u| u.get("address"))
                    .and_then(|a| a.as_str())
                    .unwrap_or_default()
                    .to_lowercase();

                // Check if this is a tracked wallet
                let wallet = match wallet_set.get(&user_addr) {
                    Some(w) => w,
                    None => {
                        self.mark_seen(tx_hash);
                        continue;
                    }
                };

                let price = parse_f64_json(event.get("price"));
                let size = parse_f64_json(event.get("size"));
                let usd_value = price * size;

                // Always dedup, even if below threshold
                self.mark_seen(tx_hash.clone());

                if usd_value < self.cfg.min_trade_usd { continue; }

                let side = event.get("side")
                    .and_then(|s| s.as_str())
                    .unwrap_or("UNKNOWN")
                    .to_string();

                let outcome = event.get("outcome")
                    .and_then(|o| o.as_str())
                    .unwrap_or("UNKNOWN")
                    .to_string();

                // Determine which token was traded
                let asset_id = event.get("market")
                    .and_then(|m| m.get("asset_id"))
                    .and_then(|a| a.as_str())
                    .unwrap_or_default()
                    .to_string();

                // Map outcome: UP->YES, DOWN->NO (Polymarket naming)
                let outcome_mapped = if outcome.eq_ignore_ascii_case("Up") { "YES" }
                    else if outcome.eq_ignore_ascii_case("Down") { "NO" }
                    else { &outcome };

                let username = event.get("user")
                    .and_then(|u| u.get("pseudonym")
                        .or(u.get("username")))
                    .and_then(|n| n.as_str())
                    .unwrap_or_default();

                info!("[WHALE] ${:.0} {} {} on {} by rank #{} {} (PNL ${:.0})",
                    usd_value, side, outcome_mapped, market.slug,
                    wallet.rank,
                    if username.is_empty() { &user_addr[..8.min(user_addr.len())] } else { username },
                    wallet.pnl
                );

                let log = WhaleTradeLog {
                    ts: now,
                    wallet: wallet.address.clone(),
                    wallet_rank: wallet.rank,
                    wallet_pnl: wallet.pnl,
                    market_slug: market.slug.clone(),
                    asset: market.asset.clone(),
                    tf: market.tf,
                    side,
                    outcome: outcome_mapped.to_string(),
                    price,
                    size,
                    usd_value,
                    trade_id: tx_hash,
                    condition_id: market.condition_id.clone(),
                    token_id: asset_id,
                    taker_or_maker: "TAKER".to_string(), // live-activity doesn't distinguish
                };

                all_trades.push(log);
                self.trade_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(all_trades)
    }

    /// Fetch recent trade events via public CLOB endpoint.
    /// GET /live-activity/events/{condition_id}
    /// Returns array of trade events with user.address, side, size, price, etc.
    /// No auth required.
    async fn fetch_live_activity(&self, condition_id: &str) -> Result<Vec<serde_json::Value>> {
        let url = format!("{}/live-activity/events/{}", self.cfg.clob_rest, condition_id);

        let resp = self.client.get(&url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .context("CLOB live-activity request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            debug!("[WHALE] live-activity returned {} for {}", status, condition_id);
            // Fallback: try older trades endpoint by token_id
            return Ok(Vec::new());
        }

        let text = resp.text().await.unwrap_or_default();

        // Response is typically an array of trade event objects
        if let Ok(events) = serde_json::from_str::<Vec<serde_json::Value>>(&text) {
            return Ok(events);
        }

        // Or might be wrapped: { "data": [...] } or { "events": [...] }
        if let Ok(obj) = serde_json::from_str::<serde_json::Value>(&text) {
            for key in &["data", "events", "trades"] {
                if let Some(arr) = obj.get(key) {
                    if let Ok(events) = serde_json::from_value::<Vec<serde_json::Value>>(arr.clone()) {
                        return Ok(events);
                    }
                }
            }
            // If it's an object with trade-like fields, wrap it
            if obj.get("transaction_hash").is_some() {
                return Ok(vec![obj]);
            }
        }

        Ok(Vec::new())
    }

    /// Write trade logs to disk
    fn write_trades(trades: &[WhaleTradeLog]) {
        if trades.is_empty() { return; }
        let date = Self::today();
        let path = format!("data/whale_trades_{}.jsonl", date);
        if let Ok(mut f) = open_append(&path) {
            for trade in trades {
                if let Ok(line) = serde_json::to_string(trade) {
                    let _ = writeln!(f, "{}", line);
                }
            }
        }
    }
}

// -- Main tracker loop (spawned as tokio task) --------------------------------

pub async fn run_whale_tracker(
    cfg: TrackerConfig,
    client: Client,
    trade_count: Arc<AtomicU64>,
    wallet_count: Arc<AtomicU64>,
    active_markets: Arc<tokio::sync::RwLock<Vec<ActiveMarketInfo>>>,
) {
    let mut tracker = WhaleTracker::new(cfg.clone(), client, trade_count, wallet_count);

    // Ensure data dir exists (may run before main creates it)
    let _ = std::fs::create_dir_all("data");

    info!("[WHALE] Starting whale tracker (poll={}ms, min=${}, top={})",
        cfg.poll_interval_ms, cfg.min_trade_usd, cfg.leaderboard_limit);

    // Initial wallet load
    if let Err(e) = tracker.refresh_wallets().await {
        error!("[WHALE] Initial wallet load failed: {}", e);
    }

    let poll = Duration::from_millis(cfg.poll_interval_ms);
    let refresh_interval = cfg.refresh_wallets_s as f64;

    loop {
        tokio::time::sleep(poll).await;

        let now = WhaleTracker::now_secs();

        // Refresh wallets periodically
        if now - tracker.last_refresh > refresh_interval {
            if let Err(e) = tracker.refresh_wallets().await {
                warn!("[WHALE] Wallet refresh failed: {}", e);
            }
        }

        // Get current active markets
        let markets = active_markets.read().await.clone();
        if markets.is_empty() { continue; }

        // Poll trades
        match tracker.poll_trades(&markets).await {
            Ok(trades) => {
                if !trades.is_empty() {
                    WhaleTracker::write_trades(&trades);
                }
            }
            Err(e) => {
                debug!("[WHALE] Trade poll error: {}", e);
            }
        }
    }
}

// -- Helpers ------------------------------------------------------------------

fn open_append(path: &str) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
}

/// Parse f64 from JSON value (can be number or string)
fn parse_f64_json(v: Option<&serde_json::Value>) -> f64 {
    match v {
        Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
        Some(serde_json::Value::String(s)) => s.parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    }
}
