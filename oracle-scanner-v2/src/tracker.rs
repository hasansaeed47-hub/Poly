/// tracker.rs -- Whale / Smart-Money Tracker
///
/// Polls Polymarket leaderboard for top crypto wallets, then polls their
/// trades on today's active short-term markets. Logs large trades ($5k+)
/// to data/whale_trades_{date}.jsonl.
///
/// Data sections:
///   - whale_trades:  individual large trades by tracked wallets
///   - whale_wallets: the leaderboard snapshot (refreshed periodically)

use std::collections::{HashMap, HashSet};
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

// -- Trades API response ------------------------------------------------------

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct TradeEntry {
    #[serde(default)]
    id:            Option<String>,
    #[serde(default, alias = "tradeId")]
    trade_id:      Option<String>,
    #[serde(default)]
    maker:         Option<String>,
    #[serde(default)]
    taker:         Option<String>,
    #[serde(default, alias = "makerAddress")]
    maker_address: Option<String>,
    #[serde(default, alias = "takerAddress")]
    taker_address: Option<String>,
    #[serde(default)]
    price:         Option<serde_json::Value>,  // can be string or float
    #[serde(default)]
    size:          Option<serde_json::Value>,
    #[serde(default)]
    side:          Option<String>,  // "BUY" or "SELL"
    #[serde(default)]
    outcome:       Option<String>,
    #[serde(default, alias = "asset_id", alias = "assetId")]
    token_id:      Option<String>,
    #[serde(default, alias = "conditionId", alias = "condition_id")]
    condition_id:  Option<String>,
    #[serde(default)]
    timestamp:     Option<serde_json::Value>,
    #[serde(default, alias = "matchTime")]
    match_time:    Option<serde_json::Value>,
    #[serde(default, alias = "market")]
    market_slug:   Option<String>,
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
    seen_trades:    HashSet<String>,        // dedup by trade_id
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
            seen_trades: HashSet::new(),
            trade_count,
            wallet_count,
            last_refresh: 0.0,
        }
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

    /// Poll trades for tracked wallets on active markets
    async fn poll_trades(&mut self, active_markets: &[ActiveMarketInfo]) -> Result<Vec<WhaleTradeLog>> {
        if self.wallets.is_empty() || active_markets.is_empty() {
            return Ok(Vec::new());
        }

        let now = Self::now_secs();
        let mut all_trades: Vec<WhaleTradeLog> = Vec::new();

        // Strategy: poll trades by token_id (more efficient than per-wallet)
        // Then filter for our tracked wallets
        let wallet_set: HashMap<String, &TrackedWallet> = self.wallets.iter()
            .map(|w| (w.address.to_lowercase(), w))
            .collect();

        for market in active_markets {
            // Poll trades for YES token
            for (token_id, outcome) in [
                (&market.token_yes, "YES"),
                (&market.token_no, "NO"),
            ] {
                let trades = self.fetch_recent_trades(token_id).await;
                let trades = match trades {
                    Ok(t) => t,
                    Err(e) => {
                        debug!("[WHALE] trade fetch failed for {}: {}", market.slug, e);
                        continue;
                    }
                };

                for trade in &trades {
                    let trade_id = trade.id.as_deref()
                        .or(trade.trade_id.as_deref())
                        .unwrap_or_default()
                        .to_string();
                    if trade_id.is_empty() { continue; }
                    if self.seen_trades.contains(&trade_id) { continue; }

                    // Check if maker or taker is a tracked wallet
                    let maker_addr = trade.maker.as_deref()
                        .or(trade.maker_address.as_deref())
                        .unwrap_or_default()
                        .to_lowercase();
                    let taker_addr = trade.taker.as_deref()
                        .or(trade.taker_address.as_deref())
                        .unwrap_or_default()
                        .to_lowercase();

                    let (wallet, taker_or_maker) = if let Some(w) = wallet_set.get(&maker_addr) {
                        (*w, "MAKER")
                    } else if let Some(w) = wallet_set.get(&taker_addr) {
                        (*w, "TAKER")
                    } else {
                        continue; // not a tracked wallet
                    };

                    let price = parse_f64_value(trade.price.as_ref());
                    let size = parse_f64_value(trade.size.as_ref());
                    let usd_value = price * size;

                    // Filter by minimum trade size
                    if usd_value < self.cfg.min_trade_usd {
                        self.seen_trades.insert(trade_id);
                        continue;
                    }

                    let side = trade.side.clone().unwrap_or_else(|| "UNKNOWN".to_string());

                    info!("[WHALE] {} ${:.0} {} {} on {} (rank #{}, PNL ${:.0})",
                        taker_or_maker,
                        usd_value, &side, outcome, market.slug,
                        wallet.rank, wallet.pnl
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
                        outcome: outcome.to_string(),
                        price,
                        size,
                        usd_value,
                        trade_id: trade_id.clone(),
                        condition_id: market.condition_id.clone(),
                        token_id: token_id.clone(),
                        taker_or_maker: taker_or_maker.to_string(),
                    };

                    all_trades.push(log);
                    self.seen_trades.insert(trade_id);
                    self.trade_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Prevent unbounded growth of seen_trades (keep last 50k)
        if self.seen_trades.len() > 50_000 {
            let to_remove: Vec<String> = self.seen_trades.iter()
                .take(self.seen_trades.len() - 25_000)
                .cloned()
                .collect();
            for id in to_remove {
                self.seen_trades.remove(&id);
            }
        }

        Ok(all_trades)
    }

    /// Fetch recent trades for a token_id from CLOB API
    async fn fetch_recent_trades(&self, token_id: &str) -> Result<Vec<TradeEntry>> {
        // CLOB endpoint: GET /trades?asset_id={token_id}
        let url = format!("{}/trades?asset_id={}", self.cfg.clob_rest, token_id);

        let resp = self.client.get(&url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .context("CLOB trades request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            debug!("[WHALE] trades endpoint returned {} for {}: {}",
                status, token_id, &body[..body.len().min(200)]);

            // Fallback: try data-api trades endpoint
            return self.fetch_trades_data_api(token_id).await;
        }

        let text = resp.text().await.unwrap_or_default();

        // Response might be array or { trades: [...] } or { data: [...] }
        if let Ok(trades) = serde_json::from_str::<Vec<TradeEntry>>(&text) {
            return Ok(trades);
        }

        if let Ok(obj) = serde_json::from_str::<serde_json::Value>(&text) {
            for key in &["trades", "data", "results"] {
                if let Some(arr) = obj.get(key) {
                    if let Ok(trades) = serde_json::from_value::<Vec<TradeEntry>>(arr.clone()) {
                        return Ok(trades);
                    }
                }
            }
        }

        Ok(Vec::new())
    }

    /// Fallback: use data-api for trades
    async fn fetch_trades_data_api(&self, token_id: &str) -> Result<Vec<TradeEntry>> {
        let url = format!("{}/trades?asset_id={}&limit=100", self.cfg.data_api, token_id);

        let resp = self.client.get(&url)
            .timeout(Duration::from_secs(5))
            .send()
            .await;

        match resp {
            Ok(r) if r.status().is_success() => {
                let text = r.text().await.unwrap_or_default();
                if let Ok(trades) = serde_json::from_str::<Vec<TradeEntry>>(&text) {
                    return Ok(trades);
                }
                if let Ok(obj) = serde_json::from_str::<serde_json::Value>(&text) {
                    for key in &["trades", "data"] {
                        if let Some(arr) = obj.get(key) {
                            if let Ok(trades) = serde_json::from_value::<Vec<TradeEntry>>(arr.clone()) {
                                return Ok(trades);
                            }
                        }
                    }
                }
            }
            _ => {}
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

fn parse_f64_value(v: Option<&serde_json::Value>) -> f64 {
    match v {
        Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
        Some(serde_json::Value::String(s)) => s.parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    }
}
