//! CL LAG BOT — A/B TEST
//! Runs two strategies in parallel on identical signals:
//!   Engine A: postOnly maker, min_edge=0.25 (current)
//!   Engine B: taker IOC,      min_edge=0.27 (taker fee compensation)
//! Both paper trade. All results logged to ab_results.jsonl.

mod fair_value;

use anyhow::{bail, Context, Result};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{watch, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

fn now() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64()
}

fn rand01() -> f64 {
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as f64;
    (ns % 99991.0) / 99991.0
}

// ═══════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════

#[derive(Clone, Debug)]
struct Config {
    gamma_url:    &'static str,
    rtds_url:     &'static str,
    clob_ws:      &'static str,
    // Engine A — maker postOnly
    a_min_edge:   f64,
    a_fill_prob:  f64,  // ~25% maker fill rate
    // Engine B — taker IOC
    b_min_edge:   f64,
    b_fill_prob:  f64,  // ~92% taker fill rate
    b_taker_fee:  f64,  // 2% of stake
    // Shared
    min_secs:     f64,
    max_secs:     f64,
    max_book_age: f64,
    stake:        f64,
    max_open:     usize,
    log_file:     &'static str,
}

impl Config {
    fn default() -> Self {
        Self {
            gamma_url:    "https://gamma-api.polymarket.com",
            rtds_url:     "wss://ws-live-data.polymarket.com",
            clob_ws:      "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            a_min_edge:   0.25,
            a_fill_prob:  0.25,
            b_min_edge:   0.27,
            b_fill_prob:  0.92,
            b_taker_fee:  0.02,
            min_secs:     60.0,
            max_secs:     300.0,
            max_book_age: 3.0,
            stake:        5.0,
            max_open:     6,
            log_file:     "/opt/polybot/cl_lag/ab_results.jsonl",
        }
    }
}

// ═══════════════════════════════════════════════════════════════
// FEEDS — identical to main bot
// ═══════════════════════════════════════════════════════════════

#[derive(Debug, Default)]
struct ClEntry {
    price:   f64,
    last_ts: f64,
    history: VecDeque<(f64, f64)>,
}
impl ClEntry {
    fn update(&mut self, price: f64) {
        let t = now();
        self.price   = price;
        self.last_ts = t;
        self.history.push_back((t, price));
        while self.history.len() > 300 { self.history.pop_front(); }
    }
    fn is_stale(&self) -> bool { now() - self.last_ts > 15.0 }
    fn realized_sigma(&self) -> f64 {
        let pts: Vec<_> = self.history.iter().collect();
        if pts.len() < 10 { return 0.0; }
        let sq: Vec<f64> = pts.windows(2).filter_map(|w| {
            let dt = w[1].0 - w[0].0;
            if dt <= 0.0 { return None; }
            Some(((w[1].1 - w[0].1).abs() / dt.sqrt()).powi(2))
        }).collect();
        if sq.is_empty() { return 0.0; }
        (sq.iter().sum::<f64>() / sq.len() as f64).sqrt()
    }
}

type ClState   = Arc<DashMap<&'static str, ClEntry>>;
type BnState   = Arc<DashMap<&'static str, (f64, f64)>>;
type BookState = Arc<DashMap<String, (f64, f64, f64, u64)>>;

fn new_cl() -> ClState {
    let m = DashMap::new();
    for a in ["btc","eth","sol"] { m.insert(a, ClEntry::default()); }
    Arc::new(m)
}
fn new_bn() -> BnState {
    let m = DashMap::new();
    for a in ["btc","eth","sol"] { m.insert(a, (0.0, 0.0)); }
    Arc::new(m)
}

async fn run_cl_feed(state: ClState, url: &'static str) {
    loop {
        match cl_connect(state.clone(), url).await {
            Ok(_)  => warn!("[CL] disconnected"),
            Err(e) => warn!("[CL] error: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn cl_connect(state: ClState, url: &str) -> Result<()> {
    let (ws, _) = connect_async(url).await?;
    info!("[CL] connected");
    let (mut tx, mut rx) = ws.split();
    tx.send(Message::Text(json!({
        "action": "subscribe",
        "subscriptions": [{"topic": "crypto_prices_chainlink", "type": "*", "filters": ""}]
    }).to_string())).await?;
    tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_secs(5));
        loop {
            t.tick().await;
            if tx.send(Message::Text(json!({"action":"ping"}).to_string())).await.is_err() { break; }
        }
    });
    while let Some(msg) = rx.next().await {
        if let Message::Text(text) = msg? {
            let v: Value = match serde_json::from_str(&text) { Ok(v) => v, Err(_) => continue };
            if v.get("topic").and_then(|t| t.as_str()) != Some("crypto_prices_chainlink") { continue; }
            let sym = match v.get("payload").and_then(|p| p.get("symbol")).and_then(|s| s.as_str()) {
                Some(s) => s.to_lowercase(), None => continue,
            };
            let price = match v.get("payload").and_then(|p| p.get("value")).and_then(|v| v.as_f64()) {
                Some(p) if p > 0.0 => p, _ => continue,
            };
            let asset: &'static str = if sym.contains("btc") { "btc" }
                else if sym.contains("eth") { "eth" }
                else if sym.contains("sol") { "sol" }
                else { continue };
            if let Some(mut e) = state.get_mut(asset) {
                let first = e.price == 0.0;
                e.update(price);
                if first { info!("[CL] first tick: {asset}={price:.4}"); }
            }
        }
    }
    Ok(())
}

async fn run_bn_feed(state: BnState) {
    const URL: &str = "wss://stream.binance.com:9443/ws/btcusdt@markPrice/ethusdt@markPrice/solusdt@markPrice";
    loop {
        if let Err(e) = bn_connect(state.clone(), URL).await { warn!("[BN] error: {e}"); }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn bn_connect(state: BnState, url: &str) -> Result<()> {
    let (ws, _) = connect_async(url).await?;
    info!("[BN] connected");
    let (_, mut rx) = ws.split();
    while let Some(msg) = rx.next().await {
        if let Message::Text(text) = msg? {
            let v: Value = match serde_json::from_str(&text) { Ok(v) => v, Err(_) => continue };
            if v.get("e").and_then(|e| e.as_str()) != Some("markPriceUpdate") { continue; }
            let sym   = v.get("s").and_then(|s| s.as_str()).unwrap_or("").to_lowercase();
            let price = match v.get("p").and_then(|p| p.as_str()).and_then(|p| p.parse::<f64>().ok()) {
                Some(p) if p > 0.0 => p, _ => continue,
            };
            let asset: &'static str = match sym.as_str() {
                "btcusdt" => "btc", "ethusdt" => "eth", "solusdt" => "sol", _ => continue,
            };
            if let Some(mut e) = state.get_mut(asset) { *e = (price, now()); }
        }
    }
    Ok(())
}

async fn run_book_feed(state: BookState, mut token_rx: watch::Receiver<Vec<String>>, ws_url: &'static str) {
    loop {
        let tokens = token_rx.borrow_and_update().clone();
        if tokens.is_empty() { tokio::time::sleep(Duration::from_secs(2)).await; continue; }
        if let Err(e) = book_connect(state.clone(), tokens, ws_url).await { warn!("[BOOK] error: {e}"); }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn book_connect(state: BookState, tokens: Vec<String>, url: &str) -> Result<()> {
    let (ws, _) = connect_async(url).await?;
    info!("[BOOK] connected — {} tokens", tokens.len());
    let (mut tx, mut rx) = ws.split();
    tx.send(Message::Text(json!({"assets_ids": tokens, "type": "subscribe"}).to_string())).await?;
    tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_secs(5));
        loop {
            t.tick().await;
            if tx.send(Message::Text(json!({"action":"ping"}).to_string())).await.is_err() { break; }
        }
    });
    while let Some(msg) = rx.next().await {
        if let Message::Text(text) = msg? {
            let v: Value = match serde_json::from_str(&text) { Ok(v) => v, Err(_) => continue };
            let items: Vec<&Value> = match &v {
                Value::Array(a) => a.iter().collect(),
                Value::Object(_) => vec![&v],
                _ => continue,
            };
            for item in items { book_process(item, &state); }
        }
    }
    Ok(())
}

fn book_process(item: &Value, state: &BookState) {
    let ev = item.get("event_type").and_then(|v| v.as_str()).unwrap_or("");
    if matches!(ev, "pong" | "subscribed" | "") { return; }
    match ev {
        "book" => {
            let token_id = match item.get("asset_id").and_then(|v| v.as_str()) {
                Some(id) => id.to_string(), None => return,
            };
            let mut entry = state.entry(token_id).or_insert((0.0, 0.0, 0.0, 0));
            if let Some(bids) = item.get("bids").and_then(|b| b.as_array()) {
                if let Some(bid) = best_price(bids, false) { entry.1 = bid; }
            }
            if let Some(asks) = item.get("asks").and_then(|a| a.as_array()) {
                if let Some(ask) = best_price(asks, true) { entry.0 = ask; }
            }
            entry.2 = now();
            entry.3 += 1;
        }
        "price_change" => {
            if let Some(changes) = item.get("price_changes").and_then(|c| c.as_array()) {
                for ch in changes {
                    let token_id = match ch.get("asset_id").and_then(|v| v.as_str()) {
                        Some(id) => id.to_string(), None => continue,
                    };
                    let price = pf64(ch.get("price"));
                    if price <= 0.0 || price >= 1.0 { continue; }
                    if let Some(mut e) = state.get_mut(&token_id) { e.2 = now(); e.3 += 1; }
                }
            }
        }
        _ => {}
    }
}

fn best_price(levels: &[Value], is_ask: bool) -> Option<f64> {
    let prices: Vec<f64> = levels.iter().filter_map(|lv| {
        let (p, s) = parse_level(lv)?;
        if s > 0.0 { Some(p) } else { None }
    }).collect();
    if is_ask { prices.into_iter().reduce(f64::min) }
    else      { prices.into_iter().reduce(f64::max) }
}
fn parse_level(lv: &Value) -> Option<(f64, f64)> {
    match lv {
        Value::Object(o) => Some((pf64(o.get("price")), pf64(o.get("size")))),
        Value::Array(a) if a.len() >= 2 => Some((pf64(Some(&a[0])), pf64(Some(&a[1])))),
        _ => None,
    }
}
fn pf64(v: Option<&Value>) -> f64 {
    match v { Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0), Some(Value::String(s)) => s.parse().unwrap_or(0.0), _ => 0.0 }
}

// ═══════════════════════════════════════════════════════════════
// WINDOWS
// ═══════════════════════════════════════════════════════════════

#[derive(Clone, Debug)]
struct Window {
    asset:      &'static str,
    slug:       String,
    yes_token:  String,
    no_token:   String,
    open_ts:    u64,
    close_ts:   u64,
    open_price: Option<f64>,
}
impl Window {
    fn secs_left(&self) -> f64 { (self.close_ts as f64 - now()).max(0.0) }
    fn is_active(&self) -> bool { let n = now(); n >= self.open_ts as f64 && n < self.close_ts as f64 }
    fn annual_vol(&self) -> f64 { match self.asset { "btc" => 0.70, "eth" => 0.80, _ => 1.00 } }
}

async fn fetch_windows(http: &Client, gamma_url: &str) -> Result<Vec<Window>> {
    let now_u = now() as u64;
    let mut out = Vec::new();
    for &(asset, tf) in &[("btc",5u64),("eth",5),("sol",5)] {
        let slot       = tf * 60;
        let slot_start = (now_u / slot) * slot;
        let close_ts   = slot_start + slot;
        let slug = format!("{asset}-updown-{tf}m-{slot_start}");
        let resp = match http.get(format!("{gamma_url}/markets"))
            .query(&[("slug", &slug), ("active", &"true".to_string())])
            .timeout(Duration::from_secs(8)).send().await
        { Ok(r) => r, Err(e) => { warn!("[DISC] {slug}: {e}"); continue; } };
        if resp.status() != 200 { continue; }
        let body: Value = match resp.json().await { Ok(v) => v, Err(_) => continue };
        let markets = match body.as_array().cloned()
            .or_else(|| body.get("markets").and_then(|m| m.as_array()).cloned())
        { Some(m) if !m.is_empty() => m, _ => continue };
        if let Ok((yes_token, no_token)) = parse_tokens(&markets) {
            info!("[DISC] {slug} | {:.0}s left", close_ts as f64 - now());
            out.push(Window { asset, slug, yes_token, no_token, open_ts: slot_start, close_ts, open_price: None });
        }
    }
    info!("[DISC] {} windows", out.len());
    Ok(out)
}

fn parse_tokens(markets: &[Value]) -> Result<(String, String)> {
    let mut yes: Option<String> = None;
    let mut no:  Option<String> = None;
    for m in markets {
        let outcomes:  Vec<String> = serde_json::from_str(m.get("outcomes")   .and_then(|v| v.as_str()).unwrap_or("[]")).unwrap_or_default();
        let token_ids: Vec<String> = serde_json::from_str(m.get("clobTokenIds").and_then(|v| v.as_str()).unwrap_or("[]")).unwrap_or_default();
        if outcomes.len() < 2 || token_ids.len() < 2 { continue; }
        for (i, o) in outcomes.iter().enumerate() {
            let ol = o.to_lowercase();
            if ol.contains("up") || ol.contains("yes") { yes = Some(token_ids[i].clone()); }
            else if ol.contains("down") || ol.contains("no") { no = Some(token_ids[i].clone()); }
        }
    }
    match (yes, no) { (Some(y), Some(n)) => Ok((y, n)), _ => bail!("tokens not found") }
}

fn lock_open_prices(windows: &mut Vec<Window>, cl: &ClState) {
    for w in windows.iter_mut() {
        if w.open_price.is_none() {
            if let Some(e) = cl.get(w.asset) {
                if e.price > 0.0 {
                    w.open_price = Some(e.price);
                    info!("[MAIN] open locked: {} CL={:.4}", w.slug, e.price);
                }
            }
        }
    }
}

fn all_tokens(windows: &[Window]) -> Vec<String> {
    windows.iter().flat_map(|w| [w.yes_token.clone(), w.no_token.clone()]).collect()
}

// ═══════════════════════════════════════════════════════════════
// A/B ENGINES
// ═══════════════════════════════════════════════════════════════

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
enum Direction { Yes, No }

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TradeLog {
    engine:      String,   // "A" or "B"
    slug:        String,
    asset:       String,
    direction:   Direction,
    entry_price: f64,
    fair:        f64,
    edge:        f64,
    secs_left:   f64,
    book_age:    f64,
    stake:       f64,
    filled:      bool,
    fill_prob:   f64,
    outcome:     Option<String>,  // "WIN" | "LOSS" | "UNFILLED"
    pnl:         Option<f64>,
    entry_ts:    f64,
    settle_ts:   Option<f64>,
}

#[derive(Clone, Debug)]
struct Position {
    log:     TradeLog,
    closed:  bool,
}

#[derive(Default, Debug, Clone)]
struct EngineStats {
    signals:    u64,
    orders:     u64,
    fills:      u64,
    wins:       u64,
    losses:     u64,
    unfilled:   u64,
    total_pnl:  f64,
    gross_pnl:  f64,  // before fees
    fees_paid:  f64,
}

impl EngineStats {
    fn wr(&self) -> f64 {
        let c = self.wins + self.losses;
        if c == 0 { 0.0 } else { self.wins as f64 / c as f64 }
    }
    fn fill_rate(&self) -> f64 {
        if self.orders == 0 { 0.0 } else { self.fills as f64 / self.orders as f64 }
    }
}

struct Engine {
    name:      &'static str,
    min_edge:  f64,
    fill_prob: f64,
    taker_fee: f64,   // 0.0 for maker, 0.02 for taker
    is_taker:  bool,
    positions: Arc<DashMap<String, Position>>,
    stats:     Arc<Mutex<EngineStats>>,
    log_tx:    tokio::sync::mpsc::UnboundedSender<TradeLog>,
    max_open:  usize,
    stake:     f64,
}

impl Engine {
    fn new(
        name: &'static str, min_edge: f64, fill_prob: f64,
        taker_fee: f64, is_taker: bool, max_open: usize, stake: f64,
        log_tx: tokio::sync::mpsc::UnboundedSender<TradeLog>,
    ) -> Self {
        Self {
            name, min_edge, fill_prob, taker_fee, is_taker,
            positions: Arc::new(DashMap::new()),
            stats: Arc::new(Mutex::new(EngineStats::default())),
            log_tx, max_open, stake,
        }
    }

    async fn try_enter(
        &self, slug: &str, asset: &str, direction: Direction,
        entry_price: f64, fair: f64, edge: f64,
        secs: f64, age: f64,
    ) {
        if self.positions.contains_key(slug) { return; }
        if self.positions.len() >= self.max_open { return; }
        if edge < self.min_edge { return; }

        // Simulate fill
        let fill_prob = if self.is_taker {
            self.fill_prob // taker ~92%
        } else {
            // maker: high-edge signals = fast move = lower fill
            if edge > 0.35 { self.fill_prob * 0.5 } else { self.fill_prob }
        };

        let filled = rand01() < fill_prob;

        // Taker: simulate slippage — pay 1 tick worse (0.01)
        let effective_price = if self.is_taker && filled {
            (entry_price + 0.01).min(0.99)
        } else {
            entry_price
        };

        let fee = if self.is_taker && filled { self.taker_fee * self.stake } else { 0.0 };

        let log = TradeLog {
            engine: self.name.to_string(),
            slug: slug.to_string(),
            asset: asset.to_string(),
            direction,
            entry_price: effective_price,
            fair,
            edge,
            secs_left: secs,
            book_age: age,
            stake: self.stake,
            filled,
            fill_prob,
            outcome: if !filled { Some("UNFILLED".to_string()) } else { None },
            pnl: if !filled { Some(0.0) } else { None },
            entry_ts: now(),
            settle_ts: None,
        };

        if filled {
            info!("[{}] FILLED {slug} {:?} edge={edge:.3} price={effective_price:.3} fee=${fee:.3}",
                self.name, direction);
        } else {
            info!("[{}] NO-FILL {slug} {:?} edge={edge:.3} prob={fill_prob:.2}",
                self.name, direction);
        }

        self.positions.insert(slug.to_string(), Position { log, closed: false });

        let mut s = self.stats.lock().await;
        s.signals += 1;
        s.orders  += 1;
        if filled { s.fills += 1; } else { s.unfilled += 1; }
    }

    async fn settle(&self, slug: &str, yes_wins: bool) {
        let pos = match self.positions.get(slug) {
            Some(p) => p.clone(), None => return,
        };
        if pos.closed { return; }
        drop(pos);

        if let Some(mut pos) = self.positions.get_mut(slug) {
            if pos.closed { return; }
            pos.closed = true;

            let log = &mut pos.log;
            let settle_ts = now();

            if !log.filled {
                log.outcome   = Some("UNFILLED".to_string());
                log.pnl       = Some(0.0);
                log.settle_ts = Some(settle_ts);
                let completed = log.clone();
                drop(pos);
                self.positions.remove(slug);
                let _ = self.log_tx.send(completed);
                return;
            }

            let we_win = match log.direction { Direction::Yes => yes_wins, Direction::No => !yes_wins };
            let gross_pnl = if we_win { (1.0 - log.entry_price) * log.stake } else { -log.entry_price * log.stake };
            let fee       = if self.is_taker { self.taker_fee * log.stake } else { 0.0 };
            let net_pnl   = gross_pnl - fee;

            log.outcome   = Some(if we_win { "WIN".to_string() } else { "LOSS".to_string() });
            log.pnl       = Some(net_pnl);
            log.settle_ts = Some(settle_ts);

            info!("[{}] SETTLE {slug}: {} gross={gross_pnl:+.3} fee={fee:.3} net={net_pnl:+.3}",
                self.name, log.outcome.as_deref().unwrap_or("?"));

            let completed = log.clone();
            drop(pos);
            self.positions.remove(slug);

            let mut s = self.stats.lock().await;
            s.total_pnl += net_pnl;
            s.gross_pnl += gross_pnl;
            s.fees_paid += fee;
            if we_win { s.wins += 1; } else { s.losses += 1; }
            let _ = self.log_tx.send(completed);
        }
    }

    async fn print_stats(&self) {
        let s = self.stats.lock().await;
        info!("[{}] sig={} ord={} fill={} ({:.1}%) W={} L={} WR={:.1}% unfill={} pnl=${:+.2} fees=${:.2} gross=${:+.2}",
            self.name,
            s.signals, s.orders, s.fills, s.fill_rate() * 100.0,
            s.wins, s.losses, s.wr() * 100.0,
            s.unfilled, s.total_pnl, s.fees_paid, s.gross_pnl,
        );
    }
}

// ═══════════════════════════════════════════════════════════════
// LOG WRITER
// ═══════════════════════════════════════════════════════════════

async fn log_writer(mut rx: tokio::sync::mpsc::UnboundedReceiver<TradeLog>, path: &str) {
    use tokio::io::AsyncWriteExt;
    let mut file = match tokio::fs::OpenOptions::new()
        .create(true).append(true).open(path).await
    {
        Ok(f) => f,
        Err(e) => { warn!("[LOG] cannot open {path}: {e}"); return; }
    };

    while let Some(log) = rx.recv().await {
        let line = match serde_json::to_string(&log) {
            Ok(j) => format!("{j}\n"),
            Err(e) => { warn!("[LOG] serialize error: {e}"); continue; }
        };
        if let Err(e) = file.write_all(line.as_bytes()).await {
            warn!("[LOG] write error: {e}");
        }
        let _ = file.flush().await;
    }
}

// ═══════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "cl_ab_test=info".into()))
        .init();

    let cfg = Config::default();

    info!("════════════════════════════════════════════════════════");
    info!("  CL LAG BOT — A/B TEST  paper=true  stake=${:.2}", cfg.stake);
    info!("  Engine A: postOnly maker  min_edge={:.2}  fill~{:.0}%", cfg.a_min_edge, cfg.a_fill_prob * 100.0);
    info!("  Engine B: taker IOC       min_edge={:.2}  fill~{:.0}%  fee={:.0}%",
        cfg.b_min_edge, cfg.b_fill_prob * 100.0, cfg.b_taker_fee * 100.0);
    info!("  Logging to: {}", cfg.log_file);
    info!("════════════════════════════════════════════════════════");

    let http = Client::new();
    let cl   = new_cl();
    let bn   = new_bn();
    let book: BookState = Arc::new(DashMap::new());

    let (token_tx, token_rx) = watch::channel(Vec::<String>::new());
    let (log_tx, log_rx)     = tokio::sync::mpsc::unbounded_channel::<TradeLog>();

    tokio::spawn(run_cl_feed(cl.clone(), cfg.rtds_url));
    tokio::spawn(run_bn_feed(bn.clone()));
    tokio::spawn(run_book_feed(book.clone(), token_rx, cfg.clob_ws));
    tokio::spawn(log_writer(log_rx, cfg.log_file));

    // Engine A — postOnly maker
    let eng_a = Arc::new(Engine::new(
        "A-MAKER", cfg.a_min_edge, cfg.a_fill_prob, 0.0, false,
        cfg.max_open, cfg.stake, log_tx.clone(),
    ));
    // Engine B — taker IOC
    let eng_b = Arc::new(Engine::new(
        "B-TAKER", cfg.b_min_edge, cfg.b_fill_prob, cfg.b_taker_fee, true,
        cfg.max_open, cfg.stake, log_tx.clone(),
    ));

    // Windows
    let windows: Arc<Mutex<Vec<Window>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let mut ws = windows.lock().await;
        *ws = fetch_windows(&http, cfg.gamma_url).await?;
        lock_open_prices(&mut ws, &cl);
        let _ = token_tx.send(all_tokens(&ws));
    }

    // 60s window refresh
    {
        let windows  = windows.clone();
        let http     = http.clone();
        let gamma    = cfg.gamma_url;
        let cl_c     = cl.clone();
        let token_tx = token_tx.clone();
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(60));
            loop {
                t.tick().await;
                if let Ok(fresh) = fetch_windows(&http, gamma).await {
                    let mut ws = windows.lock().await;
                    let existing: HashMap<String, Option<f64>> = ws.iter()
                        .map(|w| (w.slug.clone(), w.open_price)).collect();
                    *ws = fresh;
                    for w in ws.iter_mut() {
                        if let Some(op) = existing.get(&w.slug).copied().flatten() {
                            w.open_price = Some(op);
                        }
                    }
                    lock_open_prices(&mut ws, &cl_c);
                    let _ = token_tx.send(all_tokens(&ws));
                    info!("[MAIN] windows refreshed: {}", ws.len());
                }
            }
        });
    }

    // Settlement checker every 10s
    {
        let windows = windows.clone();
        let cl_c    = cl.clone();
        let ea      = eng_a.clone();
        let eb      = eng_b.clone();
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(10));
            loop {
                t.tick().await;
                let n  = now();
                let ws = windows.lock().await;
                for w in ws.iter() {
                    if n > w.close_ts as f64 && n < w.close_ts as f64 + 30.0 {
                        if let (Some(cl_e), Some(open_p)) = (cl_c.get(w.asset), w.open_price) {
                            let yes_wins = cl_e.price > open_p;
                            ea.settle(&w.slug, yes_wins).await;
                            eb.settle(&w.slug, yes_wins).await;
                        }
                    }
                }
            }
        });
    }

    // Stats comparison every 60s
    {
        let ea = eng_a.clone();
        let eb = eng_b.clone();
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(60));
            loop {
                t.tick().await;
                info!("──────────────────────────────────────────────────────");
                ea.print_stats().await;
                eb.print_stats().await;
                info!("──────────────────────────────────────────────────────");
            }
        });
    }

    // Main scan loop — 500ms
    let mut scan       = tokio::time::interval(Duration::from_millis(500));
    let mut last_stats = now();

    loop {
        scan.tick().await;
        if !cl.iter().any(|e| e.value().price > 0.0) { continue; }

        // Lock open prices immediately
        {
            let mut ws = windows.lock().await;
            let any_missing = ws.iter().any(|w| w.is_active() && w.open_price.is_none());
            if any_missing { lock_open_prices(&mut ws, &cl); }
        }

        let ws = windows.lock().await;
        for w in ws.iter() {
            if !w.is_active() { continue; }
            let open_p = match w.open_price { Some(p) => p, None => continue };

            let cl_e = match cl.get(w.asset) { Some(e) => e, None => continue };
            if cl_e.is_stale() { continue; }
            let cl_price = cl_e.price;
            let sigma = {
                let s = cl_e.realized_sigma();
                if s > 0.0 { s } else { fair_value::sigma_fallback(w.annual_vol(), cl_price) }
            };
            drop(cl_e);

            let secs = w.secs_left();
            if secs < cfg.min_secs || secs > cfg.max_secs { continue; }

            let fair = fair_value::compute(cl_price, open_p, sigma, secs);
            if (fair - 0.5).abs() < 0.05 { continue; }

            let direction = if fair > 0.5 { Direction::Yes } else { Direction::No };
            let token_id  = if fair > 0.5 { &w.yes_token } else { &w.no_token };

            let bk = match book.get(token_id) { Some(b) => b, None => continue };
            let (yes_ask, yes_bid, book_ts, _) = *bk;
            drop(bk);

            let age = now() - book_ts;
            if age > cfg.max_book_age { continue; }
            if yes_ask <= 0.0 || yes_bid <= 0.0 { continue; }
            if yes_ask < 0.02 || yes_ask > 0.98 { continue; }
            if yes_bid < 0.02 || yes_bid > 0.98 { continue; }
            if yes_ask - yes_bid > 0.10 { continue; }

            let edge = match direction {
                Direction::Yes => fair - yes_ask,
                Direction::No  => yes_bid - fair,
            };
            let entry_price = match direction {
                Direction::Yes => yes_ask.min(0.99),
                Direction::No  => (1.0 - yes_bid).min(0.99),
            };

            let slug  = w.slug.clone();
            let asset = w.asset;
            drop(ws);

            // Both engines see the SAME signal — A decides independently, B decides independently
            eng_a.try_enter(&slug, asset, direction, entry_price, fair, edge, secs, age).await;
            eng_b.try_enter(&slug, asset, direction, entry_price, fair, edge, secs, age).await;

            break;
        }

        if now() - last_stats > 60.0 {
            last_stats = now();
        }
    }
}
