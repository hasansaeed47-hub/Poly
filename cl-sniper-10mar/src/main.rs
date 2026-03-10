//! CL Sniper — 10th March 2026 — Live Signal Generator
//!
//! 5 engines: A(5m sniper) B(5m D1) C(15m sniper) D(15m D1) E(late scalper)
//! Based on cl-sniper-9mar v8.0.0 (96% WR, +$21.44 / 109 trades)
//!
//! Feeds: CL (Polymarket RTDS), BN (Binance aggTrade), CLOB (REST books)
//! Entry: last 57-44s of window (A-D), last 25s (E)
//! Delta: raw CL % change from open, stdev-scaled (A-D). Price-based (E).
//! Order: maker at ask-0.01 for 2s, then taker fallback
//! SL: bid ≤ 50% of fill → taker exit, skip rest of window
//! Max DD: $50 cumulative → halt all engines

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

// ── Constants ────────────────────────────────────────────────────────────────
const ASSETS: &[&str] = &["btc", "eth", "sol", "xrp"];
const STDEV: &[(&str, f64)] = &[("btc", 0.167), ("eth", 0.194), ("sol", 0.247), ("xrp", 0.440)];
const STDEV_BASE: f64 = 0.167;

const STAKE: f64 = 5.0;
const MAX_DD: f64 = 50.0;
const SL_PCT: f64 = 0.50;
const SLIP: f64 = 0.005;

// A-D entry range
const MIN_ENTRY: f64 = 0.88;
const MAX_ENTRY: f64 = 0.98;

// E entry range
const E_MIN_ENTRY: f64 = 0.95;
const E_MAX_ENTRY: f64 = 0.975;

// A-D entry window (seconds left in window)
const ENTRY_START: i64 = 57;
const TAKER_DEADLINE: i64 = 44;

// E entry window
const E_ENTRY_START: i64 = 25;
const E_TAKER_DEADLINE: i64 = 3;

// Maker chase duration in ticks (2s = 4 ticks at 500ms)
const MAKER_CHASE_TICKS: u32 = 4;

// Regime threshold
const REGIME_THRESH: f64 = 0.3;

const RTDS_WS: &str = "wss://ws-live-data.polymarket.com";
const BN_WS: &str = "wss://stream.binance.com:9443/ws";
const GAMMA: &str = "https://gamma-api.polymarket.com";
const CLOB: &str = "https://clob.polymarket.com";

fn stdev(a: &str) -> f64 {
    STDEV.iter().find(|(k, _)| *k == a).map(|(_, v)| *v).unwrap_or(STDEV_BASE)
}

fn pm_fee(px: f64) -> f64 {
    px * (1.0 - px) * 0.0625
}

// ── State ────────────────────────────────────────────────────────────────────
type SS = Arc<RwLock<State>>;

struct State {
    cl: HashMap<&'static str, f64>,
    snap: HashMap<&'static str, HashMap<i64, f64>>,
    bn: HashMap<&'static str, f64>,
    bnh: HashMap<&'static str, VecDeque<(f64, f64)>>,
}

impl State {
    fn new() -> Self {
        State {
            cl: HashMap::new(),
            snap: HashMap::new(),
            bn: HashMap::new(),
            bnh: HashMap::new(),
        }
    }

    fn cl_up(&mut self, a: &'static str, px: f64, ts: f64) {
        self.cl.insert(a, px);
        let s = self.snap.entry(a).or_default();
        s.insert(ts as i64, px);
        let c = ts as i64 - 7200;
        s.retain(|k, _| *k > c);
    }

    fn bn_up(&mut self, a: &'static str, px: f64) {
        let ts = Utc::now().timestamp_millis() as f64 / 1000.0;
        self.bn.insert(a, px);
        let h = self.bnh.entry(a).or_default();
        h.push_back((ts, px));
        if h.len() > 14400 {
            h.pop_front();
        }
    }

    fn cl_at(&self, a: &str, t: i64) -> Option<f64> {
        let s = self.snap.get(a)?;
        // Search ±3s to account for irregular CL oracle update intervals
        for off in [0i64, 1, -1, 2, -2, 3, -3] {
            if let Some(&p) = s.get(&(t + off)) { return Some(p); }
        }
        None
    }

    fn cl_latest(&self, a: &str) -> Option<f64> {
        self.cl.get(a).copied()
    }

    fn bn_trend(&self, a: &str, sec: u64) -> Option<f64> {
        let h = self.bnh.get(a)?;
        if h.len() < 2 { return None; }
        let now = Utc::now().timestamp_millis() as f64 / 1000.0;
        let old = h.iter().find(|(t, _)| *t >= now - sec as f64)?;
        if old.1 <= 0.0 { return None; }
        Some((h.back()?.1 - old.1) / old.1 * 100.0)
    }

    fn cl_trend(&self, a: &str, sec: u64) -> Option<f64> {
        let s = self.snap.get(a)?;
        if s.is_empty() { return None; }
        let now = Utc::now().timestamp();
        let cut = now - sec as i64;
        let cur = self.cl.get(a)?;
        let old = s.iter()
            .filter(|(&t, _)| t >= cut)
            .min_by_key(|(&t, _)| t)
            .map(|(_, &p)| p)?;
        if old <= 0.0 { return None; }
        Some((cur - old) / old * 100.0)
    }

    fn hour_range(&self, a: &str) -> f64 {
        let s = match self.snap.get(a) { Some(s) => s, None => return 0.0 };
        let now = Utc::now().timestamp();
        let cut = now - 3600;
        let prices: Vec<f64> = s.iter().filter(|(&t, _)| t > cut).map(|(_, &p)| p).collect();
        if prices.len() < 10 { return 999.0; }
        let hi = prices.iter().cloned().fold(f64::MIN, f64::max);
        let lo = prices.iter().cloned().fold(f64::MAX, f64::min);
        if lo <= 0.0 { return 0.0; }
        (hi - lo) / lo * 100.0
    }
}

// ── Feeds ────────────────────────────────────────────────────────────────────
fn cl_asset(s: &str) -> Option<&'static str> {
    match s {
        "btc/usd" | "btcusd" | "btc" => Some("btc"),
        "eth/usd" | "ethusd" | "eth" => Some("eth"),
        "sol/usd" | "solusd" | "sol" => Some("sol"),
        "xrp/usd" | "xrpusd" | "xrp" => Some("xrp"),
        _ => None,
    }
}

fn bnsym(a: &str) -> &'static str {
    match a {
        "btc" => "btcusdt", "eth" => "ethusdt",
        "sol" => "solusdt", "xrp" => "xrpusdt", _ => "btcusdt",
    }
}

async fn cl_feed(st: SS) {
    loop {
        info!("[CL] Connecting...");
        if let Err(e) = cl_ws(&st).await { error!("[CL] {}", e); }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn cl_ws(st: &SS) -> Result<()> {
    let (mut ws, _) = connect_async(RTDS_WS).await.context("CL connect")?;
    ws.send(Message::Text(json!({
        "action": "subscribe",
        "subscriptions": [{"topic": "crypto_prices_chainlink", "type": "*", "filters": ""}]
    }).to_string())).await?;
    info!("[CL] Connected");

    let mut ping = tokio::time::interval(Duration::from_secs(5));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ping.tick() => {
                let _ = ws.send(Message::Text(json!({"action":"ping"}).to_string())).await;
            }
            msg = ws.next() => { match msg {
                Some(Ok(Message::Text(t))) => {
                    if t == "pong" { continue; }
                    let d: Value = match serde_json::from_str(&t) { Ok(d) => d, _ => continue };
                    if d.get("topic").and_then(|t| t.as_str()) != Some("crypto_prices_chainlink") { continue; }
                    let p = match d.get("payload") { Some(p) => p, _ => continue };
                    let sym = p.get("symbol").and_then(|s| s.as_str()).unwrap_or("").to_lowercase();
                    let val = p.get("value").and_then(|v| v.as_f64().or(v.as_str().and_then(|s| s.parse().ok())));
                    let rts = p.get("timestamp").and_then(|t| t.as_f64().or(t.as_i64().map(|i| i as f64))).unwrap_or(0.0);
                    let ts = if rts > 1e12 { rts / 1000.0 } else if rts > 1e9 { rts } else { Utc::now().timestamp() as f64 };
                    if let (Some(a), Some(px)) = (cl_asset(&sym), val) {
                        if px > 0.0 { st.write().await.cl_up(a, px, ts); }
                    }
                }
                Some(Ok(Message::Ping(d))) => { let _ = ws.send(Message::Pong(d)).await; }
                Some(Err(e)) => { error!("[CL] {}", e); break; }
                None => break,
                _ => {}
            }}
        }
    }
    Ok(())
}

async fn bn_feed(st: SS) {
    loop {
        info!("[BN] Connecting...");
        if let Err(e) = bn_ws(&st).await { error!("[BN] {}", e); }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn bn_ws(st: &SS) -> Result<()> {
    let streams: Vec<String> = ASSETS.iter().map(|a| format!("{}@aggTrade", bnsym(a))).collect();
    let (mut ws, _) = connect_async(format!("{}/{}", BN_WS, streams.join("/"))).await.context("BN connect")?;
    info!("[BN] Connected");
    while let Some(msg) = ws.next().await {
        match msg {
            Ok(Message::Text(t)) => {
                let d: Value = match serde_json::from_str(&t) { Ok(d) => d, _ => continue };
                let i = d.get("data").unwrap_or(&d);
                let sym = i.get("s").and_then(|s| s.as_str()).unwrap_or("").to_lowercase();
                let px = i.get("p").and_then(|p| p.as_str().and_then(|s| s.parse::<f64>().ok()));
                let a: Option<&'static str> = match sym.as_str() {
                    "btcusdt" => Some("btc"), "ethusdt" => Some("eth"),
                    "solusdt" => Some("sol"), "xrpusdt" => Some("xrp"), _ => None,
                };
                if let (Some(a), Some(p)) = (a, px) {
                    if p > 0.0 { st.write().await.bn_up(a, p); }
                }
            }
            Ok(Message::Ping(d)) => { let _ = ws.send(Message::Pong(d)).await; }
            Err(e) => { error!("[BN] {}", e); break; }
            _ => {}
        }
    }
    Ok(())
}

// ── Market Scanner ───────────────────────────────────────────────────────────
#[derive(Clone, Debug)]
struct Win {
    slug: String,
    asset: &'static str,
    wmin: u32,
    tid_up: String,
    tid_dn: String,
    start_ts: i64,
    end_ts: i64,
}

impl Win {
    fn left(&self) -> i64 { self.end_ts - Utc::now().timestamp() }
}

struct Scan {
    http: reqwest::Client,
    cache: Vec<Win>,
    last: Instant,
}

impl Scan {
    fn new() -> Self {
        Scan {
            http: reqwest::Client::builder()
                .user_agent("cls/10")
                .timeout(Duration::from_secs(5))
                .build().expect("http client"),
            cache: Vec::new(),
            last: Instant::now() - Duration::from_secs(999),
        }
    }

    async fn get(&mut self) -> Vec<Win> {
        if self.last.elapsed() < Duration::from_secs(10) {
            return self.cache.iter().filter(|w| w.left() > 0).cloned().collect();
        }
        let now = Utc::now().timestamp();
        let mut ws = Vec::new();
        for &a in ASSETS {
            for &wm in &[5u32, 15] {
                let iv = wm as i64 * 60;
                let s0 = (now / iv) * iv;
                for st in [s0, s0 + iv] {
                    let et = st + iv;
                    if et < now { continue; }
                    let slug = format!("{}-updown-{}m-{}", a, wm, st);
                    let r = match self.http.get(format!("{}/markets", GAMMA))
                        .query(&[("slug", &slug)]).send().await
                    {
                        Ok(r) if r.status().is_success() => r,
                        _ => continue,
                    };
                    let d: Value = match r.json().await { Ok(d) => d, _ => continue };
                    let m = if d.is_array() {
                        match d.as_array().and_then(|a| a.first()) { Some(m) => m.clone(), None => continue }
                    } else { d };
                    let tr = m.get("clobTokenIds").unwrap_or(&Value::Null);
                    let tids: Vec<String> = if tr.is_string() {
                        serde_json::from_str(tr.as_str().unwrap_or("[]")).unwrap_or_default()
                    } else {
                        serde_json::from_value(tr.clone()).unwrap_or_default()
                    };
                    if tids.len() < 2 { continue; }
                    let or = m.get("outcomes").unwrap_or(&Value::Null);
                    let outs: Vec<String> = if or.is_string() {
                        serde_json::from_str(or.as_str().unwrap_or("[]")).unwrap_or_default()
                    } else {
                        serde_json::from_value(or.clone()).unwrap_or_default()
                    };
                    let (tu, td) = if outs.len() >= 2 && outs[0] == "Down" {
                        (tids[1].clone(), tids[0].clone())
                    } else {
                        (tids[0].clone(), tids[1].clone())
                    };
                    ws.push(Win { slug, asset: a, wmin: wm, tid_up: tu, tid_dn: td, start_ts: st, end_ts: et });
                }
            }
        }
        self.cache = ws.clone();
        self.last = Instant::now();
        ws.into_iter().filter(|w| w.left() > 0).collect()
    }
}

// ── Book Cache ───────────────────────────────────────────────────────────────
#[derive(Clone, Default, Debug)]
struct Bk {
    bb: f64,
    ba: f64,
    ha: bool,
    hb: bool,
}

struct BkC {
    http: reqwest::Client,
    c: HashMap<String, Bk>,
    t: HashMap<String, Instant>,
}

impl BkC {
    fn new() -> Self {
        BkC {
            http: reqwest::Client::builder()
                .user_agent("cls/10")
                .timeout(Duration::from_secs(2))
                .build().expect("http"),
            c: HashMap::new(),
            t: HashMap::new(),
        }
    }

    fn get(&self, tid: &str) -> Bk {
        self.c.get(tid).cloned().unwrap_or_default()
    }

    async fn refresh(&mut self, tids: &[String]) {
        let stale: Vec<&String> = tids.iter()
            .filter(|t| self.t.get(*t).map(|ts| ts.elapsed() >= Duration::from_millis(400)).unwrap_or(true))
            .collect();
        if stale.is_empty() { return; }
        let body: Vec<Value> = stale.iter().map(|t| json!({"token_id": t})).collect();
        let r = match self.http.post(format!("{}/books", CLOB)).json(&body).send().await {
            Ok(r) if r.status().is_success() => r, _ => return,
        };
        let res: Vec<Value> = match r.json().await { Ok(d) => d, _ => return };
        let now = Instant::now();
        for item in &res {
            let tid = item.get("asset_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            if tid.is_empty() { continue; }
            let mut bk = Bk::default();
            if let Some(bids) = item.get("bids").and_then(|b| b.as_array()) {
                let mut p: Vec<f64> = bids.iter()
                    .filter_map(|b| b.get("price").and_then(|p| p.as_str().and_then(|s| s.parse().ok())))
                    .collect();
                p.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
                if let Some(&v) = p.first() { bk.bb = v; bk.hb = true; }
            }
            if let Some(asks) = item.get("asks").and_then(|a| a.as_array()) {
                let mut p: Vec<f64> = asks.iter()
                    .filter_map(|a| a.get("price").and_then(|p| p.as_str().and_then(|s| s.parse().ok())))
                    .collect();
                p.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                if let Some(&v) = p.first() { bk.ba = v; bk.ha = true; }
            }
            self.c.insert(tid.clone(), bk);
            self.t.insert(tid, now);
        }
    }
}

// ── Engine Config ────────────────────────────────────────────────────────────
#[derive(Clone)]
struct EngCfg {
    id: &'static str,
    delta: f64,
    continuity: u32,
    bn_contra: bool,
    cl_fade: bool,
    regime: bool,
    wmin: u32,          // 5, 15, or 0 (both) for Engine E
    is_late_scalper: bool,
}

fn engines() -> Vec<EngCfg> {
    vec![
        EngCfg {
            id: "A", delta: 0.04, continuity: 4,
            bn_contra: true, cl_fade: true, regime: true,
            wmin: 5, is_late_scalper: false,
        },
        EngCfg {
            id: "B", delta: 0.15, continuity: 0,
            bn_contra: true, cl_fade: true, regime: true,
            wmin: 5, is_late_scalper: false,
        },
        EngCfg {
            id: "C", delta: 0.04, continuity: 4,
            bn_contra: true, cl_fade: true, regime: true,
            wmin: 15, is_late_scalper: false,
        },
        EngCfg {
            id: "D", delta: 0.15, continuity: 0,
            bn_contra: true, cl_fade: true, regime: true,
            wmin: 15, is_late_scalper: false,
        },
        EngCfg {
            id: "E", delta: 0.0, continuity: 0,
            bn_contra: false, cl_fade: false, regime: false,
            wmin: 0, is_late_scalper: true,
        },
    ]
}

// ── Paper Position ───────────────────────────────────────────────────────────
#[derive(Clone)]
struct PT {
    dir: String,
    asset: &'static str,
    px: f64,
    shares: f64,
    sl_px: f64,
    end_ts: i64,
    tid: String,
    tid_up: String,
    tid_dn: String,
    slug: String,
    entry_fee: f64,
}

// ── Tracker (one per engine) ─────────────────────────────────────────────────
struct Tracker {
    cfg: EngCfg,
    w: u32,
    l: u32,
    sl_count: u32,
    pnl: f64,
    active: Option<PT>,
    done: HashSet<String>,        // slugs already traded or SL'd this cycle
    delta_ticks: HashMap<String, u32>,
    maker_ticks: HashMap<String, u32>,  // slug → ticks since maker posted
}

impl Tracker {
    fn new(cfg: EngCfg) -> Self {
        Tracker {
            cfg, w: 0, l: 0, sl_count: 0, pnl: 0.0,
            active: None, done: HashSet::new(),
            delta_ticks: HashMap::new(),
            maker_ticks: HashMap::new(),
        }
    }
    fn total(&self) -> u32 { self.w + self.l + self.sl_count }
    fn wr(&self) -> f64 {
        if self.total() > 0 { self.w as f64 / self.total() as f64 * 100.0 } else { 0.0 }
    }
}

// ── Sniper Engine ────────────────────────────────────────────────────────────
struct Sniper {
    st: SS,
    scan: Scan,
    bk: BkC,
    trackers: Vec<Tracker>,
    cl_opens: HashMap<String, f64>,
    start: Instant,
    halted: bool,
}

impl Sniper {
    fn new(st: SS) -> Self {
        let trackers: Vec<Tracker> = engines().into_iter().map(Tracker::new).collect();
        info!("[BOOT] {} engines initialized", trackers.len());
        Sniper {
            st, scan: Scan::new(), bk: BkC::new(),
            trackers, cl_opens: HashMap::new(),
            start: Instant::now(), halted: false,
        }
    }

    fn cumulative_pnl(&self) -> f64 {
        self.trackers.iter().map(|t| t.pnl).sum()
    }

    fn has_active(&self) -> bool {
        self.trackers.iter().any(|t| t.active.is_some())
    }

    async fn tick(&mut self) {
        // ── Kill switch check ────────────────────────────────────────────
        if self.halted { return; }
        let cum = self.cumulative_pnl();
        if cum <= -MAX_DD {
            info!("═══════════════════════════════════════════════════════");
            info!("  ██  KILL SWITCH  ██  Cumulative P&L: ${:+.2}", cum);
            info!("  ██  Max DD ${:.0} breached — HALTING ALL ENGINES  ██", MAX_DD);
            info!("═══════════════════════════════════════════════════════");
            // Force-close any open positions at settlement value
            for tr in &mut self.trackers {
                if let Some(pt) = tr.active.take() {
                    let loss = -STAKE;
                    info!("[DD] FORCE_CLOSE [{}] {} {} — booking ${:+.2}", tr.cfg.id, pt.dir, pt.asset.to_uppercase(), loss);
                    tr.pnl += loss;
                    tr.l += 1;
                }
            }
            self.halted = true;
            return;
        }

        let wins = self.scan.get().await;

        // Collect token IDs for book refresh
        let mut tids: Vec<String> = Vec::new();
        for w in &wins {
            if w.left() > 0 && w.left() < 120 {
                tids.push(w.tid_up.clone());
                tids.push(w.tid_dn.clone());
            }
        }
        for tr in &self.trackers {
            if let Some(pt) = &tr.active {
                tids.push(pt.tid.clone());
                tids.push(pt.tid_up.clone());
                tids.push(pt.tid_dn.clone());
            }
        }
        tids.sort();
        tids.dedup();
        if !tids.is_empty() { self.bk.refresh(&tids).await; }

        // Record CL open prices — use snap at start_ts, not live price
        {
            let s = self.st.read().await;
            for w in &wins {
                if self.cl_opens.contains_key(&w.slug) { continue; }
                let now = Utc::now().timestamp();
                if now >= w.start_ts && now <= w.end_ts {
                    // Prefer the exact CL snapshot at window start_ts
                    let px = s.cl_at(w.asset, w.start_ts)
                        .or_else(|| s.cl.get(w.asset).copied())
                        .unwrap_or(0.0);
                    if px > 0.0 {
                        self.cl_opens.insert(w.slug.clone(), px);
                    }
                }
            }
        }

        // Read state
        let s = self.st.read().await;
        let hour_ranges: HashMap<&str, f64> = ASSETS.iter().map(|&a| (a, s.hour_range(a))).collect();

        for tr in &mut self.trackers {
            // ── SETTLE / SL active trade ─────────────────────────────────
            if let Some(pt) = &tr.active {
                let now = Utc::now().timestamp();

                // SL: bid ≤ 50% of fill
                let bk = self.bk.get(&pt.tid);
                if bk.hb && bk.bb <= pt.sl_px {
                    let recovery = pt.shares * (bk.bb - SLIP).max(0.0);
                    let exit_fee = pm_fee(bk.bb) * pt.shares;
                    let pnl = recovery - STAKE - pt.entry_fee - exit_fee;

                    info!("═══════════════════════════════════════════════════════");
                    info!("  [{}] ██ SL ██ {} {} {}m", tr.cfg.id, pt.dir, pt.asset.to_uppercase(), wins.iter().find(|w| w.slug == pt.slug).map(|w| w.wmin).unwrap_or(0));
                    info!("  bid={:.3} ≤ {:.3} (50% of {:.3})", bk.bb, pt.sl_px, pt.px);
                    info!("  recovery=${:.2}  fee=${:.2}  P&L=${:+.2}", recovery, pt.entry_fee + exit_fee, pnl);
                    info!("  ⚠ SELL NOW at market — skip rest of window");
                    info!("═══════════════════════════════════════════════════════");

                    tr.sl_count += 1;
                    tr.pnl += pnl;
                    // Window skip: mark slug as done so no re-entry
                    tr.done.insert(pt.slug.clone());
                    tr.active = None;
                    continue;
                }

                // Settlement: end_ts + 3s
                if now >= pt.end_ts + 3 {
                    let cl_open = self.cl_opens.get(&pt.slug).copied().unwrap_or(0.0);
                    // No cl_latest fallback — only use snap at end_ts (±3s)
                    let cl_close = s.cl_at(pt.asset, pt.end_ts).unwrap_or(0.0);

                    // CLOB cross-check (uses stored token IDs, not expired wins)
                    let bk_up = self.bk.get(&pt.tid_up);
                    let bk_dn = self.bk.get(&pt.tid_dn);

                    let cl_dir = if cl_open > 0.0 && cl_close > 0.0 {
                        Some(if cl_close >= cl_open { "UP" } else { "DOWN" })
                    } else { None };

                    let clob_dir = if bk_up.hb && bk_up.bb > 0.80 { Some("UP") }
                        else if bk_dn.hb && bk_dn.bb > 0.80 { Some("DOWN") }
                        else { None };

                    // Debug: log CL open/close so mismatches can be diagnosed
                    info!("[{}] SETTLE {} cl_open={:.2} cl_close={:.2} cl={:?} clob={:?} up_bb={:.3} dn_bb={:.3}",
                        tr.cfg.id, pt.slug, cl_open, cl_close,
                        cl_dir, clob_dir, bk_up.bb, bk_dn.bb);

                    if let (Some(cd), Some(cb)) = (cl_dir, clob_dir) {
                        if cd != cb {
                            warn!("[{}] CL/CLOB disagree: CL={} CLOB={} {} — using CLOB (ground truth)",
                                tr.cfg.id, cd, cb, pt.slug);
                        }
                    }

                    // CLOB post-settlement bids are ground truth; CL is fallback
                    let actual = clob_dir.or(cl_dir);

                    if let Some(actual) = actual {
                        let won = actual == pt.dir;
                        let pnl = if won {
                            pt.shares * 1.0 - STAKE - pt.entry_fee
                        } else {
                            -STAKE - pt.entry_fee
                        };

                        if won {
                            info!("[{}] ✓ WIN {} {} @{:.3} → ${:+.2} (cum ${:+.2})",
                                tr.cfg.id, pt.asset.to_uppercase(), pt.dir, pt.px, pnl, tr.pnl + pnl);
                            tr.w += 1;
                        } else {
                            info!("[{}] ✗ LOSS {} {} @{:.3} → ${:+.2} (cum ${:+.2})",
                                tr.cfg.id, pt.asset.to_uppercase(), pt.dir, pt.px, pnl, tr.pnl + pnl);
                            tr.l += 1;
                        }
                        tr.pnl += pnl;
                    } else {
                        warn!("[{}] NO_SETTLE {} — returning stake", tr.cfg.id, pt.slug);
                    }
                    tr.active = None;
                }
                continue;
            }

            // ── ENTRY evaluation ─────────────────────────────────────────
            if tr.active.is_some() { continue; }

            for w in &wins {
                // Timeframe filter
                if tr.cfg.wmin != 0 && w.wmin != tr.cfg.wmin { continue; }
                let left = w.left();
                if tr.done.contains(&w.slug) { continue; }

                if tr.cfg.is_late_scalper {
                    // ── Engine E: late scalper ────────────────────────────
                    if left > E_ENTRY_START || left < E_TAKER_DEADLINE { continue; }

                    let tid_up = &w.tid_up;
                    let tid_dn = &w.tid_dn;
                    let bk_up = self.bk.get(tid_up);
                    let bk_dn = self.bk.get(tid_dn);

                    // Find the side with ask ≥ 0.95
                    let (dir, tid, bk) = if bk_up.ha && bk_up.ba >= E_MIN_ENTRY {
                        ("UP", tid_up, bk_up)
                    } else if bk_dn.ha && bk_dn.ba >= E_MIN_ENTRY {
                        ("DOWN", tid_dn, bk_dn)
                    } else {
                        continue;
                    };

                    if bk.ba > E_MAX_ENTRY { continue; }

                    // Maker-first logic with 2s chase
                    let mk = ((bk.ba - 0.01) * 100.0).round() / 100.0;
                    let mk = mk.max(E_MIN_ENTRY); // clamp to min entry

                    let maker_elapsed = tr.maker_ticks.entry(w.slug.clone()).or_insert(0);
                    *maker_elapsed += 1;

                    let fp = if mk >= bk.ba {
                        mk // crossing — instant fill
                    } else if *maker_elapsed > MAKER_CHASE_TICKS {
                        // 2s elapsed — taker fallback
                        let tp = (bk.ba + SLIP).min(0.99);
                        if tp > E_MAX_ENTRY { continue; }
                        tp
                    } else {
                        // Still in maker phase — assume fill if 2+ ticks
                        if *maker_elapsed >= 2 { mk } else { continue; }
                    };

                    let shares = STAKE / fp;
                    let entry_fee = pm_fee(fp) * shares;
                    let sl_px = fp * SL_PCT;

                    info!("═══════════════════════════════════════════════════════");
                    info!("  [E] ▶ SIGNAL: BUY {} {} {}m @{:.3} ({:.0}s left)",
                        dir, w.asset.to_uppercase(), w.wmin, fp, left);
                    info!("  book={:.3}  maker={:.3}  taker={:.3}  fee=${:.4}",
                        bk.ba, mk, bk.ba + SLIP, entry_fee);
                    info!("  SL at bid ≤ {:.3} | Hold to settlement", sl_px);
                    info!("═══════════════════════════════════════════════════════");

                    tr.active = Some(PT {
                        dir: dir.to_string(), asset: w.asset, px: fp, shares,
                        sl_px, end_ts: w.end_ts, tid: tid.to_string(),
                        tid_up: w.tid_up.clone(), tid_dn: w.tid_dn.clone(),
                        slug: w.slug.clone(), entry_fee,
                    });
                    tr.done.insert(w.slug.clone());
                    tr.maker_ticks.remove(&w.slug);
                    break;

                } else {
                    // ── Engines A-D: delta-based ─────────────────────────
                    if left > ENTRY_START || left < TAKER_DEADLINE { continue; }

                    let cl_open = match self.cl_opens.get(&w.slug) { Some(&p) if p > 0.0 => p, _ => continue };
                    let cl_now = match s.cl.get(w.asset) { Some(&p) if p > 0.0 => p, _ => continue };
                    let delta = (cl_now - cl_open) / cl_open * 100.0;
                    if delta.abs() < 0.001 { continue; }

                    let dir = if delta > 0.0 { "UP" } else { "DOWN" };
                    let sc = stdev(w.asset) / STDEV_BASE;
                    let threshold = tr.cfg.delta * sc;

                    // Delta threshold
                    if delta.abs() < threshold {
                        tr.delta_ticks.remove(&w.slug);
                        tr.maker_ticks.remove(&w.slug);
                        continue;
                    }

                    // Continuity check
                    if tr.cfg.continuity > 0 {
                        let ticks = tr.delta_ticks.entry(w.slug.clone()).or_insert(0);
                        *ticks += 1;
                        if *ticks < tr.cfg.continuity { continue; }
                    }

                    // BN contra
                    if tr.cfg.bn_contra {
                        if let Some(bt) = s.bn_trend(w.asset, 15) {
                            if (dir == "UP" && bt < -0.02) || (dir == "DOWN" && bt > 0.02) { continue; }
                        }
                    }

                    // CL fade
                    if tr.cfg.cl_fade {
                        if let Some(ct) = s.cl_trend(w.asset, 10) {
                            if (dir == "UP" && ct < -0.03) || (dir == "DOWN" && ct > 0.03) { continue; }
                        }
                    }

                    // Regime check
                    if tr.cfg.regime {
                        let range = hour_ranges.get(w.asset).copied().unwrap_or(999.0);
                        if range < REGIME_THRESH { continue; }
                    }

                    // Book check
                    let tid = if dir == "UP" { &w.tid_up } else { &w.tid_dn };
                    let bk = self.bk.get(tid);
                    if !bk.ha || bk.ba < MIN_ENTRY || bk.ba > MAX_ENTRY { continue; }

                    // Maker-first with 2s chase
                    let mk = ((bk.ba - 0.01) * 100.0).round() / 100.0;
                    let mk = mk.max(MIN_ENTRY); // clamp maker to min entry

                    let maker_elapsed = tr.maker_ticks.entry(w.slug.clone()).or_insert(0);
                    *maker_elapsed += 1;

                    let fp = if mk >= bk.ba {
                        mk
                    } else if *maker_elapsed > MAKER_CHASE_TICKS || left <= (TAKER_DEADLINE + 1) {
                        // 2s elapsed or about to hit deadline — taker
                        (bk.ba + SLIP).min(0.99)
                    } else {
                        // Maker phase — assume fill after 2 ticks
                        if *maker_elapsed >= 2 { mk } else { continue; }
                    };

                    if fp > MAX_ENTRY || fp < MIN_ENTRY { continue; }

                    let shares = STAKE / fp;
                    let entry_fee = pm_fee(fp) * shares;
                    let sl_px = fp * SL_PCT;

                    let bn_now = s.bn.get(w.asset).copied().unwrap_or(0.0);
                    let hr = hour_ranges.get(w.asset).copied().unwrap_or(0.0);

                    info!("═══════════════════════════════════════════════════════");
                    info!("  [{}] ▶ SIGNAL: BUY {} {} {}m @{:.3} ({:.0}s left)",
                        tr.cfg.id, dir, w.asset.to_uppercase(), w.wmin, fp, left);
                    let actual_cont = tr.delta_ticks.get(&w.slug).copied().unwrap_or(0);
                    info!("  δ={:+.4}% thr={:.4}% cont={}/{} book={:.3}",
                        delta, threshold, actual_cont, tr.cfg.continuity, bk.ba);
                    info!("  CL={:.2} open={:.2} BN={:.2} 1hRange={:.2}%",
                        cl_now, cl_open, bn_now, hr);
                    info!("  maker={:.3} taker={:.3} fee=${:.4} SL≤{:.3}",
                        mk, bk.ba + SLIP, entry_fee, sl_px);
                    info!("═══════════════════════════════════════════════════════");

                    tr.active = Some(PT {
                        dir: dir.to_string(), asset: w.asset, px: fp, shares,
                        sl_px, end_ts: w.end_ts, tid: tid.to_string(),
                        tid_up: w.tid_up.clone(), tid_dn: w.tid_dn.clone(),
                        slug: w.slug.clone(), entry_fee,
                    });
                    tr.done.insert(w.slug.clone());
                    tr.delta_ticks.remove(&w.slug);
                    tr.maker_ticks.remove(&w.slug);
                    break;
                }
            }
        }

        drop(s);

        // Cleanup stale data
        let now = Utc::now().timestamp();
        let cut = now - 3600;
        self.cl_opens.retain(|k, _| slug_ts(k) > cut);
        for tr in &mut self.trackers {
            tr.done.retain(|k| slug_ts(k) > cut);
            tr.delta_ticks.retain(|k, _| slug_ts(k) > cut);
            tr.maker_ticks.retain(|k, _| slug_ts(k) > cut);
        }
    }

    fn status(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        for tr in &self.trackers {
            let active = if tr.active.is_some() { "*" } else { "" };
            if tr.total() > 0 {
                parts.push(format!("{}{}:{}W/{}L/{}S${:+.1}",
                    tr.cfg.id, active, tr.w, tr.l, tr.sl_count, tr.pnl));
            } else if tr.active.is_some() {
                parts.push(format!("{}*:active", tr.cfg.id));
            }
        }
        let cum = self.cumulative_pnl();
        let active = self.trackers.iter().filter(|t| t.active.is_some()).count();
        format!("active={} cum=${:+.2} | {}", active, cum, parts.join(" | "))
    }
}

fn slug_ts(slug: &str) -> i64 {
    slug.rsplit('-').next()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0)
}

// ── Main ─────────────────────────────────────────────────────────────────────
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("cl_sniper_10mar=info")
        .with_target(false)
        .init();
    dotenvy::dotenv().ok();

    info!("═══════════════════════════════════════════════════════");
    info!("  CL SNIPER — 10th March 2026 — LIVE SIGNAL GENERATOR");
    info!("═══════════════════════════════════════════════════════");
    info!("  A: 5m  δ≥0.04%  cont=4  filters=ON");
    info!("  B: 5m  δ≥0.15%  cont=0  filters=ON  (D1 clone)");
    info!("  C: 15m δ≥0.04%  cont=4  filters=ON");
    info!("  D: 15m δ≥0.15%  cont=0  filters=ON  (D1 clone)");
    info!("  E: ≤25s book≥0.95  late scalper");
    info!("  ─────────────────────────────────────────────────────");
    info!("  Stake: ${:.0}  Max DD: ${:.0}  SL: bid≤50%  Range: {:.2}-{:.2}",
        STAKE, MAX_DD, MIN_ENTRY, MAX_ENTRY);
    info!("  Entry: 57-44s left (A-D) | ≤25s (E) | Maker 2s→taker");
    info!("  Fee: px*(1-px)*0.0625 | Maker=0%");
    info!("═══════════════════════════════════════════════════════");

    let st: SS = Arc::new(RwLock::new(State::new()));

    // Start feeds
    let c = st.clone();
    tokio::spawn(async move { cl_feed(c).await; });
    let b = st.clone();
    tokio::spawn(async move { bn_feed(b).await; });

    info!("[BOOT] Waiting for feeds...");
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let s = st.read().await;
        if s.cl.contains_key("btc") && s.bn.contains_key("btc") { break; }
    }
    {
        let s = st.read().await;
        for &a in ASSETS {
            info!("  {}: CL=${:.2} BN=${:.2}",
                a.to_uppercase(),
                s.cl.get(a).copied().unwrap_or(0.0),
                s.bn.get(a).copied().unwrap_or(0.0));
        }
    }

    let mut sniper = Sniper::new(st.clone());
    info!("[BOOT] Signal generator running — 5 engines");

    let mut last_status = Instant::now();
    let mut last_detail = Instant::now();

    loop {
        let sleep_ms = if sniper.has_active() { 500 } else { 1000 };
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;

        if sniper.halted {
            // Keep printing status but don't process
            if last_status.elapsed().as_secs() >= 30 {
                info!("██ HALTED ██ {}", sniper.status());
                last_status = Instant::now();
            }
            continue;
        }

        sniper.tick().await;

        // Status every 60s
        if last_status.elapsed().as_secs() >= 60 {
            let s = st.read().await;
            let px: Vec<String> = ASSETS.iter()
                .filter_map(|&a| s.cl.get(a).map(|p| format!("{}=${:.0}", a.to_uppercase(), p)))
                .collect();
            let hrs = sniper.start.elapsed().as_secs_f64() / 3600.0;
            info!("── {} | {:.1}h | {} ──", px.join(" "), hrs, sniper.status());
            last_status = Instant::now();
        }

        // Detailed per-engine stats every 5 min
        if last_detail.elapsed().as_secs() >= 300 {
            info!("═══════════════════════════════════════════════════════");
            info!("  STATS — {:.1}h elapsed", sniper.start.elapsed().as_secs_f64() / 3600.0);
            for tr in &sniper.trackers {
                let active = if tr.active.is_some() { " [ACTIVE]" } else { "" };
                info!("  [{}] {}W/{}L/{}S  WR={:.0}%  P&L=${:+.2}{}",
                    tr.cfg.id, tr.w, tr.l, tr.sl_count, tr.wr(), tr.pnl, active);
            }
            info!("  CUMULATIVE: ${:+.2} / -${:.0} DD limit", sniper.cumulative_pnl(), MAX_DD);
            info!("═══════════════════════════════════════════════════════");
            last_detail = Instant::now();
        }
    }
}
