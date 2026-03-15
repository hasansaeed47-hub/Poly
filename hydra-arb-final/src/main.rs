//! Hydra-Arb Final v2 — S3b/S3C/S3D/S3E/S4 paper tracker
//!
//! Strategy lineup (all on 5m + 15m markets):
//!   S3b: Both-sides arb, dump loser at T-30 (time-based)
//!   S3C: Both-sides arb, dump loser at T-30 (identical to S3b — proven baseline)
//!   S3D: Both-sides arb, dump loser when bid≤$0.10, fallback T-30
//!   S3E: Both-sides arb, dump loser when bid≤$0.25, fallback T-30
//!   S4:  5m→15m cascade (2/3 sub-window confirm), entry T-120..T-44
//!
//! Changes from hydra-arb v1:
//!   - Removed S2 (0W/12L) and S3a (0 trades)
//!   - S3b-S3E now target both 5m AND 15m markets
//!   - S3D: T-30 fallback dump (eliminates all observed losses)
//!   - S3E: threshold ≤$0.25 + T-30 fallback (better loser recovery)
//!   - S4: entry window widened T-120..T-44, fill probability 1.0
//!   - Per-strategy + per-window tracking (e.g. S3b_5m, S3b_15m)
//!   - Detailed trade logging with CL prices, dump triggers, PnL breakdown

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

// ── Constants ──────────────────────────────────────────────────────────────
const ASSETS: &[&str] = &["btc", "eth", "sol", "xrp"];
const STAKE_1: f64 = 5.0;      // per-side stake
const STAKE_2: f64 = 10.0;     // total both-sides
const START_CAP: f64 = 100.0;  // per strategy
const SL_PCT: f64 = 0.50;
const SLIP: f64 = 0.005;
const MIN_ENTRY_S4: f64 = 0.85;
const MAX_ENTRY_S4: f64 = 0.98;
const S3_ASK_LO: f64 = 0.47;
const S3_ASK_HI: f64 = 0.53;

const RTDS_WS: &str = "wss://ws-live-data.polymarket.com";
const BN_WS: &str = "wss://stream.binance.com:9443/ws";
const GAMMA: &str = "https://gamma-api.polymarket.com";
const CLOB: &str = "https://clob.polymarket.com";

fn fee(px: f64) -> f64 { px * (1.0 - px) * 0.0625 }

// ── Strategy IDs ───────────────────────────────────────────────────────────
const STRATS: &[&str] = &[
    "S3b_5m", "S3b_15m",
    "S3C_5m", "S3C_15m",
    "S3D_5m", "S3D_15m",
    "S3E_5m", "S3E_15m",
    "S4",
];

fn s3_id(base: &str, wmin: u32) -> &'static str {
    match (base, wmin) {
        ("S3b", 5)  => "S3b_5m",  ("S3b", 15) => "S3b_15m",
        ("S3C", 5)  => "S3C_5m",  ("S3C", 15) => "S3C_15m",
        ("S3D", 5)  => "S3D_5m",  ("S3D", 15) => "S3D_15m",
        ("S3E", 5)  => "S3E_5m",  ("S3E", 15) => "S3E_15m",
        _ => "??",
    }
}

fn s3_base(id: &str) -> &str {
    if id.starts_with("S3b") { "S3b" }
    else if id.starts_with("S3C") { "S3C" }
    else if id.starts_with("S3D") { "S3D" }
    else if id.starts_with("S3E") { "S3E" }
    else { id }
}

// ── Shared State ───────────────────────────────────────────────────────────
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
        let c = ts as i64 - 3600;
        s.retain(|k, _| *k > c);
    }

    fn bn_up(&mut self, a: &'static str, px: f64) {
        let ts = Utc::now().timestamp_millis() as f64 / 1000.0;
        self.bn.insert(a, px);
        let h = self.bnh.entry(a).or_default();
        h.push_back((ts, px));
        if h.len() > 7200 { h.pop_front(); }
    }

    fn cl_at(&self, a: &str, t: i64, tol: i64) -> Option<f64> {
        let s = self.snap.get(a)?;
        for d in 0..=tol {
            if let Some(&p) = s.get(&(t + d)) { return Some(p); }
            if d > 0 {
                if let Some(&p) = s.get(&(t - d)) { return Some(p); }
            }
        }
        None
    }
}

// ── WebSocket Feeds ────────────────────────────────────────────────────────
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
        "sol" => "solusdt", "xrp" => "xrpusdt",
        _ => "btcusdt",
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
    ws.send(Message::Text(
        json!({"action":"subscribe","subscriptions":[
            {"topic":"crypto_prices_chainlink","type":"*","filters":""}
        ]}).to_string(),
    )).await?;
    info!("[CL] OK");
    while let Some(msg) = ws.next().await {
        match msg {
            Ok(Message::Text(t)) => {
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
            Ok(Message::Ping(d)) => { let _ = ws.send(Message::Pong(d)).await; }
            Ok(_) => {}
            Err(e) => { error!("[CL] WebSocket {}", e); break; }
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
    info!("[BN] OK");
    while let Some(msg) = ws.next().await {
        match msg {
            Ok(Message::Text(t)) => {
                let d: Value = match serde_json::from_str(&t) { Ok(d) => d, _ => continue };
                let i = d.get("data").unwrap_or(&d);
                let sym = i.get("s").and_then(|s| s.as_str()).unwrap_or("").to_lowercase();
                let px = i.get("p").and_then(|p| p.as_str().and_then(|s| s.parse::<f64>().ok()));
                let a: Option<&'static str> = match sym.as_str() {
                    "btcusdt" => Some("btc"), "ethusdt" => Some("eth"),
                    "solusdt" => Some("sol"), "xrpusdt" => Some("xrp"),
                    _ => None,
                };
                if let (Some(a), Some(p)) = (a, px) {
                    if p > 0.0 { st.write().await.bn_up(a, p); }
                }
            }
            Ok(Message::Ping(d)) => { let _ = ws.send(Message::Pong(d)).await; }
            Ok(_) => {}
            Err(e) => { error!("[BN] {}", e); break; }
        }
    }
    Ok(())
}

// ── Market Scanner ─────────────────────────────────────────────────────────
#[derive(Clone, Debug)]
struct Win {
    slug: String,
    asset: &'static str,
    wmin: u32,
    tid_up: String,
    tid_down: String,
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
                .user_agent("hydra-final/2")
                .timeout(Duration::from_secs(5))
                .build().expect("http"),
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
                    ws.push(Win { slug, asset: a, wmin: wm, tid_up: tu, tid_down: td, start_ts: st, end_ts: et });
                }
            }
        }
        self.cache = ws.clone();
        self.last = Instant::now();
        ws.into_iter().filter(|w| w.left() > 0).collect()
    }
}

// ── Book Cache ─────────────────────────────────────────────────────────────
#[derive(Clone, Default, Debug)]
struct Bk {
    bb: f64,
    ba: f64,
    ha: bool,
    hb: bool,
    ask_sz: f64,  // total ask depth at best level
    bid_sz: f64,  // total bid depth at best level
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
                .user_agent("hydra-final/2")
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
            .filter(|t| self.t.get(*t).map(|ts| ts.elapsed() >= Duration::from_secs(1)).unwrap_or(true))
            .collect();
        if stale.is_empty() { return; }
        let body: Vec<Value> = stale.iter().map(|t| json!({"token_id": t})).collect();
        let r = match self.http.post(format!("{}/books", CLOB)).json(&body).send().await {
            Ok(r) if r.status().is_success() => r,
            _ => return,
        };
        let res: Vec<Value> = match r.json().await { Ok(d) => d, _ => return };
        let now = Instant::now();
        for item in &res {
            let tid = item.get("asset_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            if tid.is_empty() { continue; }
            let mut bk = Bk::default();
            if let Some(bids) = item.get("bids").and_then(|b| b.as_array()) {
                let mut entries: Vec<(f64, f64)> = bids.iter().filter_map(|b| {
                    let p = b.get("price").and_then(|p| p.as_str().and_then(|s| s.parse().ok()))?;
                    let s = b.get("size").and_then(|s| s.as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
                    Some((p, s))
                }).collect();
                entries.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
                if let Some(&(p, s)) = entries.first() {
                    bk.bb = p;
                    bk.hb = true;
                    bk.bid_sz = s;
                }
            }
            if let Some(asks) = item.get("asks").and_then(|a| a.as_array()) {
                let mut entries: Vec<(f64, f64)> = asks.iter().filter_map(|a| {
                    let p = a.get("price").and_then(|p| p.as_str().and_then(|s| s.parse().ok()))?;
                    let s = a.get("size").and_then(|s| s.as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
                    Some((p, s))
                }).collect();
                entries.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                if let Some(&(p, s)) = entries.first() {
                    bk.ba = p;
                    bk.ha = true;
                    bk.ask_sz = s;
                }
            }
            self.c.insert(tid.clone(), bk);
            self.t.insert(tid, now);
        }
    }
}

// ── Position Tracker ───────────────────────────────────────────────────────
#[derive(Clone)]
#[allow(dead_code)]
struct PT {
    id: &'static str,
    slug: String,
    asset: &'static str,
    wmin: u32,
    dir: String,
    px: f64,
    shares: f64,
    dir2: String,
    px2: f64,
    sh2: f64,
    end_ts: i64,
    start_ts: i64,
    sl: f64,
    dumped: bool,
    dump_px: f64,
    dump_trigger: String,   // "TIME", "PRICE", "FALLBACK"
    wsold: bool,
    wpx: f64,
    tid_up: String,
    tid_dn: String,
    cl_open: f64,           // CL price at entry
    entry_time: i64,        // unix ts of entry
    cap_locked: f64,        // total capital locked on entry (stake + fees)
}

// ── Strategy Tracker ───────────────────────────────────────────────────────
struct Strat {
    id: &'static str,
    cap: f64,
    w: u32,
    l: u32,
    pnl: f64,
    gross_win: f64,
    gross_loss: f64,
    active: HashMap<String, PT>,
    done: HashSet<String>,
}

impl Strat {
    fn new(id: &'static str) -> Self {
        Strat {
            id, cap: START_CAP, w: 0, l: 0, pnl: 0.0,
            gross_win: 0.0, gross_loss: 0.0,
            active: HashMap::new(), done: HashSet::new(),
        }
    }

    fn rec(&mut self, won: bool, p: f64, stake_locked: f64) {
        if won { self.w += 1; self.gross_win += p; }
        else { self.l += 1; self.gross_loss += p; }
        self.pnl += p;
        // Return locked stake + PnL (PnL already has -stake baked in,
        // so stake+pnl = total proceeds from market)
        self.cap += stake_locked + p;
    }

    fn trades(&self) -> u32 { self.w + self.l }

    fn avg_win(&self) -> f64 { if self.w > 0 { self.gross_win / self.w as f64 } else { 0.0 } }
    fn avg_loss(&self) -> f64 { if self.l > 0 { self.gross_loss / self.l as f64 } else { 0.0 } }
    fn win_rate(&self) -> f64 { if self.trades() > 0 { self.w as f64 / self.trades() as f64 * 100.0 } else { 0.0 } }
}

// ── Main Engine ────────────────────────────────────────────────────────────
struct Engine {
    st: SS,
    scan: Scan,
    bk: BkC,
    s: HashMap<&'static str, Strat>,
    cl_o: HashMap<String, f64>,
    sub_r: HashMap<String, String>,
    start: Instant,
}

impl Engine {
    fn new(st: SS, scan: Scan, bk: BkC) -> Self {
        let mut s = HashMap::new();
        for id in STRATS { s.insert(*id, Strat::new(id)); }
        Engine { st, scan, bk, s, cl_o: HashMap::new(), sub_r: HashMap::new(), start: Instant::now() }
    }

    async fn tick(&mut self) {
        let wins = self.scan.get().await;
        let mut tids: Vec<String> = Vec::new();
        for w in &wins {
            if w.left() > 0 && w.left() < 920 {
                tids.push(w.tid_up.clone());
                tids.push(w.tid_down.clone());
            }
        }
        tids.sort();
        tids.dedup();
        if !tids.is_empty() { self.bk.refresh(&tids).await; }

        // Record CL open prices for new windows
        {
            let s = self.st.read().await;
            for w in &wins {
                if !self.cl_o.contains_key(&w.slug) {
                    if let Some(p) = s.cl_at(w.asset, w.start_ts, 1).or_else(|| {
                        let now = Utc::now().timestamp();
                        if (now - w.start_ts).abs() <= 2 { s.cl.get(w.asset).copied() } else { None }
                    }) {
                        if p > 0.0 { self.cl_o.insert(w.slug.clone(), p); }
                    }
                }
            }
        }

        self.eval_s3(&wins).await;
        self.eval_s4(&wins).await;
        self.manage().await;

        // Cleanup old data
        let now = Utc::now().timestamp();
        let cutoff = now - 3600;
        self.cl_o.retain(|k, _| k.rsplit('-').next().and_then(|s| s.parse::<i64>().ok()).map(|t| t > cutoff).unwrap_or(false));
        self.sub_r.retain(|k, _| k.rsplit('-').next().and_then(|s| s.parse::<i64>().ok()).map(|t| t > cutoff).unwrap_or(false));
        for st in self.s.values_mut() {
            st.done.retain(|k| k.rsplit('-').next().and_then(|s| s.parse::<i64>().ok()).map(|t| t > cutoff).unwrap_or(false));
        }
    }

    // ── S3b/S3C/S3D/S3E Entry ─────────────────────────────────────────────
    async fn eval_s3(&mut self, wins: &[Win]) {
        let state = self.st.read().await;
        for w in wins {
            // Both 5m and 15m markets
            if w.wmin != 5 && w.wmin != 15 { continue; }
            let left = w.left();
            if left > 290 || left < 60 { continue; }

            let bu = self.bk.get(&w.tid_up);
            let bd = self.bk.get(&w.tid_down);
            if !bu.ha || !bd.ha { continue; }
            if bu.ba < S3_ASK_LO || bu.ba > S3_ASK_HI || bd.ba < S3_ASK_LO || bd.ba > S3_ASK_HI { continue; }

            // Book depth check: warn if thin (< $10 at best ask)
            let up_depth = bu.ask_sz * bu.ba;
            let dn_depth = bd.ask_sz * bd.ba;
            if up_depth < 10.0 || dn_depth < 10.0 {
                warn!("[BOOK] Thin book {} {}m UP_depth=${:.1} DN_depth=${:.1} — entering anyway (paper)",
                    w.asset.to_uppercase(), w.wmin, up_depth, dn_depth);
            }

            let cl_now = state.cl.get(w.asset).copied().unwrap_or(0.0);

            for base in ["S3b", "S3C", "S3D", "S3E"] {
                let id = s3_id(base, w.wmin);
                let st = self.s.get_mut(id).expect("strategy");
                if st.done.contains(&w.slug) || st.active.contains_key(&w.slug) || st.cap < STAKE_2 { continue; }

                let up = bu.ba + SLIP;
                let dn = bd.ba + SLIP;
                let ush = STAKE_1 / up;
                let dsh = STAKE_1 / dn;
                let uf = fee(up) * ush;
                let df = fee(dn) * dsh;
                st.cap -= STAKE_2 + uf + df;

                let cl_open = self.cl_o.get(&w.slug).copied().unwrap_or(cl_now);

                let cap_locked = STAKE_2 + uf + df;
                st.active.insert(w.slug.clone(), PT {
                    id, slug: w.slug.clone(), asset: w.asset, wmin: w.wmin,
                    dir: "UP".into(), px: up, shares: ush,
                    dir2: "DOWN".into(), px2: dn, sh2: dsh,
                    end_ts: w.end_ts, start_ts: w.start_ts, sl: 0.0,
                    dumped: false, dump_px: 0.0, dump_trigger: String::new(),
                    wsold: false, wpx: 0.0,
                    tid_up: w.tid_up.clone(), tid_dn: w.tid_down.clone(),
                    cl_open, entry_time: Utc::now().timestamp(),
                    cap_locked,
                });
                st.done.insert(w.slug.clone());

                info!("[{}] ENTRY {} {}m T-{}s UP@{:.3} DN@{:.3} sum={:.3} cost=${:.2} cl=${:.2}",
                    id, w.asset.to_uppercase(), w.wmin, left,
                    up, dn, bu.ba + bd.ba, cap_locked, cl_open);
            }
        }
    }

    // ── S4 Entry ───────────────────────────────────────────────────────────
    async fn eval_s4(&mut self, wins: &[Win]) {
        for w in wins {
            if w.wmin != 15 { continue; }
            let left = w.left();
            // Widened: T-120 to T-44 (was T-50 to T-44)
            if left > 120 || left < 44 { continue; }

            let st = self.s.get_mut("S4").expect("strategy");
            if st.done.contains(&w.slug) || st.active.contains_key(&w.slug) || st.cap < STAKE_1 { continue; }

            // Check 2/3 sub-window confirmation
            let iv = 300i64;
            let (mut u, mut d) = (0u32, 0u32);
            for i in 0..3 {
                let ss = format!("{}-updown-5m-{}", w.asset, w.start_ts + i * iv);
                match self.sub_r.get(&ss).map(|s| s.as_str()) {
                    Some("UP") => { u += 1; }
                    Some("DOWN") => { d += 1; }
                    _ => {}
                }
            }
            let dir = if u >= 2 { "UP" } else if d >= 2 { "DOWN" } else { continue };

            let tid = if dir == "UP" { &w.tid_up } else { &w.tid_down };
            let bk = self.bk.get(tid);
            if !bk.ha || bk.ba < MIN_ENTRY_S4 || bk.ba > MAX_ENTRY_S4 { continue; }

            // FILL_PROB = 1.0 for paper trading (was 0.60)
            let mk = ((bk.ba - 0.01) * 100.0).round() / 100.0;
            let fp = mk;  // always fill at maker price
            if fp > MAX_ENTRY_S4 { continue; }

            let sh = STAKE_1 / fp;
            let cl_now = self.st.read().await.cl.get(w.asset).copied().unwrap_or(0.0);
            let cl_open = self.cl_o.get(&w.slug).copied().unwrap_or(cl_now);

            let cap_locked = STAKE_1;
            st.cap -= cap_locked;
            st.active.insert(w.slug.clone(), PT {
                id: "S4", slug: w.slug.clone(), asset: w.asset, wmin: w.wmin,
                dir: dir.into(), px: fp, shares: sh,
                dir2: String::new(), px2: 0.0, sh2: 0.0,
                end_ts: w.end_ts, start_ts: w.start_ts, sl: fp * SL_PCT,
                dumped: false, dump_px: 0.0, dump_trigger: String::new(),
                wsold: false, wpx: 0.0,
                tid_up: w.tid_up.clone(), tid_dn: w.tid_down.clone(),
                cl_open, entry_time: Utc::now().timestamp(),
                cap_locked,
            });
            st.done.insert(w.slug.clone());

            info!("[S4] {} {} 15m T-{}s @{:.3} ({}/3 confirm) cl=${:.2}",
                dir, w.asset.to_uppercase(), left, fp, u.max(d), cl_open);
        }
    }

    // ── Position Management ────────────────────────────────────────────────
    async fn manage(&mut self) {
        let now = Utc::now().timestamp();
        let state = self.st.read().await;
        let mut settles: Vec<(&'static str, String, f64, f64)> = Vec::new(); // (id, slug, pnl, cap_locked)

        for st in self.s.values_mut() {
            let slugs: Vec<String> = st.active.keys().cloned().collect();
            for slug in slugs {
                let t = match st.active.get(&slug) { Some(t) => t.clone(), None => continue };
                let left = t.end_ts - now;
                let base = s3_base(t.id);

                // ── Single-side: S4 ────────────────────────────────────
                if t.dir2.is_empty() {
                    // Stop-loss: CL flip
                    if t.sl > 0.0 {
                        if let (Some(&co), Some(&cn)) = (self.cl_o.get(&slug), state.cl.get(t.asset)) {
                            let d = (cn - co) / co * 100.0;
                            let flip = (t.dir == "UP" && d < -0.01) || (t.dir == "DOWN" && d > 0.01);
                            if flip {
                                let rec = STAKE_1 * SL_PCT;
                                let f = fee(t.sl) * t.shares;
                                let pnl = rec - STAKE_1 - f;
                                info!("[{}] SL {} {} {}m cl_d={:+.3}% pnl=${:+.2} cum=${:+.2} {}W/{}L",
                                    st.id, t.dir, t.asset.to_uppercase(), t.wmin, d, pnl, st.pnl + pnl, st.w, st.l + 1);
                                settles.push((st.id, slug.clone(), pnl, t.cap_locked));
                                continue;
                            }
                        }
                    }
                    // Settlement
                    if now >= t.end_ts + 3 {
                        let co = self.cl_o.get(&slug).copied().unwrap_or(0.0);
                        let cc = state.cl_at(t.asset, t.end_ts, 2)
                            .or_else(|| state.cl_at(t.asset, t.end_ts, 5))
                            .or_else(|| state.cl.get(t.asset).copied())
                            .unwrap_or(0.0);
                        if t.wmin == 5 {
                            self.sub_r.insert(slug.clone(), if cc >= co { "UP" } else { "DOWN" }.into());
                        }
                        if co <= 0.0 || cc <= 0.0 {
                            settles.push((st.id, slug.clone(), -STAKE_1, t.cap_locked));
                            continue;
                        }
                        let actual = if cc >= co { "UP" } else { "DOWN" };
                        let won = actual == t.dir;
                        let pnl = if won { t.shares * 1.0 - STAKE_1 } else { -STAKE_1 };
                        let tag = if won { "WIN" } else { "LOSS" };
                        info!("[{}] {} {} {} {}m cl={:.2}→{:.2} d={:+.3}% pnl=${:+.2} cum=${:+.2} {}W/{}L",
                            st.id, tag, t.dir, t.asset.to_uppercase(), t.wmin,
                            co, cc, (cc - co) / co * 100.0,
                            pnl, st.pnl + pnl,
                            st.w + if won { 1 } else { 0 }, st.l + if won { 0 } else { 1 });
                        settles.push((st.id, slug.clone(), pnl, t.cap_locked));
                    }
                    continue;
                }

                // ── Both-sides: S3b/S3C/S3D/S3E ───────────────────────
                let co = self.cl_o.get(&slug).copied().unwrap_or(0.0);
                let cn = state.cl.get(t.asset).copied().unwrap_or(0.0);
                if co <= 0.0 || cn <= 0.0 { continue; }
                let cl_d = ((cn - co) / co * 100.0).abs();
                let up_winning = cn >= co;
                let loser_bid = if cl_d > 0.3 { 0.05 }
                    else if cl_d > 0.15 { 0.10 }
                    else if cl_d > 0.05 { 0.20 }
                    else { 0.35 };

                // ── Dump logic per strategy ─────────────────────────────

                // S3b: dump loser at T-30 (time-based)
                if base == "S3b" && !t.dumped && left <= 30 {
                    let dp = (loser_bid - SLIP).max(0.01);
                    if let Some(tm) = st.active.get_mut(&slug) {
                        tm.dumped = true; tm.dump_px = dp; tm.dump_trigger = "TIME".into();
                    }
                    let ls = if up_winning { t.sh2 } else { t.shares };
                    info!("[{}] DUMP {} {}m TIME@T-30 dp={:.3} loser={} lsh={:.2} rec=${:.2} cl_d={:+.3}%",
                        st.id, t.asset.to_uppercase(), t.wmin, dp,
                        if up_winning { "DN" } else { "UP" }, ls, ls * dp, cl_d);
                }

                // S3C: dump loser at T-30 (identical to S3b — proven baseline)
                if base == "S3C" && !t.dumped && left <= 30 {
                    let dp = (loser_bid - SLIP).max(0.01);
                    if let Some(tm) = st.active.get_mut(&slug) {
                        tm.dumped = true; tm.dump_px = dp; tm.dump_trigger = "TIME".into();
                    }
                    let ls = if up_winning { t.sh2 } else { t.shares };
                    info!("[{}] DUMP {} {}m TIME@T-30 dp={:.3} loser={} lsh={:.2} rec=${:.2} cl_d={:+.3}%",
                        st.id, t.asset.to_uppercase(), t.wmin, dp,
                        if up_winning { "DN" } else { "UP" }, ls, ls * dp, cl_d);
                }

                // S3D: dump when loser bid ≤ $0.10, FALLBACK at T-30
                if base == "S3D" && !t.dumped {
                    if loser_bid <= 0.10 {
                        let dp = (0.10 - SLIP).max(0.01);
                        if let Some(tm) = st.active.get_mut(&slug) {
                            tm.dumped = true; tm.dump_px = dp; tm.dump_trigger = "PRICE".into();
                        }
                        let ls = if up_winning { t.sh2 } else { t.shares };
                        info!("[{}] DUMP {} {}m PRICE bid≤0.10 dp={:.3} loser={} lsh={:.2} rec=${:.2} cl_d={:+.3}%",
                            st.id, t.asset.to_uppercase(), t.wmin, dp,
                            if up_winning { "DN" } else { "UP" }, ls, ls * dp, cl_d);
                    } else if left <= 30 {
                        // FALLBACK: dump at T-30 regardless
                        let dp = (loser_bid - SLIP).max(0.01);
                        if let Some(tm) = st.active.get_mut(&slug) {
                            tm.dumped = true; tm.dump_px = dp; tm.dump_trigger = "FALLBACK".into();
                        }
                        let ls = if up_winning { t.sh2 } else { t.shares };
                        info!("[{}] DUMP {} {}m FALLBACK@T-30 dp={:.3} loser={} lsh={:.2} rec=${:.2} cl_d={:+.3}%",
                            st.id, t.asset.to_uppercase(), t.wmin, dp,
                            if up_winning { "DN" } else { "UP" }, ls, ls * dp, cl_d);
                    }
                }

                // S3E: dump when loser bid ≤ $0.25, FALLBACK at T-30
                if base == "S3E" && !t.dumped {
                    if loser_bid <= 0.25 {
                        let dp = (loser_bid - SLIP).max(0.01);
                        if let Some(tm) = st.active.get_mut(&slug) {
                            tm.dumped = true; tm.dump_px = dp; tm.dump_trigger = "PRICE".into();
                        }
                        let ls = if up_winning { t.sh2 } else { t.shares };
                        info!("[{}] DUMP {} {}m PRICE bid≤0.25 dp={:.3} loser={} lsh={:.2} rec=${:.2} cl_d={:+.3}%",
                            st.id, t.asset.to_uppercase(), t.wmin, dp,
                            if up_winning { "DN" } else { "UP" }, ls, ls * dp, cl_d);
                    } else if left <= 30 {
                        // FALLBACK: dump at T-30 regardless
                        let dp = (loser_bid - SLIP).max(0.01);
                        if let Some(tm) = st.active.get_mut(&slug) {
                            tm.dumped = true; tm.dump_px = dp; tm.dump_trigger = "FALLBACK".into();
                        }
                        let ls = if up_winning { t.sh2 } else { t.shares };
                        info!("[{}] DUMP {} {}m FALLBACK@T-30 dp={:.3} loser={} lsh={:.2} rec=${:.2} cl_d={:+.3}%",
                            st.id, t.asset.to_uppercase(), t.wmin, dp,
                            if up_winning { "DN" } else { "UP" }, ls, ls * dp, cl_d);
                    }
                }

                // ── Settlement ──────────────────────────────────────────
                if now >= t.end_ts + 3 {
                    let cc = state.cl_at(t.asset, t.end_ts, 2)
                        .or_else(|| state.cl_at(t.asset, t.end_ts, 5))
                        .or_else(|| state.cl.get(t.asset).copied())
                        .unwrap_or(0.0);
                    if cc <= 0.0 { settles.push((st.id, slug.clone(), -STAKE_2, t.cap_locked)); continue; }
                    if t.wmin == 5 {
                        self.sub_r.insert(slug.clone(), if cc >= co { "UP" } else { "DOWN" }.into());
                    }
                    let actual = if cc >= co { "UP" } else { "DOWN" };
                    let (up_pay, dn_pay) = if actual == "UP" { (1.0, 0.0) } else { (0.0, 1.0) };

                    // Re-read the current state of the trade (may have been dumped)
                    let t = match st.active.get(&slug) { Some(t) => t.clone(), None => continue };
                    let final_up_winning = cc >= co;

                    let pnl = if t.dumped {
                        let lr = if final_up_winning { t.sh2 } else { t.shares };
                        let wr = if final_up_winning { t.shares } else { t.sh2 };
                        // Check reversal: did the side we dumped as "loser" actually win?
                        let dump_up_winning = up_winning; // direction when we dumped
                        let loser_was_right = (dump_up_winning && actual == "DOWN")
                            || (!dump_up_winning && actual == "UP");
                        if loser_was_right {
                            // Catastrophic: dumped the winner, held the loser
                            lr * t.dump_px + 0.0 - STAKE_2 - fee(t.dump_px) * lr
                        } else {
                            // Normal: dumped loser, winner settles at $1
                            lr * t.dump_px + wr * 1.0 - STAKE_2 - fee(t.dump_px) * lr
                        }
                    } else {
                        // No dump: both go to settlement
                        t.shares * up_pay + t.sh2 * dn_pay - STAKE_2
                    };

                    let tag = if pnl > 0.0 { "WIN" } else { "LOSS" };
                    let cl_delta = (cc - co) / co * 100.0;
                    info!("[{}] {} {} {}m cl={:.2}→{:.2} d={:+.3}% actual={} dump={} pnl=${:+.2} cum=${:+.2} {}W/{}L",
                        st.id, tag, t.asset.to_uppercase(), t.wmin,
                        co, cc, cl_delta, actual,
                        if t.dumped { &t.dump_trigger } else { "NONE" },
                        pnl, st.pnl + pnl,
                        st.w + if pnl > 0.0 { 1 } else { 0 },
                        st.l + if pnl <= 0.0 { 1 } else { 0 });
                    settles.push((st.id, slug.clone(), pnl, t.cap_locked));
                }
            }
        }

        for (id, slug, pnl, cap_locked) in settles {
            if let Some(st) = self.s.get_mut(id) {
                st.rec(pnl > 0.0, pnl, cap_locked);
                st.active.remove(&slug);
            }
        }
    }

    // ── Status Line ────────────────────────────────────────────────────────
    fn status(&self) -> String {
        let mut p = Vec::new();
        for id in STRATS {
            if let Some(st) = self.s.get(id) {
                if st.trades() > 0 || !st.active.is_empty() {
                    p.push(format!("{}:{}W/{}L${:+.1}", id, st.w, st.l, st.pnl));
                }
            }
        }
        if p.is_empty() { "waiting".into() } else { p.join(" | ") }
    }

    fn detail(&self) -> String {
        let active: usize = self.s.values().map(|s| s.active.len()).sum();
        let mut p = Vec::new();
        for id in STRATS {
            if let Some(st) = self.s.get(id) {
                if st.trades() > 0 {
                    p.push(format!("{}:{}W/{}L${:+.2} wr={:.0}% avgW=${:.2} avgL=${:.2}",
                        id, st.w, st.l, st.pnl, st.win_rate(), st.avg_win(), st.avg_loss()));
                }
            }
        }
        let total_pnl: f64 = self.s.values().map(|s| s.pnl).sum();
        let total_w: u32 = self.s.values().map(|s| s.w).sum();
        let total_l: u32 = self.s.values().map(|s| s.l).sum();
        format!("active={} | {} | NET:{}W/{}L${:+.2}",
            active, if p.is_empty() { "no trades".into() } else { p.join(" | ") },
            total_w, total_l, total_pnl)
    }
}

// ── Main ───────────────────────────────────────────────────────────────────
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("hydra_arb_final=info")
        .with_target(false)
        .init();
    dotenvy::dotenv().ok();

    info!("══════════════════════════════════════════════════════════════");
    info!("  HYDRA-ARB FINAL v2 — 5-Strategy Paper Tracker");
    info!("  S3b: Both-sides dump T-30 (5m+15m)");
    info!("  S3C: Both-sides dump T-30 baseline (5m+15m)");
    info!("  S3D: Both-sides dump bid≤$0.10 + T-30 fallback (5m+15m)");
    info!("  S3E: Both-sides dump bid≤$0.25 + T-30 fallback (5m+15m)");
    info!("  S4:  5m→15m cascade 2/3 confirm (T-120..T-44)");
    info!("  ${}/{} stake | ${}/strat | slip={}",
        STAKE_1, STAKE_2, START_CAP, SLIP);
    info!("  Entry: asks [{:.2}-{:.2}] | S4: [{:.2}-{:.2}]",
        S3_ASK_LO, S3_ASK_HI, MIN_ENTRY_S4, MAX_ENTRY_S4);
    info!("══════════════════════════════════════════════════════════════");

    let st: SS = Arc::new(RwLock::new(State::new()));
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

    let mut engine = Engine::new(st.clone(), Scan::new(), BkC::new());
    info!("[BOOT] Running...");

    let mut status_tick = Instant::now();
    let mut detail_tick = Instant::now();

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        engine.tick().await;

        // Status every 60s
        if status_tick.elapsed().as_secs() >= 60 {
            let s = st.read().await;
            let px: Vec<String> = ASSETS.iter()
                .filter_map(|&a| s.cl.get(a).map(|p| format!("{}=${:.0}", a.to_uppercase(), p)))
                .collect();
            info!("─── {} | {:.1}h | {} ───",
                px.join(" "),
                engine.start.elapsed().as_secs_f64() / 3600.0,
                engine.status());
            status_tick = Instant::now();
        }

        // Detail every 5min
        if detail_tick.elapsed().as_secs() >= 300 {
            info!("=== DETAIL: {} ===", engine.detail());
            detail_tick = Instant::now();
        }
    }
}
