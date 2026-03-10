//! CL Sniper — 9th March 2026
//! 10 engines × 2 timeframes × 2 regime modes = 40 paper trackers
//! A/A1: δ≥0.10% | B/B1: δ≥0.10%+3tick | C/C1: δ≥0.03%+3tick
//! D/D1: δ≥0.15% | E/E1: δ≥0.05%
//! Each: 5m, 5m+regime, 15m, 15m+regime
//! Regime: skip entry when 1h BTC range < 0.3%
//! SL: maker sell at 50% of fill, posted on entry, cancel at T-3
//! Tick: 500ms during active, 1s idle

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
const STDEV: &[(&str,f64)] = &[("btc",0.167),("eth",0.194),("sol",0.247),("xrp",0.440)];
const STDEV_BASE: f64 = 0.167;
const STAKE: f64 = 5.0;
const MIN_ENTRY: f64 = 0.85;
const MAX_ENTRY: f64 = 0.98;
const ENTRY_START: i64 = 57;
const TAKER_DEADLINE: i64 = 44;
const SL_SHARE_PCT: f64 = 0.50;
const FILL_PROB: f64 = 0.60;
const SLIP: f64 = 0.005;
const MAX_DD: f64 = 35.0;
const MAX_CONSEC: u32 = 4;
const MAX_CONC: usize = 6;
const REGIME_THRESH: f64 = 0.3; // 1h range < 0.3% = CHOP

const RTDS_WS: &str = "wss://ws-live-data.polymarket.com";
const BN_WS: &str = "wss://stream.binance.com:9443/ws";
const GAMMA: &str = "https://gamma-api.polymarket.com";
const CLOB: &str = "https://clob.polymarket.com";

fn stdev(a: &str) -> f64 { STDEV.iter().find(|(k,_)|*k==a).map(|(_,v)|*v).unwrap_or(STDEV_BASE) }

// ── State ────────────────────────────────────────────────────────────────────
type SS = Arc<RwLock<State>>;
struct State {
    cl: HashMap<&'static str, f64>,
    snap: HashMap<&'static str, HashMap<i64, f64>>,
    bn: HashMap<&'static str, f64>,
    bnh: HashMap<&'static str, VecDeque<(f64,f64)>>,
}
impl State {
    fn new() -> Self { State { cl:HashMap::new(), snap:HashMap::new(), bn:HashMap::new(), bnh:HashMap::new() } }
    fn cl_up(&mut self, a: &'static str, px: f64, ts: f64) {
        self.cl.insert(a, px);
        let s = self.snap.entry(a).or_default(); s.insert(ts as i64, px);
        let c = ts as i64 - 7200; s.retain(|k,_|*k>c);
    }
    fn bn_up(&mut self, a: &'static str, px: f64) {
        let ts = Utc::now().timestamp_millis() as f64/1000.0;
        self.bn.insert(a, px);
        let h = self.bnh.entry(a).or_default(); h.push_back((ts,px));
        if h.len()>14400 { h.pop_front(); }
    }
    fn cl_at(&self, a: &str, t: i64) -> Option<f64> {
        // Exact first, then ±1s
        let s = self.snap.get(a)?;
        if let Some(&p) = s.get(&t) { return Some(p); }
        if let Some(&p) = s.get(&(t+1)) { return Some(p); }
        if let Some(&p) = s.get(&(t-1)) { return Some(p); }
        None
    }
    fn cl_latest(&self, a: &str) -> Option<f64> { self.cl.get(a).copied() }
    fn bn_trend(&self, a: &str, sec: u64) -> Option<f64> {
        let h = self.bnh.get(a)?; if h.len()<2 { return None; }
        let now = Utc::now().timestamp_millis() as f64/1000.0;
        let old = h.iter().find(|(t,_)|*t>=now-sec as f64)?;
        if old.1<=0.0 { return None; }
        Some((h.back()?.1-old.1)/old.1*100.0)
    }
    fn cl_trend(&self, a: &str, sec: u64) -> Option<f64> {
        let s = self.snap.get(a)?; if s.is_empty() { return None; }
        let now = Utc::now().timestamp(); let cut = now-sec as i64;
        let cur = self.cl.get(a)?;
        let old = s.iter().filter(|(&t,_)|t>=cut).min_by_key(|(&t,_)|t).map(|(_,&p)|p)?;
        if old<=0.0 { return None; }
        Some((cur-old)/old*100.0)
    }
    /// 1h range for regime detection
    fn hour_range(&self, a: &str) -> f64 {
        let s = match self.snap.get(a) { Some(s)=>s, None=>return 0.0 };
        let now = Utc::now().timestamp(); let cut = now-3600;
        let prices: Vec<f64> = s.iter().filter(|(&t,_)|t>cut).map(|(_,&p)|p).collect();
        if prices.len()<10 { return 999.0; } // not enough data, allow trading
        let hi = prices.iter().cloned().fold(f64::MIN, f64::max);
        let lo = prices.iter().cloned().fold(f64::MAX, f64::min);
        if lo<=0.0 { return 0.0; }
        (hi-lo)/lo*100.0
    }
}

// ── Feeds ────────────────────────────────────────────────────────────────────
fn cl_asset(s: &str) -> Option<&'static str> {
    match s { "btc/usd"|"btcusd"|"btc"=>Some("btc"), "eth/usd"|"ethusd"|"eth"=>Some("eth"),
              "sol/usd"|"solusd"|"sol"=>Some("sol"), "xrp/usd"|"xrpusd"|"xrp"=>Some("xrp"), _=>None }
}
fn bnsym(a: &str) -> &'static str { match a {"btc"=>"btcusdt","eth"=>"ethusdt","sol"=>"solusdt","xrp"=>"xrpusdt",_=>"btcusdt"} }

async fn cl_feed(st: SS) { loop {
    info!("[CL] Connecting..."); if let Err(e) = cl_ws(&st).await { error!("[CL] {}", e); }
    tokio::time::sleep(Duration::from_secs(3)).await;
}}
async fn cl_ws(st: &SS) -> Result<()> {
    let (mut ws,_) = connect_async(RTDS_WS).await.context("CL")?;
    ws.send(Message::Text(json!({"action":"subscribe","subscriptions":[
        {"topic":"crypto_prices_chainlink","type":"*","filters":""}]}).to_string())).await?;
    info!("[CL] OK");
    while let Some(msg) = ws.next().await { match msg {
        Ok(Message::Text(t)) => {
            let d: Value = match serde_json::from_str(&t) { Ok(d)=>d, _=>continue };
            if d.get("topic").and_then(|t|t.as_str())!=Some("crypto_prices_chainlink") { continue; }
            let p = match d.get("payload") { Some(p)=>p, _=>continue };
            let sym = p.get("symbol").and_then(|s|s.as_str()).unwrap_or("").to_lowercase();
            let val = p.get("value").and_then(|v|v.as_f64().or(v.as_str().and_then(|s|s.parse().ok())));
            let rts = p.get("timestamp").and_then(|t|t.as_f64().or(t.as_i64().map(|i|i as f64))).unwrap_or(0.0);
            let ts = if rts>1e12{rts/1000.0} else if rts>1e9{rts} else {Utc::now().timestamp() as f64};
            if let (Some(a),Some(px)) = (cl_asset(&sym),val) { if px>0.0 { st.write().await.cl_up(a,px,ts); } }
        }
        Ok(Message::Ping(d)) => { let _ = ws.send(Message::Pong(d)).await; }
        Ok(_)=>{} Err(e)=>{error!("[CL] {}",e); break;}
    }}
    Ok(())
}
async fn bn_feed(st: SS) { loop {
    info!("[BN] Connecting..."); if let Err(e) = bn_ws(&st).await { error!("[BN] {}",e); }
    tokio::time::sleep(Duration::from_secs(3)).await;
}}
async fn bn_ws(st: &SS) -> Result<()> {
    let streams: Vec<String> = ASSETS.iter().map(|a|format!("{}@aggTrade",bnsym(a))).collect();
    let (mut ws,_) = connect_async(format!("{}/{}",BN_WS,streams.join("/"))).await.context("BN")?;
    info!("[BN] OK");
    while let Some(msg) = ws.next().await { match msg {
        Ok(Message::Text(t)) => {
            let d: Value = match serde_json::from_str(&t) { Ok(d)=>d, _=>continue };
            let i = d.get("data").unwrap_or(&d);
            let sym = i.get("s").and_then(|s|s.as_str()).unwrap_or("").to_lowercase();
            let px = i.get("p").and_then(|p|p.as_str().and_then(|s|s.parse::<f64>().ok()));
            let a: Option<&'static str> = match sym.as_str() {
                "btcusdt"=>Some("btc"),"ethusdt"=>Some("eth"),"solusdt"=>Some("sol"),"xrpusdt"=>Some("xrp"),_=>None};
            if let (Some(a),Some(p)) = (a,px) { if p>0.0 { st.write().await.bn_up(a,p); } }
        }
        Ok(Message::Ping(d)) => { let _ = ws.send(Message::Pong(d)).await; }
        Ok(_)=>{} Err(e)=>{error!("[BN] {}",e); break;}
    }}
    Ok(())
}

// ── Scanner ──────────────────────────────────────────────────────────────────
#[derive(Clone,Debug)]
struct Win { slug: String, asset: &'static str, wmin: u32, tid_up: String, tid_dn: String, start_ts: i64, end_ts: i64 }
impl Win { fn left(&self)->i64 { self.end_ts-Utc::now().timestamp() } }

struct Scan { http: reqwest::Client, cache: Vec<Win>, last: Instant }
impl Scan {
    fn new()->Self { Scan { http:reqwest::Client::builder().user_agent("cls/8").timeout(Duration::from_secs(5)).build().expect("h"), cache:Vec::new(), last:Instant::now()-Duration::from_secs(999) } }
    async fn get(&mut self)->Vec<Win> {
        if self.last.elapsed()<Duration::from_secs(10) { return self.cache.iter().filter(|w|w.left()>0).cloned().collect(); }
        let now = Utc::now().timestamp(); let mut ws = Vec::new();
        for &a in ASSETS { for &wm in &[5u32,15] {
            let iv = wm as i64*60; let s0 = (now/iv)*iv;
            for st in [s0, s0+iv] {
                let et = st+iv; if et<now { continue; }
                let slug = format!("{}-updown-{}m-{}",a,wm,st);
                let r = match self.http.get(format!("{}/markets",GAMMA)).query(&[("slug",&slug)]).send().await { Ok(r) if r.status().is_success()=>r, _=>continue };
                let d: Value = match r.json().await { Ok(d)=>d, _=>continue };
                let m = if d.is_array() { match d.as_array().and_then(|a|a.first()) { Some(m)=>m.clone(), None=>continue } } else { d };
                let tr = m.get("clobTokenIds").unwrap_or(&Value::Null);
                let tids: Vec<String> = if tr.is_string() { serde_json::from_str(tr.as_str().unwrap_or("[]")).unwrap_or_default() } else { serde_json::from_value(tr.clone()).unwrap_or_default() };
                if tids.len()<2 { continue; }
                let or = m.get("outcomes").unwrap_or(&Value::Null);
                let outs: Vec<String> = if or.is_string() { serde_json::from_str(or.as_str().unwrap_or("[]")).unwrap_or_default() } else { serde_json::from_value(or.clone()).unwrap_or_default() };
                let (tu,td) = if outs.len()>=2 && outs[0]=="Down" { (tids[1].clone(),tids[0].clone()) } else { (tids[0].clone(),tids[1].clone()) };
                ws.push(Win { slug, asset:a, wmin:wm, tid_up:tu, tid_dn:td, start_ts:st, end_ts:et });
            }
        }}
        self.cache = ws.clone(); self.last = Instant::now();
        ws.into_iter().filter(|w|w.left()>0).collect()
    }
}

// ── Book Cache ───────────────────────────────────────────────────────────────
#[derive(Clone,Default,Debug)]
struct Bk { bb: f64, ba: f64, ha: bool, hb: bool }
struct BkC { http: reqwest::Client, c: HashMap<String,Bk>, t: HashMap<String,Instant> }
impl BkC {
    fn new()->Self { BkC { http:reqwest::Client::builder().user_agent("cls/8").timeout(Duration::from_secs(2)).build().expect("h"), c:HashMap::new(), t:HashMap::new() } }
    fn get(&self, tid: &str)->Bk { self.c.get(tid).cloned().unwrap_or_default() }
    async fn refresh(&mut self, tids: &[String]) {
        let stale: Vec<&String> = tids.iter().filter(|t|self.t.get(*t).map(|ts|ts.elapsed()>=Duration::from_millis(400)).unwrap_or(true)).collect();
        if stale.is_empty() { return; }
        let body: Vec<Value> = stale.iter().map(|t|json!({"token_id":t})).collect();
        let r = match self.http.post(format!("{}/books",CLOB)).json(&body).send().await { Ok(r) if r.status().is_success()=>r, _=>return };
        let res: Vec<Value> = match r.json().await { Ok(d)=>d, _=>return };
        let now = Instant::now();
        for item in &res {
            let tid = item.get("asset_id").and_then(|v|v.as_str()).unwrap_or("").to_string();
            if tid.is_empty() { continue; }
            let mut bk = Bk::default();
            if let Some(bids) = item.get("bids").and_then(|b|b.as_array()) {
                let mut p: Vec<f64> = bids.iter().filter_map(|b|b.get("price").and_then(|p|p.as_str().and_then(|s|s.parse().ok()))).collect();
                p.sort_by(|a,b|b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
                if let Some(&v) = p.first() { bk.bb=v; bk.hb=true; }
            }
            if let Some(asks) = item.get("asks").and_then(|a|a.as_array()) {
                let mut p: Vec<f64> = asks.iter().filter_map(|a|a.get("price").and_then(|p|p.as_str().and_then(|s|s.parse().ok()))).collect();
                p.sort_by(|a,b|a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                if let Some(&v) = p.first() { bk.ba=v; bk.ha=true; }
            }
            self.c.insert(tid.clone(), bk); self.t.insert(tid, now);
        }
    }
}

// ── Engine Config ────────────────────────────────────────────────────────────
#[derive(Clone)]
struct EngCfg {
    id: &'static str,
    delta: f64,          // base threshold (scaled by asset stdev)
    continuity: u32,     // 0=single tick, 3=sustained
    bn_contra: bool,     // BN contra 0.02% on 15s
    cl_fade: bool,       // CL fade 0.03% on 10s
    wmin: u32,           // 5 or 15
    regime: bool,        // skip chop
}

fn all_engines() -> Vec<EngCfg> {
    let bases: Vec<(&str, f64, u32, bool, bool)> = vec![
        ("A",   0.10, 0, false, false),
        ("A1",  0.10, 0, true,  true),
        ("B",   0.10, 3, false, false),
        ("B1",  0.10, 3, true,  true),
        ("C",   0.03, 3, false, false),
        ("C1",  0.03, 3, true,  true),
        ("D",   0.15, 0, false, false),
        ("D1",  0.15, 0, true,  true),
        ("E",   0.05, 0, false, false),
        ("E1",  0.05, 0, true,  true),
    ];
    let mut out = Vec::new();
    for (id, delta, cont, bn, cl) in &bases {
        for &wmin in &[5u32, 15] {
            for &regime in &[false, true] {
                let suffix = format!("{}_{}m{}", id, wmin, if regime {"R"} else {""});
                // Leak string to get 'static — fine for fixed config
                let sid: &'static str = Box::leak(suffix.into_boxed_str());
                out.push(EngCfg { id: sid, delta: *delta, continuity: *cont,
                    bn_contra: *bn, cl_fade: *cl, wmin, regime });
            }
        }
    }
    out
}

// ── Paper Tracker ────────────────────────────────────────────────────────────
#[derive(Clone)]
struct PT {
    dir: String, asset: &'static str, px: f64, shares: f64,
    sl_px: f64, end_ts: i64, tid: String,
}

struct Tracker {
    cfg: EngCfg,
    cap: f64, w: u32, l: u32, sl: u32, pnl: f64,
    active: Option<(String, PT)>, // slug -> trade
    done: HashSet<String>,
    delta_ticks: HashMap<String, u32>, // slug -> consecutive ticks above threshold
}

impl Tracker {
    fn new(cfg: EngCfg) -> Self {
        Tracker { cfg, cap: 100.0, w:0, l:0, sl:0, pnl:0.0,
                  active:None, done:HashSet::new(), delta_ticks:HashMap::new() }
    }
    fn total(&self) -> u32 { self.w+self.l+self.sl }
    fn wr(&self) -> f64 { if self.total()>0 { self.w as f64/self.total() as f64*100.0 } else { 0.0 } }
    fn record_win(&mut self, p: f64) { self.w+=1; self.pnl+=p; }
    fn record_loss(&mut self, p: f64) { self.l+=1; self.pnl+=p; }
    fn record_sl(&mut self, p: f64) { self.sl+=1; self.pnl+=p; }
}

// ── Main Engine ──────────────────────────────────────────────────────────────
struct Sniper {
    st: SS, scan: Scan, bk: BkC,
    trackers: Vec<Tracker>,
    cl_opens: HashMap<String, f64>,
    start: Instant,
}

impl Sniper {
    fn new(st: SS, scan: Scan, bk: BkC) -> Self {
        let cfgs = all_engines();
        let trackers: Vec<Tracker> = cfgs.into_iter().map(|c| Tracker::new(c)).collect();
        info!("[BOOT] {} trackers initialized", trackers.len());
        Sniper { st, scan, bk, trackers, cl_opens: HashMap::new(), start: Instant::now() }
    }

    fn has_active(&self) -> bool { self.trackers.iter().any(|t| t.active.is_some()) }

    async fn tick(&mut self) {
        let wins = self.scan.get().await;

        // Collect tids for book refresh
        let mut tids: Vec<String> = Vec::new();
        for w in &wins { if w.left()>0 && w.left()<120 { tids.push(w.tid_up.clone()); tids.push(w.tid_dn.clone()); } }
        // Add held position tids
        for tr in &self.trackers {
            if let Some((_,pt)) = &tr.active { tids.push(pt.tid.clone()); }
        }
        tids.sort(); tids.dedup();
        if !tids.is_empty() { self.bk.refresh(&tids).await; }

        // Record CL opens
        { let s = self.st.read().await;
          for w in &wins {
              if self.cl_opens.contains_key(&w.slug) { continue; }
              let now = Utc::now().timestamp();
              if now>=w.start_ts && now<=w.end_ts {
                  if let Some(&px) = s.cl.get(w.asset) { if px>0.0 { self.cl_opens.insert(w.slug.clone(), px); } }
              }
          }
        }

        // Evaluate each tracker
        let s = self.st.read().await;
        let hour_ranges: HashMap<&str, f64> = ASSETS.iter().map(|&a| (a, s.hour_range(a))).collect();

        for tr in &mut self.trackers {
            // === SETTLE active trade ===
            if let Some((slug, pt)) = &tr.active {
                let now = Utc::now().timestamp();
                let left = pt.end_ts - now;

                // SL check: bid <= 50% of fill price
                let bk = self.bk.get(&pt.tid);
                if bk.hb && bk.bb <= pt.sl_px {
                    let recovery = pt.shares * (bk.bb - SLIP).max(0.0);
                    let pnl = recovery - STAKE;
                    info!("[{}] SL {} {} bid={:.3}<={:.3} ${:+.2}", tr.cfg.id, pt.dir, pt.asset.to_uppercase(), bk.bb, pt.sl_px, pnl);
                    tr.record_sl(pnl);
                    tr.active = None;
                    continue;
                }

                // Settlement
                if now >= pt.end_ts + 3 {
                    let cl_open = self.cl_opens.get(slug).copied().unwrap_or(0.0);
                    // Exact CL at end, fallback to latest
                    let cl_close = s.cl_at(pt.asset, pt.end_ts)
                        .or_else(|| s.cl_latest(pt.asset)).unwrap_or(0.0);

                    // CLOB cross-check
                    let bk_up = self.bk.get(&self.win_tid_up(slug, &wins));
                    let bk_dn = self.bk.get(&self.win_tid_dn(slug, &wins));

                    let cl_dir = if cl_open>0.0 && cl_close>0.0 {
                        Some(if cl_close>=cl_open {"UP"} else {"DOWN"})
                    } else { None };

                    let clob_dir = if bk_up.hb && bk_up.bb>0.80 { Some("UP") }
                        else if bk_dn.hb && bk_dn.bb>0.80 { Some("DOWN") }
                        else { None };

                    if let (Some(cd), Some(cb)) = (cl_dir, clob_dir) {
                        if cd != cb { warn!("[{}] CL/CLOB disagree: CL={} CLOB={} {}", tr.cfg.id, cd, cb, slug); }
                    }

                    let actual = cl_dir.or(clob_dir);

                    if let Some(actual) = actual {
                        let won = actual == pt.dir;
                        let pnl = if won { pt.shares * 1.0 - STAKE } else { -STAKE };
                        if won {
                            info!("[{}] WIN {} {} @{:.3} ${:+.2}", tr.cfg.id, pt.asset.to_uppercase(), pt.dir, pt.px, pnl);
                            tr.record_win(pnl);
                        } else {
                            info!("[{}] LOSS {} {} @{:.3} ${:+.2}", tr.cfg.id, pt.asset.to_uppercase(), pt.dir, pt.px, pnl);
                            tr.record_loss(pnl);
                        }
                    } else {
                        warn!("[{}] NO_SETTLE {} — returning stake", tr.cfg.id, slug);
                    }
                    tr.active = None;
                }
                continue;
            }

            // === ENTRY evaluation ===
            if tr.active.is_some() { continue; }
            if tr.cap < STAKE { continue; }

            for w in &wins {
                if w.wmin != tr.cfg.wmin { continue; }
                let left = w.left();
                if left > ENTRY_START || left < TAKER_DEADLINE { continue; }
                if tr.done.contains(&w.slug) { continue; }

                let cl_open = match self.cl_opens.get(&w.slug) { Some(&p) if p>0.0=>p, _=>continue };
                let cl_now = match s.cl.get(w.asset) { Some(&p) if p>0.0=>p, _=>continue };
                let delta = (cl_now - cl_open) / cl_open * 100.0;
                if delta.abs() < 0.001 { continue; }

                let dir = if delta > 0.0 { "UP" } else { "DOWN" };
                let sc = stdev(w.asset) / STDEV_BASE;
                let threshold = tr.cfg.delta * sc;

                // Delta threshold
                if delta.abs() < threshold { 
                    tr.delta_ticks.remove(&w.slug);
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
                        if (dir=="UP" && bt < -0.02) || (dir=="DOWN" && bt > 0.02) { continue; }
                    }
                }

                // CL fade
                if tr.cfg.cl_fade {
                    if let Some(ct) = s.cl_trend(w.asset, 10) {
                        if (dir=="UP" && ct < -0.03) || (dir=="DOWN" && ct > 0.03) { continue; }
                    }
                }

                // Regime check
                if tr.cfg.regime {
                    let range = hour_ranges.get(w.asset).copied().unwrap_or(999.0);
                    if range < REGIME_THRESH { continue; }
                }

                // Book check
                let tid = if dir=="UP" { &w.tid_up } else { &w.tid_dn };
                let bk = self.bk.get(tid);
                if !bk.ha || bk.ba < MIN_ENTRY || bk.ba > MAX_ENTRY { continue; }

                // Fill simulation
                let mk = ((bk.ba - 0.01) * 100.0).round() / 100.0;
                let fp = if mk >= bk.ba { mk }
                    else if left <= 45 { (bk.ba + SLIP).min(0.99) }  // taker fallback
                    else {
                        // Maker fill prob per 500ms tick ≈ 35%
                        let elapsed = tr.delta_ticks.get(&w.slug).copied().unwrap_or(1);
                        if elapsed > 2 { mk } else { continue; }
                    };
                if fp > MAX_ENTRY { continue; }

                let shares = STAKE / fp;
                let sl_px = fp * SL_SHARE_PCT;

                tr.active = Some((w.slug.clone(), PT {
                    dir: dir.to_string(), asset: w.asset, px: fp, shares,
                    sl_px, end_ts: w.end_ts, tid: tid.to_string(),
                }));
                tr.done.insert(w.slug.clone());
                tr.delta_ticks.remove(&w.slug);

                info!("[{}] {} {} {}m @{:.3} d={:+.3}%", tr.cfg.id, dir, w.asset.to_uppercase(), w.wmin, fp, delta);
                break; // one entry per tracker per tick
            }
        }
        drop(s);

        // Cleanup
        let now = Utc::now().timestamp(); let cut = now - 3600;
        self.cl_opens.retain(|k,_| k.rsplit('-').next().and_then(|s|s.parse::<i64>().ok()).map(|t|t>cut).unwrap_or(false));
        for tr in &mut self.trackers {
            tr.done.retain(|k| k.rsplit('-').next().and_then(|s|s.parse::<i64>().ok()).map(|t|t>cut).unwrap_or(false));
            tr.delta_ticks.retain(|k,_| k.rsplit('-').next().and_then(|s|s.parse::<i64>().ok()).map(|t|t>cut).unwrap_or(false));
        }
    }

    fn win_tid_up(&self, slug: &str, wins: &[Win]) -> String {
        wins.iter().find(|w| w.slug == slug).map(|w| w.tid_up.clone()).unwrap_or_default()
    }
    fn win_tid_dn(&self, slug: &str, wins: &[Win]) -> String {
        wins.iter().find(|w| w.slug == slug).map(|w| w.tid_dn.clone()).unwrap_or_default()
    }

    fn status(&self) -> String {
        let mut active_count = 0;
        let mut parts: Vec<String> = Vec::new();
        for tr in &self.trackers {
            if tr.active.is_some() { active_count += 1; }
            if tr.total() > 0 {
                parts.push(format!("{}:{}W/{}L/{}S${:+.1}", tr.cfg.id, tr.w, tr.l, tr.sl, tr.pnl));
            }
        }
        if parts.is_empty() { format!("active={} waiting", active_count) }
        else { format!("active={} | {}", active_count, parts.join(" | ")) }
    }

    fn summary(&self) -> String {
        // Group by base engine
        let mut groups: HashMap<String, (u32,u32,u32,f64,u32)> = HashMap::new();
        for tr in &self.trackers {
            let base = tr.cfg.id.split('_').next().unwrap_or("?").to_string();
            let e = groups.entry(base).or_insert((0,0,0,0.0,0));
            e.0 += tr.w; e.1 += tr.l; e.2 += tr.sl; e.3 += tr.pnl; e.4 += tr.total();
        }
        let mut parts: Vec<String> = Vec::new();
        for base in ["A","A1","B","B1","C","C1","D","D1","E","E1"] {
            if let Some((w,l,sl,pnl,_)) = groups.get(base) {
                if *w+*l+*sl > 0 {
                    parts.push(format!("{}:{}W/{}L/{}S${:+.1}", base, w, l, sl, pnl));
                }
            }
        }
        parts.join(" | ")
    }
}

// ── Main ─────────────────────────────────────────────────────────────────────
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("cl_sniper_9mar=info").with_target(false).init();
    dotenvy::dotenv().ok();

    info!("================================================================");
    info!("  CL SNIPER — 9th March 2026");
    info!("  10 engines x 2 timeframes x 2 regime = 40 trackers");
    info!("  A/A1: d>=0.10% | B/B1: d>=0.10%+3tick | C/C1: d>=0.03%+3tick");
    info!("  D/D1: d>=0.15% | E/E1: d>=0.05%");
    info!("  SL: maker sell at 50% of fill | Tick: 500ms/1s adaptive");
    info!("  Regime: 1h range < {}% = CHOP, skip", REGIME_THRESH);
    info!("================================================================");

    let st: SS = Arc::new(RwLock::new(State::new()));
    let c = st.clone(); tokio::spawn(async move { cl_feed(c).await; });
    let b = st.clone(); tokio::spawn(async move { bn_feed(b).await; });

    info!("[BOOT] Waiting for feeds...");
    for _ in 0..20 { tokio::time::sleep(Duration::from_secs(1)).await;
        let s = st.read().await; if s.cl.contains_key("btc") && s.bn.contains_key("btc") { break; } }
    { let s = st.read().await; for &a in ASSETS {
        info!("  {}: CL=${:.2} BN=${:.2}", a.to_uppercase(), s.cl.get(a).copied().unwrap_or(0.0), s.bn.get(a).copied().unwrap_or(0.0));
    }}

    let mut sniper = Sniper::new(st.clone(), Scan::new(), BkC::new());
    info!("[BOOT] Running {} trackers...", sniper.trackers.len());

    let mut last_status = Instant::now();
    let mut last_summary = Instant::now();
    loop {
        let sleep_ms = if sniper.has_active() { 500 } else { 1000 };
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        sniper.tick().await;

        if last_status.elapsed().as_secs() >= 60 {
            let s = st.read().await;
            let px: Vec<String> = ASSETS.iter().filter_map(|&a|s.cl.get(a).map(|p|format!("{}=${:.0}",a.to_uppercase(),p))).collect();
            let hrs = sniper.start.elapsed().as_secs_f64()/3600.0;
            info!("--- {} | {:.1}h | {} ---", px.join(" "), hrs, sniper.summary());
            last_status = Instant::now();
        }

        // Detailed status every 5 min
        if last_summary.elapsed().as_secs() >= 300 {
            info!("=== DETAIL: {} ===", sniper.status());
            last_summary = Instant::now();
        }
    }
}
