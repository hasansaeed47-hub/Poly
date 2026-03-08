//! Hydra — 3-strategy both-sides paper tracker for Polymarket 5m markets
//! S3b: Maker dump T-30 | S3C: Taker dump T-30 | S3D: Taker dump bid≤$0.10
//!
//! Terminal: clean one-liners only
//! File log: hydra_trades.csv — full audit trail for post-analysis

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info};

const ASSETS: &[&str] = &["btc", "eth", "sol", "xrp"];
const STAKE_1: f64 = 5.0;
const STAKE_2: f64 = 10.0;
const START_CAP: f64 = 100.0;
const LATENCY_TICKS: f64 = 0.001;
const STALE_SECS: u64 = 3;
const SPREAD_WARN: f64 = 0.05;
const RTDS_WS: &str = "wss://ws-live-data.polymarket.com";
const BN_WS: &str = "wss://stream.binance.com:9443/ws";
const GAMMA: &str = "https://gamma-api.polymarket.com";
const CLOB: &str = "https://clob.polymarket.com";
const LOG_FILE: &str = "hydra_trades.csv";

fn fee(px: f64) -> f64 { px * (1.0 - px) * 0.0625 }

// ── CSV trade log ──────────────────────────────────────────────────
struct TradeLog { f: std::fs::File }
impl TradeLog {
    fn new() -> Self {
        let exists = std::path::Path::new(LOG_FILE).exists();
        let f = OpenOptions::new().create(true).append(true).open(LOG_FILE).expect("open log");
        let mut tl = TradeLog { f };
        if !exists {
            tl.hdr();
        }
        tl
    }
    fn hdr(&mut self) {
        let _ = writeln!(self.f, "ts,event,strategy,asset,slug,left_s,\
            up_ask,up_bid,up_spread,up_age_ms,\
            dn_ask,dn_bid,dn_spread,dn_age_ms,\
            fill_up,fill_dn,cost_up,cost_dn,\
            dump_side,dump_px,dump_bid,dump_bid_src,dump_rec,\
            cl_open,cl_close,cl_delta_pct,\
            settle_dir,settle_src,clob_up_bid,clob_dn_bid,\
            entry_fees,dump_fee,\
            pnl,cum_pnl,cap,w,l,flags");
    }
    fn entry(&mut self, id: &str, asset: &str, slug: &str, left: i64,
             bu: &Bk, bd: &Bk, up_age: u64, dn_age: u64,
             fill_up: f64, fill_dn: f64, flags: &str) {
        let _ = writeln!(self.f, "{},ENTRY,{},{},{},{},\
            {:.4},{:.4},{:.4},{},\
            {:.4},{:.4},{:.4},{},\
            {:.4},{:.4},{:.4},{:.4},\
            ,,,,\
            ,,,,,,\
            ,,\
            ,,,,,,{}",
            Utc::now().to_rfc3339(), id, asset, slug, left,
            bu.ba, if bu.hb {bu.bb} else {0.0}, bu.spread(), up_age,
            bd.ba, if bd.hb {bd.bb} else {0.0}, bd.spread(), dn_age,
            fill_up, fill_dn, fill_up*(STAKE_1/fill_up), fill_dn*(STAKE_1/fill_dn),
            flags);
        let _ = self.f.flush();
    }
    fn dump(&mut self, id: &str, asset: &str, slug: &str, left: i64,
            dump_side: &str, dump_px: f64, dump_bid: f64, bid_src: &str,
            dump_rec: f64, cl_open: f64, cl_now: f64, flags: &str) {
        let cl_d = if cl_open > 0.0 { (cl_now-cl_open)/cl_open*100.0 } else { 0.0 };
        let _ = writeln!(self.f, "{},DUMP,{},{},{},{},\
            ,,,,\
            ,,,,\
            ,,,,\
            {},{:.4},{:.4},{},{:.4},\
            {:.2},{:.2},{:+.4},\
            ,,,,\
            ,,\
            ,,,,,,{}",
            Utc::now().to_rfc3339(), id, asset, slug, left,
            dump_side, dump_px, dump_bid, bid_src, dump_rec,
            cl_open, cl_now, cl_d,
            flags);
        let _ = self.f.flush();
    }
    #[allow(clippy::too_many_arguments, unused_variables)]
    fn settle(&mut self, id: &str, asset: &str, slug: &str,
              cl_open: f64, cl_close: f64,
              settle_dir: &str, settle_src: &str,
              clob_up: f64, clob_dn: f64,
              entry_fees: f64, dump_fee: f64,
              dumped: bool, dump_px: f64,
              pnl: f64, cum_pnl: f64, cap: f64, w: u32, l: u32,
              entry_left: i64, entry_up_spread: f64, entry_dn_spread: f64,
              flags: &str) {
        let cl_d = if cl_open > 0.0 { (cl_close-cl_open)/cl_open*100.0 } else { 0.0 };
        let _ = writeln!(self.f, "{},SETTLE,{},{},{},0,\
            ,,{:.4},,\
            ,,{:.4},,\
            ,,,,\
            ,{:.4},,,\
            {:.2},{:.2},{:+.4},\
            {},{},{:.4},{:.4},\
            {:.4},{:.4},\
            {:+.4},{:+.4},{:.4},{},{},{}",
            Utc::now().to_rfc3339(), id, asset, slug,
            entry_up_spread,
            entry_dn_spread,
            if dumped {dump_px} else {0.0},
            cl_open, cl_close, cl_d,
            settle_dir, settle_src, clob_up, clob_dn,
            entry_fees, dump_fee,
            pnl, cum_pnl, cap, w, l, flags);
        let _ = self.f.flush();
    }
}

// ── State / Feeds ──────────────────────────────────────────────────
type SS = Arc<RwLock<State>>;

struct State {
    cl: HashMap<&'static str, f64>,
    snap: HashMap<&'static str, HashMap<i64, f64>>,
    bn: HashMap<&'static str, f64>,
    bnh: HashMap<&'static str, VecDeque<(f64,f64)>>,
}
impl State {
    fn new() -> Self { State { cl: HashMap::new(), snap: HashMap::new(), bn: HashMap::new(), bnh: HashMap::new() } }
    fn cl_up(&mut self, a: &'static str, px: f64, ts: f64) {
        self.cl.insert(a, px);
        let s = self.snap.entry(a).or_default();
        s.insert(ts as i64, px);
        let c = ts as i64 - 3600; s.retain(|k,_| *k > c);
    }
    fn bn_up(&mut self, a: &'static str, px: f64) {
        let ts = Utc::now().timestamp_millis() as f64 / 1000.0;
        self.bn.insert(a, px);
        let h = self.bnh.entry(a).or_default();
        h.push_back((ts, px)); if h.len() > 7200 { h.pop_front(); }
    }
    fn cl_at(&self, a: &str, t: i64, tol: i64) -> Option<f64> {
        let s = self.snap.get(a)?;
        for d in 0..=tol {
            if let Some(&p) = s.get(&(t+d)) { return Some(p); }
            if d > 0 { if let Some(&p) = s.get(&(t-d)) { return Some(p); } }
        }
        None
    }
}

fn cl_asset(s: &str) -> Option<&'static str> {
    match s { "btc/usd"|"btcusd"|"btc" => Some("btc"), "eth/usd"|"ethusd"|"eth" => Some("eth"),
              "sol/usd"|"solusd"|"sol" => Some("sol"), "xrp/usd"|"xrpusd"|"xrp" => Some("xrp"), _ => None }
}
fn bnsym(a: &str) -> &'static str {
    match a { "btc"=>"btcusdt","eth"=>"ethusdt","sol"=>"solusdt","xrp"=>"xrpusdt",_=>"btcusdt" }
}

async fn cl_feed(st: SS) { loop {
    if let Err(e) = cl_ws(&st).await { error!("[CL] {}", e); }
    tokio::time::sleep(Duration::from_secs(3)).await;
}}
async fn cl_ws(st: &SS) -> Result<()> {
    let (mut ws, _) = connect_async(RTDS_WS).await.context("CL")?;
    ws.send(Message::Text(json!({"action":"subscribe","subscriptions":[
        {"topic":"crypto_prices_chainlink","type":"*","filters":""}]}).to_string())).await?;
    while let Some(msg) = ws.next().await { match msg {
        Ok(Message::Text(t)) => {
            let d: Value = match serde_json::from_str(&t) { Ok(d)=>d, _=>continue };
            if d.get("topic").and_then(|t|t.as_str()) != Some("crypto_prices_chainlink") { continue; }
            let p = match d.get("payload") { Some(p)=>p, _=>continue };
            let sym = p.get("symbol").and_then(|s|s.as_str()).unwrap_or("").to_lowercase();
            let val = p.get("value").and_then(|v|v.as_f64().or(v.as_str().and_then(|s|s.parse().ok())));
            let rts = p.get("timestamp").and_then(|t|t.as_f64().or(t.as_i64().map(|i|i as f64))).unwrap_or(0.0);
            let ts = if rts>1e12 {rts/1000.0} else if rts>1e9 {rts} else {Utc::now().timestamp() as f64};
            if let (Some(a),Some(px)) = (cl_asset(&sym),val) { if px>0.0 { st.write().await.cl_up(a,px,ts); } }
        }
        Ok(Message::Ping(d)) => { let _ = ws.send(Message::Pong(d)).await; }
        Ok(_) => {} Err(e) => { error!("[CL] {}", e); break; }
    }}
    Ok(())
}
async fn bn_feed(st: SS) { loop {
    if let Err(e) = bn_ws(&st).await { error!("[BN] {}", e); }
    tokio::time::sleep(Duration::from_secs(3)).await;
}}
async fn bn_ws(st: &SS) -> Result<()> {
    let streams: Vec<String> = ASSETS.iter().map(|a| format!("{}@aggTrade", bnsym(a))).collect();
    let (mut ws, _) = connect_async(format!("{}/{}", BN_WS, streams.join("/"))).await.context("BN")?;
    while let Some(msg) = ws.next().await { match msg {
        Ok(Message::Text(t)) => {
            let d: Value = match serde_json::from_str(&t) { Ok(d)=>d, _=>continue };
            let i = d.get("data").unwrap_or(&d);
            let sym = i.get("s").and_then(|s|s.as_str()).unwrap_or("").to_lowercase();
            let px = i.get("p").and_then(|p|p.as_str().and_then(|s|s.parse::<f64>().ok()));
            let a: Option<&'static str> = match sym.as_str() {
                "btcusdt"=>Some("btc"),"ethusdt"=>Some("eth"),"solusdt"=>Some("sol"),"xrpusdt"=>Some("xrp"),_=>None };
            if let (Some(a),Some(p)) = (a,px) { if p>0.0 { st.write().await.bn_up(a,p); } }
        }
        Ok(Message::Ping(d)) => { let _ = ws.send(Message::Pong(d)).await; }
        Ok(_) => {} Err(e) => { error!("[BN] {}", e); break; }
    }}
    Ok(())
}

// ── Market scanning / Orderbook ────────────────────────────────────
#[derive(Clone, Debug)]
struct Win { slug: String, asset: &'static str, wmin: u32, tid_up: String, tid_down: String, start_ts: i64, end_ts: i64 }
impl Win { fn left(&self) -> i64 { self.end_ts - Utc::now().timestamp() } }

struct Scan { http: reqwest::Client, cache: Vec<Win>, last: Instant }
impl Scan {
    fn new() -> Self { Scan { http: reqwest::Client::builder().user_agent("hydra/1").timeout(Duration::from_secs(5)).build().expect("h"), cache: Vec::new(), last: Instant::now()-Duration::from_secs(999) } }
    async fn get(&mut self) -> Vec<Win> {
        if self.last.elapsed() < Duration::from_secs(10) { return self.cache.iter().filter(|w|w.left()>0).cloned().collect(); }
        let now = Utc::now().timestamp();
        let mut ws = Vec::new();
        for &a in ASSETS {
            let wm = 5u32;
            let iv = wm as i64 * 60;
            let s0 = (now/iv)*iv;
            for st in [s0, s0+iv] {
                let et = st+iv; if et<now { continue; }
                let slug = format!("{}-updown-{}m-{}", a, wm, st);
                let r = match self.http.get(format!("{}/markets", GAMMA)).query(&[("slug",&slug)]).send().await { Ok(r) if r.status().is_success()=>r, _=>continue };
                let d: Value = match r.json().await { Ok(d)=>d, _=>continue };
                let m = if d.is_array() { match d.as_array().and_then(|a|a.first()) { Some(m)=>m.clone(), None=>continue } } else { d };
                let tr = m.get("clobTokenIds").unwrap_or(&Value::Null);
                let tids: Vec<String> = if tr.is_string() { serde_json::from_str(tr.as_str().unwrap_or("[]")).unwrap_or_default() } else { serde_json::from_value(tr.clone()).unwrap_or_default() };
                if tids.len()<2 { continue; }
                let or = m.get("outcomes").unwrap_or(&Value::Null);
                let outs: Vec<String> = if or.is_string() { serde_json::from_str(or.as_str().unwrap_or("[]")).unwrap_or_default() } else { serde_json::from_value(or.clone()).unwrap_or_default() };
                let (tu,td) = if outs.len()>=2 && outs[0]=="Down" { (tids[1].clone(),tids[0].clone()) } else { (tids[0].clone(),tids[1].clone()) };
                ws.push(Win { slug, asset: a, wmin: wm, tid_up: tu, tid_down: td, start_ts: st, end_ts: et });
            }
        }
        self.cache = ws.clone(); self.last = Instant::now();
        ws.into_iter().filter(|w|w.left()>0).collect()
    }
}

#[derive(Clone,Default,Debug)]
struct Bk { bb: f64, ba: f64, ha: bool, hb: bool }
impl Bk {
    fn spread(&self) -> f64 { if self.ha && self.hb && self.ba > 0.0 { self.ba - self.bb } else { 999.0 } }
    fn mid(&self) -> f64 { if self.ha && self.hb { (self.ba + self.bb) / 2.0 } else { 0.0 } }
}
struct BkC { http: reqwest::Client, c: HashMap<String,Bk>, t: HashMap<String,Instant> }
impl BkC {
    fn new() -> Self { BkC { http: reqwest::Client::builder().user_agent("hydra/1").timeout(Duration::from_secs(2)).build().expect("h"), c: HashMap::new(), t: HashMap::new() } }
    fn get(&self, tid: &str) -> Bk {
        match self.t.get(tid) {
            Some(ts) if ts.elapsed() < Duration::from_secs(STALE_SECS) =>
                self.c.get(tid).cloned().unwrap_or_default(),
            _ => Bk::default(),
        }
    }
    fn age_ms(&self, tid: &str) -> u64 {
        self.t.get(tid).map(|ts| ts.elapsed().as_millis() as u64).unwrap_or(99999)
    }
    async fn refresh(&mut self, tids: &[String]) {
        let stale: Vec<&String> = tids.iter().filter(|t| self.t.get(*t).map(|ts|ts.elapsed()>=Duration::from_secs(1)).unwrap_or(true)).collect();
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

// ── Position / Strategy ────────────────────────────────────────────
#[derive(Clone)]
#[allow(dead_code)]
struct PT {
    id: &'static str, slug: String, asset: &'static str, wmin: u32,
    dir: String, px: f64, shares: f64,
    dir2: String, px2: f64, sh2: f64,
    end_ts: i64,
    dumped: bool, dump_px: f64,
    tid_up: String, tid_dn: String,
    entry_up_bid: f64, entry_dn_bid: f64,
    entry_up_spread: f64, entry_dn_spread: f64,
    entry_left: i64,
}

struct Strat { id: &'static str, cap: f64, w: u32, l: u32, pnl: f64, active: HashMap<String,PT>, done: HashSet<String> }
impl Strat {
    fn new(id: &'static str) -> Self { Strat { id, cap: START_CAP, w:0, l:0, pnl:0.0, active: HashMap::new(), done: HashSet::new() } }
    fn rec(&mut self, won: bool, p: f64) { if won {self.w+=1} else {self.l+=1}; self.pnl+=p; self.cap+=p; }
    fn t(&self) -> u32 { self.w+self.l }
}

// ── Hydra engine ───────────────────────────────────────────────────
struct Hydra { st: SS, scan: Scan, bk: BkC, s: HashMap<&'static str, Strat>,
    cl_o: HashMap<String,f64>, start: Instant, log: TradeLog }

impl Hydra {
    fn new(st: SS, scan: Scan, bk: BkC) -> Self {
        let mut s = HashMap::new();
        for id in ["S3b","S3C","S3D"] { s.insert(id, Strat::new(id)); }
        Hydra { st, scan, bk, s, cl_o: HashMap::new(), start: Instant::now(), log: TradeLog::new() }
    }

    async fn tick(&mut self) {
        let wins = self.scan.get().await;
        let mut tids: Vec<String> = Vec::new();
        for w in &wins { if w.left()>0 && w.left()<300 { tids.push(w.tid_up.clone()); tids.push(w.tid_down.clone()); } }
        for st in self.s.values() {
            for t in st.active.values() {
                tids.push(t.tid_up.clone());
                tids.push(t.tid_dn.clone());
            }
        }
        tids.sort(); tids.dedup();
        if !tids.is_empty() { self.bk.refresh(&tids).await; }

        { let s = self.st.read().await;
          for w in &wins { if !self.cl_o.contains_key(&w.slug) {
              if let Some(p) = s.cl_at(w.asset,w.start_ts,1).or_else(|| {
                  let now = Utc::now().timestamp();
                  if (now-w.start_ts).abs()<=2 { s.cl.get(w.asset).copied() } else { None }
              }) { if p>0.0 { self.cl_o.insert(w.slug.clone(),p); } }
          }}
        }

        self.eval_s3(&wins).await;
        self.manage().await;

        let now = Utc::now().timestamp();
        let c = now-3600;
        self.cl_o.retain(|k,_| k.rsplit('-').next().and_then(|s|s.parse::<i64>().ok()).map(|t|t>c).unwrap_or(false));
        for st in self.s.values_mut() { st.done.retain(|k| k.rsplit('-').next().and_then(|s|s.parse::<i64>().ok()).map(|t|t>c).unwrap_or(false)); }
    }

    async fn eval_s3(&mut self, wins: &[Win]) {
        for w in wins {
            if w.wmin!=5 { continue; }
            let left = w.left(); if left>290||left<60 { continue; }
            let bu = self.bk.get(&w.tid_up); let bd = self.bk.get(&w.tid_down);
            if !bu.ha||!bd.ha { continue; }
            if bu.ba<0.47||bu.ba>0.53||bd.ba<0.47||bd.ba>0.53 { continue; }

            // Build flags for CSV (all realism warnings go here, not terminal)
            let mut flags = Vec::new();
            let up_spread = bu.spread();
            let dn_spread = bd.spread();
            let up_mid = bu.mid();
            let dn_mid = bd.mid();
            if up_mid > 0.0 && up_spread / up_mid > SPREAD_WARN { flags.push("WIDE_SPREAD_UP"); }
            if dn_mid > 0.0 && dn_spread / dn_mid > SPREAD_WARN { flags.push("WIDE_SPREAD_DN"); }
            if !bu.hb || bu.bb <= 0.0 { flags.push("THIN_BOOK_UP"); }
            if !bd.hb || bd.bb <= 0.0 { flags.push("THIN_BOOK_DN"); }

            let up_age = self.bk.age_ms(&w.tid_up);
            let dn_age = self.bk.age_ms(&w.tid_down);
            let flag_str = flags.join("|");

            for id in ["S3b","S3C","S3D"] {
                let st = self.s.get_mut(id).expect("s");
                if st.done.contains(&w.slug)||st.active.contains_key(&w.slug)||st.cap<STAKE_2 { continue; }
                let up = bu.ba + LATENCY_TICKS; let dn = bd.ba + LATENCY_TICKS;
                let ush = STAKE_1/up; let dsh = STAKE_1/dn;
                st.active.insert(w.slug.clone(), PT { id, slug:w.slug.clone(), asset:w.asset, wmin:w.wmin,
                    dir:"UP".into(), px:up, shares:ush, dir2:"DOWN".into(), px2:dn, sh2:dsh,
                    end_ts:w.end_ts, dumped:false, dump_px:0.0,
                    tid_up:w.tid_up.clone(), tid_dn:w.tid_down.clone(),
                    entry_up_bid: if bu.hb {bu.bb} else {0.0},
                    entry_dn_bid: if bd.hb {bd.bb} else {0.0},
                    entry_up_spread: up_spread, entry_dn_spread: dn_spread,
                    entry_left: left });
                st.done.insert(w.slug.clone());

                // CSV: full details
                self.log.entry(id, w.asset, &w.slug, left, &bu, &bd, up_age, dn_age, up, dn, &flag_str);

                // Terminal: clean one-liner
                info!("[{}] ENTRY {} T-{}s  UP@{:.3} DN@{:.3}", id, w.asset.to_uppercase(), left, bu.ba, bd.ba);
            }
        }
    }

    async fn manage(&mut self) {
        let now = Utc::now().timestamp();
        let s = self.st.read().await;
        let mut settles: Vec<(&'static str, String, f64)> = Vec::new();

        for st in self.s.values_mut() {
            let slugs: Vec<String> = st.active.keys().cloned().collect();
            for slug in slugs {
                let t = match st.active.get(&slug) { Some(t)=>t.clone(), None=>continue };
                let left = t.end_ts - now;

                let co = self.cl_o.get(&slug).copied().unwrap_or(0.0);
                let cn = s.cl.get(t.asset).copied().unwrap_or(0.0);
                if co<=0.0||cn<=0.0 { continue; }
                let cl_d = ((cn-co)/co*100.0).abs();
                let up_winning = cn >= co;

                let loser_tid = if up_winning {&t.tid_dn} else {&t.tid_up};
                let loser_bk = self.bk.get(loser_tid);
                let loser_side = if up_winning {"DN"} else {"UP"};
                let (loser_bid, bid_source) = if loser_bk.hb && loser_bk.bb > 0.0 {
                    (loser_bk.bb, "REAL")
                } else {
                    let est = if cl_d>0.3 {0.05} else if cl_d>0.15 {0.10} else if cl_d>0.05 {0.20} else {0.35};
                    (est, "EST")
                };

                // S3b: MAKER dump at T-30
                if st.id=="S3b" && !t.dumped && left<=30 {
                    let dp = (loser_bid + 0.01).min(0.50).max(0.01);
                    if let Some(tm) = st.active.get_mut(&slug) { tm.dumped=true; tm.dump_px=dp; }
                    let ls = if up_winning {t.sh2} else {t.shares};
                    let mut flags = vec!["MAKER_FILL_RISK"];
                    if bid_source == "EST" { flags.push("EST_BID"); }
                    self.log.dump("S3b", t.asset, &slug, left, loser_side, dp, loser_bid, bid_source, ls*dp, co, cn, &flags.join("|"));
                    info!("[S3b] DUMP {} {} @{:.3}  T-{}s", t.asset.to_uppercase(), loser_side, dp, left);
                }
                // S3C: TAKER dump at T-30
                if st.id=="S3C" && !t.dumped && left<=30 {
                    let dp = (loser_bid - LATENCY_TICKS).max(0.01);
                    if let Some(tm) = st.active.get_mut(&slug) { tm.dumped=true; tm.dump_px=dp; }
                    let ls = if up_winning {t.sh2} else {t.shares};
                    let flags = if bid_source == "EST" { "EST_BID" } else { "" };
                    self.log.dump("S3C", t.asset, &slug, left, loser_side, dp, loser_bid, bid_source, ls*dp, co, cn, flags);
                    info!("[S3C] DUMP {} {} @{:.3}  T-{}s", t.asset.to_uppercase(), loser_side, dp, left);
                }
                // S3D: taker dump when loser bid ≤ 0.10
                if st.id=="S3D" && !t.dumped && loser_bid<=0.10 {
                    let dp = (loser_bid - LATENCY_TICKS).max(0.01);
                    if let Some(tm) = st.active.get_mut(&slug) { tm.dumped=true; tm.dump_px=dp; }
                    let ls = if up_winning {t.sh2} else {t.shares};
                    let flags = if bid_source == "EST" { "EST_BID" } else { "" };
                    self.log.dump("S3D", t.asset, &slug, left, loser_side, dp, loser_bid, bid_source, ls*dp, co, cn, flags);
                    info!("[S3D] DUMP {} {} @{:.3}  T-{}s", t.asset.to_uppercase(), loser_side, dp, left);
                }

                // Settlement
                if now >= t.end_ts {
                    let cc = s.cl_at(t.asset,t.end_ts,0)
                        .or_else(|| s.cl.get(t.asset).copied())
                        .unwrap_or(0.0);
                    let up_bk = self.bk.get(&t.tid_up);
                    let dn_bk = self.bk.get(&t.tid_dn);

                    let mut flags = Vec::new();

                    let (actual, settle_source) = if co > 0.0 && cc > 0.0 {
                        let cl_dir = if cc >= co {"UP"} else {"DOWN"};
                        if up_bk.hb && dn_bk.hb {
                            let clob_dir = if up_bk.bb > dn_bk.bb {"UP"} else {"DOWN"};
                            if clob_dir != cl_dir { flags.push("CL_CLOB_SPLIT"); }
                        }
                        (cl_dir, "CL")
                    } else if up_bk.hb && dn_bk.hb && (up_bk.bb > 0.80 || dn_bk.bb > 0.80) {
                        let dir = if up_bk.bb > dn_bk.bb {"UP"} else {"DOWN"};
                        flags.push("CL_MISSING");
                        (dir, "CLOB_FALLBACK")
                    } else {
                        flags.push("NO_DATA");
                        self.log.settle(st.id, t.asset, &slug, co, 0.0, "?", "NONE",
                            0.0, 0.0, 0.0, 0.0, false, 0.0,
                            -STAKE_2, st.pnl-STAKE_2, st.cap-STAKE_2, st.w, st.l+1,
                            t.entry_left, t.entry_up_spread, t.entry_dn_spread, "NO_DATA");
                        settles.push((st.id,slug.clone(),-STAKE_2));
                        info!("[{}] LOSS {} -${:.2} (no data)", st.id, t.asset.to_uppercase(), STAKE_2);
                        continue;
                    };

                    let entry_fees = fee(t.px)*t.shares + fee(t.px2)*t.sh2;
                    let (pnl, dump_fee) = if t.dumped {
                        let lr = if up_winning {t.sh2} else {t.shares};
                        let wr = if up_winning {t.shares} else {t.sh2};
                        let loser_was_right = (!up_winning && actual=="UP")||(up_winning && actual=="DOWN");
                        let df = fee(t.dump_px)*lr;
                        if loser_was_right {
                            flags.push("REVERSAL");
                            (lr*t.dump_px - STAKE_2 - entry_fees - df, df)
                        } else {
                            (lr*t.dump_px + wr*1.0 - STAKE_2 - entry_fees - df, df)
                        }
                    } else {
                        flags.push("NO_DUMP");
                        let (up_pay, dn_pay) = if actual=="UP" {(1.0,0.0)} else {(0.0,1.0)};
                        (t.shares*up_pay + t.sh2*dn_pay - STAKE_2 - entry_fees, 0.0)
                    };

                    let tag = if pnl>0.0 {"WIN"} else {"LOSS"};
                    let flag_str = flags.join("|");

                    // CSV: full audit
                    self.log.settle(st.id, t.asset, &slug, co, cc,
                        actual, settle_source,
                        if up_bk.hb {up_bk.bb} else {0.0}, if dn_bk.hb {dn_bk.bb} else {0.0},
                        entry_fees, dump_fee,
                        t.dumped, t.dump_px,
                        pnl, st.pnl+pnl, st.cap+pnl, st.w + if pnl>0.0{1}else{0}, st.l + if pnl<=0.0{1}else{0},
                        t.entry_left, t.entry_up_spread, t.entry_dn_spread,
                        &flag_str);

                    // Terminal: clean one-liner
                    info!("[{}] {} {} ${:+.2}  cum=${:+.2}  {}W/{}L",
                        st.id, tag, t.asset.to_uppercase(), pnl, st.pnl+pnl,
                        st.w + if pnl>0.0{1}else{0}, st.l + if pnl<=0.0{1}else{0});
                    settles.push((st.id, slug.clone(), pnl));
                }
            }
        }
        for (id, slug, pnl) in settles {
            if let Some(st) = self.s.get_mut(id) { st.rec(pnl>0.0, pnl); st.active.remove(&slug); }
        }
    }

    fn status(&self) -> String {
        let mut p = Vec::new();
        for id in ["S3b","S3C","S3D"] {
            if let Some(st) = self.s.get(id) {
                if st.t()>0||!st.active.is_empty() {
                    p.push(format!("{}:{}W/{}L${:+.1}", id, st.w, st.l, st.pnl));
                }
            }
        }
        if p.is_empty() { "waiting".into() } else { p.join(" | ") }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("hydra=info").with_target(false).init();
    dotenvy::dotenv().ok();
    info!("HYDRA — S3b/S3C/S3D | ${}/{} stake | log → {}", STAKE_1, STAKE_2, LOG_FILE);

    let st: SS = Arc::new(RwLock::new(State::new()));
    let c = st.clone(); tokio::spawn(async move { cl_feed(c).await; });
    let b = st.clone(); tokio::spawn(async move { bn_feed(b).await; });

    for _ in 0..20 { tokio::time::sleep(Duration::from_secs(1)).await;
        let s = st.read().await; if s.cl.contains_key("btc")&&s.bn.contains_key("btc") { break; } }
    { let s = st.read().await;
      let px: Vec<String> = ASSETS.iter().filter_map(|&a|{
          s.cl.get(a).map(|p| format!("{}=${:.0}", a.to_uppercase(), p))
      }).collect();
      info!("Feeds OK: {}", px.join(" "));
    }

    let mut hydra = Hydra::new(st.clone(), Scan::new(), BkC::new());
    let mut ls = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        hydra.tick().await;
        if ls.elapsed().as_secs() >= 60 {
            let s = st.read().await;
            let px: Vec<String> = ASSETS.iter().filter_map(|&a|{
                s.cl.get(a).map(|p| format!("{}=${:.0}", a.to_uppercase(), p))
            }).collect();
            info!("{} | {:.1}h | {}", px.join(" "), hydra.start.elapsed().as_secs_f64()/3600.0, hydra.status());
            ls = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_close(a: f64, b: f64, label: &str) {
        assert!((a - b).abs() < 0.0001, "{}: expected {:.4}, got {:.4}", label, b, a);
    }

    #[test]
    fn test_fee_function() {
        assert_close(fee(0.50), 0.50 * 0.50 * 0.0625, "fee at 0.50");
        assert_close(fee(0.90), 0.90 * 0.10 * 0.0625, "fee at 0.90");
        assert_close(fee(0.0), 0.0, "fee at 0.0");
        assert_close(fee(1.0), 0.0, "fee at 1.0");
    }

    #[test]
    fn test_spread_detection() {
        let bk = Bk { bb: 0.45, ba: 0.55, ha: true, hb: true };
        assert_close(bk.spread(), 0.10, "spread");
        assert_close(bk.mid(), 0.50, "mid");
        assert!(bk.spread() / bk.mid() > SPREAD_WARN, "10%/50% spread must trigger warning");

        let thin = Bk { bb: 0.0, ba: 0.50, ha: true, hb: false };
        assert!(thin.spread() > 100.0, "no bid = infinite spread");
    }

    #[test]
    fn test_both_sides_dumped_normal() {
        let mut st = Strat::new("S3b");
        let cap0 = st.cap;
        let up_px = 0.50;
        let dn_px = 0.50;
        let ush = STAKE_1 / up_px;
        let dsh = STAKE_1 / dn_px;
        let entry_fees = fee(up_px) * ush + fee(dn_px) * dsh;
        let dump_px = 0.06;
        let lr = dsh; let wr = ush;
        let pnl = lr * dump_px + wr * 1.0 - STAKE_2 - entry_fees - fee(dump_px) * lr;
        st.rec(pnl > 0.0, pnl);
        assert_close(st.cap, cap0 + pnl, "S3b maker dumped cap");
        assert!(pnl > 0.0, "S3b normal must be profitable");
    }

    #[test]
    fn test_both_sides_taker_dump_normal() {
        let mut st = Strat::new("S3C");
        let cap0 = st.cap;
        let px = 0.50;
        let sh = STAKE_1 / px;
        let entry_fees = fee(px) * sh * 2.0;
        let dump_px = 0.05;
        let pnl = sh * dump_px + sh * 1.0 - STAKE_2 - entry_fees - fee(dump_px) * sh;
        st.rec(pnl > 0.0, pnl);
        assert_close(st.cap, cap0 + pnl, "S3C taker dumped cap");
        let s3b_pnl = sh * 0.06 + sh * 1.0 - STAKE_2 - entry_fees - fee(0.06) * sh;
        assert!(pnl < s3b_pnl, "S3C taker must get worse PnL than S3b maker");
    }

    #[test]
    fn test_both_sides_dumped_reversal() {
        let mut st = Strat::new("S3b");
        let cap0 = st.cap;
        let px = 0.50;
        let sh = STAKE_1 / px;
        let entry_fees = fee(px) * sh * 2.0;
        let dump_px = 0.06;
        let pnl = sh * dump_px - STAKE_2 - entry_fees - fee(dump_px) * sh;
        st.rec(false, pnl);
        assert_close(st.cap, cap0 + pnl, "S3b reversal cap");
        assert!(pnl < -STAKE_2 * 0.5, "reversal must be a big loss");
    }

    #[test]
    fn test_no_dump_settle() {
        let mut st = Strat::new("S3D");
        let cap0 = st.cap;
        let px = 0.50;
        let sh = STAKE_1 / px;
        let entry_fees = fee(px) * sh * 2.0;
        let pnl = sh * 1.0 + sh * 0.0 - STAKE_2 - entry_fees;
        st.rec(pnl > 0.0, pnl);
        assert_close(st.cap, cap0 + pnl, "S3D no-dump settle cap");
        assert!(pnl < sh * 0.06 + sh * 1.0 - STAKE_2 - entry_fees - fee(0.06)*sh,
            "no-dump loses loser recovery compared to dumped");
    }

    #[test]
    fn test_csv_header() {
        let path = "/tmp/hydra_test_log.csv";
        let _ = std::fs::remove_file(path);
        {
            let f = OpenOptions::new().create(true).append(true).open(path).unwrap();
            let mut tl = TradeLog { f };
            tl.hdr();
        }
        let content = std::fs::read_to_string(path).unwrap();
        assert!(content.starts_with("ts,event,strategy,"), "CSV header must start with ts,event,strategy");
        assert!(content.contains("flags"), "CSV header must contain flags column");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_full_capital_trace() {
        println!("\n  HYDRA — 3 strategies, both-sides, all PnL paths");
        let bpx = 0.50;
        let bsh = STAKE_1 / bpx;
        let bef = fee(bpx) * bsh * 2.0;

        for &(id, dp) in &[("S3b", 0.06_f64), ("S3C", 0.05), ("S3D", 0.10)] {
            let dfee = fee(dp) * bsh;
            let mut st = Strat::new(id);
            let p1 = bsh * dp + bsh * 1.0 - STAKE_2 - bef - dfee;
            st.rec(p1 > 0.0, p1);
            let p2 = bsh * dp - STAKE_2 - bef - dfee; // reversal
            st.rec(false, p2);
            let p3 = bsh * 1.0 - STAKE_2 - bef; // no dump
            st.rec(p3 > 0.0, p3);
            assert_close(st.cap, START_CAP + p1 + p2 + p3, &format!("{} 3-trade", id));
            println!("  {} | {}W/{}L | PnL=${:+.4} | Cap=${:.2}", id, st.w, st.l, st.pnl, st.cap);
        }
    }
}
