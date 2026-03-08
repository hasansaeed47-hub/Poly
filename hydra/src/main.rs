//! Hydra — 3-strategy both-sides paper tracker for Polymarket 5m markets
//! S3b: Maker dump T-30 | S3C: Taker dump T-30 | S3D: Taker dump bid≤$0.10
//!
//! Terminal: clean one-liners only
//! File log: hydra_trades.csv — full audit trail for post-analysis

use std::collections::{HashMap, HashSet};
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
struct TradeLog { f: std::fs::File, run_id: String }
impl TradeLog {
    fn new() -> Self {
        let run_id = format!("R{}", Utc::now().format("%m%d_%H%M%S"));
        let exists = std::path::Path::new(LOG_FILE).exists();
        let f = OpenOptions::new().create(true).append(true).open(LOG_FILE).expect("open log");
        let mut tl = TradeLog { f, run_id };
        if !exists {
            tl.hdr();
        }
        tl
    }
    fn hdr(&mut self) {
        let _ = writeln!(self.f, "ts,run_id,event,strategy,asset,slug,left_s,\
            bn_px,\
            up_ask,up_bid,up_spread,up_bid_sz,up_ask_sz,up_n_bids,up_n_asks,up_age_ms,\
            dn_ask,dn_bid,dn_spread,dn_bid_sz,dn_ask_sz,dn_n_bids,dn_n_asks,dn_age_ms,\
            fill_up,fill_dn,cost_up,cost_dn,\
            dump_side,dump_px,dump_bid,dump_bid_src,dump_rec,\
            cl_open,cl_close,cl_delta_pct,\
            settle_dir,settle_src,clob_up_bid,clob_dn_bid,\
            entry_fees,dump_fee,\
            pnl,cum_pnl,cap,w,l,flags");
    }
    fn entry(&mut self, id: &str, asset: &str, slug: &str, left: i64,
             bu: &Bk, bd: &Bk, up_age: u64, dn_age: u64,
             fill_up: f64, fill_dn: f64, bn_px: f64, flags: &str) {
        let _ = writeln!(self.f, "{},{},ENTRY,{},{},{},{},\
            {:.2},\
            {:.4},{:.4},{:.4},{:.1},{:.1},{},{},{},\
            {:.4},{:.4},{:.4},{:.1},{:.1},{},{},{},\
            {:.4},{:.4},{:.4},{:.4},\
            ,,,,\
            ,,,,,,\
            ,,\
            ,,,,,,{}",
            Utc::now().to_rfc3339(), self.run_id, id, asset, slug, left,
            bn_px,
            bu.ba, if bu.hb {bu.bb} else {0.0}, bu.spread(), bu.bid_sz, bu.ask_sz, bu.n_bids, bu.n_asks, up_age,
            bd.ba, if bd.hb {bd.bb} else {0.0}, bd.spread(), bd.bid_sz, bd.ask_sz, bd.n_bids, bd.n_asks, dn_age,
            fill_up, fill_dn, fill_up*(STAKE_1/fill_up), fill_dn*(STAKE_1/fill_dn),
            flags);
        let _ = self.f.flush();
    }
    fn dump(&mut self, id: &str, asset: &str, slug: &str, left: i64,
            dump_side: &str, dump_px: f64, dump_bid: f64, bid_src: &str,
            dump_rec: f64, cl_open: f64, cl_now: f64, bn_px: f64,
            loser_bk: &Bk, flags: &str) {
        let cl_d = if cl_open > 0.0 { (cl_now-cl_open)/cl_open*100.0 } else { 0.0 };
        let _ = writeln!(self.f, "{},{},DUMP,{},{},{},{},\
            {:.2},\
            ,,,,,,,,\
            ,,,,{:.1},,{},{},\
            ,,,,\
            {},{:.4},{:.4},{},{:.4},\
            {:.2},{:.2},{:+.4},\
            ,,,,\
            ,,\
            ,,,,,,{}",
            Utc::now().to_rfc3339(), self.run_id, id, asset, slug, left,
            bn_px,
            loser_bk.bid_sz, loser_bk.n_bids, loser_bk.n_asks,
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
              bn_px: f64, flags: &str) {
        let cl_d = if cl_open > 0.0 { (cl_close-cl_open)/cl_open*100.0 } else { 0.0 };
        let _ = writeln!(self.f, "{},{},SETTLE,{},{},{},0,\
            {:.2},\
            ,,{:.4},,,,,,\
            ,,{:.4},,,,,,\
            ,,,,\
            ,{:.4},,,\
            {:.2},{:.2},{:+.4},\
            {},{},{:.4},{:.4},\
            {:.4},{:.4},\
            {:+.4},{:+.4},{:.4},{},{},{}",
            Utc::now().to_rfc3339(), self.run_id, id, asset, slug,
            bn_px,
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
}
impl State {
    fn new() -> Self { State { cl: HashMap::new(), snap: HashMap::new(), bn: HashMap::new() } }
    fn cl_up(&mut self, a: &'static str, px: f64, ts: f64) {
        self.cl.insert(a, px);
        let s = self.snap.entry(a).or_default();
        s.insert(ts as i64, px);
        let c = ts as i64 - 3600; s.retain(|k,_| *k > c);
    }
    fn bn_up(&mut self, a: &'static str, px: f64) {
        self.bn.insert(a, px);
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
struct Bk { bb: f64, ba: f64, ha: bool, hb: bool,
    bid_sz: f64, ask_sz: f64, n_bids: u32, n_asks: u32 }
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
                bk.n_bids = bids.len() as u32;
                let mut p: Vec<(f64,f64)> = bids.iter().filter_map(|b|{
                    let px = b.get("price").and_then(|p|p.as_str().and_then(|s|s.parse().ok()))?;
                    let sz = b.get("size").and_then(|s|s.as_str().and_then(|s|s.parse().ok())).unwrap_or(0.0);
                    Some((px,sz))
                }).collect();
                p.sort_by(|a,b|b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
                if let Some(&(v,sz)) = p.first() { bk.bb=v; bk.hb=true; bk.bid_sz=sz; }
            }
            if let Some(asks) = item.get("asks").and_then(|a|a.as_array()) {
                bk.n_asks = asks.len() as u32;
                let mut p: Vec<(f64,f64)> = asks.iter().filter_map(|a|{
                    let px = a.get("price").and_then(|p|p.as_str().and_then(|s|s.parse().ok()))?;
                    let sz = a.get("size").and_then(|s|s.as_str().and_then(|s|s.parse().ok())).unwrap_or(0.0);
                    Some((px,sz))
                }).collect();
                p.sort_by(|a,b|a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                if let Some(&(v,sz)) = p.first() { bk.ba=v; bk.ha=true; bk.ask_sz=sz; }
            }
            self.c.insert(tid.clone(), bk); self.t.insert(tid, now);
        }
    }
}

// ── Position / Strategy ────────────────────────────────────────────
#[derive(Clone)]
struct PT {
    asset: &'static str,
    px: f64, shares: f64,
    px2: f64, sh2: f64,
    end_ts: i64,
    dumped: bool, dump_px: f64,
    tid_up: String, tid_dn: String,
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
        let bn_snap: HashMap<&str, f64> = {
            let s = self.st.read().await;
            ASSETS.iter().filter_map(|&a| s.bn.get(a).map(|&p| (a, p))).collect()
        };
        for w in wins {
            if w.wmin!=5 { continue; }
            let left = w.left(); if left>290||left<60 { continue; }
            let bu = self.bk.get(&w.tid_up); let bd = self.bk.get(&w.tid_down);
            if !bu.ha||!bd.ha { continue; }
            if bu.ba<0.47||bu.ba>0.53||bd.ba<0.47||bd.ba>0.53 { continue; }

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
            let bn_px = bn_snap.get(w.asset).copied().unwrap_or(0.0);
            let flag_str = flags.join("|");

            for id in ["S3b","S3C","S3D"] {
                let st = self.s.get_mut(id).expect("s");
                if st.done.contains(&w.slug)||st.active.contains_key(&w.slug)||st.cap<STAKE_2 { continue; }
                let up = bu.ba + LATENCY_TICKS; let dn = bd.ba + LATENCY_TICKS;
                let ush = STAKE_1/up; let dsh = STAKE_1/dn;
                st.active.insert(w.slug.clone(), PT { asset:w.asset,
                    px:up, shares:ush, px2:dn, sh2:dsh,
                    end_ts:w.end_ts, dumped:false, dump_px:0.0,
                    tid_up:w.tid_up.clone(), tid_dn:w.tid_down.clone(),
                    entry_up_spread: up_spread, entry_dn_spread: dn_spread,
                    entry_left: left });
                st.done.insert(w.slug.clone());

                self.log.entry(id, w.asset, &w.slug, left, &bu, &bd, up_age, dn_age, up, dn, bn_px, &flag_str);
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
                let bn_px = s.bn.get(t.asset).copied().unwrap_or(0.0);

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
                    self.log.dump("S3b", t.asset, &slug, left, loser_side, dp, loser_bid, bid_source, ls*dp, co, cn, bn_px, &loser_bk, &flags.join("|"));
                    info!("[S3b] DUMP {} {} @{:.3}  T-{}s", t.asset.to_uppercase(), loser_side, dp, left);
                }
                // S3C: TAKER dump at T-30
                if st.id=="S3C" && !t.dumped && left<=30 {
                    let dp = (loser_bid - LATENCY_TICKS).max(0.01);
                    if let Some(tm) = st.active.get_mut(&slug) { tm.dumped=true; tm.dump_px=dp; }
                    let ls = if up_winning {t.sh2} else {t.shares};
                    let flags = if bid_source == "EST" { "EST_BID" } else { "" };
                    self.log.dump("S3C", t.asset, &slug, left, loser_side, dp, loser_bid, bid_source, ls*dp, co, cn, bn_px, &loser_bk, flags);
                    info!("[S3C] DUMP {} {} @{:.3}  T-{}s", t.asset.to_uppercase(), loser_side, dp, left);
                }
                // S3D: taker dump when loser bid ≤ 0.10
                if st.id=="S3D" && !t.dumped && loser_bid<=0.10 {
                    let dp = (loser_bid - LATENCY_TICKS).max(0.01);
                    if let Some(tm) = st.active.get_mut(&slug) { tm.dumped=true; tm.dump_px=dp; }
                    let ls = if up_winning {t.sh2} else {t.shares};
                    let flags = if bid_source == "EST" { "EST_BID" } else { "" };
                    self.log.dump("S3D", t.asset, &slug, left, loser_side, dp, loser_bid, bid_source, ls*dp, co, cn, bn_px, &loser_bk, flags);
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
                            t.entry_left, t.entry_up_spread, t.entry_dn_spread, bn_px, "NO_DATA");
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

                    self.log.settle(st.id, t.asset, &slug, co, cc,
                        actual, settle_source,
                        if up_bk.hb {up_bk.bb} else {0.0}, if dn_bk.hb {dn_bk.bb} else {0.0},
                        entry_fees, dump_fee,
                        t.dumped, t.dump_px,
                        pnl, st.pnl+pnl, st.cap+pnl, st.w + if pnl>0.0{1}else{0}, st.l + if pnl<=0.0{1}else{0},
                        t.entry_left, t.entry_up_spread, t.entry_dn_spread,
                        bn_px, &flag_str);

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

    // ── Exact PnL formulas mirroring manage() ──
    // Normal: dumped loser + winner settles $1
    fn pnl_normal(ask: f64, dump_px: f64) -> f64 {
        let sh = STAKE_1 / (ask + LATENCY_TICKS);
        let ef = fee(ask + LATENCY_TICKS) * sh * 2.0;
        let df = fee(dump_px) * sh;
        sh * dump_px + sh * 1.0 - STAKE_2 - ef - df
    }
    // Reversal: dumped side was actually the winner
    fn pnl_reversal(ask: f64, dump_px: f64) -> f64 {
        let sh = STAKE_1 / (ask + LATENCY_TICKS);
        let ef = fee(ask + LATENCY_TICKS) * sh * 2.0;
        let df = fee(dump_px) * sh;
        sh * dump_px - STAKE_2 - ef - df
    }
    // No dump: winner settles $1, loser settles $0
    fn pnl_nodump(ask: f64) -> f64 {
        let sh = STAKE_1 / (ask + LATENCY_TICKS);
        let ef = fee(ask + LATENCY_TICKS) * sh * 2.0;
        sh * 1.0 - STAKE_2 - ef
    }
    // S3b dump price: maker posts at loser_bid + 0.01
    fn s3b_dump(loser_bid: f64) -> f64 { (loser_bid + 0.01).min(0.50).max(0.01) }
    // S3C/S3D dump price: taker hits bid - latency
    fn taker_dump(loser_bid: f64) -> f64 { (loser_bid - LATENCY_TICKS).max(0.01) }

    #[test]
    fn test_fee_function() {
        assert_close(fee(0.50), 0.015625, "fee at 0.50");
        assert_close(fee(0.90), 0.005625, "fee at 0.90");
        assert_close(fee(0.0), 0.0, "fee at 0.0");
        assert_close(fee(1.0), 0.0, "fee at 1.0");
    }

    #[test]
    fn test_spread_and_depth() {
        let bk = Bk { bb: 0.45, ba: 0.55, ha: true, hb: true,
            bid_sz: 100.0, ask_sz: 50.0, n_bids: 5, n_asks: 3 };
        assert_close(bk.spread(), 0.10, "spread");
        assert_close(bk.mid(), 0.50, "mid");
        assert!(bk.spread() / bk.mid() > SPREAD_WARN, "10%/50% triggers WIDE_SPREAD");
        assert_eq!(bk.n_bids, 5);
        assert_close(bk.bid_sz, 100.0, "bid_sz");

        let thin = Bk { bb: 0.0, ba: 0.50, ha: true, hb: false, ..Default::default() };
        assert!(thin.spread() > 100.0, "no bid = infinite spread");
        assert_eq!(thin.n_bids, 0);
    }

    #[test]
    fn test_csv_header_column_count() {
        let path = "/tmp/hydra_test_hdr.csv";
        let _ = std::fs::remove_file(path);
        {
            let f = OpenOptions::new().create(true).append(true).open(path).unwrap();
            let mut tl = TradeLog { f, run_id: "TEST".into() };
            tl.hdr();
        }
        let hdr = std::fs::read_to_string(path).unwrap();
        let cols: Vec<&str> = hdr.trim().split(',').collect();
        assert_eq!(cols.len(), 48, "CSV must have 48 columns, got {}: {:?}", cols.len(), cols);
        assert_eq!(cols[0], "ts");
        assert_eq!(cols[1], "run_id");
        assert_eq!(cols[2], "event");
        assert_eq!(cols[7], "bn_px");
        assert_eq!(cols[cols.len()-1], "flags");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_full_chain_simulation() {
        // Simulate 10 trades per strategy through exact engine math.
        // Scenarios: normal win, reversal, wide spread, thin book, no dump.
        println!("\n  ═══ FULL CHAIN SIMULATION ═══");

        struct Scenario { name: &'static str, ask: f64, loser_bid: f64, outcome: &'static str }
        let scenarios = [
            Scenario { name: "normal_tight",   ask: 0.50, loser_bid: 0.05, outcome: "normal" },
            Scenario { name: "normal_wide",    ask: 0.52, loser_bid: 0.04, outcome: "normal" },
            Scenario { name: "reversal_tight", ask: 0.50, loser_bid: 0.05, outcome: "reversal" },
            Scenario { name: "reversal_wide",  ask: 0.48, loser_bid: 0.08, outcome: "reversal" },
            Scenario { name: "thin_book",      ask: 0.50, loser_bid: 0.01, outcome: "normal" },
            Scenario { name: "deep_bid",       ask: 0.50, loser_bid: 0.15, outcome: "normal" },
            Scenario { name: "nodump_s3d",     ask: 0.50, loser_bid: 0.20, outcome: "normal" }, // bid>0.10, S3D won't dump
            Scenario { name: "edge_low_ask",   ask: 0.47, loser_bid: 0.03, outcome: "normal" },
            Scenario { name: "edge_high_ask",  ask: 0.53, loser_bid: 0.06, outcome: "normal" },
            Scenario { name: "reversal_thin",  ask: 0.50, loser_bid: 0.02, outcome: "reversal" },
        ];

        let strats: [(&str, fn(f64)->f64); 2] = [
            ("S3b", s3b_dump as fn(f64)->f64),
            ("S3C", taker_dump as fn(f64)->f64),
        ];
        for (id, dump_fn) in &strats {
            let mut st = Strat::new(id);
            println!("  ── {} ──", id);
            for sc in &scenarios {
                let dp = dump_fn(sc.loser_bid);
                let pnl = match sc.outcome {
                    "normal"   => pnl_normal(sc.ask, dp),
                    "reversal" => pnl_reversal(sc.ask, dp),
                    _ => unreachable!(),
                };
                let tag = if pnl > 0.0 { "WIN" } else { "LOSS" };
                st.rec(pnl > 0.0, pnl);
                println!("    {} {:16} ask={:.2} bid={:.2} dp={:.3} pnl={:+.4} cap={:.2}",
                    tag, sc.name, sc.ask, sc.loser_bid, dp, pnl, st.cap);
            }
            // Verify capital consistency
            assert!((st.cap - START_CAP - st.pnl).abs() < 0.0001,
                "{} cap drift: cap={:.4} start+pnl={:.4}", id, st.cap, START_CAP + st.pnl);
            assert_eq!(st.w + st.l, 10, "{} must have 10 trades", id);
            println!("    TOTAL: {}W/{}L pnl=${:+.4} cap=${:.2}", st.w, st.l, st.pnl, st.cap);
        }

        // S3D: special — dump only when loser_bid ≤ 0.10
        {
            let mut st = Strat::new("S3D");
            println!("  ── S3D (dump only if bid≤$0.10) ──");
            for sc in &scenarios {
                let pnl = if sc.loser_bid <= 0.10 {
                    let dp = taker_dump(sc.loser_bid);
                    match sc.outcome {
                        "normal"   => pnl_normal(sc.ask, dp),
                        "reversal" => pnl_reversal(sc.ask, dp),
                        _ => unreachable!(),
                    }
                } else {
                    // No dump — winner settles $1, loser $0
                    match sc.outcome {
                        "normal"   => pnl_nodump(sc.ask),
                        "reversal" => { // "reversal" with no dump: held side = loser, settles $0
                            let sh = STAKE_1 / (sc.ask + LATENCY_TICKS);
                            let ef = fee(sc.ask + LATENCY_TICKS) * sh * 2.0;
                            sh * 0.0 - STAKE_2 - ef // winner was the other side, held side settles $0
                        }
                        _ => unreachable!(),
                    }
                };
                let tag = if pnl > 0.0 { "WIN" } else { "LOSS" };
                let dumped = sc.loser_bid <= 0.10;
                st.rec(pnl > 0.0, pnl);
                println!("    {} {:16} ask={:.2} bid={:.2} dump={:5} pnl={:+.4} cap={:.2}",
                    tag, sc.name, sc.ask, sc.loser_bid, dumped, pnl, st.cap);
            }
            assert!((st.cap - START_CAP - st.pnl).abs() < 0.0001, "S3D cap drift");
            assert_eq!(st.w + st.l, 10, "S3D must have 10 trades");
            println!("    TOTAL: {}W/{}L pnl=${:+.4} cap=${:.2}", st.w, st.l, st.pnl, st.cap);
        }

        // Cross-strategy invariants
        println!("\n  ── CROSS-STRATEGY INVARIANTS ──");
        let bid = 0.05;
        let ask = 0.50;
        let s3b_dp = s3b_dump(bid);
        let s3c_dp = taker_dump(bid);
        let s3b_pnl = pnl_normal(ask, s3b_dp);
        let s3c_pnl = pnl_normal(ask, s3c_dp);
        let s3d_dp = taker_dump(bid);
        let s3d_pnl = pnl_normal(ask, s3d_dp);
        println!("    S3b maker dp={:.3} pnl={:+.4}", s3b_dp, s3b_pnl);
        println!("    S3C taker dp={:.3} pnl={:+.4}", s3c_dp, s3c_pnl);
        println!("    S3D taker dp={:.3} pnl={:+.4}", s3d_dp, s3d_pnl);
        assert!(s3b_dp > s3c_dp, "maker dump must beat taker dump on price");
        assert!(s3b_pnl > s3c_pnl, "S3b must beat S3C on same-bid normal");
        assert_close(s3c_pnl, s3d_pnl, "S3C and S3D same dump price when bid≤0.10");

        // Reversal is always a big loss for all strategies
        for bid in [0.02, 0.05, 0.10, 0.15] {
            let dp_maker = s3b_dump(bid);
            let dp_taker = taker_dump(bid);
            let rev_maker = pnl_reversal(0.50, dp_maker);
            let rev_taker = pnl_reversal(0.50, dp_taker);
            assert!(rev_maker < -STAKE_2 * 0.4, "reversal@bid={:.2} must lose >40% stake, got {:.4}", bid, rev_maker);
            assert!(rev_taker < -STAKE_2 * 0.4, "reversal@bid={:.2} taker must lose >40%, got {:.4}", bid, rev_taker);
        }

        // Latency impact verification
        let no_lat_sh = STAKE_1 / 0.50;
        let lat_sh = STAKE_1 / (0.50 + LATENCY_TICKS);
        assert!(no_lat_sh > lat_sh, "latency must reduce shares bought");
        println!("    Latency: {:.6} fewer shares per side ({:.4} vs {:.4})",
            no_lat_sh - lat_sh, no_lat_sh, lat_sh);

        println!("  ═══ ALL CHAIN CHECKS PASSED ═══\n");
    }

    #[test]
    fn test_state_cl_snapshot() {
        let mut s = State::new();
        s.cl_up("btc", 87000.0, 1000.0);
        s.cl_up("btc", 87100.0, 1001.0);
        assert_close(s.cl.get("btc").copied().unwrap(), 87100.0, "latest CL");
        assert_close(s.cl_at("btc", 1000, 0).unwrap(), 87000.0, "snap exact");
        assert_close(s.cl_at("btc", 1001, 0).unwrap(), 87100.0, "snap exact 2");
        assert!(s.cl_at("btc", 999, 0).is_none(), "snap miss");
        assert_close(s.cl_at("btc", 999, 1).unwrap(), 87000.0, "snap tol=1");
        s.bn_up("btc", 87050.0);
        assert_close(s.bn.get("btc").copied().unwrap(), 87050.0, "BN price");
    }
}
