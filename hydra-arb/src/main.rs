//! Hydra-Arb — S-strategy paper tracker for Polymarket 5m/15m markets
//! S2: Contrarian | S3a-3E: Both-sides arb | S4: 5m→15m cascade

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info};

const ASSETS: &[&str] = &["btc", "eth", "sol", "xrp"];
const STAKE_1: f64 = 5.0;
const STAKE_2: f64 = 10.0;
const START_CAP: f64 = 100.0;
const MIN_ENTRY: f64 = 0.85;
const MAX_ENTRY: f64 = 0.98;
const SL_PCT: f64 = 0.50;
const SLIP: f64 = 0.005;
const FILL_PROB: f64 = 0.60;
const RTDS_WS: &str = "wss://ws-live-data.polymarket.com";
const BN_WS: &str = "wss://stream.binance.com:9443/ws";
const GAMMA: &str = "https://gamma-api.polymarket.com";
const CLOB: &str = "https://clob.polymarket.com";

fn fee(px: f64) -> f64 { px * (1.0 - px) * 0.0625 }

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
    info!("[CL] Connecting..."); if let Err(e) = cl_ws(&st).await { error!("[CL] {}", e); }
    tokio::time::sleep(Duration::from_secs(3)).await;
}}
async fn cl_ws(st: &SS) -> Result<()> {
    let (mut ws, _) = connect_async(RTDS_WS).await.context("CL")?;
    ws.send(Message::Text(json!({"action":"subscribe","subscriptions":[
        {"topic":"crypto_prices_chainlink","type":"*","filters":""}]}).to_string())).await?;
    info!("[CL] OK");
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
    info!("[BN] Connecting..."); if let Err(e) = bn_ws(&st).await { error!("[BN] {}", e); }
    tokio::time::sleep(Duration::from_secs(3)).await;
}}
async fn bn_ws(st: &SS) -> Result<()> {
    let streams: Vec<String> = ASSETS.iter().map(|a| format!("{}@aggTrade", bnsym(a))).collect();
    let (mut ws, _) = connect_async(format!("{}/{}", BN_WS, streams.join("/"))).await.context("BN")?;
    info!("[BN] OK");
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

#[derive(Clone, Debug)]
struct Win { slug: String, asset: &'static str, wmin: u32, tid_up: String, tid_down: String, start_ts: i64, end_ts: i64 }
impl Win { fn left(&self) -> i64 { self.end_ts - Utc::now().timestamp() } }

struct Scan { http: reqwest::Client, cache: Vec<Win>, last: Instant }
impl Scan {
    fn new() -> Self { Scan { http: reqwest::Client::builder().user_agent("hydra-arb/1").timeout(Duration::from_secs(5)).build().expect("h"), cache: Vec::new(), last: Instant::now()-Duration::from_secs(999) } }
    async fn get(&mut self) -> Vec<Win> {
        if self.last.elapsed() < Duration::from_secs(10) { return self.cache.iter().filter(|w|w.left()>0).cloned().collect(); }
        let now = Utc::now().timestamp();
        let mut ws = Vec::new();
        for &a in ASSETS { for &wm in &[5u32,15] {
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
        }}
        self.cache = ws.clone(); self.last = Instant::now();
        ws.into_iter().filter(|w|w.left()>0).collect()
    }
}

#[derive(Clone,Default,Debug)]
struct Bk { bb: f64, ba: f64, ha: bool, hb: bool }
struct BkC { http: reqwest::Client, c: HashMap<String,Bk>, t: HashMap<String,Instant> }
impl BkC {
    fn new() -> Self { BkC { http: reqwest::Client::builder().user_agent("hydra-arb/1").timeout(Duration::from_secs(2)).build().expect("h"), c: HashMap::new(), t: HashMap::new() } }
    fn get(&self, tid: &str) -> Bk { self.c.get(tid).cloned().unwrap_or_default() }
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

#[derive(Clone)]
#[allow(dead_code)]
struct PT {
    id: &'static str, slug: String, asset: &'static str, wmin: u32,
    dir: String, px: f64, shares: f64,
    dir2: String, px2: f64, sh2: f64,
    end_ts: i64, sl: f64,
    dumped: bool, dump_px: f64,
    wsold: bool, wpx: f64,
    tid_up: String, tid_dn: String,
}

struct Strat { id: &'static str, cap: f64, w: u32, l: u32, pnl: f64, active: HashMap<String,PT>, done: HashSet<String> }
impl Strat {
    fn new(id: &'static str) -> Self { Strat { id, cap: START_CAP, w:0, l:0, pnl:0.0, active: HashMap::new(), done: HashSet::new() } }
    fn rec(&mut self, won: bool, p: f64) { if won {self.w+=1} else {self.l+=1}; self.pnl+=p; self.cap+=p; }
    fn t(&self) -> u32 { self.w+self.l }
}

const STRATS: &[&str] = &["S2","S3a","S3b","S3C","S3D","S3E","S4"];

struct HydraArb { st: SS, scan: Scan, bk: BkC, s: HashMap<&'static str, Strat>,
    cl_o: HashMap<String,f64>, sub_r: HashMap<String,String>, start: Instant }

impl HydraArb {
    fn new(st: SS, scan: Scan, bk: BkC) -> Self {
        let mut s = HashMap::new();
        for id in STRATS { s.insert(*id, Strat::new(id)); }
        HydraArb { st, scan, bk, s, cl_o: HashMap::new(), sub_r: HashMap::new(), start: Instant::now() }
    }

    async fn tick(&mut self) {
        let wins = self.scan.get().await;
        let mut tids: Vec<String> = Vec::new();
        for w in &wins { if w.left()>0 && w.left()<300 { tids.push(w.tid_up.clone()); tids.push(w.tid_down.clone()); } }
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

        self.eval_s2(&wins).await;
        self.eval_s3a(&wins).await;
        self.eval_s3(&wins).await;
        self.eval_s4(&wins).await;
        self.manage().await;

        let now = Utc::now().timestamp();
        let c = now-3600;
        self.cl_o.retain(|k,_| k.rsplit('-').next().and_then(|s|s.parse::<i64>().ok()).map(|t|t>c).unwrap_or(false));
        self.sub_r.retain(|k,_| k.rsplit('-').next().and_then(|s|s.parse::<i64>().ok()).map(|t|t>c).unwrap_or(false));
        for st in self.s.values_mut() { st.done.retain(|k| k.rsplit('-').next().and_then(|s|s.parse::<i64>().ok()).map(|t|t>c).unwrap_or(false)); }
    }

    async fn eval_s2(&mut self, wins: &[Win]) {
        for w in wins {
            if w.wmin!=5 { continue; }
            let left = w.left(); if left>57||left<44 { continue; }
            let st = self.s.get_mut("S2").expect("s");
            if st.done.contains(&w.slug)||st.active.contains_key(&w.slug)||st.cap<STAKE_1 { continue; }
            let bu = self.bk.get(&w.tid_up); let bd = self.bk.get(&w.tid_down);
            if !bu.ha||!bd.ha { continue; }
            let (dir,ask) = if bu.ba<=bd.ba && bu.ba<=0.40 { ("UP",bu.ba) }
                else if bd.ba<=0.40 { ("DOWN",bd.ba) } else { continue };
            let fp = ask+SLIP; let sh = STAKE_1/fp;
            st.cap -= STAKE_1;
            st.active.insert(w.slug.clone(), PT { id:"S2", slug:w.slug.clone(), asset:w.asset, wmin:w.wmin,
                dir:dir.into(), px:fp, shares:sh, dir2:String::new(), px2:0.0, sh2:0.0,
                end_ts:w.end_ts, sl:fp*SL_PCT, dumped:false, dump_px:0.0, wsold:false, wpx:0.0,
                tid_up:w.tid_up.clone(), tid_dn:w.tid_down.clone() });
            st.done.insert(w.slug.clone());
            info!("[S2] {} {} @{:.3}", dir, w.asset.to_uppercase(), fp);
        }
    }

    async fn eval_s3a(&mut self, wins: &[Win]) {
        for w in wins {
            if w.wmin!=5 { continue; }
            let left = w.left(); if left>57||left<44 { continue; }
            let st = self.s.get_mut("S3a").expect("s");
            if st.done.contains(&w.slug)||st.active.contains_key(&w.slug)||st.cap<STAKE_2 { continue; }
            let bu = self.bk.get(&w.tid_up); let bd = self.bk.get(&w.tid_down);
            if !bu.ha||!bd.ha { continue; }
            if bu.ba+bd.ba >= 0.98 { continue; }
            let sh = (STAKE_1/bu.ba).min(STAKE_1/bd.ba);
            let cost = sh*bu.ba + sh*bd.ba;
            st.cap -= cost;
            st.active.insert(w.slug.clone(), PT { id:"S3a", slug:w.slug.clone(), asset:w.asset, wmin:w.wmin,
                dir:"UP".into(), px:bu.ba, shares:sh, dir2:"DOWN".into(), px2:bd.ba, sh2:sh,
                end_ts:w.end_ts, sl:0.0, dumped:false, dump_px:0.0, wsold:false, wpx:0.0,
                tid_up:w.tid_up.clone(), tid_dn:w.tid_down.clone() });
            st.done.insert(w.slug.clone());
            info!("[S3a] ARB {} sum={:.3}", w.asset.to_uppercase(), bu.ba+bd.ba);
        }
    }

    async fn eval_s3(&mut self, wins: &[Win]) {
        for w in wins {
            if w.wmin!=5 { continue; }
            let left = w.left(); if left>290||left<60 { continue; }
            let bu = self.bk.get(&w.tid_up); let bd = self.bk.get(&w.tid_down);
            if !bu.ha||!bd.ha { continue; }
            if bu.ba<0.47||bu.ba>0.53||bd.ba<0.47||bd.ba>0.53 { continue; }
            for id in ["S3b","S3C","S3D","S3E"] {
                let st = self.s.get_mut(id).expect("s");
                if st.done.contains(&w.slug)||st.active.contains_key(&w.slug)||st.cap<STAKE_2 { continue; }
                let up = bu.ba+SLIP; let dn = bd.ba+SLIP;
                let ush = STAKE_1/up; let dsh = STAKE_1/dn;
                let uf = fee(up)*ush; let df = fee(dn)*dsh;
                st.cap -= STAKE_2 + uf + df;
                st.active.insert(w.slug.clone(), PT { id, slug:w.slug.clone(), asset:w.asset, wmin:w.wmin,
                    dir:"UP".into(), px:up, shares:ush, dir2:"DOWN".into(), px2:dn, sh2:dsh,
                    end_ts:w.end_ts, sl:0.0, dumped:false, dump_px:0.0, wsold:false, wpx:0.0,
                    tid_up:w.tid_up.clone(), tid_dn:w.tid_down.clone() });
                st.done.insert(w.slug.clone());
                info!("[{}] BOTH {} UP@{:.3} DN@{:.3}", id, w.asset.to_uppercase(), up, dn);
            }
        }
    }

    async fn eval_s4(&mut self, wins: &[Win]) {
        let mut rng = rand::thread_rng();
        for w in wins {
            if w.wmin!=15 { continue; }
            let left = w.left(); if left>50||left<44 { continue; }
            let st = self.s.get_mut("S4").expect("s");
            if st.done.contains(&w.slug)||st.active.contains_key(&w.slug)||st.cap<STAKE_1 { continue; }
            let iv = 300i64;
            let (mut u,mut d) = (0u32,0u32);
            for i in 0..3 {
                let ss = format!("{}-updown-5m-{}", w.asset, w.start_ts+i*iv);
                match self.sub_r.get(&ss).map(|s|s.as_str()) { Some("UP")=>{u+=1}, Some("DOWN")=>{d+=1}, _=>{} }
            }
            let dir = if u>=2 {"UP"} else if d>=2 {"DOWN"} else { continue };
            let tid = if dir=="UP" {&w.tid_up} else {&w.tid_down};
            let bk = self.bk.get(tid);
            if !bk.ha||bk.ba<MIN_ENTRY||bk.ba>MAX_ENTRY { continue; }
            let mk = ((bk.ba-0.01)*100.0).round()/100.0;
            let fp = if rng.gen::<f64>()<FILL_PROB { mk } else { bk.ba+SLIP };
            if fp>MAX_ENTRY { continue; }
            let sh = STAKE_1/fp;
            st.cap -= STAKE_1;
            st.active.insert(w.slug.clone(), PT { id:"S4", slug:w.slug.clone(), asset:w.asset, wmin:w.wmin,
                dir:dir.into(), px:fp, shares:sh, dir2:String::new(), px2:0.0, sh2:0.0,
                end_ts:w.end_ts, sl:fp*SL_PCT, dumped:false, dump_px:0.0, wsold:false, wpx:0.0,
                tid_up:w.tid_up.clone(), tid_dn:w.tid_down.clone() });
            st.done.insert(w.slug.clone());
            info!("[S4] {} {} 15m @{:.3} ({}/3)", dir, w.asset.to_uppercase(), fp, u.max(d));
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

                // Single-side: S2, S4
                if t.dir2.is_empty() {
                    // SL: CL flip
                    if t.sl > 0.0 {
                        if let (Some(&co), Some(&cn)) = (self.cl_o.get(&slug), s.cl.get(t.asset)) {
                            let d = (cn-co)/co*100.0;
                            let flip = (t.dir=="UP" && d < -0.01)||(t.dir=="DOWN" && d>0.01);
                            if flip {
                                let rec = STAKE_1*SL_PCT;
                                let f = fee(t.sl)*t.shares;
                                let pnl = rec - STAKE_1 - f;
                                info!("[{}] SL {} {} ${:+.2}", st.id, t.dir, t.asset.to_uppercase(), pnl);
                                settles.push((st.id, slug.clone(), pnl)); continue;
                            }
                        }
                    }
                    // Settle
                    if now >= t.end_ts+3 {
                        let co = self.cl_o.get(&slug).copied().unwrap_or(0.0);
                        let cc = s.cl_at(t.asset,t.end_ts,2).or_else(||s.cl_at(t.asset,t.end_ts,5)).or_else(||s.cl.get(t.asset).copied()).unwrap_or(0.0);
                        if t.wmin==5 { self.sub_r.insert(slug.clone(), if cc>=co {"UP"} else {"DOWN"}.into()); }
                        if co<=0.0||cc<=0.0 { settles.push((st.id,slug.clone(),-STAKE_1)); continue; }
                        let actual = if cc>=co {"UP"} else {"DOWN"};
                        let won = actual==t.dir;
                        let pnl = if won { t.shares*1.0-STAKE_1 } else { -STAKE_1 };
                        info!("[{}] {} {} {}m @{:.3} → {} ${:+.2}", st.id, if won{"WIN"}else{"LOSS"}, t.asset.to_uppercase(), t.wmin, t.px, actual, pnl);
                        settles.push((st.id, slug.clone(), pnl));
                    }
                    continue;
                }

                // Both-sides: S3a/S3b/S3C/S3D/S3E
                let co = self.cl_o.get(&slug).copied().unwrap_or(0.0);
                let cn = s.cl.get(t.asset).copied().unwrap_or(0.0);
                if co<=0.0||cn<=0.0 { continue; }
                let cl_d = ((cn-co)/co*100.0).abs();
                let up_winning = cn >= co;
                let loser_bid = if cl_d>0.3 {0.05} else if cl_d>0.15 {0.10} else if cl_d>0.05 {0.20} else {0.35};

                // S3a: hold to settle
                // S3b: dump at T-30 maker→taker
                if st.id=="S3b" && !t.dumped && left<=30 {
                    let dp = (loser_bid-SLIP).max(0.01);
                    if let Some(tm) = st.active.get_mut(&slug) { tm.dumped=true; tm.dump_px=dp; }
                    let ls = if up_winning {t.sh2} else {t.shares};
                    info!("[S3b] DUMP {} @{:.3} rec=${:.2}", t.asset.to_uppercase(), dp, ls*dp);
                }
                // S3C: taker dump at T-30
                if st.id=="S3C" && !t.dumped && left<=30 {
                    let dp = (loser_bid-SLIP).max(0.01);
                    if let Some(tm) = st.active.get_mut(&slug) { tm.dumped=true; tm.dump_px=dp; }
                    info!("[S3C] DUMP {} @{:.3}", t.asset.to_uppercase(), dp);
                }
                // S3D: dump when loser ≤ 0.10
                if st.id=="S3D" && !t.dumped && loser_bid<=0.10 {
                    let dp = (0.10-SLIP).max(0.01);
                    if let Some(tm) = st.active.get_mut(&slug) { tm.dumped=true; tm.dump_px=dp; }
                    info!("[S3D] DUMP {} @{:.3}", t.asset.to_uppercase(), dp);
                }
                // S3E: dump loser at 0.10, sell winner at 0.95
                if st.id=="S3E" && !t.dumped && loser_bid<=0.10 {
                    let dp = (0.10-SLIP).max(0.01);
                    if let Some(tm) = st.active.get_mut(&slug) { tm.dumped=true; tm.dump_px=dp; }
                    info!("[S3E] DUMP loser {} @{:.3}", t.asset.to_uppercase(), dp);
                }
                if st.id=="S3E" && t.dumped && !t.wsold {
                    let winner_approx = if cl_d>0.3 {0.95} else if cl_d>0.15 {0.85} else {0.70};
                    if winner_approx >= 0.95 {
                        let sp = 0.95-SLIP;
                        if let Some(tm) = st.active.get_mut(&slug) { tm.wsold=true; tm.wpx=sp; }
                        info!("[S3E] SELL winner {} @{:.3}", t.asset.to_uppercase(), sp);
                    }
                }

                // Settlement
                if now >= t.end_ts+3 {
                    let cc = s.cl_at(t.asset,t.end_ts,2).or_else(||s.cl_at(t.asset,t.end_ts,5)).or_else(||s.cl.get(t.asset).copied()).unwrap_or(0.0);
                    if cc<=0.0 { settles.push((st.id,slug.clone(),-STAKE_2)); continue; }
                    if t.wmin==5 { self.sub_r.insert(slug.clone(), if cc>=co {"UP"} else {"DOWN"}.into()); }
                    let actual = if cc>=co {"UP"} else {"DOWN"};
                    let (up_pay, dn_pay) = if actual=="UP" {(1.0,0.0)} else {(0.0,1.0)};

                    let pnl = if st.id=="S3a" {
                        t.shares * 1.0 - (t.px*t.shares + t.px2*t.sh2)
                    } else if st.id=="S3E" && t.wsold {
                        let lr = if up_winning {t.sh2} else {t.shares};
                        let wr = if up_winning {t.shares} else {t.sh2};
                        let lrec = lr * t.dump_px;
                        let wrec = wr * t.wpx;
                        lrec + wrec - STAKE_2 - fee(t.dump_px)*lr - fee(t.wpx)*wr
                    } else if t.dumped {
                        let lr = if up_winning {t.sh2} else {t.shares};
                        let wr = if up_winning {t.shares} else {t.sh2};
                        let loser_was_right = (!up_winning && actual=="UP")||(up_winning && actual=="DOWN");
                        if loser_was_right {
                            lr*t.dump_px + 0.0 - STAKE_2 - fee(t.dump_px)*lr
                        } else {
                            lr*t.dump_px + wr*1.0 - STAKE_2 - fee(t.dump_px)*lr
                        }
                    } else {
                        t.shares*up_pay + t.sh2*dn_pay - STAKE_2
                    };

                    let tag = if pnl>0.0 {"WIN"} else {"LOSS"};
                    info!("[{}] {} {} ${:+.2} (cum ${:+.2})", st.id, tag, t.asset.to_uppercase(), pnl, st.pnl+pnl);
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
        for id in STRATS {
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
    tracing_subscriber::fmt().with_env_filter("hydra_arb=info").with_target(false).init();
    dotenvy::dotenv().ok();
    info!("══════════════════════════════════════════════════════════");
    info!("  HYDRA-ARB — S-Strategy Paper Tracker");
    info!("  S2: Contrarian ≤$0.40");
    info!("  S3a: Both-sides arb (hold to settle)");
    info!("  S3b: Both-sides (dump T-30 maker→taker)");
    info!("  S3C: Both-sides (taker dump T-30)");
    info!("  S3D: Both-sides (dump when loser ≤$0.10)");
    info!("  S3E: Both-sides (dump loser@0.10 + sell winner@0.95)");
    info!("  S4:  5m→15m cascade (2/3 confirm)");
    info!("  ${}/{} stake | ${}/strat", STAKE_1, STAKE_2, START_CAP);
    info!("══════════════════════════════════════════════════════════");

    let st: SS = Arc::new(RwLock::new(State::new()));
    let c = st.clone(); tokio::spawn(async move { cl_feed(c).await; });
    let b = st.clone(); tokio::spawn(async move { bn_feed(b).await; });

    info!("[BOOT] Waiting for feeds...");
    for _ in 0..20 { tokio::time::sleep(Duration::from_secs(1)).await;
        let s = st.read().await; if s.cl.contains_key("btc")&&s.bn.contains_key("btc") { break; } }
    { let s = st.read().await; for &a in ASSETS {
        info!("  {}: CL=${:.2} BN=${:.2}", a.to_uppercase(), s.cl.get(a).copied().unwrap_or(0.0), s.bn.get(a).copied().unwrap_or(0.0));
    }}

    let mut hydra = HydraArb::new(st.clone(), Scan::new(), BkC::new());
    info!("[BOOT] Running...");
    let mut ls = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        hydra.tick().await;
        if ls.elapsed().as_secs() >= 60 {
            let s = st.read().await;
            let px: Vec<String> = ASSETS.iter().filter_map(|&a|s.cl.get(a).map(|p|format!("{}=${:.0}",a.to_uppercase(),p))).collect();
            info!("─── {} | {:.1}h | {} ───", px.join(" "), hydra.start.elapsed().as_secs_f64()/3600.0, hydra.status());
            ls = Instant::now();
        }
    }
}
