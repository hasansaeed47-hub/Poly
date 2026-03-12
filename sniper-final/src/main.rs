/// main.rs — Sniper Final v1.0 — Hybrid Live Execution Architecture
///
/// Architecture:
/// 1. Three WS feeds: CL (Polymarket RTDS), BN (Binance aggTrade), PM Book (CLOB)
/// 2. REST fallback: batch book refresh every 2s for new tokens
/// 3. Market discovery: Gamma API, auto-refresh every 60s
/// 4. Five engines (A-E) evaluate entry signals independently
/// 5. Maker-first entry (ask-0.01 for 2s, then taker fallback)
/// 6. SL with flip confirmation (bid <= 50% AND opp_bid >= 0.80)
/// 7. Settlement: CLOB post-settle bids (ground truth) + CL fallback
/// 8. Kill switch: cumulative P&L <= -$50 halts all engines
///
/// Order placement via official Polymarket Rust SDK (polymarket-client-sdk).
/// All thresholds in config.toml — nothing hardcoded except defaults.

mod engine;
mod execution;
mod feeds;
mod order;
mod runner;
mod wallet;

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use dashmap::DashMap;
use reqwest::Client;
use serde::Deserialize;
use tracing::{info, warn, debug};
use tracing_subscriber::EnvFilter;

use engine::{EngineConfig, ExecConfig, default_engines, default_exec};
use feeds::{
    BookState, BnHistory, BnPrices, ClPrices, ClSnapshots, RateLimiter,
    build_slug, cl_at, current_window_starts, fetch_books_batch, fetch_market_meta, hour_range,
    run_bn_feed, run_book_feed, run_cl_feed, MarketMeta,
};
use order::{ClobClient, ProxyConfig};
use runner::{Tracker, MarketWindow};
use wallet::Wallet;

// -- Config file types --------------------------------------------------------

#[derive(Deserialize, Debug)]
struct AppConfig {
    feed:     FeedConfig,
    scan:     ScanConfig,
    exec:     Option<ExecConfig>,
    wallet:   Option<WalletConfig>,
    engines:  Option<HashMap<String, EngineConfig>>,
}

#[derive(Deserialize, Debug)]
struct FeedConfig {
    assets:           Vec<String>,
    timeframes:       Vec<u32>,
    clob_rest:        String,
    clob_ws:          String,
    live_ws:          String,
    gamma_api:        String,
    #[serde(default = "default_bn_ws")]
    bn_ws:            String,
    #[serde(default = "default_throttle")]
    rest_throttle_ms: u64,
    #[serde(default = "default_warmup")]
    book_warmup_secs: u64,
    #[serde(default = "default_open_delay")]
    max_open_delay:   f64,
}

fn default_bn_ws() -> String { "wss://stream.binance.com:9443/ws".into() }
fn default_throttle() -> u64 { 500 }
fn default_warmup() -> u64 { 5 }
fn default_open_delay() -> f64 { 5.0 }

#[derive(Deserialize, Debug)]
struct ScanConfig {
    tick_ms: u64,
}

#[derive(Deserialize, Debug, Default)]
struct WalletConfig {
    #[serde(default)]
    private_key: Option<String>,
    #[serde(default)]
    api_url:     Option<String>,
}

// -- Helpers ------------------------------------------------------------------

fn now_secs() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64()
}

fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

// -- Log writers --------------------------------------------------------------

fn open_log(path: &str) -> BufWriter<File> {
    let file = OpenOptions::new()
        .create(true).append(true).open(path)
        .unwrap_or_else(|e| panic!("Cannot open {}: {}", path, e));
    BufWriter::new(file)
}

fn log_json(w: &mut BufWriter<File>, v: &serde_json::Value) {
    if let Ok(line) = serde_json::to_string(v) {
        let _ = writeln!(w, "{}", line);
    }
}

// -- Main ---------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("sniper_final=info".parse()?)
        )
        .with_target(false)
        .init();

    dotenvy::dotenv().ok();

    let cfg_text = std::fs::read_to_string("config.toml")
        .context("cannot read config.toml")?;
    let cfg: AppConfig = toml::from_str(&cfg_text)
        .context("invalid config.toml")?;

    let exec_cfg = cfg.exec.unwrap_or_else(default_exec);
    let engine_cfgs: Vec<EngineConfig> = match cfg.engines {
        Some(ref map) => {
            let mut v: Vec<EngineConfig> = map.values().cloned().collect();
            v.sort_by(|a, b| a.id.cmp(&b.id));
            v
        }
        None => default_engines(),
    };

    // -- Initialize CLOB client (official Polymarket SDK) ---------------------

    let wcfg = cfg.wallet.unwrap_or_default();

    let pk_opt = wcfg.private_key.clone()
        .or_else(|| std::env::var("PRIVATE_KEY").ok());

    let clob_client: Option<Arc<ClobClient>> = match pk_opt {
        Some(pk) => {
            let w = Wallet::from_hex(&pk).context("invalid private_key")?;
            info!("Wallet loaded: {}", w.address());

            let base = wcfg.api_url.clone()
                .or_else(|| std::env::var("CLOB_API_URL").ok())
                .unwrap_or_else(|| "https://clob.polymarket.com".into());

            // Check for proxy/funder mode (Polymarket UI deposits)
            let funder_opt = std::env::var("POLY_FUNDER").ok();
            let client = if let Some(funder_hex) = funder_opt {
                let funder_addr: polymarket_client_sdk::types::Address = funder_hex.parse()
                    .context("invalid POLY_FUNDER address")?;

                // Load existing API credentials if available
                let creds = match (
                    std::env::var("CLOB_API_KEY").ok(),
                    std::env::var("CLOB_API_SECRET").ok(),
                    std::env::var("CLOB_PASSPHRASE").ok(),
                ) {
                    (Some(k), Some(s), Some(p)) => Some((k, s, p)),
                    _ => None,
                };

                let proxy = ProxyConfig {
                    funder: funder_addr,
                    credentials: creds,
                };
                info!("Proxy mode: funder=0x{:x}", funder_addr);
                ClobClient::new_with_proxy(&base, w, proxy)
            } else {
                info!("EOA mode: direct wallet signing");
                ClobClient::new(&base, w)
            };

            Some(Arc::new(client))
        }
        None => {
            warn!("private_key not set — running in PAPER mode (no live orders)");
            None
        }
    };

    let _live_mode = clob_client.is_some();

    // Pre-authenticate SDK so first order doesn't pay 1-2s auth latency
    if let Some(ref clob) = clob_client {
        if let Err(e) = clob.ensure_auth().await {
            warn!("SDK pre-auth failed (will retry on first order): {:#}", e);
        }
    }

    info!("═══════════════════════════════════════════════════════");
    info!("  SNIPER FINAL v1.0 — PAPER (diagnostic logging)");
    info!("═══════════════════════════════════════════════════════");
    info!("  Assets: {:?}", cfg.feed.assets);
    info!("  Timeframes: {:?}m", cfg.feed.timeframes);
    info!("  Engines: {}", engine_cfgs.len());
    for eng in &engine_cfgs {
        if eng.is_late_scalper {
            info!("    [{}] late scalper  book>={:.2}  entry<={}s",
                eng.id, eng.min_entry, eng.entry_start);
        } else {
            info!("    [{}] tf={}m  d>={:.2}%  cont={}  bn={} cl={} regime={}",
                eng.id, eng.tf, eng.delta, eng.continuity,
                eng.bn_contra, eng.cl_fade, eng.regime);
        }
    }
    info!("  Stake: ${:.0}  Max DD: ${:.0}  SL: bid<={}%  Confirm: opp>={:.2}",
        exec_cfg.stake, exec_cfg.max_dd, (exec_cfg.sl_pct * 100.0) as u32, exec_cfg.sl_confirm_bid);
    info!("  Maker chase: {} ticks  Slip: {:.3}  Settle delay: {}s",
        exec_cfg.maker_chase_ticks, exec_cfg.slip, exec_cfg.settle_delay_secs);
    info!("═══════════════════════════════════════════════════════");

    std::fs::create_dir_all("logs").context("cannot create logs/")?;

    let mut event_log = open_log("logs/events.jsonl");

    // Shared state
    let cl_prices:    ClPrices    = Arc::new(DashMap::new());
    let cl_snapshots: ClSnapshots = Arc::new(DashMap::new());
    let book_state:   BookState   = Arc::new(DashMap::new());
    let bn_prices:    BnPrices    = Arc::new(DashMap::new());
    let bn_hist:      BnHistory   = Arc::new(DashMap::new());
    let token_ids:    Arc<DashMap<String, ()>> = Arc::new(DashMap::new());
    let book_live:    Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

    let http = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("HTTP client build failed")?;

    let limiter = Arc::new(RateLimiter::new(cfg.feed.rest_throttle_ms));

    // -- Discover markets -----------------------------------------------------

    info!("Discovering active markets...");
    let mut markets: HashMap<String, MarketMeta> = HashMap::new();

    for asset in &cfg.feed.assets {
        for &tf in &cfg.feed.timeframes {
            let now = now_unix();
            for window_start in current_window_starts(tf, now) {
                let slug = build_slug(asset, tf, window_start);

                match fetch_market_meta(
                    &http, &cfg.feed.gamma_api, &slug, asset, tf, &limiter,
                ).await {
                    Ok(Some(meta)) => {
                        info!("[DISCOVER] {} YES={} NO={} ws={} we={}",
                            slug,
                            &meta.token_yes[..8.min(meta.token_yes.len())],
                            &meta.token_no[..8.min(meta.token_no.len())],
                            meta.window_start, meta.window_end,
                        );
                        log_json(&mut event_log, &serde_json::json!({
                            "event": "DISCOVER", "ts": now_secs(),
                            "slug": &slug, "asset": asset, "tf": tf,
                            "window_start": meta.window_start,
                            "window_end": meta.window_end,
                            "token_yes": &meta.token_yes,
                            "token_no": &meta.token_no,
                        }));
                        token_ids.insert(meta.token_yes.clone(), ());
                        token_ids.insert(meta.token_no.clone(), ());
                        markets.insert(slug, meta);
                    }
                    Ok(None) => {}
                    Err(e) => warn!("[DISCOVER] {} error: {}", slug, e),
                }
            }
        }
    }

    info!("Discovered {} active markets", markets.len());

    // -- Start WebSocket feeds ------------------------------------------------

    {
        let cp = cl_prices.clone();
        let cs = cl_snapshots.clone();
        let assets = cfg.feed.assets.clone();
        let ws = cfg.feed.live_ws.clone();
        tokio::spawn(async move {
            run_cl_feed(ws, assets, cp, cs).await;
        });
    }

    {
        let bs = book_state.clone();
        let ti = token_ids.clone();
        let bl = book_live.clone();
        let ws = cfg.feed.clob_ws.clone();
        tokio::spawn(async move {
            run_book_feed(ws, ti, bs, bl).await;
        });
    }

    {
        let bp = bn_prices.clone();
        let bh = bn_hist.clone();
        let assets = cfg.feed.assets.clone();
        let ws = cfg.feed.bn_ws.clone();
        tokio::spawn(async move {
            run_bn_feed(ws, assets, bp, bh).await;
        });
    }

    // -- Build engine trackers ------------------------------------------------

    let mut trackers: Vec<Tracker> = engine_cfgs.into_iter()
        .map(|ec| Tracker::new(ec, exec_cfg.clone(), "logs"))
        .collect();

    let mut cl_opens: HashMap<String, f64> = HashMap::new();

    // -- Warmup gate ----------------------------------------------------------

    info!("Waiting {}s for feed warmup...", cfg.feed.book_warmup_secs);
    tokio::time::sleep(Duration::from_secs(cfg.feed.book_warmup_secs)).await;

    {
        for asset in &cfg.feed.assets {
            let cl = cl_prices.get(asset.as_str()).map(|e| e.1).unwrap_or(0.0);
            let bn = bn_prices.get(asset.as_str()).map(|v| *v).unwrap_or(0.0);
            info!("  {}: CL=${:.2} BN=${:.2}", asset.to_uppercase(), cl, bn);
        }
    }
    info!("Warmup complete — scan loop starting");

    // -- Main scan loop -------------------------------------------------------

    let mut tick_count:    u64 = 0;
    let mut last_stats:    Instant = Instant::now();
    let mut last_detail:   Instant = Instant::now();
    let mut last_discover: f64 = now_secs();
    let mut halted = false;
    let start_time = Instant::now();

    let mut settled: HashMap<String, bool> = HashMap::new();
    let max_open_delay = cfg.feed.max_open_delay;
    let mut hour_ranges: HashMap<String, f64> = HashMap::new();
    let mut market_slugs: Vec<String> = Vec::with_capacity(32);

    loop {
        let sleep_ms = if trackers.iter().any(|t| t.active.is_some()) {
            cfg.scan.tick_ms
        } else {
            cfg.scan.tick_ms * 2
        };
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        tick_count += 1;

        let now = now_secs();
        let now_u = now as u64;
        let now_i = now as i64;

        // Kill switch
        if halted {
            if last_stats.elapsed().as_secs() >= 30 {
                let cum: f64 = trackers.iter().map(|t| t.stats.pnl).sum();
                info!("HALTED cum=${:+.2}", cum);
                last_stats = Instant::now();
            }
            continue;
        }

        let cum_pnl: f64 = trackers.iter().map(|t| t.stats.pnl).sum();
        if cum_pnl <= -exec_cfg.max_dd {
            info!("═══════════════════════════════════════════════════════");
            info!("  KILL SWITCH  Cumulative P&L: ${:+.2}", cum_pnl);
            info!("  Max DD ${:.0} breached — HALTING ALL ENGINES", exec_cfg.max_dd);
            info!("═══════════════════════════════════════════════════════");
            for tr in &mut trackers {
                if let Some(pos) = tr.active.take() {
                    let loss = -exec_cfg.stake;
                    info!("[DD] FORCE_CLOSE [{}] {} {} — ${:+.2}", tr.cfg.id, pos.dir, pos.asset.to_uppercase(), loss);
                    tr.stats.pnl += loss;
                    tr.stats.losses += 1;
                }
            }
            halted = true;
            continue;
        }

        // Periodic market rediscovery (every 60s)
        if now - last_discover > 60.0 {
            last_discover = now;
            for asset in &cfg.feed.assets {
                for &tf in &cfg.feed.timeframes {
                    for window_start in current_window_starts(tf, now_u) {
                        let slug = build_slug(asset, tf, window_start);
                        if !markets.contains_key(&slug) {
                            if let Ok(Some(meta)) = fetch_market_meta(
                                &http, &cfg.feed.gamma_api, &slug, asset, tf, &limiter,
                            ).await {
                                info!("[DISCOVER] New: {} ws={} we={}", slug, meta.window_start, meta.window_end);
                                log_json(&mut event_log, &serde_json::json!({
                                    "event": "DISCOVER", "ts": now,
                                    "slug": &slug, "asset": asset, "tf": tf,
                                    "window_start": meta.window_start,
                                    "window_end": meta.window_end,
                                }));
                                token_ids.insert(meta.token_yes.clone(), ());
                                token_ids.insert(meta.token_no.clone(), ());
                                markets.insert(slug, meta);
                            }
                        }
                    }
                }
            }
        }

        // Batch book refresh (every 2s via REST fallback)
        if tick_count % 4 == 0 {
            let mut all_tokens: Vec<String> = token_ids.iter().map(|e| e.key().clone()).collect();
            for tr in &trackers {
                if let Some(pos) = &tr.active {
                    all_tokens.push(pos.tid.clone());
                    all_tokens.push(pos.tid_up.clone());
                    all_tokens.push(pos.tid_dn.clone());
                }
            }
            all_tokens.sort();
            all_tokens.dedup();

            if !all_tokens.is_empty() {
                match fetch_books_batch(&http, &cfg.feed.clob_rest, &all_tokens, &limiter).await {
                    Ok(books) => {
                        for (tid, entry) in books {
                            book_state.insert(tid, entry);
                        }
                    }
                    Err(e) => warn!("[BOOK REST] batch failed: {}", e),
                }
            }
        }

        // Compute hour ranges for regime filter (every ~5s)
        if tick_count % 10 == 1 {
            for a in &cfg.feed.assets {
                hour_ranges.insert(a.clone(), hour_range(&cl_snapshots, a));
            }
        }

        // Process each market
        let live_since = book_live.load(Ordering::Relaxed);
        let book_ready = live_since > 0 && now_u.saturating_sub(live_since) >= cfg.feed.book_warmup_secs;

        market_slugs.clear();
        market_slugs.extend(markets.keys().cloned());

        for slug in &market_slugs {
            let meta = match markets.get_mut(slug) {
                Some(m) => m,
                None => continue,
            };

            if settled.get(slug.as_str()).copied().unwrap_or(false) {
                continue;
            }

            // Capture CL open price
            if meta.open_price <= 0.0 && !meta.open_missed {
                if now < meta.window_start as f64 { continue; }
                let delay = now - meta.window_start as f64;
                if delay > max_open_delay {
                    meta.open_missed = true;
                    warn!("[OPEN] {} MISSED — {:.1}s late", slug, delay);
                    continue;
                }

                if let Some(px) = cl_at(&cl_snapshots, &meta.asset, meta.window_start as i64) {
                    if px > 0.0 {
                        meta.open_price = px;
                        meta.open_cl_ts = meta.window_start as f64;
                        cl_opens.insert(slug.clone(), px);
                        info!("[OPEN] {} tf={}m open={:.2} delay={:.1}s", slug, meta.tf, px, delay);
                        log_json(&mut event_log, &serde_json::json!({
                            "event": "OPEN", "ts": now, "slug": slug,
                            "asset": &meta.asset, "tf": meta.tf,
                            "open_price": px, "delay_s": delay,
                        }));
                    }
                } else if let Some(cl_entry) = cl_prices.get(&meta.asset) {
                    let (cl_ts, cl_price) = *cl_entry;
                    if cl_ts >= meta.window_start as f64 && cl_price > 0.0 {
                        meta.open_price = cl_price;
                        meta.open_cl_ts = cl_ts;
                        cl_opens.insert(slug.clone(), cl_price);
                        info!("[OPEN] {} tf={}m open={:.2} delay={:.1}s (live)", slug, meta.tf, cl_price, delay);
                    }
                }
                continue;
            }

            if meta.open_missed || meta.open_price <= 0.0 { continue; }

            let secs_left = meta.window_end as i64 - now_i;

            // SL check for all trackers with active positions
            for tr in &mut trackers {
                if let Some(pos) = &tr.active {
                    if pos.slug == *slug {
                        if let Some(result) = tr.check_stop_loss(&book_state, now) {
                            if let Some(ref clob) = clob_client {
                                let c = clob.clone();
                                let tid = result.tid.clone();
                                let exit_px = result.exit_px;
                                let shares = result.shares;
                                let eid = result.engine_id.clone();
                                tokio::spawn(async move {
                                    match c.place_market_order(&tid, exit_px, shares, "SELL").await {
                                        Ok(resp) => info!("[CLOB] [{}] SL SELL placed: {:?}", eid, resp),
                                        Err(e) => warn!("[CLOB] [{}] SL SELL failed (px={} sz={}): {:#}", eid, exit_px, shares, e),
                                    }
                                });
                            }
                            log_json(&mut event_log, &serde_json::json!({
                                "event": "SL", "ts": now, "slug": slug,
                                "engine": result.engine_id, "dir": result.dir,
                                "pnl": result.pnl, "exit_px": result.exit_px,
                            }));
                        }
                    }
                }
            }

            // Settlement check
            if secs_left <= 0 {
                let settle_ready = now_u >= meta.window_end + exec_cfg.settle_delay_secs;
                if settle_ready {
                    for tr in &mut trackers {
                        tr.check_settlement(&book_state, &cl_snapshots, &cl_opens, now);
                    }
                    settled.insert(slug.clone(), true);

                    log_json(&mut event_log, &serde_json::json!({
                        "event": "SETTLE", "ts": now, "slug": slug,
                        "asset": &meta.asset, "tf": meta.tf,
                    }));
                }
                continue;
            }

            if !book_ready { continue; }

            // Entry evaluation for all engines
            let win = MarketWindow {
                slug,
                asset:     &meta.asset,
                wmin:      meta.tf,
                end_ts:    meta.window_end as i64,
                tid_up:    &meta.token_yes,
                tid_dn:    &meta.token_no,
                secs_left,
            };

            for tr in &mut trackers {
                let entered = tr.evaluate_entry(
                    &win, &cl_prices, &cl_snapshots, &cl_opens,
                    &book_state, &bn_prices, &bn_hist, &hour_ranges, now,
                );

                if entered {
                    if let (Some(clob), Some(pos)) = (&clob_client, &tr.active) {
                        let c = clob.clone();
                        let tid = pos.tid.clone();
                        // Use tight limit price: current book ask + slip (not fill_px which can be
                        // much higher and would sweep a crashed book if there's auth/network delay)
                        let book_ask = book_state.get(&tid).map(|b| b.best_ask).unwrap_or(pos.fill_px);
                        let limit_px = (book_ask + exec_cfg.slip).min(pos.fill_px);
                        let shares = pos.shares;
                        let eid = pos.engine_id.clone();
                        let slug_s = pos.slug.clone();
                        tokio::spawn(async move {
                            debug!("[CLOB] [{}] BUY attempt for {}: limit_px={} shares={} tid={}",
                                eid, slug_s, limit_px, shares, &tid[..tid.len().min(20)]);
                            match c.place_limit_order(&tid, limit_px, shares, "BUY").await {
                                Ok(resp) => info!("[CLOB] [{}] BUY placed for {}: {:?}", eid, slug_s, resp),
                                Err(e) => warn!("[CLOB] [{}] BUY failed for {} (px={} sz={}): {:#}", eid, slug_s, limit_px, shares, e),
                            }
                        });
                    }

                    log_json(&mut event_log, &serde_json::json!({
                        "event": "ENTRY", "ts": now, "slug": slug,
                        "engine": tr.cfg.id,
                        "dir": tr.active.as_ref().map(|p| p.dir.as_str()).unwrap_or("?"),
                        "fill_px": tr.active.as_ref().map(|p| p.fill_px).unwrap_or(0.0),
                        "asset": tr.active.as_ref().map(|p| p.asset.as_str()).unwrap_or("?"),
                    }));
                }
            }
        }

        // Flush logs periodically
        if tick_count % 20 == 0 {
            let _ = event_log.flush();
        }

        // Status every 60s
        if last_stats.elapsed().as_secs() >= 60 {
            let px: Vec<String> = cfg.feed.assets.iter()
                .filter_map(|a| cl_prices.get(a.as_str()).map(|e| format!("{}=${:.0}", a.to_uppercase(), e.1)))
                .collect();
            let hrs = start_time.elapsed().as_secs_f64() / 3600.0;
            let active = trackers.iter().filter(|t| t.active.is_some()).count();
            let cum: f64 = trackers.iter().map(|t| t.stats.pnl).sum();
            let statuses: Vec<String> = trackers.iter().map(|t| t.status()).collect();
            info!("── {} | {:.1}h | active={} cum=${:+.2} | {} ──",
                px.join(" "), hrs, active, cum, statuses.join(" | "));
            last_stats = Instant::now();
        }

        // Detailed stats every 5 min
        if last_detail.elapsed().as_secs() >= 300 {
            info!("═══════════════════════════════════════════════════════");
            info!("  STATS — {:.1}h elapsed", start_time.elapsed().as_secs_f64() / 3600.0);
            for tr in &trackers {
                tr.print_stats();
            }
            let cum: f64 = trackers.iter().map(|t| t.stats.pnl).sum();
            info!("  CUMULATIVE: ${:+.2} / -${:.0} DD limit", cum, exec_cfg.max_dd);
            info!("  Markets: {} active, {} settled, book_state={}, cl={}",
                markets.len() - settled.len(), settled.len(), book_state.len(), cl_prices.len());
            info!("═══════════════════════════════════════════════════════");
            last_detail = Instant::now();
        }

        // Cleanup stale data (every 60s)
        if tick_count % 120 == 0 {
            let cutoff = now_i - 3600;
            cl_opens.retain(|k, _| slug_ts(k) > cutoff);
            for tr in &mut trackers {
                tr.cleanup(cutoff);
            }
            let stale: Vec<String> = markets.keys()
                .filter(|k| slug_ts(k) < cutoff)
                .cloned()
                .collect();
            for k in stale {
                markets.remove(&k);
                settled.remove(&k);
            }
        }
    }
}

fn slug_ts(slug: &str) -> i64 {
    runner::slug_ts(slug)
}
