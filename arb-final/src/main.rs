/// main.rs — Arb Final v1.0 — Pair Arbitrage on Polymarket Updown Markets
///
/// Architecture:
///   1. Three WS feeds: CL (chainlink), BN (Binance), PM Book (CLOB)
///   2. REST fallback: batch book refresh every 2s
///   3. Market discovery: Gamma API for 15m + 60m updown markets
///   4. One global ArbTracker: sequential maker-only orders
///   5. Matched sets with $10 max exposure per window
///   6. Trend filter: skip trending markets (enter only non-directional)
///   7. Merged pairs free capital for re-entry within same window
///
/// Paper mode by default (no private key = no live orders).

mod arb;
mod feeds;
mod order;
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

use feeds::{
    BookState, BnHistory, BnPrices, ClPrices, ClSnapshots, RateLimiter,
    build_slug, cl_at, current_window_starts, fetch_books_batch, fetch_market_meta,
    run_bn_feed, run_book_feed, run_cl_feed, MarketMeta,
};
use order::{ClobClient, ProxyConfig};
use wallet::Wallet;

use arb::{
    ArbBook, ArbConfig, ArbPhase, ArbTradeLog, PendingOrder, Side,
    maker_limit_price, pm_fee,
};

// -- Config -------------------------------------------------------------------

#[derive(Deserialize, Debug)]
struct AppConfig {
    feed: FeedConfig,
    scan: ScanConfig,
    arb:  ArbConfig,
    #[serde(default)]
    wallet: WalletConfig,
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

fn open_log(path: &str) -> BufWriter<File> {
    let file = OpenOptions::new()
        .create(true).append(true).open(path)
        .unwrap_or_else(|e| panic!("Cannot open {}: {}", path, e));
    BufWriter::new(file)
}

fn log_arb(w: &mut BufWriter<File>, entry: &ArbTradeLog) {
    if let Ok(line) = serde_json::to_string(entry) {
        let _ = writeln!(w, "{}", line);
    }
}

fn slug_ts(slug: &str) -> i64 {
    slug.rsplit('-').next()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0)
}

// -- Main ---------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("arb_final=info".parse()?)
        )
        .with_target(false)
        .init();

    dotenvy::dotenv().ok();

    let cfg_text = std::fs::read_to_string("config.toml")
        .context("cannot read config.toml")?;
    let cfg: AppConfig = toml::from_str(&cfg_text)
        .context("invalid config.toml")?;

    let arb_cfg = cfg.arb.clone();

    // -- CLOB client ----------------------------------------------------------

    let pk_opt = cfg.wallet.private_key.clone()
        .or_else(|| std::env::var("PRIVATE_KEY").ok());

    let clob_client: Option<Arc<ClobClient>> = match pk_opt {
        Some(pk) => {
            let w = Wallet::from_hex(&pk).context("invalid private_key")?;
            info!("Wallet loaded: {}", w.address());

            let base = cfg.wallet.api_url.clone()
                .or_else(|| std::env::var("CLOB_API_URL").ok())
                .unwrap_or_else(|| "https://clob.polymarket.com".into());

            let funder_opt = std::env::var("POLY_FUNDER").ok();
            let client = if let Some(funder_hex) = funder_opt {
                let funder_addr: polymarket_client_sdk::types::Address = funder_hex.parse()
                    .context("invalid POLY_FUNDER address")?;
                let creds = match (
                    std::env::var("CLOB_API_KEY").ok(),
                    std::env::var("CLOB_API_SECRET").ok(),
                    std::env::var("CLOB_PASSPHRASE").ok(),
                ) {
                    (Some(k), Some(s), Some(p)) => Some((k, s, p)),
                    _ => None,
                };
                let proxy = ProxyConfig { funder: funder_addr, credentials: creds };
                ClobClient::new_with_proxy(&base, w, proxy)
            } else {
                ClobClient::new(&base, w)
            };
            Some(Arc::new(client))
        }
        None => {
            warn!("private_key not set — running in PAPER mode");
            None
        }
    };

    if let Some(ref clob) = clob_client {
        if let Err(e) = clob.ensure_auth().await {
            warn!("SDK pre-auth failed: {:#}", e);
        }
    }

    // -- Banner ---------------------------------------------------------------

    let mode = if clob_client.is_some() { "LIVE" } else { "PAPER" };
    info!("=======================================================");
    info!("  ARB FINAL v1.0 — {} mode", mode);
    info!("=======================================================");
    info!("  Assets: {:?}", cfg.feed.assets);
    info!("  Timeframes: {:?}m", cfg.feed.timeframes);
    info!("  Unit: ${:.0}  Max exposure: ${:.0}  Max pair cost: {:.2}",
        arb_cfg.unit_size, arb_cfg.max_exposure, arb_cfg.max_pair_cost);
    info!("  Ask range: {:.2}-{:.2}  Maker timeout: {:.0}s",
        arb_cfg.min_ask, arb_cfg.max_ask, arb_cfg.maker_timeout_secs);
    info!("  Trend threshold: {:.2}% (BTC base, stdev-scaled)", arb_cfg.trend_threshold_pct);
    info!("  Observe: 15m={:.0}s 60m={:.0}s  Lockdown: 15m={:.0}s 60m={:.0}s",
        arb_cfg.observe_secs_15m, arb_cfg.observe_secs_60m,
        arb_cfg.lockdown_secs_15m, arb_cfg.lockdown_secs_60m);
    info!("  Strategy: MAKER ONLY | SEQUENTIAL | MATCHED SETS");
    info!("=======================================================");

    std::fs::create_dir_all("logs").context("cannot create logs/")?;
    let mut trade_log = open_log("logs/arb_trades.jsonl");

    // -- Shared state ---------------------------------------------------------

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

    info!("Discovering 15m + 60m updown markets...");
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
                        info!("[DISCOVER] {} YES={} NO={}",
                            slug,
                            &meta.token_yes[..8.min(meta.token_yes.len())],
                            &meta.token_no[..8.min(meta.token_no.len())]);
                        token_ids.insert(meta.token_yes.clone(), ());
                        token_ids.insert(meta.token_no.clone(), ());
                        markets.insert(slug, meta);
                    }
                    Ok(None) => debug!("[DISCOVER] {} — not found", slug),
                    Err(e) => warn!("[DISCOVER] {} error: {}", slug, e),
                }
            }
        }
    }
    info!("Discovered {} active markets", markets.len());

    // -- Start feeds ----------------------------------------------------------

    {
        let cp = cl_prices.clone();
        let cs = cl_snapshots.clone();
        let assets = cfg.feed.assets.clone();
        let ws = cfg.feed.live_ws.clone();
        tokio::spawn(async move { run_cl_feed(ws, assets, cp, cs).await; });
    }
    {
        let bs = book_state.clone();
        let ti = token_ids.clone();
        let bl = book_live.clone();
        let ws = cfg.feed.clob_ws.clone();
        tokio::spawn(async move { run_book_feed(ws, ti, bs, bl).await; });
    }
    {
        let bp = bn_prices.clone();
        let bh = bn_hist.clone();
        let assets = cfg.feed.assets.clone();
        let ws = cfg.feed.bn_ws.clone();
        tokio::spawn(async move { run_bn_feed(ws, assets, bp, bh).await; });
    }

    // -- Warmup ---------------------------------------------------------------

    info!("Waiting {}s for feed warmup...", cfg.feed.book_warmup_secs);
    tokio::time::sleep(Duration::from_secs(cfg.feed.book_warmup_secs)).await;

    for asset in &cfg.feed.assets {
        let cl = cl_prices.get(asset.as_str()).map(|e| e.1).unwrap_or(0.0);
        let bn = bn_prices.get(asset.as_str()).map(|v| *v).unwrap_or(0.0);
        info!("  {}: CL=${:.2} BN=${:.2}", asset.to_uppercase(), cl, bn);
    }
    info!("Warmup complete — arb loop starting");

    // -- Main loop state ------------------------------------------------------

    let mut arb_books: HashMap<String, ArbBook> = HashMap::new();
    let mut pending_order: Option<PendingOrder> = None;
    let mut cl_opens: HashMap<String, f64> = HashMap::new();
    let mut settled: HashMap<String, bool> = HashMap::new();

    let mut tick_count: u64 = 0;
    let mut last_stats = Instant::now();
    let mut last_discover = now_secs();
    let mut cum_pnl: f64 = 0.0;
    let mut total_pairs: u32 = 0;
    let start_time = Instant::now();

    let max_open_delay = cfg.feed.max_open_delay;

    // -- Shutdown signal ------------------------------------------------------

    let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let s = shutdown.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutdown signal received — flushing logs...");
            s.store(true, Ordering::Relaxed);
        });
    }

    // -- Main loop ------------------------------------------------------------

    loop {
        if shutdown.load(Ordering::Relaxed) {
            let _ = trade_log.flush();
            info!("=======================================================");
            info!("  SHUTDOWN — pairs={} cum_pnl=${:+.2}", total_pairs, cum_pnl);
            for (slug, book) in &arb_books {
                if book.yes_fills > 0 || book.no_fills > 0 {
                    info!("  {} {}", slug, book.status());
                }
            }
            info!("=======================================================");
            break;
        }

        let sleep_ms = if pending_order.is_some() {
            cfg.scan.tick_ms      // faster ticks when order pending
        } else {
            cfg.scan.tick_ms * 2  // slower when scanning
        };
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        tick_count += 1;

        let now = now_secs();
        let now_u = now as u64;

        // -- Periodic market rediscovery (every 60s) --------------------------

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
                                token_ids.insert(meta.token_yes.clone(), ());
                                token_ids.insert(meta.token_no.clone(), ());
                                markets.insert(slug, meta);
                            }
                        }
                    }
                }
            }
        }

        // -- Batch book refresh (every 2s) ------------------------------------

        if tick_count % 4 == 0 {
            let all_tokens: Vec<String> = token_ids.iter().map(|e| e.key().clone()).collect();
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

        // -- Check pending maker order ----------------------------------------

        if let Some(ref po) = pending_order {
            let elapsed = now - po.posted_at;
            let book_ask = book_state.get(&po.token_id).map(|b| b.best_ask).unwrap_or(0.0);

            // Paper mode fill simulation:
            // - Taker: fills immediately when book has liquidity (crosses spread)
            // - Maker: fills when ask drops TO our limit (real selling pressure)
            let filled = if po.is_taker {
                elapsed >= 0.5 && book_ask > 0.0
            } else {
                elapsed >= 1.0 && book_ask > 0.0 && book_ask <= po.price
            };

            // Shorter timeout in lockdown to retry completion faster
            // (90s lockdown / 10s timeout = 9 retries vs 3 with 30s)
            let in_lockdown = arb_books.get(&po.slug)
                .map(|b| b.phase == ArbPhase::Lockdown)
                .unwrap_or(false);
            let timeout = if in_lockdown {
                10.0_f64.min(arb_cfg.maker_timeout_secs)
            } else {
                arb_cfg.maker_timeout_secs
            };
            let timed_out = elapsed >= timeout;

            if filled {
                let po = pending_order.take().unwrap();
                info!("[ARB] FILL {} {} @{:.3} ({:.0} shares ${:.2}) — {:.1}s",
                    po.side, po.slug, po.price, po.shares, po.cost, elapsed);

                if let Some(book) = arb_books.get_mut(&po.slug) {
                    book.record_fill(po.side, po.price, po.shares, po.cost);

                    log_arb(&mut trade_log, &ArbTradeLog {
                        event: "FILL".into(), ts: now,
                        slug: po.slug.clone(), asset: book.asset.clone(), tf: book.tf,
                        side: Some(po.side.to_string()), price: Some(po.price),
                        shares: Some(po.shares), cost: Some(po.cost),
                        yes_avg: Some(book.yes_avg()), no_avg: Some(book.no_avg()),
                        pair_cost: if book.pair_cost() > 0.0 { Some(book.pair_cost()) } else { None },
                        pairs_complete: Some(book.pairs_complete()),
                        net_at_risk: Some(book.net_at_risk()),
                        locked_profit: Some(book.locked_profit()),
                        pnl: None, cl_delta_pct: None, phase: None,
                    });
                }
            } else if timed_out {
                let po = pending_order.take().unwrap();
                info!("[ARB] TIMEOUT {} {} @{:.3} — cancelled after {:.0}s (ask was {:.3})",
                    po.side, po.slug, po.price, elapsed, book_ask);

                log_arb(&mut trade_log, &ArbTradeLog {
                    event: "CANCEL".into(), ts: now,
                    slug: po.slug.clone(), asset: String::new(), tf: 0,
                    side: Some(po.side.to_string()), price: Some(po.price),
                    shares: None, cost: None, yes_avg: None, no_avg: None,
                    pair_cost: None, pairs_complete: None, net_at_risk: None,
                    locked_profit: None, pnl: None, cl_delta_pct: None, phase: None,
                });
            }
        }

        // -- Process each market window ---------------------------------------

        let live_since = book_live.load(Ordering::Relaxed);
        let book_ready = live_since > 0 && now_u.saturating_sub(live_since) >= cfg.feed.book_warmup_secs;

        let slugs: Vec<String> = markets.keys().cloned().collect();
        for slug in &slugs {
            let meta = match markets.get(slug) {
                Some(m) => m,
                None => continue,
            };

            if settled.get(slug.as_str()).copied().unwrap_or(false) {
                continue;
            }

            // -- Ensure ArbBook exists for this window ------------------------

            if !arb_books.contains_key(slug) {
                arb_books.insert(slug.clone(), ArbBook::new(
                    slug.clone(), meta.asset.clone(), meta.tf,
                    meta.window_start, meta.window_end,
                    meta.token_yes.clone(), meta.token_no.clone(),
                ));
            }
            let book = arb_books.get_mut(slug).unwrap();

            // -- Capture CL open price ----------------------------------------

            if book.cl_open <= 0.0 {
                if now < meta.window_start as f64 { continue; }
                let delay = now - meta.window_start as f64;
                if delay > max_open_delay {
                    if let Some(px) = cl_at(&cl_snapshots, &meta.asset, meta.window_start as i64) {
                        if px > 0.0 {
                            book.cl_open = px;
                            cl_opens.insert(slug.clone(), px);
                        }
                    } else if let Some(cl_entry) = cl_prices.get(&meta.asset) {
                        if cl_entry.1 > 0.0 {
                            book.cl_open = cl_entry.1;
                            cl_opens.insert(slug.clone(), cl_entry.1);
                        }
                    }
                } else if let Some(px) = cl_at(&cl_snapshots, &meta.asset, meta.window_start as i64) {
                    if px > 0.0 {
                        book.cl_open = px;
                        cl_opens.insert(slug.clone(), px);
                        info!("[OPEN] {} tf={}m open={:.2} delay={:.1}s",
                            slug, meta.tf, px, delay);
                    }
                } else if let Some(cl_entry) = cl_prices.get(&meta.asset) {
                    let (cl_ts, cl_price) = *cl_entry;
                    if cl_ts >= meta.window_start as f64 && cl_price > 0.0 {
                        book.cl_open = cl_price;
                        cl_opens.insert(slug.clone(), cl_price);
                        info!("[OPEN] {} tf={}m open={:.2} delay={:.1}s (live)",
                            slug, meta.tf, cl_price, delay);
                    }
                }
                if book.cl_open <= 0.0 { continue; }
            }

            // -- Update phase -------------------------------------------------

            book.update_phase(now, &arb_cfg);

            // -- Trend detection ----------------------------------------------

            if let Some(cl_entry) = cl_prices.get(&meta.asset) {
                book.check_trend(cl_entry.1, &arb_cfg);
            }

            // -- Settlement ---------------------------------------------------

            if book.phase == ArbPhase::Settled {
                if !settled.get(slug.as_str()).copied().unwrap_or(false) {
                    let s = book.settle();

                    if s.pairs_complete > 0 || s.unmatched_yes_shares > 0.0 || s.unmatched_no_shares > 0.0 {
                        // Determine winner from CLOB bids
                        let bk_yes = book_state.get(&book.token_yes);
                        let bk_no = book_state.get(&book.token_no);
                        let winner = match (&bk_yes, &bk_no) {
                            (Some(y), _) if y.best_bid > 0.80 => "UP",
                            (_, Some(n)) if n.best_bid > 0.80 => "DOWN",
                            _ => {
                                // CL fallback
                                let cl_close = cl_at(&cl_snapshots, &meta.asset, meta.window_end as i64)
                                    .or_else(|| cl_prices.get(&meta.asset).map(|e| e.1))
                                    .unwrap_or(0.0);
                                if book.cl_open > 0.0 && cl_close > 0.0 {
                                    if cl_close >= book.cl_open { "UP" } else { "DOWN" }
                                } else {
                                    "UNKNOWN"
                                }
                            }
                        };

                        let pnl = if winner != "UNKNOWN" { s.final_pnl(winner) } else { s.matched_pnl };

                        info!("=======================================================");
                        info!("  [ARB] SETTLE {} winner={}", slug, winner);
                        info!("  pairs={} matched={:.2} matched_pnl=${:+.4}",
                            s.pairs_complete, s.matched_shares, s.matched_pnl);
                        if s.unmatched_yes_shares > 0.01 {
                            info!("  unmatched_yes={:.2} cost=${:.2}", s.unmatched_yes_shares, s.unmatched_yes_cost);
                        }
                        if s.unmatched_no_shares > 0.01 {
                            info!("  unmatched_no={:.2} cost=${:.2}", s.unmatched_no_shares, s.unmatched_no_cost);
                        }
                        info!("  pair_cost={:.4} total_cost=${:.2} P&L=${:+.4}",
                            s.pair_cost, s.total_cost, pnl);
                        info!("=======================================================");

                        cum_pnl += pnl;
                        total_pairs += s.pairs_complete;

                        log_arb(&mut trade_log, &ArbTradeLog {
                            event: "SETTLE".into(), ts: now,
                            slug: slug.clone(), asset: s.asset.clone(), tf: s.tf,
                            side: None, price: None, shares: Some(s.matched_shares),
                            cost: Some(s.total_cost),
                            yes_avg: Some(s.yes_avg), no_avg: Some(s.no_avg),
                            pair_cost: Some(s.pair_cost),
                            pairs_complete: Some(s.pairs_complete),
                            net_at_risk: None, locked_profit: None,
                            pnl: Some(pnl), cl_delta_pct: None,
                            phase: Some(format!("winner={}", winner)),
                        });
                    }

                    settled.insert(slug.clone(), true);
                }
                continue;
            }

            if !book_ready { continue; }
        }

        // -- Find best entry opportunity (if no pending order) ----------------

        if pending_order.is_none() {
            let mut best: Option<(String, Side, f64, f64)> = None; // (slug, side, ask, priority)

            for (slug, book) in &arb_books {
                if book.phase != ArbPhase::Active && book.phase != ArbPhase::Lockdown {
                    continue;
                }

                let yes_ask = book_state.get(&book.token_yes).map(|b| b.best_ask).unwrap_or(0.0);
                let no_ask = book_state.get(&book.token_no).map(|b| b.best_ask).unwrap_or(0.0);

                let allowed = book.allowed_sides();

                for side in allowed {
                    let ask = match side {
                        Side::Yes => yes_ask,
                        Side::No  => no_ask,
                    };

                    if ask <= 0.0 { continue; }
                    if !book.can_buy(side, ask, &arb_cfg) { continue; }

                    // SAFETY GUARDS for new pairs (balanced fills → starting fresh)
                    if !book.has_unmatched() {
                        // Don't start new pairs too close to lockdown —
                        // need time for BOTH sides to fill before lockdown
                        let remaining = book.window_end as f64 - now;
                        let lockdown_secs = if book.tf == 60 {
                            arb_cfg.lockdown_secs_60m
                        } else {
                            arb_cfg.lockdown_secs_15m
                        };
                        if remaining < lockdown_secs + arb_cfg.maker_timeout_secs * 2.0 {
                            continue;
                        }
                        // Pre-flight: other side must also have a tradeable ask
                        let other_ask = match side {
                            Side::Yes => no_ask,
                            Side::No => yes_ask,
                        };
                        if other_ask <= 0.0
                            || other_ask > arb_cfg.max_ask
                            || other_ask < arb_cfg.min_ask
                        {
                            continue;
                        }
                        // Rough pair cost must be feasible (account for maker rebate)
                        if ask + other_ask > arb_cfg.max_pair_cost + 0.02 {
                            continue;
                        }
                    }

                    // Project pair cost for ranking
                    let new_shares = arb_cfg.unit_size / ask;
                    let projected = match side {
                        Side::Yes => {
                            let new_yes_avg = (book.yes_cost + arb_cfg.unit_size) / (book.yes_shares + new_shares);
                            if book.no_shares > 0.0 {
                                new_yes_avg + book.no_avg()
                            } else {
                                new_yes_avg + no_ask.max(arb_cfg.min_ask)
                            }
                        }
                        Side::No => {
                            let new_no_avg = (book.no_cost + arb_cfg.unit_size) / (book.no_shares + new_shares);
                            if book.yes_shares > 0.0 {
                                book.yes_avg() + new_no_avg
                            } else {
                                yes_ask.max(arb_cfg.min_ask) + new_no_avg
                            }
                        }
                    };

                    // Prioritize: lowest projected pair cost wins
                    // In lockdown, strongly prefer completing an unmatched pair
                    let priority = if book.phase == ArbPhase::Lockdown && book.has_unmatched() {
                        projected - 1.0 // heavy priority boost
                    } else {
                        projected
                    };

                    if best.is_none() || priority < best.as_ref().unwrap().3 {
                        best = Some((slug.clone(), side, ask, priority));
                    }
                }
            }

            // Post order for best opportunity (maker or taker)
            if let Some((slug, side, ask, _priority)) = best {
                let book = arb_books.get(&slug).unwrap();

                // Decide maker vs taker:
                // Taker mode in late lockdown (last 1/3) when completing unmatched pair
                let remaining = book.window_end as f64 - now;
                let lockdown_secs = if book.tf == 60 {
                    arb_cfg.lockdown_secs_60m
                } else {
                    arb_cfg.lockdown_secs_15m
                };
                let use_taker = book.phase == ArbPhase::Lockdown
                    && book.has_unmatched()
                    && remaining < lockdown_secs / 3.0;

                let (limit_px, shares, is_taker) = if use_taker {
                    // Taker: buy at ask, pay PM fee, get fewer shares
                    let fee = pm_fee(ask);
                    let effective = ask + fee;
                    (ask, arb_cfg.unit_size / effective, true)
                } else {
                    // Maker: bid at ask-0.01, no fee
                    let lp = maker_limit_price(ask);
                    (lp, arb_cfg.unit_size / lp, false)
                };

                // Skip if price falls below minimum
                if limit_px >= arb_cfg.min_ask {
                    let token_id = match side {
                        Side::Yes => book.token_yes.clone(),
                        Side::No  => book.token_no.clone(),
                    };

                    let mode = if is_taker { "TAKER" } else { "MAKER" };
                    info!("[ARB] POST {} {} {} @{:.3} (ask={:.3}) {:.0} shares ${:.2} | {}",
                        mode, side, slug, limit_px, ask, shares, arb_cfg.unit_size,
                        book.status());

                    // Place live order if wallet configured
                    if let Some(ref clob) = clob_client {
                        let c = clob.clone();
                        let tid = token_id.clone();
                        let px = limit_px;
                        let sz = shares;
                        let s = slug.clone();
                        tokio::spawn(async move {
                            match c.place_limit_order(&tid, px, sz, "BUY").await {
                                Ok(resp) => info!("[CLOB] BUY placed for {}: {:?}", s, resp),
                                Err(e) => warn!("[CLOB] BUY failed for {} (px={} sz={}): {:#}", s, px, sz, e),
                            }
                        });
                    }

                    pending_order = Some(PendingOrder {
                        slug: slug.clone(),
                        side,
                        token_id,
                        price: limit_px,
                        shares,
                        cost: arb_cfg.unit_size,
                        posted_at: now,
                        is_taker,
                    });

                    log_arb(&mut trade_log, &ArbTradeLog {
                        event: "POST".into(), ts: now,
                        slug, asset: book.asset.clone(), tf: book.tf,
                        side: Some(side.to_string()), price: Some(limit_px),
                        shares: Some(shares), cost: Some(arb_cfg.unit_size),
                        yes_avg: None, no_avg: None, pair_cost: None,
                        pairs_complete: None, net_at_risk: None,
                        locked_profit: None, pnl: None, cl_delta_pct: None,
                        phase: Some(format!("{:?}{}", book.phase, if is_taker { " TAKER" } else { "" })),
                    });
                }
            }
        }

        // -- Flush logs -------------------------------------------------------

        if tick_count % 20 == 0 {
            let _ = trade_log.flush();
        }

        // -- Status every 60s -------------------------------------------------

        if last_stats.elapsed().as_secs() >= 60 {
            let px: Vec<String> = cfg.feed.assets.iter()
                .filter_map(|a| cl_prices.get(a.as_str()).map(|e| format!("{}=${:.0}", a.to_uppercase(), e.1)))
                .collect();
            let hrs = start_time.elapsed().as_secs_f64() / 3600.0;

            let active_windows: Vec<String> = arb_books.values()
                .filter(|b| b.phase == ArbPhase::Active || b.phase == ArbPhase::Lockdown)
                .map(|b| b.status())
                .collect();

            let pending_str = match &pending_order {
                Some(po) => format!("pending={} {} @{:.3}", po.side, po.slug, po.price),
                None => "no pending".into(),
            };

            info!("-- {:.1}h | {} | pairs={} pnl=${:+.2} | {} | {} --",
                hrs, px.join(" "), total_pairs, cum_pnl, pending_str,
                if active_windows.is_empty() { "no active".to_string() }
                else { format!("{} active", active_windows.len()) });

            for w in &active_windows {
                info!("  {}", w);
            }

            last_stats = Instant::now();
        }

        // -- Cleanup stale data (every 60s) -----------------------------------

        if tick_count % 120 == 0 {
            let cutoff = now as i64 - 7200;
            cl_opens.retain(|k, _| slug_ts(k) > cutoff);
            // Protect arb_books with unmatched positions from cleanup
            arb_books.retain(|k, b| slug_ts(k) > cutoff || b.has_unmatched());
            let stale: Vec<String> = markets.keys()
                .filter(|k| slug_ts(k) < cutoff)
                .cloned()
                .collect();
            // Collect stale token IDs before removing markets
            for k in &stale {
                if let Some(meta) = markets.get(k) {
                    token_ids.remove(&meta.token_yes);
                    token_ids.remove(&meta.token_no);
                }
                markets.remove(k);
                settled.remove(k);
            }
        }
    }

    Ok(())
}
