/// Lag Sniper — 14th March 2026
///
/// BN-leading, CL-lagging, maker-first lag exploitation bot.
///
/// Architecture:
///   3 parallel WS feeds: CL (chainlink), BN (binance aggTrade), PM book (REST)
///   Lag detector: computes fair value from BN (truth) vs CL (market view)
///   Runner: maker-first entry when lag detected, convergence-based exit
///
/// Flow:
///   1. BN moves → fair_bn diverges from fair_cl
///   2. PM book is priced off stale CL → stale asks
///   3. Lag detector fires → maker bid inside spread
///   4. MM cancel-replace cycle takes 200ms+ → we get filled
///   5. CL catches up → book reprices → we sell at new bid
///
/// All entries maker (0% fee). Taker only for emergency SL.

mod feeds;
mod signal;
mod execution;
mod lag;
mod runner;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use dashmap::DashMap;
use reqwest::Client;
use serde::Deserialize;
use tracing::{info, warn, error};
use tracing_subscriber::EnvFilter;

use feeds::{
    BnPrices, BnTradeFlow, BookState, ClCadence, ClPrices, MarketMeta, PriceHistory, RateLimiter,
    build_slug, current_window_starts, fetch_books_batch, fetch_market_meta,
    run_cl_feed, run_bn_feed, momentum, bn_flow_imbalance, cl_cadence_info,
};
use signal::estimate_sigma;
use lag::{LagDetector, LagConfig};
use runner::{LagRunner, RunnerConfig};

// ── Config ──────────────────────────────────────────────────────────────────

#[derive(Deserialize, Debug)]
struct AppConfig {
    wallet:   WalletConfig,
    feed:     FeedConfig,
    scan:     ScanConfig,
    lag:      LagConfigFile,
    strategy: StrategyConfigFile,
}

#[derive(Deserialize, Debug)]
struct WalletConfig {
    private_key:    String,
    funder_address: String,
}

#[derive(Deserialize, Debug)]
struct FeedConfig {
    assets:           Vec<String>,
    timeframes:       Vec<u32>,
    clob_rest:        String,
    live_ws:          String,
    gamma_api:        String,
    rest_throttle_ms: u64,
    book_warmup_secs: u64,
    book_stale_secs:  f64,
}

#[derive(Deserialize, Debug)]
struct ScanConfig {
    tick_ms:            u64,
    sigma_window_secs:  f64,
    min_secs:           f64,
}

#[derive(Deserialize, Debug)]
struct LagConfigFile {
    min_divergence_pct:  f64,
    min_edge:            f64,
    min_fair_gap:        f64,
    min_bn_momentum:     f64,
    min_secs_left:       f64,
    max_secs_left:       f64,
    max_sigma:           f64,
    min_depth_multiple:  f64,
    min_entry_price:     f64,
    max_entry_price:     f64,
    min_bn_flow_confirm: f64,
}

/// Override stale config values with tuned minimums
fn enforce_lag_floor(cfg: &mut LagConfigFile) {
    if cfg.min_divergence_pct < 0.035 { cfg.min_divergence_pct = 0.035; }
    if cfg.min_edge            < 0.08  { cfg.min_edge            = 0.08;  }
    if cfg.min_fair_gap        < 0.05  { cfg.min_fair_gap        = 0.05;  }
    if cfg.min_bn_momentum     < 0.0003 { cfg.min_bn_momentum   = 0.0003; }
    if cfg.min_secs_left       < 90.0  { cfg.min_secs_left       = 90.0;  }
    if cfg.max_secs_left       > 600.0 { cfg.max_secs_left       = 600.0; }
    if cfg.min_entry_price     < 0.12  { cfg.min_entry_price     = 0.12;  }
    if cfg.max_entry_price     > 0.88  { cfg.max_entry_price     = 0.88;  }
}

#[derive(Deserialize, Debug)]
struct StrategyConfigFile {
    stake:              f64,
    maker_chase_ticks:  u32,
    chase_interval_ms:  u64,
    max_concurrent:     usize,
    max_hold_secs:      f64,
    min_profit:         f64,
    partial_tp_pct:     f64,
    taker_fee_rate:     f64,
    max_drawdown:       f64,
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn now_secs() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64()
}

fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

// ── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("lag_sniper=info".parse()?)
                .add_directive("polymarket_client_sdk=warn".parse()?)
        )
        .with_target(false)
        .init();

    let cfg_text = std::fs::read_to_string("config.toml")
        .context("cannot read config.toml")?;
    let mut cfg: AppConfig = toml::from_str(&cfg_text)
        .context("invalid config.toml")?;
    enforce_lag_floor(&mut cfg.lag);

    info!("═══════════════════════════════════════════════════════════");
    info!("  LAG SNIPER — BN-Leading Maker-First — 14th March 2026");
    info!("═══════════════════════════════════════════════════════════");
    info!("Assets: {:?}", cfg.feed.assets);
    info!("Timeframes: {:?}m", cfg.feed.timeframes);
    info!("Lag: div>={:.3}% edge>={:.2} gap>={:.2} momentum>={:.4}",
        cfg.lag.min_divergence_pct, cfg.lag.min_edge, cfg.lag.min_fair_gap,
        cfg.lag.min_bn_momentum);
    info!("Strategy: stake=${} chase={}ticks hold<{}s profit>={:.2} max_pos={} max_dd=${}",
        cfg.strategy.stake, cfg.strategy.maker_chase_ticks,
        cfg.strategy.max_hold_secs, cfg.strategy.min_profit,
        cfg.strategy.max_concurrent, cfg.strategy.max_drawdown);

    // ── Wallet keys ─────────────────────────────────────────────────────────

    let private_key = if cfg.wallet.private_key.is_empty() {
        std::env::var("POLY_PRIVATE_KEY")
            .context("private_key empty in config.toml and POLY_PRIVATE_KEY env var not set")?
    } else {
        cfg.wallet.private_key.clone()
    };
    let funder_address = if cfg.wallet.funder_address.is_empty() {
        std::env::var("POLY_FUNDER_ADDRESS").unwrap_or_default()
    } else {
        cfg.wallet.funder_address.clone()
    };

    std::fs::create_dir_all("logs").context("cannot create logs/")?;

    // ── Initialize execution layer ──────────────────────────────────────────

    info!("Initializing execution layer...");
    let exec = execution::ExecutionLayer::new(
        &private_key,
        &funder_address,
        &cfg.feed.clob_rest,
    ).await.context("Execution layer init failed")?;
    let exec = Arc::new(exec);

    // Cancel stale orders on startup
    info!("Cancelling stale orders...");
    match exec.cancel_all().await {
        Ok(_)  => info!("[EXEC] Stale orders cancelled"),
        Err(e) => warn!("[EXEC] Cancel all on startup: {}", e),
    }

    // Balance check
    match exec.check_balance().await {
        Ok(bal) => {
            info!("[EXEC] USDC balance: ${:.2}", bal);
            if bal < cfg.strategy.stake {
                warn!("[EXEC] Balance ${:.2} < stake ${:.2}!", bal, cfg.strategy.stake);
            }
        }
        Err(e) => warn!("[EXEC] Balance check failed: {}", e),
    }

    // ── Shared state ────────────────────────────────────────────────────────

    let cl_prices:     ClPrices     = Arc::new(DashMap::new());
    let bn_prices:     BnPrices     = Arc::new(DashMap::new());
    let book_state:    BookState    = Arc::new(DashMap::new());
    let price_history: PriceHistory = Arc::new(DashMap::new());
    let bn_trades:     BnTradeFlow  = Arc::new(DashMap::new());
    let cl_cadence:    ClCadence    = Arc::new(DashMap::new());
    let token_ids:     Arc<DashMap<String, ()>> = Arc::new(DashMap::new());

    let http = Client::builder()
        .user_agent("lag-sniper/1")
        .timeout(Duration::from_secs(10))
        .build()
        .context("HTTP client build failed")?;

    let limiter = Arc::new(RateLimiter::new(cfg.feed.rest_throttle_ms));

    // ── Discover markets ────────────────────────────────────────────────────

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
                        info!("[DISCOVER] {}", slug);
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

    // ── Start feeds ─────────────────────────────────────────────────────────

    // CL WebSocket
    {
        let cp = cl_prices.clone();
        let ph = price_history.clone();
        let cc = cl_cadence.clone();
        let ws = cfg.feed.live_ws.clone();
        tokio::spawn(async move {
            run_cl_feed(ws, cp, ph, cc).await;
        });
    }

    // BN WebSocket
    {
        let bp = bn_prices.clone();
        let bh = price_history.clone();
        let bt = bn_trades.clone();
        let assets = cfg.feed.assets.clone();
        tokio::spawn(async move {
            run_bn_feed(assets, bp, bh, bt).await;
        });
    }

    // ── Build lag detector ──────────────────────────────────────────────────

    let lag_config = LagConfig {
        min_divergence_pct:  cfg.lag.min_divergence_pct,
        min_edge:            cfg.lag.min_edge,
        min_fair_gap:        cfg.lag.min_fair_gap,
        min_bn_momentum:     cfg.lag.min_bn_momentum,
        min_secs_left:       cfg.lag.min_secs_left,
        max_secs_left:       cfg.lag.max_secs_left,
        max_sigma:           cfg.lag.max_sigma,
        min_depth_multiple:  cfg.lag.min_depth_multiple,
        min_entry_price:     cfg.lag.min_entry_price,
        max_entry_price:     cfg.lag.max_entry_price,
        stake:               cfg.strategy.stake,
        min_bn_flow_confirm: cfg.lag.min_bn_flow_confirm,
    };
    let detector = LagDetector::new(lag_config);

    // ── Build runner ────────────────────────────────────────────────────────

    let runner_config = RunnerConfig {
        stake:              cfg.strategy.stake,
        maker_chase_ticks:  cfg.strategy.maker_chase_ticks,
        chase_interval_ms:  cfg.strategy.chase_interval_ms,
        max_concurrent:     cfg.strategy.max_concurrent,
        max_hold_secs:      cfg.strategy.max_hold_secs,
        min_profit:         cfg.strategy.min_profit,
        partial_tp_pct:     cfg.strategy.partial_tp_pct,
        taker_fee_rate:     cfg.strategy.taker_fee_rate,
        max_drawdown:       cfg.strategy.max_drawdown,
    };
    let mut runner = LagRunner::new(runner_config, exec.clone(), "logs");

    // ── Main scan loop ──────────────────────────────────────────────────────

    let tick = Duration::from_millis(cfg.scan.tick_ms);
    let mut tick_count:    u64 = 0;
    let mut last_stats_ts: f64 = now_secs();
    let mut last_discover: f64 = now_secs();
    let mut settled: HashMap<String, bool> = HashMap::new();
    let mut cl_close_snap: HashMap<String, f64> = HashMap::new();

    // ── Graceful shutdown ───────────────────────────────────────────────────
    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let shutdown = shutdown.clone();
        let exec = exec.clone();
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                let mut sigterm = tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::terminate()
                ).expect("failed to register SIGTERM handler");

                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        warn!("SIGINT — cancelling all orders...");
                    }
                    _ = sigterm.recv() => {
                        warn!("SIGTERM — cancelling all orders...");
                    }
                }
            }

            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
                warn!("SIGINT — cancelling all orders...");
            }

            shutdown.store(true, Ordering::SeqCst);
            match exec.cancel_all().await {
                Ok(_)  => info!("[SHUTDOWN] All orders cancelled"),
                Err(e) => error!("[SHUTDOWN] Cancel all failed: {}", e),
            }
        });
    }

    // ── Feed warmup ─────────────────────────────────────────────────────────
    info!("Waiting for CL + BN feeds...");
    for _ in 0..40u32 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let cl_ok = cfg.feed.assets.iter().all(|a| cl_prices.contains_key(a.as_str()));
        let bn_ok = cfg.feed.assets.iter().all(|a| bn_prices.contains_key(a.as_str()));
        if cl_ok && bn_ok {
            for asset in &cfg.feed.assets {
                let cl = cl_prices.get(asset.as_str()).map(|v| v.1).unwrap_or(0.0);
                let bn = bn_prices.get(asset.as_str()).map(|v| v.1).unwrap_or(0.0);
                info!("[FEEDS] {} CL={:.2} BN={:.2} div={:.4}%",
                    asset.to_uppercase(), cl, bn,
                    if cl > 0.0 { ((bn - cl) / cl).abs() * 100.0 } else { 0.0 });
            }
            break;
        }
    }

    info!("Starting scan loop ({}ms tick)...", cfg.scan.tick_ms);

    loop {
        tokio::time::sleep(tick).await;

        if shutdown.load(Ordering::SeqCst) {
            info!("Shutdown — exiting");
            runner.print_stats();
            break;
        }

        tick_count += 1;
        let now = now_secs();
        let now_u = now as u64;

        // ── Periodic market rediscovery (every 60s) ─────────────────────

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
                                info!("[DISCOVER] New: {}", slug);
                                token_ids.insert(meta.token_yes.clone(), ());
                                token_ids.insert(meta.token_no.clone(), ());
                                markets.insert(slug, meta);
                            }
                        }
                    }
                }
            }
        }

        // ── Batch book refresh (every tick — 250ms for fresher quotes) ──

        {
            let all_tokens: Vec<String> = token_ids.iter().map(|e| e.key().clone()).collect();
            if !all_tokens.is_empty() {
                match fetch_books_batch(&http, &cfg.feed.clob_rest, &all_tokens, &limiter).await {
                    Ok(books) => {
                        for (tid, entry) in books {
                            book_state.insert(tid, entry);
                        }
                    }
                    Err(e) => warn!("[BOOK] batch failed: {}", e),
                }
            }
        }

        // ── CL close snap for settlement ────────────────────────────────

        for (slug, meta) in &markets {
            if settled.get(slug.as_str()).copied().unwrap_or(false) { continue; }
            if cl_close_snap.contains_key(slug.as_str()) { continue; }
            if now_u > meta.window_end {
                let cl_now = cl_prices.get(&meta.asset).map(|v| v.1).unwrap_or(0.0);
                if cl_now > 0.0 {
                    cl_close_snap.insert(slug.clone(), cl_now);
                    info!("[CL_CLOSE] {} snap={:.2}", slug, cl_now);
                }
            }
        }

        // ── Settlements ─────────────────────────────────────────────────

        let mut newly_settled: Vec<String> = Vec::new();
        for (slug, meta) in &markets {
            if settled.get(slug.as_str()).copied().unwrap_or(false) { continue; }
            if now_u >= meta.window_end + 5 {
                let cl_settle = match cl_close_snap.get(slug.as_str()) {
                    Some(&p) => p,
                    None => continue,
                };
                if cl_settle <= 0.0 || meta.open_price <= 0.0 { continue; }

                let outcome = if cl_settle > meta.open_price { 1.0 } else { 0.0 };
                info!("[SETTLE] {} outcome={}", slug, if outcome == 1.0 { "YES" } else { "NO" });

                runner.on_settlement(slug, outcome, now).await;
                settled.insert(slug.clone(), true);
                newly_settled.push(slug.clone());
            }
        }
        // Clean up settled markets to prevent unbounded memory growth
        for slug in &newly_settled {
            if let Some(meta) = markets.remove(slug) {
                token_ids.remove(&meta.token_yes);
                token_ids.remove(&meta.token_no);
                book_state.remove(&meta.token_yes);
                book_state.remove(&meta.token_no);
            }
            cl_close_snap.remove(slug.as_str());
        }

        // ── Warmup gate ─────────────────────────────────────────────────

        if tick_count < (cfg.feed.book_warmup_secs * 1000 / cfg.scan.tick_ms) {
            continue;
        }

        // ── Lag detection + runner dispatch ─────────────────────────────

        for (slug, meta) in &mut markets {
            if settled.get(slug.as_str()).copied().unwrap_or(false) { continue; }

            let secs_left = meta.window_end as f64 - now;
            if secs_left < cfg.scan.min_secs { continue; }

            // Get CL price + timestamp
            let (cl_ts, cl_price) = match cl_prices.get(&meta.asset) {
                Some(v) => (v.0, v.1),
                None => continue,
            };

            // Get BN price + timestamp
            let (bn_ts, bn_price) = match bn_prices.get(&meta.asset) {
                Some(v) => (v.0, v.1),
                None => continue,
            };

            // Set open price at window start
            if meta.open_price <= 0.0 {
                if now_u >= meta.window_start {
                    meta.open_price = cl_price;
                    info!("[OPEN] {} open={:.2}", slug, cl_price);
                }
                continue;
            }

            // Get books
            let book_yes = match book_state.get(&meta.token_yes) {
                Some(b) if b.best_ask > 0.0 => b.clone(),
                _ => continue,
            };
            let book_no = match book_state.get(&meta.token_no) {
                Some(b) if b.best_ask > 0.0 => b.clone(),
                _ => continue,
            };

            // Skip stale books
            if now - book_yes.ts > cfg.feed.book_stale_secs
                || now - book_no.ts > cfg.feed.book_stale_secs {
                continue;
            }

            // BN 5s momentum (compute first — used for adaptive sigma)
            let bn_mom = {
                let hist_key = format!("bn_{}", meta.asset);
                let hist = price_history.get(&hist_key);
                match hist {
                    Some(h) => momentum(&h, now, 5.0).0,
                    None => 0.0,
                }
            };

            // Adaptive sigma: use 60s window in fast moves, 300s otherwise
            // Fast moves need responsive vol; slow markets need stable vol
            let sigma = {
                let hist_key = format!("bn_{}", meta.asset);
                let hist = price_history.get(&hist_key);
                let window = if bn_mom.abs() > 0.001 { 60.0 } else { cfg.scan.sigma_window_secs };
                match hist {
                    Some(h) => estimate_sigma(&h, window, now),
                    None => 0.50,
                }
            };

            // BN trade flow imbalance (last 5s)
            let bn_flow = {
                let trades = bn_trades.get(&meta.asset);
                match trades {
                    Some(t) => bn_flow_imbalance(&t, now, 5.0).2,
                    None => 0.0,
                }
            };

            // ── Check exits first ───────────────────────────────────────
            runner.check_exits(
                slug,
                bn_price, cl_price, meta.open_price,
                sigma, secs_left, bn_mom,
                book_yes.best_bid, book_no.best_bid,
                now,
            ).await;

            // ── CL cadence info ────────────────────────────────────────
            let cl_cad = {
                let cad = cl_cadence.get(&meta.asset);
                match cad {
                    Some(c) => cl_cadence_info(&c, now),
                    None => (0.0, 20.0),
                }
            };

            // ── Lag detection ───────────────────────────────────────────
            if let Some(lag_sig) = detector.detect(
                slug, &meta.asset, meta.tf,
                meta.open_price, secs_left, sigma,
                bn_price, bn_ts,
                cl_price, cl_ts,
                bn_mom, bn_flow,
                &book_yes, &book_no,
                now,
                cl_cad,
            ) {
                runner.on_lag_signal(
                    &lag_sig, meta.window_end,
                    &meta.token_yes, &meta.token_no,
                ).await;
            }
        }

        // ── DD kill switch → full shutdown when positions drained ────────

        if runner.is_dd_halted() && runner.open_count() == 0 {
            warn!("[MAIN] DD halt + no open positions → shutting down");
            runner.print_stats();
            break;
        }

        // ── Stats (every 30s) ───────────────────────────────────────────

        if now - last_stats_ts >= 30.0 {
            last_stats_ts = now;

            // Quick feed status
            for asset in &cfg.feed.assets {
                let cl = cl_prices.get(asset.as_str()).map(|v| (now - v.0, v.1));
                let bn = bn_prices.get(asset.as_str()).map(|v| (now - v.0, v.1));
                if let (Some((cl_age, cl_p)), Some((bn_age, bn_p))) = (cl, bn) {
                    let div = if cl_p > 0.0 { ((bn_p - cl_p) / cl_p).abs() * 100.0 } else { 0.0 };
                    info!(
                        "[FEED] {} CL={:.2}({:.1}s) BN={:.2}({:.1}s) div={:.4}%",
                        asset.to_uppercase(), cl_p, cl_age, bn_p, bn_age, div
                    );
                }
            }

            runner.print_stats();
            info!("markets={} settled={} books={}", markets.len(), settled.len(), book_state.len());
        }
    }

    Ok(())
}
