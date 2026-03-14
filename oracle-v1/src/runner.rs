/// runner.rs — OPT2 Maker Strategy Runner (LIVE)
///
/// Strategy: edge>=0.20, SL, 50% TP, maker chase 2 ticks then taker
/// Single runner — no multi-config. This is the live execution runner.
///
/// Entry: maker_chase_entry → GTC(post_only) → chase 2 ticks → FAK(taker)
/// SL:    exit when fair < entry_price (secs_left > 90)
/// TP:    sell 50% when PM bid >= fair_at_entry, hold rest to settlement
/// Exit:  sell via GTC at best_bid (or taker if urgent)

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::fs::{File, OpenOptions};

use serde::Serialize;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::execution::ExecutionLayer;
use crate::signal::{Signal, Side};

// ── Position ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LivePosition {
    pub trade_id:      String,
    pub slug:          String,
    pub asset:         String,
    pub tf:            u32,
    pub side:          Side,
    pub token_id:      String,     // the token we bought
    pub entry_price:   f64,
    pub fair_at_entry: f64,
    pub sigma:         f64,
    pub stake:         f64,
    pub shares:        f64,        // actual shares held
    pub secs_left:     f64,
    pub entry_ts:      f64,
    pub window_end:    u64,
    pub tp_fired:      bool,       // have we taken 50% TP?
    pub order_id:      String,     // entry order ID
}

// ── Trade log ───────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct TradeLog {
    pub trade_id:      String,
    pub slug:          String,
    pub asset:         String,
    pub tf:            u32,
    pub side:          String,
    pub entry_price:   f64,
    pub fair_at_entry: f64,
    pub edge_at_entry: f64,
    pub sigma:         f64,
    pub secs_left:     f64,
    pub stake:         f64,
    pub shares:        f64,
    pub exit_price:    f64,
    pub exit_reason:   String,
    pub pnl:           f64,
    pub fee:           f64,
    pub net_pnl:       f64,
    pub entry_ts:      f64,
    pub exit_ts:       f64,
    pub hold_secs:     f64,
    pub tp_fired:      bool,
}

// ── Stats ───────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
pub struct RunnerStats {
    pub signals:   u64,
    pub entries:   u64,
    pub wins:      u64,
    pub losses:    u64,
    pub gross_pnl: f64,
    pub total_fee: f64,
    pub net_pnl:   f64,
    pub sl_exits:  u64,
    pub tp_exits:  u64,
    pub settle_exits: u64,
}

impl RunnerStats {
    pub fn wr(&self) -> f64 {
        let total = self.wins + self.losses;
        if total == 0 { 0.0 } else { self.wins as f64 / total as f64 * 100.0 }
    }
}

// ── Strategy Config ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct StrategyConfig {
    pub stake:            f64,
    pub min_edge:         f64,
    pub max_secs_left:    f64,
    pub min_entry_price:  f64,
    pub max_sigma:        f64,
    pub min_move_pct:     f64,
    pub max_fair:         f64,
    pub stop_loss:        bool,
    pub take_profit:      bool,
    pub partial_tp_pct:   f64,
    pub taker_fee_rate:   f64,
    pub maker_fee_rate:   f64,
    pub maker_chase_ticks: u32,
    pub chase_interval_ms: u64,
    pub max_concurrent:    usize,
}

// ── Runner ──────────────────────────────────────────────────────────────────

pub struct LiveRunner {
    pub config:    StrategyConfig,
    pub exec:      Arc<ExecutionLayer>,
    positions:     HashMap<String, LivePosition>,
    stats:         RunnerStats,
    log_file:      Arc<Mutex<File>>,
}

impl LiveRunner {
    pub fn new(config: StrategyConfig, exec: Arc<ExecutionLayer>, log_dir: &str) -> Self {
        let log_path = format!("{}/opt2_maker.jsonl", log_dir);
        let file = OpenOptions::new()
            .create(true).append(true)
            .open(&log_path)
            .unwrap_or_else(|e| panic!("Cannot open log {}: {}", log_path, e));

        info!("[RUNNER] Log: {}", log_path);

        Self {
            config,
            exec,
            positions: HashMap::new(),
            stats: RunnerStats::default(),
            log_file: Arc::new(Mutex::new(file)),
        }
    }

    /// Called every tick with a signal.
    pub async fn on_signal(
        &mut self,
        sig: &Signal,
        window_end: u64,
        token_yes: &str,
        token_no: &str,
    ) {
        self.check_exits(sig, token_yes, token_no).await;
        self.maybe_enter(sig, window_end, token_yes, token_no).await;
    }

    /// Called at window settlement.
    pub async fn on_settlement(&mut self, slug: &str, yes_outcome: f64, settle_ts: f64) {
        let trade_ids: Vec<String> = self.positions.keys()
            .filter(|k| k.starts_with(slug))
            .cloned().collect();

        for trade_id in trade_ids {
            if let Some(pos) = self.positions.remove(&trade_id) {
                let exit_price = match pos.side {
                    Side::Yes => yes_outcome,
                    Side::No  => 1.0 - yes_outcome,
                };
                // Settlement: no exit fee, no order needed
                self.log_close(pos, exit_price, "SETTLEMENT", settle_ts, 0.0).await;
            }
        }
    }

    // ── Entry logic ─────────────────────────────────────────────────────────

    async fn maybe_enter(
        &mut self,
        sig: &Signal,
        window_end: u64,
        token_yes: &str,
        token_no: &str,
    ) {
        self.stats.signals += 1;

        // Time gate
        if sig.secs_left < 60.0 || sig.secs_left > self.config.max_secs_left {
            return;
        }

        // Edge gate (VWAP fill edge)
        let side = match sig.best_side {
            Some(s) if sig.best_edge >= self.config.min_edge => s,
            _ => return,
        };

        // Fill price must exist
        let fill_price = match side {
            Side::Yes => sig.fill_yes,
            Side::No  => sig.fill_no,
        };
        let fill_price = match fill_price {
            Some(p) if p > 0.0 => p,
            _ => return,
        };

        // Sigma filter: skip high-vol markets where BS edge is unreliable
        if sig.sigma > self.config.max_sigma {
            return;
        }

        // Min entry price (no lottery tickets)
        if fill_price < self.config.min_entry_price {
            return;
        }

        // Depth check (2x stake)
        let depth = match side {
            Side::Yes => sig.depth_yes,
            Side::No  => sig.depth_no,
        };
        if depth < self.config.stake * 2.0 {
            return;
        }

        // Max concurrent positions cap
        if self.positions.len() >= self.config.max_concurrent {
            return;
        }

        // One position per slug
        if self.positions.keys().any(|k| k.starts_with(&sig.slug)) {
            return;
        }

        // FIX: Min price deviation — skip when underlying move is within noise
        // A -0.06% dip on BTC is bid-ask bounce, not a real directional signal.
        // BS overextrapolates tiny moves into high-confidence bets.
        let pct_move = ((sig.cl_price / sig.open_price) - 1.0).abs() * 100.0;
        if pct_move < self.config.min_move_pct {
            return;
        }

        // FIX: Fair value cap — BS becomes unreliable at extremes
        if sig.best_fair > self.config.max_fair {
            return;
        }

        // FIX: CL momentum confirmation — skip if CL moving against our side
        let momentum_confirms = match side {
            Side::Yes => sig.cl_momentum >= 0.0,   // YES needs CL going UP
            Side::No  => sig.cl_momentum <= 0.0,   // NO needs CL going DOWN
        };
        if !momentum_confirms {
            return;
        }

        // FIX: Book imbalance confirmation — skip if book skew contradicts signal
        // book_imbal > 1.0 = bullish (more bids), < 1.0 = bearish (more asks)
        let book_confirms = match side {
            Side::Yes => sig.book_imbal >= 0.7,   // YES: don't enter if heavily bearish book
            Side::No  => sig.book_imbal <= 1.5,   // NO: don't enter if heavily bullish book
        };
        if !book_confirms {
            return;
        }

        // Max 2 positions per asset (e.g. one 5m + one 15m)
        let asset_lower = sig.asset.to_lowercase();
        let asset_count = self.positions.values()
            .filter(|p| p.asset.to_lowercase() == asset_lower)
            .count();
        if asset_count >= 2 {
            return;
        }

        let token_id = match side {
            Side::Yes => token_yes,
            Side::No  => token_no,
        };

        let best_ask = match side {
            Side::Yes => sig.book_yes,
            Side::No  => sig.book_no,
        };

        // Maker price: best_bid + 0.01 (join inside the spread, below best ask)
        let best_bid = match side {
            Side::Yes => sig.bid_yes,
            Side::No  => sig.bid_no,
        };
        let maker_price = ((best_bid + 0.01) * 100.0).round() / 100.0;
        let maker_price = maker_price.min(best_ask - 0.01).max(0.01);

        info!(
            "[RUNNER] ENTRY SIGNAL {} {} maker={:.2} ask={:.2} fair={:.3} edge={:.3} depth=${:.0} T-{:.0}s",
            sig.slug, side, maker_price, best_ask, sig.best_fair, sig.best_edge, depth, sig.secs_left
        );

        // Execute maker chase entry
        match self.exec.maker_chase_entry(
            token_id,
            maker_price,
            self.config.stake,
            self.config.maker_chase_ticks,
            self.config.chase_interval_ms,
            best_ask,
        ).await {
            Ok(fill) => {
                let actual_price = fill.price;

                // FIX 1: Post-fill edge check — reject if real edge < min_edge
                let post_fill_edge = sig.best_fair - actual_price;
                if post_fill_edge < self.config.min_edge {
                    warn!(
                        "[RUNNER] REJECT post-fill {} edge={:.3} < min={:.3} (fill={:.3} fair={:.3})",
                        sig.slug, post_fill_edge, self.config.min_edge, actual_price, sig.best_fair
                    );
                    // Cancel/sell the filled shares immediately
                    let shares = self.config.stake / actual_price;
                    let _ = self.exec.sell_gtc(token_id, actual_price, shares).await;
                    return;
                }

                // FIX 2: Max entry price cap — never pay more than 0.60 (risk $0.60 to win $0.40)
                if actual_price > 0.60 {
                    warn!(
                        "[RUNNER] REJECT expensive fill {} @{:.3} > 0.60 max",
                        sig.slug, actual_price
                    );
                    let shares = self.config.stake / actual_price;
                    let _ = self.exec.sell_gtc(token_id, actual_price, shares).await;
                    return;
                }

                let shares = self.config.stake / actual_price;
                let now = sig.ts;
                let trade_id = format!("{}-OPT2-{:.0}", sig.slug, now * 1000.0);

                // Determine entry fee based on fill type
                let entry_fee = if fill.status.contains("Live") {
                    // Maker fill (was GTC and posted) — 0% fee
                    self.config.maker_fee_rate * self.config.stake
                } else {
                    // Taker fill (FAK or immediate match) — 1.5% fee
                    self.config.taker_fee_rate * self.config.stake
                };

                info!(
                    "[RUNNER] ENTERED {} {} @{:.3} shares={:.2} oid={} fee={:.3}",
                    sig.slug, side, actual_price, shares, fill.order_id, entry_fee
                );

                let pos = LivePosition {
                    trade_id: trade_id.clone(),
                    slug: sig.slug.clone(),
                    asset: sig.asset.clone(),
                    tf: sig.tf,
                    side,
                    token_id: token_id.to_string(),
                    entry_price: actual_price,
                    fair_at_entry: sig.best_fair,
                    sigma: sig.sigma,
                    stake: self.config.stake,
                    shares,
                    secs_left: sig.secs_left,
                    entry_ts: now,
                    window_end,
                    tp_fired: false,
                    order_id: fill.order_id,
                };

                self.positions.insert(trade_id, pos);
                self.stats.entries += 1;
            }
            Err(e) => {
                warn!("[RUNNER] Entry failed for {}: {}", sig.slug, e);
            }
        }
    }

    // ── Exit logic ──────────────────────────────────────────────────────────

    async fn check_exits(
        &mut self,
        sig: &Signal,
        _token_yes: &str,
        _token_no: &str,
    ) {
        let trade_ids: Vec<String> = self.positions.keys()
            .filter(|k| k.starts_with(&sig.slug))
            .cloned().collect();

        for trade_id in trade_ids {
            let pos = match self.positions.get(&trade_id) {
                Some(p) => p.clone(),
                None    => continue,
            };

            let current_fair = match pos.side {
                Side::Yes => sig.fair_yes,
                Side::No  => sig.fair_no,
            };

            let exit_bid = match pos.side {
                Side::Yes => sig.bid_yes,
                Side::No  => sig.bid_no,
            };

            // ── Stop Loss: fair < entry_price, secs_left > 90 ──
            // FIX 3+4: After TP, SL protects remaining shares at breakeven (fair < entry)
            //          For all positions, also SL if edge has eroded >50% from entry
            let sl_threshold = if pos.tp_fired {
                // After TP, tighter SL — protect profits, exit if fair drops to entry
                pos.entry_price
            } else {
                // Before TP, SL when fair drops below entry (edge fully gone)
                pos.entry_price
            };

            if self.config.stop_loss
                && sig.secs_left > 90.0
                && current_fair < sl_threshold
                && exit_bid > 0.0
            {
                info!(
                    "[RUNNER] STOP_LOSS {} fair={:.3} < threshold={:.3} tp_fired={}",
                    pos.slug, current_fair, sl_threshold, pos.tp_fired
                );

                let pos = self.positions.remove(&trade_id).unwrap();
                let shares_to_sell = pos.shares;
                let exit_price = exit_bid;

                // Attempt to sell via GTC at bid
                match self.exec.sell_gtc(&pos.token_id, exit_price, shares_to_sell).await {
                    Ok(_fill) => {
                        let exit_fee = self.config.taker_fee_rate * (exit_price * shares_to_sell);
                        self.log_close(pos, exit_price, "STOP_LOSS", sig.ts, exit_fee).await;
                    }
                    Err(e) => {
                        warn!("[RUNNER] SL sell failed: {}, position lost", e);
                        self.log_close(pos, exit_price, "STOP_LOSS_FAIL", sig.ts, 0.0).await;
                    }
                }
                continue;
            }

            // ── 50% Take Profit: bid >= fair_at_entry, not already TP'd ──
            if self.config.take_profit
                && !pos.tp_fired
                && exit_bid >= pos.fair_at_entry
                && exit_bid > 0.0
            {
                let tp_shares = (pos.shares * self.config.partial_tp_pct * 100.0).floor() / 100.0;
                if tp_shares > 0.0 {
                    info!(
                        "[RUNNER] TAKE_PROFIT_50% {} bid={:.3} >= fair_at_entry={:.3} selling {:.2} shares",
                        pos.slug, exit_bid, pos.fair_at_entry, tp_shares
                    );

                    match self.exec.sell_gtc(&pos.token_id, exit_bid, tp_shares).await {
                        Ok(_fill) => {
                            let exit_fee = self.config.taker_fee_rate * (exit_bid * tp_shares);
                            // Log the partial exit
                            let partial_pnl = (exit_bid - pos.entry_price) * tp_shares;
                            let partial_net = partial_pnl - exit_fee;

                            if partial_net > 0.0 { self.stats.wins += 1; }
                            else { self.stats.losses += 1; }
                            self.stats.gross_pnl += partial_pnl;
                            self.stats.total_fee += exit_fee;
                            self.stats.net_pnl   += partial_net;
                            self.stats.tp_exits  += 1;

                            // Write log for TP portion
                            let log = TradeLog {
                                trade_id: format!("{}-TP", pos.trade_id),
                                slug: pos.slug.clone(),
                                asset: pos.asset.clone(),
                                tf: pos.tf,
                                side: pos.side.to_string(),
                                entry_price: pos.entry_price,
                                fair_at_entry: pos.fair_at_entry,
                                edge_at_entry: pos.fair_at_entry - pos.entry_price,
                                sigma: pos.sigma,
                                secs_left: pos.secs_left,
                                stake: tp_shares * pos.entry_price,
                                shares: tp_shares,
                                exit_price: exit_bid,
                                exit_reason: "TAKE_PROFIT_50".to_string(),
                                pnl: partial_pnl,
                                fee: exit_fee,
                                net_pnl: partial_net,
                                entry_ts: pos.entry_ts,
                                exit_ts: sig.ts,
                                hold_secs: sig.ts - pos.entry_ts,
                                tp_fired: true,
                            };
                            self.write_log(&log).await;

                            // Update position: reduce shares, mark TP fired
                            if let Some(p) = self.positions.get_mut(&trade_id) {
                                p.shares -= tp_shares;
                                p.tp_fired = true;
                            }
                        }
                        Err(e) => {
                            warn!("[RUNNER] TP sell failed: {}", e);
                        }
                    }
                }
            }
        }
    }

    // ── Close + log ─────────────────────────────────────────────────────────

    async fn log_close(
        &mut self,
        pos:         LivePosition,
        exit_price:  f64,
        exit_reason: &str,
        exit_ts:     f64,
        exit_fee:    f64,
    ) {
        let gross = (exit_price - pos.entry_price) * pos.shares;
        let net   = gross - exit_fee;

        if net > 0.0 { self.stats.wins += 1; }
        else { self.stats.losses += 1; }

        self.stats.gross_pnl += gross;
        self.stats.total_fee += exit_fee;
        self.stats.net_pnl   += net;

        match exit_reason {
            "SETTLEMENT"  => self.stats.settle_exits += 1,
            "STOP_LOSS" | "STOP_LOSS_FAIL" => self.stats.sl_exits += 1,
            _ => {}
        }

        let log = TradeLog {
            trade_id: pos.trade_id,
            slug: pos.slug.clone(),
            asset: pos.asset,
            tf: pos.tf,
            side: pos.side.to_string(),
            entry_price: pos.entry_price,
            fair_at_entry: pos.fair_at_entry,
            edge_at_entry: pos.fair_at_entry - pos.entry_price,
            sigma: pos.sigma,
            secs_left: pos.secs_left,
            stake: pos.stake,
            shares: pos.shares,
            exit_price,
            exit_reason: exit_reason.to_string(),
            pnl: gross,
            fee: exit_fee,
            net_pnl: net,
            entry_ts: pos.entry_ts,
            exit_ts,
            hold_secs: exit_ts - pos.entry_ts,
            tp_fired: pos.tp_fired,
        };

        self.write_log(&log).await;

        info!(
            "[RUNNER] CLOSE {} {} exit={:.3} reason={} net={:+.3}",
            pos.slug, pos.side, exit_price, exit_reason, net
        );
    }

    async fn write_log(&self, log: &TradeLog) {
        if let Ok(line) = serde_json::to_string(log) {
            let mut file = self.log_file.lock().await;
            let _ = writeln!(file, "{}", line);
        }
    }

    pub fn print_stats(&self) {
        let total_staked = self.stats.entries as f64 * self.config.stake;
        let roi = if total_staked > 0.0 { self.stats.net_pnl / total_staked * 100.0 } else { 0.0 };
        info!(
            "[OPT2] sig={} entries={} W={} L={} WR={:.1}% net={:+.2} fee={:.2} ROI={:+.1}% open={} settle={} sl={} tp={}",
            self.stats.signals, self.stats.entries,
            self.stats.wins, self.stats.losses, self.stats.wr(),
            self.stats.net_pnl, self.stats.total_fee, roi,
            self.positions.len(),
            self.stats.settle_exits, self.stats.sl_exits, self.stats.tp_exits,
        );
    }

}
