/// runner.rs — Lag-based position manager
///
/// Entry: on LagSignal, maker_chase_entry
/// Exit modes:
///   1. CONVERGENCE: CL catches up to BN → book reprices → sell at new bid
///   2. PROFIT: book bid >= entry + min_profit → take profit
///   3. REVERSAL: BN momentum flips against position → stop loss
///   4. TIME: hold exceeds max_hold_secs → exit at best bid
///   5. WINDOW: secs_left < 60 → hold to settlement (no exit)
///   6. SETTLEMENT: window ends → position settles at outcome price

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::fs::{File, OpenOptions};

use serde::Serialize;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::execution::ExecutionLayer;
use crate::lag::{LagSignal, Side};

// ── Position ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LagPosition {
    pub trade_id:       String,
    pub slug:           String,
    pub asset:          String,
    pub tf:             u32,
    pub side:           Side,
    pub token_id:       String,
    pub entry_price:    f64,
    pub fair_bn_entry:  f64,   // BN-derived fair at entry
    pub fair_cl_entry:  f64,   // CL-derived fair at entry
    pub edge_at_entry:  f64,
    pub divergence_pct: f64,   // BN-CL divergence at entry
    pub sigma:          f64,
    pub stake:          f64,
    pub shares:         f64,
    pub secs_left:      f64,
    pub entry_ts:       f64,
    pub window_end:     u64,
    pub order_id:       String,
    pub tp_fired:       bool,
    pub reversal_ts:    Option<f64>,  // when reversal first detected (confirmation delay)
    pub trail_high_bid: f64,          // highest bid seen since entry (for trailing stop)
}

// ── Trade log ───────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct TradeLog {
    pub trade_id:       String,
    pub slug:           String,
    pub asset:          String,
    pub tf:             u32,
    pub side:           String,
    pub entry_price:    f64,
    pub fair_bn_entry:  f64,
    pub fair_cl_entry:  f64,
    pub edge_at_entry:  f64,
    pub divergence_pct: f64,
    pub sigma:          f64,
    pub stake:          f64,
    pub shares:         f64,
    pub exit_price:     f64,
    pub exit_reason:    String,
    pub pnl:            f64,
    pub net_pnl:        f64,
    pub entry_ts:       f64,
    pub exit_ts:        f64,
    pub hold_secs:      f64,
}

// ── Stats ───────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
pub struct RunnerStats {
    pub signals:      u64,
    pub entries:      u64,
    pub wins:         u64,
    pub losses:       u64,
    pub net_pnl:      f64,
    pub convergence:  u64,   // exits due to convergence
    pub profit_exits: u64,
    pub reversal_sl:  u64,
    pub time_exits:   u64,
    pub settlements:  u64,
}

impl RunnerStats {
    pub fn wr(&self) -> f64 {
        let total = self.wins + self.losses;
        if total == 0 { 0.0 } else { self.wins as f64 / total as f64 * 100.0 }
    }
}

// ── Strategy Config ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RunnerConfig {
    pub stake:              f64,
    pub maker_chase_ticks:  u32,
    pub chase_interval_ms:  u64,
    pub max_concurrent:     usize,
    pub max_hold_secs:      f64,   // exit if held longer than this
    pub min_profit:         f64,   // cents profit to trigger early TP
    pub partial_tp_pct:     f64,   // fraction to sell on first TP
    pub taker_fee_rate:     f64,   // for emergency exits
    pub max_drawdown:       f64,   // kill switch: halt entries if net_pnl < -max_drawdown
}

// ── Runner ──────────────────────────────────────────────────────────────────

pub struct LagRunner {
    pub config:    RunnerConfig,
    pub exec:      Arc<ExecutionLayer>,
    positions:     HashMap<String, LagPosition>,
    stats:         RunnerStats,
    log_file:      Arc<Mutex<File>>,
    dd_halted:     bool,
}

impl LagRunner {
    pub fn new(config: RunnerConfig, exec: Arc<ExecutionLayer>, log_dir: &str) -> Self {
        let log_path = format!("{}/lag_sniper.jsonl", log_dir);
        let file = OpenOptions::new()
            .create(true).append(true)
            .open(&log_path)
            .unwrap_or_else(|e| panic!("Cannot open log {}: {}", log_path, e));

        info!("[LAG_RUNNER] Log: {}", log_path);

        Self {
            config,
            exec,
            positions: HashMap::new(),
            stats: RunnerStats::default(),
            log_file: Arc::new(Mutex::new(file)),
            dd_halted: false,
        }
    }

    /// Called every tick with current market state for exit checks.
    pub async fn check_exits(
        &mut self,
        slug:          &str,
        bn_price:      f64,
        cl_price:      f64,
        _open_price:   f64,
        _sigma:        f64,
        secs_left:     f64,
        bn_momentum:   f64,
        book_yes_bid:  f64,
        book_no_bid:   f64,
        now:           f64,
    ) {
        let trade_ids: Vec<String> = self.positions.keys()
            .filter(|k| k.starts_with(slug))
            .cloned().collect();

        for trade_id in trade_ids {
            let pos = match self.positions.get(&trade_id) {
                Some(p) => p.clone(),
                None    => continue,
            };

            let hold_secs = now - pos.entry_ts;
            let exit_bid = match pos.side {
                Side::Yes => book_yes_bid,
                Side::No  => book_no_bid,
            };

            if exit_bid <= 0.0 { continue; }

            // ── Update trailing high bid ──────────────────────────────
            if exit_bid > pos.trail_high_bid {
                if let Some(p) = self.positions.get_mut(&trade_id) {
                    p.trail_high_bid = exit_bid;
                }
            }
            let trail_high = pos.trail_high_bid.max(exit_bid);

            // ── Exit 1: CONVERGENCE ─────────────────────────────────────
            // CL has caught up to BN → the lag that created our edge is gone
            // Book should now be repriced → take profit at new bid
            let cl_caught_up = {
                let cl_div = ((bn_price - cl_price) / cl_price).abs() * 100.0;
                cl_div < 0.01  // CL within 0.01% of BN = converged
            };
            let book_repriced = exit_bid >= pos.entry_price + 0.01;

            if cl_caught_up && book_repriced && !pos.tp_fired {
                info!(
                    "[LAG_RUNNER] CONVERGENCE {} bid={:.3} > entry={:.3} hold={:.1}s",
                    pos.slug, exit_bid, pos.entry_price, hold_secs
                );
                self.exit_position(&trade_id, exit_bid, "CONVERGENCE", now).await;
                continue;
            }

            // ── Exit 2: TRAILING STOP ──────────────────────────────────
            // Activate once bid is 3c above entry. Trail by 1.5c.
            // Also: if trail is active and we're near time exit, take profit now.
            let trail_activated = trail_high >= pos.entry_price + 0.03;
            if trail_activated {
                if exit_bid <= trail_high - 0.015 {
                    info!(
                        "[LAG_RUNNER] TRAIL_STOP {} bid={:.3} high={:.3} drop={:.3} hold={:.1}s",
                        pos.slug, exit_bid, trail_high, trail_high - exit_bid, hold_secs
                    );
                    self.exit_position(&trade_id, exit_bid, "TRAIL_STOP", now).await;
                    continue;
                }
                // Near time limit with active trail — don't let it time out, take profit
                if hold_secs > self.config.max_hold_secs - 5.0 {
                    info!(
                        "[LAG_RUNNER] TRAIL_TIME {} bid={:.3} high={:.3} hold={:.1}s — taking profit before timeout",
                        pos.slug, exit_bid, trail_high, hold_secs
                    );
                    self.exit_position(&trade_id, exit_bid, "TRAIL_STOP", now).await;
                    continue;
                }
            }

            // ── Exit 3: REVERSAL (with confirmation delay) ─────────────
            // BN momentum has flipped against our position
            let reversed = match pos.side {
                Side::Yes => bn_momentum < -0.0005,  // BN now falling
                Side::No  => bn_momentum > 0.0005,   // BN now rising
            };
            if reversed && exit_bid < pos.entry_price && secs_left > 60.0 {
                // Start or check reversal confirmation timer
                let reversal_ts = match pos.reversal_ts {
                    Some(ts) => ts,
                    None => {
                        // First detection — record timestamp, don't SL yet
                        if let Some(p) = self.positions.get_mut(&trade_id) {
                            p.reversal_ts = Some(now);
                        }
                        continue;
                    }
                };
                // Only SL after momentum stays reversed for 3+ seconds
                if now - reversal_ts < 3.0 {
                    continue;
                }
                info!(
                    "[LAG_RUNNER] REVERSAL {} momentum={:.4} bid={:.3} < entry={:.3} confirmed={:.1}s",
                    pos.slug, bn_momentum, exit_bid, pos.entry_price, now - reversal_ts
                );
                self.exit_position(&trade_id, exit_bid, "REVERSAL_SL", now).await;
                continue;
            } else {
                // Momentum recovered OR bid is above entry — clear the reversal timer
                // so stale timers don't cause instant SL when conditions change
                if pos.reversal_ts.is_some() {
                    if let Some(p) = self.positions.get_mut(&trade_id) {
                        p.reversal_ts = None;
                    }
                }
            }

            // ── Exit 4: HARD STOP-LOSS ────────────────────────────────
            // Bid dropped 50%+ from entry → cut losses immediately
            if exit_bid <= pos.entry_price * 0.50 && secs_left > 60.0 {
                info!(
                    "[LAG_RUNNER] HARD_SL {} bid={:.3} <= 50% of entry={:.3} hold={:.1}s",
                    pos.slug, exit_bid, pos.entry_price, hold_secs
                );
                self.exit_position(&trade_id, exit_bid, "HARD_SL", now).await;
                continue;
            }

            // ── Exit 5: TIME ────────────────────────────────────────────
            // Held too long without convergence
            if hold_secs > self.config.max_hold_secs && secs_left > 60.0 {
                // If BN still supports our direction AND we haven't held 2x max,
                // hold to settlement rather than panic-selling into a thin bid
                let bn_supports = match pos.side {
                    Side::Yes => bn_momentum >= 0.0,
                    Side::No  => bn_momentum <= 0.0,
                };
                let hard_time_cap = hold_secs > self.config.max_hold_secs * 2.0;
                if !hard_time_cap && (bn_supports || exit_bid < pos.entry_price * 0.85) {
                    continue;
                }
                info!(
                    "[LAG_RUNNER] TIME_EXIT {} held={:.1}s > max={:.0}s bid={:.3} bn_mom={:.4}",
                    pos.slug, hold_secs, self.config.max_hold_secs, exit_bid, bn_momentum
                );
                self.exit_position(&trade_id, exit_bid, "TIME_EXIT", now).await;
                continue;
            }
        }
    }

    /// Called when a LagSignal fires — attempt entry.
    pub async fn on_lag_signal(
        &mut self,
        sig: &LagSignal,
        window_end: u64,
        token_yes: &str,
        token_no: &str,
    ) {
        self.stats.signals += 1;

        // DD kill switch — no new entries
        if self.dd_halted {
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

        // Max 2 per asset
        let asset_count = self.positions.values()
            .filter(|p| p.asset == sig.asset)
            .count();
        if asset_count >= 2 {
            return;
        }

        let token_id = match sig.side {
            Side::Yes => token_yes,
            Side::No  => token_no,
        };

        info!(
            "[LAG_RUNNER] ENTRY {} {} maker={:.2} ask={:.2} fair_bn={:.3} fair_cl={:.3} gap={:.3} edge={:.3} div={:.3}% T-{:.0}s",
            sig.slug, sig.side, sig.maker_price, sig.best_ask,
            sig.fair_bn, sig.fair_cl, sig.fair_gap, sig.edge,
            sig.divergence_pct, sig.secs_left
        );

        // Cap best_ask to prevent chasing too far above maker price
        // Max 3 ticks (0.03) above initial maker price
        let max_chase_price = (sig.maker_price + 0.03).min(sig.best_ask);

        // Execute maker chase entry
        match self.exec.maker_chase_entry(
            token_id,
            sig.maker_price,
            self.config.stake,
            self.config.maker_chase_ticks,
            self.config.chase_interval_ms,
            max_chase_price,
        ).await {
            Ok(fill) => {
                let actual_price = fill.price;

                // Post-fill edge check: after slippage, edge must be >= 0.10
                let post_edge = match sig.side {
                    Side::Yes => sig.fair_bn - actual_price,
                    Side::No  => (1.0 - sig.fair_bn) - actual_price,
                };
                if post_edge < 0.10 {
                    warn!(
                        "[LAG_RUNNER] REJECT post-fill {} edge={:.3} < 0.10 (fill={:.3})",
                        sig.slug, post_edge, actual_price
                    );
                    let shares = self.config.stake / actual_price;
                    let _ = self.exec.sell_gtc(token_id, actual_price, shares).await;
                    return;
                }

                // Max entry price cap
                if actual_price > 0.90 {
                    warn!("[LAG_RUNNER] REJECT expensive fill {} @{:.3}", sig.slug, actual_price);
                    let shares = self.config.stake / actual_price;
                    let _ = self.exec.sell_gtc(token_id, actual_price, shares).await;
                    return;
                }

                let shares = self.config.stake / actual_price;
                let trade_id = format!("{}-LAG-{:.0}", sig.slug, sig.ts * 1000.0);

                info!(
                    "[LAG_RUNNER] ENTERED {} {} @{:.3} shares={:.2} oid={}",
                    sig.slug, sig.side, actual_price, shares, fill.order_id
                );

                let pos = LagPosition {
                    trade_id: trade_id.clone(),
                    slug: sig.slug.clone(),
                    asset: sig.asset.clone(),
                    tf: sig.tf,
                    side: sig.side,
                    token_id: token_id.to_string(),
                    entry_price: actual_price,
                    fair_bn_entry: sig.fair_bn,
                    fair_cl_entry: sig.fair_cl,
                    edge_at_entry: post_edge,
                    divergence_pct: sig.divergence_pct,
                    sigma: sig.sigma,
                    stake: self.config.stake,
                    shares,
                    secs_left: sig.secs_left,
                    entry_ts: sig.ts,
                    window_end,
                    order_id: fill.order_id,
                    tp_fired: false,
                    reversal_ts: None,
                    trail_high_bid: 0.0,
                };

                self.positions.insert(trade_id, pos);
                self.stats.entries += 1;
            }
            Err(e) => {
                warn!("[LAG_RUNNER] Entry failed for {}: {}", sig.slug, e);
            }
        }
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
                self.log_close(pos, exit_price, "SETTLEMENT", settle_ts).await;
                self.stats.settlements += 1;
            }
        }
    }

    // ── Internal exit helpers ───────────────────────────────────────────────

    async fn exit_position(&mut self, trade_id: &str, exit_bid: f64, reason: &str, now: f64) {
        let pos = match self.positions.remove(trade_id) {
            Some(p) => p,
            None => return,
        };

        // Thin-book protection: if bid is garbage, hold to settlement instead
        if exit_bid < pos.entry_price * 0.85 && reason != "HARD_SL" {
            info!(
                "[LAG_RUNNER] SKIP_SELL {} bid={:.3} < 85% of entry={:.3} — holding to settlement",
                pos.slug, exit_bid, pos.entry_price
            );
            self.positions.insert(trade_id.to_string(), pos);
            return;
        }

        // Sell via GTC at bid
        match self.exec.sell_gtc(&pos.token_id, exit_bid, pos.shares).await {
            Ok(_) => {
                self.log_close(pos, exit_bid, reason, now).await;
            }
            Err(e) => {
                warn!("[LAG_RUNNER] Sell failed ({}): {}", reason, e);
                self.log_close(pos, exit_bid, &format!("{}_FAIL", reason), now).await;
            }
        }

        match reason {
            "CONVERGENCE" => self.stats.convergence += 1,
            "TRAIL_STOP"  => self.stats.profit_exits += 1,
            "REVERSAL_SL" | "HARD_SL" => self.stats.reversal_sl += 1,
            "TIME_EXIT"   => self.stats.time_exits += 1,
            _ => {}
        }
    }

    #[allow(dead_code)]
    async fn partial_exit(&mut self, trade_id: &str, exit_bid: f64, reason: &str, now: f64) {
        let pos = match self.positions.get(trade_id) {
            Some(p) => p.clone(),
            None => return,
        };

        let tp_shares = (pos.shares * self.config.partial_tp_pct * 100.0).floor() / 100.0;
        if tp_shares <= 0.0 { return; }

        match self.exec.sell_gtc(&pos.token_id, exit_bid, tp_shares).await {
            Ok(_) => {
                let pnl = (exit_bid - pos.entry_price) * tp_shares;
                if pnl > 0.0 { self.stats.wins += 1; }
                else { self.stats.losses += 1; }
                self.stats.net_pnl += pnl;
                self.stats.profit_exits += 1;

                let log = TradeLog {
                    trade_id: format!("{}-TP", pos.trade_id),
                    slug: pos.slug.clone(),
                    asset: pos.asset.clone(),
                    tf: pos.tf,
                    side: pos.side.to_string(),
                    entry_price: pos.entry_price,
                    fair_bn_entry: pos.fair_bn_entry,
                    fair_cl_entry: pos.fair_cl_entry,
                    edge_at_entry: pos.edge_at_entry,
                    divergence_pct: pos.divergence_pct,
                    sigma: pos.sigma,
                    stake: tp_shares * pos.entry_price,
                    shares: tp_shares,
                    exit_price: exit_bid,
                    exit_reason: reason.to_string(),
                    pnl,
                    net_pnl: pnl,
                    entry_ts: pos.entry_ts,
                    exit_ts: now,
                    hold_secs: now - pos.entry_ts,
                };
                self.write_log(&log).await;

                if let Some(p) = self.positions.get_mut(trade_id) {
                    p.shares -= tp_shares;
                    p.tp_fired = true;
                }
            }
            Err(e) => {
                warn!("[LAG_RUNNER] TP sell failed: {}", e);
            }
        }
    }

    async fn log_close(&mut self, pos: LagPosition, exit_price: f64, reason: &str, exit_ts: f64) {
        let pnl = (exit_price - pos.entry_price) * pos.shares;

        if pnl > 0.0 { self.stats.wins += 1; }
        else { self.stats.losses += 1; }
        self.stats.net_pnl += pnl;

        let log = TradeLog {
            trade_id: pos.trade_id,
            slug: pos.slug,
            asset: pos.asset,
            tf: pos.tf,
            side: pos.side.to_string(),
            entry_price: pos.entry_price,
            fair_bn_entry: pos.fair_bn_entry,
            fair_cl_entry: pos.fair_cl_entry,
            edge_at_entry: pos.edge_at_entry,
            divergence_pct: pos.divergence_pct,
            sigma: pos.sigma,
            stake: pos.stake,
            shares: pos.shares,
            exit_price,
            exit_reason: reason.to_string(),
            pnl,
            net_pnl: pnl,
            entry_ts: pos.entry_ts,
            exit_ts,
            hold_secs: exit_ts - pos.entry_ts,
        };

        self.write_log(&log).await;

        info!(
            "[LAG_RUNNER] CLOSE {} {} exit={:.3} reason={} pnl={:+.3} hold={:.1}s net={:+.2}",
            log.slug, log.side, exit_price, reason, pnl, log.hold_secs, self.stats.net_pnl
        );

        // DD kill switch check
        if self.config.max_drawdown > 0.0 && self.stats.net_pnl <= -self.config.max_drawdown {
            if !self.dd_halted {
                self.dd_halted = true;
                warn!(
                    "[LAG_RUNNER] DD KILL SWITCH — net={:+.2} breached max_dd=${:.0}. No new entries.",
                    self.stats.net_pnl, self.config.max_drawdown
                );
                // Cancel all outstanding orders
                match self.exec.cancel_all().await {
                    Ok(_)  => info!("[LAG_RUNNER] DD halt: all orders cancelled"),
                    Err(e) => warn!("[LAG_RUNNER] DD halt: cancel failed: {}", e),
                }
            }
        }
    }

    async fn write_log(&self, log: &TradeLog) {
        if let Ok(line) = serde_json::to_string(log) {
            let mut file = self.log_file.lock().await;
            let _ = writeln!(file, "{}", line);
        }
    }

    #[allow(dead_code)]
    pub fn open_count(&self) -> usize {
        self.positions.len()
    }

    pub fn is_dd_halted(&self) -> bool {
        self.dd_halted
    }

    pub fn print_stats(&self) {
        let total_staked = self.stats.entries as f64 * self.config.stake;
        let roi = if total_staked > 0.0 { self.stats.net_pnl / total_staked * 100.0 } else { 0.0 };
        info!(
            "[LAG] sig={} entries={} W={} L={} WR={:.1}% net={:+.2} ROI={:+.1}% open={} conv={} tp={} rev={} time={} settle={}",
            self.stats.signals, self.stats.entries,
            self.stats.wins, self.stats.losses, self.stats.wr(),
            self.stats.net_pnl, roi,
            self.positions.len(),
            self.stats.convergence, self.stats.profit_exits,
            self.stats.reversal_sl, self.stats.time_exits,
            self.stats.settlements,
        );
    }
}
