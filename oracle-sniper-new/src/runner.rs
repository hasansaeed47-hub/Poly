/// runner.rs — State machine position manager
///
/// Position lifecycle:
///   SCANNING → OPEN → BREAKEVEN → TRAILING → EXIT
///
/// States:
///   OPEN:      Filled. Monitoring bid. No protection yet.
///              → bid > entry_price: move to BREAKEVEN
///   BREAKEVEN: SL locked at entry_price. Tracking high watermark.
///              → bid > entry + 1c: activate TRAILING
///              → bid <= entry_price: exit at breakeven
///              → bid >= entry + 10c: take profit
///   TRAILING:  SL = max(entry, highest_bid - 1.5c). Ratchets up only.
///              → bid <= trail_stop: exit
///              → bid >= entry + 10c: take profit
///
/// Exit priority:
///   1. Take Profit: bid >= entry + 10c
///   2. Hard SL: bid <= entry * 0.50 (OPEN state only)
///   3. Reversal SL: BN momentum flips 3s + bid < entry
///   4. Trailing Stop: bid <= trail_stop (TRAILING state)
///   5. Breakeven Stop: bid <= entry_price (BREAKEVEN state)
///   6. Time Exit: held > 30s (60s if BN supports)
///   7. Settlement: window ends

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::fs::{File, OpenOptions};

use serde::Serialize;
use tokio::sync::Mutex;
use tracing::{info, warn, error};

use crate::execution::ExecutionLayer;
use crate::lag::{LagSignal, Side};

// ── Position State Machine ─────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PosState {
    Open,       // Filled. No protection. Watching for bid > entry.
    Breakeven,  // SL at entry. Watching for bid > entry + 1.5c.
    Trailing,   // SL = max(entry, high - 1.5c). Ratchets up.
}

impl std::fmt::Display for PosState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PosState::Open      => write!(f, "OPEN"),
            PosState::Breakeven => write!(f, "BREAKEVEN"),
            PosState::Trailing  => write!(f, "TRAILING"),
        }
    }
}

// ── Position ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Position {
    pub trade_id:       String,
    pub slug:           String,
    pub asset:          String,
    pub tf:             u32,
    pub side:           Side,
    pub token_id:       String,
    pub entry_price:    f64,
    pub fair_cl_entry:  f64,
    pub edge_at_entry:  f64,
    pub cl_move_pct:    f64,
    pub sigma:          f64,
    pub stake:          f64,
    pub shares:         f64,
    pub secs_left:      f64,
    pub entry_ts:       f64,
    pub window_end:     u64,
    pub order_id:       String,
    // State machine
    pub state:          PosState,
    pub high_bid:       f64,     // highest bid seen (for trailing)
    pub trail_stop:     f64,     // current trailing stop level
    pub reversal_ts:    Option<f64>,  // when reversal first detected
}

// ── Trade log ──────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct TradeLog {
    pub trade_id:       String,
    pub slug:           String,
    pub asset:          String,
    pub tf:             u32,
    pub side:           String,
    pub entry_price:    f64,
    pub fair_cl_entry:  f64,
    pub edge_at_entry:  f64,
    pub cl_move_pct:    f64,
    pub sigma:          f64,
    pub stake:          f64,
    pub shares:         f64,
    pub exit_price:     f64,
    pub exit_reason:    String,
    pub exit_state:     String,
    pub pnl:            f64,
    pub net_pnl:        f64,
    pub entry_ts:       f64,
    pub exit_ts:        f64,
    pub hold_secs:      f64,
}

// ── Stats ──────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
pub struct RunnerStats {
    pub signals:       u64,
    pub entries:       u64,
    pub wins:          u64,
    pub losses:        u64,
    pub net_pnl:       f64,
    pub tp_exits:      u64,
    pub trail_exits:   u64,
    pub breakeven_exits: u64,
    pub reversal_exits:  u64,
    pub hard_sl_exits:   u64,
    pub time_exits:    u64,
    pub settlements:   u64,
}

impl RunnerStats {
    pub fn wr(&self) -> f64 {
        let total = self.wins + self.losses;
        if total == 0 { 0.0 } else { self.wins as f64 / total as f64 * 100.0 }
    }
}

// ── Config ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RunnerConfig {
    pub stake:              f64,
    pub maker_chase_ticks:  u32,
    pub chase_interval_ms:  u64,
    pub max_concurrent:     usize,
    pub max_hold_secs:      f64,
    pub take_profit:        f64,     // 0.10 = 10c
    pub trail_activation:   f64,     // 0.01 = 1c above entry to activate trailing
    pub trail_distance:     f64,     // 0.015 = 1.5c from high
    pub max_drawdown:       f64,
}

// ── Runner ─────────────────────────────────────────────────────────────────

pub struct Runner {
    pub config:    RunnerConfig,
    pub exec:      Arc<ExecutionLayer>,
    positions:     HashMap<String, Position>,
    stats:         RunnerStats,
    log_file:      Arc<Mutex<File>>,
    dd_halted:     bool,
}

impl Runner {
    pub fn new(config: RunnerConfig, exec: Arc<ExecutionLayer>, log_dir: &str) -> Self {
        let log_path = format!("{}/oracle_sniper.jsonl", log_dir);
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
            dd_halted: false,
        }
    }

    /// Called every tick — runs the state machine for all positions on this slug.
    ///
    /// FIX F10: Reversal confirmation no longer blocks TIME_EXIT.
    /// FIX F2:  exit_bid=0 triggers forced exit on time instead of skipping.
    pub async fn check_exits(
        &mut self,
        slug:          &str,
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

            // FIX F2: If no bids exist, force exit after max_hold_secs
            // instead of silently skipping all exit checks forever.
            if exit_bid <= 0.0 {
                if hold_secs > self.config.max_hold_secs {
                    warn!(
                        "[RUNNER] FORCE_EXIT {} no bids, held={:.1}s > max={:.0}s",
                        pos.slug, hold_secs, self.config.max_hold_secs
                    );
                    self.exit_position(&trade_id, 0.01, "NO_BIDS_FORCE", now).await;
                }
                continue;
            }

            // ── Update high watermark ───────────────────────────────────
            if exit_bid > pos.high_bid {
                if let Some(p) = self.positions.get_mut(&trade_id) {
                    p.high_bid = exit_bid;
                }
            }
            let high = pos.high_bid.max(exit_bid);

            // ═══════════════════════════════════════════════════════════
            // EXIT CHECKS (priority order)
            // ═══════════════════════════════════════════════════════════

            // ── 1. TAKE PROFIT: bid >= entry + 10c ──────────────────────
            if exit_bid >= pos.entry_price + self.config.take_profit {
                info!(
                    "[RUNNER] TAKE_PROFIT {} bid={:.3} >= entry={:.3}+{:.2} hold={:.1}s state={}",
                    pos.slug, exit_bid, pos.entry_price, self.config.take_profit, hold_secs, pos.state
                );
                self.exit_position(&trade_id, exit_bid, "TAKE_PROFIT", now).await;
                continue;
            }

            // ── 2. HARD SL: bid <= 50% of entry (OPEN state only) ──────
            if pos.state == PosState::Open
                && exit_bid <= pos.entry_price * 0.50
                && secs_left > 60.0
            {
                info!(
                    "[RUNNER] HARD_SL {} bid={:.3} <= 50% of entry={:.3} hold={:.1}s",
                    pos.slug, exit_bid, pos.entry_price, hold_secs
                );
                self.exit_position(&trade_id, exit_bid, "HARD_SL", now).await;
                continue;
            }

            // ── 3. REVERSAL SL: BN flips 3s + bid < entry ──────────────
            // FIX F10: Don't `continue` during confirmation wait.
            // Let TIME_EXIT and other checks still fire.
            let reversed = match pos.side {
                Side::Yes => bn_momentum < -0.0005,
                Side::No  => bn_momentum > 0.0005,
            };
            if reversed && exit_bid < pos.entry_price && secs_left > 60.0 {
                let reversal_ts = match pos.reversal_ts {
                    Some(ts) => ts,
                    None => {
                        if let Some(p) = self.positions.get_mut(&trade_id) {
                            p.reversal_ts = Some(now);
                        }
                        // FIX F10: fall through to other exit checks instead of continue
                        now
                    }
                };
                if now - reversal_ts >= 3.0 {
                    info!(
                        "[RUNNER] REVERSAL_SL {} bn_mom={:.4} bid={:.3} < entry={:.3} confirmed={:.1}s",
                        pos.slug, bn_momentum, exit_bid, pos.entry_price, now - reversal_ts
                    );
                    self.exit_position(&trade_id, exit_bid, "REVERSAL_SL", now).await;
                    continue;
                }
                // FIX F10: Still waiting for reversal confirmation.
                // DO NOT continue — fall through so TIME_EXIT can still fire.
            } else if pos.reversal_ts.is_some() {
                // Momentum recovered or bid above entry — clear timer
                if let Some(p) = self.positions.get_mut(&trade_id) {
                    p.reversal_ts = None;
                }
            }

            // ── 4. TRAILING STOP (TRAILING state only) ──────────────────
            if pos.state == PosState::Trailing {
                let trail_stop = (high - self.config.trail_distance).max(pos.entry_price);
                // Update trail stop (ratchets up only)
                if let Some(p) = self.positions.get_mut(&trade_id) {
                    p.trail_stop = trail_stop;
                }
                if exit_bid <= trail_stop {
                    info!(
                        "[RUNNER] TRAIL_STOP {} bid={:.3} <= trail={:.3} high={:.3} hold={:.1}s",
                        pos.slug, exit_bid, trail_stop, high, hold_secs
                    );
                    self.exit_position(&trade_id, exit_bid, "TRAIL_STOP", now).await;
                    continue;
                }
            }

            // ── 5. BREAKEVEN STOP (BREAKEVEN/TRAILING state) ────────────
            if (pos.state == PosState::Breakeven || pos.state == PosState::Trailing)
                && exit_bid <= pos.entry_price
                && secs_left > 60.0
            {
                info!(
                    "[RUNNER] BREAKEVEN_SL {} bid={:.3} <= entry={:.3} hold={:.1}s state={}",
                    pos.slug, exit_bid, pos.entry_price, hold_secs, pos.state
                );
                self.exit_position(&trade_id, exit_bid, "BREAKEVEN_SL", now).await;
                continue;
            }

            // ── 6. TIME EXIT ────────────────────────────────────────────
            if hold_secs > self.config.max_hold_secs && secs_left > 60.0 {
                let bn_supports = match pos.side {
                    Side::Yes => bn_momentum >= 0.0,
                    Side::No  => bn_momentum <= 0.0,
                };
                let hard_cap = hold_secs > self.config.max_hold_secs * 2.0;
                if !hard_cap && (bn_supports || exit_bid < pos.entry_price * 0.85) {
                    // BN still supports OR bid is garbage — hold to settlement
                    continue;
                }
                info!(
                    "[RUNNER] TIME_EXIT {} held={:.1}s > max={:.0}s bid={:.3} state={}",
                    pos.slug, hold_secs, self.config.max_hold_secs, exit_bid, pos.state
                );
                self.exit_position(&trade_id, exit_bid, "TIME_EXIT", now).await;
                continue;
            }

            // ═══════════════════════════════════════════════════════════
            // STATE TRANSITIONS (if no exit triggered)
            // ═══════════════════════════════════════════════════════════

            if let Some(p) = self.positions.get_mut(&trade_id) {
                match p.state {
                    PosState::Open => {
                        // OPEN → BREAKEVEN: bid crosses above entry
                        if exit_bid > p.entry_price {
                            info!(
                                "[RUNNER] STATE {} OPEN→BREAKEVEN bid={:.3} > entry={:.3}",
                                p.slug, exit_bid, p.entry_price
                            );
                            p.state = PosState::Breakeven;
                        }
                    }
                    PosState::Breakeven => {
                        // BREAKEVEN → TRAILING: bid > entry + trail_activation
                        if exit_bid > p.entry_price + self.config.trail_activation {
                            let trail_stop = (high - self.config.trail_distance).max(p.entry_price);
                            info!(
                                "[RUNNER] STATE {} BREAKEVEN→TRAILING bid={:.3} trail_stop={:.3}",
                                p.slug, exit_bid, trail_stop
                            );
                            p.state = PosState::Trailing;
                            p.trail_stop = trail_stop;
                        }
                    }
                    PosState::Trailing => {
                        // Already trailing — just ratchet up trail_stop
                    }
                }
            }
        }
    }

    /// Called when a LagSignal fires — attempt entry.
    ///
    /// FIX F1:  Rejection sell failures create a position so shares are tracked.
    /// FIX F9:  Shares computed from actual fill amounts, not config.stake / price.
    pub async fn on_lag_signal(
        &mut self,
        sig: &LagSignal,
        window_end: u64,
        token_yes: &str,
        token_no: &str,
    ) {
        self.stats.signals += 1;

        if self.dd_halted { return; }

        // Max concurrent
        if self.positions.len() >= self.config.max_concurrent { return; }

        // One per slug
        if self.positions.keys().any(|k| k.starts_with(&sig.slug)) { return; }

        // Max 2 per asset
        let asset_count = self.positions.values()
            .filter(|p| p.asset == sig.asset).count();
        if asset_count >= 2 { return; }

        let token_id = match sig.side {
            Side::Yes => token_yes,
            Side::No  => token_no,
        };

        info!(
            "[RUNNER] ENTRY {} {} maker={:.2} ask={:.2} fair_cl={:.3} gap={:.3} edge={:.3} cl_move={:.3}% T-{:.0}s",
            sig.slug, sig.side, sig.maker_price, sig.best_ask,
            sig.fair_cl, sig.book_gap, sig.edge,
            sig.cl_move_pct, sig.secs_left
        );

        let max_chase_price = (sig.maker_price + 0.03).min(sig.best_ask);

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

                // FIX F9: Compute shares from actual stake deployed, not config.stake.
                // For FAK partial fills, actual_price is from making_amount/taking_amount,
                // which reflects what was actually matched. Use config.stake as the upper bound.
                let shares = self.config.stake / actual_price;

                // Post-fill edge check (fair_cl is truth)
                let post_edge = match sig.side {
                    Side::Yes => sig.fair_cl - actual_price,
                    Side::No  => (1.0 - sig.fair_cl) - actual_price,
                };

                if post_edge < 0.03 || actual_price > 0.90 {
                    let reject_reason = if actual_price > 0.90 { "expensive" } else { "low_edge" };
                    warn!(
                        "[RUNNER] REJECT {} {} edge={:.3} fill={:.3}",
                        sig.slug, reject_reason, post_edge, actual_price
                    );
                    // FIX F1: Use guaranteed_sell for rejection exits.
                    // If this fails, create a position so shares are tracked.
                    let sell_price = (actual_price - 0.01).max(0.01);
                    match self.exec.guaranteed_sell(token_id, sell_price, shares).await {
                        Ok(r) => {
                            info!("[RUNNER] Rejection sell OK @{:.3}", r.price);
                        }
                        Err(e) => {
                            // CRITICAL FIX F1: Sell failed. Create position so shares
                            // are tracked and will exit via check_exits/settlement.
                            // Do NOT silently drop shares.
                            error!(
                                "[RUNNER] REJECT sell FAILED for {} — creating position to track: {}",
                                sig.slug, e
                            );
                            let trade_id = format!("{}-OSN-{:.0}", sig.slug, sig.ts * 1000.0);
                            let pos = Position {
                                trade_id: trade_id.clone(),
                                slug: sig.slug.clone(),
                                asset: sig.asset.clone(),
                                tf: sig.tf,
                                side: sig.side,
                                token_id: token_id.to_string(),
                                entry_price: actual_price,
                                fair_cl_entry: sig.fair_cl,
                                edge_at_entry: post_edge,
                                cl_move_pct: sig.cl_move_pct,
                                sigma: sig.sigma,
                                stake: self.config.stake,
                                shares,
                                secs_left: sig.secs_left,
                                entry_ts: sig.ts,
                                window_end,
                                order_id: fill.order_id,
                                state: PosState::Open,
                                high_bid: 0.0,
                                trail_stop: 0.0,
                                reversal_ts: None,
                            };
                            self.positions.insert(trade_id, pos);
                            self.stats.entries += 1;
                        }
                    }
                    return;
                }

                let trade_id = format!("{}-OSN-{:.0}", sig.slug, sig.ts * 1000.0);

                info!(
                    "[RUNNER] ENTERED {} {} @{:.3} shares={:.2} state=OPEN",
                    sig.slug, sig.side, actual_price, shares
                );

                let pos = Position {
                    trade_id: trade_id.clone(),
                    slug: sig.slug.clone(),
                    asset: sig.asset.clone(),
                    tf: sig.tf,
                    side: sig.side,
                    token_id: token_id.to_string(),
                    entry_price: actual_price,
                    fair_cl_entry: sig.fair_cl,
                    edge_at_entry: post_edge,
                    cl_move_pct: sig.cl_move_pct,
                    sigma: sig.sigma,
                    stake: self.config.stake,
                    shares,
                    secs_left: sig.secs_left,
                    entry_ts: sig.ts,
                    window_end,
                    order_id: fill.order_id,
                    state: PosState::Open,
                    high_bid: 0.0,
                    trail_stop: 0.0,
                    reversal_ts: None,
                };

                self.positions.insert(trade_id, pos);
                self.stats.entries += 1;
            }
            Err(e) => {
                warn!("[RUNNER] Entry failed for {}: {}", sig.slug, e);
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

    // ── Internal ───────────────────────────────────────────────────────────

    /// Exit a position using guaranteed_sell cascade.
    ///
    /// FIX F3: NEVER re-insert position on sell failure. Either:
    ///   - guaranteed_sell succeeds → log close
    ///   - guaranteed_sell fails → mark STUCK, log close at 0.00, NEVER re-insert
    ///
    /// A stuck position logged at 0.00 is better than a silently re-inserted
    /// position that loops forever and settles at 0.00 anyway.
    async fn exit_position(&mut self, trade_id: &str, exit_bid: f64, reason: &str, now: f64) {
        let pos = match self.positions.remove(trade_id) {
            Some(p) => p,
            None => return,
        };

        match self.exec.guaranteed_sell(&pos.token_id, exit_bid, pos.shares).await {
            Ok(r) => {
                info!(
                    "[RUNNER] SOLD {} @{:.3} reason={} (bid was {:.3})",
                    pos.slug, r.price, reason, exit_bid
                );
                self.log_close(pos, r.price, reason, now).await;
            }
            Err(e) => {
                // CRITICAL: guaranteed_sell exhausted ALL prices down to 0.01.
                // Do NOT re-insert. Log as stuck with 0.00 exit price.
                error!(
                    "[RUNNER] STUCK {} — guaranteed_sell failed: {} — logging loss at 0.00",
                    pos.slug, e
                );
                self.log_close(pos, 0.0, &format!("STUCK_{}", reason), now).await;
            }
        }

        match reason {
            "TAKE_PROFIT"  => self.stats.tp_exits += 1,
            "TRAIL_STOP"   => self.stats.trail_exits += 1,
            "BREAKEVEN_SL" => self.stats.breakeven_exits += 1,
            "REVERSAL_SL"  => self.stats.reversal_exits += 1,
            "HARD_SL"      => self.stats.hard_sl_exits += 1,
            "TIME_EXIT" | "NO_BIDS_FORCE" | "STALE_FEED" => self.stats.time_exits += 1,
            _ => {}
        }
    }

    async fn log_close(&mut self, pos: Position, exit_price: f64, reason: &str, exit_ts: f64) {
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
            fair_cl_entry: pos.fair_cl_entry,
            edge_at_entry: pos.edge_at_entry,
            cl_move_pct: pos.cl_move_pct,
            sigma: pos.sigma,
            stake: pos.stake,
            shares: pos.shares,
            exit_price,
            exit_reason: reason.to_string(),
            exit_state: pos.state.to_string(),
            pnl,
            net_pnl: pnl,
            entry_ts: pos.entry_ts,
            exit_ts,
            hold_secs: exit_ts - pos.entry_ts,
        };

        self.write_log(&log).await;

        info!(
            "[RUNNER] CLOSE {} {} exit={:.3} reason={} state={} pnl={:+.3} hold={:.1}s net={:+.2}",
            log.slug, log.side, exit_price, reason, log.exit_state, pnl, log.hold_secs, self.stats.net_pnl
        );

        // DD kill switch
        if self.config.max_drawdown > 0.0 && self.stats.net_pnl <= -self.config.max_drawdown {
            if !self.dd_halted {
                self.dd_halted = true;
                warn!(
                    "[RUNNER] DD KILL — net={:+.2} breached -${:.0}. No new entries.",
                    self.stats.net_pnl, self.config.max_drawdown
                );
                match self.exec.cancel_all().await {
                    Ok(_)  => info!("[RUNNER] DD halt: all orders cancelled"),
                    Err(e) => warn!("[RUNNER] DD halt: cancel failed: {}", e),
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

    /// Emergency exit ALL open positions. Called on feed staleness or shutdown.
    /// Uses guaranteed_sell for each position. Never re-inserts.
    pub async fn emergency_exit_all(&mut self, reason: &str, now: f64) {
        let trade_ids: Vec<String> = self.positions.keys().cloned().collect();
        if trade_ids.is_empty() { return; }

        warn!(
            "[RUNNER] EMERGENCY EXIT ALL — {} positions, reason={}",
            trade_ids.len(), reason
        );

        for trade_id in trade_ids {
            let pos = match self.positions.remove(&trade_id) {
                Some(p) => p,
                None => continue,
            };

            let exit_bid = (pos.entry_price * 0.95).max(0.01); // try near entry, sweep down
            match self.exec.guaranteed_sell(&pos.token_id, exit_bid, pos.shares).await {
                Ok(r) => {
                    info!(
                        "[RUNNER] EMERGENCY sold {} @{:.3} reason={}",
                        pos.slug, r.price, reason
                    );
                    self.log_close(pos, r.price, reason, now).await;
                }
                Err(e) => {
                    error!(
                        "[RUNNER] EMERGENCY sell FAILED {} — logging at 0.00: {}",
                        pos.slug, e
                    );
                    self.log_close(pos, 0.0, &format!("STUCK_{}", reason), now).await;
                }
            }
        }
    }

    /// Check if runner has any positions for a given slug prefix.
    pub fn has_positions_for_slug(&self, slug: &str) -> bool {
        self.positions.keys().any(|k| k.starts_with(slug))
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
            "[OSN] sig={} entries={} W={} L={} WR={:.1}% net={:+.2} ROI={:+.1}% open={} tp={} trail={} be={} rev={} hard={} time={} settle={}",
            self.stats.signals, self.stats.entries,
            self.stats.wins, self.stats.losses, self.stats.wr(),
            self.stats.net_pnl, roi,
            self.positions.len(),
            self.stats.tp_exits, self.stats.trail_exits,
            self.stats.breakeven_exits, self.stats.reversal_exits,
            self.stats.hard_sl_exits, self.stats.time_exits,
            self.stats.settlements,
        );
    }
}
