/// runner.rs — ConfigRunner (v2 — production-grade)
///
/// v2 fixes:
/// - PaperPosition stores ALL entry data: tf, secs_left, sigma, cl_price, open_price, shares
/// - TradeLog includes every field for full post-analysis (no more 0-values)
/// - Max positions per config and max exposure limits
/// - Entry logging at info level (not debug) with full detail
/// - Exit logging includes all position + market state at exit
/// - Shares computed precisely: stake / entry_price
/// - Log file flushed after every write

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::fs::{File, OpenOptions};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::signal::{Signal, Side};

// ── Config definition ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct RunnerConfig {
    pub name:          String,
    pub min_edge:      f64,
    pub max_secs_left: f64,
    pub stop_loss:     bool,
    pub take_profit:   bool,
}

// ── Paper position (v2 — stores ALL entry state) ──────────────────────────────

#[derive(Debug, Clone)]
pub struct PaperPosition {
    pub trade_id:      String,
    pub slug:          String,
    pub asset:         String,
    pub side:          Side,
    pub tf:            u32,
    pub entry_price:   f64,     // best_ask at entry (what we paid)
    pub entry_bid:     f64,     // best_bid at entry (immediate exit price)
    pub fair_at_entry: f64,     // BS fair value at entry
    pub edge_at_entry: f64,     // fair - entry_price
    pub sigma_at_entry: f64,    // volatility at entry
    pub cl_at_entry:   f64,     // CL oracle price at entry
    pub open_price:    f64,     // window open price
    pub cl_vs_open:    f64,     // CL vs open % at entry
    pub secs_left_at_entry: f64,
    pub shares:        f64,     // stake / entry_price (exact share count)
    pub stake:         f64,
    pub entry_ts:      f64,
    pub window_end:    u64,
    // Book state at entry (both sides)
    pub book_yes_at_entry: f64,   // best ask YES at entry
    pub book_no_at_entry:  f64,   // best ask NO at entry
    pub bid_yes_at_entry:  f64,   // best bid YES at entry
    pub bid_no_at_entry:   f64,   // best bid NO at entry
    pub book_spread:   f64,
    pub book_depth:    f64,     // ask depth at best
    pub book_age:      f64,     // seconds since last book update
}

// ── Trade log entry (v2 — every field populated) ──────────────────────────────

#[derive(Debug, Serialize)]
pub struct TradeLog {
    // Identity
    pub config:           String,
    pub trade_id:         String,
    pub slug:             String,
    pub asset:            String,
    pub tf:               u32,
    pub side:             String,
    // Entry state
    pub entry_price:      f64,
    pub entry_bid:        f64,
    pub fair_at_entry:    f64,
    pub edge_at_entry:    f64,
    pub sigma_at_entry:   f64,
    pub cl_at_entry:      f64,
    pub open_price:       f64,
    pub cl_vs_open_entry: f64,
    pub secs_left_entry:  f64,
    pub shares:           f64,
    pub stake:            f64,
    pub book_spread_entry: f64,
    pub book_depth_entry:  f64,
    pub book_age_entry:    f64,
    // Entry book state (both sides)
    pub book_yes_at_entry: f64,
    pub book_no_at_entry:  f64,
    pub bid_yes_at_entry:  f64,
    pub bid_no_at_entry:   f64,
    // Exit state
    pub exit_price:       f64,
    pub exit_reason:      String,
    pub fair_at_exit:     f64,
    pub cl_at_exit:       f64,
    pub exit_sigma:       f64,
    pub exit_book_yes:    f64,
    pub exit_book_no:     f64,
    pub exit_bid_yes:     f64,
    pub exit_bid_no:      f64,
    pub secs_left_exit:   f64,
    // PnL
    pub gross_pnl:        f64,
    pub entry_fee:        f64,
    pub exit_fee:         f64,
    pub total_fee:        f64,
    pub net_pnl:          f64,
    pub roi_pct:          f64,   // net_pnl / stake * 100
    // Timing
    pub entry_ts:         f64,
    pub exit_ts:          f64,
    pub hold_secs:        f64,
    pub window_end:       u64,
}

// ── Stats ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
pub struct RunnerStats {
    pub signals:   u64,
    pub entries:   u64,
    pub wins:      u64,
    pub losses:    u64,
    pub gross_pnl: f64,
    pub total_fee: f64,
    pub net_pnl:   f64,
    pub max_drawdown:    f64,
    pub peak_pnl:        f64,
    pub total_exposure:  f64,
    pub rejected_edge:   u64,
    pub rejected_time:   u64,
    pub rejected_dup:    u64,
    pub rejected_limit:  u64,
    // Exit type breakdown
    pub settlement_exits:   u64,
    pub stop_loss_exits:    u64,
    pub take_profit_exits:  u64,
}

impl RunnerStats {
    pub fn wr(&self) -> f64 {
        let settled = self.wins + self.losses;
        if settled == 0 { 0.0 } else { self.wins as f64 / settled as f64 * 100.0 }
    }

    pub fn avg_pnl(&self) -> f64 {
        let settled = self.wins + self.losses;
        if settled == 0 { 0.0 } else { self.net_pnl / settled as f64 }
    }

    fn update_drawdown(&mut self) {
        if self.net_pnl > self.peak_pnl {
            self.peak_pnl = self.net_pnl;
        }
        let dd = self.peak_pnl - self.net_pnl;
        if dd > self.max_drawdown {
            self.max_drawdown = dd;
        }
    }
}

// ── Mark-to-market log ────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct MtmLog {
    pub ts:             f64,
    pub config:         String,
    pub trade_id:       String,
    pub slug:           String,
    pub side:           String,
    pub entry_price:    f64,
    pub shares:         f64,
    pub current_fair:   f64,
    pub current_bid:    f64,
    pub unrealised_pnl: f64,
    pub secs_left:      f64,
}

// ── Runner ────────────────────────────────────────────────────────────────────

pub struct ConfigRunner {
    pub config:           RunnerConfig,
    pub stake:            f64,
    pub fee_rate:         f64,
    pub max_positions:    usize,
    pub max_exposure:     f64,
    positions:            HashMap<String, PaperPosition>,
    stats:                RunnerStats,
    log_file:             Arc<Mutex<File>>,
}

impl ConfigRunner {
    pub fn new(
        config: RunnerConfig,
        stake: f64,
        fee_rate: f64,
        max_positions: usize,
        max_exposure: f64,
        log_dir: &str,
    ) -> Self {
        let log_path = format!("{}/{}.jsonl", log_dir, config.name.to_lowercase());
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .unwrap_or_else(|e| panic!("Cannot open log {}: {}", log_path, e));

        info!(
            "[{}] Initialized: stake={} fee={} max_pos={} max_exp={} log={}",
            config.name, stake, fee_rate, max_positions, max_exposure, log_path
        );

        Self {
            config,
            stake,
            fee_rate,
            max_positions,
            max_exposure,
            positions: HashMap::new(),
            stats: RunnerStats::default(),
            log_file: Arc::new(Mutex::new(file)),
        }
    }

    /// Called every tick with a new signal.
    pub async fn on_signal(&mut self, sig: &Signal, window_end: u64) {
        self.check_exits(sig).await;
        self.maybe_enter(sig, window_end).await;
    }

    /// Called at window settlement.
    pub async fn on_settlement(
        &mut self,
        slug: &str,
        outcome: f64,      // 1.0 for YES wins, 0.0 for NO wins
        cl_settle: f64,    // final CL price
        settle_ts: f64,
        secs_left: f64,
    ) {
        let trade_ids: Vec<String> = self
            .positions
            .keys()
            .filter(|k| k.starts_with(slug))
            .cloned()
            .collect();

        for trade_id in trade_ids {
            if let Some(pos) = self.positions.remove(&trade_id) {
                // Settlement exit price:
                // If we hold YES and outcome=1.0 → exit at 1.0 (full payout)
                // If we hold YES and outcome=0.0 → exit at 0.0 (total loss)
                // If we hold NO  and outcome=0.0 → exit at 1.0 (NO wins = payout)
                // If we hold NO  and outcome=1.0 → exit at 0.0 (NO loses)
                let exit_price = match pos.side {
                    Side::Yes => outcome,
                    Side::No  => 1.0 - outcome,
                };

                info!(
                    "[{}] SETTLE {} {} outcome={} exit_price={:.4} cl_settle={:.6}",
                    self.config.name, pos.slug, pos.side,
                    if outcome == 1.0 { "YES" } else { "NO" },
                    exit_price, cl_settle
                );

                self.close_position(pos, exit_price, "SETTLEMENT", settle_ts, cl_settle, secs_left, 0.0, None).await;
            }
        }
    }

    // ── Private ───────────────────────────────────────────────────────────────

    async fn maybe_enter(&mut self, sig: &Signal, window_end: u64) {
        self.stats.signals += 1;

        // Time gate: reject if outside [60s, max_secs_left]
        if sig.secs_left < 60.0 {
            return;
        }
        if sig.secs_left > self.config.max_secs_left as f64 {
            self.stats.rejected_time += 1;
            return;
        }

        // Edge gate
        let side = match sig.best_side {
            Some(s) if sig.best_edge >= self.config.min_edge => s,
            Some(_) => {
                self.stats.rejected_edge += 1;
                return;
            }
            None => return,
        };

        // Already in this slug? (one position per slug per config)
        let already_in = self.positions.values().any(|p| p.slug == sig.slug);
        if already_in {
            self.stats.rejected_dup += 1;
            return;
        }

        // Position limit
        if self.positions.len() >= self.max_positions {
            self.stats.rejected_limit += 1;
            debug!(
                "[{}] REJECT {} — at max positions ({})",
                self.config.name, sig.slug, self.max_positions
            );
            return;
        }

        // Exposure limit
        let current_exposure: f64 = self.positions.values().map(|p| p.stake).sum();
        if current_exposure + self.stake > self.max_exposure {
            self.stats.rejected_limit += 1;
            debug!(
                "[{}] REJECT {} — exposure {:.2}+{:.2} > max {:.2}",
                self.config.name, sig.slug, current_exposure, self.stake, self.max_exposure
            );
            return;
        }

        let entry_price = sig.best_book;
        let shares = self.stake / entry_price;
        let (book_age, book_spread, book_depth, entry_bid) = match side {
            Side::Yes => (sig.book_age_yes, sig.spread_yes, sig.ask_depth_yes, sig.bid_yes),
            Side::No  => (sig.book_age_no,  sig.spread_no,  sig.ask_depth_no,  sig.bid_no),
        };

        let trade_id = format!("{}-{}-{:.0}", sig.slug, self.config.name, sig.ts * 1000.0);

        let pos = PaperPosition {
            trade_id:      trade_id.clone(),
            slug:          sig.slug.clone(),
            asset:         sig.asset.clone(),
            side,
            tf:            sig.tf,
            entry_price,
            entry_bid,
            fair_at_entry: sig.best_fair,
            edge_at_entry: sig.best_edge,
            sigma_at_entry: sig.sigma,
            cl_at_entry:   sig.cl_price,
            open_price:    sig.open_price,
            cl_vs_open:    sig.cl_vs_open,
            secs_left_at_entry: sig.secs_left,
            shares,
            stake:         self.stake,
            entry_ts:      sig.ts,
            window_end,
            book_yes_at_entry: sig.book_yes,
            book_no_at_entry:  sig.book_no,
            bid_yes_at_entry:  sig.bid_yes,
            bid_no_at_entry:   sig.bid_no,
            book_spread,
            book_depth,
            book_age,
        };

        info!(
            "[{}] ENTER {} {} @{:.4} fair={:.4} edge={:.4} shares={:.4} cl={:.6} open={:.6} cl_vs_open={:+.4}% sigma={:.6} secs={:.1} spread={:.4} depth={:.1} book_age={:.1}s",
            self.config.name, sig.slug, side,
            entry_price, sig.best_fair, sig.best_edge, shares,
            sig.cl_price, sig.open_price, sig.cl_vs_open,
            sig.sigma, sig.secs_left,
            book_spread, book_depth, book_age
        );

        self.stats.entries += 1;
        self.stats.total_exposure += self.stake;
        self.positions.insert(trade_id, pos);
    }

    async fn check_exits(&mut self, sig: &Signal) {
        let trade_ids: Vec<String> = self
            .positions
            .keys()
            .filter(|k| k.starts_with(&sig.slug))
            .cloned()
            .collect();

        for trade_id in trade_ids {
            let pos = match self.positions.get(&trade_id) {
                Some(p) => p.clone(),
                None    => continue,
            };

            let (current_fair, current_book, current_bid) = match pos.side {
                Side::Yes => (sig.fair_yes, sig.book_yes, sig.bid_yes),
                Side::No  => (sig.fair_no,  sig.book_no,  sig.bid_no),
            };

            // Config 4: Stop-loss — exit when current_fair < entry_price
            if self.config.stop_loss
                && sig.secs_left > 90.0
                && current_fair < pos.entry_price
            {
                let pos = self.positions.remove(&trade_id).unwrap();
                // Exit at bid (selling back), not ask
                let exit_price = current_bid.max(0.001);
                info!(
                    "[{}] STOP_LOSS {} {} fair={:.4} < entry={:.4} exit_bid={:.4} cl={:.6} secs={:.1}",
                    self.config.name, pos.slug, pos.side,
                    current_fair, pos.entry_price, exit_price,
                    sig.cl_price, sig.secs_left
                );
                self.close_position(pos, exit_price, "STOP_LOSS", sig.ts, sig.cl_price, sig.secs_left, current_fair, Some(sig)).await;
                continue;
            }

            // Config 5: Take-profit — exit when book_price >= fair_at_entry
            if self.config.take_profit
                && current_bid >= pos.fair_at_entry
            {
                let pos = self.positions.remove(&trade_id).unwrap();
                let exit_price = current_bid;
                info!(
                    "[{}] TAKE_PROFIT {} {} bid={:.4} >= fair_at_entry={:.4} cl={:.6} secs={:.1}",
                    self.config.name, pos.slug, pos.side,
                    current_bid, pos.fair_at_entry,
                    sig.cl_price, sig.secs_left
                );
                self.close_position(pos, exit_price, "TAKE_PROFIT", sig.ts, sig.cl_price, sig.secs_left, current_fair, Some(sig)).await;
                continue;
            }

            // Log position status every ~30s (60 ticks at 500ms)
            let age = sig.ts - pos.entry_ts;
            if age > 0.0 && ((age * 2.0) as u64) % 60 == 0 {
                let unrealized = (current_book - pos.entry_price) * pos.shares;
                debug!(
                    "[{}] HOLD {} {} age={:.0}s fair={:.4} book={:.4} unreal={:+.4} cl={:.6}",
                    self.config.name, pos.slug, pos.side,
                    age, current_fair, current_book, unrealized, sig.cl_price
                );
            }
        }
    }

    async fn close_position(
        &mut self,
        pos:          PaperPosition,
        exit_price:   f64,
        exit_reason:  &str,
        exit_ts:      f64,
        cl_at_exit:   f64,
        secs_left:    f64,
        fair_at_exit: f64,
        exit_sig:     Option<&Signal>,
    ) {
        let shares = pos.shares;

        // Gross PnL: (exit_price - entry_price) * shares
        let gross = (exit_price - pos.entry_price) * shares;

        // Fee: taker fee on entry always
        // For stop_loss / take_profit: also pay taker fee on exit
        // For settlement: no exit fee (binary resolution, no order placed)
        let entry_fee = self.fee_rate * pos.stake;
        let exit_fee = if exit_reason != "SETTLEMENT" {
            self.fee_rate * (exit_price * shares)
        } else {
            0.0
        };
        let total_fee = entry_fee + exit_fee;
        let net = gross - total_fee;
        let roi_pct = if pos.stake > 0.0 { net / pos.stake * 100.0 } else { 0.0 };
        let hold_secs = exit_ts - pos.entry_ts;

        if net > 0.0 {
            self.stats.wins += 1;
        } else {
            self.stats.losses += 1;
        }

        self.stats.gross_pnl += gross;
        self.stats.total_fee += total_fee;
        self.stats.net_pnl   += net;
        self.stats.update_drawdown();

        match exit_reason {
            "SETTLEMENT"   => self.stats.settlement_exits  += 1,
            "STOP_LOSS"    => self.stats.stop_loss_exits   += 1,
            "TAKE_PROFIT"  => self.stats.take_profit_exits += 1,
            _              => {}
        }

        // Extract exit book state from signal if available
        let (exit_sigma, exit_book_yes, exit_book_no, exit_bid_yes, exit_bid_no) =
            match exit_sig {
                Some(s) => (s.sigma, s.book_yes, s.book_no, s.bid_yes, s.bid_no),
                None    => (0.0, 0.0, 0.0, 0.0, 0.0),
            };

        let log = TradeLog {
            config:            self.config.name.clone(),
            trade_id:          pos.trade_id.clone(),
            slug:              pos.slug.clone(),
            asset:             pos.asset.clone(),
            tf:                pos.tf,
            side:              pos.side.to_string(),
            entry_price:       pos.entry_price,
            entry_bid:         pos.entry_bid,
            fair_at_entry:     pos.fair_at_entry,
            edge_at_entry:     pos.edge_at_entry,
            sigma_at_entry:    pos.sigma_at_entry,
            cl_at_entry:       pos.cl_at_entry,
            open_price:        pos.open_price,
            cl_vs_open_entry:  pos.cl_vs_open,
            secs_left_entry:   pos.secs_left_at_entry,
            shares:            pos.shares,
            stake:             pos.stake,
            book_spread_entry: pos.book_spread,
            book_depth_entry:  pos.book_depth,
            book_age_entry:    pos.book_age,
            book_yes_at_entry: pos.book_yes_at_entry,
            book_no_at_entry:  pos.book_no_at_entry,
            bid_yes_at_entry:  pos.bid_yes_at_entry,
            bid_no_at_entry:   pos.bid_no_at_entry,
            exit_price,
            exit_reason:       exit_reason.to_string(),
            fair_at_exit,
            cl_at_exit,
            exit_sigma,
            exit_book_yes,
            exit_book_no,
            exit_bid_yes,
            exit_bid_no,
            secs_left_exit:    secs_left,
            gross_pnl:         gross,
            entry_fee,
            exit_fee,
            total_fee,
            net_pnl:           net,
            roi_pct,
            entry_ts:          pos.entry_ts,
            exit_ts,
            hold_secs,
            window_end:        pos.window_end,
        };

        // Write to JSONL log and flush immediately
        if let Ok(line) = serde_json::to_string(&log) {
            let mut file = self.log_file.lock().await;
            if let Err(e) = writeln!(file, "{}", line) {
                warn!("[{}] Failed to write trade log: {}", self.config.name, e);
            }
            if let Err(e) = file.flush() {
                warn!("[{}] Failed to flush trade log: {}", self.config.name, e);
            }
        }

        info!(
            "[{}] CLOSE {} {} exit={:.4} reason={} gross={:+.4} fee={:.4} net={:+.4} roi={:+.2}% hold={:.1}s shares={:.4} cl_exit={:.6}",
            self.config.name, pos.slug, pos.side,
            exit_price, exit_reason,
            gross, total_fee, net, roi_pct, hold_secs, shares, cl_at_exit
        );
    }

    pub fn print_stats(&self) {
        let open_count = self.positions.len();
        let open_exposure: f64 = self.positions.values().map(|p| p.stake).sum();
        info!(
            "[{}] sig={} ent={} W={} L={} WR={:.1}% net={:+.3} gross={:+.3} fee={:.3} avg={:+.3} dd={:.3} open={} exp={:.2} rej_edge={} rej_time={} rej_dup={} rej_lim={} settle={} sl={} tp={}",
            self.config.name,
            self.stats.signals,
            self.stats.entries,
            self.stats.wins,
            self.stats.losses,
            self.stats.wr(),
            self.stats.net_pnl,
            self.stats.gross_pnl,
            self.stats.total_fee,
            self.stats.avg_pnl(),
            self.stats.max_drawdown,
            open_count,
            open_exposure,
            self.stats.rejected_edge,
            self.stats.rejected_time,
            self.stats.rejected_dup,
            self.stats.rejected_limit,
            self.stats.settlement_exits,
            self.stats.stop_loss_exits,
            self.stats.take_profit_exits,
        );
    }

    pub fn open_position_count(&self) -> usize {
        self.positions.len()
    }

    pub fn open_exposure(&self) -> f64 {
        self.positions.values().map(|p| p.stake).sum()
    }

    /// Generate mark-to-market log entries for all open positions
    pub fn get_mtm_entries(&self, sig: &Signal) -> Vec<MtmLog> {
        self.positions
            .values()
            .filter(|p| p.slug == sig.slug)
            .map(|pos| {
                let (current_fair, current_bid) = match pos.side {
                    Side::Yes => (sig.fair_yes, sig.bid_yes),
                    Side::No  => (sig.fair_no,  sig.bid_no),
                };
                let unrealised_pnl = (current_bid - pos.entry_price) * pos.shares;
                MtmLog {
                    ts:             sig.ts,
                    config:         self.config.name.clone(),
                    trade_id:       pos.trade_id.clone(),
                    slug:           pos.slug.clone(),
                    side:           pos.side.to_string(),
                    entry_price:    pos.entry_price,
                    shares:         pos.shares,
                    current_fair,
                    current_bid,
                    unrealised_pnl,
                    secs_left:      sig.secs_left,
                }
            })
            .collect()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::{Signal, Side, BookInput};
    use tempfile::tempdir;

    fn make_config(name: &str, min_edge: f64, max_secs: f64, sl: bool, tp: bool) -> RunnerConfig {
        RunnerConfig {
            name:          name.to_string(),
            min_edge,
            max_secs_left: max_secs,
            stop_loss:     sl,
            take_profit:   tp,
        }
    }

    fn make_signal(slug: &str, fair_yes: f64, book_yes: f64, book_no: f64, secs: f64, ts: f64) -> Signal {
        let fair_no = 1.0 - fair_yes;
        let ey = fair_yes - book_yes;
        let en = fair_no  - book_no;
        let (best_side, best_edge, best_book, best_fair) = if ey > en && ey > 0.0 {
            (Some(Side::Yes), ey, book_yes, fair_yes)
        } else if en > ey && en > 0.0 {
            (Some(Side::No), en, book_no, fair_no)
        } else {
            (None, 0.0, 0.0, 0.0)
        };
        Signal {
            slug: slug.to_string(), asset: "btc".to_string(), tf: 5,
            open_price: 100.0, cl_price: 100.5, cl_vs_open: 0.5, sigma: 0.001,
            secs_left: secs, fair_yes, fair_no,
            book_yes, book_no,
            bid_yes: book_yes - 0.02, bid_no: book_no - 0.02,
            ask_depth_yes: 10.0, ask_depth_no: 10.0,
            bid_depth_yes: 10.0, bid_depth_no: 10.0,
            spread_yes: 0.02, spread_no: 0.02,
            edge_yes: ey, edge_no: en,
            best_side, best_edge, best_book, best_fair,
            book_age_yes: 1.0, book_age_no: 1.0,
            ts,
        }
    }

    #[tokio::test]
    async fn c1_enters_on_edge() {
        let dir = tempdir().unwrap();
        let mut runner = ConfigRunner::new(
            make_config("C1_TEST", 0.12, 840.0, false, false),
            5.0, 0.015, 10, 100.0,
            dir.path().to_str().unwrap(),
        );
        let sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 1);
    }

    #[tokio::test]
    async fn c2_rejects_outside_time_window() {
        let dir = tempdir().unwrap();
        let mut runner = ConfigRunner::new(
            make_config("C2_TEST", 0.12, 180.0, false, false),
            5.0, 0.015, 10, 100.0,
            dir.path().to_str().unwrap(),
        );
        let sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 0);
    }

    #[tokio::test]
    async fn c3_rejects_small_edge() {
        let dir = tempdir().unwrap();
        let mut runner = ConfigRunner::new(
            make_config("C3_TEST", 0.25, 840.0, false, false),
            5.0, 0.015, 10, 100.0,
            dir.path().to_str().unwrap(),
        );
        let sig = make_signal("btc-updown-5m-test", 0.65, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 0);
    }

    #[tokio::test]
    async fn c4_stop_loss_exits_when_fair_below_entry() {
        let dir = tempdir().unwrap();
        let mut runner = ConfigRunner::new(
            make_config("C4_TEST", 0.12, 840.0, true, false),
            5.0, 0.015, 10, 100.0,
            dir.path().to_str().unwrap(),
        );
        let enter_sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&enter_sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 1);

        let reverse_sig = make_signal("btc-updown-5m-test", 0.30, 0.32, 0.67, 200.0, 1010.0);
        runner.on_signal(&reverse_sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 0, "stop loss should have closed position");
    }

    #[tokio::test]
    async fn c5_take_profit_exits_when_bid_hits_fair() {
        let dir = tempdir().unwrap();
        let mut runner = ConfigRunner::new(
            make_config("C5_TEST", 0.12, 840.0, false, true),
            5.0, 0.015, 10, 100.0,
            dir.path().to_str().unwrap(),
        );
        let enter_sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&enter_sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 1);

        // bid_yes = 0.81 - 0.02 = 0.79; fair_at_entry = 0.80
        // Need bid >= fair_at_entry (0.80), so book_yes must be 0.82+ (bid = 0.82-0.02=0.80)
        let mut reprice_sig = make_signal("btc-updown-5m-test", 0.83, 0.83, 0.18, 280.0, 1015.0);
        reprice_sig.bid_yes = 0.81; // explicitly set bid >= fair_at_entry
        runner.on_signal(&reprice_sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 0, "take profit should have closed position");
    }

    #[tokio::test]
    async fn settlement_closes_position() {
        let dir = tempdir().unwrap();
        let mut runner = ConfigRunner::new(
            make_config("SETTLE_TEST", 0.12, 840.0, false, false),
            5.0, 0.015, 10, 100.0,
            dir.path().to_str().unwrap(),
        );
        let sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 1);

        runner.on_settlement("btc-updown-5m-test", 1.0, 100.5, 1300.0, 0.0).await;
        assert_eq!(runner.open_position_count(), 0);
    }

    #[tokio::test]
    async fn max_positions_enforced() {
        let dir = tempdir().unwrap();
        let mut runner = ConfigRunner::new(
            make_config("LIM_TEST", 0.12, 840.0, false, false),
            5.0, 0.015, 1, 100.0,  // max 1 position
            dir.path().to_str().unwrap(),
        );
        let sig1 = make_signal("btc-updown-5m-test1", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig1, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 1);

        let sig2 = make_signal("btc-updown-5m-test2", 0.80, 0.50, 0.49, 300.0, 1001.0);
        runner.on_signal(&sig2, 1001 + 300).await;
        assert_eq!(runner.open_position_count(), 1, "should reject due to max_positions=1");
    }

    #[tokio::test]
    async fn max_exposure_enforced() {
        let dir = tempdir().unwrap();
        let mut runner = ConfigRunner::new(
            make_config("EXP_TEST", 0.12, 840.0, false, false),
            5.0, 0.015, 10, 7.0,  // max $7 exposure, stake=$5 → only 1 position
            dir.path().to_str().unwrap(),
        );
        let sig1 = make_signal("btc-updown-5m-test1", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig1, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 1);

        let sig2 = make_signal("btc-updown-5m-test2", 0.80, 0.50, 0.49, 300.0, 1001.0);
        runner.on_signal(&sig2, 1001 + 300).await;
        assert_eq!(runner.open_position_count(), 1, "should reject due to exposure limit");
    }

    #[tokio::test]
    async fn settlement_yes_side_pays_correctly() {
        let dir = tempdir().unwrap();
        let mut runner = ConfigRunner::new(
            make_config("PNL_TEST", 0.12, 840.0, false, false),
            5.0, 0.0, 10, 100.0, // zero fee for easy PnL check
            dir.path().to_str().unwrap(),
        );
        let sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;

        // YES wins → exit at 1.0. Entry was 0.50, shares=5/0.50=10
        // PnL = (1.0 - 0.50) * 10 = 5.0
        runner.on_settlement("btc-updown-5m-test", 1.0, 101.0, 1300.0, 0.0).await;
        assert!((runner.stats.net_pnl - 5.0).abs() < 0.01, "net_pnl should be ~5.0, got {}", runner.stats.net_pnl);
    }
}
