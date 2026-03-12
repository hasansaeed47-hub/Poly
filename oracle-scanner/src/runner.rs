/// runner.rs — ConfigRunner
///
/// One instance per config (5 total).
/// Receives signals from the shared scan loop.
/// Manages paper positions independently per config.
/// Writes results to a separate JSONL log per config.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::fs::{File, OpenOptions};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::info;

use crate::signal::{Signal, Side};

// ── Config definition ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct RunnerConfig {
    pub name:          String,
    pub min_edge:      f64,
    pub max_secs_left: f64,
    pub stop_loss:     bool,  // exit when current_fair < entry_price
    pub take_profit:   bool,  // exit when book_price >= fair_at_entry (Config 5)
}

// ── Paper position ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PaperPosition {
    pub trade_id:      String,
    pub slug:          String,
    pub asset:         String,
    pub side:          Side,
    pub entry_price:   f64,
    pub fair_at_entry: f64,  // Black-Scholes fair at entry — used as TP target for C5
    pub stake:         f64,
    pub entry_ts:      f64,
    pub window_end:    u64,  // unix timestamp when window closes
}

// ── Trade log entry ───────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct TradeLog {
    pub config:        String,
    pub trade_id:      String,
    pub slug:          String,
    pub asset:         String,
    pub tf:            u32,
    pub side:          String,
    pub entry_price:   f64,
    pub fair_at_entry: f64,
    pub edge_at_entry: f64,
    pub secs_left:     f64,
    pub stake:         f64,
    pub exit_price:    f64,
    pub exit_reason:   String,  // "SETTLEMENT" | "STOP_LOSS" | "TAKE_PROFIT"
    pub pnl:           f64,
    pub fee:           f64,
    pub net_pnl:       f64,
    pub entry_ts:      f64,
    pub exit_ts:       f64,
    pub hold_secs:     f64,
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
}

// ── Runner ────────────────────────────────────────────────────────────────────

pub struct ConfigRunner {
    pub config:    RunnerConfig,
    pub stake:     f64,
    pub fee_rate:  f64,
    positions:     HashMap<String, PaperPosition>, // trade_id → position
    stats:         RunnerStats,
    log_file:      Arc<Mutex<File>>,
}

impl ConfigRunner {
    pub fn new(config: RunnerConfig, stake: f64, fee_rate: f64, log_dir: &str) -> Self {
        let log_path = format!("{}/{}.jsonl", log_dir, config.name.to_lowercase());
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .unwrap_or_else(|e| panic!("Cannot open log {}: {}", log_path, e));

        info!("[{}] Log: {}", config.name, log_path);

        Self {
            config,
            stake,
            fee_rate,
            positions: HashMap::new(),
            stats: RunnerStats::default(),
            log_file: Arc::new(Mutex::new(file)),
        }
    }

    /// Called every tick with a new signal.
    /// Decides whether to enter, and checks existing positions for exit conditions.
    pub async fn on_signal(&mut self, sig: &Signal, window_end: u64) {
        // 1. Check exit conditions on all open positions first
        self.check_exits(sig).await;

        // 2. Decide entry
        self.maybe_enter(sig, window_end).await;
    }

    /// Called at window settlement with the final CL price.
    /// Settles all positions for this slug at the settlement price.
    pub async fn on_settlement(&mut self, slug: &str, cl_settle: f64, settle_ts: f64) {
        let trade_ids: Vec<String> = self
            .positions
            .keys()
            .filter(|k| k.starts_with(slug))
            .cloned()
            .collect();

        for trade_id in trade_ids {
            if let Some(pos) = self.positions.remove(&trade_id) {
                // Settlement: YES=1.0 if CL closed above open, NO=1.0 if below
                // But since PM settles at CL price, the binary outcome is:
                // YES wins if cl_settle > open_price
                // We don't have open_price here — use the fair_at_entry as proxy
                // Better: settlement price IS the token price at close (0 or 1)
                // For paper trading we use: exit_price = 1.0 if we were on winning side
                //
                // Winning condition:
                //   Side::Yes → cl_settle implicitly > open (would need open_price)
                //   We log the actual settle price and compute pnl from 0/1 outcome
                //
                // For now: use fair_at_entry direction as proxy
                // In production: settlement feed will give exact 0/1
                let exit_price = cl_settle; // caller passes 1.0 or 0.0
                self.close_position(pos, exit_price, "SETTLEMENT", settle_ts).await;
            }
        }
    }

    // ── Private ───────────────────────────────────────────────────────────────

    async fn maybe_enter(&mut self, sig: &Signal, window_end: u64) {
        self.stats.signals += 1;

        // Time gate
        if sig.secs_left < 60.0 || sig.secs_left > self.config.max_secs_left {
            return;
        }

        // Edge gate — uses VWAP fill edge (realistic)
        let side = match sig.best_side {
            Some(s) if sig.best_edge >= self.config.min_edge => s,
            _ => return,
        };

        // Check fill price exists (enough liquidity to fill stake)
        let fill_price = match side {
            Side::Yes => sig.fill_yes,
            Side::No  => sig.fill_no,
        };
        let fill_price = match fill_price {
            Some(p) if p > 0.0 => p,
            _ => return, // insufficient liquidity
        };

        // Check minimum depth (at least 2x stake available)
        let depth = match side {
            Side::Yes => sig.depth_yes,
            Side::No  => sig.depth_no,
        };
        if depth < self.stake * 2.0 {
            return; // too thin
        }

        // Already in this slug? (one position per slug per config)
        let already_in = self.positions.keys().any(|k| k.starts_with(&sig.slug));
        if already_in {
            return;
        }

        let now = sig.ts;
        let trade_id = format!("{}-{}-{:.0}", sig.slug, self.config.name, now * 1000.0);

        let pos = PaperPosition {
            trade_id:      trade_id.clone(),
            slug:          sig.slug.clone(),
            asset:         sig.asset.clone(),
            side,
            entry_price:   fill_price,  // VWAP fill, not best ask
            fair_at_entry: sig.best_fair,
            stake:         self.stake,
            entry_ts:      now,
            window_end,
        };

        info!(
            "[{}] ENTER {} {} @{:.3} (best={:.3}) fair={:.3} edge={:.3} depth=${:.0} secs={:.0}",
            self.config.name, sig.slug, side,
            fill_price, sig.best_book, sig.best_fair, sig.best_edge,
            depth, sig.secs_left
        );

        self.positions.insert(trade_id, pos);
        self.stats.entries += 1;
    }

    async fn check_exits(&mut self, sig: &Signal) {
        // Only check positions for this slug
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

            // Get current fair for this side
            let current_fair = match pos.side {
                Side::Yes => sig.fair_yes,
                Side::No  => sig.fair_no,
            };

            // Exit price = best bid (selling back to market)
            let exit_bid = match pos.side {
                Side::Yes => sig.bid_yes,
                Side::No  => sig.bid_no,
            };

            // Config 4: Stop-loss — exit when current_fair < entry_price
            if self.config.stop_loss
                && sig.secs_left > 90.0  // not in final 90s (exit fee not worth it)
                && current_fair < pos.entry_price
                && exit_bid > 0.0        // need a bid to sell into
            {
                let pos = self.positions.remove(&trade_id).unwrap();
                let exit_price = exit_bid;  // sell at best bid
                info!(
                    "[{}] STOP_LOSS {} fair={:.3} < entry={:.3} exit_bid={:.3}",
                    self.config.name, pos.slug, current_fair, pos.entry_price, exit_price
                );
                self.close_position(pos, exit_price, "STOP_LOSS", sig.ts).await;
                continue;
            }

            // Config 5: Take-profit — exit when bid_price >= fair_at_entry
            if self.config.take_profit
                && exit_bid >= pos.fair_at_entry
                && exit_bid > 0.0
            {
                let pos = self.positions.remove(&trade_id).unwrap();
                let exit_price = exit_bid;  // sell at best bid
                info!(
                    "[{}] TAKE_PROFIT {} bid={:.3} >= fair_at_entry={:.3}",
                    self.config.name, pos.slug, exit_price, pos.fair_at_entry
                );
                self.close_position(pos, exit_price, "TAKE_PROFIT", sig.ts).await;
                continue;
            }
        }
    }

    async fn close_position(
        &mut self,
        pos:         PaperPosition,
        exit_price:  f64,
        exit_reason: &str,
        exit_ts:     f64,
    ) {
        let shares = pos.stake / pos.entry_price;

        // Gross PnL: (exit_price - entry_price) * shares
        let gross = (exit_price - pos.entry_price) * shares;

        // Fee: taker fee on entry always
        // For stop_loss / take_profit: also pay taker fee on exit
        // For settlement: no exit fee (binary resolution, no order placed)
        let entry_fee = self.fee_rate * pos.stake;
        let exit_fee  = if exit_reason != "SETTLEMENT" {
            self.fee_rate * (exit_price * shares)
        } else {
            0.0
        };
        let total_fee = entry_fee + exit_fee;

        let net = gross - total_fee;

        // Win/loss determined by net PnL
        if net > 0.0 {
            self.stats.wins += 1;
        } else {
            self.stats.losses += 1;
        }

        self.stats.gross_pnl += gross;
        self.stats.total_fee += total_fee;
        self.stats.net_pnl   += net;

        match exit_reason {
            "SETTLEMENT"   => self.stats.settlement_exits  += 1,
            "STOP_LOSS"    => self.stats.stop_loss_exits   += 1,
            "TAKE_PROFIT"  => self.stats.take_profit_exits += 1,
            _              => {}
        }

        let log = TradeLog {
            config:        self.config.name.clone(),
            trade_id:      pos.trade_id.clone(),
            slug:          pos.slug.clone(),
            asset:         pos.asset.clone(),
            tf:            0, // filled by caller if needed
            side:          pos.side.to_string(),
            entry_price:   pos.entry_price,
            fair_at_entry: pos.fair_at_entry,
            edge_at_entry: pos.fair_at_entry - pos.entry_price,
            secs_left:     0.0, // not stored in pos, logged at entry
            stake:         pos.stake,
            exit_price,
            exit_reason:   exit_reason.to_string(),
            pnl:           gross,
            fee:           total_fee,
            net_pnl:       net,
            entry_ts:      pos.entry_ts,
            exit_ts,
            hold_secs:     exit_ts - pos.entry_ts,
        };

        // Write to JSONL log
        if let Ok(line) = serde_json::to_string(&log) {
            let mut file = self.log_file.lock().await;
            let _ = writeln!(file, "{}", line);
        }

        info!(
            "[{}] CLOSE {} {} exit={:.3} reason={} net={:+.3}",
            self.config.name, pos.slug, pos.side,
            exit_price, exit_reason, net
        );
    }

    pub fn print_stats(&self) {
        let total_staked = self.stats.entries as f64 * self.stake;
        let roi = if total_staked > 0.0 { self.stats.net_pnl / total_staked * 100.0 } else { 0.0 };
        info!(
            "[{}] sig={} entries={} W={} L={} WR={:.1}% net={:+.2} fee={:.2} ROI={:+.1}% open={} settle={} sl={} tp={}",
            self.config.name,
            self.stats.signals,
            self.stats.entries,
            self.stats.wins,
            self.stats.losses,
            self.stats.wr(),
            self.stats.net_pnl,
            self.stats.total_fee,
            roi,
            self.positions.len(),
            self.stats.settlement_exits,
            self.stats.stop_loss_exits,
            self.stats.take_profit_exits,
        );
    }

    #[allow(dead_code)]
    pub fn open_position_count(&self) -> usize {
        self.positions.len()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::{Signal, Side};
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
        let fair_no  = 1.0 - fair_yes;
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
            open_price: 100.0, cl_price: 100.5, sigma: 0.001,
            secs_left: secs, fair_yes, fair_no,
            book_yes, book_no, edge_yes: ey, edge_no: en,
            best_side, best_edge, best_book, best_fair,
            fill_yes: Some(book_yes), fill_no: Some(book_no),
            depth_yes: 100.0, depth_no: 100.0,
            bid_yes: book_yes - 0.01, bid_no: book_no - 0.01,
            edge_fill_yes: ey, edge_fill_no: en,
            ts,
        }
    }

    fn make_tmp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("cl_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn c1_enters_on_edge() {
        let dir = make_tmp_dir();
        let mut runner = ConfigRunner::new(
            make_config("C1_TEST", 0.12, 840.0, false, false),
            5.0, 0.015,
            dir.to_str().unwrap(),
        );
        // fair_yes=0.80, book_yes=0.50 → edge=0.30 → should enter YES
        let sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 1);
    }

    #[tokio::test]
    async fn c2_rejects_outside_time_window() {
        let dir = make_tmp_dir();
        let mut runner = ConfigRunner::new(
            make_config("C2_TEST", 0.12, 180.0, false, false),
            5.0, 0.015,
            dir.to_str().unwrap(),
        );
        // secs_left=300 → above max_secs_left=180 → should NOT enter
        let sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 0);
    }

    #[tokio::test]
    async fn c3_rejects_small_edge() {
        let dir = make_tmp_dir();
        let mut runner = ConfigRunner::new(
            make_config("C3_TEST", 0.25, 840.0, false, false),
            5.0, 0.015,
            dir.to_str().unwrap(),
        );
        // edge=0.15 < min_edge=0.25 → should NOT enter
        let sig = make_signal("btc-updown-5m-test", 0.65, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 0);
    }

    #[tokio::test]
    async fn c4_stop_loss_exits_when_fair_below_entry() {
        let dir = make_tmp_dir();
        let mut runner = ConfigRunner::new(
            make_config("C4_TEST", 0.12, 840.0, true, false),
            5.0, 0.015,
            dir.to_str().unwrap(),
        );
        // Enter: fair_yes=0.80, entry=0.50
        let enter_sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&enter_sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 1);

        // CL reverses: fair_yes drops to 0.30 < entry_price=0.50
        // secs_left=200 > 90 → stop loss should fire
        let reverse_sig = make_signal("btc-updown-5m-test", 0.30, 0.32, 0.67, 200.0, 1010.0);
        runner.on_signal(&reverse_sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 0, "stop loss should have closed position");
    }

    #[tokio::test]
    async fn c5_take_profit_exits_when_book_hits_fair() {
        let dir = make_tmp_dir();
        let mut runner = ConfigRunner::new(
            make_config("C5_TEST", 0.12, 840.0, false, true),
            5.0, 0.015,
            dir.to_str().unwrap(),
        );
        // Enter: fair_yes=0.80, entry=0.50 → fair_at_entry=0.80
        let enter_sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&enter_sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 1);

        // PM reprices: book_yes=0.81 >= fair_at_entry=0.80 → TP fires
        let reprice_sig = make_signal("btc-updown-5m-test", 0.82, 0.81, 0.18, 280.0, 1015.0);
        runner.on_signal(&reprice_sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 0, "take profit should have closed position");
    }

    #[tokio::test]
    async fn settlement_closes_position() {
        let dir = make_tmp_dir();
        let mut runner = ConfigRunner::new(
            make_config("SETTLE_TEST", 0.12, 840.0, false, false),
            5.0, 0.015,
            dir.to_str().unwrap(),
        );
        let sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 1);

        // Settle at 1.0 (YES wins)
        runner.on_settlement("btc-updown-5m-test", 1.0, 1300.0).await;
        assert_eq!(runner.open_position_count(), 0);
    }
}
