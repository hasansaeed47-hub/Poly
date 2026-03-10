/// runner.rs — ConfigRunner
///
/// One instance per config (10 total: 5 per timeframe).
/// Each runner only processes signals matching its configured timeframe.
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

// -- Config definition --------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct RunnerConfig {
    pub name:          String,
    pub tf:            u32,       // timeframe in minutes — only processes matching markets
    pub min_edge:      f64,
    pub max_secs_left: f64,
    pub min_secs:      f64,       // minimum seconds remaining to consider entry
    pub stop_loss:     bool,
    pub take_profit:   bool,
}

// -- Paper position -----------------------------------------------------------

#[derive(Debug, Clone)]
pub struct PaperPosition {
    pub trade_id:        String,
    pub slug:            String,
    pub asset:           String,
    pub side:            Side,
    pub entry_price:     f64,
    pub fair_at_entry:   f64,
    pub edge_at_entry:   f64,
    pub secs_left_entry: f64,
    pub stake:           f64,
    pub entry_ts:        f64,
    pub window_end:      u64,
}

// -- Trade log entry ----------------------------------------------------------

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
    pub exit_reason:   String,
    pub pnl:           f64,
    pub fee:           f64,
    pub net_pnl:       f64,
    pub entry_ts:      f64,
    pub exit_ts:       f64,
    pub hold_secs:     f64,
}

// -- Stats --------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
pub struct RunnerStats {
    pub signals:   u64,
    pub entries:   u64,
    pub wins:      u64,
    pub losses:    u64,
    pub gross_pnl: f64,
    pub total_fee: f64,
    pub net_pnl:   f64,
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

// -- Runner -------------------------------------------------------------------

pub struct ConfigRunner {
    pub config:    RunnerConfig,
    pub stake:     f64,
    pub fee_rate:  f64,
    positions:     HashMap<String, PaperPosition>,
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

    pub async fn on_signal(&mut self, sig: &Signal, window_end: u64) {
        // Only process signals for this runner's timeframe
        if sig.tf != self.config.tf {
            return;
        }
        self.check_exits(sig).await;
        self.maybe_enter(sig, window_end).await;
    }

    pub async fn on_settlement(&mut self, slug: &str, cl_settle: f64, settle_ts: f64) {
        let trade_ids: Vec<String> = self
            .positions
            .keys()
            .filter(|k| k.starts_with(slug))
            .cloned()
            .collect();

        for trade_id in trade_ids {
            if let Some(pos) = self.positions.remove(&trade_id) {
                let exit_price = cl_settle; // caller passes 1.0 or 0.0
                self.close_position(pos, exit_price, "SETTLEMENT", settle_ts).await;
            }
        }
    }

    // -- Private --------------------------------------------------------------

    async fn maybe_enter(&mut self, sig: &Signal, window_end: u64) {
        self.stats.signals += 1;

        // Time gate — per-config min/max window
        if sig.secs_left < self.config.min_secs || sig.secs_left > self.config.max_secs_left {
            return;
        }

        // Edge gate — sig.best_edge is already fee-adjusted from signal::compute
        let side = match sig.best_side {
            Some(s) if sig.best_edge >= self.config.min_edge => s,
            _ => return,
        };

        // One position per slug per config
        let already_in = self.positions.keys().any(|k| k.starts_with(&sig.slug));
        if already_in {
            return;
        }

        let now = sig.ts;
        let trade_id = format!("{}-{}-{:.0}", sig.slug, self.config.name, now * 1000.0);

        let pos = PaperPosition {
            trade_id:        trade_id.clone(),
            slug:            sig.slug.clone(),
            asset:           sig.asset.clone(),
            side,
            entry_price:     sig.best_book,
            fair_at_entry:   sig.best_fair,
            edge_at_entry:   sig.best_edge,
            secs_left_entry: sig.secs_left,
            stake:           self.stake,
            entry_ts:        now,
            window_end,
        };

        info!(
            "[{}] ENTER {} {} @{:.3} fair={:.3} edge={:.3} secs={:.0}",
            self.config.name, sig.slug, side,
            sig.best_book, sig.best_fair, sig.best_edge, sig.secs_left
        );

        self.positions.insert(trade_id, pos);
        self.stats.entries += 1;
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

            let current_fair = match pos.side {
                Side::Yes => sig.fair_yes,
                Side::No  => sig.fair_no,
            };

            let current_book = match pos.side {
                Side::Yes => sig.book_yes,
                Side::No  => sig.book_no,
            };

            // Stop-loss: exit when current fair < entry price.
            // Disabled in the last 30% of the window — near expiry, hold to settlement.
            let sl_cutoff = self.config.tf as f64 * 60.0 * 0.3;
            if self.config.stop_loss
                && sig.secs_left > sl_cutoff
                && current_fair < pos.entry_price
            {
                let pos = self.positions.remove(&trade_id).unwrap();
                let exit_price = current_book.max(0.001);
                info!(
                    "[{}] STOP_LOSS {} fair={:.3} < entry={:.3}",
                    self.config.name, pos.slug, current_fair, pos.entry_price
                );
                self.close_position(pos, exit_price, "STOP_LOSS", sig.ts).await;
                continue;
            }

            // Take-profit: exit when book >= fair_at_entry
            if self.config.take_profit
                && current_book >= pos.fair_at_entry
            {
                let pos = self.positions.remove(&trade_id).unwrap();
                let exit_price = current_book;
                info!(
                    "[{}] TAKE_PROFIT {} book={:.3} >= fair_at_entry={:.3}",
                    self.config.name, pos.slug, current_book, pos.fair_at_entry
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

        let gross = (exit_price - pos.entry_price) * shares;

        // Fee: taker fee on entry always.
        // For SL/TP exits: also pay taker fee on exit (selling back into book).
        // For settlement: no exit fee (binary resolves automatically).
        let entry_fee = self.fee_rate * pos.stake;
        let exit_fee  = if exit_reason != "SETTLEMENT" {
            self.fee_rate * (exit_price * shares)
        } else {
            0.0
        };
        let total_fee = entry_fee + exit_fee;

        let net = gross - total_fee;

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
            tf:            self.config.tf,
            side:          pos.side.to_string(),
            entry_price:   pos.entry_price,
            fair_at_entry: pos.fair_at_entry,
            edge_at_entry: pos.edge_at_entry,
            secs_left:     pos.secs_left_entry,
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
        info!(
            "[{}] tf={}m sig={} entries={} W={} L={} WR={:.1}% net={:+.2} fee={:.2} settle={} sl={} tp={}",
            self.config.name,
            self.config.tf,
            self.stats.signals,
            self.stats.entries,
            self.stats.wins,
            self.stats.losses,
            self.stats.wr(),
            self.stats.net_pnl,
            self.stats.total_fee,
            self.stats.settlement_exits,
            self.stats.stop_loss_exits,
            self.stats.take_profit_exits,
        );
    }

    pub fn open_position_count(&self) -> usize {
        self.positions.len()
    }
}

// -- Tests --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::{Signal, Side};
    use tempfile::tempdir;

    fn make_config(name: &str, tf: u32, min_edge: f64, max_secs: f64, min_secs: f64, sl: bool, tp: bool) -> RunnerConfig {
        RunnerConfig {
            name:          name.to_string(),
            tf,
            min_edge,
            max_secs_left: max_secs,
            min_secs,
            stop_loss:     sl,
            take_profit:   tp,
        }
    }

    /// Build a test signal. Edge values are fee-adjusted (as compute() now does).
    fn make_signal(slug: &str, fair_yes: f64, book_yes: f64, book_no: f64, secs: f64, ts: f64) -> Signal {
        let fair_no  = 1.0 - fair_yes;
        // Fee-adjusted edge (matching new signal::compute behaviour)
        let fee_rate = 0.015;
        let ey = fair_yes - book_yes * (1.0 + fee_rate);
        let en = fair_no  - book_no  * (1.0 + fee_rate);
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
            best_side, best_edge, best_book, best_fair, ts,
        }
    }

    #[tokio::test]
    async fn c1_enters_on_edge() {
        let dir = tempdir().unwrap();
        let mut runner = ConfigRunner::new(
            make_config("C1_TEST", 5, 0.12, 840.0, 20.0, false, false),
            5.0, 0.015,
            dir.path().to_str().unwrap(),
        );
        // fair_yes=0.80, book_yes=0.50 → fee-adjusted edge = 0.80 - 0.5075 = 0.2925
        let sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 1);
    }

    #[tokio::test]
    async fn c2_rejects_outside_time_window() {
        let dir = tempdir().unwrap();
        let mut runner = ConfigRunner::new(
            make_config("C2_TEST", 5, 0.12, 180.0, 20.0, false, false),
            5.0, 0.015,
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
            make_config("C3_TEST", 5, 0.25, 840.0, 20.0, false, false),
            5.0, 0.015,
            dir.path().to_str().unwrap(),
        );
        // fair_yes=0.65, book=0.50 → fee-adj edge = 0.65 - 0.5075 = 0.1425 < 0.25
        let sig = make_signal("btc-updown-5m-test", 0.65, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 0);
    }

    #[tokio::test]
    async fn c4_stop_loss_exits_when_fair_below_entry() {
        let dir = tempdir().unwrap();
        let mut runner = ConfigRunner::new(
            make_config("C4_TEST", 5, 0.12, 840.0, 20.0, true, false),
            5.0, 0.015,
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
    async fn c5_take_profit_exits_when_book_hits_fair() {
        let dir = tempdir().unwrap();
        let mut runner = ConfigRunner::new(
            make_config("C5_TEST", 5, 0.12, 840.0, 20.0, false, true),
            5.0, 0.015,
            dir.path().to_str().unwrap(),
        );
        let enter_sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&enter_sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 1);

        let reprice_sig = make_signal("btc-updown-5m-test", 0.82, 0.81, 0.18, 280.0, 1015.0);
        runner.on_signal(&reprice_sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 0, "take profit should have closed position");
    }

    #[tokio::test]
    async fn settlement_closes_position() {
        let dir = tempdir().unwrap();
        let mut runner = ConfigRunner::new(
            make_config("SETTLE_TEST", 5, 0.12, 840.0, 20.0, false, false),
            5.0, 0.015,
            dir.path().to_str().unwrap(),
        );
        let sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 1);

        runner.on_settlement("btc-updown-5m-test", 1.0, 1300.0).await;
        assert_eq!(runner.open_position_count(), 0);
    }
}
