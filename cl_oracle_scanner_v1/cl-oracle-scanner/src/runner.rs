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

// ── Polymarket fee model ────────────────────────────────────────────────────

/// Taker fee per share at price p: p * (1 - p) * 3.14%
/// Total fee = fee_per_share * shares
fn taker_fee_per_share(p: f64) -> f64 {
    p * (1.0 - p) * 0.0314
}

// ── Config definition ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct RunnerConfig {
    pub name:          String,
    pub min_edge:      f64,
    pub max_secs_left: f64,
    pub stop_loss:     bool,  // exit when current_fair < entry_price
    pub take_profit:   bool,  // exit when book_price >= fair_at_entry (Config 5)
    pub max_positions: usize, // max concurrent positions
    pub max_exposure:  f64,   // max total USD exposure
}

// ── Paper position ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PaperPosition {
    pub trade_id:      String,
    pub slug:          String,
    pub asset:         String,
    pub tf:            u32,
    pub side:          Side,
    pub entry_price:   f64,
    pub fair_at_entry: f64,  // Black-Scholes fair at entry — used as TP target for C5
    pub stake:         f64,
    pub shares:        f64,  // stake / entry_price
    pub entry_ts:      f64,
    pub secs_left_at_entry: f64,
    pub window_end:    u64,  // unix timestamp when window closes
    // Full state snapshot at entry
    pub sigma_at_entry:    f64,
    pub cl_price_at_entry: f64,
    pub open_price:        f64,
    pub book_yes_at_entry: f64,
    pub book_no_at_entry:  f64,
    pub bid_yes_at_entry:  f64,
    pub bid_no_at_entry:   f64,
    pub edge_at_entry:     f64,
    pub spread_at_entry:   f64,
    pub depth_at_entry:    f64,
    pub book_age_at_entry: f64,
    pub cl_age_at_entry:   f64,
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
    pub shares:        f64,
    pub exit_price:    f64,
    pub exit_reason:   String,  // "SETTLEMENT" | "STOP_LOSS" | "TAKE_PROFIT"
    pub pnl:           f64,
    pub entry_fee:     f64,
    pub exit_fee:      f64,
    pub fee:           f64,
    pub net_pnl:       f64,
    pub roi_pct:       f64,     // net_pnl / stake * 100
    pub entry_ts:      f64,
    pub exit_ts:       f64,
    pub hold_secs:     f64,
    // Full state at entry
    pub sigma_at_entry:    f64,
    pub cl_price_at_entry: f64,
    pub open_price:        f64,
    pub book_yes_at_entry: f64,
    pub book_no_at_entry:  f64,
    pub bid_yes_at_entry:  f64,
    pub bid_no_at_entry:   f64,
    pub spread_at_entry:   f64,
    pub depth_at_entry:    f64,
    pub book_age_at_entry: f64,
    pub cl_age_at_entry:   f64,
    // State at exit
    pub exit_fair:         f64,
    pub exit_cl_price:     f64,
    pub exit_sigma:        f64,
    pub exit_book_yes:     f64,
    pub exit_book_no:      f64,
    pub exit_bid_yes:      f64,
    pub exit_bid_no:       f64,
    pub exit_secs_left:    f64,
}

// ── Mark-to-market log entry ──────────────────────────────────────────────────

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
    // Rejection breakdown
    pub reject_time:      u64,
    pub reject_edge:      u64,
    pub reject_duplicate: u64,
    pub reject_max_pos:   u64,
    pub reject_max_exp:   u64,
    // Drawdown tracking
    pub peak_pnl:     f64,
    pub max_drawdown:  f64,
}

impl RunnerStats {
    pub fn wr(&self) -> f64 {
        let settled = self.wins + self.losses;
        if settled == 0 { 0.0 } else { self.wins as f64 / settled as f64 * 100.0 }
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

// ── Runner ────────────────────────────────────────────────────────────────────

pub struct ConfigRunner {
    pub config:    RunnerConfig,
    pub stake:     f64,
    pub tf_filter: u32,  // only trade this timeframe (5 or 15)
    min_secs:      f64,
    positions:     HashMap<String, PaperPosition>, // trade_id → position
    stats:         RunnerStats,
    log_file:      Arc<Mutex<File>>,
}

impl ConfigRunner {
    pub fn new(config: RunnerConfig, stake: f64, tf_filter: u32, min_secs: f64, log_dir: &str) -> Self {
        let log_path = format!("{}/{}.jsonl", log_dir, config.name.to_lowercase());
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .unwrap_or_else(|e| panic!("Cannot open log {}: {}", log_path, e));

        info!("[{}] Log: {} tf={}m max_pos={} max_exp=${}", config.name, log_path, tf_filter, config.max_positions, config.max_exposure);

        Self {
            config,
            stake,
            tf_filter,
            min_secs,
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

    /// Called at window settlement.
    /// winning_side: Side::Yes if CL closed above open, Side::No if below.
    /// Settles all positions for this slug — winning side gets 1.0, losing gets 0.0.
    pub async fn on_settlement(&mut self, slug: &str, winning_side: Side, settle_ts: f64) {
        let trade_ids: Vec<String> = self
            .positions
            .iter()
            .filter(|(_, pos)| pos.slug == slug)
            .map(|(k, _)| k.clone())
            .collect();

        for trade_id in trade_ids {
            if let Some(pos) = self.positions.remove(&trade_id) {
                // Binary settlement: winning side token → $1.00, losing → $0.00
                let exit_price = if pos.side == winning_side { 1.0 } else { 0.0 };
                self.close_position(pos, exit_price, "SETTLEMENT", settle_ts, None).await;
            }
        }
    }

    // ── Private ───────────────────────────────────────────────────────────────

    async fn maybe_enter(&mut self, sig: &Signal, window_end: u64) {
        // Timeframe filter — only trade matching window length
        if sig.tf != self.tf_filter {
            return;
        }

        self.stats.signals += 1;

        // Time gate — use config min_secs, not hardcoded
        if sig.secs_left < self.min_secs || sig.secs_left > self.config.max_secs_left as f64 {
            self.stats.reject_time += 1;
            return;
        }

        // Edge gate
        let side = match sig.best_side {
            Some(s) if sig.best_edge >= self.config.min_edge => s,
            _ => {
                self.stats.reject_edge += 1;
                return;
            }
        };

        // Already in this slug? (one position per slug per config)
        let already_in = self.positions.values().any(|p| p.slug == sig.slug);
        if already_in {
            self.stats.reject_duplicate += 1;
            return;
        }

        // Max positions gate
        if self.positions.len() >= self.config.max_positions {
            self.stats.reject_max_pos += 1;
            return;
        }

        // Max exposure gate
        let current_exposure: f64 = self.positions.values().map(|p| p.stake).sum();
        if current_exposure + self.stake > self.config.max_exposure {
            self.stats.reject_max_exp += 1;
            return;
        }

        let now = sig.ts;
        let trade_id = format!("{}-{}-{:.0}", sig.slug, self.config.name, now * 1000.0);

        let entry_price = sig.best_book;
        let shares = self.stake / entry_price;

        // Get spread/depth/age for the entry side
        let (spread_at_entry, depth_at_entry, book_age_at_entry) = match side {
            Side::Yes => (sig.spread_yes, sig.depth_ask_yes, sig.book_age_yes),
            Side::No  => (sig.spread_no,  sig.depth_ask_no,  sig.book_age_no),
        };

        let pos = PaperPosition {
            trade_id:      trade_id.clone(),
            slug:          sig.slug.clone(),
            asset:         sig.asset.clone(),
            tf:            sig.tf,
            side,
            entry_price,
            fair_at_entry: sig.best_fair,
            stake:         self.stake,
            shares,
            entry_ts:      now,
            secs_left_at_entry: sig.secs_left,
            window_end,
            sigma_at_entry:    sig.sigma,
            cl_price_at_entry: sig.cl_price,
            open_price:        sig.open_price,
            book_yes_at_entry: sig.book_yes,
            book_no_at_entry:  sig.book_no,
            bid_yes_at_entry:  sig.bid_yes,
            bid_no_at_entry:   sig.bid_no,
            edge_at_entry:     sig.best_edge,
            spread_at_entry,
            depth_at_entry,
            book_age_at_entry,
            cl_age_at_entry:   sig.cl_age,
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
        // Only check positions for this slug
        let trade_ids: Vec<String> = self
            .positions
            .iter()
            .filter(|(_, pos)| pos.slug == sig.slug)
            .map(|(k, _)| k.clone())
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

            // Get current book BID for this side (selling = hitting the bid)
            let current_bid = match pos.side {
                Side::Yes => sig.bid_yes,
                Side::No  => sig.bid_no,
            };

            // Config 4: Stop-loss — exit when current_fair < entry_price
            if self.config.stop_loss
                && sig.secs_left > 90.0  // not in final 90s
                && current_fair < pos.entry_price
            {
                let pos = self.positions.remove(&trade_id).unwrap();
                let exit_price = current_bid.max(0.001);
                info!(
                    "[{}] STOP_LOSS {} fair={:.3} < entry={:.3}",
                    self.config.name, pos.slug, current_fair, pos.entry_price
                );
                self.close_position(pos, exit_price, "STOP_LOSS", sig.ts, Some(sig)).await;
                continue;
            }

            // Config 5: Take-profit — exit when bid >= fair_at_entry
            if self.config.take_profit
                && current_bid >= pos.fair_at_entry
            {
                let pos = self.positions.remove(&trade_id).unwrap();
                let exit_price = current_bid;
                info!(
                    "[{}] TAKE_PROFIT {} bid={:.3} >= fair_at_entry={:.3}",
                    self.config.name, pos.slug, current_bid, pos.fair_at_entry
                );
                self.close_position(pos, exit_price, "TAKE_PROFIT", sig.ts, Some(sig)).await;
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
        exit_sig:    Option<&Signal>,
    ) {
        let shares = pos.shares;

        // Gross PnL: (exit_price - entry_price) * shares
        let gross = (exit_price - pos.entry_price) * shares;

        // Fee: Polymarket curve — p*(1-p)*3.14% per share
        // Entry: always taker fee
        let entry_fee = taker_fee_per_share(pos.entry_price) * shares;
        // Exit: taker fee only on early exits (SL/TP), settlement is free
        let exit_fee = if exit_reason != "SETTLEMENT" {
            taker_fee_per_share(exit_price) * shares
        } else {
            0.0
        };
        let total_fee = entry_fee + exit_fee;

        let net = gross - total_fee;
        let roi_pct = if pos.stake > 0.0 { net / pos.stake * 100.0 } else { 0.0 };

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

        // Extract exit state from signal if available
        let (exit_fair, exit_cl, exit_sigma, exit_by, exit_bn, exit_bidy, exit_bidn, exit_secs) =
            match exit_sig {
                Some(s) => {
                    let fair = match pos.side { Side::Yes => s.fair_yes, Side::No => s.fair_no };
                    (fair, s.cl_price, s.sigma, s.book_yes, s.book_no, s.bid_yes, s.bid_no, s.secs_left)
                }
                None => (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            };

        let log = TradeLog {
            config:        self.config.name.clone(),
            trade_id:      pos.trade_id.clone(),
            slug:          pos.slug.clone(),
            asset:         pos.asset.clone(),
            tf:            pos.tf,
            side:          pos.side.to_string(),
            entry_price:   pos.entry_price,
            fair_at_entry: pos.fair_at_entry,
            edge_at_entry: pos.edge_at_entry,
            secs_left:     pos.secs_left_at_entry,
            stake:         pos.stake,
            shares,
            exit_price,
            exit_reason:   exit_reason.to_string(),
            pnl:           gross,
            entry_fee,
            exit_fee,
            fee:           total_fee,
            net_pnl:       net,
            roi_pct,
            entry_ts:      pos.entry_ts,
            exit_ts,
            hold_secs:     exit_ts - pos.entry_ts,
            sigma_at_entry:    pos.sigma_at_entry,
            cl_price_at_entry: pos.cl_price_at_entry,
            open_price:        pos.open_price,
            book_yes_at_entry: pos.book_yes_at_entry,
            book_no_at_entry:  pos.book_no_at_entry,
            bid_yes_at_entry:  pos.bid_yes_at_entry,
            bid_no_at_entry:   pos.bid_no_at_entry,
            spread_at_entry:   pos.spread_at_entry,
            depth_at_entry:    pos.depth_at_entry,
            book_age_at_entry: pos.book_age_at_entry,
            cl_age_at_entry:   pos.cl_age_at_entry,
            exit_fair,
            exit_cl_price:     exit_cl,
            exit_sigma,
            exit_book_yes:     exit_by,
            exit_book_no:      exit_bn,
            exit_bid_yes:      exit_bidy,
            exit_bid_no:       exit_bidn,
            exit_secs_left:    exit_secs,
        };

        // Write to JSONL log
        if let Ok(line) = serde_json::to_string(&log) {
            let mut file = self.log_file.lock().await;
            let _ = writeln!(file, "{}", line);
        }

        info!(
            "[{}] CLOSE {} {} exit={:.3} reason={} net={:+.3} roi={:+.1}%",
            self.config.name, pos.slug, pos.side,
            exit_price, exit_reason, net, roi_pct
        );
    }

    pub fn print_stats(&self) {
        info!(
            "[{}] sig={} entries={} W={} L={} WR={:.1}% net={:+.2} fee={:.2} settle={} sl={} tp={} peak={:+.2} dd={:.2}",
            self.config.name,
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
            self.stats.peak_pnl,
            self.stats.max_drawdown,
        );
        info!(
            "[{}] rejects: time={} edge={} dup={} max_pos={} max_exp={} open={}",
            self.config.name,
            self.stats.reject_time,
            self.stats.reject_edge,
            self.stats.reject_duplicate,
            self.stats.reject_max_pos,
            self.stats.reject_max_exp,
            self.positions.len(),
        );
    }

    pub fn open_position_count(&self) -> usize {
        self.positions.len()
    }

    /// Get mark-to-market entries for all open positions matching this signal's slug
    pub fn get_mtm_entries(&self, sig: &Signal) -> Vec<MtmLog> {
        self.positions.values()
            .filter(|pos| pos.slug == sig.slug)
            .map(|pos| {
                let current_fair = match pos.side {
                    Side::Yes => sig.fair_yes,
                    Side::No  => sig.fair_no,
                };
                let current_bid = match pos.side {
                    Side::Yes => sig.bid_yes,
                    Side::No  => sig.bid_no,
                };
                let unrealised_pnl = (current_bid - pos.entry_price) * pos.shares;
                MtmLog {
                    ts: sig.ts,
                    config: self.config.name.clone(),
                    trade_id: pos.trade_id.clone(),
                    slug: pos.slug.clone(),
                    side: pos.side.to_string(),
                    entry_price: pos.entry_price,
                    shares: pos.shares,
                    current_fair,
                    current_bid,
                    unrealised_pnl,
                    secs_left: sig.secs_left,
                }
            })
            .collect()
    }

    /// Print per-window result after settlement
    pub fn print_window_result(&self, slug: &str, winning_side: Side) {
        // Only print if this runner's tf matches the slug's window
        info!(
            "[{}] {} winner={} | sig={} entries={} W={} L={} WR={:.1}% net={:+.2} fee={:.2} settle={} sl={} tp={} peak={:+.2} dd={:.2}",
            self.config.name, slug, winning_side,
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
            self.stats.peak_pnl,
            self.stats.max_drawdown,
        );
    }

    /// Flush log file (for graceful shutdown)
    pub async fn flush(&self) {
        let mut file = self.log_file.lock().await;
        let _ = file.flush();
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::{Signal, Side, BookSnap};

    fn make_config(name: &str, min_edge: f64, max_secs: f64, sl: bool, tp: bool) -> RunnerConfig {
        RunnerConfig {
            name:          name.to_string(),
            min_edge,
            max_secs_left: max_secs,
            stop_loss:     sl,
            take_profit:   tp,
            max_positions: 10,
            max_exposure:  100.0,
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
        } else if ey > 0.0 {
            // Equal edges — default to YES
            (Some(Side::Yes), ey, book_yes, fair_yes)
        } else {
            (None, 0.0, 0.0, 0.0)
        };
        Signal {
            slug: slug.to_string(), asset: "btc".to_string(), tf: 5,
            open_price: 100.0, cl_price: 100.5, sigma: 0.001,
            secs_left: secs, fair_yes, fair_no,
            book_yes, book_no,
            bid_yes: book_yes - 0.01, bid_no: book_no - 0.01,
            edge_yes: ey, edge_no: en,
            best_side, best_edge, best_book, best_fair, ts,
            spread_yes: 0.01, spread_no: 0.01,
            depth_ask_yes: 10.0, depth_bid_yes: 10.0,
            depth_ask_no: 10.0, depth_bid_no: 10.0,
            book_age_yes: 0.5, book_age_no: 0.5,
            cl_age: 0.5,
        }
    }

    fn make_runner(name: &str, min_edge: f64, max_secs: f64, sl: bool, tp: bool) -> ConfigRunner {
        let dir = std::env::temp_dir();
        ConfigRunner::new(
            make_config(name, min_edge, max_secs, sl, tp),
            5.0, 5, 60.0,
            dir.to_str().unwrap(),
        )
    }

    #[tokio::test]
    async fn c1_enters_on_edge() {
        let mut runner = make_runner("C1_TEST", 0.12, 840.0, false, false);
        let sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 1);
    }

    #[tokio::test]
    async fn c2_rejects_outside_time_window() {
        let mut runner = make_runner("C2_TEST", 0.12, 180.0, false, false);
        let sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 0);
        assert_eq!(runner.stats.reject_time, 1);
    }

    #[tokio::test]
    async fn c3_rejects_small_edge() {
        let mut runner = make_runner("C3_TEST", 0.25, 840.0, false, false);
        let sig = make_signal("btc-updown-5m-test", 0.65, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 0);
        assert_eq!(runner.stats.reject_edge, 1);
    }

    #[tokio::test]
    async fn max_positions_enforced() {
        let cfg = RunnerConfig {
            name: "MAX_POS_TEST".to_string(),
            min_edge: 0.12,
            max_secs_left: 840.0,
            stop_loss: false,
            take_profit: false,
            max_positions: 1,
            max_exposure: 1000.0,
        };
        let dir = std::env::temp_dir();
        let mut runner = ConfigRunner::new(cfg, 5.0, 5, 60.0, dir.to_str().unwrap());

        let sig1 = make_signal("btc-updown-5m-test1", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig1, 1300).await;
        assert_eq!(runner.open_position_count(), 1);

        let sig2 = make_signal("eth-updown-5m-test2", 0.80, 0.50, 0.49, 300.0, 1001.0);
        runner.on_signal(&sig2, 1301).await;
        assert_eq!(runner.open_position_count(), 1); // blocked
        assert_eq!(runner.stats.reject_max_pos, 1);
    }

    #[tokio::test]
    async fn settlement_yes_wins_yes_position_profits() {
        let mut runner = make_runner("SETTLE_YES", 0.12, 840.0, false, false);
        let sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 1);

        // YES wins — our YES position should get exit_price=1.0
        runner.on_settlement("btc-updown-5m-test", Side::Yes, 1300.0).await;
        assert_eq!(runner.open_position_count(), 0);
        assert!(runner.stats.net_pnl > 0.0, "YES pos should profit when YES wins");
    }

    #[tokio::test]
    async fn settlement_no_wins_yes_position_loses() {
        let mut runner = make_runner("SETTLE_NO", 0.12, 840.0, false, false);
        let sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;
        assert_eq!(runner.open_position_count(), 1);

        // NO wins — our YES position should get exit_price=0.0
        runner.on_settlement("btc-updown-5m-test", Side::No, 1300.0).await;
        assert_eq!(runner.open_position_count(), 0);
        assert!(runner.stats.net_pnl < 0.0, "YES pos should lose when NO wins");
    }

    #[tokio::test]
    async fn fee_model_correct() {
        // Entry at p=0.50, stake=$5 → shares=10
        // Fee per share = 0.50 * 0.50 * 0.0314 = 0.00785
        // Total entry fee = 0.00785 * 10 = 0.0785
        let mut runner = make_runner("FEE_TEST", 0.12, 840.0, false, false);
        let sig = make_signal("btc-updown-5m-test", 0.80, 0.50, 0.49, 300.0, 1000.0);
        runner.on_signal(&sig, 1000 + 300).await;
        runner.on_settlement("btc-updown-5m-test", Side::Yes, 1300.0).await;
        // Fee should be ~$0.0785 (entry only, settlement has no exit fee)
        assert!((runner.stats.total_fee - 0.0785).abs() < 0.001,
            "expected fee ~0.0785, got {}", runner.stats.total_fee);
    }
}
