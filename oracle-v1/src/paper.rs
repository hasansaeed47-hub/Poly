/// paper.rs — Paper scanner running alongside live execution
///
/// Runs multiple config variants in paper mode (no real orders).
/// Logs what-if filter data to JSONL for post-hoc analysis.
/// Merged from oracle-scanner to avoid running two separate processes.

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
pub struct PaperRunnerConfig {
    pub name:          String,
    pub min_edge:      f64,
    pub max_secs_left: f64,
    pub stop_loss:     bool,
    pub take_profit:   bool,
}

// ── Paper position ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PaperPosition {
    pub trade_id:      String,
    pub slug:          String,
    pub asset:         String,
    pub tf:            u32,
    pub side:          Side,
    pub entry_price:   f64,
    pub fair_at_entry: f64,
    pub stake:         f64,
    pub secs_left:     f64,
    pub entry_ts:      f64,
    pub window_end:    u64,
    // What-if filter data captured at entry
    pub cl_momentum:          f64,
    pub book_imbal:           f64,
    pub sigma:                f64,
    pub pct_move:             f64,
    pub blocked_by_momentum:  bool,
    pub blocked_by_bookimbal: bool,
    pub blocked_by_both:      bool,
}

// ── Trade log entry ───────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct PaperTradeLog {
    pub config:        String,
    pub trade_id:      String,
    pub slug:          String,
    pub asset:         String,
    pub tf:            u32,
    pub side:          String,
    pub entry_price:   f64,
    pub fair_at_entry: f64,
    pub edge_at_entry: f64,
    pub sigma:         f64,
    pub pct_move:      f64,
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
    pub cl_momentum:          f64,
    pub book_imbal:           f64,
    pub blocked_by_momentum:  bool,
    pub blocked_by_bookimbal: bool,
    pub blocked_by_both:      bool,
}

// ── Stats ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
pub struct PaperStats {
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

impl PaperStats {
    pub fn wr(&self) -> f64 {
        let settled = self.wins + self.losses;
        if settled == 0 { 0.0 } else { self.wins as f64 / settled as f64 * 100.0 }
    }
}

// ── Runner ────────────────────────────────────────────────────────────────────

pub struct PaperRunner {
    pub config:    PaperRunnerConfig,
    pub stake:     f64,
    pub fee_rate:  f64,
    positions:     HashMap<String, PaperPosition>,
    stats:         PaperStats,
    log_file:      Arc<Mutex<File>>,
}

impl PaperRunner {
    pub fn new(config: PaperRunnerConfig, stake: f64, fee_rate: f64, log_dir: &str) -> Self {
        let log_path = format!("{}/paper_{}.jsonl", log_dir, config.name.to_lowercase());
        let file = OpenOptions::new()
            .create(true).append(true)
            .open(&log_path)
            .unwrap_or_else(|e| panic!("Cannot open log {}: {}", log_path, e));

        info!("[PAPER:{}] Log: {}", config.name, log_path);

        Self {
            config,
            stake,
            fee_rate,
            positions: HashMap::new(),
            stats: PaperStats::default(),
            log_file: Arc::new(Mutex::new(file)),
        }
    }

    pub async fn on_signal(&mut self, sig: &Signal, window_end: u64) {
        self.check_exits(sig).await;
        self.maybe_enter(sig, window_end).await;
    }

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
                self.close_position(pos, exit_price, "SETTLEMENT", settle_ts).await;
            }
        }
    }

    async fn maybe_enter(&mut self, sig: &Signal, window_end: u64) {
        self.stats.signals += 1;

        if sig.secs_left < 60.0 || sig.secs_left > self.config.max_secs_left {
            return;
        }

        let side = match sig.best_side {
            Some(s) if sig.best_edge >= self.config.min_edge => s,
            _ => return,
        };

        let fill_price = match side {
            Side::Yes => sig.fill_yes,
            Side::No  => sig.fill_no,
        };
        let fill_price = match fill_price {
            Some(p) if p > 0.0 => p,
            _ => return,
        };

        let depth = match side {
            Side::Yes => sig.depth_yes,
            Side::No  => sig.depth_no,
        };
        if depth < self.stake * 2.0 {
            return;
        }

        if self.positions.keys().any(|k| k.starts_with(&sig.slug)) {
            return;
        }

        let now = sig.ts;
        let trade_id = format!("{}-{}-{:.0}", sig.slug, self.config.name, now * 1000.0);

        // What-if filters: would momentum/book_imbal have blocked this entry?
        let momentum_confirms = match side {
            Side::Yes => sig.cl_momentum >= 0.0,
            Side::No  => sig.cl_momentum <= 0.0,
        };
        let book_confirms = match side {
            Side::Yes => sig.book_imbal >= 0.7,
            Side::No  => sig.book_imbal <= 1.5,
        };
        let blocked_by_momentum  = !momentum_confirms;
        let blocked_by_bookimbal = !book_confirms;
        let blocked_by_both      = blocked_by_momentum || blocked_by_bookimbal;

        let pct_move = ((sig.cl_price / sig.open_price) - 1.0).abs() * 100.0;

        let pos = PaperPosition {
            trade_id: trade_id.clone(),
            slug: sig.slug.clone(),
            asset: sig.asset.clone(),
            tf: sig.tf,
            side,
            entry_price: fill_price,
            fair_at_entry: sig.best_fair,
            stake: self.stake,
            secs_left: sig.secs_left,
            entry_ts: now,
            window_end,
            cl_momentum: sig.cl_momentum,
            book_imbal: sig.book_imbal,
            sigma: sig.sigma,
            pct_move,
            blocked_by_momentum,
            blocked_by_bookimbal,
            blocked_by_both,
        };

        info!(
            "[PAPER:{}] ENTER {} {} @{:.3} fair={:.3} edge={:.3}",
            self.config.name, sig.slug, side,
            fill_price, sig.best_fair, sig.best_edge
        );

        self.positions.insert(trade_id, pos);
        self.stats.entries += 1;
    }

    async fn check_exits(&mut self, sig: &Signal) {
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

            // Stop-loss: exit when current_fair < entry_price
            if self.config.stop_loss
                && sig.secs_left > 90.0
                && current_fair < pos.entry_price
                && exit_bid > 0.0
            {
                let pos = self.positions.remove(&trade_id).unwrap();
                info!(
                    "[PAPER:{}] STOP_LOSS {} fair={:.3} < entry={:.3}",
                    self.config.name, pos.slug, current_fair, pos.entry_price
                );
                self.close_position(pos, exit_bid, "STOP_LOSS", sig.ts).await;
                continue;
            }

            // Take-profit: exit when bid >= fair_at_entry
            if self.config.take_profit
                && exit_bid >= pos.fair_at_entry
                && exit_bid > 0.0
            {
                let pos = self.positions.remove(&trade_id).unwrap();
                info!(
                    "[PAPER:{}] TAKE_PROFIT {} bid={:.3} >= fair={:.3}",
                    self.config.name, pos.slug, exit_bid, pos.fair_at_entry
                );
                self.close_position(pos, exit_bid, "TAKE_PROFIT", sig.ts).await;
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

        let entry_fee = self.fee_rate * pos.stake;
        let exit_fee  = if exit_reason != "SETTLEMENT" {
            self.fee_rate * (exit_price * shares)
        } else {
            0.0
        };
        let total_fee = entry_fee + exit_fee;
        let net = gross - total_fee;

        if net > 0.0 { self.stats.wins += 1; }
        else { self.stats.losses += 1; }

        self.stats.gross_pnl += gross;
        self.stats.total_fee += total_fee;
        self.stats.net_pnl   += net;

        match exit_reason {
            "SETTLEMENT"   => self.stats.settlement_exits  += 1,
            "STOP_LOSS"    => self.stats.stop_loss_exits   += 1,
            "TAKE_PROFIT"  => self.stats.take_profit_exits += 1,
            _              => {}
        }

        let log = PaperTradeLog {
            config:        self.config.name.clone(),
            trade_id:      pos.trade_id.clone(),
            slug:          pos.slug.clone(),
            asset:         pos.asset.clone(),
            tf:            pos.tf,
            side:          pos.side.to_string(),
            entry_price:   pos.entry_price,
            fair_at_entry: pos.fair_at_entry,
            edge_at_entry: pos.fair_at_entry - pos.entry_price,
            sigma:         pos.sigma,
            pct_move:      pos.pct_move,
            secs_left:     pos.secs_left,
            stake:         pos.stake,
            exit_price,
            exit_reason:   exit_reason.to_string(),
            pnl:           gross,
            fee:           total_fee,
            net_pnl:       net,
            entry_ts:      pos.entry_ts,
            exit_ts,
            hold_secs:     exit_ts - pos.entry_ts,
            cl_momentum:          pos.cl_momentum,
            book_imbal:           pos.book_imbal,
            blocked_by_momentum:  pos.blocked_by_momentum,
            blocked_by_bookimbal: pos.blocked_by_bookimbal,
            blocked_by_both:      pos.blocked_by_both,
        };

        if let Ok(line) = serde_json::to_string(&log) {
            let mut file = self.log_file.lock().await;
            let _ = writeln!(file, "{}", line);
        }

        info!(
            "[PAPER:{}] CLOSE {} {} exit={:.3} reason={} net={:+.3}",
            self.config.name, pos.slug, pos.side,
            exit_price, exit_reason, net
        );
    }

    pub fn print_stats(&self) {
        let total_staked = self.stats.entries as f64 * self.stake;
        let roi = if total_staked > 0.0 { self.stats.net_pnl / total_staked * 100.0 } else { 0.0 };
        info!(
            "[PAPER:{}] sig={} entries={} W={} L={} WR={:.1}% net={:+.2} fee={:.2} ROI={:+.1}% open={} settle={} sl={} tp={}",
            self.config.name,
            self.stats.signals, self.stats.entries,
            self.stats.wins, self.stats.losses, self.stats.wr(),
            self.stats.net_pnl, self.stats.total_fee, roi,
            self.positions.len(),
            self.stats.settlement_exits, self.stats.stop_loss_exits, self.stats.take_profit_exits,
        );
    }
}
