/// runner.rs — Engine Tracker
///
/// One Tracker per engine (5 total: A-E). Each independently evaluates
/// entry signals, manages active positions, handles SL and settlement.
///
/// Ports the proven cl-sniper-10mar Tracker logic into the scanner's
/// modular architecture with config-driven parameters.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::fs::{File, OpenOptions};

use serde::Serialize;
use tracing::{info, warn};

use crate::engine::{EngineConfig, ExecConfig, min_delta};
use crate::execution::{
    Position, TradeResult, check_sl, execute_sl, execute_settlement,
    fill_price, open_position, resolve_settlement,
};
use crate::feeds::{
    BookState, BnHistory, BnPrices, ClPrices, ClSnapshots,
    bn_trend, cl_trend,
};

// -- Market window (matches feeds::MarketMeta enriched with token IDs) --------

/// Lightweight view of a market window for entry evaluation
pub struct MarketWindow<'a> {
    pub slug:       &'a str,
    pub asset:      &'a str,
    pub wmin:       u32,
    pub start_ts:   i64,
    pub end_ts:     i64,
    pub tid_up:     &'a str,
    pub tid_dn:     &'a str,
    pub secs_left:  i64,
}

// -- Trade log entry ----------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct TradeLog {
    pub engine:      String,
    pub slug:        String,
    pub asset:       String,
    pub tf:          u32,
    pub dir:         String,
    pub fill_px:     f64,
    pub shares:      f64,
    pub exit_px:     f64,
    pub exit_reason: String,
    pub pnl:         f64,
    pub entry_fee:   f64,
    pub exit_fee:    f64,
    pub entry_ts:    f64,
    pub exit_ts:     f64,
    pub hold_secs:   f64,
}

// -- Stats --------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
pub struct TrackerStats {
    pub wins:     u32,
    pub losses:   u32,
    pub sl_count: u32,
    pub pnl:      f64,
}

impl TrackerStats {
    pub fn total(&self) -> u32 { self.wins + self.losses + self.sl_count }
    pub fn wr(&self) -> f64 {
        let t = self.total();
        if t == 0 { 0.0 } else { self.wins as f64 / t as f64 * 100.0 }
    }
}

// -- Tracker (one per engine) -------------------------------------------------

pub struct Tracker {
    pub cfg:          EngineConfig,
    pub exec:         ExecConfig,
    pub stats:        TrackerStats,
    pub active:       Option<Position>,
    done:             HashSet<String>,          // slugs already traded this cycle
    delta_ticks:      HashMap<String, u32>,     // continuity counter
    maker_ticks:      HashMap<String, u32>,     // maker chase counter
    sl_skip_logged:   bool,
    log_file:         File,
}

impl Tracker {
    pub fn new(cfg: EngineConfig, exec: ExecConfig, log_dir: &str) -> Self {
        let log_path = format!("{}/engine_{}.jsonl", log_dir, cfg.id.to_lowercase());
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .unwrap_or_else(|e| panic!("Cannot open log {}: {}", log_path, e));

        info!("[{}] Initialized — tf={}m delta={:.2}% cont={} late={}",
            cfg.id, cfg.tf, cfg.delta, cfg.continuity, cfg.is_late_scalper);

        Tracker {
            cfg,
            exec,
            stats: TrackerStats::default(),
            active: None,
            done: HashSet::new(),
            delta_ticks: HashMap::new(),
            maker_ticks: HashMap::new(),
            sl_skip_logged: false,
            log_file: file,
        }
    }

    // -- SL check (called every tick while position is active) ----------------

    pub fn check_stop_loss(&mut self, book_state: &BookState, now: f64) -> Option<TradeResult> {
        let pos = self.active.as_ref()?;

        let our_bk = book_state.get(&pos.tid)?;
        let opp_tid = if pos.dir == "UP" { &pos.tid_dn } else { &pos.tid_up };
        let opp_bk = book_state.get(opp_tid)?;

        let (fire, reason) = check_sl(
            our_bk.best_bid,
            our_bk.best_bid > 0.0,
            pos.sl_px,
            opp_bk.best_bid,
            opp_bk.best_bid > 0.0,
            self.exec.sl_confirm_bid,
        );

        if fire {
            let pos = self.active.take().unwrap();
            let result = execute_sl(&pos, our_bk.best_bid, &self.exec, now);

            info!("═══════════════════════════════════════════════════════");
            info!("  [{}] SL {} {} {}m", self.cfg.id, pos.dir, pos.asset.to_uppercase(), pos.wmin);
            info!("  bid={:.3} <= {:.3} (50% of {:.3})  opp_bid={:.3} <- CONFIRMED",
                our_bk.best_bid, pos.sl_px, pos.fill_px, opp_bk.best_bid);
            info!("  P&L=${:+.2}", result.pnl);
            info!("═══════════════════════════════════════════════════════");

            self.stats.sl_count += 1;
            self.stats.pnl += result.pnl;
            self.done.insert(pos.slug.clone());
            self.log_trade(&result);
            return Some(result);
        } else if reason == "THIN_BOOK" && !self.sl_skip_logged {
            info!("  [{}] SL skip: bid={:.3}<={:.3} but opp_bid={:.3} (thin book, holding)",
                self.cfg.id, our_bk.best_bid, pos.sl_px, opp_bk.best_bid);
            self.sl_skip_logged = true;
        }

        None
    }

    // -- Settlement (called when window_end + settle_delay has passed) --------

    pub fn check_settlement(
        &mut self,
        book_state:   &BookState,
        cl_snapshots: &ClSnapshots,
        cl_opens:     &HashMap<String, f64>,
        now:          f64,
    ) -> Option<TradeResult> {
        let pos = self.active.as_ref()?;
        let now_ts = now as i64;

        if now_ts < pos.end_ts + self.exec.settle_delay_secs as i64 {
            return None;
        }

        let pos = self.active.take().unwrap();
        let actual = resolve_settlement(book_state, cl_snapshots, cl_opens, &pos);

        if let Some(actual_dir) = actual {
            let result = execute_settlement(&pos, &actual_dir, &self.exec, now);

            match result.exit_reason.as_str() {
                "WIN" => {
                    info!("[{}] WIN {} {} @{:.3} -> ${:+.2} (cum ${:+.2})",
                        self.cfg.id, pos.asset.to_uppercase(), pos.dir, pos.fill_px,
                        result.pnl, self.stats.pnl + result.pnl);
                    self.stats.wins += 1;
                }
                "LOSS" => {
                    info!("[{}] LOSS {} {} @{:.3} -> ${:+.2} (cum ${:+.2})",
                        self.cfg.id, pos.asset.to_uppercase(), pos.dir, pos.fill_px,
                        result.pnl, self.stats.pnl + result.pnl);
                    self.stats.losses += 1;
                }
                _ => {}
            }

            self.stats.pnl += result.pnl;
            self.log_trade(&result);
            Some(result)
        } else {
            warn!("[{}] NO_SETTLE {} — returning stake", self.cfg.id, pos.slug);
            self.active = None;
            None
        }
    }

    // -- Entry evaluation (called every tick for each market window) -----------

    pub fn evaluate_entry(
        &mut self,
        win:          &MarketWindow<'_>,
        cl_prices:    &ClPrices,
        cl_snapshots: &ClSnapshots,
        cl_opens:     &HashMap<String, f64>,
        book_state:   &BookState,
        bn_prices:    &BnPrices,
        bn_hist:      &BnHistory,
        hour_ranges:  &HashMap<String, f64>,
        now:          f64,
    ) -> bool {
        // Already have a position
        if self.active.is_some() { return false; }

        // Already traded this slug
        if self.done.contains(win.slug) { return false; }

        // Timeframe filter (Engine E: tf=0 means both)
        if self.cfg.tf != 0 && win.wmin != self.cfg.tf { return false; }

        let left = win.secs_left;

        // Entry window check
        if left > self.cfg.entry_start || left < self.cfg.taker_deadline { return false; }

        if self.cfg.is_late_scalper {
            self.evaluate_late_scalper(win, cl_prices, cl_opens, book_state, now)
        } else {
            self.evaluate_delta(win, cl_prices, cl_snapshots, cl_opens, book_state,
                                bn_prices, bn_hist, hour_ranges, now)
        }
    }

    // -- Engine E: late scalper entry ------------------------------------------

    fn evaluate_late_scalper(
        &mut self,
        win:        &MarketWindow<'_>,
        cl_prices:  &ClPrices,
        cl_opens:   &HashMap<String, f64>,
        book_state: &BookState,
        now:        f64,
    ) -> bool {
        // Min delta filter for Engine E too
        if let (Some(&co), Some(cn_ref)) = (cl_opens.get(win.slug), cl_prices.get(win.asset)) {
            let cn = cn_ref.1;
            if co > 0.0 && cn > 0.0 {
                let d = ((cn - co) / co * 100.0).abs();
                if d < min_delta(win.asset) { return false; }
            }
        }

        let bk_up = book_state.get(win.tid_up);
        let bk_dn = book_state.get(win.tid_dn);

        // Find the side with ask >= min_entry
        let (dir, tid, best_ask) = match (&bk_up, &bk_dn) {
            (Some(up), _) if up.best_ask >= self.cfg.min_entry => ("UP", win.tid_up, up.best_ask),
            (_, Some(dn)) if dn.best_ask >= self.cfg.min_entry => ("DOWN", win.tid_dn, dn.best_ask),
            _ => return false,
        };

        if best_ask > self.cfg.max_entry || best_ask <= 0.0 { return false; }

        // Maker/taker fill
        let maker_elapsed = self.maker_ticks.entry(win.slug.to_string()).or_insert(0);
        *maker_elapsed += 1;

        let fp = match fill_price(
            best_ask, self.cfg.min_entry, self.cfg.max_entry, self.exec.slip,
            *maker_elapsed, self.exec.maker_chase_ticks,
            win.secs_left, self.cfg.taker_deadline,
        ) {
            Some(fp) => fp,
            None => return false,
        };

        let pos = open_position(
            &self.cfg.id, win.slug, win.asset, dir, fp, &self.exec,
            win.end_ts, tid, win.tid_up, win.tid_dn, win.wmin, now,
        );

        info!("═══════════════════════════════════════════════════════");
        info!("  [E] SIGNAL: BUY {} {} {}m @{:.3} ({:.0}s left)",
            dir, win.asset.to_uppercase(), win.wmin, fp, win.secs_left);
        info!("  book={:.3}  maker={:.3}  fee=${:.4}  SL<={:.3}",
            best_ask, crate::execution::maker_price(best_ask, self.cfg.min_entry),
            pos.entry_fee, pos.sl_px);
        info!("═══════════════════════════════════════════════════════");

        self.active = Some(pos);
        self.sl_skip_logged = false;
        self.done.insert(win.slug.to_string());
        self.maker_ticks.remove(win.slug);
        true
    }

    // -- Engines A-D: delta-based entry ---------------------------------------

    fn evaluate_delta(
        &mut self,
        win:          &MarketWindow<'_>,
        cl_prices:    &ClPrices,
        cl_snapshots: &ClSnapshots,
        cl_opens:     &HashMap<String, f64>,
        book_state:   &BookState,
        bn_prices:    &BnPrices,
        bn_hist:      &BnHistory,
        hour_ranges:  &HashMap<String, f64>,
        now:          f64,
    ) -> bool {
        let cl_open = match cl_opens.get(win.slug) {
            Some(&p) if p > 0.0 => p,
            _ => return false,
        };
        let cl_now = match cl_prices.get(win.asset) {
            Some(entry) if entry.1 > 0.0 => entry.1,
            _ => return false,
        };

        let delta = (cl_now - cl_open) / cl_open * 100.0;

        // Min delta floor
        if delta.abs() < min_delta(win.asset) { return false; }

        let dir = if delta > 0.0 { "UP" } else { "DOWN" };
        let threshold = self.cfg.scaled_delta(win.asset);

        // Delta threshold
        if delta.abs() < threshold {
            self.delta_ticks.remove(win.slug);
            self.maker_ticks.remove(win.slug);
            return false;
        }

        // Continuity check
        if self.cfg.continuity > 0 {
            let ticks = self.delta_ticks.entry(win.slug.to_string()).or_insert(0);
            *ticks += 1;
            if *ticks < self.cfg.continuity { return false; }
        }

        // BN contra filter
        if self.cfg.bn_contra {
            if let Some(bt) = bn_trend(bn_hist, win.asset, self.exec.bn_contra_secs) {
                let thresh = self.exec.bn_contra_thresh;
                if (dir == "UP" && bt < -thresh) || (dir == "DOWN" && bt > thresh) {
                    return false;
                }
            }
        }

        // CL fade filter
        if self.cfg.cl_fade {
            if let Some(ct) = cl_trend(cl_snapshots, cl_prices, win.asset, self.exec.cl_fade_secs) {
                let thresh = self.exec.cl_fade_thresh;
                if (dir == "UP" && ct < -thresh) || (dir == "DOWN" && ct > thresh) {
                    return false;
                }
            }
        }

        // Regime check
        if self.cfg.regime {
            let range = hour_ranges.get(win.asset).copied().unwrap_or(999.0);
            if range < self.exec.regime_thresh { return false; }
        }

        // Book check
        let tid = if dir == "UP" { win.tid_up } else { win.tid_dn };
        let bk = match book_state.get(tid) {
            Some(b) if b.best_ask >= self.cfg.min_entry && b.best_ask <= self.cfg.max_entry => b,
            _ => return false,
        };
        let best_ask = bk.best_ask;
        drop(bk);

        // Maker/taker fill
        let maker_elapsed = self.maker_ticks.entry(win.slug.to_string()).or_insert(0);
        *maker_elapsed += 1;

        let fp = match fill_price(
            best_ask, self.cfg.min_entry, self.cfg.max_entry, self.exec.slip,
            *maker_elapsed, self.exec.maker_chase_ticks,
            win.secs_left, self.cfg.taker_deadline,
        ) {
            Some(fp) => fp,
            None => return false,
        };

        let pos = open_position(
            &self.cfg.id, win.slug, win.asset, dir, fp, &self.exec,
            win.end_ts, tid, win.tid_up, win.tid_dn, win.wmin, now,
        );

        let bn_now = bn_prices.get(win.asset).map(|v| *v).unwrap_or(0.0);
        let hr = hour_ranges.get(win.asset).copied().unwrap_or(0.0);

        info!("═══════════════════════════════════════════════════════");
        info!("  [{}] SIGNAL: BUY {} {} {}m @{:.3} ({:.0}s left)",
            self.cfg.id, dir, win.asset.to_uppercase(), win.wmin, fp, win.secs_left);
        let cont = self.delta_ticks.get(win.slug).copied().unwrap_or(0);
        info!("  d={:+.4}% thr={:.4}% cont={}/{} book={:.3}",
            delta, threshold, cont, self.cfg.continuity, best_ask);
        info!("  CL={:.2} open={:.2} BN={:.2} 1hRange={:.2}%", cl_now, cl_open, bn_now, hr);
        info!("  maker={:.3} fee=${:.4} SL<={:.3}",
            crate::execution::maker_price(best_ask, self.cfg.min_entry), pos.entry_fee, pos.sl_px);
        info!("═══════════════════════════════════════════════════════");

        self.active = Some(pos);
        self.sl_skip_logged = false;
        self.done.insert(win.slug.to_string());
        self.delta_ticks.remove(win.slug);
        self.maker_ticks.remove(win.slug);
        true
    }

    // -- Logging --------------------------------------------------------------

    fn log_trade(&mut self, result: &TradeResult) {
        let log = TradeLog {
            engine:      result.engine_id.clone(),
            slug:        result.slug.clone(),
            asset:       result.asset.clone(),
            tf:          result.wmin,
            dir:         result.dir.clone(),
            fill_px:     result.fill_px,
            shares:      result.shares,
            exit_px:     result.exit_px,
            exit_reason: result.exit_reason.clone(),
            pnl:         result.pnl,
            entry_fee:   result.entry_fee,
            exit_fee:    result.exit_fee,
            entry_ts:    result.entry_ts,
            exit_ts:     result.exit_ts,
            hold_secs:   result.exit_ts - result.entry_ts,
        };
        if let Ok(line) = serde_json::to_string(&log) {
            let _ = writeln!(self.log_file, "{}", line);
            let _ = self.log_file.flush();
        }
    }

    // -- Status ---------------------------------------------------------------

    pub fn status(&self) -> String {
        let active = if self.active.is_some() { "*" } else { "" };
        if self.stats.total() > 0 {
            format!("{}{}:{}W/{}L/{}S${:+.1}",
                self.cfg.id, active, self.stats.wins, self.stats.losses,
                self.stats.sl_count, self.stats.pnl)
        } else if self.active.is_some() {
            format!("{}*:active", self.cfg.id)
        } else {
            format!("{}:-", self.cfg.id)
        }
    }

    pub fn print_stats(&self) {
        let active = if self.active.is_some() { " [ACTIVE]" } else { "" };
        info!("  [{}] {}W/{}L/{}S  WR={:.0}%  P&L=${:+.2}{}",
            self.cfg.id, self.stats.wins, self.stats.losses, self.stats.sl_count,
            self.stats.wr(), self.stats.pnl, active);
    }

    // -- Cleanup stale data ---------------------------------------------------

    pub fn cleanup(&mut self, cutoff_ts: i64) {
        self.done.retain(|k| slug_ts(k) > cutoff_ts);
        self.delta_ticks.retain(|k, _| slug_ts(k) > cutoff_ts);
        self.maker_ticks.retain(|k, _| slug_ts(k) > cutoff_ts);
    }
}

pub fn slug_ts(slug: &str) -> i64 {
    slug.rsplit('-').next()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0)
}

// -- RunnerConfig (kept for backward compat with old paper configs) -----------

#[allow(dead_code)]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct RunnerConfig {
    pub name:          String,
    pub tf:            u32,
    pub min_edge:      f64,
    pub max_secs_left: f64,
    pub min_secs:      f64,
    pub stop_loss:     bool,
    pub take_profit:   bool,
}
