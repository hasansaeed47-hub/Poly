/// arb.rs — Pair Arbitrage Engine
///
/// Strategy: Buy YES and NO shares in matched sets so they settle for $1.00.
/// Profit = $1.00 - (avg_yes + avg_no) per matched share.
///
/// Core rules:
///   1. Matched sets: max 1 unmatched unit at any time
///   2. Trend filter: skip trending markets (directional move = bad for arb)
///   3. Maker only: zero entry fees
///   4. Sequential: one order at a time globally
///   5. Entry condition: keep avg(YES) + avg(NO) < max_pair_cost ($0.98)
///   6. Max $10 exposure per window; merged pairs free up capital
///   7. 15m + 60m markets only

use serde::{Deserialize, Serialize};
use tracing::info;

// -- Helpers ------------------------------------------------------------------

const STDEV_BASE: f64 = 0.167;

fn stdev_for(asset: &str) -> f64 {
    match asset {
        "btc" => 0.167,
        "eth" => 0.194,
        "sol" => 0.247,
        "xrp" => 0.440,
        _ => STDEV_BASE,
    }
}

pub fn stdev_scale(asset: &str) -> f64 {
    stdev_for(asset) / STDEV_BASE
}

// -- Config -------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct ArbConfig {
    /// Dollars per buy unit
    pub unit_size: f64,
    /// Max net-at-risk per window (merged pairs don't count)
    pub max_exposure: f64,
    /// Stop accumulating if running pair cost exceeds this
    pub max_pair_cost: f64,
    /// Never buy asks above this price
    pub max_ask: f64,
    /// Skip near-zero illiquid tokens
    pub min_ask: f64,
    /// Cancel unfilled maker order after this many seconds
    pub maker_timeout_secs: f64,
    /// |CL delta from open| > this% (BTC base, stdev-scaled) = trending
    pub trend_threshold_pct: f64,
    /// Observation warmup for 15m windows (seconds from window_start)
    pub observe_secs_15m: f64,
    /// Observation warmup for 60m windows
    pub observe_secs_60m: f64,
    /// Stop new units with this many secs left (15m)
    pub lockdown_secs_15m: f64,
    /// Stop new units with this many secs left (60m)
    pub lockdown_secs_60m: f64,
}

impl Default for ArbConfig {
    fn default() -> Self {
        ArbConfig {
            unit_size: 2.0,
            max_exposure: 10.0,
            max_pair_cost: 0.98,
            max_ask: 0.50,
            min_ask: 0.03,
            maker_timeout_secs: 30.0,
            trend_threshold_pct: 0.08,
            observe_secs_15m: 120.0,
            observe_secs_60m: 300.0,
            lockdown_secs_15m: 90.0,
            lockdown_secs_60m: 300.0,
        }
    }
}

// -- Types --------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Yes,
    No,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Side::Yes => write!(f, "YES"),
            Side::No  => write!(f, "NO"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbPhase {
    /// Warmup: collecting price baseline, no entries
    Observing,
    /// Main body: accepting entries
    Active,
    /// Near window end: completing pending pairs only, no new units
    Lockdown,
    /// Window expired: compute settlement
    Settled,
}

/// A pending maker limit order (one at a time globally)
#[derive(Debug, Clone)]
pub struct PendingOrder {
    pub slug:      String,
    pub side:      Side,
    pub token_id:  String,
    pub price:     f64,     // our limit price (ask - 0.01)
    pub shares:    f64,     // unit_size / price
    pub cost:      f64,     // unit_size
    pub posted_at: f64,     // unix ts
}

// -- ArbBook (per-window) -----------------------------------------------------

/// Per-window arbitrage accumulator. Tracks matched YES/NO positions.
pub struct ArbBook {
    pub slug:         String,
    pub asset:        String,
    pub tf:           u32,
    pub window_start: u64,
    pub window_end:   u64,
    pub token_yes:    String,
    pub token_no:     String,

    // YES accumulator
    pub yes_shares: f64,
    pub yes_cost:   f64,
    pub yes_fills:  u32,

    // NO accumulator
    pub no_shares: f64,
    pub no_cost:   f64,
    pub no_fills:  u32,

    // Phase
    pub phase: ArbPhase,

    // CL open price (for trend detection)
    pub cl_open: f64,

    // Trend state
    pub is_trending: bool,
}

impl ArbBook {
    pub fn new(
        slug: String, asset: String, tf: u32,
        window_start: u64, window_end: u64,
        token_yes: String, token_no: String,
    ) -> Self {
        ArbBook {
            slug, asset, tf, window_start, window_end,
            token_yes, token_no,
            yes_shares: 0.0, yes_cost: 0.0, yes_fills: 0,
            no_shares: 0.0, no_cost: 0.0, no_fills: 0,
            phase: ArbPhase::Observing,
            cl_open: 0.0,
            is_trending: false,
        }
    }

    // -- Averages and metrics -------------------------------------------------

    pub fn yes_avg(&self) -> f64 {
        if self.yes_shares > 0.0 { self.yes_cost / self.yes_shares } else { 0.0 }
    }

    pub fn no_avg(&self) -> f64 {
        if self.no_shares > 0.0 { self.no_cost / self.no_shares } else { 0.0 }
    }

    /// Running pair cost (meaningful only when both sides have shares)
    pub fn pair_cost(&self) -> f64 {
        if self.yes_shares > 0.0 && self.no_shares > 0.0 {
            self.yes_avg() + self.no_avg()
        } else {
            0.0
        }
    }

    pub fn pairs_complete(&self) -> u32 {
        self.yes_fills.min(self.no_fills)
    }

    pub fn matched_shares(&self) -> f64 {
        self.yes_shares.min(self.no_shares)
    }

    /// Net capital at risk = total spent - guaranteed settlement return.
    /// Matched shares settle for $1.00 each regardless of outcome.
    pub fn net_at_risk(&self) -> f64 {
        let total_spent = self.yes_cost + self.no_cost;
        let guaranteed = self.matched_shares(); // * $1.00
        total_spent - guaranteed
    }

    /// Locked profit from fully matched pairs
    pub fn locked_profit(&self) -> f64 {
        let matched = self.matched_shares();
        if matched > 0.0 && self.pair_cost() > 0.0 {
            matched * (1.0 - self.pair_cost())
        } else {
            0.0
        }
    }

    /// Has an unmatched unit pending completion?
    pub fn has_unmatched(&self) -> bool {
        self.yes_fills != self.no_fills
    }

    // -- Matched sets constraint ----------------------------------------------

    /// Which sides are allowed for the next buy?
    /// Balanced -> either. Imbalanced -> must buy the lagging side.
    pub fn allowed_sides(&self) -> Vec<Side> {
        if self.yes_fills == self.no_fills {
            vec![Side::Yes, Side::No]
        } else if self.yes_fills > self.no_fills {
            vec![Side::No]  // YES ahead, must buy NO
        } else {
            vec![Side::Yes] // NO ahead, must buy YES
        }
    }

    // -- Entry validation -----------------------------------------------------

    /// Can we buy `side` at `ask_price` and stay within all constraints?
    pub fn can_buy(&self, side: Side, ask_price: f64, cfg: &ArbConfig) -> bool {
        // Phase check
        if self.phase != ArbPhase::Active && self.phase != ArbPhase::Lockdown {
            return false;
        }

        // In lockdown, only allow completing an unmatched pair
        if self.phase == ArbPhase::Lockdown {
            if !self.has_unmatched() { return false; }
            let needed = if self.yes_fills > self.no_fills { Side::No } else { Side::Yes };
            if side != needed { return false; }
        }

        // Trending -> skip arb
        if self.is_trending { return false; }

        // Matched sets
        if !self.allowed_sides().contains(&side) { return false; }

        // Ask price bounds
        if ask_price > cfg.max_ask || ask_price < cfg.min_ask { return false; }

        // Exposure cap (net at risk + new unit cost)
        if self.net_at_risk() + cfg.unit_size > cfg.max_exposure { return false; }

        // Pair cost projection
        let new_shares = cfg.unit_size / ask_price;
        match side {
            Side::Yes => {
                let new_yes_avg = (self.yes_cost + cfg.unit_size) / (self.yes_shares + new_shares);
                if self.no_shares > 0.0 {
                    if new_yes_avg + self.no_avg() > cfg.max_pair_cost { return false; }
                } else {
                    // No NO yet — YES avg must leave room for NO
                    if new_yes_avg > cfg.max_pair_cost - cfg.min_ask { return false; }
                }
            }
            Side::No => {
                let new_no_avg = (self.no_cost + cfg.unit_size) / (self.no_shares + new_shares);
                if self.yes_shares > 0.0 {
                    if self.yes_avg() + new_no_avg > cfg.max_pair_cost { return false; }
                } else {
                    if new_no_avg > cfg.max_pair_cost - cfg.min_ask { return false; }
                }
            }
        }

        true
    }

    // -- Record a fill --------------------------------------------------------

    pub fn record_fill(&mut self, side: Side, _price: f64, shares: f64, cost: f64) {
        match side {
            Side::Yes => {
                self.yes_shares += shares;
                self.yes_cost += cost;
                self.yes_fills += 1;
            }
            Side::No => {
                self.no_shares += shares;
                self.no_cost += cost;
                self.no_fills += 1;
            }
        }

        // Check for pair completion
        let pairs = self.pairs_complete();
        if pairs > 0 && self.yes_fills == self.no_fills {
            info!("[ARB] PAIR MERGED {} | pairs={} pair_cost=${:.4} locked_profit=${:.4} net_risk=${:.2}",
                self.slug, pairs, self.pair_cost(), self.locked_profit(), self.net_at_risk());
        }
    }

    // -- Phase management -----------------------------------------------------

    pub fn update_phase(&mut self, now: f64, cfg: &ArbConfig) {
        if self.phase == ArbPhase::Settled { return; }

        let elapsed = now - self.window_start as f64;
        let remaining = self.window_end as f64 - now;
        let observe_secs = if self.tf == 60 { cfg.observe_secs_60m } else { cfg.observe_secs_15m };
        let lockdown_secs = if self.tf == 60 { cfg.lockdown_secs_60m } else { cfg.lockdown_secs_15m };

        if remaining <= 0.0 {
            self.phase = ArbPhase::Settled;
        } else if remaining <= lockdown_secs {
            if self.phase != ArbPhase::Settled {
                self.phase = ArbPhase::Lockdown;
            }
        } else if self.phase == ArbPhase::Observing && elapsed >= observe_secs && self.cl_open > 0.0 {
            self.phase = ArbPhase::Active;
        }
    }

    // -- Trend detection ------------------------------------------------------

    /// Update trending flag based on CL delta from window open
    pub fn check_trend(&mut self, cl_now: f64, cfg: &ArbConfig) {
        if self.cl_open <= 0.0 || cl_now <= 0.0 { return; }
        let delta_pct = ((cl_now - self.cl_open) / self.cl_open * 100.0).abs();
        let threshold = cfg.trend_threshold_pct * stdev_scale(&self.asset);
        let was_trending = self.is_trending;
        self.is_trending = delta_pct > threshold;

        if self.is_trending && !was_trending {
            info!("[ARB] TREND {} | delta={:+.4}% thr={:.4}% — skipping arb",
                self.slug, delta_pct, threshold);
        }
    }

    // -- Settlement -----------------------------------------------------------

    /// Compute settlement P&L
    pub fn settle(&self) -> ArbSettlement {
        let matched = self.matched_shares();
        let unmatched_yes = self.yes_shares - matched;
        let unmatched_no = self.no_shares - matched;

        // Matched shares: guaranteed $1.00 return, cost = matched * pair_cost
        let matched_pnl = if matched > 0.0 {
            matched * 1.0 - matched * (self.yes_avg() + self.no_avg())
        } else {
            0.0
        };

        ArbSettlement {
            asset: self.asset.clone(),
            tf: self.tf,
            pairs_complete: self.pairs_complete(),
            matched_shares: matched,
            matched_pnl,
            unmatched_yes_shares: unmatched_yes,
            unmatched_no_shares: unmatched_no,
            unmatched_yes_cost: if unmatched_yes > 0.0 { unmatched_yes * self.yes_avg() } else { 0.0 },
            unmatched_no_cost: if unmatched_no > 0.0 { unmatched_no * self.no_avg() } else { 0.0 },
            yes_avg: self.yes_avg(),
            no_avg: self.no_avg(),
            pair_cost: self.pair_cost(),
            total_cost: self.yes_cost + self.no_cost,
        }
    }

    // -- Status ---------------------------------------------------------------

    pub fn status(&self) -> String {
        let phase = match self.phase {
            ArbPhase::Observing => "OBS",
            ArbPhase::Active    => "ACT",
            ArbPhase::Lockdown  => "LCK",
            ArbPhase::Settled   => "SET",
        };
        let trend = if self.is_trending { "T" } else { "" };
        format!("{} {} {}Y/{}N pc={:.3} nar=${:.2} lp=${:.2}{}",
            self.slug, phase,
            self.yes_fills, self.no_fills,
            self.pair_cost(), self.net_at_risk(), self.locked_profit(),
            trend)
    }
}

// -- Settlement result --------------------------------------------------------

#[derive(Debug)]
pub struct ArbSettlement {
    pub asset:              String,
    pub tf:                 u32,
    pub pairs_complete:     u32,
    pub matched_shares:     f64,
    pub matched_pnl:        f64,
    pub unmatched_yes_shares: f64,
    pub unmatched_no_shares:  f64,
    pub unmatched_yes_cost:   f64,
    pub unmatched_no_cost:    f64,
    pub yes_avg:            f64,
    pub no_avg:             f64,
    pub pair_cost:          f64,
    pub total_cost:         f64,
}

impl ArbSettlement {
    /// Compute final P&L given settlement direction ("UP" = YES wins)
    pub fn final_pnl(&self, winner: &str) -> f64 {
        let mut pnl = self.matched_pnl;

        // Unmatched YES shares
        if self.unmatched_yes_shares > 0.0 {
            if winner == "UP" {
                pnl += self.unmatched_yes_shares * 1.0 - self.unmatched_yes_cost;
            } else {
                pnl -= self.unmatched_yes_cost;
            }
        }

        // Unmatched NO shares
        if self.unmatched_no_shares > 0.0 {
            if winner == "DOWN" {
                pnl += self.unmatched_no_shares * 1.0 - self.unmatched_no_cost;
            } else {
                pnl -= self.unmatched_no_cost;
            }
        }

        pnl
    }
}

// -- Trade log ----------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct ArbTradeLog {
    pub event:  String,
    pub ts:     f64,
    pub slug:   String,
    pub asset:  String,
    pub tf:     u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub side:   Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price:  Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shares: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost:   Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub yes_avg: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub no_avg:  Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pair_cost: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pairs_complete: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub net_at_risk: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locked_profit: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pnl: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cl_delta_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
}

// -- Maker price helper -------------------------------------------------------

/// Maker limit price: ask - 0.01, clamped to 2dp
pub fn maker_limit_price(best_ask: f64) -> f64 {
    ((best_ask - 0.01) * 100.0).round() / 100.0
}

// -- Tests --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cfg() -> ArbConfig {
        ArbConfig {
            unit_size: 2.0,
            max_exposure: 10.0,
            max_pair_cost: 0.98,
            max_ask: 0.50,
            min_ask: 0.03,
            maker_timeout_secs: 30.0,
            trend_threshold_pct: 0.08,
            observe_secs_15m: 120.0,
            observe_secs_60m: 300.0,
            lockdown_secs_15m: 90.0,
            lockdown_secs_60m: 300.0,
        }
    }

    fn test_book(tf: u32) -> ArbBook {
        let ws = 1000u64;
        let we = ws + tf as u64 * 60;
        let mut book = ArbBook::new(
            format!("btc-updown-{}m-{}", tf, ws),
            "btc".into(), tf, ws, we,
            "yes_token".into(), "no_token".into(),
        );
        book.cl_open = 100000.0;
        book.phase = ArbPhase::Active;
        book
    }

    #[test]
    fn matched_shares_balanced() {
        let mut b = test_book(15);
        b.record_fill(Side::Yes, 0.45, 4.44, 2.0);
        b.record_fill(Side::No, 0.48, 4.17, 2.0);
        assert!((b.matched_shares() - 4.17).abs() < 0.01);
        assert!(b.pair_cost() < 0.98);
        assert!(b.locked_profit() > 0.0);
    }

    #[test]
    fn net_at_risk_decreases_with_pairs() {
        let mut b = test_book(15);
        b.record_fill(Side::Yes, 0.45, 4.44, 2.0);
        let nar_one_side = b.net_at_risk();
        assert!((nar_one_side - 2.0).abs() < 0.01);

        b.record_fill(Side::No, 0.48, 4.17, 2.0);
        let nar_paired = b.net_at_risk();
        assert!(nar_paired < nar_one_side);
        assert!(nar_paired < 0.5);
    }

    #[test]
    fn matched_sets_constraint() {
        let cfg = test_cfg();
        let mut b = test_book(15);
        assert!(b.can_buy(Side::Yes, 0.45, &cfg));
        assert!(b.can_buy(Side::No, 0.45, &cfg));

        b.record_fill(Side::Yes, 0.45, 4.44, 2.0);
        b.yes_fills = 1;
        assert!(!b.can_buy(Side::Yes, 0.45, &cfg));
        assert!(b.can_buy(Side::No, 0.48, &cfg));

        b.record_fill(Side::No, 0.48, 4.17, 2.0);
        b.no_fills = 1;
        assert!(b.can_buy(Side::Yes, 0.45, &cfg));
        assert!(b.can_buy(Side::No, 0.48, &cfg));
    }

    #[test]
    fn exposure_cap() {
        let mut b = test_book(15);
        for _ in 0..4 {
            b.record_fill(Side::Yes, 0.45, 4.44, 2.0);
            b.yes_fills += 1;
            b.record_fill(Side::No, 0.48, 4.17, 2.0);
            b.no_fills += 1;
        }
        assert!(b.net_at_risk() < 2.0);
    }

    #[test]
    fn pair_cost_rejects_expensive() {
        let cfg = test_cfg();
        let b = test_book(15);
        assert!(b.can_buy(Side::Yes, 0.50, &cfg)); // at max_ask boundary
        assert!(!b.can_buy(Side::Yes, 0.51, &cfg)); // over max_ask
    }

    #[test]
    fn pair_cost_rejects_above_threshold() {
        let cfg = test_cfg();
        let mut b = test_book(15);
        // YES at 0.50, NO at 0.49 → pair_cost = 0.99 > 0.98
        b.record_fill(Side::Yes, 0.50, 4.0, 2.0);
        b.yes_fills = 1;
        assert!(!b.can_buy(Side::No, 0.49, &cfg)); // would exceed max_pair_cost
    }

    #[test]
    fn trending_blocks_entry() {
        let cfg = test_cfg();
        let mut b = test_book(15);
        b.is_trending = true;
        assert!(!b.can_buy(Side::Yes, 0.45, &cfg));
    }

    #[test]
    fn settlement_matched_profit() {
        let mut b = test_book(15);
        b.record_fill(Side::Yes, 0.45, 4.44, 2.0);
        b.yes_fills = 1;
        b.record_fill(Side::No, 0.48, 4.17, 2.0);
        b.no_fills = 1;

        let s = b.settle();
        assert!(s.matched_pnl > 0.0);
        assert!(s.unmatched_yes_shares < 0.5);
        let pnl_up = s.final_pnl("UP");
        let pnl_down = s.final_pnl("DOWN");
        assert!(pnl_up > 0.0 || pnl_down > 0.0);
    }

    #[test]
    fn maker_limit_price_calc() {
        assert_eq!(maker_limit_price(0.45), 0.44);
        assert_eq!(maker_limit_price(0.50), 0.49);
        assert_eq!(maker_limit_price(0.03), 0.02);
    }

    #[test]
    fn phase_transitions() {
        let cfg = test_cfg();
        let mut b = test_book(15);
        b.phase = ArbPhase::Observing;
        b.cl_open = 100000.0;

        b.update_phase(b.window_start as f64 + 60.0, &cfg);
        assert_eq!(b.phase, ArbPhase::Observing);

        b.update_phase(b.window_start as f64 + 130.0, &cfg);
        assert_eq!(b.phase, ArbPhase::Active);

        b.update_phase(b.window_end as f64 - 80.0, &cfg);
        assert_eq!(b.phase, ArbPhase::Lockdown);

        b.update_phase(b.window_end as f64 + 1.0, &cfg);
        assert_eq!(b.phase, ArbPhase::Settled);
    }

    #[test]
    fn trend_detection_scales_by_asset() {
        let cfg = test_cfg();

        let mut btc = test_book(15);
        btc.cl_open = 100000.0;
        btc.check_trend(100100.0, &cfg); // +0.1% > 0.08% threshold
        assert!(btc.is_trending);

        let mut sol = test_book(15);
        sol.asset = "sol".into();
        sol.cl_open = 100.0;
        sol.check_trend(100.10, &cfg); // +0.1% vs 0.08*1.48=0.118% threshold
        assert!(!sol.is_trending); // SOL needs bigger move due to stdev scaling
    }

    #[test]
    fn lockdown_only_completes_pairs() {
        let cfg = test_cfg();
        let mut b = test_book(15);
        b.phase = ArbPhase::Lockdown;

        // No unmatched — cannot buy anything
        assert!(!b.can_buy(Side::Yes, 0.45, &cfg));
        assert!(!b.can_buy(Side::No, 0.45, &cfg));

        // Create imbalance
        b.record_fill(Side::Yes, 0.45, 4.44, 2.0);
        b.yes_fills = 1;

        // Can only buy the lagging side
        assert!(!b.can_buy(Side::Yes, 0.45, &cfg));
        assert!(b.can_buy(Side::No, 0.48, &cfg));
    }
}
