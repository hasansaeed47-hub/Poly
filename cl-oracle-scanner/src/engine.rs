/// engine.rs — 5-Engine Configuration
///
/// Ports the proven A-E engine setup from cl-sniper-10mar into config-driven structs.
/// All thresholds loaded from config.toml — nothing hardcoded in binary.
///
/// Engines:
///   A: 5m  sniper  — low delta, continuity=4, all filters
///   B: 5m  D1      — high delta, instant, all filters
///   C: 15m sniper  — low delta, continuity=4, all filters
///   D: 15m D1      — high delta, instant, all filters
///   E: late scalper — book-price driven, no filters, last 25s

use serde::Deserialize;

// -- Stdev scaling constants (per asset) --------------------------------------

pub const STDEV: &[(&str, f64)] = &[
    ("btc", 0.167),
    ("eth", 0.194),
    ("sol", 0.247),
    ("xrp", 0.440),
];
pub const STDEV_BASE: f64 = 0.167;

pub const MIN_DELTA: &[(&str, f64)] = &[
    ("btc", 0.015),
    ("eth", 0.020),
    ("sol", 0.030),
    ("xrp", 0.050),
];

pub fn stdev_scale(asset: &str) -> f64 {
    let s = STDEV.iter().find(|(k, _)| *k == asset).map(|(_, v)| *v).unwrap_or(STDEV_BASE);
    s / STDEV_BASE
}

pub fn min_delta(asset: &str) -> f64 {
    MIN_DELTA.iter().find(|(k, _)| *k == asset).map(|(_, v)| *v).unwrap_or(0.020)
}

/// Polymarket fee: px * (1 - px) * 0.0625
pub fn pm_fee(px: f64) -> f64 {
    px * (1.0 - px) * 0.0625
}

// -- Engine config (from TOML) ------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    pub id:              String,
    pub tf:              u32,       // 5 or 15 (0 = both, for Engine E)
    pub delta:           f64,       // delta threshold (scaled by stdev)
    pub continuity:      u32,       // ticks above threshold before entry
    pub bn_contra:       bool,      // Binance contra-momentum filter
    pub cl_fade:         bool,      // CL fade filter
    pub regime:          bool,      // 1h regime filter
    pub is_late_scalper: bool,      // Engine E mode

    // Entry window (seconds left in window)
    pub entry_start:     i64,       // max secs left to start considering entry
    pub taker_deadline:  i64,       // min secs left (taker fallback deadline)

    // Book price range
    pub min_entry:       f64,       // minimum ask price to enter
    pub max_entry:       f64,       // maximum ask price to enter
}

impl EngineConfig {
    /// Get the stdev-scaled delta threshold for a specific asset
    pub fn scaled_delta(&self, asset: &str) -> f64 {
        self.delta * stdev_scale(asset)
    }
}

// -- Execution params (from TOML) ---------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct ExecConfig {
    pub stake:              f64,    // $ per trade per engine
    pub max_dd:             f64,    // cumulative drawdown kill switch
    pub sl_pct:             f64,    // SL: bid ≤ this % of fill
    pub sl_confirm_bid:     f64,    // opposing bid must be ≥ this to confirm SL
    pub slip:               f64,    // taker slippage
    pub maker_chase_ticks:  u32,    // maker chase duration (ticks)
    pub settle_delay_secs:  u64,    // wait after window_end for settlement
    pub regime_thresh:      f64,    // 1h range < this = chop, skip
    pub bn_contra_thresh:   f64,    // BN trend threshold for contra filter
    pub cl_fade_thresh:     f64,    // CL trend threshold for fade filter
    pub bn_contra_secs:     u64,    // BN lookback for contra
    pub cl_fade_secs:       u64,    // CL lookback for fade
}

// -- Default engine configs (matching proven 10-Mar setup) --------------------

pub fn default_engines() -> Vec<EngineConfig> {
    vec![
        EngineConfig {
            id: "A".into(), tf: 5, delta: 0.04, continuity: 4,
            bn_contra: true, cl_fade: true, regime: true, is_late_scalper: false,
            entry_start: 57, taker_deadline: 44, min_entry: 0.88, max_entry: 0.98,
        },
        EngineConfig {
            id: "B".into(), tf: 5, delta: 0.15, continuity: 0,
            bn_contra: true, cl_fade: true, regime: true, is_late_scalper: false,
            entry_start: 57, taker_deadline: 44, min_entry: 0.88, max_entry: 0.98,
        },
        EngineConfig {
            id: "C".into(), tf: 15, delta: 0.04, continuity: 4,
            bn_contra: true, cl_fade: true, regime: true, is_late_scalper: false,
            entry_start: 57, taker_deadline: 44, min_entry: 0.88, max_entry: 0.98,
        },
        EngineConfig {
            id: "D".into(), tf: 15, delta: 0.15, continuity: 0,
            bn_contra: true, cl_fade: true, regime: true, is_late_scalper: false,
            entry_start: 57, taker_deadline: 44, min_entry: 0.88, max_entry: 0.98,
        },
        EngineConfig {
            id: "E".into(), tf: 0, delta: 0.0, continuity: 0,
            bn_contra: false, cl_fade: false, regime: false, is_late_scalper: true,
            entry_start: 25, taker_deadline: 3, min_entry: 0.95, max_entry: 0.975,
        },
    ]
}

pub fn default_exec() -> ExecConfig {
    ExecConfig {
        stake: 5.0,
        max_dd: 50.0,
        sl_pct: 0.50,
        sl_confirm_bid: 0.80,
        slip: 0.005,
        maker_chase_ticks: 4,
        settle_delay_secs: 8,
        regime_thresh: 0.3,
        bn_contra_thresh: 0.02,
        cl_fade_thresh: 0.03,
        bn_contra_secs: 15,
        cl_fade_secs: 10,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stdev_scaling() {
        assert!((stdev_scale("btc") - 1.0).abs() < 0.001);
        assert!(stdev_scale("sol") > 1.4);
        assert!(stdev_scale("xrp") > 2.5);
    }

    #[test]
    fn scaled_delta_thresholds() {
        let eng = &default_engines()[0]; // Engine A, delta=0.04
        assert!((eng.scaled_delta("btc") - 0.04).abs() < 0.001);
        assert!(eng.scaled_delta("xrp") > 0.10); // ~0.106
    }

    #[test]
    fn pm_fee_matches_formula() {
        let f = pm_fee(0.90);
        assert!((f - 0.90 * 0.10 * 0.0625).abs() < 1e-10);
    }
}
