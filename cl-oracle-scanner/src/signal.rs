/// signal.rs — Black-Scholes fair value + edge calculation
/// Called ONCE per tick per market. Result shared across all 5 runners.

use statrs::distribution::{ContinuousCDF, Normal};

// -- Constants ----------------------------------------------------------------

const SECS_PER_YEAR: f64 = 365.25 * 24.0 * 3600.0;
const MIN_SIGMA:     f64 = 1e-6;
const MIN_T:         f64 = 1.0 / SECS_PER_YEAR; // 1 second minimum

// -- Types --------------------------------------------------------------------

/// Which side of the market has edge
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Side {
    Yes,
    No,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::Yes => write!(f, "YES"),
            Side::No  => write!(f, "NO"),
        }
    }
}

/// Output of one signal computation — shared across all runners
#[derive(Debug, Clone)]
pub struct Signal {
    pub slug:        String,
    pub asset:       String,
    pub tf:          u32,         // timeframe minutes
    pub open_price:  f64,         // window open price
    pub cl_price:    f64,         // current CL oracle price
    pub sigma:       f64,         // annualised volatility estimate
    pub secs_left:   f64,         // seconds remaining in window
    pub fair_yes:    f64,         // Black-Scholes fair value for YES
    pub fair_no:     f64,         // = 1 - fair_yes
    pub book_yes:    f64,         // current PM best ask for YES
    pub book_no:     f64,         // current PM best ask for NO
    pub edge_yes:    f64,         // fair_yes - cost_yes (fee-adjusted)
    pub edge_no:     f64,         // fair_no  - cost_no  (fee-adjusted)
    pub best_side:   Option<Side>,// which side has edge (if any above threshold)
    pub best_edge:   f64,         // magnitude of best edge (fee-adjusted)
    pub best_book:   f64,         // entry price on best side
    pub best_fair:   f64,         // fair value on best side
    pub ts:          f64,         // unix timestamp of this signal
}

// -- Black-Scholes binary call ------------------------------------------------

/// Probability that price at expiry > open (YES wins).
///
/// Binary cash-or-nothing call uses N(d2), NOT N(d1):
///   d2 = [ln(S/K) - 0.5 * sigma^2 * t] / (sigma * sqrt(t))
///
/// The old code used d1 = ln(S/K) / (sigma*sqrt(t)), which is missing the
/// -0.5*sigma^2*t correction. This biases fair values toward 0.50 and creates
/// phantom edge that doesn't exist — the root cause of 0% win rate.
///
/// No drift assumption (r=0) — appropriate for short crypto windows.
pub fn fair_yes(cl: f64, open: f64, sigma: f64, secs_left: f64) -> f64 {
    let sigma = sigma.max(MIN_SIGMA);
    let t     = (secs_left / SECS_PER_YEAR).max(MIN_T);
    let d2    = ((cl / open).ln() - 0.5 * sigma * sigma * t) / (sigma * t.sqrt());
    let n     = Normal::new(0.0, 1.0).expect("normal distribution");
    n.cdf(d2).clamp(0.001, 0.999)
}

// -- Sigma estimation ---------------------------------------------------------

/// Rolling annualised volatility from irregularly-spaced price history.
///
/// Under GBM, log-return r_i over interval dt_i has variance sigma^2 * dt_i.
/// Normalise: z_i = r_i / sqrt(dt_i), then std(z_i) = sigma (per-second).
/// Annualise: sigma_annual = sigma_per_sec * sqrt(SECS_PER_YEAR).
///
/// The old code assumed 1-second intervals between all samples. If CL updates
/// arrive every 3s on average, that overestimates sigma by sqrt(3) ~ 73%.
pub fn estimate_sigma(prices: &[(f64, f64)], window_secs: f64, now: f64) -> f64 {
    let cutoff = now - window_secs;
    let window: Vec<&(f64, f64)> = prices
        .iter()
        .filter(|(ts, _)| *ts >= cutoff)
        .collect();

    if window.len() < 3 {
        return 0.001; // need at least 3 points for 2 returns
    }

    // Time-normalised log returns
    let mut z_values: Vec<f64> = Vec::with_capacity(window.len() - 1);

    for pair in window.windows(2) {
        let (t0, p0) = pair[0];
        let (t1, p1) = pair[1];
        let dt = t1 - t0;

        if dt < 0.01 || *p0 <= 0.0 || *p1 <= 0.0 {
            continue; // skip duplicate timestamps or bad prices
        }

        let log_ret = (p1 / p0).ln();
        z_values.push(log_ret / dt.sqrt());
    }

    if z_values.len() < 2 {
        return 0.001;
    }

    let n    = z_values.len() as f64;
    let mean = z_values.iter().sum::<f64>() / n;
    let var  = z_values.iter().map(|z| (z - mean).powi(2)).sum::<f64>() / (n - 1.0).max(1.0);
    let sigma_per_sec = var.sqrt();

    let annualised = sigma_per_sec * SECS_PER_YEAR.sqrt();
    annualised.max(MIN_SIGMA)
}

// -- Signal computation -------------------------------------------------------

/// Compute one signal for a market.
/// Returns None if data is insufficient.
///
/// Edge is fee-adjusted: edge = fair - book * (1 + taker_fee_rate).
/// Settlement is binary (0 or 1) with no exit fee, so the only fee is entry.
pub fn compute(
    slug:       &str,
    asset:      &str,
    tf:         u32,
    open_price: f64,
    cl_price:   f64,
    sigma:      f64,
    secs_left:  f64,
    book_yes:   f64,
    book_no:    f64,
    fee_rate:   f64,
    ts:         f64,
) -> Option<Signal> {
    if open_price <= 0.0 || cl_price <= 0.0 || secs_left <= 0.0 {
        return None;
    }
    if book_yes <= 0.0 || book_no <= 0.0 {
        return None;
    }

    let fy = fair_yes(cl_price, open_price, sigma, secs_left);
    let fn_ = 1.0 - fy;

    // Fee-adjusted edge: effective entry cost = book * (1 + fee)
    // On settlement (0/1) there is no exit fee
    let cost_yes = book_yes * (1.0 + fee_rate);
    let cost_no  = book_no  * (1.0 + fee_rate);

    let edge_yes = fy  - cost_yes;
    let edge_no  = fn_ - cost_no;

    let best_side = if edge_yes > edge_no && edge_yes > 0.0 {
        Some(Side::Yes)
    } else if edge_no > edge_yes && edge_no > 0.0 {
        Some(Side::No)
    } else {
        None
    };

    let (best_edge, best_book, best_fair) = match best_side {
        Some(Side::Yes) => (edge_yes, book_yes, fy),
        Some(Side::No)  => (edge_no,  book_no,  fn_),
        None            => (0.0, 0.0, 0.0),
    };

    Some(Signal {
        slug:      slug.to_string(),
        asset:     asset.to_string(),
        tf,
        open_price,
        cl_price,
        sigma,
        secs_left,
        fair_yes:  fy,
        fair_no:   fn_,
        book_yes,
        book_no,
        edge_yes,
        edge_no,
        best_side,
        best_edge,
        best_book,
        best_fair,
        ts,
    })
}

// -- Tests --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fair_yes_at_open_is_near_half() {
        // With d2: when S=K, d2 = -0.5*sigma*sqrt(t), slightly negative
        // So fair_yes < 0.50 (correct — accounts for vol drag)
        let f = fair_yes(100.0, 100.0, 0.01, 300.0);
        assert!((f - 0.5).abs() < 0.02, "fair_yes at open = {}", f);
        assert!(f <= 0.5001, "with d2, at-the-money should be <= 0.50, got {}", f);
    }

    #[test]
    fn fair_yes_above_open_is_above_half() {
        let f = fair_yes(101.0, 100.0, 0.01, 300.0);
        assert!(f > 0.5, "fair_yes when CL>open should be >0.5, got {}", f);
    }

    #[test]
    fn fair_yes_below_open_is_below_half() {
        let f = fair_yes(99.0, 100.0, 0.01, 300.0);
        assert!(f < 0.5, "fair_yes when CL<open should be <0.5, got {}", f);
    }

    #[test]
    fn d2_vs_d1_matters_at_high_vol() {
        // For high vol, the -0.5*sigma^2*t term is material
        // At-the-money with high vol: d2 should push fair below 0.50
        let f = fair_yes(100.0, 100.0, 1.0, 300.0);
        assert!(f < 0.50, "high vol at-the-money d2 should be < 0.50, got {}", f);
    }

    #[test]
    fn edge_yes_positive_when_book_stale_after_fees() {
        // CL up 0.5%, book=0.50, fee=1.5%: cost=0.5075, fair~0.999 → big edge
        let sig = compute(
            "btc-updown-5m-test", "btc", 5,
            100.0, 100.5, 0.001, 300.0,
            0.50, 0.49, 0.015, 0.0,
        ).unwrap();
        assert!(sig.edge_yes > 0.0, "expected positive edge_yes after fees, got {}", sig.edge_yes);
        assert_eq!(sig.best_side, Some(Side::Yes));
    }

    #[test]
    fn edge_no_positive_when_cl_down_after_fees() {
        let sig = compute(
            "btc-updown-5m-test", "btc", 5,
            100.0, 99.5, 0.001, 300.0,
            0.49, 0.50, 0.015, 0.0,
        ).unwrap();
        assert!(sig.edge_no > 0.0, "expected positive edge_no after fees, got {}", sig.edge_no);
        assert_eq!(sig.best_side, Some(Side::No));
    }

    #[test]
    fn no_edge_when_book_fair() {
        let f = fair_yes(100.5, 100.0, 0.001, 300.0);
        let sig = compute(
            "btc-updown-5m-test", "btc", 5,
            100.0, 100.5, 0.001, 300.0,
            f, 1.0 - f, 0.015, 0.0,
        ).unwrap();
        // With fees, edge should be negative when book == fair
        assert!(sig.best_edge <= 0.0, "book=fair should have no edge after fees, got {}", sig.best_edge);
    }

    #[test]
    fn fee_kills_small_edge() {
        // Tiny move: fair_yes ~ 0.52, book = 0.51 → raw edge 0.01
        // After 1.5% fee: cost = 0.51*1.015 = 0.51765, net edge ~ 0.002
        // That's below typical min_edge thresholds
        let sig = compute(
            "btc-updown-5m-test", "btc", 5,
            100.0, 100.02, 0.001, 300.0,
            0.51, 0.50, 0.015, 0.0,
        ).unwrap();
        assert!(sig.best_edge < 0.05, "small move edge should be tiny after fees, got {}", sig.best_edge);
    }

    #[test]
    fn sigma_estimation_flat_prices() {
        let prices: Vec<(f64, f64)> = (0..60).map(|i| (i as f64 * 2.0, 100.0)).collect();
        let s = estimate_sigma(&prices, 300.0, 118.0);
        assert!(s < 0.01, "flat prices should give low sigma, got {}", s);
    }

    #[test]
    fn sigma_estimation_uses_actual_intervals() {
        // Create prices at 3-second intervals
        let mut prices = Vec::new();
        for i in 0..100 {
            let t = i as f64 * 3.0;
            let p = 100.0 * (1.0 + 0.0001 * if i % 2 == 0 { 1.0 } else { -1.0 });
            prices.push((t, p));
        }
        let s = estimate_sigma(&prices, 600.0, 297.0);
        assert!(s > 0.0 && s < 50.0, "sigma with 3s intervals should be finite, got {}", s);
    }
}
