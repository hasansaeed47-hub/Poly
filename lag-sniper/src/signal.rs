/// signal.rs — Black-Scholes fair value + sigma estimation
///
/// fair_yes(spot, strike, sigma, secs_left) -> probability YES wins
/// Used by lag detector: compute fair value from BN price (leading) AND CL price (lagging)
/// The divergence between fair_bn and fair_cl IS the lag signal.

use statrs::distribution::{ContinuousCDF, Normal};

const SECS_PER_YEAR: f64 = 365.25 * 24.0 * 3600.0;
const MIN_SIGMA:     f64 = 0.30;
const MIN_T:         f64 = 1.0 / SECS_PER_YEAR;
const MIN_SAMPLES:   usize = 30;

/// Microstructure noise floor: crypto prices at the 5-min timescale exhibit
/// ~0.10% noise from bid-ask bounce, spread crossing, and CL feed jitter.
/// When BS remaining_vol drops below this, the model becomes overconfident
/// on tiny moves. Inflate sigma so remaining_vol never falls below this floor.
const NOISE_FLOOR: f64 = 0.0010;

/// Binary call fair value: P(spot > strike at expiry)
/// = N(d1) where d1 = ln(S/K) / (sigma * sqrt(T))
pub fn fair_yes(cl: f64, open: f64, sigma: f64, secs_left: f64) -> f64 {
    let sigma = sigma.max(MIN_SIGMA);
    let t     = (secs_left / SECS_PER_YEAR).max(MIN_T);

    let remaining_vol = sigma * t.sqrt();
    let effective_sigma = if remaining_vol < NOISE_FLOOR {
        NOISE_FLOOR / t.sqrt()
    } else {
        sigma
    };

    let d1    = (cl / open).ln() / (effective_sigma * t.sqrt());
    let n     = Normal::new(0.0, 1.0).expect("normal distribution");
    n.cdf(d1).clamp(0.001, 0.999)
}

/// Rolling realized volatility from price history.
/// Returns annualized sigma.
pub fn estimate_sigma(prices: &[(f64, f64)], window_secs: f64, now: f64) -> f64 {
    let cutoff = now - window_secs;
    let window: Vec<f64> = prices.iter()
        .filter(|(ts, _)| *ts >= cutoff)
        .map(|(_, p)| *p)
        .collect();

    if window.len() < MIN_SAMPLES { return 0.50; }

    let returns: Vec<f64> = window.windows(2)
        .filter(|w| w[0] > 0.0 && w[1] > 0.0)
        .map(|w| (w[1] / w[0]).ln())
        .collect();

    if returns.len() < 2 { return MIN_SIGMA; }

    let n    = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let var  = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0).max(1.0);
    let std  = var.sqrt();

    let annualised = std * SECS_PER_YEAR.sqrt();
    if annualised.is_nan() || annualised.is_infinite() { return MIN_SIGMA; }
    annualised.clamp(MIN_SIGMA, 5.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fair_yes_at_open_is_half() {
        let f = fair_yes(100.0, 100.0, 0.01, 300.0);
        assert!((f - 0.5).abs() < 0.01, "fair_yes at open = {}", f);
    }

    #[test]
    fn fair_yes_above_open_is_above_half() {
        let f = fair_yes(101.0, 100.0, 0.01, 300.0);
        assert!(f > 0.5, "got {}", f);
    }

    #[test]
    fn sigma_estimation_basic() {
        let prices: Vec<(f64, f64)> = (0..60).map(|i| (i as f64, 100.0)).collect();
        let s = estimate_sigma(&prices, 300.0, 59.0);
        assert!(s <= MIN_SIGMA + 0.01, "flat prices should give min sigma, got {}", s);
    }

    #[test]
    fn fair_divergence_from_lag() {
        // Simulate BN at 100.05 (0.05% move), CL at 100, open at 100
        // With sigma=0.50 and 300s left, a 0.05% move produces modest divergence
        let fair_bn = fair_yes(100.05, 100.0, 0.50, 300.0);
        let fair_cl = fair_yes(100.0, 100.0, 0.50, 300.0);
        let divergence = fair_bn - fair_cl;
        assert!(divergence > 0.0, "BN should show higher fair than CL");
        assert!((fair_cl - 0.5).abs() < 0.01, "CL at open should be ~0.50");
        assert!(fair_bn > 0.5, "BN above open should give fair > 0.50");
    }
}
