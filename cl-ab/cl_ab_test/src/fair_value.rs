/// Black-Scholes binary (cash-or-nothing call): P(CL_close > CL_open).
/// Zero drift. Both prices must be Chainlink — PM settles on CL, not Binance.
/// Returns probability in [0.0, 1.0]. Returns 0.5 on degenerate input.
pub fn compute(cl_price: f64, cl_open: f64, sigma_per_sec: f64, secs_remaining: f64) -> f64 {
    if secs_remaining <= 0.0 {
        return if cl_price > cl_open { 1.0 } else { 0.0 };
    }
    if sigma_per_sec <= 0.0 || cl_open <= 0.0 || cl_price <= 0.0 {
        return 0.5;
    }
    let d = (cl_price - cl_open) / (sigma_per_sec * secs_remaining.sqrt());
    0.5 * (1.0 + erf(d / std::f64::consts::SQRT_2))
}

/// Annualised vol → per-second sigma in price units (fallback when history thin).
pub fn sigma_fallback(asset_annual_vol: f64, price: f64) -> f64 {
    (asset_annual_vol / 365_f64.sqrt()) * price / 86400_f64.sqrt()
}

/// Abramowitz & Stegun erf approximation — error < 1.5e-7.
fn erf(x: f64) -> f64 {
    let t = 1.0 / (1.0 + 0.3275911 * x.abs());
    let p = t * (0.254829592 + t * (-0.284496736 + t * (1.421413741 + t * (-1.453152027 + t * 1.061405429))));
    x.signum() * (1.0 - p * (-x * x).exp())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn expiry()   { assert_eq!(compute(95100.0, 95000.0, 9.7, 0.0), 1.0); }
    #[test] fn symmetry() { let u = compute(95100.0, 95000.0, 9.7, 300.0); let d = compute(94900.0, 95000.0, 9.7, 300.0); assert!((u+d-1.0).abs() < 1e-9); }
    #[test] fn degenerate() { assert_eq!(compute(95000.0, 95000.0, 0.0, 300.0), 0.5); }
}
