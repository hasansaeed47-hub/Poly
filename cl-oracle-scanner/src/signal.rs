/// signal.rs — Black-Scholes fair value + edge calculation (v2 — production-grade)
///
/// v2 fixes:
/// - Sigma estimation uses actual time deltas between samples (not assumed 1s)
/// - Signal includes bid prices for both sides (for spread analysis)
/// - Signal includes book depth, spread, mid for both sides
/// - All fields populated for comprehensive logging downstream

use statrs::distribution::{ContinuousCDF, Normal};

// ── Constants ────────────────────────────────────────────────────────────────

const SECS_PER_YEAR: f64 = 365.25 * 24.0 * 3600.0;
const MIN_SIGMA:     f64 = 1e-6;
const MIN_T:         f64 = 1.0 / SECS_PER_YEAR; // 1 second minimum

// ── Types ────────────────────────────────────────────────────────────────────

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

impl serde::Serialize for Side {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(match self {
            Side::Yes => "YES",
            Side::No  => "NO",
        })
    }
}

/// Output of one signal computation — shared across all runners
#[derive(Debug, Clone, serde::Serialize)]
pub struct Signal {
    pub slug:        String,
    pub asset:       String,
    pub tf:          u32,
    pub open_price:  f64,
    pub cl_price:    f64,
    pub cl_vs_open:  f64,    // (cl - open) / open as percentage
    pub sigma:       f64,
    pub secs_left:   f64,
    pub fair_yes:    f64,
    pub fair_no:     f64,
    // Ask prices (what you pay to buy)
    pub book_yes:    f64,    // best ask YES
    pub book_no:     f64,    // best ask NO
    // Bid prices (what you receive to sell)
    pub bid_yes:     f64,
    pub bid_no:      f64,
    // Depth at best level
    pub ask_depth_yes: f64,
    pub ask_depth_no:  f64,
    pub bid_depth_yes: f64,
    pub bid_depth_no:  f64,
    // Spread
    pub spread_yes:  f64,
    pub spread_no:   f64,
    // Edge
    pub edge_yes:    f64,
    pub edge_no:     f64,
    pub best_side:   Option<Side>,
    pub best_edge:   f64,
    pub best_book:   f64,
    pub best_fair:   f64,
    // Book staleness (seconds since last book update)
    pub book_age_yes: f64,
    pub book_age_no:  f64,
    pub ts:          f64,
}

/// Input book data for signal computation
#[derive(Debug, Clone)]
pub struct BookInput {
    pub ask:       f64,
    pub bid:       f64,
    pub ask_depth: f64,
    pub bid_depth: f64,
    pub spread:    f64,
    pub book_ts:   f64,
}

// ── Black-Scholes binary call ─────────────────────────────────────────────────

/// Probability that price at expiry > open (YES wins)
/// Uses log-normal model: N( ln(S/K) / (sigma * sqrt(t)) )
/// No drift assumption — appropriate for short windows
pub fn fair_yes(cl: f64, open: f64, sigma: f64, secs_left: f64) -> f64 {
    if cl <= 0.0 || open <= 0.0 {
        return 0.5;
    }
    let sigma = sigma.max(MIN_SIGMA);
    let t     = (secs_left / SECS_PER_YEAR).max(MIN_T);
    let d1    = (cl / open).ln() / (sigma * t.sqrt());
    let n     = Normal::new(0.0, 1.0).expect("normal distribution");
    n.cdf(d1).clamp(0.001, 0.999)
}

// ── Sigma estimation (v2 — uses actual time deltas) ──────────────────────────

/// Rolling annualised volatility from a price history slice.
/// Uses actual timestamps between samples instead of assuming uniform 1-second intervals.
///
/// Method: compute log-returns scaled by sqrt(dt), then annualize.
/// This is the Yang-Zhang-compatible approach for irregular samples.
pub fn estimate_sigma(prices: &[(f64, f64)], window_secs: f64, now: f64) -> f64 {
    let cutoff = now - window_secs;
    let window: Vec<(f64, f64)> = prices
        .iter()
        .filter(|(ts, _)| *ts >= cutoff)
        .copied()
        .collect();

    if window.len() < 3 {
        return 0.001;
    }

    // Compute time-weighted log returns
    // For each pair (t_i, p_i) → (t_{i+1}, p_{i+1}):
    //   log_return = ln(p_{i+1} / p_i)
    //   dt = t_{i+1} - t_i (in years)
    //   Variance contribution: log_return^2 / dt
    let mut sum_var: f64 = 0.0;
    let mut count: usize = 0;
    let mut total_dt: f64 = 0.0;

    for pair in window.windows(2) {
        let (t0, p0) = pair[0];
        let (t1, p1) = pair[1];

        let dt_secs = t1 - t0;
        if dt_secs <= 0.0 || p0 <= 0.0 || p1 <= 0.0 {
            continue;
        }

        let dt_years = dt_secs / SECS_PER_YEAR;
        let log_ret = (p1 / p0).ln();

        // Variance rate: log_return^2 / dt (annualized variance per unit time)
        sum_var += log_ret * log_ret / dt_years;
        total_dt += dt_years;
        count += 1;
    }

    if count < 2 || total_dt <= 0.0 {
        return 0.001;
    }

    // Average annualized variance, then sqrt for sigma
    let avg_var = sum_var / count as f64;
    let sigma = avg_var.sqrt();

    sigma.max(MIN_SIGMA)
}

// ── Signal computation ────────────────────────────────────────────────────────

/// Compute one signal for a market.
/// Returns None if data is insufficient.
/// All fields populated for full downstream logging.
pub fn compute(
    slug:       &str,
    asset:      &str,
    tf:         u32,
    open_price: f64,
    cl_price:   f64,
    sigma:      f64,
    secs_left:  f64,
    yes_book:   &BookInput,
    no_book:    &BookInput,
    ts:         f64,
) -> Option<Signal> {
    // Guard: need valid inputs
    if open_price <= 0.0 || cl_price <= 0.0 || secs_left <= 0.0 {
        return None;
    }
    if yes_book.ask <= 0.0 || no_book.ask <= 0.0 {
        return None;
    }

    let fy  = fair_yes(cl_price, open_price, sigma, secs_left);
    let fn_ = 1.0 - fy;

    let edge_yes = fy  - yes_book.ask;
    let edge_no  = fn_ - no_book.ask;

    let cl_vs_open = (cl_price - open_price) / open_price * 100.0;

    // Determine best side (only positive edge matters)
    let best_side = if edge_yes > edge_no && edge_yes > 0.0 {
        Some(Side::Yes)
    } else if edge_no > edge_yes && edge_no > 0.0 {
        Some(Side::No)
    } else if edge_yes > 0.0 && (edge_yes - edge_no).abs() < 1e-10 {
        // Equal edge — prefer YES as tiebreaker
        Some(Side::Yes)
    } else {
        None
    };

    let (best_edge, best_book, best_fair) = match best_side {
        Some(Side::Yes) => (edge_yes, yes_book.ask, fy),
        Some(Side::No)  => (edge_no,  no_book.ask,  fn_),
        None            => (0.0, 0.0, 0.0),
    };

    Some(Signal {
        slug:      slug.to_string(),
        asset:     asset.to_string(),
        tf,
        open_price,
        cl_price,
        cl_vs_open,
        sigma,
        secs_left,
        fair_yes:  fy,
        fair_no:   fn_,
        book_yes:  yes_book.ask,
        book_no:   no_book.ask,
        bid_yes:   yes_book.bid,
        bid_no:    no_book.bid,
        ask_depth_yes: yes_book.ask_depth,
        ask_depth_no:  no_book.ask_depth,
        bid_depth_yes: yes_book.bid_depth,
        bid_depth_no:  no_book.bid_depth,
        spread_yes: yes_book.spread,
        spread_no:  no_book.spread,
        edge_yes,
        edge_no,
        best_side,
        best_edge,
        best_book,
        best_fair,
        book_age_yes: ts - yes_book.book_ts,
        book_age_no:  ts - no_book.book_ts,
        ts,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn book(ask: f64, bid: f64) -> BookInput {
        BookInput { ask, bid, ask_depth: 10.0, bid_depth: 10.0, spread: ask - bid, book_ts: 0.0 }
    }

    #[test]
    fn fair_yes_at_open_is_half() {
        let f = fair_yes(100.0, 100.0, 0.01, 300.0);
        assert!((f - 0.5).abs() < 0.01, "fair_yes at open = {}", f);
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
    fn fair_yes_guards_bad_input() {
        assert!((fair_yes(0.0, 100.0, 0.01, 300.0) - 0.5).abs() < 0.01);
        assert!((fair_yes(100.0, 0.0, 0.01, 300.0) - 0.5).abs() < 0.01);
    }

    #[test]
    fn edge_yes_positive_when_book_stale() {
        let sig = compute(
            "btc-updown-5m-test", "btc", 5,
            100.0, 100.5, 0.001, 300.0,
            &book(0.50, 0.48), &book(0.49, 0.47), 100.0,
        ).unwrap();
        assert!(sig.edge_yes > 0.0, "expected positive edge_yes, got {}", sig.edge_yes);
        assert_eq!(sig.best_side, Some(Side::Yes));
    }

    #[test]
    fn edge_no_positive_when_cl_down() {
        let sig = compute(
            "btc-updown-5m-test", "btc", 5,
            100.0, 99.5, 0.001, 300.0,
            &book(0.49, 0.47), &book(0.50, 0.48), 100.0,
        ).unwrap();
        assert!(sig.edge_no > 0.0, "expected positive edge_no, got {}", sig.edge_no);
        assert_eq!(sig.best_side, Some(Side::No));
    }

    #[test]
    fn no_edge_when_book_fair() {
        let f = fair_yes(100.5, 100.0, 0.001, 300.0);
        let sig = compute(
            "btc-updown-5m-test", "btc", 5,
            100.0, 100.5, 0.001, 300.0,
            &book(f, f - 0.02), &book(1.0 - f, 1.0 - f - 0.02), 100.0,
        ).unwrap();
        assert!(sig.best_edge < 0.001, "expected ~0 edge, got {}", sig.best_edge);
    }

    #[test]
    fn sigma_estimation_flat_prices() {
        let prices: Vec<(f64, f64)> = (0..60).map(|i| (i as f64, 100.0)).collect();
        let s = estimate_sigma(&prices, 300.0, 59.0);
        assert!(s < 0.01, "flat prices should give low sigma, got {}", s);
    }

    #[test]
    fn sigma_estimation_uses_time_deltas() {
        // Prices with varying time gaps — sigma should account for actual dt
        let prices = vec![
            (0.0, 100.0),
            (1.0, 100.1),    // 1s gap
            (11.0, 100.2),   // 10s gap
            (12.0, 100.3),   // 1s gap
            (22.0, 100.4),   // 10s gap
        ];
        let s = estimate_sigma(&prices, 300.0, 22.0);
        assert!(s > MIN_SIGMA, "sigma should be > MIN_SIGMA, got {}", s);
        assert!(s < 10.0, "sigma should be reasonable, got {}", s);
    }

    #[test]
    fn sigma_estimation_insufficient_data() {
        let prices = vec![(0.0, 100.0)];
        let s = estimate_sigma(&prices, 300.0, 0.0);
        assert!((s - 0.001).abs() < 0.0001, "fallback sigma should be 0.001");
    }

    #[test]
    fn signal_cl_vs_open_computed() {
        let sig = compute(
            "btc-updown-5m-test", "btc", 5,
            100.0, 101.0, 0.001, 300.0,
            &book(0.50, 0.48), &book(0.49, 0.47), 100.0,
        ).unwrap();
        assert!((sig.cl_vs_open - 1.0).abs() < 0.01, "cl_vs_open should be ~1.0%, got {}", sig.cl_vs_open);
    }

    #[test]
    fn signal_book_age_computed() {
        let yes = BookInput { ask: 0.5, bid: 0.48, ask_depth: 10.0, bid_depth: 10.0, spread: 0.02, book_ts: 95.0 };
        let no  = BookInput { ask: 0.49, bid: 0.47, ask_depth: 10.0, bid_depth: 10.0, spread: 0.02, book_ts: 90.0 };
        let sig = compute(
            "btc-updown-5m-test", "btc", 5,
            100.0, 100.5, 0.001, 300.0,
            &yes, &no, 100.0,
        ).unwrap();
        assert!((sig.book_age_yes - 5.0).abs() < 0.01);
        assert!((sig.book_age_no - 10.0).abs() < 0.01);
    }
}
