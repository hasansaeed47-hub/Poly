/// signal.rs — Black-Scholes fair value + edge calculation
/// Called ONCE per tick per market. Result shared across all 5 runners.

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
    pub bid_yes:     f64,         // current PM best bid for YES (for exits)
    pub bid_no:      f64,         // current PM best bid for NO  (for exits)
    pub edge_yes:    f64,         // fair_yes - book_yes
    pub edge_no:     f64,         // fair_no  - book_no
    pub best_side:   Option<Side>,// which side has edge (if any above threshold)
    pub best_edge:   f64,         // magnitude of best edge
    pub best_book:   f64,         // entry price on best side
    pub best_fair:   f64,         // fair value on best side
    pub ts:          f64,         // unix timestamp of this signal
    // Book microstructure
    pub spread_yes:    f64,       // book_yes - bid_yes
    pub spread_no:     f64,       // book_no - bid_no
    pub depth_ask_yes: f64,       // size at best ask for YES
    pub depth_bid_yes: f64,       // size at best bid for YES
    pub depth_ask_no:  f64,       // size at best ask for NO
    pub depth_bid_no:  f64,       // size at best bid for NO
    pub book_age_yes:  f64,       // seconds since last YES book update
    pub book_age_no:   f64,       // seconds since last NO book update
    pub cl_age:        f64,       // seconds since last CL price update
}

// ── Black-Scholes binary call ─────────────────────────────────────────────────

/// Probability that price at expiry > open (YES wins)
/// Uses log-normal model: N( ln(S/K) / (sigma * sqrt(t)) )
/// No drift assumption — appropriate for short windows
pub fn fair_yes(cl: f64, open: f64, sigma: f64, secs_left: f64) -> f64 {
    let sigma = sigma.max(MIN_SIGMA);
    let t     = (secs_left / SECS_PER_YEAR).max(MIN_T);
    let d1    = (cl / open).ln() / (sigma * t.sqrt());
    let n     = Normal::new(0.0, 1.0).expect("normal distribution");
    n.cdf(d1).clamp(0.001, 0.999)
}

// ── Sigma estimation ──────────────────────────────────────────────────────────

/// Rolling annualised volatility from a price history slice.
/// prices: Vec of (unix_ts, price) sorted ascending.
/// window_secs: how far back to look.
///
/// Uses actual time deltas between samples to properly annualise,
/// since CL WS updates arrive at irregular intervals.
pub fn estimate_sigma(prices: &[(f64, f64)], window_secs: f64, now: f64) -> f64 {
    // Filter to window
    let cutoff = now - window_secs;
    let window: Vec<(f64, f64)> = prices
        .iter()
        .filter(|(ts, _)| *ts >= cutoff)
        .copied()
        .collect();

    if window.len() < 2 {
        return 0.001; // fallback
    }

    // Log returns with time deltas
    let mut sum_var = 0.0;
    let mut count = 0u32;

    for pair in window.windows(2) {
        let (t0, p0) = pair[0];
        let (t1, p1) = pair[1];
        if p0 <= 0.0 || p1 <= 0.0 {
            continue; // skip invalid prices to avoid NaN
        }
        let dt = (t1 - t0).max(0.01); // avoid division by zero
        let log_ret = (p1 / p0).ln();

        // Variance per second: (log_ret^2) / dt
        // This normalises each return to a per-second basis
        sum_var += (log_ret * log_ret) / dt;
        count += 1;
    }

    if count == 0 {
        return 0.001;
    }

    // Average variance per second, then annualise
    let var_per_sec = sum_var / count as f64;
    let annualised = var_per_sec.sqrt() * SECS_PER_YEAR.sqrt();
    annualised.max(MIN_SIGMA)
}

// ── Signal computation ────────────────────────────────────────────────────────

/// Book snapshot for one token — passed into compute
#[derive(Debug, Clone)]
pub struct BookSnap {
    pub best_ask:  f64,
    pub best_bid:  f64,
    pub ask_depth: f64,
    pub bid_depth: f64,
    pub book_age:  f64,  // seconds since last update
}

/// Compute one signal for a market.
/// Returns None if data is insufficient (no open price, no book, etc.)
pub fn compute(
    slug:       &str,
    asset:      &str,
    tf:         u32,
    open_price: f64,
    cl_price:   f64,
    sigma:      f64,
    secs_left:  f64,
    yes_book:   &BookSnap,
    no_book:    &BookSnap,
    cl_age:     f64,
    ts:         f64,
) -> Option<Signal> {
    let book_yes = yes_book.best_ask;
    let book_no  = no_book.best_ask;
    let bid_yes  = yes_book.best_bid;
    let bid_no   = no_book.best_bid;
    // Guard: need valid inputs
    if open_price <= 0.0 || cl_price <= 0.0 || secs_left <= 0.0 {
        return None;
    }
    if book_yes <= 0.0 || book_no <= 0.0 {
        return None;
    }

    let fy = fair_yes(cl_price, open_price, sigma, secs_left);
    let fn_ = 1.0 - fy;

    let edge_yes = fy  - book_yes;
    let edge_no  = fn_ - book_no;

    // Determine best side (only positive edge matters)
    // When equal, default to YES to avoid dropping the signal
    let best_side = if edge_yes >= edge_no && edge_yes > 0.0 {
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
        bid_yes,
        bid_no,
        edge_yes,
        edge_no,
        best_side,
        best_edge,
        best_book,
        best_fair,
        ts,
        spread_yes:    book_yes - bid_yes,
        spread_no:     book_no - bid_no,
        depth_ask_yes: yes_book.ask_depth,
        depth_bid_yes: yes_book.bid_depth,
        depth_ask_no:  no_book.ask_depth,
        depth_bid_no:  no_book.bid_depth,
        book_age_yes:  yes_book.book_age,
        book_age_no:   no_book.book_age,
        cl_age,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

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
        assert!(f > 0.5, "fair_yes when CL>open should be >0.5, got {}", f);
    }

    #[test]
    fn fair_yes_below_open_is_below_half() {
        let f = fair_yes(99.0, 100.0, 0.01, 300.0);
        assert!(f < 0.5, "fair_yes when CL<open should be <0.5, got {}", f);
    }

    fn snap(ask: f64, bid: f64) -> BookSnap {
        BookSnap { best_ask: ask, best_bid: bid, ask_depth: 10.0, bid_depth: 10.0, book_age: 0.5 }
    }

    #[test]
    fn edge_yes_positive_when_book_stale() {
        let sig = compute(
            "btc-updown-5m-test", "btc", 5,
            100.0, 100.5, 0.001, 300.0,
            &snap(0.50, 0.49), &snap(0.49, 0.48), 0.5, 0.0,
        ).unwrap();
        assert!(sig.edge_yes > 0.0, "expected positive edge_yes, got {}", sig.edge_yes);
        assert_eq!(sig.best_side, Some(Side::Yes));
    }

    #[test]
    fn edge_no_positive_when_cl_down() {
        let sig = compute(
            "btc-updown-5m-test", "btc", 5,
            100.0, 99.5, 0.001, 300.0,
            &snap(0.49, 0.48), &snap(0.50, 0.49), 0.5, 0.0,
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
            &snap(f, f - 0.01), &snap(1.0 - f, (1.0 - f) - 0.01), 0.5, 0.0,
        ).unwrap();
        assert!(sig.best_edge < 0.001, "expected ~0 edge, got {}", sig.best_edge);
    }

    #[test]
    fn equal_edges_picks_yes() {
        // Both sides have same positive edge — should pick YES, not None
        let sig = compute(
            "btc-updown-5m-test", "btc", 5,
            100.0, 100.0, 0.01, 300.0,
            &snap(0.48, 0.47), &snap(0.48, 0.47), 0.5, 0.0,
        ).unwrap();
        // fair_yes ≈ 0.50, edge_yes ≈ 0.02, edge_no ≈ 0.02
        assert_eq!(sig.best_side, Some(Side::Yes), "equal edges should default to YES");
    }

    #[test]
    fn sigma_estimation_with_real_deltas() {
        // 1-second spaced flat prices → near-zero sigma
        let prices: Vec<(f64, f64)> = (0..60).map(|i| (i as f64, 100.0)).collect();
        let s = estimate_sigma(&prices, 300.0, 59.0);
        assert!(s < 0.01, "flat prices should give low sigma, got {}", s);
    }

    #[test]
    fn sigma_estimation_irregular_intervals() {
        // Irregular intervals should still produce reasonable sigma
        let prices = vec![
            (0.0, 100.0), (0.5, 100.01), (3.0, 100.02),
            (3.1, 100.01), (10.0, 100.03), (15.0, 100.0),
        ];
        let s = estimate_sigma(&prices, 300.0, 15.0);
        assert!(s > MIN_SIGMA, "should get positive sigma from varied prices");
        assert!(s < 10.0, "sigma shouldn't be insanely large, got {}", s);
    }
}
