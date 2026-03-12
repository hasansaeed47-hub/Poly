/// signal.rs — Black-Scholes fair value + edge calculation
/// Called ONCE per tick per market. Result shared across all 5 runners.

use statrs::distribution::{ContinuousCDF, Normal};

// ── Constants ────────────────────────────────────────────────────────────────

const SECS_PER_YEAR: f64 = 365.25 * 24.0 * 3600.0;
const MIN_SIGMA:     f64 = 0.30;  // floor: 30% annualised (crypto is volatile)
const MIN_T:         f64 = 1.0 / SECS_PER_YEAR; // 1 second minimum
const MIN_SAMPLES:   usize = 30;  // need at least 30 price ticks before trusting sigma

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
#[allow(dead_code)]
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
    pub edge_yes:    f64,         // fair_yes - book_yes (using best ask)
    pub edge_no:     f64,         // fair_no  - book_no  (using best ask)
    pub best_side:   Option<Side>,// which side has edge (if any above threshold)
    pub best_edge:   f64,         // magnitude of best edge
    pub best_book:   f64,         // entry price on best side (best ask)
    pub best_fair:   f64,         // fair value on best side
    // Realistic fill data
    pub fill_yes:    Option<f64>, // VWAP fill price for YES (walk the book)
    pub fill_no:     Option<f64>, // VWAP fill price for NO
    pub depth_yes:   f64,         // total USD liquidity on YES ask side
    pub depth_no:    f64,         // total USD liquidity on NO ask side
    pub bid_yes:     f64,         // best bid for YES (exit price)
    pub bid_no:      f64,         // best bid for NO  (exit price)
    pub edge_fill_yes: f64,       // fair_yes - fill_yes (realistic edge)
    pub edge_fill_no:  f64,       // fair_no  - fill_no  (realistic edge)
    pub ts:          f64,         // unix timestamp of this signal
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
pub fn estimate_sigma(prices: &[(f64, f64)], window_secs: f64, now: f64) -> f64 {
    // Filter to window
    let cutoff = now - window_secs;
    let window: Vec<f64> = prices
        .iter()
        .filter(|(ts, _)| *ts >= cutoff)
        .map(|(_, p)| *p)
        .collect();

    if window.len() < MIN_SAMPLES {
        return 0.50; // fallback — assume ~50% annualised vol until we have enough data
    }

    // Log returns
    let returns: Vec<f64> = window
        .windows(2)
        .map(|w| (w[1] / w[0]).ln())
        .collect();

    let n    = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let var  = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0).max(1.0);
    let std  = var.sqrt();

    // Annualise: assume 1-second samples
    let annualised = std * SECS_PER_YEAR.sqrt();
    annualised.clamp(MIN_SIGMA, 5.0) // floor 30%, cap 500%
}

// ── Signal computation ────────────────────────────────────────────────────────

use crate::feeds::{PriceLevel, vwap_fill};

/// Book data passed to signal computation
#[allow(dead_code)]
pub struct BookData {
    pub best_ask:  f64,
    pub best_bid:  f64,
    pub asks:      Vec<PriceLevel>,
    pub bids:      Vec<PriceLevel>,
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
    book_yes:   &BookData,
    book_no:    &BookData,
    stake:      f64,   // USD stake for VWAP fill calculation
    ts:         f64,
) -> Option<Signal> {
    // Guard: need valid inputs
    if open_price <= 0.0 || cl_price <= 0.0 || secs_left <= 0.0 {
        return None;
    }
    if book_yes.best_ask <= 0.0 || book_no.best_ask <= 0.0 {
        return None;
    }

    let fy = fair_yes(cl_price, open_price, sigma, secs_left);
    let fn_ = 1.0 - fy;

    // Edge using best ask (optimistic)
    let edge_yes = fy  - book_yes.best_ask;
    let edge_no  = fn_ - book_no.best_ask;

    // VWAP fill prices (realistic)
    let fill_yes_data = vwap_fill(&book_yes.asks, stake);
    let fill_no_data  = vwap_fill(&book_no.asks, stake);

    let fill_yes = fill_yes_data.map(|(p, _)| p);
    let fill_no  = fill_no_data.map(|(p, _)| p);

    // Realistic edge using VWAP fill
    let edge_fill_yes = fill_yes.map(|f| fy  - f).unwrap_or(0.0);
    let edge_fill_no  = fill_no.map(|f| fn_ - f).unwrap_or(0.0);

    // Total depth (USD) available
    let depth_yes: f64 = book_yes.asks.iter().map(|l| l.price * l.size).sum();
    let depth_no:  f64 = book_no.asks.iter().map(|l| l.price * l.size).sum();

    // Determine best side using REALISTIC fill edge
    let best_side = if edge_fill_yes > edge_fill_no && edge_fill_yes > 0.0 {
        Some(Side::Yes)
    } else if edge_fill_no > edge_fill_yes && edge_fill_no > 0.0 {
        Some(Side::No)
    } else {
        None
    };

    let (best_edge, best_book, best_fair) = match best_side {
        Some(Side::Yes) => (edge_fill_yes, fill_yes.unwrap_or(book_yes.best_ask), fy),
        Some(Side::No)  => (edge_fill_no,  fill_no.unwrap_or(book_no.best_ask),  fn_),
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
        book_yes:  book_yes.best_ask,
        book_no:   book_no.best_ask,
        edge_yes,
        edge_no,
        best_side,
        best_edge,
        best_book,
        best_fair,
        fill_yes,
        fill_no,
        depth_yes,
        depth_no,
        bid_yes:   book_yes.best_bid,
        bid_no:    book_no.best_bid,
        edge_fill_yes,
        edge_fill_no,
        ts,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fair_yes_at_open_is_half() {
        // When CL == open, fair should be ~0.50
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

    use crate::feeds::PriceLevel;

    fn make_book(ask: f64, bid: f64, size: f64) -> BookData {
        BookData {
            best_ask: ask,
            best_bid: bid,
            asks: vec![PriceLevel { price: ask, size }],
            bids: vec![PriceLevel { price: bid, size }],
        }
    }

    #[test]
    fn edge_yes_positive_when_book_stale() {
        // CL up 0.5%, book still at 0.50 — YES should have edge
        let by = make_book(0.50, 0.49, 1000.0);
        let bn = make_book(0.49, 0.48, 1000.0);
        let sig = compute(
            "btc-updown-5m-test", "btc", 5,
            100.0, 100.5, 0.001, 300.0,
            &by, &bn, 5.0, 0.0,
        ).unwrap();
        assert!(sig.edge_yes > 0.0, "expected positive edge_yes, got {}", sig.edge_yes);
        assert_eq!(sig.best_side, Some(Side::Yes));
    }

    #[test]
    fn edge_no_positive_when_cl_down() {
        // CL down 0.5%, book still at 0.50 — NO should have edge
        let by = make_book(0.49, 0.48, 1000.0);
        let bn = make_book(0.50, 0.49, 1000.0);
        let sig = compute(
            "btc-updown-5m-test", "btc", 5,
            100.0, 99.5, 0.001, 300.0,
            &by, &bn, 5.0, 0.0,
        ).unwrap();
        assert!(sig.edge_no > 0.0, "expected positive edge_no, got {}", sig.edge_no);
        assert_eq!(sig.best_side, Some(Side::No));
    }

    #[test]
    fn no_edge_when_book_fair() {
        // Book perfectly priced — no edge
        let f = fair_yes(100.5, 100.0, 0.001, 300.0);
        let by = make_book(f, f - 0.01, 1000.0);
        let bn = make_book(1.0 - f, 1.0 - f - 0.01, 1000.0);
        let sig = compute(
            "btc-updown-5m-test", "btc", 5,
            100.0, 100.5, 0.001, 300.0,
            &by, &bn, 5.0, 0.0,
        ).unwrap();
        assert!(sig.best_edge < 0.001, "expected ~0 edge, got {}", sig.best_edge);
    }

    #[test]
    fn sigma_estimation_basic() {
        // Flat prices → near-zero sigma (but above MIN_SIGMA floor)
        let prices: Vec<(f64, f64)> = (0..60).map(|i| (i as f64, 100.0)).collect();
        let s = estimate_sigma(&prices, 300.0, 59.0);
        assert!(s <= MIN_SIGMA + 0.01, "flat prices should give min sigma, got {}", s);
    }
}
