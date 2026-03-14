/// signal.rs — Black-Scholes fair value + edge calculation
/// Called ONCE per tick per market. Result shared across runner.

use statrs::distribution::{ContinuousCDF, Normal};

const SECS_PER_YEAR: f64 = 365.25 * 24.0 * 3600.0;
const MIN_SIGMA:     f64 = 0.30;
const MIN_T:         f64 = 1.0 / SECS_PER_YEAR;
const MIN_SAMPLES:   usize = 30;

// ── Types ───────────────────────────────────────────────────────────────────

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

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Signal {
    pub slug:        String,
    pub asset:       String,
    pub tf:          u32,
    pub open_price:  f64,
    pub cl_price:    f64,
    pub sigma:       f64,
    pub secs_left:   f64,
    pub fair_yes:    f64,
    pub fair_no:     f64,
    pub book_yes:    f64,
    pub book_no:     f64,
    pub best_side:   Option<Side>,
    pub best_edge:   f64,
    pub best_book:   f64,
    pub best_fair:   f64,
    pub fill_yes:    Option<f64>,
    pub fill_no:     Option<f64>,
    pub depth_yes:   f64,
    pub depth_no:    f64,
    pub bid_yes:     f64,
    pub bid_no:      f64,
    pub edge_fill_yes: f64,
    pub edge_fill_no:  f64,
    pub cl_momentum:   f64,   // CL price change over last 30s (positive = UP)
    pub book_imbal:    f64,   // bid_depth / ask_depth ratio (>1 = bullish)
    pub ts:          f64,
}

// ── Black-Scholes binary call ───────────────────────────────────────────────

pub fn fair_yes(cl: f64, open: f64, sigma: f64, secs_left: f64) -> f64 {
    let sigma = sigma.max(MIN_SIGMA);
    let t     = (secs_left / SECS_PER_YEAR).max(MIN_T);
    let d1    = (cl / open).ln() / (sigma * t.sqrt());
    let n     = Normal::new(0.0, 1.0).expect("normal distribution");
    n.cdf(d1).clamp(0.001, 0.999)
}

// ── Sigma estimation ────────────────────────────────────────────────────────

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

// ── Signal computation ──────────────────────────────────────────────────────

use crate::feeds::{PriceLevel, vwap_fill};

#[allow(dead_code)]
pub struct BookData {
    pub best_ask: f64,
    pub best_bid: f64,
    pub asks:     Vec<PriceLevel>,
    pub bids:     Vec<PriceLevel>,
}

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
    stake:      f64,
    cl_momentum: f64,
    ts:         f64,
) -> Option<Signal> {
    if open_price <= 0.0 || cl_price <= 0.0 || secs_left <= 0.0 { return None; }
    if book_yes.best_ask <= 0.0 || book_no.best_ask <= 0.0 { return None; }

    let fy  = fair_yes(cl_price, open_price, sigma, secs_left);
    let fn_ = 1.0 - fy;

    let fill_yes_data = vwap_fill(&book_yes.asks, stake);
    let fill_no_data  = vwap_fill(&book_no.asks, stake);

    let fill_yes = fill_yes_data.map(|(p, _)| p);
    let fill_no  = fill_no_data.map(|(p, _)| p);

    let edge_fill_yes = fill_yes.map(|f| fy  - f).unwrap_or(0.0);
    let edge_fill_no  = fill_no.map(|f| fn_ - f).unwrap_or(0.0);

    let depth_yes: f64 = book_yes.asks.iter().map(|l| l.price * l.size).sum();
    let depth_no:  f64 = book_no.asks.iter().map(|l| l.price * l.size).sum();

    // Book imbalance: YES bid depth / YES ask depth (>1 = market is bullish)
    let bid_depth_yes: f64 = book_yes.bids.iter().map(|l| l.price * l.size).sum();
    let ask_depth_yes: f64 = depth_yes.max(0.01);
    let book_imbal = bid_depth_yes / ask_depth_yes;

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
        slug: slug.to_string(), asset: asset.to_string(), tf,
        open_price, cl_price, sigma, secs_left,
        fair_yes: fy, fair_no: fn_,
        book_yes: book_yes.best_ask, book_no: book_no.best_ask,
        best_side, best_edge, best_book, best_fair,
        fill_yes, fill_no,
        depth_yes, depth_no,
        bid_yes: book_yes.best_bid, bid_no: book_no.best_bid,
        edge_fill_yes, edge_fill_no,
        cl_momentum,
        book_imbal,
        ts,
    })
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
}
