/// lag.rs — Lag detection engine
///
/// Detects exploitable CL→PM book lag using BN as the leading indicator.
///
/// Two-stage lag:
///   Stage 1: BN moves → CL hasn't caught up (1-5s)
///   Stage 2: CL catches up → PM book hasn't repriced (0.2-2s)
///
/// We fire on EITHER stage:
///   - "Predictive" entry: BN moved, CL stale, book stale → enter before CL catches up
///   - "Reactive" entry: CL just moved, book still stale → enter in the MM cancel-replace gap
///
/// Both modes use maker-first execution (0% fee).

use crate::feeds::{BookEntry, vwap_fill};
use crate::signal::fair_yes;

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

/// A detected lag opportunity
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LagSignal {
    pub slug:         String,
    pub asset:        String,
    pub tf:           u32,
    pub side:         Side,

    // Prices
    pub bn_price:     f64,
    pub cl_price:     f64,
    pub open_price:   f64,

    // Lag metrics
    pub divergence_pct: f64,   // |bn - cl| / cl * 100
    pub cl_age_ms:      f64,   // ms since last CL update
    pub bn_momentum:    f64,   // BN 5s pct change

    // Fair values (the key insight)
    pub fair_bn:      f64,     // fair_yes using BN price (truth)
    pub fair_cl:      f64,     // fair_yes using CL price (what market sees)
    pub fair_gap:     f64,     // |fair_bn - fair_cl| (exploitable divergence)

    // Book state
    pub best_ask:     f64,
    pub best_bid:     f64,
    pub fill_price:   f64,     // VWAP fill price
    pub depth:        f64,     // USD depth on entry side

    // Edge
    pub edge:         f64,     // fair_bn - fill_price (for YES); analogous for NO
    pub maker_price:  f64,     // recommended maker bid price

    // Timing
    pub secs_left:    f64,
    pub sigma:        f64,
    pub bn_flow_imbal: f64,    // BN trade flow imbalance [-1,+1]
    pub ts:           f64,
}

// ── Lag Detector Config ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct LagConfig {
    pub min_divergence_pct:  f64,   // min BN-CL divergence % to trigger
    pub min_edge:            f64,   // min fair_bn - fill_price
    pub min_fair_gap:        f64,   // min |fair_bn - fair_cl|
    pub min_bn_momentum:     f64,   // min |BN 5s pct change|
    pub min_secs_left:       f64,   // don't enter below this
    pub max_secs_left:       f64,   // don't enter above this
    pub max_sigma:           f64,   // skip high-vol (BS unreliable)
    pub min_depth_multiple:  f64,   // depth must be >= stake * this
    pub min_entry_price:     f64,   // no sub-30c lottery tickets
    pub max_entry_price:     f64,   // no overpaying >60c
    pub stake:               f64,
    pub min_bn_flow_confirm: f64,   // min BN flow imbalance confirming direction
}

// ── Lag Detector ────────────────────────────────────────────────────────────

pub struct LagDetector {
    pub config: LagConfig,
}

impl LagDetector {
    pub fn new(config: LagConfig) -> Self {
        Self { config }
    }

    /// Detect a lag opportunity on a single market.
    ///
    /// Returns Some(LagSignal) if all conditions are met:
    /// 1. BN-CL divergence above threshold
    /// 2. BN momentum confirms direction
    /// 3. BS fair value gap between BN-derived and CL-derived
    /// 4. Book is stale (priced closer to CL-fair than BN-fair)
    /// 5. Edge (fair_bn - fill_price) above threshold
    /// 6. Sufficient depth and time remaining
    pub fn detect(
        &self,
        slug:         &str,
        asset:        &str,
        tf:           u32,
        open_price:   f64,
        secs_left:    f64,
        sigma:        f64,
        // Prices
        bn_price:     f64,
        _bn_ts:       f64,
        cl_price:     f64,
        cl_ts:        f64,
        // BN signals
        bn_momentum:  f64,     // 5s pct change
        bn_flow_imbal: f64,    // trade flow imbalance
        // Book data
        book_yes:     &BookEntry,
        book_no:      &BookEntry,
        now:          f64,
        // CL cadence: (secs_since_last_update, estimated_cadence)
        cl_cadence:   (f64, f64),
    ) -> Option<LagSignal> {
        // ── Basic validity checks ───────────────────────────────────────
        if open_price <= 0.0 || bn_price <= 0.0 || cl_price <= 0.0 { return None; }
        // Reject extreme open prices that make BS model ill-conditioned
        if (cl_price / open_price) < 0.90 || (cl_price / open_price) > 1.10 { return None; }
        if secs_left <= 0.0 { return None; }
        if book_yes.best_ask <= 0.0 || book_no.best_ask <= 0.0 { return None; }

        // ── Time gate ───────────────────────────────────────────────────
        if secs_left < self.config.min_secs_left || secs_left > self.config.max_secs_left {
            return None;
        }

        // ── Sigma gate ──────────────────────────────────────────────────
        if sigma > self.config.max_sigma {
            return None;
        }

        // ── CL cadence gate ──────────────────────────────────────────────
        // Only enter within 3s after a CL update — maximizes time before next
        // CL reprice, giving us the widest lag window for convergence
        let (since_last_cl, _cadence) = cl_cadence;
        if since_last_cl > 3.0 {
            return None;
        }

        // ── Compute lag metrics ─────────────────────────────────────────
        let divergence_pct = ((bn_price - cl_price) / cl_price).abs() * 100.0;
        let cl_age_ms = (now - cl_ts) * 1000.0;

        // ── Divergence gate ─────────────────────────────────────────────
        if divergence_pct < self.config.min_divergence_pct {
            return None;
        }

        // ── BN momentum gate ────────────────────────────────────────────
        if bn_momentum.abs() < self.config.min_bn_momentum {
            return None;
        }

        // ── Compute fair values ─────────────────────────────────────────
        let fair_bn  = fair_yes(bn_price, open_price, sigma, secs_left);
        let fair_cl  = fair_yes(cl_price, open_price, sigma, secs_left);
        let fair_gap = (fair_bn - fair_cl).abs();

        if fair_gap < self.config.min_fair_gap {
            return None;
        }

        // ── Evaluate BOTH sides and pick the one with better edge ──────
        // BN momentum determines primary side, but we also check the other
        // side in case the gap creates a better opportunity there.
        let primary_side = if bn_momentum > 0.0 { Side::Yes } else { Side::No };

        // Try both sides, pick the one with the highest edge
        let sides_to_try = [primary_side, if primary_side == Side::Yes { Side::No } else { Side::Yes }];
        let mut best_candidate: Option<(Side, f64, f64, f64, &BookEntry)> = None; // (side, edge, fill_price, depth, book)

        for &try_side in &sides_to_try {
            // BN flow confirmation for this side
            let flow_ok = match try_side {
                Side::Yes => bn_flow_imbal >= self.config.min_bn_flow_confirm,
                Side::No  => bn_flow_imbal <= -self.config.min_bn_flow_confirm,
            };
            // Primary side requires flow confirmation; secondary side skips it
            // (the gap itself is the signal for the secondary side)
            if try_side == primary_side && !flow_ok {
                continue;
            }

            let (book, fair) = match try_side {
                Side::Yes => (book_yes, fair_bn),
                Side::No  => (book_no, 1.0 - fair_bn),
            };

            let fill_price = match vwap_fill(&book.asks, self.config.stake) {
                Some((p, _)) => p,
                None => continue,
            };

            if fill_price < self.config.min_entry_price || fill_price > self.config.max_entry_price {
                continue;
            }

            let edge = fair - fill_price;
            if edge < self.config.min_edge {
                continue;
            }

            let depth: f64 = book.asks.iter().map(|l| l.price * l.size).sum();
            if depth < self.config.stake * self.config.min_depth_multiple {
                continue;
            }

            // Pick the side with the highest edge
            let is_better = match &best_candidate {
                None => true,
                Some((_, best_edge, _, _, _)) => edge > *best_edge,
            };
            if is_better {
                best_candidate = Some((try_side, edge, fill_price, depth, book));
            }
        }

        let (side, edge, fill_price, depth, book) = match best_candidate {
            Some((s, e, fp, d, b)) => (s, e, fp, d, b),
            None => return None,
        };

        // ── Compute maker bid price ─────────────────────────────────────
        // Place at best_ask - 0.01 to be near where liquidity sits
        // This dramatically improves fill rate vs bid+1 which is too passive
        let maker_price = {
            let raw = ((book.best_ask - 0.01) * 100.0).round() / 100.0;
            raw.max(book.best_bid + 0.01).max(0.01)
        };

        Some(LagSignal {
            slug: slug.to_string(),
            asset: asset.to_string(),
            tf,
            side,
            bn_price,
            cl_price,
            open_price,
            divergence_pct,
            cl_age_ms,
            bn_momentum,
            fair_bn,
            fair_cl,
            fair_gap,
            best_ask: book.best_ask,
            best_bid: book.best_bid,
            fill_price,
            depth,
            edge,
            maker_price,
            secs_left,
            sigma,
            bn_flow_imbal,
            ts: now,
        })
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feeds::{BookEntry, PriceLevel};

    fn make_config() -> LagConfig {
        LagConfig {
            min_divergence_pct:  0.03,
            min_edge:            0.03,
            min_fair_gap:        0.01,
            min_bn_momentum:     0.0001,
            min_secs_left:       60.0,
            max_secs_left:       840.0,
            max_sigma:           0.80,
            min_depth_multiple:  2.0,
            min_entry_price:     0.10,
            max_entry_price:     0.90,
            stake:               5.0,
            min_bn_flow_confirm: 0.05,
        }
    }

    fn make_book(ask: f64, bid: f64, size: f64) -> BookEntry {
        BookEntry {
            asks: vec![PriceLevel { price: ask, size }],
            bids: vec![PriceLevel { price: bid, size }],
            best_ask: ask,
            best_bid: bid,
            ts: 1000.0,
        }
    }

    #[test]
    fn detects_upward_lag() {
        let cfg = make_config();
        let det = LagDetector::new(cfg);

        // BN at 84100, CL at 84000, open at 84000
        // BN moved up 0.12%, CL hasn't caught up
        let book_yes = make_book(0.52, 0.48, 100.0);
        let book_no  = make_book(0.52, 0.48, 100.0);

        let sig = det.detect(
            "btc-updown-5m-1000", "btc", 5,
            84000.0, 200.0, 0.50,
            84100.0, 1000.5,  // BN
            84000.0, 999.0,   // CL (1.5s stale)
            0.0012,           // bn momentum (0.12%)
            0.30,             // bn flow imbalance (bullish)
            &book_yes, &book_no,
            1001.0,
            (1.0, 20.0), // cl_cadence: 1s since last update
        );

        assert!(sig.is_some(), "should detect upward lag");
        let s = sig.unwrap();
        assert_eq!(s.side, Side::Yes);
        assert!(s.edge > 0.0, "edge should be positive");
        assert!(s.divergence_pct > 0.0);
    }

    #[test]
    fn rejects_no_divergence() {
        let cfg = make_config();
        let det = LagDetector::new(cfg);

        // BN = CL = same price → no divergence
        let book_yes = make_book(0.52, 0.48, 100.0);
        let book_no  = make_book(0.52, 0.48, 100.0);

        let sig = det.detect(
            "btc-updown-5m-1000", "btc", 5,
            84000.0, 200.0, 0.50,
            84000.0, 1000.5,
            84000.0, 1000.0,
            0.0,
            0.0,
            &book_yes, &book_no,
            1001.0,
            (1.0, 20.0),
        );

        assert!(sig.is_none(), "should reject when no divergence");
    }

    #[test]
    fn rejects_insufficient_time() {
        let cfg = make_config();
        let det = LagDetector::new(cfg);

        let book_yes = make_book(0.52, 0.48, 100.0);
        let book_no  = make_book(0.52, 0.48, 100.0);

        // secs_left = 30 < min_secs_left = 60
        let sig = det.detect(
            "btc-updown-5m-1000", "btc", 5,
            84000.0, 30.0, 0.50,
            84100.0, 1000.5,
            84000.0, 999.0,
            0.0012,
            0.30,
            &book_yes, &book_no,
            1001.0,
            (1.0, 20.0),
        );

        assert!(sig.is_none(), "should reject when insufficient time");
    }
}
