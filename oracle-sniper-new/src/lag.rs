/// lag.rs — CL-leading lag detection
///
/// PM settles on Chainlink. When CL updates, PM book MMs are slow to
/// reprice (200ms-2s). We detect the CL move, confirm with BN momentum
/// + flow, and buy the stale book before MMs catch up.
///
/// Signal:  CL moved → fair_cl changed → PM book still priced at old fair
/// Confirm: BN momentum agrees with CL direction
/// Edge:    fair_cl - fill_price (what PM settles on minus what we pay)

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
    pub cl_price:     f64,
    pub bn_price:     f64,
    pub open_price:   f64,

    // Lag metrics
    pub cl_move_pct:    f64,   // |CL 5s pct change| — the trigger
    pub cl_age_ms:      f64,   // ms since last CL update
    pub bn_momentum:    f64,   // BN 5s pct change (confirmation)

    // Fair values
    pub fair_cl:      f64,     // fair_yes using CL price (TRUTH)
    pub book_gap:     f64,     // fair_cl - best_ask (book staleness)

    // Book state
    pub best_ask:     f64,
    pub best_bid:     f64,
    pub fill_price:   f64,     // VWAP fill price
    pub depth:        f64,     // USD depth on entry side

    // Edge
    pub edge:         f64,     // fair_cl - fill_price
    pub maker_price:  f64,     // recommended maker bid price

    // Timing
    pub secs_left:    f64,
    pub sigma:        f64,
    pub bn_flow_imbal: f64,
    pub ts:           f64,
}

// ── Config ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct LagConfig {
    pub min_cl_move_pct:     f64,   // min |CL 5s pct change| to trigger
    pub min_edge:            f64,   // min fair_cl - fill_price
    pub min_book_gap:        f64,   // min fair_cl - best_ask
    pub min_bn_momentum:     f64,   // min |BN 5s pct change| (confirmation)
    pub min_secs_left:       f64,
    pub max_secs_left:       f64,
    pub max_sigma:           f64,
    pub min_depth_multiple:  f64,
    pub min_entry_price:     f64,   // 0.40 — market must agree
    pub max_entry_price:     f64,   // 0.88
    pub stake:               f64,
    pub min_bn_flow_confirm: f64,
}

// ── Detector ────────────────────────────────────────────────────────────────

pub struct LagDetector {
    pub config: LagConfig,
}

impl LagDetector {
    pub fn new(config: LagConfig) -> Self {
        Self { config }
    }

    /// Detect a CL→PM lag opportunity.
    ///
    /// CL is truth. Signal fires when:
    /// 1. CL just moved (cl_momentum above threshold)
    /// 2. PM book is stale (fair_cl vs book price gap)
    /// 3. BN momentum confirms direction
    /// 4. BN trade flow confirms direction
    /// 5. Edge (fair_cl - fill_price) above threshold
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
        cl_price:     f64,
        cl_ts:        f64,
        bn_price:     f64,
        // Momentum
        cl_momentum:  f64,     // CL 5s pct change (PRIMARY)
        bn_momentum:  f64,     // BN 5s pct change (confirmation)
        bn_flow_imbal: f64,    // BN trade flow imbalance
        // Book data
        book_yes:     &BookEntry,
        book_no:      &BookEntry,
        now:          f64,
        // CL cadence
        cl_cadence:   (f64, f64),
    ) -> Option<LagSignal> {
        // ── Basic validity ──────────────────────────────────────────────
        if open_price <= 0.0 || cl_price <= 0.0 { return None; }
        if (cl_price / open_price) < 0.90 || (cl_price / open_price) > 1.10 { return None; }
        if secs_left <= 0.0 { return None; }
        if book_yes.best_ask <= 0.0 || book_no.best_ask <= 0.0 { return None; }

        // ── Time gate ───────────────────────────────────────────────────
        if secs_left < self.config.min_secs_left || secs_left > self.config.max_secs_left {
            return None;
        }

        // ── Sigma gate ──────────────────────────────────────────────────
        if sigma > self.config.max_sigma { return None; }

        // ── CL cadence gate ─────────────────────────────────────────────
        // Only enter within 3s after CL update — fresh data, max lag window
        let (since_last_cl, _) = cl_cadence;
        if since_last_cl > 3.0 { return None; }

        let cl_age_ms = (now - cl_ts) * 1000.0;

        // ── CL momentum gate (PRIMARY TRIGGER) ─────────────────────────
        let cl_move_pct = cl_momentum.abs() * 100.0;
        if cl_move_pct < self.config.min_cl_move_pct { return None; }

        // ── BN momentum confirmation ────────────────────────────────────
        if bn_momentum.abs() < self.config.min_bn_momentum { return None; }
        // BN and CL must agree on direction
        if (cl_momentum > 0.0) != (bn_momentum > 0.0) { return None; }

        // ── Compute fair value (CL is truth) ────────────────────────────
        let fair_cl = fair_yes(cl_price, open_price, sigma, secs_left);

        // ── Side from CL direction ──────────────────────────────────────
        let side = if cl_momentum > 0.0 { Side::Yes } else { Side::No };

        // ── BN flow confirmation ────────────────────────────────────────
        let flow_ok = match side {
            Side::Yes => bn_flow_imbal >= self.config.min_bn_flow_confirm,
            Side::No  => bn_flow_imbal <= -self.config.min_bn_flow_confirm,
        };
        if !flow_ok { return None; }

        // ── Book data for our side ──────────────────────────────────────
        let (book, fair) = match side {
            Side::Yes => (book_yes, fair_cl),
            Side::No  => (book_no, 1.0 - fair_cl),
        };

        // ── Book staleness ──────────────────────────────────────────────
        let book_gap = fair - book.best_ask;
        if book_gap < self.config.min_book_gap { return None; }

        // ── VWAP fill price ─────────────────────────────────────────────
        let fill_price = match vwap_fill(&book.asks, self.config.stake) {
            Some((p, _)) => p,
            None => return None,
        };

        // ── Entry price gates ───────────────────────────────────────────
        // Min 0.40: market must partially agree with CL direction
        // Max 0.88: need room for 10c TP
        if fill_price < self.config.min_entry_price || fill_price > self.config.max_entry_price {
            return None;
        }

        // ── Edge ────────────────────────────────────────────────────────
        let edge = fair - fill_price;
        if edge < self.config.min_edge { return None; }

        // ── Depth gate ──────────────────────────────────────────────────
        let depth: f64 = book.asks.iter().map(|l| l.price * l.size).sum();
        if depth < self.config.stake * self.config.min_depth_multiple { return None; }

        // ── Maker price ─────────────────────────────────────────────────
        let maker_price = {
            let raw = ((book.best_ask - 0.01) * 100.0).round() / 100.0;
            raw.max(book.best_bid + 0.01).max(0.01)
        };

        Some(LagSignal {
            slug: slug.to_string(),
            asset: asset.to_string(),
            tf,
            side,
            cl_price,
            bn_price,
            open_price,
            cl_move_pct,
            cl_age_ms,
            bn_momentum,
            fair_cl,
            book_gap,
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
            min_cl_move_pct:     0.03,
            min_edge:            0.03,
            min_book_gap:        0.01,
            min_bn_momentum:     0.0001,
            min_secs_left:       60.0,
            max_secs_left:       840.0,
            max_sigma:           0.80,
            min_depth_multiple:  2.0,
            min_entry_price:     0.40,
            max_entry_price:     0.88,
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

        let book_yes = make_book(0.52, 0.48, 100.0);
        let book_no  = make_book(0.52, 0.48, 100.0);

        let sig = det.detect(
            "btc-updown-5m-1000", "btc", 5,
            84000.0, 200.0, 0.50,
            84100.0, 1000.5,
            84100.0,
            0.0012, 0.0012, 0.30,
            &book_yes, &book_no,
            1001.0,
            (1.0, 20.0),
        );

        assert!(sig.is_some(), "should detect upward lag");
        let s = sig.unwrap();
        assert_eq!(s.side, Side::Yes);
        assert!(s.edge > 0.0);
    }

    #[test]
    fn rejects_no_cl_move() {
        let cfg = make_config();
        let det = LagDetector::new(cfg);

        let book_yes = make_book(0.52, 0.48, 100.0);
        let book_no  = make_book(0.52, 0.48, 100.0);

        let sig = det.detect(
            "btc-updown-5m-1000", "btc", 5,
            84000.0, 200.0, 0.50,
            84000.0, 1000.5,
            84000.0,
            0.0, 0.0, 0.0,
            &book_yes, &book_no,
            1001.0,
            (1.0, 20.0),
        );

        assert!(sig.is_none(), "should reject when CL hasn't moved");
    }

    #[test]
    fn rejects_cheap_entry() {
        let cfg = make_config();
        let det = LagDetector::new(cfg);

        // Book ask at 0.30 — below min_entry_price 0.40
        // Market disagrees with CL direction → skip
        let book_yes = make_book(0.30, 0.26, 100.0);
        let book_no  = make_book(0.52, 0.48, 100.0);

        let sig = det.detect(
            "btc-updown-5m-1000", "btc", 5,
            84000.0, 200.0, 0.50,
            84100.0, 1000.5,
            84100.0,
            0.0012, 0.0012, 0.30,
            &book_yes, &book_no,
            1001.0,
            (1.0, 20.0),
        );

        assert!(sig.is_none(), "should reject when market disagrees (cheap entry)");
    }

    #[test]
    fn rejects_bn_disagrees() {
        let cfg = make_config();
        let det = LagDetector::new(cfg);

        let book_yes = make_book(0.52, 0.48, 100.0);
        let book_no  = make_book(0.52, 0.48, 100.0);

        // CL up, BN down → disagree → reject
        let sig = det.detect(
            "btc-updown-5m-1000", "btc", 5,
            84000.0, 200.0, 0.50,
            84100.0, 1000.5,
            83900.0,
            0.0012,   // CL up
            -0.0012,  // BN down (disagrees)
            -0.30,    // flow bearish
            &book_yes, &book_no,
            1001.0,
            (1.0, 20.0),
        );

        assert!(sig.is_none(), "should reject when BN disagrees with CL");
    }
}
