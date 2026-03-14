/// execution.rs — Order Execution, SL Validation, Settlement
///
/// Handles:
/// 1. Maker/taker entry — maker at ask-0.01 for N ticks, then taker fallback
/// 2. Stop-loss with flip confirmation — bid ≤ SL AND opposing bid ≥ 0.80
/// 3. Settlement resolution — CLOB post-settle bids (ground truth) + CL fallback
/// 4. P&L calculation with correct fee asymmetry

use crate::engine::{pm_fee, ExecConfig};
use crate::feeds::{BookState, ClSnapshots, cl_at};

use tracing::info;

// -- Position -----------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Position {
    pub engine_id:   String,
    pub slug:        String,
    pub asset:       String,
    pub dir:         String,      // "UP" or "DOWN"
    pub fill_px:     f64,         // actual fill price
    pub shares:      f64,         // STAKE / fill_px
    pub sl_px:       f64,         // fill_px * SL_PCT
    pub entry_fee:   f64,         // pm_fee(fill_px) * shares
    pub entry_ts:    f64,
    pub end_ts:      i64,         // window end timestamp
    pub tid:         String,      // token ID of our side
    pub tid_up:      String,      // UP token ID
    pub tid_dn:      String,      // DOWN token ID
    pub wmin:        u32,         // window minutes
}

// -- Trade result -------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TradeResult {
    pub engine_id:   String,
    pub slug:        String,
    pub tid:         String,      // token ID of our side (for CLOB sell)
    pub asset:       String,
    pub dir:         String,
    pub fill_px:     f64,
    pub shares:      f64,
    pub exit_px:     f64,
    pub exit_reason: String,      // WIN, LOSS, SL
    pub pnl:         f64,
    pub entry_fee:   f64,
    pub exit_fee:    f64,
    pub entry_ts:    f64,
    pub exit_ts:     f64,
    pub wmin:        u32,
}

// -- Maker/taker entry --------------------------------------------------------

/// Calculate maker price: ask - 0.01, clamped to min_entry
pub fn maker_price(best_ask: f64, min_entry: f64) -> f64 {
    let mk = ((best_ask - 0.01) * 100.0).round() / 100.0;
    mk.max(min_entry)
}

/// Determine fill price given maker phase ticks elapsed.
/// Returns Some(fill_price) when ready to fill, None if still waiting.
pub fn fill_price(
    best_ask:          f64,
    min_entry:         f64,
    max_entry:         f64,
    slip:              f64,
    maker_ticks:       u32,
    maker_chase_ticks: u32,
    secs_left:         i64,
    taker_deadline:    i64,
) -> Option<f64> {
    let mk = maker_price(best_ask, min_entry);

    let fp = if mk >= best_ask {
        // Crossing spread — instant fill at maker price
        mk
    } else if maker_ticks > maker_chase_ticks || secs_left <= (taker_deadline + 1) {
        // Chase expired or deadline approaching — taker fallback
        let tp = (best_ask + slip).min(0.99);
        if tp > max_entry { return None; }
        tp
    } else {
        // Still in maker phase — assume fill after 2+ ticks
        if maker_ticks >= 2 { mk } else { return None; }
    };

    if fp > max_entry || fp < min_entry {
        return None;
    }

    Some(fp)
}

/// Create a position from a fill.
pub fn open_position(
    engine_id: &str,
    slug:      &str,
    asset:     &str,
    dir:       &str,
    fill_px:   f64,
    exec:      &ExecConfig,
    end_ts:    i64,
    tid:       &str,
    tid_up:    &str,
    tid_dn:    &str,
    wmin:      u32,
    now:       f64,
) -> Position {
    let shares    = exec.stake / fill_px;
    let entry_fee = pm_fee(fill_px) * shares;
    let sl_px     = fill_px * exec.sl_pct;

    Position {
        engine_id: engine_id.to_string(),
        slug:      slug.to_string(),
        asset:     asset.to_string(),
        dir:       dir.to_string(),
        fill_px,
        shares,
        sl_px,
        entry_fee,
        entry_ts: now,
        end_ts,
        tid:    tid.to_string(),
        tid_up: tid_up.to_string(),
        tid_dn: tid_dn.to_string(),
        wmin,
    }
}

// -- SL validation ------------------------------------------------------------

/// Check if stop-loss should fire.
/// Returns:
///   (true,  "CONFIRMED") — real adverse move, exit now
///   (false, "THIN_BOOK") — both sides low, hold to settlement
///   (false, "ABOVE_SL")  — bid still above SL threshold
pub fn check_sl(
    our_bid:       f64,
    our_has_bid:   bool,
    sl_px:         f64,
    opp_bid:       f64,
    opp_has_bid:   bool,
    confirm_bid:   f64,
) -> (bool, &'static str) {
    if !our_has_bid || our_bid > sl_px {
        return (false, "ABOVE_SL");
    }

    // Our bid is below SL threshold — check if flip is real
    let real_flip = opp_has_bid && opp_bid >= confirm_bid;

    if real_flip {
        (true, "CONFIRMED")
    } else {
        (false, "THIN_BOOK")
    }
}

/// Execute SL exit and return the trade result.
pub fn execute_sl(pos: &Position, our_bid: f64, exec: &ExecConfig, now: f64) -> TradeResult {
    let recovery = pos.shares * (our_bid - exec.slip).max(0.0);
    let exit_fee = pm_fee(our_bid) * pos.shares;
    let pnl      = recovery - exec.stake - pos.entry_fee - exit_fee;

    TradeResult {
        engine_id:   pos.engine_id.clone(),
        slug:        pos.slug.clone(),
        tid:         pos.tid.clone(),
        asset:       pos.asset.clone(),
        dir:         pos.dir.clone(),
        fill_px:     pos.fill_px,
        shares:      pos.shares,
        exit_px:     our_bid,
        exit_reason: "SL".to_string(),
        pnl,
        entry_fee:   pos.entry_fee,
        exit_fee,
        entry_ts:    pos.entry_ts,
        exit_ts:     now,
        wmin:        pos.wmin,
    }
}

// -- Settlement ---------------------------------------------------------------

/// Resolve settlement outcome using CLOB post-settle bids + CL fallback.
///
/// CLOB bids are ground truth: whichever side has bid > 0.80 after settlement
/// is the winning side. CL price comparison is fallback only.
///
/// Returns "UP" or "DOWN", or None if indeterminate.
pub fn resolve_settlement(
    book_state:   &BookState,
    cl_snapshots: &ClSnapshots,
    cl_opens:     &std::collections::HashMap<String, f64>,
    pos:          &Position,
) -> Option<String> {
    // 1. CLOB post-settle bids (ground truth)
    let bk_up = book_state.get(&pos.tid_up);
    let bk_dn = book_state.get(&pos.tid_dn);

    let clob_dir = match (&bk_up, &bk_dn) {
        (Some(up), _) if up.best_bid > 0.80 => Some("UP"),
        (_, Some(dn)) if dn.best_bid > 0.80 => Some("DOWN"),
        _ => None,
    };

    // 2. CL fallback — compare open vs close
    let cl_open  = cl_opens.get(&pos.slug).copied().unwrap_or(0.0);
    let cl_close = cl_at(cl_snapshots, &pos.asset, pos.end_ts).unwrap_or(0.0);

    let cl_dir = if cl_open > 0.0 && cl_close > 0.0 {
        Some(if cl_close >= cl_open { "UP" } else { "DOWN" })
    } else {
        None
    };

    // Log disagreements
    if let (Some(cd), Some(cb)) = (cl_dir, clob_dir) {
        if cd != cb {
            info!("[SETTLE] CL/CLOB disagree: CL={} CLOB={} {} — using CLOB (ground truth)",
                cd, cb, pos.slug);
        }
    }

    // CLOB is ground truth; CL is fallback
    clob_dir.or(cl_dir).map(|s| s.to_string())
}

/// Execute settlement and return the trade result.
pub fn execute_settlement(pos: &Position, actual_dir: &str, exec: &ExecConfig, now: f64) -> TradeResult {
    let won = actual_dir == pos.dir;
    let pnl = if won {
        pos.shares * 1.0 - exec.stake - pos.entry_fee
    } else {
        -exec.stake - pos.entry_fee
    };

    TradeResult {
        engine_id:   pos.engine_id.clone(),
        slug:        pos.slug.clone(),
        tid:         pos.tid.clone(),
        asset:       pos.asset.clone(),
        dir:         pos.dir.clone(),
        fill_px:     pos.fill_px,
        shares:      pos.shares,
        exit_px:     if won { 1.0 } else { 0.0 },
        exit_reason: if won { "WIN".to_string() } else { "LOSS".to_string() },
        pnl,
        entry_fee:   pos.entry_fee,
        exit_fee:    0.0, // no exit fee on settlement
        entry_ts:    pos.entry_ts,
        exit_ts:     now,
        wmin:        pos.wmin,
    }
}

// -- Engine F: reprice exit ---------------------------------------------------

/// Execute a reprice exit (Engine F): taker sell at current bid.
/// Entry was maker (0 fee), exit is taker.
pub fn execute_reprice_exit(
    pos:    &Position,
    bid:    f64,
    reason: &str,
    exec:   &ExecConfig,
    now:    f64,
) -> TradeResult {
    let exit_px  = (bid - exec.slip).max(0.01);
    let exit_fee = pm_fee(exit_px) * pos.shares;
    let recovery = pos.shares * exit_px;
    let pnl      = recovery - exec.stake - pos.entry_fee - exit_fee;

    TradeResult {
        engine_id:   pos.engine_id.clone(),
        slug:        pos.slug.clone(),
        tid:         pos.tid.clone(),
        asset:       pos.asset.clone(),
        dir:         pos.dir.clone(),
        fill_px:     pos.fill_px,
        shares:      pos.shares,
        exit_px,
        exit_reason: reason.to_string(),
        pnl,
        entry_fee:   pos.entry_fee,
        exit_fee,
        entry_ts:    pos.entry_ts,
        exit_ts:     now,
        wmin:        pos.wmin,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maker_price_clamps() {
        assert_eq!(maker_price(0.93, 0.88), 0.92);
        assert_eq!(maker_price(0.89, 0.88), 0.88);
        assert_eq!(maker_price(0.88, 0.88), 0.88); // at boundary
    }

    #[test]
    fn fill_price_maker_phase() {
        // Tick 1: too early, no fill
        assert!(fill_price(0.93, 0.88, 0.98, 0.005, 1, 4, 55, 44).is_none());
        // Tick 2: assumed maker fill
        let fp = fill_price(0.93, 0.88, 0.98, 0.005, 2, 4, 55, 44);
        assert_eq!(fp, Some(0.92));
    }

    #[test]
    fn fill_price_taker_fallback() {
        // Past maker chase — taker
        let fp = fill_price(0.93, 0.88, 0.98, 0.005, 5, 4, 50, 44);
        assert_eq!(fp, Some(0.935));
    }

    #[test]
    fn fill_price_rejects_out_of_range() {
        // Ask too high
        assert!(fill_price(0.99, 0.88, 0.98, 0.005, 5, 4, 50, 44).is_none());
    }

    #[test]
    fn sl_check_above_threshold() {
        let (fire, reason) = check_sl(0.55, true, 0.45, 0.20, true, 0.80);
        assert!(!fire);
        assert_eq!(reason, "ABOVE_SL");
    }

    #[test]
    fn sl_check_thin_book() {
        let (fire, reason) = check_sl(0.40, true, 0.45, 0.30, true, 0.80);
        assert!(!fire);
        assert_eq!(reason, "THIN_BOOK");
    }

    #[test]
    fn sl_check_confirmed() {
        let (fire, reason) = check_sl(0.40, true, 0.45, 0.85, true, 0.80);
        assert!(fire);
        assert_eq!(reason, "CONFIRMED");
    }

    #[test]
    fn settlement_win_pnl() {
        let exec = crate::engine::default_exec();
        let pos = Position {
            engine_id: "A".into(), slug: "test".into(), asset: "btc".into(),
            dir: "UP".into(), fill_px: 0.90, shares: 5.0 / 0.90,
            sl_px: 0.45, entry_fee: pm_fee(0.90) * (5.0 / 0.90),
            entry_ts: 100.0, end_ts: 400, tid: "t1".into(),
            tid_up: "t1".into(), tid_dn: "t2".into(), wmin: 5,
        };
        let result = execute_settlement(&pos, "UP", &exec, 408.0);
        assert_eq!(result.exit_reason, "WIN");
        assert!(result.pnl > 0.0);
        assert_eq!(result.exit_fee, 0.0); // no exit fee on settlement
    }

    #[test]
    fn settlement_loss_pnl() {
        let exec = crate::engine::default_exec();
        let pos = Position {
            engine_id: "A".into(), slug: "test".into(), asset: "btc".into(),
            dir: "UP".into(), fill_px: 0.90, shares: 5.0 / 0.90,
            sl_px: 0.45, entry_fee: pm_fee(0.90) * (5.0 / 0.90),
            entry_ts: 100.0, end_ts: 400, tid: "t1".into(),
            tid_up: "t1".into(), tid_dn: "t2".into(), wmin: 5,
        };
        let result = execute_settlement(&pos, "DOWN", &exec, 408.0);
        assert_eq!(result.exit_reason, "LOSS");
        assert!(result.pnl < 0.0);
    }
}
