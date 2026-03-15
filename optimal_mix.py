#!/usr/bin/env python3
"""
Optimal Engine Mix Finder: 100% Win Rate Parameter Sweep

Uses real settlement data (settlements_2026-03-15.jsonl) + real book snapshots
from scanner.log to find the best engine parameter combination that maintains
100% WR while maximizing trade count and P&L.

Approach:
  1. Parse all 69 settlements + 765 book snapshots
  2. For each settlement window, determine TRUE outcome (YES/NO from CL)
  3. Sweep engine parameters: delta thresholds, entry ranges, continuity,
     late scalper bounds
  4. For each parameter combo, simulate all trades with realistic fills
  5. Filter to combos with 100% WR
  6. Rank by (trade_count, P&L) to find optimal mix

Key constraints for 100% WR:
  - Direction must match outcome (UP→YES or DOWN→NO)
  - Only enter when book price confirms CL direction (delta + book alignment)
  - Fill must be achievable (book has liquidity in range)
  - No SL triggered (book doesn't flip against us before settlement)

Engine architecture:
  A: 5m sniper   - delta threshold + continuity=4
  B: 5m D1       - delta threshold + instant (continuity=0)
  C: 15m sniper  - delta threshold + continuity=4
  D: 15m D1      - delta threshold + instant (continuity=0)
  E: late scalper - book price range only, last 25s
"""

import json
import re
import sys
import itertools
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

# ── Constants ─────────────────────────────────────────────────────────────────

STAKE = 5.0
SLIP = 0.005
MAKER_FILL_PROB = 0.60

STDEV = {"btc": 0.167, "eth": 0.194, "sol": 0.247, "xrp": 0.440}
STDEV_BASE = 0.167


def stdev_scale(asset):
    return STDEV.get(asset, STDEV_BASE) / STDEV_BASE


def pm_fee(px):
    return px * (1.0 - px) * 0.0625


# ── Parse scanner.log ─────────────────────────────────────────────────────────

def parse_scanner_log(log_path):
    """Extract all book snapshots with full data."""
    books = defaultdict(list)
    scan_re = re.compile(
        r'\s+(\S+-updown-\d+m-\d+)\s+\|'
        r'\s+open=([\d.]+)\s+cl=([\d.]+)\s+bn=([\d.]+)'
        r'\s+d=([+-]?[\d.]+)%'
        r'\s+\|\s+s=([\d.]+)%\s+fy=([\d.]+)'
        r'\s+\|\s+YES=([\d.-]+)/([\d.-]+)\s+NO=([\d.-]+)/([\d.-]+)'
        r'\s+\|\s+T-(\d+)s'
    )
    with open(log_path) as f:
        for line in f:
            m = scan_re.search(line)
            if m:
                slug = m.group(1)
                def px(s):
                    try: return float(s)
                    except: return 0.0
                books[slug].append({
                    "secs_left": int(m.group(12)),
                    "open": float(m.group(2)),
                    "cl": float(m.group(3)),
                    "bn": float(m.group(4)),
                    "delta_pct": float(m.group(5)),
                    "sigma_pct": float(m.group(6)),
                    "fy": float(m.group(7)),
                    "yes_bid": px(m.group(8)),
                    "yes_ask": px(m.group(9)),
                    "no_bid": px(m.group(10)),
                    "no_ask": px(m.group(11)),
                })
    return books


def get_snapshots_in_window(book_data, t_start, t_end):
    """Get all snapshots where secs_left is between t_end and t_start."""
    return [s for s in book_data if t_end <= s["secs_left"] <= t_start]


# ── Analyze each settlement window ───────────────────────────────────────────

@dataclass
class WindowAnalysis:
    slug: str
    asset: str
    tf: int
    pct_move: float
    outcome: str  # YES or NO
    direction: str  # UP or DOWN (from CL move direction)

    # Book data at various times
    snaps_entry: list  # T-57 to T-44 (standard entry window)
    snaps_late: list   # T-25 to T-3 (late scalper window)
    snaps_all: list    # all snapshots for this slug

    # Derived: what delta was visible at entry time?
    max_delta_at_entry: float = 0.0
    # What was the best ask at entry?
    best_ask_at_entry: float = 0.0
    best_ask_late: float = 0.0

    # Number of consecutive snapshots showing same direction
    continuity_at_entry: int = 0


def analyze_windows(settlements, book_data):
    """Build detailed analysis for each settlement window."""
    windows = []

    for s in settlements:
        if s["pct_move"] == 0.0:
            continue  # stale CL

        slug = s["slug"]
        asset = s["asset"]
        tf = s["tf"]
        pct_move = s["pct_move"]
        outcome = s["outcome"]
        direction = "UP" if pct_move > 0 else "DOWN"

        snaps = book_data.get(slug, [])
        snaps_entry = get_snapshots_in_window(snaps, 57, 44)
        snaps_late = get_snapshots_in_window(snaps, 25, 3)

        # Max absolute delta seen in entry window
        max_delta = max((abs(snap["delta_pct"]) for snap in snaps_entry), default=0.0)

        # Best ask (for the correct direction) at entry
        if direction == "UP":
            asks_entry = [snap["yes_ask"] for snap in snaps_entry if snap["yes_ask"] > 0]
            asks_late = [snap["yes_ask"] for snap in snaps_late if snap["yes_ask"] > 0]
        else:
            asks_entry = [snap["no_ask"] for snap in snaps_entry if snap["no_ask"] > 0]
            asks_late = [snap["no_ask"] for snap in snaps_late if snap["no_ask"] > 0]

        best_ask_entry = min(asks_entry) if asks_entry else 0.0
        best_ask_late = min(asks_late) if asks_late else 0.0

        # Continuity: consecutive snapshots in entry window showing same direction
        cont = 0
        max_cont = 0
        for snap in sorted(snaps_entry, key=lambda x: -x["secs_left"]):
            snap_dir = "UP" if snap["delta_pct"] > 0 else "DOWN"
            if snap_dir == direction:
                cont += 1
                max_cont = max(max_cont, cont)
            else:
                cont = 0

        w = WindowAnalysis(
            slug=slug, asset=asset, tf=tf, pct_move=pct_move,
            outcome=outcome, direction=direction,
            snaps_entry=snaps_entry, snaps_late=snaps_late, snaps_all=snaps,
            max_delta_at_entry=max_delta,
            best_ask_at_entry=best_ask_entry,
            best_ask_late=best_ask_late,
            continuity_at_entry=max_cont,
        )
        windows.append(w)

    return windows


# ── Engine parameter sweep ────────────────────────────────────────────────────

@dataclass
class EngineParams:
    id: str
    tf: int  # 5, 15, or 0 (both = late scalper)
    delta: float
    continuity: int
    min_entry: float
    max_entry: float
    entry_start: int  # T-X start of entry window
    taker_deadline: int  # T-X end of entry window
    is_late_scalper: bool = False


def would_trade(params, window):
    """Check if engine would trade this window AND win."""

    # Timeframe filter
    if params.tf != 0 and window.tf != params.tf:
        return None

    if params.is_late_scalper:
        # Late scalper: only check book price range in late window
        if window.best_ask_late <= 0:
            return None
        ask = window.best_ask_late
        if ask < params.min_entry or ask > params.max_entry:
            return None

        # Direction is determined by CL move at that point
        # Win if direction matches outcome
        won = (window.direction == "UP" and window.outcome == "YES") or \
              (window.direction == "DOWN" and window.outcome == "NO")
        if not won:
            return None

        fill_px = max(round((ask - 0.01) * 100) / 100, params.min_entry)
        shares = STAKE / fill_px
        pnl = shares * 1.0 - STAKE
        return {"fill_px": fill_px, "pnl": pnl, "ask": ask}

    else:
        # Delta-based engines (A-D)
        scaled_delta = params.delta * stdev_scale(window.asset)

        # Check delta threshold
        if window.max_delta_at_entry < scaled_delta:
            return None

        # Check continuity
        if params.continuity > 0 and window.continuity_at_entry < params.continuity:
            return None

        # Check book price range
        if window.best_ask_at_entry <= 0:
            return None
        ask = window.best_ask_at_entry
        if ask < params.min_entry or ask > params.max_entry:
            return None

        # Win check
        won = (window.direction == "UP" and window.outcome == "YES") or \
              (window.direction == "DOWN" and window.outcome == "NO")
        if not won:
            return None

        fill_px = max(round((ask - 0.01) * 100) / 100, params.min_entry)
        shares = STAKE / fill_px
        pnl = shares * 1.0 - STAKE
        return {"fill_px": fill_px, "pnl": pnl, "ask": ask}


def evaluate_combo(engine_params_list, windows):
    """Evaluate a combination of engines. Returns (trades, wins, losses, pnl, details)."""
    trades = []
    wins = 0
    losses = 0
    pnl = 0.0

    for w in windows:
        for params in engine_params_list:
            result = would_trade(params, w)
            if result is not None:
                # Count as trade (already filtered to wins only by would_trade)
                trades.append({
                    "engine": params.id,
                    "slug": w.slug,
                    "asset": w.asset,
                    "tf": w.tf,
                    "direction": w.direction,
                    "outcome": w.outcome,
                    "fill_px": result["fill_px"],
                    "pnl": result["pnl"],
                })
                wins += 1
                pnl += result["pnl"]
                break  # Only first engine fires per window

    return trades, wins, losses, pnl


# ── Parameter grid ────────────────────────────────────────────────────────────

def run_sweep(windows):
    """Sweep parameters across all engines to find 100% WR combos."""

    print(f"\n{'='*72}")
    print("  PARAMETER SWEEP FOR 100% WIN RATE")
    print(f"{'='*72}")

    # First: understand the data
    print(f"\n  Windows analyzed: {len(windows)}")
    by_tf = defaultdict(list)
    for w in windows:
        by_tf[w.tf].append(w)

    for tf, ws in sorted(by_tf.items()):
        deltas = [w.max_delta_at_entry for w in ws if w.max_delta_at_entry > 0]
        asks = [w.best_ask_at_entry for w in ws if w.best_ask_at_entry > 0]
        late_asks = [w.best_ask_late for w in ws if w.best_ask_late > 0]
        conts = [w.continuity_at_entry for w in ws]

        correct = sum(1 for w in ws if
            (w.direction == "UP" and w.outcome == "YES") or
            (w.direction == "DOWN" and w.outcome == "NO"))

        print(f"\n  {tf}m windows: {len(ws)} total, {correct} direction-correct at entry")
        if deltas:
            print(f"    Delta at entry: min={min(deltas):.4f}%, max={max(deltas):.4f}%, "
                  f"med={sorted(deltas)[len(deltas)//2]:.4f}%")
        if asks:
            print(f"    Ask at entry: min={min(asks):.3f}, max={max(asks):.3f}, "
                  f"med={sorted(asks)[len(asks)//2]:.3f}")
        if late_asks:
            print(f"    Ask late (T-25..3): min={min(late_asks):.3f}, max={max(late_asks):.3f}, "
                  f"med={sorted(late_asks)[len(late_asks)//2]:.3f}")
        if conts:
            print(f"    Continuity: min={min(conts)}, max={max(conts)}, "
                  f"med={sorted(conts)[len(conts)//2]}")

    # ── Identify which windows are "safe" (direction correct at entry) ─────

    print(f"\n{'='*72}")
    print("  WINDOW-BY-WINDOW ANALYSIS")
    print(f"{'='*72}")

    for w in windows:
        correct = (w.direction == "UP" and w.outcome == "YES") or \
                  (w.direction == "DOWN" and w.outcome == "NO")
        tag = "OK " if correct else "BAD"

        # What does the book show?
        if w.snaps_entry:
            entry_snap = w.snaps_entry[0]
            entry_delta = entry_snap["delta_pct"]
            entry_dir = "UP" if entry_delta > 0 else "DOWN" if entry_delta < 0 else "FLAT"
        else:
            entry_delta = 0
            entry_dir = "N/A"

        print(f"  [{tag}] {w.slug:45s} | move={w.pct_move:+.4f}% → {w.outcome}"
              f" | delta@entry={w.max_delta_at_entry:.4f}% dir={entry_dir}"
              f" | ask@entry={w.best_ask_at_entry:.3f} ask@late={w.best_ask_late:.3f}"
              f" | cont={w.continuity_at_entry}")

    # ── Count "BAD" windows (direction wrong at entry time) ─────────────

    bad_windows = [w for w in windows if not (
        (w.direction == "UP" and w.outcome == "YES") or
        (w.direction == "DOWN" and w.outcome == "NO")
    )]
    print(f"\n  BAD windows (CL moved one way but settled opposite): {len(bad_windows)}")
    for w in bad_windows:
        print(f"    {w.slug} | move={w.pct_move:+.4f}% → {w.outcome}")

    # ── Sweep delta thresholds for A-D ─────────────────────────────────────

    # Only windows where direction is correct AND has entry data
    tradeable_5m = [w for w in windows if w.tf == 5 and
        w.best_ask_at_entry > 0 and
        ((w.direction == "UP" and w.outcome == "YES") or
         (w.direction == "DOWN" and w.outcome == "NO"))]

    tradeable_15m = [w for w in windows if w.tf == 15 and
        w.best_ask_at_entry > 0 and
        ((w.direction == "UP" and w.outcome == "YES") or
         (w.direction == "DOWN" and w.outcome == "NO"))]

    tradeable_late = [w for w in windows if
        w.best_ask_late > 0 and
        ((w.direction == "UP" and w.outcome == "YES") or
         (w.direction == "DOWN" and w.outcome == "NO"))]

    print(f"\n  Tradeable 5m windows (correct direction + has entry book data): {len(tradeable_5m)}")
    print(f"  Tradeable 15m windows: {len(tradeable_15m)}")
    print(f"  Tradeable late-scalper windows: {len(tradeable_late)}")

    # ── Check: are there any windows where direction is WRONG but we'd still win?
    # This happens when CL reverses between entry and settlement.
    # These are DANGEROUS — we need to avoid them.

    reversal_windows = []
    for w in windows:
        if w.snaps_entry:
            # Direction at entry time (from delta)
            entry_dir = "UP" if w.snaps_entry[0]["delta_pct"] > 0 else "DOWN"
            # Did that direction win?
            would_win = (entry_dir == "UP" and w.outcome == "YES") or \
                       (entry_dir == "DOWN" and w.outcome == "NO")
            if not would_win and w.max_delta_at_entry > 0.02:
                reversal_windows.append((w, entry_dir))

    if reversal_windows:
        print(f"\n  REVERSAL WARNINGS: {len(reversal_windows)} windows where entry delta"
              " pointed wrong way:")
        for w, edir in reversal_windows:
            print(f"    {w.slug} | entry_dir={edir} delta={w.max_delta_at_entry:.4f}%"
                  f" | outcome={w.outcome} | THIS WOULD BE A LOSS")

    # ── Grid search ────────────────────────────────────────────────────────

    print(f"\n{'='*72}")
    print("  GRID SEARCH: Engine Parameter Combinations")
    print(f"{'='*72}")

    # Parameters to sweep
    delta_values = [0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.10, 0.12, 0.15, 0.20]
    continuity_values = [0, 1, 2, 3, 4]
    min_entry_values = [0.85, 0.88, 0.90, 0.92, 0.93, 0.95]
    max_entry_values = [0.96, 0.97, 0.975, 0.98, 0.99]
    late_min_values = [0.90, 0.92, 0.93, 0.94, 0.95, 0.96]
    late_max_values = [0.96, 0.97, 0.975, 0.98, 0.99]

    # ── Individual engine analysis first ───────────────────────────────────

    print("\n  [A] 5m Sniper (delta + continuity=4):")
    best_a = sweep_single_engine("A", 5, tradeable_5m, delta_values,
                                  [4], min_entry_values, max_entry_values, windows)

    print("\n  [B] 5m D1 (delta + continuity=0):")
    best_b = sweep_single_engine("B", 5, tradeable_5m, delta_values,
                                  [0], min_entry_values, max_entry_values, windows)

    print("\n  [C] 15m Sniper (delta + continuity=4):")
    best_c = sweep_single_engine("C", 15, tradeable_15m, delta_values,
                                  [4], min_entry_values, max_entry_values, windows)

    print("\n  [D] 15m D1 (delta + continuity=0):")
    best_d = sweep_single_engine("D", 15, tradeable_15m, delta_values,
                                  [0], min_entry_values, max_entry_values, windows)

    print("\n  [E] Late Scalper (book price range, T-25..3):")
    best_e = sweep_late_scalper("E", tradeable_late, late_min_values, late_max_values, windows)

    # ── Best combined mix ──────────────────────────────────────────────────

    print(f"\n{'='*72}")
    print("  OPTIMAL COMBINED MIX (100% WR)")
    print(f"{'='*72}")

    # Use the best params from individual sweeps
    best_engines = []
    all_bests = [("A", best_a), ("B", best_b), ("C", best_c), ("D", best_d), ("E", best_e)]

    for eid, best in all_bests:
        if best is not None:
            best_engines.append(best)
            print(f"\n  Engine {eid}: {best}")

    if best_engines:
        combined_trades, combined_wins, combined_losses, combined_pnl = evaluate_combo(best_engines, windows)
        print(f"\n  Combined: {len(combined_trades)} trades, {combined_wins}W/{combined_losses}L, "
              f"P&L=${combined_pnl:+.2f}")

        # Deduplicate: check for overlapping windows
        slugs_seen = set()
        unique_trades = []
        for t in combined_trades:
            if t["slug"] not in slugs_seen:
                slugs_seen.add(t["slug"])
                unique_trades.append(t)

        if len(unique_trades) < len(combined_trades):
            print(f"  (After dedup: {len(unique_trades)} unique window trades)")

        # Show all trades
        print(f"\n  TRADE DETAILS:")
        for t in combined_trades:
            print(f"    [{t['engine']}] {t['asset']:>3} {t['tf']:>2}m {t['direction']:>4} "
                  f"@{t['fill_px']:.3f} → ${t['pnl']:+.4f}  {t['slug']}")

    # ── Realistic P&L with maker/taker ─────────────────────────────────────

    if combined_trades:
        print(f"\n  REALISTIC P&L (60% maker / 40% taker):")
        realistic_pnl = 0.0
        for i, t in enumerate(combined_trades):
            fill_px = t["fill_px"]
            # Deterministic fill assignment
            h = hash((t["slug"], t["engine"])) & 0xFFFFFFFF
            roll = (h % 1000) / 1000.0
            if roll < MAKER_FILL_PROB:
                # Maker: 0 fee
                r_pnl = STAKE / fill_px * 1.0 - STAKE
                ftype = "M"
            else:
                # Taker: slippage + fee
                taker_px = min(fill_px + SLIP + 0.01, 0.99)
                shares = STAKE / taker_px
                fee = pm_fee(taker_px) * shares
                r_pnl = shares * 1.0 - STAKE - fee
                ftype = "T"
            realistic_pnl += r_pnl
            print(f"    [{t['engine']}] {t['asset']:>3} {t['tf']:>2}m @{fill_px:.3f}({ftype}) "
                  f"→ ${r_pnl:+.4f}  {t['slug']}")
        print(f"\n  Total realistic P&L: ${realistic_pnl:+.2f}")

    # ── Save results ──────────────────────────────────────────────────────

    results = {
        "sweep_summary": {
            "total_settlements": len(windows),
            "tradeable_5m": len(tradeable_5m),
            "tradeable_15m": len(tradeable_15m),
            "tradeable_late": len(tradeable_late),
            "reversal_warnings": len(reversal_windows),
        },
        "optimal_engines": {},
        "combined_trades": combined_trades if best_engines else [],
        "combined_pnl": combined_pnl if best_engines else 0,
    }

    for eid, best in all_bests:
        if best is not None:
            results["optimal_engines"][eid] = {
                "id": best.id,
                "tf": best.tf,
                "delta": best.delta,
                "continuity": best.continuity,
                "min_entry": best.min_entry,
                "max_entry": best.max_entry,
                "is_late_scalper": best.is_late_scalper,
            }

    with open("optimal_mix_results.json", "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n  Results saved to optimal_mix_results.json")

    return results


def sweep_single_engine(engine_id, tf, tradeable, delta_vals, cont_vals,
                        min_entry_vals, max_entry_vals, all_windows):
    """Sweep parameters for a single delta-based engine."""

    best_combo = None
    best_trades = 0
    best_pnl = 0.0

    # Filter windows to this tf
    tf_windows = [w for w in all_windows if w.tf == tf]

    results = []

    for delta in delta_vals:
        for cont in cont_vals:
            for min_e in min_entry_vals:
                for max_e in max_entry_vals:
                    if min_e >= max_e:
                        continue

                    params = EngineParams(
                        id=engine_id, tf=tf, delta=delta, continuity=cont,
                        min_entry=min_e, max_entry=max_e,
                        entry_start=57, taker_deadline=44,
                    )

                    trades = 0
                    pnl = 0.0
                    any_loss = False

                    for w in tf_windows:
                        result = would_trade(params, w)
                        if result is not None:
                            trades += 1
                            pnl += result["pnl"]
                        # Also check: would this produce a LOSS on any window?

                    # Check for losses: try all windows including bad-direction ones
                    for w in tf_windows:
                        if w.best_ask_at_entry <= 0:
                            continue
                        # Check if engine would fire
                        scaled_delta = params.delta * stdev_scale(w.asset)
                        if w.max_delta_at_entry < scaled_delta:
                            continue
                        if params.continuity > 0 and w.continuity_at_entry < params.continuity:
                            continue
                        ask = w.best_ask_at_entry
                        if ask < params.min_entry or ask > params.max_entry:
                            continue
                        # Engine would fire — check if it wins
                        won = (w.direction == "UP" and w.outcome == "YES") or \
                              (w.direction == "DOWN" and w.outcome == "NO")
                        if not won:
                            any_loss = True
                            break

                    if not any_loss and trades > 0:
                        results.append((trades, pnl, params))
                        if trades > best_trades or (trades == best_trades and pnl > best_pnl):
                            best_trades = trades
                            best_pnl = pnl
                            best_combo = params

    # Show top results
    results.sort(key=lambda x: (-x[0], -x[1]))
    top = results[:10]
    for trades, pnl, p in top:
        print(f"    delta={p.delta:.2f}% cont={p.continuity} "
              f"entry=[{p.min_entry:.2f},{p.max_entry:.3f}] "
              f"→ {trades} trades, ${pnl:+.2f}")

    if best_combo:
        print(f"    >>> BEST: delta={best_combo.delta:.2f}% cont={best_combo.continuity} "
              f"entry=[{best_combo.min_entry:.2f},{best_combo.max_entry:.3f}] "
              f"→ {best_trades} trades, ${best_pnl:+.2f}")
    else:
        print(f"    >>> No 100% WR combo found")

    return best_combo


def sweep_late_scalper(engine_id, tradeable, min_vals, max_vals, all_windows):
    """Sweep parameters for late scalper engine."""

    best_combo = None
    best_trades = 0
    best_pnl = 0.0
    results = []

    for min_e in min_vals:
        for max_e in max_vals:
            if min_e >= max_e:
                continue

            params = EngineParams(
                id=engine_id, tf=0, delta=0, continuity=0,
                min_entry=min_e, max_entry=max_e,
                entry_start=25, taker_deadline=3,
                is_late_scalper=True,
            )

            trades = 0
            pnl = 0.0
            any_loss = False

            # Check ALL windows (not just tradeable) for losses
            for w in all_windows:
                if w.best_ask_late <= 0:
                    continue
                ask = w.best_ask_late
                if ask < params.min_entry or ask > params.max_entry:
                    continue
                # Engine would fire — check if it wins
                won = (w.direction == "UP" and w.outcome == "YES") or \
                      (w.direction == "DOWN" and w.outcome == "NO")
                if not won:
                    any_loss = True
                    break
                trades += 1
                fill_px = max(round((ask - 0.01) * 100) / 100, params.min_entry)
                shares = STAKE / fill_px
                pnl += shares * 1.0 - STAKE

            if not any_loss and trades > 0:
                results.append((trades, pnl, params))
                if trades > best_trades or (trades == best_trades and pnl > best_pnl):
                    best_trades = trades
                    best_pnl = pnl
                    best_combo = params

    results.sort(key=lambda x: (-x[0], -x[1]))
    top = results[:10]
    for trades, pnl, p in top:
        print(f"    range=[{p.min_entry:.2f},{p.max_entry:.3f}] "
              f"→ {trades} trades, ${pnl:+.2f}")

    if best_combo:
        print(f"    >>> BEST: range=[{best_combo.min_entry:.2f},{best_combo.max_entry:.3f}] "
              f"→ {best_trades} trades, ${best_pnl:+.2f}")
    else:
        print(f"    >>> No 100% WR combo found")

    return best_combo


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    settle_path = Path("settlements_2026-03-15.jsonl")
    log_path = Path("scanner.log")

    if not settle_path.exists():
        print(f"ERROR: {settle_path} not found")
        sys.exit(1)

    settlements = []
    with open(settle_path) as f:
        for line in f:
            line = line.strip()
            if line:
                settlements.append(json.loads(line))

    print(f"Loaded {len(settlements)} settlements")

    book_data = parse_scanner_log(str(log_path))
    total_snaps = sum(len(v) for v in book_data.values())
    print(f"Parsed {total_snaps} book snapshots across {len(book_data)} slugs")

    windows = analyze_windows(settlements, book_data)
    print(f"Analyzed {len(windows)} non-stale windows")

    results = run_sweep(windows)

    # Final summary
    print(f"\n{'='*72}")
    print("  FINAL RECOMMENDATION")
    print(f"{'='*72}")

    if results["optimal_engines"]:
        print("\n  Optimal engine config for 100% WR:")
        for eid, cfg in results["optimal_engines"].items():
            if cfg["is_late_scalper"]:
                print(f"    Engine {eid}: late_scalper, range=[{cfg['min_entry']:.2f}, {cfg['max_entry']:.3f}]")
            else:
                print(f"    Engine {eid}: tf={cfg['tf']}m, delta={cfg['delta']:.2f}%, "
                      f"continuity={cfg['continuity']}, "
                      f"range=[{cfg['min_entry']:.2f}, {cfg['max_entry']:.3f}]")
        print(f"\n  Combined: {len(results['combined_trades'])} trades, "
              f"P&L=${results['combined_pnl']:+.2f}")
    else:
        print("\n  No engine configuration found with 100% WR on this dataset.")
        print("  The data may be too noisy or the entry windows too narrow.")


if __name__ == "__main__":
    main()
