#!/usr/bin/env python3
"""
Forward Test: Oracle Scanner V2 Settlements → CL Sniper 5-Engine Simulation

Reads settlements_2026-03-15.jsonl (actual market outcomes from scanner v2 data capture)
and simulates what the 5-engine sniper (cl-sniper-10mar / cl-oracle-scanner v4.0) would
have traded and earned.

Engines:
  A: 5m  sniper  — delta>=0.04% (stdev-scaled), continuity=4, all filters
  B: 5m  D1      — delta>=0.15% (stdev-scaled), instant, all filters
  C: 15m sniper  — delta>=0.04% (stdev-scaled), continuity=4, all filters
  D: 15m D1      — delta>=0.15% (stdev-scaled), instant, all filters
  E: late scalper — book>=0.95, no delta threshold, last 25s

Also parses scanner.log for book snapshots to get real book prices at entry time.
"""

import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

# ── Engine Parameters (matching config.toml / cl-sniper-10mar) ──────────────

STAKE = 5.0
MAX_DD = 50.0

STDEV = {"btc": 0.167, "eth": 0.194, "sol": 0.247, "xrp": 0.440}
STDEV_BASE = 0.167

MIN_DELTA = {"btc": 0.015, "eth": 0.020, "sol": 0.030, "xrp": 0.050}


def stdev_scale(asset: str) -> float:
    return STDEV.get(asset, STDEV_BASE) / STDEV_BASE


def pm_fee(px: float) -> float:
    return px * (1.0 - px) * 0.0625


@dataclass
class EngineConfig:
    id: str
    tf: int  # 5, 15, or 0 (both)
    delta: float  # % threshold (pre-scaling)
    continuity: int
    bn_contra: bool
    cl_fade: bool
    regime: bool
    is_late_scalper: bool
    entry_start: int  # max secs_left
    taker_deadline: int  # min secs_left
    min_entry: float
    max_entry: float

    def scaled_delta(self, asset: str) -> float:
        return self.delta * stdev_scale(asset)


ENGINES = [
    EngineConfig("A", 5, 0.04, 4, True, True, True, False, 57, 44, 0.88, 0.98),
    EngineConfig("B", 5, 0.15, 0, True, True, True, False, 57, 44, 0.88, 0.98),
    EngineConfig("C", 15, 0.04, 4, True, True, True, False, 57, 44, 0.88, 0.98),
    EngineConfig("D", 15, 0.15, 0, True, True, True, False, 57, 44, 0.88, 0.98),
    EngineConfig("E", 0, 0.0, 0, False, False, False, True, 25, 3, 0.95, 0.975),
]


@dataclass
class Trade:
    engine: str
    slug: str
    asset: str
    tf: int
    direction: str
    fill_px: float
    shares: float
    entry_fee: float
    outcome: str  # YES or NO
    pnl: float
    exit_reason: str  # WIN, LOSS


@dataclass
class EngineStats:
    wins: int = 0
    losses: int = 0
    sl_count: int = 0
    pnl: float = 0.0
    trades: list = field(default_factory=list)

    @property
    def total(self):
        return self.wins + self.losses + self.sl_count

    @property
    def wr(self):
        return (self.wins / self.total * 100.0) if self.total > 0 else 0.0


# ── Parse scanner.log for book prices at T-57s to T-44s ────────────────────

def parse_scanner_log(log_path: str) -> dict:
    """
    Extract book snapshots from scanner.log.
    Returns: {slug: [(secs_left, yes_ask, yes_bid, no_ask, no_bid)]}
    """
    books = defaultdict(list)

    scan_re = re.compile(
        r'\s+(\S+-updown-\d+m-\d+)\s+\|'
        r'\s+open=([\d.]+)\s+cl=([\d.]+)\s+bn=([\d.]+)'
        r'\s+d=([+-]?[\d.]+)%'
        r'\s+\|\s+s=([\d.]+)%\s+fy=([\d.]+)'
        r'\s+\|\s+YES=([\d.-]+)/([\d.-]+)\s+NO=([\d.-]+)/([\d.-]+)'
        r'\s+\|\s+T-(\d+)s'
    )

    path = Path(log_path)
    if not path.exists():
        return books

    with open(path, 'r') as f:
        for line in f:
            m = scan_re.search(line)
            if m:
                slug = m.group(1)
                cl_now = float(m.group(3))
                delta_pct = float(m.group(5))
                fy = float(m.group(7))
                yes_bid = m.group(8)
                yes_ask = m.group(9)
                no_bid = m.group(10)
                no_ask = m.group(11)
                secs_left = int(m.group(12))

                # Parse prices (-- means no book)
                def parse_px(s):
                    try:
                        return float(s)
                    except ValueError:
                        return 0.0

                books[slug].append({
                    "secs_left": secs_left,
                    "cl_now": cl_now,
                    "delta_pct": delta_pct,
                    "fy": fy,
                    "yes_bid": parse_px(yes_bid),
                    "yes_ask": parse_px(yes_ask),
                    "no_bid": parse_px(no_bid),
                    "no_ask": parse_px(no_ask),
                })

    return books


def get_entry_snapshot(book_data: list, entry_start: int, taker_deadline: int):
    """Find best book snapshot in the entry window [taker_deadline, entry_start]."""
    candidates = [
        snap for snap in book_data
        if taker_deadline <= snap["secs_left"] <= entry_start
    ]
    if not candidates:
        return None
    # Pick the snapshot closest to entry_start (earliest in the window)
    return min(candidates, key=lambda s: abs(s["secs_left"] - entry_start))


# ── Forward test logic ──────────────────────────────────────────────────────

def simulate_engine(eng: EngineConfig, settlement: dict, book_data: list) -> Trade | None:
    """Simulate whether an engine would have entered and what the P&L would be."""

    asset = settlement["asset"]
    tf = settlement["tf"]
    pct_move = abs(settlement["pct_move"])
    outcome = settlement["outcome"]  # YES or NO

    # Timeframe filter
    if eng.tf != 0 and tf != eng.tf:
        return None

    # Get book snapshot in entry window
    snap = get_entry_snapshot(book_data, eng.entry_start, eng.taker_deadline)

    if eng.is_late_scalper:
        # Engine E: late scalper
        # Look for book snapshot in E's window (25s to 3s left)
        snap = get_entry_snapshot(book_data, eng.entry_start, eng.taker_deadline)
        if snap is None:
            return None

        # Min delta check (even for E)
        if pct_move < MIN_DELTA.get(asset, 0.020):
            return None

        # Determine direction from CL move
        direction = "UP" if settlement["pct_move"] > 0 else "DOWN"

        # Pick the correct side's ask
        if direction == "UP":
            best_ask = snap["yes_ask"]
        else:
            best_ask = snap["no_ask"]

        if best_ask < eng.min_entry or best_ask > eng.max_entry or best_ask <= 0:
            return None

    else:
        # Engines A-D: delta-based
        if snap is None:
            # No book data in entry window — try to estimate from settlement data
            # If delta is large enough, the book was likely in range
            if pct_move < eng.scaled_delta(asset):
                return None
            if pct_move < MIN_DELTA.get(asset, 0.020):
                return None
            # Estimate entry price from fair value (approximate)
            # With large delta at T-55s, book typically shows 0.88-0.96
            snap = None  # Will use estimated fill below

        else:
            # Check delta threshold
            delta = abs(snap["delta_pct"])
            if delta < eng.scaled_delta(asset):
                return None
            if delta < MIN_DELTA.get(asset, 0.020):
                return None

        direction = "UP" if settlement["pct_move"] > 0 else "DOWN"

        if snap:
            if direction == "UP":
                best_ask = snap["yes_ask"]
            else:
                best_ask = snap["no_ask"]

            if best_ask < eng.min_entry or best_ask > eng.max_entry or best_ask <= 0:
                return None
        else:
            # No book data — estimate fill price based on delta magnitude
            # Typical relationship: 0.04% delta → ~0.55 fair → ~0.88-0.92 book (lagging)
            # 0.15% delta → ~0.85 fair → ~0.93-0.97 book
            estimated_fair = min(0.50 + pct_move * 5.0, 0.98)
            best_ask = max(estimated_fair - 0.02, eng.min_entry)
            if best_ask > eng.max_entry:
                return None

    # Maker fill: ask - 0.01 (clamped to min_entry)
    maker_px = max(round((best_ask - 0.01) * 100) / 100, eng.min_entry)
    fill_px = maker_px  # Assume maker fill (0% fee)

    shares = STAKE / fill_px
    entry_fee = pm_fee(fill_px) * shares  # Maker = 0% in practice, but compute for taker scenarios

    # Settlement P&L
    won = (direction == "UP" and outcome == "YES") or (direction == "DOWN" and outcome == "NO")

    if won:
        # WIN: shares * $1.00 - stake - entry_fee (maker fee = 0)
        pnl = shares * 1.0 - STAKE  # Maker = no fee
        exit_reason = "WIN"
    else:
        # LOSS: lose entire stake
        pnl = -STAKE
        exit_reason = "LOSS"

    return Trade(
        engine=eng.id,
        slug=settlement["slug"],
        asset=asset,
        tf=tf,
        direction=direction,
        fill_px=fill_px,
        shares=shares,
        entry_fee=entry_fee,
        outcome=outcome,
        pnl=pnl,
        exit_reason=exit_reason,
    )


def main():
    settle_path = Path("settlements_2026-03-15.jsonl")
    log_path = Path("scanner.log")

    if not settle_path.exists():
        print(f"ERROR: {settle_path} not found")
        sys.exit(1)

    # Load settlements
    settlements = []
    with open(settle_path) as f:
        for line in f:
            line = line.strip()
            if line:
                settlements.append(json.loads(line))

    print(f"Loaded {len(settlements)} settlements")

    # Parse scanner log for book data
    print(f"Parsing {log_path} for book snapshots...")
    book_data = parse_scanner_log(str(log_path))
    slugs_with_books = sum(1 for v in book_data.values() if v)
    print(f"Found book data for {slugs_with_books} slugs")

    # Group settlements
    by_tf = defaultdict(list)
    by_asset = defaultdict(list)
    for s in settlements:
        by_tf[s["tf"]].append(s)
        by_asset[s["asset"]].append(s)

    print(f"\nSettlements by timeframe:")
    for tf, items in sorted(by_tf.items()):
        yes_count = sum(1 for i in items if i["outcome"] == "YES")
        no_count = sum(1 for i in items if i["outcome"] == "NO")
        avg_move = sum(abs(i["pct_move"]) for i in items) / len(items)
        print(f"  {tf}m: {len(items)} windows (YES={yes_count}, NO={no_count}, avg_move={avg_move:.4f}%)")

    print(f"\nSettlements by asset:")
    for asset, items in sorted(by_asset.items()):
        avg_move = sum(abs(i["pct_move"]) for i in items) / len(items)
        print(f"  {asset}: {len(items)} windows, avg_move={avg_move:.4f}%")

    # Check for stale CL (pct_move == 0 means CL didn't update)
    stale = [s for s in settlements if s["pct_move"] == 0.0]
    if stale:
        print(f"\n  WARNING: {len(stale)} windows had 0% CL move (CL feed stale)")
        for s in stale:
            div = s.get("cl_bn_divergence", 0)
            print(f"    {s['slug']} | BN close={s.get('bn_close', '?')} | CL/BN div={div:.4f}")

    # ── Run 5-engine simulation ─────────────────────────────────────────────

    print("\n" + "=" * 72)
    print("  FORWARD TEST: 5-Engine Sniper vs Scanner V2 Settlements")
    print("=" * 72)

    engine_stats: dict[str, EngineStats] = {e.id: EngineStats() for e in ENGINES}
    all_trades: list[Trade] = []
    cum_pnl = 0.0
    halted = False

    for settlement in settlements:
        slug = settlement["slug"]
        books = book_data.get(slug, [])

        for eng in ENGINES:
            if halted:
                break

            stats = engine_stats[eng.id]
            trade = simulate_engine(eng, settlement, books)

            if trade is None:
                continue

            stats.trades.append(trade)
            if trade.exit_reason == "WIN":
                stats.wins += 1
            else:
                stats.losses += 1
            stats.pnl += trade.pnl
            cum_pnl += trade.pnl
            all_trades.append(trade)

            # Kill switch
            if cum_pnl <= -MAX_DD:
                print(f"\n  KILL SWITCH at cum=${cum_pnl:+.2f} (max DD=${MAX_DD})")
                halted = True
                break

    # ── Results ──────────────────────────────────────────────────────────────

    print(f"\n{'Engine':<8} {'Trades':>6} {'W':>4} {'L':>4} {'WR':>7} {'P&L':>10} {'Avg P&L':>10}")
    print("-" * 60)

    for eng in ENGINES:
        s = engine_stats[eng.id]
        avg = s.pnl / s.total if s.total > 0 else 0.0
        print(f"  {eng.id:<6} {s.total:>6} {s.wins:>4} {s.losses:>4} {s.wr:>6.1f}% ${s.pnl:>+8.2f} ${avg:>+8.4f}")

    total_trades = sum(s.total for s in engine_stats.values())
    total_wins = sum(s.wins for s in engine_stats.values())
    total_losses = sum(s.losses for s in engine_stats.values())
    total_wr = (total_wins / total_trades * 100) if total_trades > 0 else 0

    print("-" * 60)
    print(f"  {'TOTAL':<6} {total_trades:>6} {total_wins:>4} {total_losses:>4} {total_wr:>6.1f}% ${cum_pnl:>+8.2f}")

    # ── Per-engine trade details ────────────────────────────────────────────

    print(f"\n{'=' * 72}")
    print("  TRADE LOG")
    print("=" * 72)

    for eng in ENGINES:
        s = engine_stats[eng.id]
        if not s.trades:
            print(f"\n  [{eng.id}] No trades")
            continue
        print(f"\n  [{eng.id}] {s.total} trades, {s.wins}W/{s.losses}L, WR={s.wr:.1f}%, P&L=${s.pnl:+.2f}")
        for t in s.trades:
            tag = "WIN" if t.exit_reason == "WIN" else "LOSS"
            print(f"    {tag:4} {t.asset:>3} {t.tf:>2}m {t.direction:>4} @{t.fill_px:.3f} -> ${t.pnl:+.4f}  {t.slug}")

    # ── Analysis ────────────────────────────────────────────────────────────

    print(f"\n{'=' * 72}")
    print("  ANALYSIS")
    print("=" * 72)

    # Win/loss by direction
    up_trades = [t for t in all_trades if t.direction == "UP"]
    dn_trades = [t for t in all_trades if t.direction == "DOWN"]
    up_wins = sum(1 for t in up_trades if t.exit_reason == "WIN")
    dn_wins = sum(1 for t in dn_trades if t.exit_reason == "WIN")

    if up_trades:
        print(f"\n  UP trades:   {len(up_trades)} ({up_wins}W, WR={up_wins/len(up_trades)*100:.1f}%)")
    if dn_trades:
        print(f"  DOWN trades: {len(dn_trades)} ({dn_wins}W, WR={dn_wins/len(dn_trades)*100:.1f}%)")

    # Win/loss by asset
    print(f"\n  By asset:")
    for asset in sorted(set(t.asset for t in all_trades)):
        at = [t for t in all_trades if t.asset == asset]
        wins = sum(1 for t in at if t.exit_reason == "WIN")
        pnl = sum(t.pnl for t in at)
        print(f"    {asset:>3}: {len(at)} trades, {wins}W, WR={wins/len(at)*100:.1f}%, P&L=${pnl:+.2f}")

    # Win/loss by timeframe
    print(f"\n  By timeframe:")
    for tf in sorted(set(t.tf for t in all_trades)):
        tt = [t for t in all_trades if t.tf == tf]
        wins = sum(1 for t in tt if t.exit_reason == "WIN")
        pnl = sum(t.pnl for t in tt)
        print(f"    {tf:>2}m: {len(tt)} trades, {wins}W, WR={wins/len(tt)*100:.1f}%, P&L=${pnl:+.2f}")

    # Average fill price
    if all_trades:
        avg_fill = sum(t.fill_px for t in all_trades) / len(all_trades)
        avg_win_fill = sum(t.fill_px for t in all_trades if t.exit_reason == "WIN") / max(total_wins, 1)
        avg_loss_fill = sum(t.fill_px for t in all_trades if t.exit_reason == "LOSS") / max(total_losses, 1)
        print(f"\n  Avg fill price: {avg_fill:.3f}")
        print(f"  Avg WIN  fill:  {avg_win_fill:.3f}")
        if total_losses > 0:
            print(f"  Avg LOSS fill:  {avg_loss_fill:.3f}")

    # CL/BN divergence analysis
    if settlements:
        div_values = [s.get("cl_bn_divergence", 0) for s in settlements if s.get("cl_bn_divergence")]
        if div_values:
            avg_div = sum(div_values) / len(div_values)
            max_div = max(div_values)
            print(f"\n  CL/BN divergence: avg={avg_div:.6f}, max={max_div:.6f}")

    # Stale CL impact
    if stale:
        stale_slugs = {s["slug"] for s in stale}
        stale_trades = [t for t in all_trades if t.slug in stale_slugs]
        if stale_trades:
            print(f"\n  WARNING: {len(stale_trades)} trades on stale-CL windows")

    # Theoretical max if perfect entries
    max_theoretical = sum(
        (STAKE / 0.92 * 1.0 - STAKE) if s["pct_move"] != 0 else 0
        for s in settlements
    )
    print(f"\n  Theoretical max P&L (all windows, @0.92 fill): ${max_theoretical:+.2f}")
    print(f"  Actual simulated P&L: ${cum_pnl:+.2f}")
    if max_theoretical > 0:
        print(f"  Capture rate: {cum_pnl / max_theoretical * 100:.1f}%")

    # ── Save detailed results ───────────────────────────────────────────────

    results_path = Path("forward_test_results.jsonl")
    with open(results_path, 'w') as f:
        for t in all_trades:
            f.write(json.dumps({
                "engine": t.engine,
                "slug": t.slug,
                "asset": t.asset,
                "tf": t.tf,
                "direction": t.direction,
                "fill_px": t.fill_px,
                "shares": t.shares,
                "entry_fee": t.entry_fee,
                "outcome": t.outcome,
                "pnl": t.pnl,
                "exit_reason": t.exit_reason,
            }) + "\n")

    print(f"\n  Detailed results saved to {results_path}")
    print(f"  Total trades: {len(all_trades)}")


if __name__ == "__main__":
    main()
