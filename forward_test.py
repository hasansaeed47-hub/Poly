#!/usr/bin/env python3
"""
Forward Test: Oracle Scanner V2 Settlements → CL Sniper 5-Engine Simulation

Reads settlements_2026-03-15.jsonl (actual market outcomes from scanner v2 data capture)
and simulates what the 5-engine sniper would have traded and earned.

Runs THREE scenarios:
  1. ORIGINAL: Production config (B=0.15%, E=0.95-0.975)
  2. TUNED:    B=0.12%, E=0.93-0.98
  3. REALISTIC: Tuned + maker fill probability + taker slippage + fees

Fill model (REALISTIC):
  - Maker fill probability: 60% (from Hydra paper trading data)
  - Failed maker → taker fallback at ask + SLIP (0.5%)
  - Taker fee: pm_fee(fill_px) per share (maker = 0%)
  - Book depth: require ask size >= STAKE / ask_px (approximated)

Also parses scanner.log for book snapshots to get real book prices at entry time.
"""

import json
import re
import sys
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path

# ── Engine Parameters (matching config.toml / cl-sniper-10mar) ──────────────

STAKE = 5.0
MAX_DD = 50.0
SLIP = 0.005  # taker slippage
MAKER_FILL_PROB = 0.60  # from Hydra paper trading

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


def make_engines_original():
    return [
        EngineConfig("A", 5, 0.04, 4, True, True, True, False, 57, 44, 0.88, 0.98),
        EngineConfig("B", 5, 0.15, 0, True, True, True, False, 57, 44, 0.88, 0.98),
        EngineConfig("C", 15, 0.04, 4, True, True, True, False, 57, 44, 0.88, 0.98),
        EngineConfig("D", 15, 0.15, 0, True, True, True, False, 57, 44, 0.88, 0.98),
        EngineConfig("E", 0, 0.0, 0, False, False, False, True, 25, 3, 0.95, 0.975),
    ]


def make_engines_tuned():
    return [
        EngineConfig("A", 5, 0.04, 4, True, True, True, False, 57, 44, 0.88, 0.98),
        EngineConfig("B", 5, 0.12, 0, True, True, True, False, 57, 44, 0.88, 0.98),  # 0.15 → 0.12
        EngineConfig("C", 15, 0.04, 4, True, True, True, False, 57, 44, 0.88, 0.98),
        EngineConfig("D", 15, 0.15, 0, True, True, True, False, 57, 44, 0.88, 0.98),
        EngineConfig("E", 0, 0.0, 0, False, False, False, True, 25, 3, 0.93, 0.98),  # 0.95-0.975 → 0.93-0.98
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
    fill_type: str = "maker"  # maker or taker


@dataclass
class EngineStats:
    wins: int = 0
    losses: int = 0
    sl_count: int = 0
    pnl: float = 0.0
    trades: list = field(default_factory=list)
    maker_fills: int = 0
    taker_fills: int = 0

    @property
    def total(self):
        return self.wins + self.losses + self.sl_count

    @property
    def wr(self):
        return (self.wins / self.total * 100.0) if self.total > 0 else 0.0


# ── Parse scanner.log for book prices ───────────────────────────────────────

def parse_scanner_log(log_path: str) -> dict:
    """Extract book snapshots from scanner.log."""
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
                secs_left = int(m.group(12))

                def parse_px(s):
                    try:
                        return float(s)
                    except ValueError:
                        return 0.0

                books[slug].append({
                    "secs_left": secs_left,
                    "cl_now": float(m.group(3)),
                    "delta_pct": float(m.group(5)),
                    "fy": float(m.group(7)),
                    "yes_bid": parse_px(m.group(8)),
                    "yes_ask": parse_px(m.group(9)),
                    "no_bid": parse_px(m.group(10)),
                    "no_ask": parse_px(m.group(11)),
                })

    return books


def get_entry_snapshot(book_data: list, entry_start: int, taker_deadline: int):
    """Find best book snapshot in the entry window."""
    candidates = [
        snap for snap in book_data
        if taker_deadline <= snap["secs_left"] <= entry_start
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda s: abs(s["secs_left"] - entry_start))


# ── Deterministic fill seed (reproducible "randomness" per trade) ───────────

def fill_hash(slug: str, engine_id: str) -> float:
    """Deterministic 0.0-1.0 value per (slug, engine) for fill simulation."""
    h = hash((slug, engine_id)) & 0xFFFFFFFF
    return (h % 1000) / 1000.0


# ── Forward test logic ──────────────────────────────────────────────────────

def simulate_engine(
    eng: EngineConfig,
    settlement: dict,
    book_data: list,
    realistic: bool = False,
) -> Trade | None:
    """Simulate whether an engine would have entered and what the P&L would be."""

    asset = settlement["asset"]
    tf = settlement["tf"]
    pct_move = abs(settlement["pct_move"])
    outcome = settlement["outcome"]

    # Skip stale-CL windows (0% move = CL feed was dead)
    if settlement["pct_move"] == 0.0:
        return None

    # Timeframe filter
    if eng.tf != 0 and tf != eng.tf:
        return None

    snap = get_entry_snapshot(book_data, eng.entry_start, eng.taker_deadline)

    if eng.is_late_scalper:
        snap = get_entry_snapshot(book_data, eng.entry_start, eng.taker_deadline)
        if snap is None:
            return None

        if pct_move < MIN_DELTA.get(asset, 0.020):
            return None

        direction = "UP" if settlement["pct_move"] > 0 else "DOWN"
        best_ask = snap["yes_ask"] if direction == "UP" else snap["no_ask"]

        if best_ask < eng.min_entry or best_ask > eng.max_entry or best_ask <= 0:
            return None

    else:
        # Engines A-D: delta-based
        if snap is None:
            if pct_move < eng.scaled_delta(asset):
                return None
            if pct_move < MIN_DELTA.get(asset, 0.020):
                return None
            snap = None
        else:
            delta = abs(snap["delta_pct"])
            if delta < eng.scaled_delta(asset):
                return None
            if delta < MIN_DELTA.get(asset, 0.020):
                return None

        direction = "UP" if settlement["pct_move"] > 0 else "DOWN"

        if snap:
            best_ask = snap["yes_ask"] if direction == "UP" else snap["no_ask"]
            if best_ask < eng.min_entry or best_ask > eng.max_entry or best_ask <= 0:
                return None
        else:
            estimated_fair = min(0.50 + pct_move * 5.0, 0.98)
            best_ask = max(estimated_fair - 0.02, eng.min_entry)
            if best_ask > eng.max_entry:
                return None

    # ── Fill simulation ─────────────────────────────────────────────────────

    maker_px = max(round((best_ask - 0.01) * 100) / 100, eng.min_entry)

    if realistic:
        # Deterministic fill simulation
        roll = fill_hash(settlement["slug"], eng.id)
        if roll < MAKER_FILL_PROB:
            # Maker fill — 0% fee
            fill_px = maker_px
            fill_type = "maker"
            entry_fee = 0.0
        else:
            # Taker fallback — pay fee + slippage
            taker_px = min(best_ask + SLIP, 0.99)
            if taker_px > eng.max_entry:
                return None
            fill_px = taker_px
            fill_type = "taker"
            shares_pre = STAKE / fill_px
            entry_fee = pm_fee(fill_px) * shares_pre
    else:
        # Optimistic: assume maker fill always
        fill_px = maker_px
        fill_type = "maker"
        entry_fee = 0.0

    shares = STAKE / fill_px

    # ── P&L calculation ─────────────────────────────────────────────────────

    won = (direction == "UP" and outcome == "YES") or (direction == "DOWN" and outcome == "NO")

    if won:
        # WIN: shares settle at $1.00 each, minus stake, minus entry fee
        pnl = shares * 1.0 - STAKE - entry_fee
        exit_reason = "WIN"
    else:
        # LOSS: lose stake + entry fee
        pnl = -STAKE - entry_fee
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
        fill_type=fill_type,
    )


def run_scenario(
    name: str,
    engines: list[EngineConfig],
    settlements: list[dict],
    book_data: dict,
    realistic: bool = False,
) -> tuple[float, list[Trade]]:
    """Run one scenario and print results."""

    print(f"\n{'=' * 72}")
    print(f"  SCENARIO: {name}")
    if realistic:
        print(f"  Fill model: {MAKER_FILL_PROB*100:.0f}% maker (0% fee), "
              f"{(1-MAKER_FILL_PROB)*100:.0f}% taker (+{SLIP*100:.1f}% slip + PM fee)")
    else:
        print(f"  Fill model: 100% maker assumed (0% fee, no slippage)")
    print("=" * 72)

    engine_stats: dict[str, EngineStats] = {e.id: EngineStats() for e in engines}
    all_trades: list[Trade] = []
    cum_pnl = 0.0
    halted = False

    for settlement in settlements:
        slug = settlement["slug"]
        books = book_data.get(slug, [])

        for eng in engines:
            if halted:
                break

            stats = engine_stats[eng.id]
            trade = simulate_engine(eng, settlement, books, realistic=realistic)

            if trade is None:
                continue

            stats.trades.append(trade)
            if trade.exit_reason == "WIN":
                stats.wins += 1
            else:
                stats.losses += 1
            if trade.fill_type == "maker":
                stats.maker_fills += 1
            else:
                stats.taker_fills += 1
            stats.pnl += trade.pnl
            cum_pnl += trade.pnl
            all_trades.append(trade)

            if cum_pnl <= -MAX_DD:
                print(f"\n  KILL SWITCH at cum=${cum_pnl:+.2f} (max DD=${MAX_DD})")
                halted = True
                break

    # ── Summary table ────────────────────────────────────────────────────────

    if realistic:
        print(f"\n{'Engine':<8} {'Trades':>6} {'W':>4} {'L':>4} {'WR':>7} {'P&L':>10} "
              f"{'Maker':>6} {'Taker':>6} {'Fees':>8}")
        print("-" * 76)
        for eng in engines:
            s = engine_stats[eng.id]
            fees = sum(t.entry_fee for t in s.trades)
            print(f"  {eng.id:<6} {s.total:>6} {s.wins:>4} {s.losses:>4} {s.wr:>6.1f}% "
                  f"${s.pnl:>+8.2f} {s.maker_fills:>5}M {s.taker_fills:>5}T ${fees:>+7.4f}")
    else:
        print(f"\n{'Engine':<8} {'Trades':>6} {'W':>4} {'L':>4} {'WR':>7} {'P&L':>10} {'Avg P&L':>10}")
        print("-" * 60)
        for eng in engines:
            s = engine_stats[eng.id]
            avg = s.pnl / s.total if s.total > 0 else 0.0
            print(f"  {eng.id:<6} {s.total:>6} {s.wins:>4} {s.losses:>4} {s.wr:>6.1f}% "
                  f"${s.pnl:>+8.2f} ${avg:>+8.4f}")

    total_trades = sum(s.total for s in engine_stats.values())
    total_wins = sum(s.wins for s in engine_stats.values())
    total_losses = sum(s.losses for s in engine_stats.values())
    total_wr = (total_wins / total_trades * 100) if total_trades > 0 else 0

    print("-" * 76 if realistic else "-" * 60)
    print(f"  {'TOTAL':<6} {total_trades:>6} {total_wins:>4} {total_losses:>4} "
          f"{total_wr:>6.1f}% ${cum_pnl:>+8.2f}")

    # ── Trade log ────────────────────────────────────────────────────────────

    print(f"\n  TRADE LOG:")
    for eng in engines:
        s = engine_stats[eng.id]
        if not s.trades:
            print(f"    [{eng.id}] No trades")
            continue
        print(f"    [{eng.id}] {s.total} trades, {s.wins}W/{s.losses}L, "
              f"WR={s.wr:.1f}%, P&L=${s.pnl:+.2f}")
        for t in s.trades:
            tag = "WIN " if t.exit_reason == "WIN" else "LOSS"
            ft = "M" if t.fill_type == "maker" else "T"
            fee_str = f" fee=${t.entry_fee:.4f}" if t.entry_fee > 0 else ""
            print(f"      {tag} {t.asset:>3} {t.tf:>2}m {t.direction:>4} "
                  f"@{t.fill_px:.3f}({ft}) -> ${t.pnl:+.4f}{fee_str}  {t.slug}")

    # ── Breakdown ────────────────────────────────────────────────────────────

    if all_trades:
        print(f"\n  By direction:")
        for d in ["UP", "DOWN"]:
            dt = [t for t in all_trades if t.direction == d]
            if dt:
                dw = sum(1 for t in dt if t.exit_reason == "WIN")
                dp = sum(t.pnl for t in dt)
                print(f"    {d:>4}: {len(dt)} trades, {dw}W, "
                      f"WR={dw/len(dt)*100:.1f}%, P&L=${dp:+.2f}")

        print(f"\n  By asset:")
        for asset in sorted(set(t.asset for t in all_trades)):
            at = [t for t in all_trades if t.asset == asset]
            wins = sum(1 for t in at if t.exit_reason == "WIN")
            pnl = sum(t.pnl for t in at)
            print(f"    {asset:>3}: {len(at)} trades, {wins}W, "
                  f"WR={wins/len(at)*100:.1f}%, P&L=${pnl:+.2f}")

        print(f"\n  By timeframe:")
        for tf in sorted(set(t.tf for t in all_trades)):
            tt = [t for t in all_trades if t.tf == tf]
            wins = sum(1 for t in tt if t.exit_reason == "WIN")
            pnl = sum(t.pnl for t in tt)
            print(f"    {tf:>2}m: {len(tt)} trades, {wins}W, "
                  f"WR={wins/len(tt)*100:.1f}%, P&L=${pnl:+.2f}")

        avg_fill = sum(t.fill_px for t in all_trades) / len(all_trades)
        print(f"\n  Avg fill price: {avg_fill:.3f}")

    return cum_pnl, all_trades


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

    # Parse scanner log
    print(f"Parsing {log_path} for book snapshots...")
    book_data = parse_scanner_log(str(log_path))
    slugs_with_books = sum(1 for v in book_data.values() if v)
    print(f"Found book data for {slugs_with_books} slugs")

    # Settlement overview
    by_tf = defaultdict(list)
    for s in settlements:
        by_tf[s["tf"]].append(s)

    print(f"\nSettlements by timeframe:")
    for tf, items in sorted(by_tf.items()):
        yes = sum(1 for i in items if i["outcome"] == "YES")
        no = sum(1 for i in items if i["outcome"] == "NO")
        avg = sum(abs(i["pct_move"]) for i in items) / len(items)
        print(f"  {tf}m: {len(items)} windows (YES={yes}, NO={no}, avg_move={avg:.4f}%)")

    stale = [s for s in settlements if s["pct_move"] == 0.0]
    if stale:
        print(f"\n  WARNING: {len(stale)} windows had 0% CL move (stale feed, excluded from sim)")

    # ── Scenario 1: ORIGINAL ────────────────────────────────────────────────

    pnl1, trades1 = run_scenario(
        "ORIGINAL (B=0.15%, E=0.95-0.975)",
        make_engines_original(),
        settlements,
        book_data,
        realistic=False,
    )

    # ── Scenario 2: TUNED ───────────────────────────────────────────────────

    pnl2, trades2 = run_scenario(
        "TUNED (B=0.12%, E=0.93-0.98)",
        make_engines_tuned(),
        settlements,
        book_data,
        realistic=False,
    )

    # ── Scenario 3: REALISTIC ───────────────────────────────────────────────

    pnl3, trades3 = run_scenario(
        "REALISTIC (Tuned + 60% maker fill + taker slip + fees)",
        make_engines_tuned(),
        settlements,
        book_data,
        realistic=True,
    )

    # ── Comparison ──────────────────────────────────────────────────────────

    print(f"\n{'=' * 72}")
    print("  SCENARIO COMPARISON")
    print("=" * 72)
    print(f"\n  {'Scenario':<50} {'Trades':>6} {'WR':>7} {'P&L':>10}")
    print("  " + "-" * 68)

    for label, trades, pnl in [
        ("1. ORIGINAL (B=0.15%, E=0.95-0.975)", trades1, pnl1),
        ("2. TUNED (B=0.12%, E=0.93-0.98)", trades2, pnl2),
        ("3. REALISTIC (Tuned + fills + fees)", trades3, pnl3),
    ]:
        n = len(trades)
        w = sum(1 for t in trades if t.exit_reason == "WIN")
        wr = w / n * 100 if n > 0 else 0
        print(f"  {label:<50} {n:>6} {wr:>6.1f}% ${pnl:>+8.2f}")

    # Delta from original
    if pnl1 != 0:
        print(f"\n  Tuned vs Original:    +{len(trades2)-len(trades1)} trades, "
              f"${pnl2-pnl1:+.2f} P&L delta")
        print(f"  Realistic vs Original: +{len(trades3)-len(trades1)} trades, "
              f"${pnl3-pnl1:+.2f} P&L delta")

    # Realistic fill breakdown
    maker_count = sum(1 for t in trades3 if t.fill_type == "maker")
    taker_count = sum(1 for t in trades3 if t.fill_type == "taker")
    total_fees = sum(t.entry_fee for t in trades3)
    print(f"\n  Realistic fill breakdown:")
    print(f"    Maker fills: {maker_count} ({maker_count/len(trades3)*100:.0f}%)" if trades3 else "")
    print(f"    Taker fills: {taker_count} ({taker_count/len(trades3)*100:.0f}%)" if trades3 else "")
    print(f"    Total taker fees paid: ${total_fees:.4f}")

    # Theoretical max
    non_stale = [s for s in settlements if s["pct_move"] != 0]
    max_theoretical = sum((STAKE / 0.92 * 1.0 - STAKE) for _ in non_stale)
    print(f"\n  Theoretical max (all {len(non_stale)} non-stale windows @0.92): "
          f"${max_theoretical:+.2f}")
    print(f"  Realistic capture rate: {pnl3/max_theoretical*100:.1f}%")

    # ── CL/BN divergence ────────────────────────────────────────────────────

    div_values = [s.get("cl_bn_divergence", 0) for s in settlements if s.get("cl_bn_divergence")]
    if div_values:
        avg_div = sum(div_values) / len(div_values)
        max_div = max(div_values)
        print(f"\n  CL/BN divergence: avg={avg_div:.6f}, max={max_div:.6f}")
        high_div = [s for s in settlements if s.get("cl_bn_divergence", 0) > 0.001]
        if high_div:
            print(f"  {len(high_div)} windows with CL/BN divergence > 0.1%:")
            for s in high_div:
                print(f"    {s['slug']} | div={s['cl_bn_divergence']:.4f} "
                      f"| pct_move={s['pct_move']:.4f}%")

    # ── Save results ────────────────────────────────────────────────────────

    results_path = Path("forward_test_results.jsonl")
    with open(results_path, 'w') as f:
        for scenario, trades in [
            ("ORIGINAL", trades1), ("TUNED", trades2), ("REALISTIC", trades3)
        ]:
            for t in trades:
                f.write(json.dumps({
                    "scenario": scenario,
                    "engine": t.engine,
                    "slug": t.slug,
                    "asset": t.asset,
                    "tf": t.tf,
                    "direction": t.direction,
                    "fill_px": t.fill_px,
                    "fill_type": t.fill_type,
                    "shares": t.shares,
                    "entry_fee": t.entry_fee,
                    "outcome": t.outcome,
                    "pnl": t.pnl,
                    "exit_reason": t.exit_reason,
                }) + "\n")

    print(f"\n  All results saved to {results_path}")


if __name__ == "__main__":
    main()
