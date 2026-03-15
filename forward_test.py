#!/usr/bin/env python3
"""
CL Sniper Forward Test — Full Production Config on 15-Mar Data

Replays the exact production system (all 5 engines, exact parameters from
cl-sniper-strategy-final.md / build-sniper-10mar.md) against captured data.

Data sources:
  - settlements_2026-03-15.jsonl: 69 settlement outcomes
  - scanner.log: ~765 book snapshots (CL + BN + book prices, ~60s cadence)

Engine config (production):
  A: 5m sniper,  δ≥0.04%, cont=4, entry [0.88,0.98], T-57..44
  B: 5m D1,      δ≥0.15%, cont=0, entry [0.88,0.98], T-57..44
  C: 15m sniper, δ≥0.04%, cont=4, entry [0.88,0.98], T-57..44
  D: 15m D1,     δ≥0.15%, cont=0, entry [0.88,0.98], T-57..44
  E: late scalp, book 0.95-0.975, T-25..3

Limitations with 60s-cadence scanner data:
  - Continuity (500ms ticks) cannot be measured → simulate with delta persistence
  - Late scalper (T-25..3) has no snapshots → skip Engine E
  - BN contra / CL fade filters need 10-15s trend → approximate from consecutive snaps
  - One snapshot per entry window (sometimes 2) → use what we have

The test uses ENTRY-TIME direction (not final outcome) to simulate what the
bot would actually see and decide at trade time.
"""

import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# ── Constants ─────────────────────────────────────────────────────────────────

STAKE = 5.0
SLIP = 0.005
MAKER_DISCOUNT = 0.01  # maker posts at ask - 0.01

STDEV = {"btc": 0.167, "eth": 0.194, "sol": 0.247, "xrp": 0.440}
STDEV_BASE = 0.167

MIN_DELTA_FLOOR = {"btc": 0.015, "eth": 0.020, "sol": 0.030, "xrp": 0.050}


def stdev_scale(asset):
    return STDEV.get(asset, STDEV_BASE) / STDEV_BASE


def pm_fee(px):
    """Polymarket taker fee: px * (1-px) * 0.0625"""
    return px * (1.0 - px) * 0.0625


# ── Engine Config ─────────────────────────────────────────────────────────────

@dataclass
class EngineConfig:
    id: str
    name: str
    tf: int              # 5 or 15 (0 = both, for E)
    delta: float         # min delta threshold (base, before stdev scaling)
    continuity: int      # ticks of sustained delta (4 = 2s in production)
    min_entry: float     # min ask to enter
    max_entry: float     # max ask to enter
    entry_start: int     # secs_left earliest (57)
    entry_end: int       # secs_left latest (44)
    bn_contra: bool      # BN trend filter
    cl_fade: bool        # CL trend filter
    regime_check: bool   # hourly vol filter
    is_late_scalper: bool = False


PRODUCTION_ENGINES = [
    EngineConfig("A", "5M_SNIPER",    5,  0.04, 4, 0.88, 0.98, 57, 44, True, True, True),
    EngineConfig("B", "5M_D1",        5,  0.15, 0, 0.88, 0.98, 57, 44, True, True, True),
    EngineConfig("C", "15M_SNIPER",  15,  0.04, 4, 0.88, 0.98, 57, 44, True, True, True),
    EngineConfig("D", "15M_D1",      15,  0.15, 0, 0.88, 0.98, 57, 44, True, True, True),
    EngineConfig("E", "LATE_SCALPER", 0,  0.00, 0, 0.95, 0.975, 25, 3, False, False, False,
                 is_late_scalper=True),
]


# ── Parse scanner.log ─────────────────────────────────────────────────────────

@dataclass
class Snapshot:
    slug: str
    asset: str
    tf: int
    secs_left: int
    cl_open: float
    cl_now: float
    bn_now: float
    delta_pct: float
    sigma_pct: float
    fy: float
    yes_bid: float
    yes_ask: float
    no_bid: float
    no_ask: float
    timestamp: str = ""


def parse_scanner_log(log_path):
    """Parse scanner.log into per-slug snapshot lists."""
    snapshots = defaultdict(list)

    # Strip ANSI escape codes
    ansi_re = re.compile(r'\x1b\[[0-9;]*m')

    scan_re = re.compile(
        r'(\d{4}-\d{2}-\d{2}T[\d:.]+Z)\s+\w+\s+'
        r'(\S+-updown-(\d+)m-\d+)\s+\|'
        r'\s+open=([\d.]+)\s+cl=([\d.]+)\s+bn=([\d.]+)'
        r'\s+d=([+-]?[\d.]+)%'
        r'\s+\|\s+s=([\d.]+)%\s+fy=([\d.]+)'
        r'\s+\|\s+YES=([\d.-]+)/([\d.-]+)\s+NO=([\d.-]+)/([\d.-]+)'
        r'\s+\|\s+T-(\d+)s'
    )

    with open(log_path) as f:
        for line in f:
            line = ansi_re.sub('', line)
            m = scan_re.search(line)
            if not m:
                continue

            def px(s):
                try:
                    return float(s)
                except:
                    return 0.0

            slug = m.group(2)
            asset = slug.split("-")[0]
            tf = int(m.group(3))

            snap = Snapshot(
                slug=slug, asset=asset, tf=tf,
                secs_left=int(m.group(14)),
                cl_open=float(m.group(4)),
                cl_now=float(m.group(5)),
                bn_now=float(m.group(6)),
                delta_pct=float(m.group(7)),
                sigma_pct=float(m.group(8)),
                fy=float(m.group(9)),
                yes_bid=px(m.group(10)),
                yes_ask=px(m.group(11)),
                no_bid=px(m.group(12)),
                no_ask=px(m.group(13)),
                timestamp=m.group(1),
            )
            snapshots[slug].append(snap)

    # Sort each slug's snapshots by secs_left descending (earlier → later)
    for slug in snapshots:
        snapshots[slug].sort(key=lambda s: -s.secs_left)

    return snapshots


# ── Parse settlements ─────────────────────────────────────────────────────────

@dataclass
class Settlement:
    slug: str
    asset: str
    tf: int
    pct_move: float
    outcome: str  # YES or NO
    cl_open: float
    cl_close: float
    bn_close: float
    window_start: int
    window_end: int


def parse_settlements(path):
    settlements = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            settlements.append(Settlement(
                slug=d["slug"],
                asset=d["asset"],
                tf=d["tf"],
                pct_move=d["pct_move"],
                outcome=d["outcome"],
                cl_open=d["open_price"],
                cl_close=d["cl_close"],
                bn_close=d["bn_close"],
                window_start=d["window_start"],
                window_end=d["window_end"],
            ))
    return settlements


# ── Trade Decision Engine ─────────────────────────────────────────────────────

@dataclass
class Trade:
    engine_id: str
    engine_name: str
    slug: str
    asset: str
    tf: int
    direction: str       # UP or DOWN at entry time
    fill_px: float
    fill_method: str     # maker or taker
    shares: float
    fee: float
    outcome: str         # YES or NO (settlement)
    won: bool
    pnl: float           # net P&L including fees
    delta_at_entry: float
    ask_at_entry: float
    secs_left: int
    reason: str = ""     # why this trade fired


@dataclass
class Skip:
    engine_id: str
    slug: str
    reason: str
    delta: float = 0.0
    ask: float = 0.0
    secs_left: int = 0


def evaluate_engine(engine: EngineConfig, settlement: Settlement,
                    snaps: list, all_snaps_map: dict = None) -> tuple:
    """Evaluate whether an engine would trade this window, and the result."""

    skips = []
    slug = settlement.slug
    asset = settlement.asset

    # ── Timeframe filter ──────────────────────────────────────────────────
    if engine.tf != 0 and settlement.tf != engine.tf:
        return None, []

    # ── Get snapshots in entry window ─────────────────────────────────────
    entry_snaps = [s for s in snaps if engine.entry_end <= s.secs_left <= engine.entry_start]

    if not entry_snaps:
        skips.append(Skip(engine.id, slug, "NO_ENTRY_SNAP"))
        return None, skips

    # ── Late scalper: pure book-price engine ──────────────────────────────
    if engine.is_late_scalper:
        # E enters based on which side has ask ≥ 0.95
        for snap in entry_snaps:
            if snap.yes_ask >= 0.95 and snap.yes_ask <= engine.max_entry:
                direction = "UP"
                ask = snap.yes_ask
            elif snap.no_ask >= 0.95 and snap.no_ask <= engine.max_entry:
                direction = "DOWN"
                ask = snap.no_ask
            else:
                skips.append(Skip(engine.id, slug,
                    f"E_NO_SIDE_0.95 yes_ask={snap.yes_ask:.3f} no_ask={snap.no_ask:.3f}",
                    secs_left=snap.secs_left))
                continue

            # Fill
            maker_px = max(round((ask - MAKER_DISCOUNT) * 100) / 100, engine.min_entry)
            fill_px = maker_px
            fill_method = "maker"

            if fill_px < engine.min_entry or fill_px > engine.max_entry:
                skips.append(Skip(engine.id, slug,
                    f"E_PRICE_OOB fill={fill_px:.3f}", ask=ask))
                continue

            shares = STAKE / fill_px
            won = (direction == "UP" and settlement.outcome == "YES") or \
                  (direction == "DOWN" and settlement.outcome == "NO")

            if won:
                pnl = shares * 1.0 - STAKE  # settle at $1
            else:
                sl_px = fill_px * 0.50
                recovery = shares * max(sl_px - SLIP, 0)
                pnl = recovery - STAKE

            return Trade(
                engine_id=engine.id, engine_name=engine.name,
                slug=slug, asset=asset, tf=settlement.tf,
                direction=direction, fill_px=fill_px,
                fill_method=fill_method, shares=shares, fee=0.0,
                outcome=settlement.outcome, won=won, pnl=pnl,
                delta_at_entry=abs(snap.delta_pct),
                ask_at_entry=ask, secs_left=snap.secs_left,
                reason=f"E: {direction} ask={ask:.3f}"
            ), skips

        return None, skips

    # ── Delta-based engines (A-D) ─────────────────────────────────────────

    # Stdev-scaled threshold
    scaled_delta = engine.delta * stdev_scale(asset)
    min_delta = MIN_DELTA_FLOOR.get(asset, 0.015)

    # Find best qualifying snapshot in entry window
    for snap in sorted(entry_snaps, key=lambda s: -abs(s.delta_pct)):
        abs_delta = abs(snap.delta_pct)

        # Delta threshold check
        if abs_delta < scaled_delta:
            skips.append(Skip(engine.id, slug,
                f"DELTA_LOW |d|={abs_delta:.4f}% < thresh={scaled_delta:.4f}%",
                delta=abs_delta, secs_left=snap.secs_left))
            continue

        # Min delta floor (noise filter)
        if abs_delta < min_delta:
            skips.append(Skip(engine.id, slug,
                f"DELTA_NOISE |d|={abs_delta:.4f}% < floor={min_delta:.3f}%",
                delta=abs_delta, secs_left=snap.secs_left))
            continue

        # Direction from CL delta at entry time
        direction = "UP" if snap.delta_pct > 0 else "DOWN"

        # Book price for our direction
        if direction == "UP":
            ask = snap.yes_ask
            our_bid = snap.yes_bid
        else:
            ask = snap.no_ask
            our_bid = snap.no_bid

        if ask <= 0:
            skips.append(Skip(engine.id, slug,
                f"NO_ASK dir={direction}", delta=abs_delta, secs_left=snap.secs_left))
            continue

        # Entry price range
        if ask < engine.min_entry:
            skips.append(Skip(engine.id, slug,
                f"ASK_LOW ask={ask:.3f} < min={engine.min_entry:.2f}",
                delta=abs_delta, ask=ask, secs_left=snap.secs_left))
            continue

        if ask > engine.max_entry:
            skips.append(Skip(engine.id, slug,
                f"ASK_HIGH ask={ask:.3f} > max={engine.max_entry:.2f}",
                delta=abs_delta, ask=ask, secs_left=snap.secs_left))
            continue

        # ── Continuity check (approximate) ────────────────────────────────
        if engine.continuity > 0:
            # With 60s snapshots we can't measure 500ms continuity.
            # Check if direction is consistent across available entry snaps.
            # This is WEAKER than production (4 × 500ms ticks).
            same_dir_count = sum(
                1 for s in entry_snaps
                if (s.delta_pct > 0) == (snap.delta_pct > 0)
                and abs(s.delta_pct) >= scaled_delta
            )
            if same_dir_count < 1:
                skips.append(Skip(engine.id, slug,
                    f"CONTINUITY_FAIL dir_snaps={same_dir_count}",
                    delta=abs_delta, secs_left=snap.secs_left))
                continue

        # ── BN contra filter ──────────────────────────────────────────────
        if engine.bn_contra:
            # Approximate: BN vs CL-open direction
            bn_vs_cl_open = (snap.bn_now - snap.cl_open) / snap.cl_open * 100
            if direction == "UP" and bn_vs_cl_open < -0.02:
                skips.append(Skip(engine.id, slug,
                    f"BN_CONTRA UP but bn_trend={bn_vs_cl_open:+.4f}%",
                    delta=abs_delta, secs_left=snap.secs_left))
                continue
            if direction == "DOWN" and bn_vs_cl_open > 0.02:
                skips.append(Skip(engine.id, slug,
                    f"BN_CONTRA DOWN but bn_trend={bn_vs_cl_open:+.4f}%",
                    delta=abs_delta, secs_left=snap.secs_left))
                continue

        # ── CL fade filter ────────────────────────────────────────────────
        if engine.cl_fade:
            # Need 10s CL trend — approximate from previous snapshot
            prev_snaps = [s for s in snaps
                          if s.secs_left > snap.secs_left
                          and s.secs_left <= snap.secs_left + 120]
            if prev_snaps:
                prev = prev_snaps[-1]  # closest previous snap
                cl_trend = (snap.cl_now - prev.cl_now) / prev.cl_now * 100
                if direction == "UP" and cl_trend < -0.03:
                    skips.append(Skip(engine.id, slug,
                        f"CL_FADE UP but cl_trend={cl_trend:+.4f}%",
                        delta=abs_delta, secs_left=snap.secs_left))
                    continue
                if direction == "DOWN" and cl_trend > 0.03:
                    skips.append(Skip(engine.id, slug,
                        f"CL_FADE DOWN but cl_trend={cl_trend:+.4f}%",
                        delta=abs_delta, secs_left=snap.secs_left))
                    continue

        # ── Regime check ──────────────────────────────────────────────────
        if engine.regime_check:
            if snap.sigma_pct < 0.3:
                skips.append(Skip(engine.id, slug,
                    f"REGIME_LOW sigma={snap.sigma_pct:.1f}% < 0.3%",
                    delta=abs_delta, secs_left=snap.secs_left))
                continue

        # ── ENTRY SIGNAL QUALIFIES ────────────────────────────────────────

        # Fill price: maker at ask - 0.01, clamped to min_entry
        maker_px = max(round((ask - MAKER_DISCOUNT) * 100) / 100, engine.min_entry)

        # 60% maker fill probability, 40% taker (deterministic by slug+engine)
        h = hash((slug, engine.id)) & 0xFFFFFFFF
        roll = (h % 100) / 100.0

        if roll < 0.60:
            fill_px = maker_px
            fill_method = "maker"
            fee = 0.0  # maker rebate
        else:
            fill_px = min(ask + SLIP, 0.99)
            fill_method = "taker"
            if fill_px > engine.max_entry:
                skips.append(Skip(engine.id, slug,
                    f"TAKER_OOB fill={fill_px:.3f}", delta=abs_delta, ask=ask))
                continue
            fee = pm_fee(fill_px) * (STAKE / fill_px)

        shares = STAKE / fill_px

        # ── Settlement outcome ────────────────────────────────────────────
        won = (direction == "UP" and settlement.outcome == "YES") or \
              (direction == "DOWN" and settlement.outcome == "NO")

        if won:
            pnl = (shares * 1.0) - STAKE - fee  # settle at $1 per share
        else:
            # LOSS: SL at 50% of entry (with SL fix, only on true flip)
            sl_px = fill_px * 0.50
            recovery = shares * max(sl_px - SLIP, 0)
            pnl = recovery - STAKE

        return Trade(
            engine_id=engine.id, engine_name=engine.name,
            slug=slug, asset=asset, tf=settlement.tf,
            direction=direction, fill_px=fill_px,
            fill_method=fill_method, shares=shares, fee=fee,
            outcome=settlement.outcome, won=won, pnl=pnl,
            delta_at_entry=abs_delta,
            ask_at_entry=ask, secs_left=snap.secs_left,
            reason=f"{engine.id}: d={abs_delta:.4f}% {direction} ask={ask:.3f}"
        ), skips

    return None, skips


# ── Main Forward Test ─────────────────────────────────────────────────────────

def run_forward_test():
    settle_path = Path("settlements_2026-03-15.jsonl")
    log_path = Path("scanner.log")

    settlements = parse_settlements(settle_path)
    snapshots = parse_scanner_log(str(log_path))
    total_snaps = sum(len(v) for v in snapshots.values())

    print(f"{'='*80}")
    print(f"  CL SNIPER FORWARD TEST -- 15 March 2026 Data")
    print(f"{'='*80}")
    print(f"\n  Settlements: {len(settlements)}")
    print(f"  Book snapshots: {total_snaps} across {len(snapshots)} slugs")
    print(f"  Scanner cadence: ~60s (vs 500ms production)")
    print(f"  Stake: ${STAKE:.2f} per trade")

    # ── Run each engine across all settlements ────────────────────────────

    all_trades = []
    all_skips = []
    engine_stats = {}

    for engine in PRODUCTION_ENGINES:
        trades = []
        skips = []
        traded_slugs = set()

        for settle in settlements:
            if settle.slug in traded_slugs:
                continue

            snaps = snapshots.get(settle.slug, [])
            trade, skip_list = evaluate_engine(engine, settle, snaps)
            skips.extend(skip_list)

            if trade:
                trades.append(trade)
                traded_slugs.add(settle.slug)

        wins = sum(1 for t in trades if t.won)
        losses = sum(1 for t in trades if not t.won)
        total_pnl = sum(t.pnl for t in trades)
        maker_count = sum(1 for t in trades if t.fill_method == "maker")
        taker_count = sum(1 for t in trades if t.fill_method == "taker")

        engine_stats[engine.id] = {
            "name": engine.name,
            "trades": len(trades),
            "wins": wins,
            "losses": losses,
            "wr": f"{wins/len(trades)*100:.1f}%" if trades else "N/A",
            "pnl": total_pnl,
            "maker": maker_count,
            "taker": taker_count,
        }

        all_trades.extend(trades)
        all_skips.extend(skips)

    # ── Engine Summary ────────────────────────────────────────────────────

    print(f"\n{'='*80}")
    print(f"  ENGINE RESULTS (Production Config)")
    print(f"{'='*80}")

    print(f"\n  {'Engine':<12} {'Trades':>6} {'W':>4} {'L':>4} {'WR':>7} "
          f"{'P&L':>10} {'M/T':>7}")
    print(f"  {'-'*56}")

    total_trades = 0
    total_wins = 0
    total_losses = 0
    total_pnl = 0.0

    for eid in ["A", "B", "C", "D", "E"]:
        s = engine_stats[eid]
        print(f"  {eid} {s['name']:<10} {s['trades']:>4} {s['wins']:>4} "
              f"{s['losses']:>4} {s['wr']:>7} ${s['pnl']:>+8.2f} "
              f"{s['maker']}M/{s['taker']}T")
        total_trades += s['trades']
        total_wins += s['wins']
        total_losses += s['losses']
        total_pnl += s['pnl']

    wr = f"{total_wins/total_trades*100:.1f}%" if total_trades else "N/A"
    print(f"  {'-'*56}")
    print(f"  {'TOTAL':<12} {total_trades:>4} {total_wins:>4} "
          f"{total_losses:>4} {wr:>7} ${total_pnl:>+8.2f}")

    # ── Trade Details ─────────────────────────────────────────────────────

    print(f"\n{'='*80}")
    print(f"  TRADE LOG")
    print(f"{'='*80}")

    if not all_trades:
        print(f"\n  No trades fired with production config!")
    else:
        cum_pnl = 0.0
        for t in sorted(all_trades, key=lambda x: x.slug):
            cum_pnl += t.pnl
            status = "WIN " if t.won else "LOSS"
            print(f"  [{t.engine_id}] {t.asset:>3} {t.tf:>2}m {t.direction:>4} "
                  f"@{t.fill_px:.3f}({t.fill_method[0]}) d={t.delta_at_entry:.4f}% "
                  f"ask={t.ask_at_entry:.3f} T-{t.secs_left}s "
                  f"-> {status} ${t.pnl:>+.4f} [cum ${cum_pnl:>+.2f}]"
                  f"  {t.slug}")

    # ── Skip Analysis ─────────────────────────────────────────────────────

    print(f"\n{'='*80}")
    print(f"  SKIP ANALYSIS (why engines didn't fire)")
    print(f"{'='*80}")

    skip_reasons = defaultdict(lambda: defaultdict(int))
    for skip in all_skips:
        reason_type = skip.reason.split(" ")[0] if " " in skip.reason else skip.reason
        skip_reasons[skip.engine_id][reason_type] += 1

    for eid in ["A", "B", "C", "D", "E"]:
        reasons = skip_reasons.get(eid, {})
        if reasons:
            print(f"\n  Engine {eid}:")
            for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
                print(f"    {reason}: {count}")

    # ── Near-miss analysis ────────────────────────────────────────────────

    print(f"\n{'='*80}")
    print(f"  NEAR MISSES (delta close to threshold but didn't qualify)")
    print(f"{'='*80}")

    traded_combos = {(t.engine_id, t.slug) for t in all_trades}

    for engine in PRODUCTION_ENGINES:
        if engine.is_late_scalper:
            continue

        near_misses = []
        for settle in settlements:
            if (engine.id, settle.slug) in traded_combos:
                continue
            if engine.tf != 0 and settle.tf != engine.tf:
                continue

            snaps = snapshots.get(settle.slug, [])
            entry_snaps = [s for s in snaps
                           if engine.entry_end <= s.secs_left <= engine.entry_start]

            for snap in entry_snaps:
                abs_d = abs(snap.delta_pct)
                scaled = engine.delta * stdev_scale(settle.asset)
                if abs_d > 0 and abs_d < scaled and abs_d >= scaled * 0.3:
                    direction = "UP" if snap.delta_pct > 0 else "DOWN"
                    would_win = (direction == "UP" and settle.outcome == "YES") or \
                               (direction == "DOWN" and settle.outcome == "NO")

                    if direction == "UP":
                        ask = snap.yes_ask
                    else:
                        ask = snap.no_ask

                    near_misses.append({
                        "slug": settle.slug,
                        "delta": abs_d,
                        "threshold": scaled,
                        "gap": scaled - abs_d,
                        "gap_pct": (scaled - abs_d) / scaled * 100,
                        "direction": direction,
                        "ask": ask,
                        "would_win": would_win,
                        "secs_left": snap.secs_left,
                    })

        if near_misses:
            near_misses.sort(key=lambda x: x["gap"])
            print(f"\n  Engine {engine.id} ({engine.name}, d>={engine.delta:.2f}%):")
            for nm in near_misses[:10]:
                win_str = "-> win" if nm["would_win"] else "-> LOSS"
                print(f"    d={nm['delta']:.4f}% (need {nm['threshold']:.4f}%, "
                      f"gap={nm['gap']:.4f}%, {nm['gap_pct']:.0f}% short) "
                      f"{nm['direction']} ask={nm['ask']:.3f} T-{nm['secs_left']}s "
                      f"{win_str}  {nm['slug']}")

    # ── What-if scenarios ─────────────────────────────────────────────────

    scenarios = [
        ("RELAXED",    "A/C=0.03%, B/D=0.08%", [
            EngineConfig("A*", "5M_SNP*",   5,  0.03, 4, 0.88, 0.98, 57, 44, True, True, True),
            EngineConfig("B*", "5M_D1*",    5,  0.08, 0, 0.88, 0.98, 57, 44, True, True, True),
            EngineConfig("C*", "15M_SNP*", 15,  0.03, 4, 0.88, 0.98, 57, 44, True, True, True),
            EngineConfig("D*", "15M_D1*",  15,  0.08, 0, 0.88, 0.98, 57, 44, True, True, True),
        ]),
        ("MODERATE",   "A/C=0.03%, B/D=0.04%", [
            EngineConfig("A+", "5M_SNP+",   5,  0.03, 4, 0.88, 0.98, 57, 44, True, True, True),
            EngineConfig("B+", "5M_D1+",    5,  0.04, 0, 0.88, 0.98, 57, 44, True, True, True),
            EngineConfig("C+", "15M_SNP+", 15,  0.03, 4, 0.88, 0.98, 57, 44, True, True, True),
            EngineConfig("D+", "15M_D1+",  15,  0.04, 0, 0.88, 0.98, 57, 44, True, True, True),
        ]),
        ("AGGRESSIVE", "B/D=0.04%, entry 0.85-0.99", [
            EngineConfig("A!", "5M_SNP!",   5,  0.03, 4, 0.85, 0.99, 57, 44, True, True, True),
            EngineConfig("B!", "5M_D1!",    5,  0.04, 0, 0.85, 0.99, 57, 44, True, True, True),
            EngineConfig("C!", "15M_SNP!", 15,  0.03, 4, 0.85, 0.99, 57, 44, True, True, True),
            EngineConfig("D!", "15M_D1!",  15,  0.04, 0, 0.85, 0.99, 57, 44, True, True, True),
        ]),
        ("NO REGIME",  "Production + regime_check=off", [
            EngineConfig("An", "5M_SNP_NR",   5,  0.04, 4, 0.88, 0.98, 57, 44, True, True, False),
            EngineConfig("Bn", "5M_D1_NR",    5,  0.15, 0, 0.88, 0.98, 57, 44, True, True, False),
            EngineConfig("Cn", "15M_SNP_NR", 15,  0.04, 4, 0.88, 0.98, 57, 44, True, True, False),
            EngineConfig("Dn", "15M_D1_NR",  15,  0.15, 0, 0.88, 0.98, 57, 44, True, True, False),
        ]),
    ]

    print(f"\n{'='*80}")
    print(f"  WHAT-IF SCENARIOS")
    print(f"{'='*80}")

    scenario_results = {}

    for name, desc, engines in scenarios:
        trades = []
        for engine in engines:
            traded_slugs = set()
            for settle in settlements:
                if settle.slug in traded_slugs:
                    continue
                snaps = snapshots.get(settle.slug, [])
                trade, _ = evaluate_engine(engine, settle, snaps)
                if trade:
                    trades.append(trade)
                    traded_slugs.add(settle.slug)

        w = sum(1 for t in trades if t.won)
        l = sum(1 for t in trades if not t.won)
        pnl = sum(t.pnl for t in trades)
        wr_s = f"{w/(w+l)*100:.1f}%" if (w+l) > 0 else "N/A"

        scenario_results[name] = {"trades": trades, "wins": w, "losses": l, "pnl": pnl}

        print(f"\n  {name} ({desc}):")
        print(f"    Trades: {w+l}, W={w}, L={l}, WR={wr_s}, P&L=${pnl:+.2f}")

        if trades:
            for t in sorted(trades, key=lambda x: x.slug):
                status = "WIN " if t.won else "LOSS"
                print(f"    [{t.engine_id}] {t.asset:>3} {t.tf:>2}m {t.direction:>4} "
                      f"@{t.fill_px:.3f}({t.fill_method[0]}) d={t.delta_at_entry:.4f}% "
                      f"-> {status} ${t.pnl:>+.4f}  {t.slug}")

    # ── Market regime analysis ────────────────────────────────────────────

    print(f"\n{'='*80}")
    print(f"  MARKET REGIME ANALYSIS")
    print(f"{'='*80}")

    from datetime import datetime, timezone

    by_hour = defaultdict(list)
    for s in settlements:
        hour = (s.window_start // 3600) * 3600
        by_hour[hour].append(s)

    print(f"\n  Settlements per hour:")
    for hour in sorted(by_hour.keys()):
        settles = by_hour[hour]
        avg_move = sum(abs(s.pct_move) for s in settles) / len(settles)
        max_move = max(abs(s.pct_move) for s in settles)
        up = sum(1 for s in settles if s.outcome == "YES")
        down = sum(1 for s in settles if s.outcome == "NO")
        dt = datetime.fromtimestamp(hour, tz=timezone.utc)
        print(f"    {dt.strftime('%H:%M')} UTC: {len(settles):>2} windows, "
              f"avg|move|={avg_move:.4f}%, max={max_move:.4f}%, "
              f"UP={up}/DOWN={down}")

    # CL delta distribution at entry time
    print(f"\n  CL Delta at entry (T-44..57) across all windows:")
    entry_deltas = []
    for settle in settlements:
        snaps = snapshots.get(settle.slug, [])
        entry_snaps = [s for s in snaps if 44 <= s.secs_left <= 57]
        for s in entry_snaps:
            entry_deltas.append(abs(s.delta_pct))

    if entry_deltas:
        entry_deltas.sort()
        n = len(entry_deltas)
        print(f"    Count: {n}")
        print(f"    Min:   {min(entry_deltas):.4f}%")
        print(f"    P25:   {entry_deltas[n//4]:.4f}%")
        print(f"    P50:   {entry_deltas[n//2]:.4f}%")
        print(f"    P75:   {entry_deltas[3*n//4]:.4f}%")
        print(f"    Max:   {max(entry_deltas):.4f}%")
        above_004 = sum(1 for d in entry_deltas if d > 0.04)
        above_008 = sum(1 for d in entry_deltas if d > 0.08)
        above_015 = sum(1 for d in entry_deltas if d > 0.15)
        print(f"    >0.04%: {above_004}/{n} ({above_004/n*100:.0f}%)")
        print(f"    >0.08%: {above_008}/{n} ({above_008/n*100:.0f}%)")
        print(f"    >0.15%: {above_015}/{n} ({above_015/n*100:.0f}%)")

    # Sigma distribution
    print(f"\n  Sigma (hourly vol) at entry:")
    entry_sigmas = []
    for settle in settlements:
        snaps = snapshots.get(settle.slug, [])
        entry_snaps = [s for s in snaps if 44 <= s.secs_left <= 57]
        for s in entry_snaps:
            entry_sigmas.append(s.sigma_pct)

    if entry_sigmas:
        entry_sigmas.sort()
        n = len(entry_sigmas)
        below_03 = sum(1 for s in entry_sigmas if s < 0.3)
        print(f"    Count: {n}")
        print(f"    Min:   {min(entry_sigmas):.2f}%")
        print(f"    P50:   {entry_sigmas[n//2]:.2f}%")
        print(f"    Max:   {max(entry_sigmas):.2f}%")
        print(f"    <0.3% (regime filtered): {below_03}/{n} ({below_03/n*100:.0f}%)")

    # ── Final summary ─────────────────────────────────────────────────────

    print(f"\n{'='*80}")
    print(f"  FORWARD TEST SUMMARY")
    print(f"{'='*80}")

    sr = scenario_results
    print(f"""
  SESSION: 15-Mar-2026 (~6 hours captured data)

  +{'─'*60}+
  | Config              | Trades |  W |  L |  WR   |    P&L   |
  +{'─'*60}+
  | PRODUCTION          | {total_trades:>5}  | {total_wins:>2} | {total_losses:>2} | {wr:>5} | ${total_pnl:>+7.2f} |
  | RELAXED (0.03/0.08) | {sr['RELAXED']['wins']+sr['RELAXED']['losses']:>5}  | {sr['RELAXED']['wins']:>2} | {sr['RELAXED']['losses']:>2} | {sr['RELAXED']['wins']/(sr['RELAXED']['wins']+sr['RELAXED']['losses'])*100 if sr['RELAXED']['wins']+sr['RELAXED']['losses'] > 0 else 0:>4.0f}% | ${sr['RELAXED']['pnl']:>+7.2f} |
  | MODERATE (0.03/0.04)| {sr['MODERATE']['wins']+sr['MODERATE']['losses']:>5}  | {sr['MODERATE']['wins']:>2} | {sr['MODERATE']['losses']:>2} | {sr['MODERATE']['wins']/(sr['MODERATE']['wins']+sr['MODERATE']['losses'])*100 if sr['MODERATE']['wins']+sr['MODERATE']['losses'] > 0 else 0:>4.0f}% | ${sr['MODERATE']['pnl']:>+7.2f} |
  | AGGRESSIVE (0.03/04)| {sr['AGGRESSIVE']['wins']+sr['AGGRESSIVE']['losses']:>5}  | {sr['AGGRESSIVE']['wins']:>2} | {sr['AGGRESSIVE']['losses']:>2} | {sr['AGGRESSIVE']['wins']/(sr['AGGRESSIVE']['wins']+sr['AGGRESSIVE']['losses'])*100 if sr['AGGRESSIVE']['wins']+sr['AGGRESSIVE']['losses'] > 0 else 0:>4.0f}% | ${sr['AGGRESSIVE']['pnl']:>+7.2f} |
  | NO REGIME           | {sr['NO REGIME']['wins']+sr['NO REGIME']['losses']:>5}  | {sr['NO REGIME']['wins']:>2} | {sr['NO REGIME']['losses']:>2} | {sr['NO REGIME']['wins']/(sr['NO REGIME']['wins']+sr['NO REGIME']['losses'])*100 if sr['NO REGIME']['wins']+sr['NO REGIME']['losses'] > 0 else 0:>4.0f}% | ${sr['NO REGIME']['pnl']:>+7.2f} |
  +{'─'*60}+

  DATA LIMITATIONS:
    - Scanner.log has ~60s cadence (production is 500ms)
    - Engine E (late scalper T-25..3) has ZERO snapshots in window
    - Continuity (A/C) approximated — real bot checks 500ms ticks
    - CL fade filter approximated from 60s-apart snapshots
    - Trade count is upper bound (missing filters reduce it)

  COMPARISON TO 10-MAR LIVE (5h, production, more volatile):
    10-Mar: 77 trades, 100% WR (SL fix), +$22.02
    15-Mar: {total_trades} trades, {wr} WR, ${total_pnl:+.2f}
""")

    # ── Save results ──────────────────────────────────────────────────────

    results = {
        "session": "2026-03-15",
        "data_hours": 6,
        "settlements": len(settlements),
        "snapshots": total_snaps,
        "scanner_cadence_ms": 60000,
        "production": {
            "config": {
                "A": {"delta": 0.04, "cont": 4, "entry": [0.88, 0.98]},
                "B": {"delta": 0.15, "cont": 0, "entry": [0.88, 0.98]},
                "C": {"delta": 0.04, "cont": 4, "entry": [0.88, 0.98]},
                "D": {"delta": 0.15, "cont": 0, "entry": [0.88, 0.98]},
                "E": {"min": 0.95, "max": 0.975, "window": "T-25..3"},
            },
            "trades": total_trades,
            "wins": total_wins,
            "losses": total_losses,
            "wr": wr,
            "pnl": round(total_pnl, 4),
        },
        "scenarios": {
            name: {
                "trades": sr[name]["wins"] + sr[name]["losses"],
                "wins": sr[name]["wins"],
                "losses": sr[name]["losses"],
                "pnl": round(sr[name]["pnl"], 4),
            }
            for name in sr
        },
        "engine_stats": {
            eid: {k: (round(v, 4) if isinstance(v, float) else v)
                  for k, v in stats.items()}
            for eid, stats in engine_stats.items()
        },
        "trade_log": [
            {
                "engine": t.engine_id, "slug": t.slug, "asset": t.asset,
                "tf": t.tf, "direction": t.direction,
                "fill_px": round(t.fill_px, 4), "fill_method": t.fill_method,
                "won": t.won, "pnl": round(t.pnl, 4),
                "delta": round(t.delta_at_entry, 4),
                "ask": round(t.ask_at_entry, 4),
                "secs_left": t.secs_left,
            }
            for t in all_trades
        ],
    }

    with open("forward_test_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"  Results saved to forward_test_results.json")

    return results


if __name__ == "__main__":
    run_forward_test()
