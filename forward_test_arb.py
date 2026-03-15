#!/usr/bin/env python3
"""Forward test: ARB-FINAL — Matched-Pair Arbitrage.

Buys matched YES + NO share sets on updown markets.
Settlement always pays $1.00 per pair. Profit = 1.00 - (avg_YES + avg_NO).
Coverage: 15m + 60m windows (BTC, ETH, SOL, XRP). Maker-only, taker if necessary.

Phases:
  Observing  — Warmup (120s for 15m, 300s for 60m)
  Active     — Main entry window (maker sequential: buy YES then NO)
  Lockdown   — Near window end, complete unmatched pairs only (allows taker)
  Settled    — Window expired, compute P&L
"""

import json
from collections import defaultdict
from datetime import datetime, timezone

signals = []
with open("/tmp/signals_lite_full.jsonl") as f:
    for line in f:
        if line.strip():
            signals.append(json.loads(line))
signals.sort(key=lambda s: s["ts"])

settlements = {}
with open("/home/user/Poly/settlements_2026-03-15.jsonl") as f:
    for line in f:
        if line.strip():
            s = json.loads(line)
            settlements[s["slug"]] = s

settle_by_end = {}
for slug, s in settlements.items():
    settle_by_end.setdefault(s["window_end"], []).append(s)

# ── Config ───────────────────────────────────────────────────────────────────

STAKE_PER_SIDE = 5.0    # $5 per side = $10 total per pair
SLIP = 0.005
MAKER_IMPROVE = 0.01    # Maker at ask - $0.01

# Phase timing (seconds left in window)
WARMUP_15M = 120        # Start observing at 120s left for 15m
WARMUP_60M = 300        # Start observing at 300s left for 60m
LOCKDOWN_SECS = 30      # Lockdown: last 30s — taker to complete pairs
ACTIVE_END = 45         # Stop new entries at 45s left

# Entry thresholds
MAX_PAIR_COST = 0.98    # Only enter if ask_yes + ask_no < this (guaranteed profit)
MAX_PAIR_COST_MAKER = 0.96  # Maker target: pair cost < 0.96 for better margin
MIN_SPREAD_PROFIT = 0.005   # Minimum per-share profit after fees

def pm_fee(px):
    """Polymarket fee: price * (1-price) * 0.0625"""
    return px * (1.0 - px) * 0.0625


class ArbEngine:
    def __init__(self, name, max_pair_cost, use_maker, use_taker_lockdown):
        self.name = name
        self.max_pair_cost = max_pair_cost
        self.use_maker = use_maker
        self.use_taker_lockdown = use_taker_lockdown

        self.pairs = {}       # slug -> pair state
        self.entered = set()  # slugs already entered
        self.wins = 0
        self.losses = 0
        self.pnl = 0.0
        self.peak = 0.0
        self.dd = 0.0
        self.total_pairs = 0
        self.trade_log = []
        self.settled_windows = set()

    def _update_dd(self):
        if self.pnl > self.peak:
            self.peak = self.pnl
        d = self.peak - self.pnl
        if d > self.dd:
            self.dd = d

    def _phase(self, secs, tf):
        """Determine phase based on seconds left and timeframe."""
        warmup = WARMUP_15M if tf == 15 else WARMUP_60M
        if secs > warmup:
            return "BEFORE"
        if secs > ACTIVE_END:
            return "ACTIVE"
        if secs > 0:
            return "LOCKDOWN"
        return "SETTLED"

    def process_signal(self, sig):
        slug = sig["slug"]
        tf = sig["tf"]
        secs = sig["secs_left"]
        asset = sig["asset"]

        # Only 15m windows (no 60m in data, and 5m is too tight for arb)
        if tf not in [15]:
            return

        phase = self._phase(secs, tf)
        if phase == "BEFORE" or phase == "SETTLED":
            return

        ask_yes = sig.get("ask_yes", 0)
        ask_no = sig.get("ask_no", 0)
        bid_yes = sig.get("bid_yes", 0)
        bid_no = sig.get("bid_no", 0)

        if ask_yes <= 0 or ask_no <= 0:
            return

        pair_cost = ask_yes + ask_no

        # ── ACTIVE phase: try to enter new pairs ──
        if phase == "ACTIVE" and slug not in self.entered:
            if pair_cost >= self.max_pair_cost:
                return

            if self.use_maker:
                # Maker: bid inside spread on both sides
                fill_yes = max(ask_yes - MAKER_IMPROVE, 0.01)
                fill_no = max(ask_no - MAKER_IMPROVE, 0.01)
                # Check if maker would cross (instant fill)
                if fill_yes >= ask_yes:
                    fill_yes = ask_yes
                if fill_no >= ask_no:
                    fill_no = ask_no
            else:
                fill_yes = ask_yes + SLIP
                fill_no = ask_no + SLIP

            actual_pair_cost = fill_yes + fill_no
            if actual_pair_cost >= 1.0:
                return

            # Fee on both sides
            fee_yes = pm_fee(fill_yes)
            fee_no = pm_fee(fill_no)

            # Per-share profit: 1.00 - pair_cost - fees
            shares = min(STAKE_PER_SIDE / fill_yes, STAKE_PER_SIDE / fill_no)
            total_cost = shares * actual_pair_cost
            total_fees = shares * (fee_yes + fee_no)
            revenue = shares * 1.0  # Settlement always pays $1 per share
            net_profit = revenue - total_cost - total_fees

            if net_profit < MIN_SPREAD_PROFIT * shares:
                return

            self.pairs[slug] = {
                "slug": slug, "asset": asset, "tf": tf,
                "fill_yes": fill_yes, "fill_no": fill_no,
                "pair_cost": actual_pair_cost,
                "shares": shares, "fees": total_fees,
                "net_expected": round(net_profit, 4),
                "secs": secs, "phase": "ACTIVE",
                "entry_ts": sig["ts"],
            }
            self.entered.add(slug)
            return

        # ── LOCKDOWN phase: complete unmatched pairs with taker ──
        if phase == "LOCKDOWN" and slug in self.pairs and self.use_taker_lockdown:
            pair = self.pairs[slug]
            if pair.get("completed"):
                return
            # Mark as completed (both sides filled)
            pair["completed"] = True
            pair["phase"] = "LOCKDOWN"

    def settle(self, slug, outcome_str):
        if slug not in self.pairs:
            return
        pair = self.pairs.pop(slug)

        # Settlement: one side pays $1, other pays $0
        # But we hold BOTH sides, so we always get $1 per share
        revenue = pair["shares"] * 1.0
        cost = pair["shares"] * pair["pair_cost"]
        fees = pair["fees"]
        net = revenue - cost - fees

        self.pnl += net
        self.total_pairs += 1
        if net > 0:
            self.wins += 1
        else:
            self.losses += 1
        self._update_dd()

        w = "WIN" if net > 0 else "LOSS"
        self.trade_log.append({
            "ts": pair["entry_ts"], "slug": slug, "asset": pair["asset"],
            "tf": pair["tf"],
            "fill_yes": round(pair["fill_yes"], 4),
            "fill_no": round(pair["fill_no"], 4),
            "pair_cost": round(pair["pair_cost"], 4),
            "shares": round(pair["shares"], 2),
            "fees": round(fees, 4),
            "net": round(net, 4),
            "cum_pnl": round(self.pnl, 2),
            "result": w, "secs": pair["secs"],
            "phase": pair["phase"],
        })


# ── Also test on 5m windows since that's most of our data ───────────────────

class ArbEngine5m(ArbEngine):
    """Same logic but allows 5m windows too."""
    def process_signal(self, sig):
        slug = sig["slug"]
        tf = sig["tf"]
        secs = sig["secs_left"]
        asset = sig["asset"]

        if tf == 5:
            warmup = 60  # 60s warmup for 5m
        elif tf == 15:
            warmup = 120
        else:
            return

        if secs > warmup or secs < 0:
            return

        ask_yes = sig.get("ask_yes", 0)
        ask_no = sig.get("ask_no", 0)
        if ask_yes <= 0 or ask_no <= 0:
            return

        pair_cost = ask_yes + ask_no
        if slug in self.entered:
            return

        if pair_cost >= self.max_pair_cost:
            return

        if self.use_maker:
            fill_yes = max(ask_yes - MAKER_IMPROVE, 0.01)
            fill_no = max(ask_no - MAKER_IMPROVE, 0.01)
            if fill_yes >= ask_yes:
                fill_yes = ask_yes
            if fill_no >= ask_no:
                fill_no = ask_no
        else:
            fill_yes = ask_yes + SLIP
            fill_no = ask_no + SLIP

        actual_pair_cost = fill_yes + fill_no
        if actual_pair_cost >= 1.0:
            return

        fee_yes = pm_fee(fill_yes)
        fee_no = pm_fee(fill_no)
        shares = min(STAKE_PER_SIDE / fill_yes, STAKE_PER_SIDE / fill_no)
        total_fees = shares * (fee_yes + fee_no)
        net_profit = shares * 1.0 - shares * actual_pair_cost - total_fees

        if net_profit < MIN_SPREAD_PROFIT * shares:
            return

        self.pairs[slug] = {
            "slug": slug, "asset": asset, "tf": tf,
            "fill_yes": fill_yes, "fill_no": fill_no,
            "pair_cost": actual_pair_cost,
            "shares": shares, "fees": total_fees,
            "net_expected": round(net_profit, 4),
            "secs": secs, "phase": "ACTIVE",
            "entry_ts": sig["ts"],
        }
        self.entered.add(slug)


# ── Run ──────────────────────────────────────────────────────────────────────

print("Processing signals chronologically...\n")

# Check data: what pair costs exist?
print("── Pair cost distribution (ask_yes + ask_no) ──")
pair_costs = defaultdict(int)
for sig in signals:
    ay = sig.get("ask_yes", 0)
    an = sig.get("ask_no", 0)
    if ay > 0 and an > 0:
        bucket = round((ay + an) * 20) / 20  # 0.05 buckets
        pair_costs[bucket] += 1

for b in sorted(pair_costs.keys()):
    if b < 1.05:
        bar = "#" * min(pair_costs[b] // 50, 60)
        arb = "ARB" if b < 0.98 else ("MARGINAL" if b < 1.00 else "")
        print(f"  {b:.2f}: {pair_costs[b]:>6d} {bar} {arb}")

# Count arb opportunities
arb_opps = sum(1 for sig in signals
               if sig.get("ask_yes", 0) > 0 and sig.get("ask_no", 0) > 0
               and sig.get("ask_yes", 0) + sig.get("ask_no", 0) < 0.98)
marginal = sum(1 for sig in signals
               if sig.get("ask_yes", 0) > 0 and sig.get("ask_no", 0) > 0
               and 0.98 <= sig.get("ask_yes", 0) + sig.get("ask_no", 0) < 1.00)
print(f"\n  Arb opportunities (sum < 0.98): {arb_opps:,}")
print(f"  Marginal (0.98-1.00): {marginal:,}")
print(f"  Total signals with both sides: {sum(pair_costs.values()):,}")

# yes_no_arb field check
arb_field = [sig.get("yes_no_arb", 0) for sig in signals if sig.get("yes_no_arb", 0) != 0]
if arb_field:
    print(f"\n  yes_no_arb field: min={min(arb_field):.4f} max={max(arb_field):.4f} "
          f"mean={sum(arb_field)/len(arb_field):.4f} count={len(arb_field)}")
    arb_positive = [x for x in arb_field if x > 0]
    print(f"  Positive yes_no_arb (real arb): {len(arb_positive)}")

# Run engines
engines = [
    ArbEngine("ARB-MAKER-98", 0.98, True, True),
    ArbEngine("ARB-MAKER-99", 0.99, True, True),
    ArbEngine("ARB-TAKER-98", 0.98, False, True),
    ArbEngine("ARB-TAKER-99", 0.99, False, True),
    ArbEngine5m("ARB-5m-MAKER-98", 0.98, True, True),
    ArbEngine5m("ARB-5m-MAKER-99", 0.99, True, True),
    ArbEngine5m("ARB-5m-TAKER-99", 0.99, False, True),
    ArbEngine5m("ARB-5m+15m-WIDE", 1.00, True, True),  # Even at cost, fees might make it negative
]

settled_windows = set()

for sig in signals:
    ts = sig["ts"]
    for wend in list(settle_by_end.keys()):
        if ts >= wend and wend not in settled_windows:
            settled_windows.add(wend)
            for s in settle_by_end[wend]:
                for eng in engines:
                    eng.settle(s["slug"], s["outcome"])

    for eng in engines:
        eng.process_signal(sig)

# Settle remaining
for eng in engines:
    for slug in list(eng.pairs.keys()):
        if slug in settlements:
            eng.settle(slug, settlements[slug]["outcome"])

# ── Report ───────────────────────────────────────────────────────────────────

ts0 = signals[0]["ts"]
ts1 = signals[-1]["ts"]
hours = (ts1 - ts0) / 3600.0

print(f"\n{'=' * 100}")
print("  ARB-FINAL — MATCHED-PAIR ARBITRAGE FORWARD TEST")
print(f"  Data: {len(signals):,} signals over {hours:.1f}h | {len(settlements)} settlements")
print(f"  Buy YES + NO on same market. Settlement always pays $1.00/share.")
print(f"  Profit = shares × (1.00 - pair_cost) - fees")
print(f"{'=' * 100}")

print(f"\n  {'Engine':<22s} {'Pairs':>5s} {'W':>3s} {'L':>3s} {'WR':>6s} {'PnL':>8s} "
      f"{'DD':>6s} {'$/pair':>7s}")
print(f"  {'-' * 65}")

for eng in engines:
    total = eng.wins + eng.losses
    wr = eng.wins / total * 100 if total > 0 else 0
    avg = eng.pnl / total if total > 0 else 0
    print(f"  {eng.name:<22s} {total:>5d} {eng.wins:>3d} {eng.losses:>3d} {wr:>5.1f}% "
          f"${eng.pnl:>+6.2f} ${eng.dd:>5.2f} ${avg:>+5.2f}")

# Trade details
for eng in engines:
    if not eng.trade_log:
        continue
    total = eng.wins + eng.losses
    wr = eng.wins / total * 100 if total > 0 else 0

    print(f"\n{'=' * 100}")
    print(f"  [{eng.name}] Pairs={total} W={eng.wins} L={eng.losses} WR={wr:.1f}% "
          f"PnL=${eng.pnl:+.2f} DD=${eng.dd:.2f}")
    print(f"{'=' * 100}")

    print(f"  {'#':>3s} {'Time':>10s} {'W/L':>4s} {'Asset':>5s} {'TF':>3s} "
          f"{'FillY':>6s} {'FillN':>6s} {'Pair$':>6s} {'Sh':>6s} {'Fees':>6s} "
          f"{'Net':>7s} {'CumPnL':>8s} {'Secs':>5s}")
    print(f"  {'-' * 85}")

    for i, t in enumerate(eng.trade_log[:25]):
        ts_str = datetime.fromtimestamp(t["ts"], tz=timezone.utc).strftime("%H:%M:%S")
        print(f"  {i+1:>3d} {ts_str:>10s} {t['result']:>4s} {t['asset']:>5s} {t['tf']:>3d}m "
              f"@{t['fill_yes']:.3f}+{t['fill_no']:.3f} ={t['pair_cost']:.3f} "
              f"{t['shares']:>5.1f}sh ${t['fees']:.3f} "
              f"${t['net']:>+5.2f} ${t['cum_pnl']:>+6.2f} {t['secs']:>5.0f}s")

# Still open
for eng in engines:
    if eng.pairs:
        print(f"\n  [{eng.name}] {len(eng.pairs)} pairs still open:")
        for slug, p in eng.pairs.items():
            print(f"    {slug} Y@{p['fill_yes']:.3f}+N@{p['fill_no']:.3f}={p['pair_cost']:.3f} "
                  f"expected=${p['net_expected']:+.4f}")
