#!/usr/bin/env python3
"""Forward test: CL Lag Bot A/B Test — trades PM vs CL fair value gap.

This is the upgraded Oracle Sniper that ONLY trades the gap between
Polymarket book prices and Chainlink-derived Black-Scholes fair value.

Engine A: Maker (postOnly), min_edge=0.25, ~25% fill rate
Engine B: Taker (IOC), min_edge=0.27, ~92% fill rate, 2% taker fee

From cl_ab_test.zip — the production gap trading system.
"""

import json, math, random
from collections import defaultdict
from datetime import datetime, timezone

random.seed(42)  # Reproducible fills

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

# ── Config (exact from cl_ab_test) ───────────────────────────────────────────

STAKE = 5.0
MIN_SECS = 60.0
MAX_SECS = 300.0
MAX_BOOK_AGE = 3.0  # seconds — can't check in replay, assume fresh

# Engine A: maker postOnly
A_MIN_EDGE = 0.25
A_FILL_PROB = 0.25  # 25% maker fill rate

# Engine B: taker IOC
B_MIN_EDGE = 0.27
B_FILL_PROB = 0.92  # 92% taker fill rate
B_TAKER_FEE = 0.02  # 2% of stake

MAX_OPEN = 6

def pm_fee(px):
    return px * (1.0 - px) * 0.0625


class GapEngine:
    def __init__(self, name, min_edge, fill_prob, taker_fee, is_taker):
        self.name = name
        self.min_edge = min_edge
        self.fill_prob = fill_prob
        self.taker_fee = taker_fee
        self.is_taker = is_taker
        self.positions = {}  # slug -> position
        self.done_slugs = set()
        self.wins = 0
        self.losses = 0
        self.pnl = 0.0
        self.peak = 0.0
        self.dd = 0.0
        self.fills = 0
        self.no_fills = 0
        self.trade_log = []

    def _update_dd(self):
        if self.pnl > self.peak:
            self.peak = self.pnl
        d = self.peak - self.pnl
        if d > self.dd:
            self.dd = d

    def try_enter(self, sig):
        slug = sig["slug"]
        secs = sig["secs_left"]

        if secs < MIN_SECS or secs > MAX_SECS:
            return
        if slug in self.done_slugs or slug in self.positions:
            return
        if len(self.positions) >= MAX_OPEN:
            return

        # Get fair value and edge from signal (pre-computed BS fair value)
        fair_yes = sig.get("fair_yes", 0.5)
        fair_no = sig.get("fair_no", 0.5)

        # Skip if fair value too close to 0.5 (no directional signal)
        if abs(fair_yes - 0.5) < 0.05:
            return

        # Determine direction: buy the side with higher fair value
        if fair_yes > 0.5:
            direction = "YES"
            fair = fair_yes
            ask = sig.get("ask_yes", 0)
            bid = sig.get("bid_yes", 0)
            edge = fair - ask  # gap between fair value and book price
        else:
            direction = "NO"
            fair = fair_no
            ask = sig.get("ask_no", 0)
            bid = sig.get("bid_no", 0)
            edge = fair - ask  # for NO: fair_no - ask_no

        if ask <= 0 or ask >= 1.0:
            return
        if bid <= 0 or bid >= 1.0:
            return
        if ask < 0.02 or ask > 0.98:
            return
        if ask - bid > 0.10:  # spread too wide
            return

        # Edge check
        if edge < self.min_edge:
            return

        # Fill simulation
        if self.is_taker:
            # Taker: fill at ask + small slip
            fill_prob = self.fill_prob
            entry_price = ask
        else:
            # Maker: fill at ask - 0.01 (limit order inside spread)
            # High edge signals = fast move = lower fill probability
            fill_prob = self.fill_prob * 0.5 if edge > 0.35 else self.fill_prob
            entry_price = max(ask - 0.01, 0.01)

        # Simulate fill
        if random.random() > fill_prob:
            self.no_fills += 1
            return

        self.fills += 1
        effective_price = min(entry_price + 0.01, 0.99) if not self.is_taker else entry_price
        shares = STAKE / effective_price
        fee = self.taker_fee * STAKE if self.is_taker else 0.0

        self.positions[slug] = {
            "slug": slug, "direction": direction, "asset": sig["asset"],
            "tf": sig["tf"], "entry_price": effective_price, "fair": fair,
            "edge": edge, "shares": shares, "fee": fee, "secs": secs,
            "entry_ts": sig["ts"],
        }
        self.done_slugs.add(slug)

    def settle(self, slug, outcome_str):
        if slug not in self.positions:
            return
        pos = self.positions.pop(slug)
        yes_wins = outcome_str == "YES"

        if pos["direction"] == "YES":
            we_win = yes_wins
        else:
            we_win = not yes_wins

        if we_win:
            gross = (1.0 - pos["entry_price"]) * STAKE
        else:
            gross = -pos["entry_price"] * STAKE

        net = gross - pos["fee"]
        self.pnl += net
        if net > 0:
            self.wins += 1
        else:
            self.losses += 1
        self._update_dd()

        w = "WIN" if net > 0 else "LOSS"
        self.trade_log.append({
            "ts": pos["entry_ts"], "slug": slug, "direction": pos["direction"],
            "asset": pos["asset"], "tf": pos["tf"],
            "entry": round(pos["entry_price"], 4), "fair": round(pos["fair"], 4),
            "edge": round(pos["edge"], 4), "net": round(net, 2),
            "cum_pnl": round(self.pnl, 2), "result": w, "secs": pos["secs"],
        })


# ── Run ──────────────────────────────────────────────────────────────────────

print("Processing signals chronologically...\n")

eng_a = GapEngine("A-MAKER", A_MIN_EDGE, A_FILL_PROB, 0.0, False)
eng_b = GapEngine("B-TAKER", B_MIN_EDGE, B_FILL_PROB, B_TAKER_FEE, True)

# Also run with deterministic fill (100% fill, no randomness) for pure signal quality
eng_c = GapEngine("C-PERFECT", 0.25, 1.0, 0.0, False)  # 100% fill, no fee
eng_d = GapEngine("D-TAKER100", 0.25, 1.0, 0.02, True)  # 100% fill, with fee

engines = [eng_a, eng_b, eng_c, eng_d]
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
        eng.try_enter(sig)

# Settle remaining
for eng in engines:
    for slug in list(eng.positions.keys()):
        if slug in settlements:
            eng.settle(slug, settlements[slug]["outcome"])

# ── Report ───────────────────────────────────────────────────────────────────

ts0 = signals[0]["ts"]
ts1 = signals[-1]["ts"]
hours = (ts1 - ts0) / 3600.0

print("=" * 100)
print("  CL LAG BOT A/B TEST — FORWARD TEST (Gap between PM book and CL fair value)")
print(f"  Data: {len(signals):,} signals over {hours:.1f}h | {len(settlements)} settlements")
print(f"  A: maker, edge≥0.25, ~25% fill | B: taker, edge≥0.27, ~92% fill, 2% fee")
print(f"  C: perfect fill edge≥0.25 | D: perfect fill edge≥0.25 + 2% fee")
print("=" * 100)

print(f"\n  {'Engine':<14s} {'Trades':>6s} {'W':>4s} {'L':>4s} {'WR':>6s} {'PnL':>8s} "
      f"{'DD':>6s} {'Fills':>6s} {'NoFill':>6s} {'$/tr':>6s}")
print(f"  {'-' * 72}")

for eng in engines:
    total = eng.wins + eng.losses
    wr = eng.wins / total * 100 if total > 0 else 0
    avg = eng.pnl / total if total > 0 else 0
    print(f"  {eng.name:<14s} {total:>6d} {eng.wins:>4d} {eng.losses:>4d} {wr:>5.1f}% "
          f"${eng.pnl:>+6.2f} ${eng.dd:>5.2f} {eng.fills:>6d} {eng.no_fills:>6d} ${avg:>+4.2f}")

# Trade details for each engine
for eng in engines:
    total = eng.wins + eng.losses
    if total == 0:
        print(f"\n  [{eng.name}] NO TRADES")
        continue

    wr = eng.wins / total * 100
    print(f"\n{'=' * 100}")
    print(f"  [{eng.name}] Trades={total} W={eng.wins} L={eng.losses} WR={wr:.1f}% "
          f"PnL=${eng.pnl:+.2f} DD=${eng.dd:.2f}")
    print(f"{'=' * 100}")

    print(f"  {'#':>3s} {'Time':>10s} {'W/L':>4s} {'Asset':>5s} {'TF':>3s} {'Dir':>4s} "
          f"{'Entry':>6s} {'Fair':>6s} {'Edge':>6s} {'Net':>7s} {'CumPnL':>8s} {'Secs':>5s}")
    print(f"  {'-' * 75}")

    for i, t in enumerate(eng.trade_log[:30]):
        ts_str = datetime.fromtimestamp(t["ts"], tz=timezone.utc).strftime("%H:%M:%S")
        print(f"  {i+1:>3d} {ts_str:>10s} {t['result']:>4s} {t['asset']:>5s} {t['tf']:>3d}m "
              f"{t['direction']:>4s} @{t['entry']:.3f} f={t['fair']:.3f} e={t['edge']:.3f} "
              f"${t['net']:>+5.2f} ${t['cum_pnl']:>+6.2f} {t['secs']:>5.0f}s")
    if len(eng.trade_log) > 30:
        print(f"  ... and {len(eng.trade_log) - 30} more trades")

# Edge distribution
print(f"\n{'=' * 100}")
print("  EDGE DISTRIBUTION (signals with edge >= 0.10)")
print(f"{'=' * 100}")

edge_counts = defaultdict(int)
for sig in signals:
    fair_yes = sig.get("fair_yes", 0.5)
    fair_no = sig.get("fair_no", 0.5)
    ask_yes = sig.get("ask_yes", 0)
    ask_no = sig.get("ask_no", 0)

    if fair_yes > 0.5 and ask_yes > 0:
        edge = fair_yes - ask_yes
    elif fair_no > 0.5 and ask_no > 0:
        edge = fair_no - ask_no
    else:
        continue

    if edge >= 0.10:
        bucket = int(edge * 100) / 100
        edge_counts[bucket] += 1

for bucket in sorted(edge_counts.keys()):
    bar = "#" * min(edge_counts[bucket] // 10, 50)
    print(f"  {bucket:.2f}: {edge_counts[bucket]:>5d} {bar}")

# Still open
for eng in engines:
    if eng.positions:
        print(f"\n  [{eng.name}] {len(eng.positions)} still open (no settlement)")
