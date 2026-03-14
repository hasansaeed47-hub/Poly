#!/usr/bin/env python3
"""
WEATHER HEAD-TO-HEAD: Your Bankroll vs gopfan2
================================================
Real scenario: Shanghai March 16, 2026
Forecast: 12°C (ECMWF/Ventusky)

Simulates both strategies across all possible outcomes
to show P&L, win rate, and bankroll trajectory.
"""

import random
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

# ─── REAL MARKET DATA (from screenshot) ──────────────────────────────────────

MARKET = "Highest temperature in Shanghai on March 16"
FORECAST_CENTER = 12  # °C — ECMWF forecast
FORECAST_CONFIDENCE = 0.85  # 1-2 day NOAA/ECMWF accuracy

# Buckets exactly as shown in the Polymarket screenshot
BUCKETS: Dict[str, dict] = {
    "≤5°C":  {"yes": 0.006, "no": 0.996, "market_pct": 0.01, "vol": 2301},
    "7°C":   {"yes": 0.027, "no": 0.998, "market_pct": 0.01, "vol": 672},
    "8°C":   {"yes": 0.058, "no": 0.998, "market_pct": 0.03, "vol": 844},
    "9°C":   {"yes": 0.080, "no": 0.980, "market_pct": 0.05, "vol": 637},
    "10°C":  {"yes": 0.140, "no": 0.890, "market_pct": 0.13, "vol": 243},
    "11°C":  {"yes": 0.220, "no": 0.870, "market_pct": 0.18, "vol": 206},
    "12°C":  {"yes": 0.450, "no": 0.590, "market_pct": 0.43, "vol": 243},
    "13°C":  {"yes": 0.200, "no": 0.860, "market_pct": 0.17, "vol": 231},
    "14°C":  {"yes": 0.150, "no": 0.900, "market_pct": 0.13, "vol": 260},
    "15°C+": {"yes": 0.100, "no": 0.970, "market_pct": 0.07, "vol": 225},
}

# Real probability distribution based on ECMWF forecast of 12°C
# (gaussian-ish centered on 12, ±2°C covers ~80%)
REAL_PROBABILITIES: Dict[str, float] = {
    "≤5°C":  0.001,   # basically impossible
    "7°C":   0.002,   # near impossible
    "8°C":   0.005,   # extremely unlikely
    "9°C":   0.015,   # very unlikely
    "10°C":  0.06,    # unlikely but possible
    "11°C":  0.15,    # possible
    "12°C":  0.42,    # forecast center — most likely
    "13°C":  0.20,    # possible
    "14°C":  0.08,    # unlikely but possible
    "15°C+": 0.027,   # very unlikely
}


# ─── DATA CLASSES ─────────────────────────────────────────────────────────────

@dataclass
class Trade:
    bucket: str
    side: str       # "YES" or "NO"
    price: float    # entry price per share
    size: float     # $ deployed
    shares: float   # shares bought = size / price

    @property
    def payout_if_win(self) -> float:
        return self.shares * 1.00

    @property
    def profit_if_win(self) -> float:
        return self.payout_if_win - self.size

    @property
    def loss_if_lose(self) -> float:
        return self.size


@dataclass
class Strategy:
    name: str
    starting_bankroll: float
    trades: List[Trade] = field(default_factory=list)

    def total_deployed(self) -> float:
        return sum(t.size for t in self.trades)

    def pnl_for_outcome(self, winning_bucket: str) -> float:
        """Calculate P&L if winning_bucket is the actual high temp."""
        pnl = 0.0
        for t in self.trades:
            if t.side == "YES":
                if t.bucket == winning_bucket:
                    pnl += t.profit_if_win
                else:
                    pnl -= t.loss_if_lose
            elif t.side == "NO":
                if t.bucket == winning_bucket:
                    pnl -= t.loss_if_lose   # NO loses when bucket wins
                else:
                    pnl += t.profit_if_win
        return pnl


# ─── STRATEGY BUILDERS ────────────────────────────────────────────────────────

def build_your_strategy(bankroll: float) -> Strategy:
    """
    YOUR approach: Buy NO on dead buckets with meaningful size.
    Informed by ECMWF forecast of 12°C. Targets buckets far from forecast.
    """
    s = Strategy(name="YOU (Smart NO)", starting_bankroll=bankroll)

    # Aggressive NO on impossible/very unlikely buckets
    s.trades.append(Trade("≤5°C", "NO", 0.996, 10.0, 10.0 / 0.996))
    s.trades.append(Trade("7°C",  "NO", 0.998, 10.0, 10.0 / 0.998))
    s.trades.append(Trade("8°C",  "NO", 0.998, 8.0,  8.0 / 0.998))
    s.trades.append(Trade("9°C",  "NO", 0.980, 12.0, 12.0 / 0.980))

    # Bigger bets on the juicy middle mispricings
    s.trades.append(Trade("10°C", "NO", 0.890, 25.0, 25.0 / 0.890))
    s.trades.append(Trade("14°C", "NO", 0.900, 25.0, 25.0 / 0.900))
    s.trades.append(Trade("15°C+","NO", 0.970, 15.0, 15.0 / 0.970))

    # Also buy YES on forecast center for upside
    s.trades.append(Trade("12°C", "YES", 0.450, 20.0, 20.0 / 0.450))
    s.trades.append(Trade("13°C", "YES", 0.200, 10.0, 10.0 / 0.200))

    return s


def build_gopfan2_strategy(bankroll: float) -> Strategy:
    """
    gopfan2's approach: $1 flat bets on everything with YES < $0.15.
    No forecast needed. Pure price-threshold.
    """
    s = Strategy(name="gopfan2 ($1 Flat)", starting_bankroll=bankroll)

    for bucket, data in BUCKETS.items():
        # Rule 1: Buy YES if price < $0.15
        if data["yes"] < 0.15:
            s.trades.append(Trade(bucket, "YES", data["yes"], 1.0, 1.0 / data["yes"]))

        # Rule 2: Buy NO if YES price > $0.45 (i.e. bucket looks overpriced)
        # gopfan2 also does NO on high-prob buckets
        if data["yes"] > 0.45:
            pass  # In this market only 12°C at 45¢ qualifies — borderline, skip

    return s


# ─── SIMULATION ───────────────────────────────────────────────────────────────

def print_header(title: str):
    w = 70
    print(f"\n{'='*w}")
    print(f"  {title}")
    print(f"{'='*w}")


def print_trades(strategy: Strategy):
    print(f"\n  {strategy.name} — Trades:")
    print(f"  {'Bucket':<10} {'Side':<5} {'Price':>7} {'Size':>7} {'Shares':>8} {'Profit if Win':>14}")
    print(f"  {'-'*10} {'-'*5} {'-'*7} {'-'*7} {'-'*8} {'-'*14}")
    for t in strategy.trades:
        print(f"  {t.bucket:<10} {t.side:<5} {t.price:>7.3f} ${t.size:>5.0f}  {t.shares:>7.1f}  ${t.profit_if_win:>12.2f}")
    print(f"\n  Total deployed: ${strategy.total_deployed():.2f} / ${strategy.starting_bankroll:.2f} bankroll")


def run_single_outcome(you: Strategy, gop: Strategy, outcome: str):
    """Show P&L for a single outcome."""
    your_pnl = you.pnl_for_outcome(outcome)
    gop_pnl = gop.pnl_for_outcome(outcome)
    return your_pnl, gop_pnl


def run_head_to_head():
    BANKROLL = 145.63  # Your actual balance from the screenshot

    you = build_your_strategy(BANKROLL)
    gop = build_gopfan2_strategy(BANKROLL)

    # ── Show Trades ──────────────────────────────────────────────────────
    print_header("HEAD TO HEAD: You vs gopfan2 — Shanghai March 16")
    print(f"\n  Market: {MARKET}")
    print(f"  Forecast: {FORECAST_CENTER}°C (ECMWF)")
    print(f"  Your bankroll: ${BANKROLL:.2f}")

    print_trades(you)
    print_trades(gop)

    # ── Outcome Table ────────────────────────────────────────────────────
    print_header("P&L BY EVERY POSSIBLE OUTCOME")
    print(f"\n  {'Outcome':<10} {'Real Prob':>9} {'YOU P&L':>10} {'gopfan2 P&L':>12} {'Winner':>10}")
    print(f"  {'-'*10} {'-'*9} {'-'*10} {'-'*12} {'-'*10}")

    you_ev = 0.0
    gop_ev = 0.0
    you_wins = 0
    gop_wins = 0

    for bucket in BUCKETS:
        prob = REAL_PROBABILITIES[bucket]
        y_pnl, g_pnl = run_single_outcome(you, gop, bucket)
        you_ev += prob * y_pnl
        gop_ev += prob * g_pnl

        winner = "YOU" if y_pnl > g_pnl else "gopfan2" if g_pnl > y_pnl else "TIE"
        if y_pnl > g_pnl:
            you_wins += 1
        elif g_pnl > y_pnl:
            gop_wins += 1

        y_sign = "+" if y_pnl >= 0 else ""
        g_sign = "+" if g_pnl >= 0 else ""
        print(f"  {bucket:<10} {prob:>8.1%}  {y_sign}${y_pnl:>8.2f}  {g_sign}${g_pnl:>10.2f}  {winner:>10}")

    # ── Expected Value ───────────────────────────────────────────────────
    print_header("EXPECTED VALUE (probability-weighted)")
    print(f"""
  YOU:     ${you_ev:>+8.2f}  (on ${you.total_deployed():.2f} deployed)
  gopfan2: ${gop_ev:>+8.2f}  (on ${gop.total_deployed():.2f} deployed)

  YOU ROI:     {you_ev / you.total_deployed() * 100:>+.1f}%
  gopfan2 ROI: {gop_ev / gop.total_deployed() * 100:>+.1f}%

  Outcomes YOU win:     {you_wins}/10
  Outcomes gopfan2 win: {gop_wins}/10
""")

    # ── Best/Worst Case ──────────────────────────────────────────────────
    print_header("BEST & WORST CASE")

    you_outcomes = {b: you.pnl_for_outcome(b) for b in BUCKETS}
    gop_outcomes = {b: gop.pnl_for_outcome(b) for b in BUCKETS}

    you_best = max(you_outcomes, key=you_outcomes.get)
    you_worst = min(you_outcomes, key=you_outcomes.get)
    gop_best = max(gop_outcomes, key=gop_outcomes.get)
    gop_worst = min(gop_outcomes, key=gop_outcomes.get)

    print(f"""
  YOU:
    Best case:  {you_best:<8} → ${you_outcomes[you_best]:>+.2f}
    Worst case: {you_worst:<8} → ${you_outcomes[you_worst]:>+.2f}
    Max drawdown from bankroll: {you_outcomes[you_worst] / BANKROLL * 100:.1f}%

  gopfan2:
    Best case:  {gop_best:<8} → ${gop_outcomes[gop_best]:>+.2f}
    Worst case: {gop_worst:<8} → ${gop_outcomes[gop_worst]:>+.2f}
    Max drawdown from bankroll: {gop_outcomes[gop_worst] / BANKROLL * 100:.1f}%
""")

    # ── Monte Carlo: 30 days of this market ──────────────────────────────
    print_header("MONTE CARLO: 30 DAYS OF WEATHER TRADING")
    print("  (Simulating 30 independent Shanghai-like markets)\n")

    DAYS = 30
    SIMS = 10000

    you_final_totals = []
    gop_final_totals = []

    for _ in range(SIMS):
        you_bank = BANKROLL
        gop_bank = BANKROLL
        for _ in range(DAYS):
            # Pick outcome weighted by real probabilities
            buckets_list = list(REAL_PROBABILITIES.keys())
            probs_list = list(REAL_PROBABILITIES.values())
            outcome = random.choices(buckets_list, weights=probs_list, k=1)[0]

            you_bank += you.pnl_for_outcome(outcome)
            gop_bank += gop.pnl_for_outcome(outcome)

        you_final_totals.append(you_bank)
        gop_final_totals.append(gop_bank)

    you_avg = sum(you_final_totals) / SIMS
    gop_avg = sum(gop_final_totals) / SIMS
    you_median = sorted(you_final_totals)[SIMS // 2]
    gop_median = sorted(gop_final_totals)[SIMS // 2]
    you_bust = sum(1 for x in you_final_totals if x <= 0) / SIMS
    gop_bust = sum(1 for x in gop_final_totals if x <= 0) / SIMS
    you_p5 = sorted(you_final_totals)[int(SIMS * 0.05)]
    you_p95 = sorted(you_final_totals)[int(SIMS * 0.95)]
    gop_p5 = sorted(gop_final_totals)[int(SIMS * 0.05)]
    gop_p95 = sorted(gop_final_totals)[int(SIMS * 0.95)]
    you_max = max(you_final_totals)
    gop_max = max(gop_final_totals)

    you_beat_gop = sum(1 for y, g in zip(you_final_totals, gop_final_totals) if y > g) / SIMS

    print(f"  {'':<20} {'YOU':>12} {'gopfan2':>12}")
    print(f"  {'-'*20} {'-'*12} {'-'*12}")
    print(f"  {'Start bankroll':<20} ${BANKROLL:>10.2f} ${BANKROLL:>10.2f}")
    print(f"  {'Avg final (30d)':<20} ${you_avg:>10.2f} ${gop_avg:>10.2f}")
    print(f"  {'Median final':<20} ${you_median:>10.2f} ${gop_median:>10.2f}")
    print(f"  {'5th percentile':<20} ${you_p5:>10.2f} ${gop_p5:>10.2f}")
    print(f"  {'95th percentile':<20} ${you_p95:>10.2f} ${gop_p95:>10.2f}")
    print(f"  {'Best run':<20} ${you_max:>10.2f} ${gop_max:>10.2f}")
    print(f"  {'Bust rate (<$0)':<20} {you_bust:>10.1%} {gop_bust:>10.1%}")
    print(f"  {'30d avg ROI':<20} {(you_avg - BANKROLL) / BANKROLL * 100:>+9.1f}% {(gop_avg - BANKROLL) / BANKROLL * 100:>+9.1f}%")
    print(f"\n  YOU beats gopfan2 in {you_beat_gop:.0%} of simulations")

    # ── Verdict ──────────────────────────────────────────────────────────
    print_header("VERDICT")
    if you_ev > gop_ev:
        edge = you_ev - gop_ev
        print(f"""
  YOUR STRATEGY WINS by ${edge:.2f} expected per market.

  Why:
  - You deploy ${you.total_deployed():.0f} vs gopfan2's ${gop.total_deployed():.0f} — more capital at work
  - Your NO bets on dead buckets are near-guaranteed income
  - Your YES bet on 12°C captures the fat center of the distribution
  - gopfan2's $1 YES bets have high upside but low probability

  Trade-off:
  - You risk more per market (bigger drawdown if forecast is wrong)
  - gopfan2 can never blow up — $1 max loss per bucket
  - gopfan2 scales across 10+ cities; you'd need more bankroll to match

  Bottom line: Your approach is a BETTER SINGLE-MARKET strategy.
  gopfan2's approach is a BETTER PORTFOLIO strategy (tiny bets, many markets).
""")
    else:
        edge = gop_ev - you_ev
        print(f"""
  gopfan2 WINS by ${edge:.2f} expected per market.

  The $1-flat approach + volume across markets beats concentrated bets.
""")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_head_to_head()
