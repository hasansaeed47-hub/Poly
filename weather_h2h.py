#!/usr/bin/env python3
"""
WEATHER HEAD-TO-HEAD: $10/stake vs gopfan2 $1/stake
=====================================================
Both use the SAME NO-on-dead-buckets strategy.
5 real cities from Polymarket screenshots, March 15 2026.
"""

import random
from dataclasses import dataclass, field
from typing import Dict, List

# ─── REAL MARKET DATA (from screenshots, March 15 2026) ─────────────────────

CITIES = {
    "Seoul": {
        "buckets": {
            "≤3°C":  {"yes": 0.001, "no": 0.000},
            "4°C":   {"yes": 0.001, "no": 0.000},
            "5°C":   {"yes": 0.005, "no": 0.999},
            "6°C":   {"yes": 0.012, "no": 0.997},
            "7°C":   {"yes": 0.019, "no": 0.986},
            "8°C":   {"yes": 0.060, "no": 0.950},
            "9°C":   {"yes": 0.250, "no": 0.770},
            "10°C":  {"yes": 0.340, "no": 0.690},
            "11°C+": {"yes": 0.400, "no": 0.630},
        },
        "forecast": "10°C",
        # Real prob distribution centered on 10°C
        "real_prob": {
            "≤3°C": 0.001, "4°C": 0.002, "5°C": 0.005, "6°C": 0.01,
            "7°C": 0.03, "8°C": 0.08, "9°C": 0.20, "10°C": 0.33,
            "11°C+": 0.342,
        },
    },
    "Shanghai": {
        "buckets": {
            "≤9°C":  {"yes": 0.001, "no": 0.000},
            "10°C":  {"yes": 0.001, "no": 0.000},
            "11°C":  {"yes": 0.002, "no": 0.999},
            "12°C":  {"yes": 0.014, "no": 0.989},
            "13°C":  {"yes": 0.050, "no": 0.960},
            "14°C":  {"yes": 0.380, "no": 0.640},
            "15°C":  {"yes": 0.410, "no": 0.620},
            "16°C":  {"yes": 0.165, "no": 0.848},
            "17°C+": {"yes": 0.054, "no": 0.981},
        },
        "forecast": "15°C",
        "real_prob": {
            "≤9°C": 0.001, "10°C": 0.002, "11°C": 0.005, "12°C": 0.015,
            "13°C": 0.05, "14°C": 0.30, "15°C": 0.35, "16°C": 0.18,
            "17°C+": 0.097,
        },
    },
    "Wellington": {
        "buckets": {
            "14°C":  {"yes": 0.001, "no": 0.000},
            "15°C":  {"yes": 0.010, "no": 0.994},
            "16°C":  {"yes": 0.014, "no": 0.989},
            "17°C":  {"yes": 0.040, "no": 0.969},
            "18°C":  {"yes": 0.140, "no": 0.880},
            "19°C":  {"yes": 0.280, "no": 0.730},
            "20°C":  {"yes": 0.330, "no": 0.700},
            "21°C+": {"yes": 0.280, "no": 0.740},
        },
        "forecast": "20°C",
        "real_prob": {
            "14°C": 0.001, "15°C": 0.005, "16°C": 0.01, "17°C": 0.04,
            "18°C": 0.10, "19°C": 0.25, "20°C": 0.32, "21°C+": 0.274,
        },
    },
    "NYC": {
        "buckets": {
            "≤37°F":  {"yes": 0.009, "no": 0.993},
            "38-39°F":{"yes": 0.006, "no": 0.997},
            "40-41°F":{"yes": 0.022, "no": 0.979},
            "42-43°F":{"yes": 0.190, "no": 0.840},
            "44-45°F":{"yes": 0.350, "no": 0.670},
            "46-47°F":{"yes": 0.260, "no": 0.750},
            "48-49°F":{"yes": 0.140, "no": 0.880},
            "50-51°F":{"yes": 0.045, "no": 0.959},
            "52°F+":  {"yes": 0.019, "no": 0.983},
        },
        "forecast": "44-45°F",
        "real_prob": {
            "≤37°F": 0.005, "38-39°F": 0.01, "40-41°F": 0.03,
            "42-43°F": 0.18, "44-45°F": 0.34, "46-47°F": 0.24,
            "48-49°F": 0.12, "50-51°F": 0.05, "52°F+": 0.025,
        },
    },
    "Atlanta": {
        "buckets": {
            "≤57°F":  {"yes": 0.003, "no": 0.998},
            "58-59°F":{"yes": 0.005, "no": 0.998},
            "60-61°F":{"yes": 0.006, "no": 0.998},
            "62-63°F":{"yes": 0.008, "no": 0.998},
            "64-65°F":{"yes": 0.011, "no": 0.995},
            "66-67°F":{"yes": 0.012, "no": 0.991},
            "68-69°F":{"yes": 0.033, "no": 0.968},
            "70-71°F":{"yes": 0.090, "no": 0.920},
            "72°F+":  {"yes": 0.860, "no": 0.150},
        },
        "forecast": "72°F+",
        "real_prob": {
            "≤57°F": 0.002, "58-59°F": 0.003, "60-61°F": 0.005,
            "62-63°F": 0.008, "64-65°F": 0.01, "66-67°F": 0.015,
            "68-69°F": 0.04, "70-71°F": 0.09, "72°F+": 0.827,
        },
    },
}


# ─── CORE LOGIC ──────────────────────────────────────────────────────────────

@dataclass
class Trade:
    city: str
    bucket: str
    side: str       # "NO"
    price: float
    size: float
    shares: float

    @property
    def profit_if_win(self) -> float:
        return (self.shares * 1.00) - self.size

    @property
    def loss_if_lose(self) -> float:
        return self.size


def pick_no_trades(city_name: str, city_data: dict, stake: float) -> List[Trade]:
    """
    Both strategies use the SAME logic: buy NO on dead buckets.
    Dead bucket = far from forecast center AND NO price offers real profit.
    Skip buckets where NO costs 99.8¢+ (no profit) or near forecast (risky).
    """
    trades = []
    forecast = city_data["forecast"]
    buckets = city_data["buckets"]
    bucket_names = list(buckets.keys())
    forecast_idx = bucket_names.index(forecast)

    for i, (bucket, data) in enumerate(buckets.items()):
        no_price = data["no"]
        distance = abs(i - forecast_idx)

        # Skip: too close to forecast (distance < 2)
        if distance < 2:
            continue
        # Skip: no liquidity (NO price is 0 or effectively 1.00)
        if no_price <= 0.001 or no_price >= 0.999:
            continue
        # Skip: profit too thin (NO costs 99.8¢+ = <0.2% return)
        if no_price >= 0.998:
            continue

        shares = stake / no_price
        trades.append(Trade(city_name, bucket, "NO", no_price, stake, shares))

    return trades


def pnl_for_outcome(trades: List[Trade], city: str, winning_bucket: str) -> float:
    """P&L for trades in a specific city given the winning bucket."""
    pnl = 0.0
    for t in trades:
        if t.city != city:
            continue
        if t.bucket == winning_bucket:
            pnl -= t.loss_if_lose   # NO loses when that bucket wins
        else:
            pnl += t.profit_if_win  # NO wins when that bucket doesn't win
    return pnl


# ─── SIMULATION ──────────────────────────────────────────────────────────────

def main():
    BANKROLL = 147.45  # From screenshots
    YOU_STAKE = 10.0
    GOP_STAKE = 1.0

    W = 78

    # Build trades for both
    you_trades: List[Trade] = []
    gop_trades: List[Trade] = []

    for city_name, city_data in CITIES.items():
        you_trades.extend(pick_no_trades(city_name, city_data, YOU_STAKE))
        gop_trades.extend(pick_no_trades(city_name, city_data, GOP_STAKE))

    you_deployed = sum(t.size for t in you_trades)
    gop_deployed = sum(t.size for t in gop_trades)

    # ── TRADE TABLE ──────────────────────────────────────────────────────
    print(f"\n{'='*W}")
    print(f"  HEAD-TO-HEAD: $10/stake YOU vs $1/stake gopfan2")
    print(f"  Same NO-on-dead-buckets strategy, 5 real cities, March 15")
    print(f"  Bankroll: ${BANKROLL:.2f}")
    print(f"{'='*W}")

    for city_name in CITIES:
        city_you = [t for t in you_trades if t.city == city_name]
        city_gop = [t for t in gop_trades if t.city == city_name]
        fc = CITIES[city_name]["forecast"]
        print(f"\n  {city_name} (forecast: {fc})")
        print(f"  {'Bucket':<12} {'NO Price':>8} {'YOU $10':>10} {'gop $1':>10} {'Profit/share':>13}")
        print(f"  {'-'*12} {'-'*8} {'-'*10} {'-'*10} {'-'*13}")
        # Merge both lists (same buckets)
        done = set()
        for t in city_you:
            gt = [g for g in city_gop if g.bucket == t.bucket]
            profit_pct = (t.profit_if_win / t.size) * 100 if t.size > 0 else 0
            gop_size = f"${gt[0].size:.0f}" if gt else "—"
            print(f"  {t.bucket:<12} {t.price:>7.3f}¢ {'$10':>10} {gop_size:>10} {profit_pct:>11.1f}%")
            done.add(t.bucket)
        for t in city_gop:
            if t.bucket not in done:
                profit_pct = (t.profit_if_win / t.size) * 100 if t.size > 0 else 0
                print(f"  {t.bucket:<12} {t.price:>7.3f}¢ {'—':>10} {'$1':>10} {profit_pct:>11.1f}%")
        you_city_total = sum(t.size for t in city_you)
        gop_city_total = sum(t.size for t in city_gop)
        print(f"  {'TOTAL':<12} {'':>8} {'$'+str(int(you_city_total)):>10} {'$'+str(int(gop_city_total)):>10}")

    print(f"\n  {'─'*W}")
    print(f"  ALL CITIES DEPLOYED:  YOU=${you_deployed:.0f}   gopfan2=${gop_deployed:.0f}")
    print(f"  Bankroll usage:       YOU={you_deployed/BANKROLL*100:.0f}%    gopfan2={gop_deployed/BANKROLL*100:.0f}%")

    # ── OUTCOME TABLE PER CITY ───────────────────────────────────────────
    print(f"\n{'='*W}")
    print(f"  P&L BY OUTCOME — EVERY CITY")
    print(f"{'='*W}")

    total_you_ev = 0.0
    total_gop_ev = 0.0

    for city_name, city_data in CITIES.items():
        fc = city_data["forecast"]
        print(f"\n  {city_name} (forecast: {fc})")
        print(f"  {'Outcome':<12} {'Prob':>6} {'YOU P&L':>10} {'gop P&L':>10} {'Winner':>8}")
        print(f"  {'-'*12} {'-'*6} {'-'*10} {'-'*10} {'-'*8}")

        city_you_ev = 0.0
        city_gop_ev = 0.0

        for bucket in city_data["buckets"]:
            prob = city_data["real_prob"][bucket]
            y_pnl = pnl_for_outcome(you_trades, city_name, bucket)
            g_pnl = pnl_for_outcome(gop_trades, city_name, bucket)
            city_you_ev += prob * y_pnl
            city_gop_ev += prob * g_pnl

            winner = "YOU" if y_pnl > g_pnl else "gop" if g_pnl > y_pnl else "tie"
            ys = "+" if y_pnl >= 0 else ""
            gs = "+" if g_pnl >= 0 else ""
            print(f"  {bucket:<12} {prob:>5.1%} {ys}${y_pnl:>8.2f} {gs}${g_pnl:>8.2f} {winner:>8}")

        total_you_ev += city_you_ev
        total_gop_ev += city_gop_ev
        print(f"  {'EV':.<12} {'':>6} ${city_you_ev:>+8.2f} ${city_gop_ev:>+8.2f}")

    # ── DAILY SUMMARY ────────────────────────────────────────────────────
    print(f"\n{'='*W}")
    print(f"  DAILY SUMMARY — ALL 5 CITIES COMBINED")
    print(f"{'='*W}")

    # Simulate all possible daily combos via Monte Carlo
    SIMS = 50000
    you_daily_pnls = []
    gop_daily_pnls = []

    for _ in range(SIMS):
        day_you = 0.0
        day_gop = 0.0
        for city_name, city_data in CITIES.items():
            buckets = list(city_data["real_prob"].keys())
            probs = list(city_data["real_prob"].values())
            outcome = random.choices(buckets, weights=probs, k=1)[0]
            day_you += pnl_for_outcome(you_trades, city_name, outcome)
            day_gop += pnl_for_outcome(gop_trades, city_name, outcome)
        you_daily_pnls.append(day_you)
        gop_daily_pnls.append(day_gop)

    you_avg = sum(you_daily_pnls) / SIMS
    gop_avg = sum(gop_daily_pnls) / SIMS
    you_med = sorted(you_daily_pnls)[SIMS // 2]
    gop_med = sorted(gop_daily_pnls)[SIMS // 2]
    you_best = max(you_daily_pnls)
    you_worst = min(you_daily_pnls)
    gop_best = max(gop_daily_pnls)
    gop_worst = min(gop_daily_pnls)
    you_win_rate = sum(1 for x in you_daily_pnls if x > 0) / SIMS
    gop_win_rate = sum(1 for x in gop_daily_pnls if x > 0) / SIMS
    you_beats_gop = sum(1 for y, g in zip(you_daily_pnls, gop_daily_pnls) if y > g) / SIMS
    you_p5 = sorted(you_daily_pnls)[int(SIMS * 0.05)]
    you_p95 = sorted(you_daily_pnls)[int(SIMS * 0.95)]
    gop_p5 = sorted(gop_daily_pnls)[int(SIMS * 0.05)]
    gop_p95 = sorted(gop_daily_pnls)[int(SIMS * 0.95)]

    print(f"\n  {'':.<28} {'YOU ($10)':>12} {'gopfan2 ($1)':>14}")
    print(f"  {'─'*28} {'─'*12} {'─'*14}")
    print(f"  {'Deployed per day':<28} ${you_deployed:>10.0f} ${gop_deployed:>12.0f}")
    print(f"  {'Expected daily P&L':<28} ${you_avg:>+10.2f} ${gop_avg:>+12.2f}")
    print(f"  {'Median daily P&L':<28} ${you_med:>+10.2f} ${gop_med:>+12.2f}")
    print(f"  {'Best day':<28} ${you_best:>+10.2f} ${gop_best:>+12.2f}")
    print(f"  {'Worst day':<28} ${you_worst:>+10.2f} ${gop_worst:>+12.2f}")
    print(f"  {'5th percentile':<28} ${you_p5:>+10.2f} ${gop_p5:>+12.2f}")
    print(f"  {'95th percentile':<28} ${you_p95:>+10.2f} ${gop_p95:>+12.2f}")
    print(f"  {'Win rate (profit day)':<28} {you_win_rate:>10.0%} {gop_win_rate:>12.0%}")
    print(f"  {'Daily ROI':<28} {you_avg/you_deployed*100:>+9.1f}% {gop_avg/gop_deployed*100:>+11.1f}%")

    # ── 30-DAY PROJECTION ────────────────────────────────────────────────
    print(f"\n{'='*W}")
    print(f"  30-DAY PROJECTION (Monte Carlo, {SIMS:,} sims)")
    print(f"{'='*W}")

    DAYS = 30
    SIMS_30 = 20000
    you_30 = []
    gop_30 = []

    for _ in range(SIMS_30):
        yb = BANKROLL
        gb = BANKROLL
        for _ in range(DAYS):
            for city_name, city_data in CITIES.items():
                buckets = list(city_data["real_prob"].keys())
                probs = list(city_data["real_prob"].values())
                outcome = random.choices(buckets, weights=probs, k=1)[0]
                yb += pnl_for_outcome(you_trades, city_name, outcome)
                gb += pnl_for_outcome(gop_trades, city_name, outcome)
        you_30.append(yb)
        gop_30.append(gb)

    y30_avg = sum(you_30) / SIMS_30
    g30_avg = sum(gop_30) / SIMS_30
    y30_med = sorted(you_30)[SIMS_30 // 2]
    g30_med = sorted(gop_30)[SIMS_30 // 2]
    y30_bust = sum(1 for x in you_30 if x <= 0) / SIMS_30
    g30_bust = sum(1 for x in gop_30 if x <= 0) / SIMS_30
    y30_p5 = sorted(you_30)[int(SIMS_30 * 0.05)]
    y30_p95 = sorted(you_30)[int(SIMS_30 * 0.95)]
    g30_p5 = sorted(gop_30)[int(SIMS_30 * 0.05)]
    g30_p95 = sorted(gop_30)[int(SIMS_30 * 0.95)]
    y30_best = max(you_30)
    g30_best = max(gop_30)
    y_beats_g_30 = sum(1 for y, g in zip(you_30, gop_30) if y > g) / SIMS_30

    print(f"\n  {'':.<28} {'YOU ($10)':>12} {'gopfan2 ($1)':>14}")
    print(f"  {'─'*28} {'─'*12} {'─'*14}")
    print(f"  {'Start':<28} ${BANKROLL:>10.2f} ${BANKROLL:>12.2f}")
    print(f"  {'Avg final (30d)':<28} ${y30_avg:>10.2f} ${g30_avg:>12.2f}")
    print(f"  {'Median final (30d)':<28} ${y30_med:>10.2f} ${g30_med:>12.2f}")
    print(f"  {'5th pctl (bad luck)':<28} ${y30_p5:>10.2f} ${g30_p5:>12.2f}")
    print(f"  {'95th pctl (good luck)':<28} ${y30_p95:>10.2f} ${g30_p95:>12.2f}")
    print(f"  {'Best 30d run':<28} ${y30_best:>10.2f} ${g30_best:>12.2f}")
    print(f"  {'Bust rate (<$0)':<28} {y30_bust:>10.1%} {g30_bust:>12.1%}")
    print(f"  {'30d ROI':<28} {(y30_avg-BANKROLL)/BANKROLL*100:>+9.1f}% {(g30_avg-BANKROLL)/BANKROLL*100:>+11.1f}%")
    print(f"  {'30d total profit':<28} ${y30_avg-BANKROLL:>+10.2f} ${g30_avg-BANKROLL:>+12.2f}")
    print(f"\n  YOU beats gopfan2 in {y_beats_g_30:.0%} of 30-day runs")

    # ── VERDICT ──────────────────────────────────────────────────────────
    print(f"\n{'='*W}")
    print(f"  VERDICT")
    print(f"{'='*W}")

    you_daily_ev = total_you_ev
    gop_daily_ev = total_gop_ev

    print(f"""
  Expected daily P&L:  YOU ${you_daily_ev:>+.2f}   gopfan2 ${gop_daily_ev:>+.2f}
  Expected 30d profit: YOU ${you_daily_ev*30:>+.2f}  gopfan2 ${gop_daily_ev*30:>+.2f}

  Same strategy. Same buckets. Same edge.
  Only difference: stake size.

  $10/stake = 10x the profit, 10x the risk, 10x the capital needed.
  $1/stake  = survives anything, but needs volume to matter.

  At 5 cities/day:
    YOU makes ${you_daily_ev:.2f}/day on ${you_deployed:.0f} deployed
    gopfan2 makes ${gop_daily_ev:.2f}/day on ${gop_deployed:.0f} deployed

  gopfan2 would need {int(you_daily_ev / gop_daily_ev) if gop_daily_ev > 0 else '∞'} cities at $1
  to match YOUR 5-city $10 income.
""")


if __name__ == "__main__":
    main()
