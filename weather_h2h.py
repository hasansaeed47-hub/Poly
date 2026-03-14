#!/usr/bin/env python3
"""
WEATHER HEAD-TO-HEAD: $10/stake vs gopfan2 $1/stake
=====================================================
3 Strategies compared on same 5 real cities (March 15 2026):
  A) NO-only on dead buckets
  B) YES-only on forecast-aligned buckets
  C) HYBRID: NO on dead + YES on forecast center
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


# ─── CORE ────────────────────────────────────────────────────────────────────

@dataclass
class Trade:
    city: str
    bucket: str
    side: str       # "YES" or "NO"
    price: float
    size: float
    shares: float

    @property
    def profit_if_win(self) -> float:
        return (self.shares * 1.00) - self.size

    @property
    def loss_if_lose(self) -> float:
        return self.size


def pnl_for_outcome(trades: List[Trade], city: str, winning_bucket: str) -> float:
    pnl = 0.0
    for t in trades:
        if t.city != city:
            continue
        if t.side == "NO":
            if t.bucket == winning_bucket:
                pnl -= t.loss_if_lose
            else:
                pnl += t.profit_if_win
        else:  # YES
            if t.bucket == winning_bucket:
                pnl += t.profit_if_win
            else:
                pnl -= t.loss_if_lose
    return pnl


def pnl_all_cities(trades: List[Trade], outcomes: Dict[str, str]) -> float:
    total = 0.0
    for city, winner in outcomes.items():
        total += pnl_for_outcome(trades, city, winner)
    return total


# ─── STRATEGY BUILDERS ──────────────────────────────────────────────────────

def build_no_only(stake: float) -> List[Trade]:
    """Strategy A: NO on dead buckets (2+ away from forecast, NO < 99.8¢)."""
    trades = []
    for city_name, cd in CITIES.items():
        bucket_names = list(cd["buckets"].keys())
        fc_idx = bucket_names.index(cd["forecast"])
        for i, (bucket, data) in enumerate(cd["buckets"].items()):
            no_price = data["no"]
            if abs(i - fc_idx) < 2:
                continue
            if no_price <= 0.001 or no_price >= 0.998:
                continue
            trades.append(Trade(city_name, bucket, "NO", no_price, stake, stake / no_price))
    return trades


def build_yes_only(stake: float) -> List[Trade]:
    """Strategy B: YES on forecast center + adjacent buckets where market underprices."""
    trades = []
    for city_name, cd in CITIES.items():
        bucket_names = list(cd["buckets"].keys())
        fc_idx = bucket_names.index(cd["forecast"])
        for i, (bucket, data) in enumerate(cd["buckets"].items()):
            yes_price = data["yes"]
            real_prob = cd["real_prob"][bucket]
            distance = abs(i - fc_idx)
            # Only bet on forecast center and ±1 adjacent
            if distance > 1:
                continue
            # Only if real prob > market price (any positive edge)
            edge = real_prob - yes_price
            if edge < 0.0:
                continue
            # Skip if YES price is too high (>90¢ = overpaying)
            if yes_price > 0.90:
                continue
            trades.append(Trade(city_name, bucket, "YES", yes_price, stake, stake / yes_price))
    return trades


def build_hybrid(stake: float) -> List[Trade]:
    """Strategy C: NO on dead buckets + YES on forecast center."""
    trades = []
    for city_name, cd in CITIES.items():
        bucket_names = list(cd["buckets"].keys())
        fc_idx = bucket_names.index(cd["forecast"])
        for i, (bucket, data) in enumerate(cd["buckets"].items()):
            distance = abs(i - fc_idx)
            real_prob = cd["real_prob"][bucket]

            # NO on dead buckets (far from forecast)
            if distance >= 2:
                no_price = data["no"]
                if 0.001 < no_price < 0.998:
                    trades.append(Trade(city_name, bucket, "NO", no_price, stake, stake / no_price))

            # YES on forecast center + adjacent (where edge exists)
            if distance <= 1:
                yes_price = data["yes"]
                edge = real_prob - yes_price
                if edge >= 0.0 and yes_price <= 0.90:
                    trades.append(Trade(city_name, bucket, "YES", yes_price, stake, stake / yes_price))
    return trades


# ─── SIMULATION ──────────────────────────────────────────────────────────────

def calc_ev(trades: List[Trade]) -> float:
    ev = 0.0
    for city_name, cd in CITIES.items():
        for bucket, prob in cd["real_prob"].items():
            ev += prob * pnl_for_outcome(trades, city_name, bucket)
    return ev


def monte_carlo_day(trades: List[Trade], sims: int = 50000) -> List[float]:
    results = []
    for _ in range(sims):
        day_pnl = 0.0
        for city_name, cd in CITIES.items():
            buckets = list(cd["real_prob"].keys())
            probs = list(cd["real_prob"].values())
            outcome = random.choices(buckets, weights=probs, k=1)[0]
            day_pnl += pnl_for_outcome(trades, city_name, outcome)
        results.append(day_pnl)
    return results


def monte_carlo_30d(trades: List[Trade], bankroll: float, sims: int = 20000) -> List[float]:
    results = []
    for _ in range(sims):
        bank = bankroll
        for _ in range(30):
            for city_name, cd in CITIES.items():
                buckets = list(cd["real_prob"].keys())
                probs = list(cd["real_prob"].values())
                outcome = random.choices(buckets, weights=probs, k=1)[0]
                bank += pnl_for_outcome(trades, city_name, outcome)
        results.append(bank)
    return results


def print_trades_table(name: str, trades: List[Trade]):
    print(f"\n  {name}")
    for city_name in CITIES:
        ct = [t for t in trades if t.city == city_name]
        if not ct:
            continue
        fc = CITIES[city_name]["forecast"]
        print(f"    {city_name} (fc: {fc})")
        for t in ct:
            pct = (t.profit_if_win / t.size * 100) if t.size > 0 else 0
            print(f"      {t.side:<3} {t.bucket:<12} @{t.price:.3f}  ${t.size:.0f}  edge:{pct:>+.1f}%")
    deployed = sum(t.size for t in trades)
    print(f"    Total deployed: ${deployed:.0f}")


def stats_block(label: str, daily: List[float], monthly: List[float],
                deployed: float, bankroll: float):
    avg_d = sum(daily) / len(daily)
    med_d = sorted(daily)[len(daily) // 2]
    best_d = max(daily)
    worst_d = min(daily)
    win_rate = sum(1 for x in daily if x > 0) / len(daily)
    p5_d = sorted(daily)[int(len(daily) * 0.05)]
    p95_d = sorted(daily)[int(len(daily) * 0.95)]

    avg_m = sum(monthly) / len(monthly)
    med_m = sorted(monthly)[len(monthly) // 2]
    p5_m = sorted(monthly)[int(len(monthly) * 0.05)]
    p95_m = sorted(monthly)[int(len(monthly) * 0.95)]
    bust = sum(1 for x in monthly if x <= 0) / len(monthly)

    return {
        "label": label, "deployed": deployed,
        "avg_d": avg_d, "med_d": med_d, "best_d": best_d, "worst_d": worst_d,
        "win_rate": win_rate, "p5_d": p5_d, "p95_d": p95_d,
        "roi_d": avg_d / deployed * 100 if deployed > 0 else 0,
        "avg_m": avg_m, "med_m": med_m, "p5_m": p5_m, "p95_m": p95_m,
        "bust": bust, "profit_30": avg_m - bankroll,
        "roi_30": (avg_m - bankroll) / bankroll * 100,
    }


def main():
    BANKROLL = 147.45
    W = 82

    # Build all 6 strategy variants (3 strategies × 2 stake sizes)
    strategies = {
        "A) NO-only $10":    build_no_only(10.0),
        "A) NO-only $1":     build_no_only(1.0),
        "B) YES-only $10":   build_yes_only(10.0),
        "B) YES-only $1":    build_yes_only(1.0),
        "C) HYBRID $10":     build_hybrid(10.0),
        "C) HYBRID $1":      build_hybrid(1.0),
    }

    print(f"\n{'='*W}")
    print(f"  WEATHER H2H: 3 Strategies × 2 Stake Sizes — 5 Real Cities")
    print(f"  Seoul | Shanghai | Wellington | NYC | Atlanta — March 15, 2026")
    print(f"  Bankroll: ${BANKROLL:.2f}")
    print(f"{'='*W}")

    # ── Show trades for each strategy ────────────────────────────────────
    for name, trades in strategies.items():
        print_trades_table(name, trades)

    # ── Expected Value per city ──────────────────────────────────────────
    print(f"\n{'='*W}")
    print(f"  EXPECTED VALUE PER CITY (analytical)")
    print(f"{'='*W}")

    strat_names = list(strategies.keys())
    header = f"  {'City':<12}" + "".join(f"{s:>13}" for s in strat_names)
    print(f"\n{header}")
    print(f"  {'-'*12}" + "".join(f" {'-'*12}" for _ in strat_names))

    for city_name, cd in CITIES.items():
        row = f"  {city_name:<12}"
        for sname, trades in strategies.items():
            ev = 0.0
            for bucket, prob in cd["real_prob"].items():
                ev += prob * pnl_for_outcome(trades, city_name, bucket)
            row += f" ${ev:>+10.2f} "
        print(row)

    # Totals
    row = f"  {'TOTAL':<12}"
    for sname, trades in strategies.items():
        ev = calc_ev(trades)
        row += f" ${ev:>+10.2f} "
    print(f"  {'─'*12}" + "".join(f" {'─'*12}" for _ in strat_names))
    print(row)

    # ── Monte Carlo ──────────────────────────────────────────────────────
    print(f"\n{'='*W}")
    print(f"  MONTE CARLO SIMULATION")
    print(f"{'='*W}")
    print(f"  Running 50k daily sims + 20k 30-day sims per strategy...")

    all_stats = {}
    for sname, trades in strategies.items():
        deployed = sum(t.size for t in trades)
        daily = monte_carlo_day(trades, 50000)
        monthly = monte_carlo_30d(trades, BANKROLL, 20000)
        all_stats[sname] = stats_block(sname, daily, monthly, deployed, BANKROLL)

    # ── Daily comparison ─────────────────────────────────────────────────
    print(f"\n  {'DAILY P&L':─<{W}}")
    print(f"\n  {'':.<24}" + "".join(f"{s:>13}" for s in strat_names))
    print(f"  {'─'*24}" + "".join(f" {'─'*12}" for _ in strat_names))

    fields_d = [
        ("Deployed/day", "deployed", "${:>.0f}"),
        ("Avg daily P&L", "avg_d", "${:>+.2f}"),
        ("Median daily", "med_d", "${:>+.2f}"),
        ("Best day", "best_d", "${:>+.2f}"),
        ("Worst day", "worst_d", "${:>+.2f}"),
        ("5th pctl", "p5_d", "${:>+.2f}"),
        ("95th pctl", "p95_d", "${:>+.2f}"),
        ("Win rate", "win_rate", "{:>.0%}"),
        ("Daily ROI", "roi_d", "{:>+.1f}%"),
    ]

    for label, key, fmt in fields_d:
        row = f"  {label:<24}"
        for sname in strat_names:
            val = all_stats[sname][key]
            if fmt.startswith("$"):
                cell = fmt.replace("$", "$").format(val)
            else:
                cell = fmt.format(val)
            row += f" {cell:>12}"
        print(row)

    # ── 30-day comparison ────────────────────────────────────────────────
    print(f"\n  {'30-DAY PROJECTION':─<{W}}")
    print(f"\n  {'':.<24}" + "".join(f"{s:>13}" for s in strat_names))
    print(f"  {'─'*24}" + "".join(f" {'─'*12}" for _ in strat_names))

    fields_m = [
        ("Avg final", "avg_m", "${:>.2f}"),
        ("Median final", "med_m", "${:>.2f}"),
        ("5th pctl", "p5_m", "${:>.2f}"),
        ("95th pctl", "p95_m", "${:>.2f}"),
        ("Bust rate", "bust", "{:>.1%}"),
        ("30d profit", "profit_30", "${:>+.2f}"),
        ("30d ROI", "roi_30", "{:>+.1f}%"),
    ]

    for label, key, fmt in fields_m:
        row = f"  {label:<24}"
        for sname in strat_names:
            val = all_stats[sname][key]
            if fmt.startswith("$"):
                cell = fmt.replace("$", "$").format(val)
            else:
                cell = fmt.format(val)
            row += f" {cell:>12}"
        print(row)

    # ── VERDICT ──────────────────────────────────────────────────────────
    print(f"\n{'='*W}")
    print(f"  VERDICT")
    print(f"{'='*W}")

    # Find best strategy
    best_name = max(all_stats, key=lambda s: all_stats[s]["avg_d"])
    best = all_stats[best_name]
    best_roi_name = max(all_stats, key=lambda s: all_stats[s]["roi_d"])
    best_roi = all_stats[best_roi_name]
    safest_name = min(all_stats, key=lambda s: all_stats[s]["bust"])
    safest = all_stats[safest_name]

    print(f"""
  HIGHEST DAILY PROFIT:  {best_name}
    ${best['avg_d']:+.2f}/day on ${best['deployed']:.0f} deployed

  BEST ROI:              {best_roi_name}
    {best_roi['roi_d']:+.1f}% daily on ${best_roi['deployed']:.0f} deployed

  SAFEST (lowest bust):  {safest_name}
    {safest['bust']:.1%} bust rate over 30 days

  COMPARISON — NO-only vs YES-only vs HYBRID:
    NO-only:  grinds thin margins, loses to tail risk on these markets
    YES-only: high variance, big wins when forecast hits, big loss when not
    HYBRID:   NO income cushions YES losses → smoother equity curve

  COMPARISON — $10 vs $1 stake:
    $10: 10x profit AND 10x risk. Works if you pick cities with fat edges.
    $1:  nearly unkillable. Needs 10+ cities to generate real income.
""")


if __name__ == "__main__":
    main()
