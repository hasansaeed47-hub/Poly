#!/usr/bin/env python3
"""
compact.py — Compress raw signal JSONL into analysis-ready CSV.

Drops the full orderbook arrays (90% of file size), keeps all scalar fields.
Adds derived depth-profile summaries so you lose no analytical value.

Usage:
    python3 compact.py                     # today's signals
    python3 compact.py 2026-03-14          # specific date
    python3 compact.py 2026-03-10 2026-03-16  # date range
    python3 compact.py --all               # all signal files

Output: data/compact_signals_{date}.csv  (~50-100 MB/day vs ~8 GB raw)

The CSV is small enough for Claude Code analysis.
"""

import json, csv, sys, os, glob
from datetime import datetime, timedelta

# Scalar fields to keep (everything except the 4 orderbook arrays)
BOOK_ARRAY_FIELDS = {"asks_yes", "bids_yes", "asks_no", "bids_no"}

# Derived depth-profile fields we compute from the arrays before dropping them
DEPTH_FIELDS = [
    "asks_yes_levels", "bids_yes_levels", "asks_no_levels", "bids_no_levels",
    "depth_usd_yes_ask_5", "depth_usd_yes_bid_5", "depth_usd_no_ask_5", "depth_usd_no_bid_5",
    "depth_usd_yes_ask_10", "depth_usd_yes_bid_10", "depth_usd_no_ask_10", "depth_usd_no_bid_10",
    "depth_usd_yes_ask_all", "depth_usd_yes_bid_all", "depth_usd_no_ask_all", "depth_usd_no_bid_all",
    "vwap_yes_ask_100", "vwap_no_ask_100",  # VWAP for $100 stake (arb sizing)
    "vwap_yes_ask_500", "vwap_no_ask_500",  # VWAP for $500 stake
    "slippage_yes_100", "slippage_no_100",  # slippage vs best ask at $100
    "slippage_yes_500", "slippage_no_500",  # slippage vs best ask at $500
]


def depth_usd(levels, n=None):
    """Sum USD notional (price * size) for top N levels."""
    subset = levels[:n] if n else levels
    return sum(p * s for p, s in subset)


def vwap_fill(levels, stake_usd):
    """Walk the book for a given USD stake. Returns avg fill price."""
    if not levels or stake_usd <= 0:
        return 0.0
    budget = stake_usd
    total_cost = 0.0
    total_shares = 0.0
    for price, size in levels:
        if budget <= 0.001 or price <= 0:
            break
        shares_wanted = budget / price
        shares = min(shares_wanted, size)
        cost = shares * price
        total_shares += shares
        total_cost += cost
        budget -= cost
    return total_cost / total_shares if total_shares > 0 else 0.0


def enrich_row(row):
    """Compute depth summaries from orderbook arrays, then drop arrays."""
    for side_key, prefix in [
        ("asks_yes", "yes_ask"), ("bids_yes", "yes_bid"),
        ("asks_no", "no_ask"), ("bids_no", "no_bid"),
    ]:
        levels = row.get(side_key, []) or []
        row[f"{side_key}_levels"] = len(levels)
        row[f"depth_usd_{prefix}_5"] = round(depth_usd(levels, 5), 2)
        row[f"depth_usd_{prefix}_10"] = round(depth_usd(levels, 10), 2)
        row[f"depth_usd_{prefix}_all"] = round(depth_usd(levels), 2)

    # VWAP + slippage at realistic stakes
    for outcome, ask_key in [("yes", "asks_yes"), ("no", "asks_no")]:
        levels = row.get(ask_key, []) or []
        best_ask = levels[0][0] if levels else 0.0
        for stake in [100, 500]:
            vwap = vwap_fill(levels, stake)
            row[f"vwap_{outcome}_ask_{stake}"] = round(vwap, 6)
            row[f"slippage_{outcome}_{stake}"] = round(vwap - best_ask, 6) if best_ask > 0 else 0.0

    # Drop the arrays
    for k in BOOK_ARRAY_FIELDS:
        row.pop(k, None)

    return row


def process_file(input_path, output_path):
    """Convert one signal JSONL to compact CSV."""
    rows = []
    fieldnames = None

    with open(input_path) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue

            row = enrich_row(row)

            if fieldnames is None:
                fieldnames = list(row.keys())
            rows.append(row)

            if line_num % 100000 == 0:
                print(f"  processed {line_num:,} lines...")

    if not rows:
        print(f"  SKIP (empty): {input_path}")
        return 0

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    in_mb = os.path.getsize(input_path) / 1_048_576
    out_mb = os.path.getsize(output_path) / 1_048_576
    ratio = in_mb / out_mb if out_mb > 0 else 0
    print(f"  {input_path}: {in_mb:.1f} MB -> {output_path}: {out_mb:.1f} MB ({ratio:.0f}x compression)")
    return len(rows)


def get_dates(args):
    """Parse CLI args into list of date strings."""
    if not args or args == ["--all"]:
        # Find all signal files
        files = sorted(glob.glob("data/signals_*.jsonl"))
        return [f.replace("data/signals_", "").replace(".jsonl", "") for f in files]

    if len(args) == 1:
        return [args[0]]

    # Date range
    start = datetime.strptime(args[0], "%Y-%m-%d")
    end = datetime.strptime(args[1], "%Y-%m-%d")
    dates = []
    d = start
    while d <= end:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return dates


def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)) + "/..")

    dates = get_dates(sys.argv[1:])
    if not dates:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        dates = [today]

    total_rows = 0
    for date in dates:
        input_path = f"data/signals_{date}.jsonl"
        output_path = f"data/compact_signals_{date}.csv"

        if not os.path.exists(input_path):
            print(f"  SKIP (not found): {input_path}")
            continue

        print(f"Processing {date}...")
        total_rows += process_file(input_path, output_path)

    print(f"\nDone. {total_rows:,} total rows across {len(dates)} days.")
    print(f"Download with: curl http://localhost:8080/section/compact_signals -o compact.jsonl")
    print(f"Or scp: scp vps:Poly/oracle-scanner-v2/data/compact_signals_*.csv .")


if __name__ == "__main__":
    main()
