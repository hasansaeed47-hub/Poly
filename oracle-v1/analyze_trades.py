#!/usr/bin/env python3
"""
Trade Analysis Engine — oracle-v1
===================================
Reads JSONL trade logs (live + paper) and identifies:
  1. Patterns that predict LOSSES → tighten filters
  2. Filters that block WINNERS → loosen for more trades
  3. Optimal thresholds for each parameter

Usage:
  python analyze_trades.py                    # analyze all logs in logs/
  python analyze_trades.py logs/paper_base.jsonl  # analyze specific file
"""

import json
import sys
import os
from collections import defaultdict
from pathlib import Path


def load_trades(path: str) -> list[dict]:
    trades = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                trades.append(json.loads(line))
    return trades


def print_header(title: str):
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


def print_subheader(title: str):
    print(f"\n--- {title} ---")


def overview(trades: list[dict], label: str):
    print_header(f"{label} ({len(trades)} trades)")

    wins = [t for t in trades if t['net_pnl'] > 0]
    losses = [t for t in trades if t['net_pnl'] <= 0]
    total_pnl = sum(t['net_pnl'] for t in trades)
    total_staked = sum(t.get('stake', 5.0) for t in trades)
    wr = len(wins) / max(len(trades), 1) * 100

    print(f"  W={len(wins)}  L={len(losses)}  WR={wr:.1f}%")
    print(f"  PnL=${total_pnl:+.2f}  ROI={total_pnl/max(total_staked,1)*100:+.1f}%")
    if wins:
        print(f"  Avg win:  ${sum(t['net_pnl'] for t in wins)/len(wins):.2f}")
    if losses:
        print(f"  Avg loss: ${sum(t['net_pnl'] for t in losses)/len(losses):.2f}")

    return wins, losses


def bucket_analysis(trades: list[dict], field: str, buckets: list[tuple], label: str):
    """Analyze WR and PnL by bucketed field values."""
    print_subheader(f"By {label}")

    results = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0.0, 'trades': []})

    for t in trades:
        val = t.get(field)
        if val is None:
            continue

        bucket_name = None
        for lo, hi, name in buckets:
            if lo <= val < hi:
                bucket_name = name
                break
        if bucket_name is None:
            continue

        if t['net_pnl'] > 0:
            results[bucket_name]['w'] += 1
        else:
            results[bucket_name]['l'] += 1
        results[bucket_name]['pnl'] += t['net_pnl']
        results[bucket_name]['trades'].append(t)

    # Print in bucket order
    for _, _, name in buckets:
        if name not in results:
            continue
        d = results[name]
        total = d['w'] + d['l']
        wr = d['w'] / total * 100 if total else 0
        marker = "  <-- LOSING" if d['pnl'] < -1 else ("  <-- weak" if wr < 60 else "")
        print(f"  {name:18s}: {total:4d} trades  W={d['w']:3d} L={d['l']:3d}  WR={wr:5.1f}%  PnL=${d['pnl']:+8.2f}{marker}")

    return results


def categorical_analysis(trades: list[dict], field: str, label: str):
    """Analyze WR and PnL by categorical field."""
    print_subheader(f"By {label}")

    results = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0.0})
    for t in trades:
        val = str(t.get(field, '?'))
        if t['net_pnl'] > 0:
            results[val]['w'] += 1
        else:
            results[val]['l'] += 1
        results[val]['pnl'] += t['net_pnl']

    for val in sorted(results.keys()):
        d = results[val]
        total = d['w'] + d['l']
        wr = d['w'] / total * 100 if total else 0
        marker = "  <-- LOSING" if d['pnl'] < -1 else ""
        print(f"  {val:18s}: {total:4d} trades  W={d['w']:3d} L={d['l']:3d}  WR={wr:5.1f}%  PnL=${d['pnl']:+8.2f}{marker}")


def threshold_sweep(trades: list[dict], field: str, thresholds: list[float],
                    direction: str, label: str):
    """
    Sweep a threshold and show the impact of applying it as a filter.
    direction='min': keep trades where field >= threshold (raising min)
    direction='max': keep trades where field <= threshold (lowering max)
    """
    print_subheader(f"Threshold sweep: {label}")
    print(f"  {'Threshold':>12s}  {'Kept':>5s}  {'Blocked':>7s}  {'W':>4s}  {'L':>3s}  {'WR':>6s}  {'PnL':>9s}  {'Blocked_W':>9s}  {'Blocked_L':>9s}  {'Lost_PnL':>9s}")

    for thresh in thresholds:
        if direction == 'min':
            kept = [t for t in trades if t.get(field, 0) >= thresh]
            blocked = [t for t in trades if t.get(field, 0) < thresh]
        else:
            kept = [t for t in trades if t.get(field, 0) <= thresh]
            blocked = [t for t in trades if t.get(field, 0) > thresh]

        w = sum(1 for t in kept if t['net_pnl'] > 0)
        l = sum(1 for t in kept if t['net_pnl'] <= 0)
        pnl = sum(t['net_pnl'] for t in kept)
        wr = w / max(w + l, 1) * 100

        blocked_w = sum(1 for t in blocked if t['net_pnl'] > 0)
        blocked_l = sum(1 for t in blocked if t['net_pnl'] <= 0)
        lost_pnl = sum(t['net_pnl'] for t in blocked if t['net_pnl'] > 0)

        marker = ""
        if blocked_l > 0 and blocked_w == 0:
            marker = " <-- PURE WIN (blocks only losers)"
        elif blocked_w > blocked_l * 2:
            marker = " <-- OVER-FILTERED (blocks too many winners)"

        print(f"  {thresh:>12.3f}  {len(kept):5d}  {len(blocked):7d}  {w:4d}  {l:3d}  {wr:5.1f}%  ${pnl:+8.2f}  {blocked_w:9d}  {blocked_l:9d}  ${lost_pnl:+8.2f}{marker}")


def whatif_filters(trades: list[dict]):
    """Analyze what-if filter fields logged by paper runner."""
    if not any('blocked_by_momentum' in t for t in trades):
        return

    print_subheader("What-If Filter Analysis (paper runner)")

    # Momentum filter
    mom_blocked = [t for t in trades if t.get('blocked_by_momentum')]
    mom_passed = [t for t in trades if not t.get('blocked_by_momentum')]

    if mom_blocked:
        bw = sum(1 for t in mom_blocked if t['net_pnl'] > 0)
        bl = sum(1 for t in mom_blocked if t['net_pnl'] <= 0)
        bpnl = sum(t['net_pnl'] for t in mom_blocked)
        print(f"  Momentum filter would block: {len(mom_blocked)} trades (W={bw} L={bl} PnL=${bpnl:+.2f})")
        if bl > bw:
            print(f"    --> GOOD: blocks more losers than winners")
        else:
            print(f"    --> BAD: blocks more winners ({bw}) than losers ({bl})")

    # Book imbalance filter
    book_blocked = [t for t in trades if t.get('blocked_by_bookimbal')]
    if book_blocked:
        bw = sum(1 for t in book_blocked if t['net_pnl'] > 0)
        bl = sum(1 for t in book_blocked if t['net_pnl'] <= 0)
        bpnl = sum(t['net_pnl'] for t in book_blocked)
        print(f"  Book imbal filter would block: {len(book_blocked)} trades (W={bw} L={bl} PnL=${bpnl:+.2f})")
        if bl > bw:
            print(f"    --> GOOD: blocks more losers than winners")
        else:
            print(f"    --> BAD: blocks more winners ({bw}) than losers ({bl})")

    # Combined
    both_blocked = [t for t in trades if t.get('blocked_by_both')]
    if both_blocked:
        bw = sum(1 for t in both_blocked if t['net_pnl'] > 0)
        bl = sum(1 for t in both_blocked if t['net_pnl'] <= 0)
        bpnl = sum(t['net_pnl'] for t in both_blocked)
        print(f"  Either filter would block:    {len(both_blocked)} trades (W={bw} L={bl} PnL=${bpnl:+.2f})")


def losing_trades_detail(losses: list[dict]):
    """Print details of all losing trades for pattern recognition."""
    if not losses:
        print("\n  No losing trades!")
        return

    print_subheader(f"All Losing Trades ({len(losses)})")
    print(f"  {'slug':30s} {'side':4s} {'entry':>6s} {'fair':>6s} {'edge':>6s} {'sigma':>6s} {'move%':>7s} {'T-sec':>5s} {'exit':>6s} {'reason':12s} {'pnl':>8s}")

    for t in sorted(losses, key=lambda x: x['net_pnl']):
        slug = t.get('slug', '?')[:30]
        side = t.get('side', '?')[:4]
        entry = t.get('entry_price', 0)
        fair = t.get('fair_at_entry', 0)
        edge = t.get('edge_at_entry', 0)
        sigma = t.get('sigma', 0)
        pct_move = t.get('pct_move', 0)
        secs = t.get('secs_left', 0)
        exit_p = t.get('exit_price', 0)
        reason = t.get('exit_reason', '?')[:12]
        pnl = t['net_pnl']

        print(f"  {slug:30s} {side:4s} {entry:6.3f} {fair:6.3f} {edge:6.3f} {sigma:6.3f} {pct_move:7.4f} {secs:5.0f} {exit_p:6.3f} {reason:12s} ${pnl:+7.2f}")


def recommendations(trades: list[dict], wins: list[dict], losses: list[dict]):
    """Generate actionable recommendations based on data patterns."""
    print_header("RECOMMENDATIONS")

    if not trades:
        print("  No trades to analyze yet. Run the bot and check back.")
        return

    wr = len(wins) / max(len(trades), 1) * 100
    total_pnl = sum(t['net_pnl'] for t in trades)

    # 1. Identify loss patterns
    if losses:
        print("\n  [BLOCK LOSSES]")
        # Check if losses cluster at low edge
        avg_loss_edge = sum(t.get('edge_at_entry', 0) for t in losses) / len(losses)
        avg_win_edge = sum(t.get('edge_at_entry', 0) for t in wins) / max(len(wins), 1)
        if avg_loss_edge < avg_win_edge * 0.8:
            print(f"    - Losses have lower edge ({avg_loss_edge:.3f}) vs wins ({avg_win_edge:.3f})")
            print(f"      --> Consider raising min_edge to {avg_loss_edge + 0.02:.2f}")

        # Check if losses cluster at low pct_move
        loss_moves = [t.get('pct_move', 0) for t in losses if t.get('pct_move')]
        win_moves = [t.get('pct_move', 0) for t in wins if t.get('pct_move')]
        if loss_moves and win_moves:
            avg_loss_move = sum(loss_moves) / len(loss_moves)
            avg_win_move = sum(win_moves) / len(win_moves)
            if avg_loss_move < avg_win_move * 0.7:
                print(f"    - Losses have smaller moves ({avg_loss_move:.4f}%) vs wins ({avg_win_move:.4f}%)")
                print(f"      --> Consider raising min_move_pct to {avg_loss_move:.3f}")

        # Check if losses cluster at high sigma
        loss_sigmas = [t.get('sigma', 0) for t in losses if t.get('sigma')]
        win_sigmas = [t.get('sigma', 0) for t in wins if t.get('sigma')]
        if loss_sigmas and win_sigmas:
            avg_loss_sigma = sum(loss_sigmas) / len(loss_sigmas)
            avg_win_sigma = sum(win_sigmas) / len(win_sigmas)
            if avg_loss_sigma > avg_win_sigma * 1.2:
                print(f"    - Losses have higher sigma ({avg_loss_sigma:.3f}) vs wins ({avg_win_sigma:.3f})")
                print(f"      --> Consider lowering max_sigma to {avg_loss_sigma - 0.01:.2f}")

        # Check time remaining
        loss_secs = [t.get('secs_left', 0) for t in losses]
        win_secs = [t.get('secs_left', 0) for t in wins]
        if loss_secs and win_secs:
            avg_loss_secs = sum(loss_secs) / len(loss_secs)
            avg_win_secs = sum(win_secs) / len(win_secs)
            print(f"    - Avg secs_left: losses={avg_loss_secs:.0f}s  wins={avg_win_secs:.0f}s")

    # 2. Identify over-filtering (for paper configs with lower thresholds)
    if wins and len(trades) > 10:
        print("\n  [INCREASE TRADES]")
        # Check if lowering min_edge adds more wins than losses
        edge_12 = [t for t in trades if t.get('edge_at_entry', 0) >= 0.12]
        edge_15 = [t for t in trades if t.get('edge_at_entry', 0) >= 0.15]
        edge_20 = [t for t in trades if t.get('edge_at_entry', 0) >= 0.20]

        for label, subset in [("edge>=0.12", edge_12), ("edge>=0.15", edge_15), ("edge>=0.20", edge_20)]:
            w = sum(1 for t in subset if t['net_pnl'] > 0)
            l = sum(1 for t in subset if t['net_pnl'] <= 0)
            pnl = sum(t['net_pnl'] for t in subset)
            wr_s = w / max(w + l, 1) * 100
            print(f"    {label}: {len(subset)} trades  WR={wr_s:.1f}%  PnL=${pnl:+.2f}")


def analyze_file(path: str):
    trades = load_trades(path)
    if not trades:
        print(f"  {path}: empty")
        return

    label = Path(path).stem
    wins, losses = overview(trades, label)

    # Categorical breakdowns
    categorical_analysis(trades, 'asset', 'Asset')
    categorical_analysis(trades, 'side', 'Side')
    if any(t.get('tf') for t in trades):
        categorical_analysis(trades, 'tf', 'Timeframe')
    if any(t.get('exit_reason') for t in trades):
        categorical_analysis(trades, 'exit_reason', 'Exit Reason')

    # Bucketed analysis
    bucket_analysis(trades, 'edge_at_entry', [
        (0.00, 0.10, '0.00-0.10'),
        (0.10, 0.15, '0.10-0.15'),
        (0.15, 0.20, '0.15-0.20'),
        (0.20, 0.25, '0.20-0.25'),
        (0.25, 0.30, '0.25-0.30'),
        (0.30, 0.40, '0.30-0.40'),
        (0.40, 1.00, '0.40+'),
    ], 'Edge at Entry')

    if any(t.get('secs_left') for t in trades):
        bucket_analysis(trades, 'secs_left', [
            (0, 90, '<90s'),
            (90, 150, '90-150s'),
            (150, 240, '150-240s'),
            (240, 360, '240-360s'),
            (360, 600, '360-600s'),
            (600, 9999, '600s+'),
        ], 'Secs Left')

    if any(t.get('sigma') for t in trades):
        bucket_analysis(trades, 'sigma', [
            (0.00, 0.30, '<0.30'),
            (0.30, 0.32, '0.30-0.32'),
            (0.32, 0.35, '0.32-0.35'),
            (0.35, 0.40, '0.35-0.40'),
            (0.40, 0.50, '0.40-0.50'),
            (0.50, 9.00, '0.50+'),
        ], 'Sigma')

    if any(t.get('pct_move') for t in trades):
        bucket_analysis(trades, 'pct_move', [
            (0.00, 0.03, '<0.03%'),
            (0.03, 0.05, '0.03-0.05%'),
            (0.05, 0.08, '0.05-0.08%'),
            (0.08, 0.12, '0.08-0.12%'),
            (0.12, 0.20, '0.12-0.20%'),
            (0.20, 0.30, '0.20-0.30%'),
            (0.30, 99.0, '0.30%+'),
        ], 'Price Move %')

    bucket_analysis(trades, 'entry_price', [
        (0.00, 0.30, '<0.30'),
        (0.30, 0.40, '0.30-0.40'),
        (0.40, 0.50, '0.40-0.50'),
        (0.50, 0.60, '0.50-0.60'),
        (0.60, 0.70, '0.60-0.70'),
        (0.70, 0.80, '0.70-0.80'),
        (0.80, 1.00, '0.80+'),
    ], 'Entry Price')

    # Threshold sweeps (only if enough data)
    if len(trades) >= 10 and any(t.get('edge_at_entry') for t in trades):
        threshold_sweep(trades, 'edge_at_entry',
                       [0.10, 0.12, 0.15, 0.18, 0.20, 0.22, 0.25, 0.30],
                       'min', 'min_edge')

    if len(trades) >= 10 and any(t.get('pct_move') for t in trades):
        threshold_sweep(trades, 'pct_move',
                       [0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.10, 0.15],
                       'min', 'min_move_pct')

    if len(trades) >= 10 and any(t.get('sigma') for t in trades):
        threshold_sweep(trades, 'sigma',
                       [0.30, 0.32, 0.35, 0.38, 0.40, 0.45, 0.50],
                       'max', 'max_sigma')

    if len(trades) >= 10 and any(t.get('secs_left') for t in trades):
        threshold_sweep(trades, 'secs_left',
                       [60, 90, 120, 150, 180, 240, 300, 600],
                       'min', 'min_secs_left')

    # What-if filter analysis
    whatif_filters(trades)

    # Losing trades detail
    losing_trades_detail(losses)

    # Recommendations
    recommendations(trades, wins, losses)


def main():
    if len(sys.argv) > 1:
        # Specific files
        for path in sys.argv[1:]:
            if os.path.exists(path) and os.path.getsize(path) > 0:
                analyze_file(path)
    else:
        # Auto-discover logs
        log_dir = os.path.join(os.path.dirname(__file__), 'logs')
        if not os.path.exists(log_dir):
            print("No logs/ directory found. Run the bot first to generate trade data.")
            print(f"Expected location: {log_dir}")
            print("\nTrade logs will be written to:")
            print("  logs/opt2_maker.jsonl    — live execution trades")
            print("  logs/paper_base.jsonl    — paper BASE config (min_edge=0.12)")
            print("  logs/paper_time_filter.jsonl")
            print("  logs/paper_edge_filter.jsonl")
            print("  logs/paper_stop_loss.jsonl")
            print("  logs/paper_cl_target.jsonl")
            print("\nRun: cargo run --release")
            print("Then: python analyze_trades.py")
            return

        files = sorted(Path(log_dir).glob('*.jsonl'))
        if not files:
            print(f"No JSONL files in {log_dir}. Let the bot run to accumulate trades.")
            return

        print(f"Found {len(files)} log files in {log_dir}/\n")
        for f in files:
            if f.stat().st_size > 0:
                analyze_file(str(f))

        # Cross-config comparison
        if len(files) >= 2:
            print_header("CROSS-CONFIG COMPARISON")
            print(f"  {'Config':20s} {'Trades':>6s} {'W':>4s} {'L':>3s} {'WR':>6s} {'PnL':>9s} {'ROI':>7s}")
            for f in files:
                if f.stat().st_size == 0:
                    continue
                trades = load_trades(str(f))
                if not trades:
                    continue
                w = sum(1 for t in trades if t['net_pnl'] > 0)
                l = sum(1 for t in trades if t['net_pnl'] <= 0)
                pnl = sum(t['net_pnl'] for t in trades)
                staked = sum(t.get('stake', 5) for t in trades)
                wr = w / max(w + l, 1) * 100
                roi = pnl / max(staked, 1) * 100
                print(f"  {f.stem:20s} {len(trades):6d} {w:4d} {l:3d} {wr:5.1f}% ${pnl:+8.2f} {roi:+6.1f}%")


if __name__ == '__main__':
    main()
