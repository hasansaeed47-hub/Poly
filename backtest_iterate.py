#!/usr/bin/env python3
"""Iterate Sniper params to find best WR + PnL mix on real data."""

import json
from collections import defaultdict

# ── Load data ────────────────────────────────────────────────────────────────

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
    we = s["window_end"]
    settle_by_end.setdefault(we, []).append(s)

STDEV = {"btc": 0.167, "eth": 0.194, "sol": 0.247, "xrp": 0.440}
STDEV_BASE = 0.167
MIN_DELTA = {"btc": 0.015, "eth": 0.020, "sol": 0.030, "xrp": 0.050}
STAKE = 5.0

def pm_fee(p):
    return p * (1.0 - p) * 0.0625

def run_config(delta_thresh, entry_start, entry_end, min_book, max_book,
               timeframes, continuity, bn_contra, cl_fade, slip, use_maker):
    """Run one config through all signals. Returns stats dict."""
    positions = {}
    entered = set()
    cont_counts = defaultdict(int)
    wins = 0; losses = 0; net = 0.0; gross = 0.0; fees = 0.0
    peak = 0.0; dd = 0.0; sl = 0
    trades = []
    settled_windows = set()

    def settle(slug, outcome_str, cl_close, settle_ts):
        nonlocal wins, losses, net, gross, fees, peak, dd
        if slug not in positions:
            return
        pos = positions.pop(slug)
        outcome = 1.0 if outcome_str == "YES" else 0.0
        exit_p = outcome if pos["side"] == "UP" else 1.0 - outcome
        g = (exit_p - pos["fill"]) * pos["shares"]
        f = pm_fee(pos["fill"]) * pos["shares"]
        n = g - f
        gross += g; fees += f; net += n
        if n > 0: wins += 1
        else: losses += 1
        if net > peak: peak = net
        d = peak - net
        if d > dd: dd = d
        trades.append({"slug": slug, "side": pos["side"], "fill": pos["fill"],
                       "exit": exit_p, "net": round(n, 2), "delta": pos["delta"],
                       "secs": pos["secs"], "asset": pos["asset"]})

    for sig in signals:
        ts = sig["ts"]

        # Settle
        for wend in list(settle_by_end.keys()):
            if ts >= wend and wend not in settled_windows:
                settled_windows.add(wend)
                for s in settle_by_end[wend]:
                    settle(s["slug"], s["outcome"], s["cl_close"], s["ts"])

        # SL check (confirmed only)
        slug = sig["slug"]
        if slug in positions:
            pos = positions[slug]
            if pos["side"] == "UP":
                our_bid = sig.get("bid_yes", 0)
                opp_bid = sig.get("bid_no", 0)
            else:
                our_bid = sig.get("bid_no", 0)
                opp_bid = sig.get("bid_yes", 0)
            if our_bid > 0 and our_bid <= pos["fill"] * 0.50 and opp_bid >= 0.80:
                p = positions.pop(slug)
                exit_p = max(our_bid - 0.005, 0.001)
                g = (exit_p - p["fill"]) * p["shares"]
                ef = pm_fee(p["fill"]) * p["shares"] + pm_fee(exit_p) * p["shares"]
                n = g - ef
                gross += g; fees += ef; net += n
                losses += 1; sl += 1
                if net > peak: peak = net
                d = peak - net
                if d > dd: dd = d
                trades.append({"slug": slug, "side": p["side"], "fill": p["fill"],
                               "exit": round(exit_p, 3), "net": round(n, 2),
                               "delta": p["delta"], "secs": p["secs"], "asset": p["asset"]})
                continue

        # Entry
        asset = sig["asset"]
        tf = sig["tf"]
        secs = sig["secs_left"]

        if tf not in timeframes: continue
        if slug in entered or slug in positions: continue
        if secs > entry_start or secs < entry_end: continue

        pct = abs(sig.get("pct_move", 0))
        direction = "UP" if sig.get("pct_move", 0) > 0 else "DOWN"

        min_d = MIN_DELTA.get(asset, 0.05)
        if pct < min_d: continue

        if delta_thresh > 0:
            scaled = delta_thresh * (STDEV.get(asset, STDEV_BASE) / STDEV_BASE)
            if pct < scaled: continue

        if bn_contra:
            bn = sig.get("bn_momentum_5s", 0) * 100
            if direction == "UP" and bn < -0.02: continue
            if direction == "DOWN" and bn > 0.02: continue

        if cl_fade:
            cl = sig.get("cl_momentum_5s", 0) * 100
            if direction == "UP" and cl < -0.03: continue
            if direction == "DOWN" and cl > 0.03: continue

        if continuity > 0:
            cont_counts[slug] += 1
            if cont_counts[slug] < continuity: continue

        if direction == "UP":
            ask = sig.get("ask_yes", 0)
        else:
            ask = sig.get("ask_no", 0)

        if ask < min_book or ask > max_book: continue

        if use_maker:
            maker = round(ask - 0.01, 2)
            maker = max(maker, min_book)
            fill = maker if maker >= ask else ask + slip
        else:
            fill = ask + slip

        fill = round(fill, 3)
        if fill < min_book or fill > max_book or fill >= 1.0 or fill <= 0: continue

        shares = STAKE / fill
        parts = slug.split("-")
        try:
            ws = int(parts[-1]); tfm = int(parts[-2].replace("m",""))
            we = ws + tfm * 60
        except:
            we = int(ts + secs)

        positions[slug] = {"slug": slug, "side": direction, "asset": asset,
                           "fill": fill, "shares": shares, "secs": secs,
                           "delta": pct, "entry_ts": ts, "window_end": we}
        entered.add(slug)
        cont_counts[slug] = 0

    # Settle remaining
    for slug in list(positions.keys()):
        if slug in settlements:
            s = settlements[slug]
            settle(slug, s["outcome"], s["cl_close"], s["ts"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    avg = net / total if total > 0 else 0
    return {"trades": total, "wins": wins, "losses": losses, "sl": sl,
            "wr": wr, "gross": round(gross, 2), "fees": round(fees, 2),
            "net": round(net, 2), "avg": round(avg, 2), "dd": round(dd, 2),
            "trade_list": trades}


# ── Parameter sweep ──────────────────────────────────────────────────────────

print(f"Iterating across parameter space...\n")

results = []

# Sweep: delta, entry window, book range, timeframes, filters
configs = [
    # Original Sniper A
    {"label": "Sniper-A (original)",
     "delta": 0.04, "entry_start": 57, "entry_end": 44, "min_book": 0.88, "max_book": 0.98,
     "tf": [5], "cont": 4, "bn": True, "cl": True, "slip": 0.005, "maker": True},

    # Wider entry window
    {"label": "A wider window (90-30s)",
     "delta": 0.04, "entry_start": 90, "entry_end": 30, "min_book": 0.88, "max_book": 0.98,
     "tf": [5], "cont": 4, "bn": True, "cl": True, "slip": 0.005, "maker": True},

    # Much wider (120-20s)
    {"label": "A wide (120-20s)",
     "delta": 0.04, "entry_start": 120, "entry_end": 20, "min_book": 0.85, "max_book": 0.98,
     "tf": [5], "cont": 4, "bn": True, "cl": True, "slip": 0.005, "maker": True},

    # Lower book floor (catch cheaper entries)
    {"label": "A low-book (0.80-0.96)",
     "delta": 0.04, "entry_start": 57, "entry_end": 44, "min_book": 0.80, "max_book": 0.96,
     "tf": [5], "cont": 4, "bn": True, "cl": True, "slip": 0.005, "maker": True},

    # Both timeframes
    {"label": "A+C combined (5m+15m)",
     "delta": 0.04, "entry_start": 57, "entry_end": 44, "min_book": 0.88, "max_book": 0.98,
     "tf": [5, 15], "cont": 4, "bn": True, "cl": True, "slip": 0.005, "maker": True},

    # No continuity (faster entry)
    {"label": "No continuity",
     "delta": 0.04, "entry_start": 57, "entry_end": 44, "min_book": 0.88, "max_book": 0.98,
     "tf": [5], "cont": 0, "bn": True, "cl": True, "slip": 0.005, "maker": True},

    # Lower delta
    {"label": "Low delta 0.02",
     "delta": 0.02, "entry_start": 57, "entry_end": 44, "min_book": 0.88, "max_book": 0.98,
     "tf": [5], "cont": 4, "bn": True, "cl": True, "slip": 0.005, "maker": True},

    # Higher delta (more selective)
    {"label": "High delta 0.06",
     "delta": 0.06, "entry_start": 57, "entry_end": 44, "min_book": 0.88, "max_book": 0.98,
     "tf": [5], "cont": 4, "bn": True, "cl": True, "slip": 0.005, "maker": True},

    # No filters (raw)
    {"label": "No filters",
     "delta": 0.04, "entry_start": 57, "entry_end": 44, "min_book": 0.88, "max_book": 0.98,
     "tf": [5], "cont": 4, "bn": False, "cl": False, "slip": 0.005, "maker": True},

    # Sweet spot: wider window + both TFs + lower book
    {"label": "SWEET: 90-30s, 5+15m, 0.85-0.97",
     "delta": 0.04, "entry_start": 90, "entry_end": 30, "min_book": 0.85, "max_book": 0.97,
     "tf": [5, 15], "cont": 4, "bn": True, "cl": True, "slip": 0.005, "maker": True},

    # Aggressive: wide everything
    {"label": "AGG: 120-20s, 5+15m, 0.80-0.97",
     "delta": 0.03, "entry_start": 120, "entry_end": 20, "min_book": 0.80, "max_book": 0.97,
     "tf": [5, 15], "cont": 2, "bn": True, "cl": True, "slip": 0.005, "maker": True},

    # Conservative: tighter everything
    {"label": "CONS: 55-45s, 0.90-0.96",
     "delta": 0.05, "entry_start": 55, "entry_end": 45, "min_book": 0.90, "max_book": 0.96,
     "tf": [5, 15], "cont": 4, "bn": True, "cl": True, "slip": 0.005, "maker": True},

    # Optimal seek: medium window + medium book + both TFs
    {"label": "OPT1: 75-35s, 0.88-0.96, d=0.04",
     "delta": 0.04, "entry_start": 75, "entry_end": 35, "min_book": 0.88, "max_book": 0.96,
     "tf": [5, 15], "cont": 3, "bn": True, "cl": True, "slip": 0.005, "maker": True},

    # Lower max book = lower entry cost = more upside per win
    {"label": "OPT2: 75-35s, 0.85-0.94, d=0.035",
     "delta": 0.035, "entry_start": 75, "entry_end": 35, "min_book": 0.85, "max_book": 0.94,
     "tf": [5, 15], "cont": 3, "bn": True, "cl": True, "slip": 0.005, "maker": True},

    # Even lower book range
    {"label": "OPT3: 80-30s, 0.82-0.93, d=0.03",
     "delta": 0.03, "entry_start": 80, "entry_end": 30, "min_book": 0.82, "max_book": 0.93,
     "tf": [5, 15], "cont": 3, "bn": True, "cl": True, "slip": 0.005, "maker": True},

    # Scalper zone: very late, high book
    {"label": "SCALP: 30-5s, 0.93-0.98",
     "delta": 0.03, "entry_start": 30, "entry_end": 5, "min_book": 0.93, "max_book": 0.98,
     "tf": [5, 15], "cont": 0, "bn": False, "cl": False, "slip": 0.005, "maker": True},

    # Mid-range value zone
    {"label": "VALUE: 90-40s, 0.80-0.92, d=0.04",
     "delta": 0.04, "entry_start": 90, "entry_end": 40, "min_book": 0.80, "max_book": 0.92,
     "tf": [5, 15], "cont": 3, "bn": True, "cl": True, "slip": 0.005, "maker": True},
]

for cfg in configs:
    r = run_config(cfg["delta"], cfg["entry_start"], cfg["entry_end"],
                   cfg["min_book"], cfg["max_book"], cfg["tf"], cfg["cont"],
                   cfg["bn"], cfg["cl"], cfg["slip"], cfg["maker"])
    r["label"] = cfg["label"]
    r["cfg"] = cfg
    results.append(r)

# Sort by net PnL
results.sort(key=lambda x: x["net"], reverse=True)

print(f"{'Rank':<5s} {'Config':<38s} {'Trades':>6s} {'W':>4s} {'L':>4s} {'WR':>6s} {'Net':>10s} {'Avg':>8s} {'DD':>8s}")
print("-" * 95)
for i, r in enumerate(results):
    print(f"{i+1:<5d} {r['label']:<38s} {r['trades']:>6d} {r['wins']:>4d} {r['losses']:>4d} "
          f"{r['wr']:>5.1f}% ${r['net']:>+8.2f} ${r['avg']:>+6.2f} ${r['dd']:>6.2f}")

# Show top 3 trade details
print(f"\n{'='*70}")
print(f"TOP 3 CONFIGS — TRADE DETAILS")
print(f"{'='*70}")

for r in results[:3]:
    print(f"\n--- {r['label']} ---")
    print(f"Net: ${r['net']:+.2f} | WR: {r['wr']:.1f}% | Trades: {r['trades']} | DD: ${r['dd']:.2f}")
    for t in r["trade_list"]:
        w = "W" if t["net"] > 0 else "L"
        print(f"  {w} {t['slug']:40s} {t['side']:4s} @{t['fill']:.3f}→{t['exit']:.3f} "
              f"d={t['delta']:.3f}% ${t['net']:+.2f} {t['secs']:.0f}s left")
