#!/usr/bin/env python3
"""Find configs that maximize trades + WR while keeping DD <= $5 (1 loss max)."""

import json
from collections import defaultdict
from itertools import product

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

def run(delta, entry_start, entry_end, min_book, max_book, tfs, cont, bn, cl_f):
    positions = {}
    entered = set()
    cont_counts = defaultdict(int)
    wins = 0; losses = 0; net = 0.0; fees = 0.0
    peak = 0.0; dd = 0.0; sl = 0
    trades = []
    settled_windows = set()
    loss_slugs = []

    def settle(slug, outcome_str, cl_close, settle_ts):
        nonlocal wins, losses, net, fees, peak, dd
        if slug not in positions: return
        pos = positions.pop(slug)
        outcome = 1.0 if outcome_str == "YES" else 0.0
        exit_p = outcome if pos["side"] == "UP" else 1.0 - outcome
        g = (exit_p - pos["fill"]) * pos["shares"]
        f = pm_fee(pos["fill"]) * pos["shares"]
        n = g - f
        net += n; fees += f
        if n > 0: wins += 1
        else:
            losses += 1
            loss_slugs.append(slug)
        if net > peak: peak = net
        d = peak - net
        if d > dd: dd = d
        trades.append({"slug": slug, "side": pos["side"], "fill": pos["fill"],
                       "exit": exit_p, "net": round(n, 2), "delta": pos["delta"],
                       "secs": pos["secs"], "asset": pos["asset"]})

    for sig in signals:
        ts = sig["ts"]
        for wend in list(settle_by_end.keys()):
            if ts >= wend and wend not in settled_windows:
                settled_windows.add(wend)
                for s in settle_by_end[wend]:
                    settle(s["slug"], s["outcome"], s["cl_close"], s["ts"])

        slug = sig["slug"]
        # SL check
        if slug in positions:
            pos = positions[slug]
            our_bid = sig.get("bid_yes" if pos["side"]=="UP" else "bid_no", 0)
            opp_bid = sig.get("bid_no" if pos["side"]=="UP" else "bid_yes", 0)
            if our_bid > 0 and our_bid <= pos["fill"] * 0.50 and opp_bid >= 0.80:
                p = positions.pop(slug)
                exit_p = max(our_bid - 0.005, 0.001)
                g = (exit_p - p["fill"]) * p["shares"]
                ef = pm_fee(p["fill"])*p["shares"] + pm_fee(exit_p)*p["shares"]
                n = g - ef; net += n; fees += ef; losses += 1; sl += 1
                loss_slugs.append(slug)
                if net > peak: peak = net
                d = peak - net
                if d > dd: dd = d
                trades.append({"slug": slug, "side": p["side"], "fill": p["fill"],
                               "exit": round(exit_p,3), "net": round(n,2),
                               "delta": p["delta"], "secs": p["secs"], "asset": p["asset"]})

        asset = sig["asset"]; tf = sig["tf"]; secs = sig["secs_left"]
        if tf not in tfs: continue
        if slug in entered or slug in positions: continue
        if secs > entry_start or secs < entry_end: continue

        pct = abs(sig.get("pct_move", 0))
        direction = "UP" if sig.get("pct_move", 0) > 0 else "DOWN"

        min_d = MIN_DELTA.get(asset, 0.05)
        if pct < min_d: continue
        if delta > 0:
            scaled = delta * (STDEV.get(asset, STDEV_BASE) / STDEV_BASE)
            if pct < scaled: continue

        if bn:
            bm = sig.get("bn_momentum_5s", 0) * 100
            if direction == "UP" and bm < -0.02: continue
            if direction == "DOWN" and bm > 0.02: continue
        if cl_f:
            cm = sig.get("cl_momentum_5s", 0) * 100
            if direction == "UP" and cm < -0.03: continue
            if direction == "DOWN" and cm > 0.03: continue

        if cont > 0:
            cont_counts[slug] += 1
            if cont_counts[slug] < cont: continue

        ask = sig.get("ask_yes" if direction=="UP" else "ask_no", 0)
        if ask < min_book or ask > max_book: continue

        maker = round(ask - 0.01, 2)
        maker = max(maker, min_book)
        fill = maker if maker >= ask else ask + 0.005
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

    for slug in list(positions.keys()):
        if slug in settlements:
            s = settlements[slug]
            settle(slug, s["outcome"], s["cl_close"], s["ts"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    return {"trades": total, "wins": wins, "losses": losses, "wr": wr,
            "net": round(net, 2), "dd": round(dd, 2), "sl": sl,
            "loss_slugs": loss_slugs, "trade_list": trades}


# ── Grid search: focus on DD <= $5.10 ────────────────────────────────────────

print("Running grid search (DD <= $5.10 target)...\n")

results = []
count = 0

for delta in [0.03, 0.035, 0.04, 0.045, 0.05]:
    for entry_start in [60, 75, 90, 105, 120]:
        for entry_end in [20, 30, 40]:
            for min_book in [0.78, 0.80, 0.82, 0.85]:
                for max_book in [0.90, 0.92, 0.94]:
                    for tfs in [[5], [5, 15]]:
                        for cont in [2, 3, 4]:
                            count += 1
                            r = run(delta, entry_start, entry_end, min_book, max_book,
                                    tfs, cont, True, True)
                            if r["trades"] >= 5 and r["dd"] <= 5.10:
                                r["params"] = (delta, entry_start, entry_end, min_book,
                                               max_book, tfs, cont)
                                results.append(r)

print(f"Tested {count} configs, {len(results)} with DD <= $5.10 and 5+ trades\n")

# Sort by: trades * wr (maximize both)
results.sort(key=lambda x: (-x["wr"], -x["trades"]))

# Also get best by net
by_net = sorted(results, key=lambda x: -x["net"])

# And best by trade count with high WR
by_count = sorted([r for r in results if r["wr"] >= 90], key=lambda x: -x["trades"])

print(f"{'='*90}")
print(f" TOP 15 BY WIN RATE (min 5 trades, DD <= $5.10)")
print(f"{'='*90}")
print(f"{'#':>3s} {'d%':>5s} {'Window':>10s} {'Book':>12s} {'TF':>6s} {'C':>2s} "
      f"{'Tr':>4s} {'W':>3s} {'L':>3s} {'WR':>6s} {'Net':>8s} {'DD':>6s}")
print("-" * 90)
for i, r in enumerate(results[:15]):
    p = r["params"]
    tf_s = "5+15" if p[5] == [5,15] else "5"
    print(f"{i+1:>3d} {p[0]:>5.3f} {p[1]:>3d}-{p[2]:<3d}s  {p[3]:.2f}-{p[4]:.2f}  {tf_s:>6s} {p[6]:>2d} "
          f"{r['trades']:>4d} {r['wins']:>3d} {r['losses']:>3d} {r['wr']:>5.1f}% ${r['net']:>+6.2f} ${r['dd']:>5.2f}")

print(f"\n{'='*90}")
print(f" TOP 15 BY NET PnL (DD <= $5.10)")
print(f"{'='*90}")
print(f"{'#':>3s} {'d%':>5s} {'Window':>10s} {'Book':>12s} {'TF':>6s} {'C':>2s} "
      f"{'Tr':>4s} {'W':>3s} {'L':>3s} {'WR':>6s} {'Net':>8s} {'DD':>6s}")
print("-" * 90)
for i, r in enumerate(by_net[:15]):
    p = r["params"]
    tf_s = "5+15" if p[5] == [5,15] else "5"
    print(f"{i+1:>3d} {p[0]:>5.3f} {p[1]:>3d}-{p[2]:<3d}s  {p[3]:.2f}-{p[4]:.2f}  {tf_s:>6s} {p[6]:>2d} "
          f"{r['trades']:>4d} {r['wins']:>3d} {r['losses']:>3d} {r['wr']:>5.1f}% ${r['net']:>+6.2f} ${r['dd']:>5.2f}")

print(f"\n{'='*90}")
print(f" TOP 15 BY TRADE COUNT (WR >= 90%, DD <= $5.10)")
print(f"{'='*90}")
print(f"{'#':>3s} {'d%':>5s} {'Window':>10s} {'Book':>12s} {'TF':>6s} {'C':>2s} "
      f"{'Tr':>4s} {'W':>3s} {'L':>3s} {'WR':>6s} {'Net':>8s} {'DD':>6s}")
print("-" * 90)
for i, r in enumerate(by_count[:15]):
    p = r["params"]
    tf_s = "5+15" if p[5] == [5,15] else "5"
    print(f"{i+1:>3d} {p[0]:>5.3f} {p[1]:>3d}-{p[2]:<3d}s  {p[3]:.2f}-{p[4]:.2f}  {tf_s:>6s} {p[6]:>2d} "
          f"{r['trades']:>4d} {r['wins']:>3d} {r['losses']:>3d} {r['wr']:>5.1f}% ${r['net']:>+6.2f} ${r['dd']:>5.02f}")

# Show THE best balanced config
print(f"\n{'='*90}")
best = sorted(results, key=lambda x: x["net"] * (x["wr"]/100) * x["trades"], reverse=True)[0]
p = best["params"]
tf_s = "5+15" if p[5] == [5,15] else "5"
print(f" RECOMMENDED CONFIG")
print(f"{'='*90}")
print(f"  delta={p[0]}% | window={p[1]}-{p[2]}s | book={p[3]}-{p[4]} | tf={tf_s} | cont={p[6]}")
print(f"  Trades={best['trades']} | W={best['wins']} L={best['losses']} | WR={best['wr']:.1f}%")
print(f"  Net=${best['net']:+.2f} | DD=${best['dd']:.2f}")
print(f"  Losses on: {best['loss_slugs']}")
print(f"\n  All trades:")
for t in best["trade_list"]:
    w = "W" if t["net"] > 0 else "L"
    print(f"    {w} {t['slug']:40s} {t['side']:4s} @{t['fill']:.3f}→{t['exit']:.3f} "
          f"d={t['delta']:.3f}% ${t['net']:+.2f} {t['secs']:.0f}s")
