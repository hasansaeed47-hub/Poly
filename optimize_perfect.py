#!/usr/bin/env python3
"""Optimize S3C (both-sides dump) + Sniper for perfect combined config."""

import json
from collections import defaultdict

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

signals_by_slug = defaultdict(list)
for sig in signals:
    signals_by_slug[sig["slug"]].append(sig)

STDEV = {"btc": 0.167, "eth": 0.194, "sol": 0.247, "xrp": 0.440}
STDEV_BASE = 0.167
MIN_DELTA = {"btc": 0.015, "eth": 0.020, "sol": 0.030, "xrp": 0.050}

def fee(px):
    return px * (1.0 - px) * 0.0625

def find_signal_near(slug, target_secs, tolerance=10):
    best = None
    best_diff = float('inf')
    for sig in signals_by_slug.get(slug, []):
        diff = abs(sig["secs_left"] - target_secs)
        if diff < best_diff and diff <= tolerance:
            best_diff = diff
            best = sig
    return best

# ══════════════════════════════════════════════════════════════════════════════
# S3C OPTIMIZER: Both sides, dump loser at T-30 with configurable dump price
# ══════════════════════════════════════════════════════════════════════════════

def run_s3c(entry_start, entry_end, ask_lo, ask_hi, dump_price, slip, assets, tfs):
    positions = {}
    entered = set()
    wins = 0; losses = 0; net = 0.0; peak = 0.0; dd = 0.0
    trades = []
    settled_windows = set()

    def settle(slug, outcome_str):
        nonlocal wins, losses, net, peak, dd
        if slug not in positions: return
        pos = positions.pop(slug)
        up_winning = outcome_str == "YES"
        up_pay = 1.0 if up_winning else 0.0
        dn_pay = 0.0 if up_winning else 1.0

        dump_sig = find_signal_near(slug, 30, tolerance=10)
        if dump_sig:
            loser_bid = dump_sig.get("bid_no" if up_winning else "bid_yes", 0)
            dp = max(min(loser_bid, dump_price) - slip, 0.01)
            loser_sh = pos["sh_dn"] if up_winning else pos["sh_up"]
            winner_sh = pos["sh_up"] if up_winning else pos["sh_dn"]
            loser_rec = loser_sh * dp
            loser_fee = fee(dp) * loser_sh
            winner_rec = winner_sh * 1.0
            pnl = loser_rec + winner_rec - pos["cost"] - loser_fee
        else:
            revenue = pos["sh_up"] * up_pay + pos["sh_dn"] * dn_pay
            pnl = revenue - pos["cost"]

        net += pnl
        if pnl > 0: wins += 1
        else: losses += 1
        if net > peak: peak = net
        d = peak - net
        if d > dd: dd = d
        trades.append({"slug": slug, "pnl": round(pnl, 2), "asset": pos["asset"]})

    for sig in signals:
        ts = sig["ts"]
        for wend in list(settle_by_end.keys()):
            if ts >= wend and wend not in settled_windows:
                settled_windows.add(wend)
                for s in settle_by_end[wend]:
                    settle(s["slug"], s["outcome"])

        slug = sig["slug"]
        if sig["tf"] not in tfs: continue
        secs = sig["secs_left"]
        if secs > entry_start or secs < entry_end: continue
        if slug in entered or slug in positions: continue
        if sig["asset"] not in assets: continue

        ask_yes = sig.get("ask_yes", 0)
        ask_no = sig.get("ask_no", 0)
        if ask_yes <= 0 or ask_no <= 0: continue
        if ask_yes < ask_lo or ask_yes > ask_hi: continue
        if ask_no < ask_lo or ask_no > ask_hi: continue

        fill_yes = ask_yes + slip
        fill_no = ask_no + slip
        if fill_yes >= 1.0 or fill_no >= 1.0: continue
        sh_up = 5.0 / fill_yes
        sh_dn = 5.0 / fill_no
        entry_fee = fee(fill_yes) * sh_up + fee(fill_no) * sh_dn
        total_cost = 10.0 + entry_fee

        positions[slug] = {"slug": slug, "asset": sig["asset"],
                           "sh_up": sh_up, "sh_dn": sh_dn, "cost": total_cost,
                           "ask_yes": ask_yes, "ask_no": ask_no, "secs": secs}
        entered.add(slug)

    for slug in list(positions.keys()):
        if slug in settlements:
            settle(slug, settlements[slug]["outcome"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    return {"trades": total, "wins": wins, "losses": losses, "wr": wr,
            "net": round(net, 2), "dd": round(dd, 2), "trade_list": trades}


# ══════════════════════════════════════════════════════════════════════════════
# SNIPER OPTIMIZER
# ══════════════════════════════════════════════════════════════════════════════

def run_sniper(delta, entry_start, entry_end, min_book, max_book, tfs, cont, bn, cl_f):
    positions = {}
    entered = set()
    cont_counts = defaultdict(int)
    wins = 0; losses = 0; net = 0.0; peak = 0.0; dd = 0.0
    trades = []
    settled_windows = set()

    def settle(slug, outcome_str):
        nonlocal wins, losses, net, peak, dd
        if slug not in positions: return
        pos = positions.pop(slug)
        outcome = 1.0 if outcome_str == "YES" else 0.0
        exit_p = outcome if pos["side"] == "UP" else 1.0 - outcome
        g = (exit_p - pos["fill"]) * pos["shares"]
        f = fee(pos["fill"]) * pos["shares"]
        n = g - f
        net += n
        if n > 0: wins += 1
        else: losses += 1
        if net > peak: peak = net
        d = peak - net
        if d > dd: dd = d
        trades.append({"slug": slug, "pnl": round(n, 2)})

    for sig in signals:
        ts = sig["ts"]
        for wend in list(settle_by_end.keys()):
            if ts >= wend and wend not in settled_windows:
                settled_windows.add(wend)
                for s in settle_by_end[wend]:
                    settle(s["slug"], s["outcome"])

        slug = sig["slug"]
        if slug in positions:
            pos = positions[slug]
            our_bid = sig.get("bid_yes" if pos["side"]=="UP" else "bid_no", 0)
            opp_bid = sig.get("bid_no" if pos["side"]=="UP" else "bid_yes", 0)
            if our_bid > 0 and our_bid <= pos["fill"] * 0.50 and opp_bid >= 0.80:
                p = positions.pop(slug)
                exit_p = max(our_bid - 0.005, 0.001)
                g = (exit_p - p["fill"]) * p["shares"]
                ef = fee(p["fill"])*p["shares"] + fee(exit_p)*p["shares"]
                n = g - ef; net += n; losses += 1
                if net > peak: peak = net
                d = peak - net
                if d > dd: dd = d
                trades.append({"slug": slug, "pnl": round(n, 2)})

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

        shares = 5.0 / fill
        positions[slug] = {"slug": slug, "side": direction, "asset": asset,
                           "fill": fill, "shares": shares, "secs": secs}
        entered.add(slug)
        cont_counts[slug] = 0

    for slug in list(positions.keys()):
        if slug in settlements:
            settle(slug, settlements[slug]["outcome"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    return {"trades": total, "wins": wins, "losses": losses, "wr": wr,
            "net": round(net, 2), "dd": round(dd, 2), "trade_list": trades}


# ══════════════════════════════════════════════════════════════════════════════
# GRID SEARCH
# ══════════════════════════════════════════════════════════════════════════════

print("=" * 90)
print("  OPTIMIZING S3C (both-sides dump)")
print("=" * 90)

s3c_results = []
count = 0
for entry_start in [120, 180, 240, 290]:
    for entry_end in [40, 60, 80, 100]:
        if entry_end >= entry_start: continue
        for ask_lo in [0.40, 0.44, 0.47]:
            for ask_hi in [0.53, 0.56, 0.60]:
                for dump_price in [0.25, 0.30, 0.35, 0.40, 0.45]:
                    for slip in [0.003, 0.005, 0.008]:
                        for tfs in [[5], [5, 15]]:
                            count += 1
                            r = run_s3c(entry_start, entry_end, ask_lo, ask_hi,
                                       dump_price, slip, ["btc", "eth", "sol", "xrp"], tfs)
                            if r["trades"] >= 3:
                                r["params"] = {"es": entry_start, "ee": entry_end,
                                              "al": ask_lo, "ah": ask_hi,
                                              "dp": dump_price, "sl": slip, "tf": tfs}
                                s3c_results.append(r)

print(f"Tested {count} S3C configs, {len(s3c_results)} with 3+ trades\n")

# Sort by net * wr compound score
s3c_results.sort(key=lambda x: -x["net"])

print(f"{'#':>3} {'Window':>10} {'Ask':>10} {'Dump':>5} {'Slip':>5} {'TF':>5} "
      f"{'Tr':>4} {'W':>3} {'L':>3} {'WR':>6} {'Net':>8} {'DD':>6}")
print("-" * 80)
for i, r in enumerate(s3c_results[:20]):
    p = r["params"]
    tf = "5+15" if p["tf"] == [5,15] else "5"
    print(f"{i+1:>3} {p['es']:>3}-{p['ee']:<3}s  {p['al']:.2f}-{p['ah']:.2f} {p['dp']:>5.2f} {p['sl']:>5.3f} {tf:>5} "
          f"{r['trades']:>4} {r['wins']:>3} {r['losses']:>3} {r['wr']:>5.1f}% ${r['net']:>+6.2f} ${r['dd']:>5.2f}")


print(f"\n{'=' * 90}")
print("  OPTIMIZING SNIPER")
print(f"{'=' * 90}")

sniper_results = []
count = 0
for delta in [0.025, 0.03, 0.035, 0.04]:
    for entry_start in [75, 90, 105, 120]:
        for entry_end in [20, 30, 40]:
            for min_book in [0.75, 0.78, 0.80, 0.82]:
                for max_book in [0.90, 0.92, 0.94]:
                    for tfs in [[5], [5, 15]]:
                        for cont in [2, 3, 4]:
                            count += 1
                            r = run_sniper(delta, entry_start, entry_end,
                                          min_book, max_book, tfs, cont, True, True)
                            if r["trades"] >= 3:
                                r["params"] = {"d": delta, "es": entry_start, "ee": entry_end,
                                              "mb": min_book, "xb": max_book, "tf": tfs, "c": cont}
                                sniper_results.append(r)

print(f"Tested {count} Sniper configs, {len(sniper_results)} with 3+ trades\n")

sniper_results.sort(key=lambda x: -x["net"])

print(f"{'#':>3} {'d%':>5} {'Window':>10} {'Book':>12} {'TF':>5} {'C':>2} "
      f"{'Tr':>4} {'W':>3} {'L':>3} {'WR':>6} {'Net':>8} {'DD':>6}")
print("-" * 80)
for i, r in enumerate(sniper_results[:20]):
    p = r["params"]
    tf = "5+15" if p["tf"] == [5,15] else "5"
    print(f"{i+1:>3} {p['d']:>5.3f} {p['es']:>3}-{p['ee']:<3}s  {p['mb']:.2f}-{p['xb']:.2f}  {tf:>5} {p['c']:>2} "
          f"{r['trades']:>4} {r['wins']:>3} {r['losses']:>3} {r['wr']:>5.1f}% ${r['net']:>+6.2f} ${r['dd']:>5.2f}")


# ── BEST COMBINED CONFIG ─────────────────────────────────────────────────────

# Best S3C with DD <= $5
best_s3c_safe = sorted([r for r in s3c_results if r["dd"] <= 5.0], key=lambda x: -x["net"])
# Best Sniper with DD <= $5
best_sniper_safe = sorted([r for r in sniper_results if r["dd"] <= 5.0], key=lambda x: -x["net"])
# Best S3C by WR with 10+ trades
best_s3c_wr = sorted([r for r in s3c_results if r["trades"] >= 10], key=lambda x: (-x["wr"], -x["net"]))
# Best Sniper by WR with 10+ trades
best_sniper_wr = sorted([r for r in sniper_results if r["trades"] >= 10], key=lambda x: (-x["wr"], -x["net"]))

print(f"\n{'=' * 90}")
print("  PERFECT CONFIG CANDIDATES")
print(f"{'=' * 90}")

if best_s3c_safe:
    r = best_s3c_safe[0]; p = r["params"]
    tf = "5+15" if p["tf"] == [5,15] else "5"
    print(f"\n  S3C BEST NET (DD≤$5):")
    print(f"    window={p['es']}-{p['ee']}s | ask={p['al']}-{p['ah']} | dump={p['dp']} | slip={p['sl']} | tf={tf}")
    print(f"    Trades={r['trades']} W={r['wins']} L={r['losses']} WR={r['wr']:.1f}% Net=${r['net']:+.2f} DD=${r['dd']:.2f}")

if best_s3c_wr:
    r = best_s3c_wr[0]; p = r["params"]
    tf = "5+15" if p["tf"] == [5,15] else "5"
    print(f"\n  S3C BEST WR (10+ trades):")
    print(f"    window={p['es']}-{p['ee']}s | ask={p['al']}-{p['ah']} | dump={p['dp']} | slip={p['sl']} | tf={tf}")
    print(f"    Trades={r['trades']} W={r['wins']} L={r['losses']} WR={r['wr']:.1f}% Net=${r['net']:+.2f} DD=${r['dd']:.2f}")

if best_sniper_safe:
    r = best_sniper_safe[0]; p = r["params"]
    tf = "5+15" if p["tf"] == [5,15] else "5"
    print(f"\n  SNIPER BEST NET (DD≤$5):")
    print(f"    delta={p['d']} | window={p['es']}-{p['ee']}s | book={p['mb']}-{p['xb']} | tf={tf} | cont={p['c']}")
    print(f"    Trades={r['trades']} W={r['wins']} L={r['losses']} WR={r['wr']:.1f}% Net=${r['net']:+.2f} DD=${r['dd']:.2f}")

if best_sniper_wr:
    r = best_sniper_wr[0]; p = r["params"]
    tf = "5+15" if p["tf"] == [5,15] else "5"
    print(f"\n  SNIPER BEST WR (10+ trades):")
    print(f"    delta={p['d']} | window={p['es']}-{p['ee']}s | book={p['mb']}-{p['xb']} | tf={tf} | cont={p['c']}")
    print(f"    Trades={r['trades']} W={r['wins']} L={r['losses']} WR={r['wr']:.1f}% Net=${r['net']:+.2f} DD=${r['dd']:.2f}")

# Combined: best S3C + best Sniper running simultaneously
print(f"\n{'=' * 90}")
print("  COMBINED PORTFOLIO: S3C + SNIPER running together")
print(f"{'=' * 90}")

if best_s3c_safe and best_sniper_safe:
    s = best_s3c_safe[0]
    n = best_sniper_safe[0]
    ct = s["trades"] + n["trades"]
    cw = s["wins"] + n["wins"]
    cl = s["losses"] + n["losses"]
    cn = s["net"] + n["net"]
    cwr = cw / ct * 100 if ct > 0 else 0
    cdd = max(s["dd"], n["dd"])
    print(f"  Trades: {ct}  W/L: {cw}/{cl}  WR: {cwr:.1f}%  Net: ${cn:+.2f}  Worst DD: ${cdd:.2f}")
    print(f"  S3C contributes ${s['net']:+.2f} ({s['trades']} trades)")
    print(f"  Sniper contributes ${n['net']:+.2f} ({n['trades']} trades)")

# Absolute best by compound score: net * wr * trades
all_results = [(r, "S3C") for r in s3c_results] + [(r, "Sniper") for r in sniper_results]
all_results.sort(key=lambda x: x[0]["net"] * (x[0]["wr"]/100) * x[0]["trades"], reverse=True)

print(f"\n  TOP 10 BY COMPOUND SCORE (net × WR × trades):")
print(f"  {'#':>3} {'Type':<8} {'Tr':>4} {'W':>3} {'L':>3} {'WR':>6} {'Net':>8} {'DD':>6} {'Score':>8}")
print(f"  {'-' * 60}")
for i, (r, typ) in enumerate(all_results[:10]):
    score = r["net"] * (r["wr"]/100) * r["trades"]
    print(f"  {i+1:>3} {typ:<8} {r['trades']:>4} {r['wins']:>3} {r['losses']:>3} "
          f"{r['wr']:>5.1f}% ${r['net']:>+6.2f} ${r['dd']:>5.2f} {score:>8.1f}")
