#!/usr/bin/env python3
"""Fast optimizer: pre-compute T-30 lookups, run S3C + Sniper grid."""

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
    settle_by_end.setdefault(s["window_end"], []).append(s)

# Pre-compute T-30 signal for each slug (once, not per config)
t30_signals = {}
for sig in signals:
    slug = sig["slug"]
    secs = sig["secs_left"]
    if 20 <= secs <= 40:
        if slug not in t30_signals or abs(secs - 30) < abs(t30_signals[slug]["secs_left"] - 30):
            t30_signals[slug] = sig

STDEV = {"btc": 0.167, "eth": 0.194, "sol": 0.247, "xrp": 0.440}
STDEV_BASE = 0.167
MIN_DELTA = {"btc": 0.015, "eth": 0.020, "sol": 0.030, "xrp": 0.050}

def fee(px):
    return px * (1.0 - px) * 0.0625

# ══════════════════════════════════════════════════════════════════════════════
# S3C: Both sides, dump loser at fixed price
# ══════════════════════════════════════════════════════════════════════════════

def run_s3c(entry_start, entry_end, ask_lo, ask_hi, dump_price, slip, tfs):
    positions = {}
    entered = set()
    wins = 0; losses = 0; net = 0.0; peak = 0.0; dd = 0.0
    settled_windows = set()

    def settle(slug, outcome_str):
        nonlocal wins, losses, net, peak, dd
        if slug not in positions: return
        pos = positions.pop(slug)
        up_winning = outcome_str == "YES"

        dump_sig = t30_signals.get(slug)
        if dump_sig:
            loser_bid = dump_sig.get("bid_no" if up_winning else "bid_yes", 0)
            dp = max(min(loser_bid, dump_price) - slip, 0.01)
            loser_sh = pos["sh_dn"] if up_winning else pos["sh_up"]
            winner_sh = pos["sh_up"] if up_winning else pos["sh_dn"]
            pnl = loser_sh * dp + winner_sh * 1.0 - pos["cost"] - fee(dp) * loser_sh
        else:
            up_pay = 1.0 if up_winning else 0.0
            dn_pay = 1.0 - up_pay
            pnl = pos["sh_up"] * up_pay + pos["sh_dn"] * dn_pay - pos["cost"]

        net += pnl
        if pnl > 0: wins += 1
        else: losses += 1
        if net > peak: peak = net
        d = peak - net
        if d > dd: dd = d

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
        total_cost = 10.0 + fee(fill_yes) * sh_up + fee(fill_no) * sh_dn

        positions[slug] = {"sh_up": sh_up, "sh_dn": sh_dn, "cost": total_cost, "secs": secs,
                           "asset": sig["asset"], "ask_yes": ask_yes, "ask_no": ask_no}
        entered.add(slug)

    for slug in list(positions.keys()):
        if slug in settlements:
            settle(slug, settlements[slug]["outcome"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    return {"trades": total, "wins": wins, "losses": losses, "wr": wr,
            "net": round(net, 2), "dd": round(dd, 2)}


# ══════════════════════════════════════════════════════════════════════════════
# SNIPER
# ══════════════════════════════════════════════════════════════════════════════

def run_sniper(delta, entry_start, entry_end, min_book, max_book, tfs, cont):
    positions = {}
    entered = set()
    cont_counts = defaultdict(int)
    wins = 0; losses = 0; net = 0.0; peak = 0.0; dd = 0.0
    settled_windows = set()

    def settle(slug, outcome_str):
        nonlocal wins, losses, net, peak, dd
        if slug not in positions: return
        pos = positions.pop(slug)
        outcome = 1.0 if outcome_str == "YES" else 0.0
        exit_p = outcome if pos["side"] == "UP" else 1.0 - outcome
        n = (exit_p - pos["fill"]) * pos["shares"] - fee(pos["fill"]) * pos["shares"]
        net += n
        if n > 0: wins += 1
        else: losses += 1
        if net > peak: peak = net
        d = peak - net
        if d > dd: dd = d

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
                ef = fee(p["fill"])*p["shares"] + fee(exit_p)*p["shares"]
                n = (exit_p - p["fill"]) * p["shares"] - ef
                net += n; losses += 1
                if net > peak: peak = net
                d = peak - net
                if d > dd: dd = d

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

        bm = sig.get("bn_momentum_5s", 0) * 100
        if direction == "UP" and bm < -0.02: continue
        if direction == "DOWN" and bm > 0.02: continue
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

        positions[slug] = {"side": direction, "asset": asset, "fill": fill,
                           "shares": 5.0 / fill, "secs": secs}
        entered.add(slug)
        cont_counts[slug] = 0

    for slug in list(positions.keys()):
        if slug in settlements:
            settle(slug, settlements[slug]["outcome"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    return {"trades": total, "wins": wins, "losses": losses, "wr": wr,
            "net": round(net, 2), "dd": round(dd, 2)}


# ══════════════════════════════════════════════════════════════════════════════
# RUN GRIDS
# ══════════════════════════════════════════════════════════════════════════════

print("=" * 90)
print("  S3C GRID SEARCH")
print("=" * 90)

s3c_results = []
cnt = 0
for es in [120, 180, 240, 290]:
    for ee in [40, 60, 80, 100]:
        if ee >= es: continue
        for al in [0.40, 0.44, 0.47]:
            for ah in [0.53, 0.56, 0.60]:
                for dp in [0.25, 0.30, 0.35, 0.40, 0.45]:
                    for sl in [0.003, 0.005]:
                        for tfs in [[5], [5, 15]]:
                            cnt += 1
                            r = run_s3c(es, ee, al, ah, dp, sl, tfs)
                            if r["trades"] >= 3:
                                r["p"] = (es, ee, al, ah, dp, sl, tfs)
                                s3c_results.append(r)

print(f"Tested {cnt} configs, {len(s3c_results)} with 3+ trades\n")
s3c_results.sort(key=lambda x: -x["net"])

print(f"{'#':>3} {'Window':>10} {'Ask':>10} {'Dump':>5} {'Slip':>5} {'TF':>5} "
      f"{'Tr':>4} {'W':>3} {'L':>3} {'WR':>6} {'Net':>8} {'DD':>6}")
print("-" * 80)
for i, r in enumerate(s3c_results[:25]):
    p = r["p"]
    tf = "5+15" if p[6] == [5,15] else "5"
    print(f"{i+1:>3} {p[0]:>3}-{p[1]:<3}s  {p[2]:.2f}-{p[3]:.2f} {p[4]:>5.2f} {p[5]:>5.3f} {tf:>5} "
          f"{r['trades']:>4} {r['wins']:>3} {r['losses']:>3} {r['wr']:>5.1f}% ${r['net']:>+6.2f} ${r['dd']:>5.2f}")


print(f"\n{'=' * 90}")
print("  SNIPER GRID SEARCH")
print(f"{'=' * 90}")

sniper_results = []
cnt = 0
for delta in [0.025, 0.03, 0.035, 0.04, 0.045]:
    for es in [75, 90, 105, 120]:
        for ee in [20, 30, 40]:
            for mb in [0.75, 0.78, 0.80, 0.82, 0.85]:
                for xb in [0.90, 0.92, 0.94]:
                    for tfs in [[5], [5, 15]]:
                        for c in [2, 3, 4]:
                            cnt += 1
                            r = run_sniper(delta, es, ee, mb, xb, tfs, c)
                            if r["trades"] >= 3:
                                r["p"] = (delta, es, ee, mb, xb, tfs, c)
                                sniper_results.append(r)

print(f"Tested {cnt} configs, {len(sniper_results)} with 3+ trades\n")
sniper_results.sort(key=lambda x: -x["net"])

print(f"{'#':>3} {'d%':>5} {'Window':>10} {'Book':>12} {'TF':>5} {'C':>2} "
      f"{'Tr':>4} {'W':>3} {'L':>3} {'WR':>6} {'Net':>8} {'DD':>6}")
print("-" * 80)
for i, r in enumerate(sniper_results[:25]):
    p = r["p"]
    tf = "5+15" if p[5] == [5,15] else "5"
    print(f"{i+1:>3} {p[0]:>5.3f} {p[1]:>3}-{p[2]:<3}s  {p[3]:.2f}-{p[4]:.2f}  {tf:>5} {p[6]:>2} "
          f"{r['trades']:>4} {r['wins']:>3} {r['losses']:>3} {r['wr']:>5.1f}% ${r['net']:>+6.2f} ${r['dd']:>5.2f}")


# ── PERFECT CONFIG ───────────────────────────────────────────────────────────

print(f"\n{'=' * 90}")
print("  PERFECT CONFIG")
print(f"{'=' * 90}")

# Best overall by different criteria
for label, pool, key_fn in [
    ("S3C: MAX NET", s3c_results, lambda x: -x["net"]),
    ("S3C: MAX NET (DD≤$3)", [r for r in s3c_results if r["dd"] <= 3], lambda x: -x["net"]),
    ("S3C: MAX WR (10+ tr)", [r for r in s3c_results if r["trades"] >= 10], lambda x: (-x["wr"], -x["net"])),
    ("SNIPER: MAX NET", sniper_results, lambda x: -x["net"]),
    ("SNIPER: MAX NET (DD≤$5)", [r for r in sniper_results if r["dd"] <= 5], lambda x: -x["net"]),
    ("SNIPER: MAX WR (10+ tr)", [r for r in sniper_results if r["trades"] >= 10], lambda x: (-x["wr"], -x["net"])),
    ("SNIPER: 0 LOSSES", [r for r in sniper_results if r["losses"] == 0 and r["trades"] >= 3], lambda x: (-x["trades"], -x["net"])),
]:
    if not pool:
        print(f"\n  {label}: no configs found")
        continue
    pool.sort(key=key_fn)
    r = pool[0]; p = r["p"]
    print(f"\n  {label}:")
    print(f"    params={p}")
    print(f"    Trades={r['trades']} W={r['wins']} L={r['losses']} WR={r['wr']:.1f}% Net=${r['net']:+.2f} DD=${r['dd']:.2f}")

# Ultimate combo
print(f"\n{'=' * 90}")
print("  ULTIMATE COMBO: best S3C + best Sniper running together")
print(f"{'=' * 90}")
if s3c_results and sniper_results:
    s = s3c_results[0]  # best net S3C
    n = sorted([r for r in sniper_results if r["dd"] <= 5], key=lambda x: -x["net"])[0]
    print(f"  S3C:    {s['trades']}tr {s['wins']}W/{s['losses']}L {s['wr']:.1f}% ${s['net']:+.2f} DD=${s['dd']:.2f}")
    print(f"          params={s['p']}")
    print(f"  Sniper: {n['trades']}tr {n['wins']}W/{n['losses']}L {n['wr']:.1f}% ${n['net']:+.2f} DD=${n['dd']:.2f}")
    print(f"          params={n['p']}")
    ct = s["trades"] + n["trades"]
    cw = s["wins"] + n["wins"]
    cl = s["losses"] + n["losses"]
    cn = s["net"] + n["net"]
    print(f"\n  COMBINED: {ct}tr {cw}W/{cl}L {cw/ct*100:.1f}% ${cn:+.2f}")
