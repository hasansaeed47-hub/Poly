#!/usr/bin/env python3
"""Sniper grid search on pre-structured data. Should complete in seconds."""

import json, time
from collections import defaultdict

t0 = time.time()
with open("/tmp/poly_data.json") as f:
    data = json.load(f)

slugs = data["slugs"]
STAKE = 5.0
STDEV = {"btc":0.167,"eth":0.194,"sol":0.247,"xrp":0.440}
SB = 0.167
MD = {"btc":0.015,"eth":0.020,"sol":0.030,"xrp":0.050}

def pmfee(px):
    if px<=0 or px>=1: return 0
    return px*(1-px)*0.0625

def run(delta, es, ee, mb, xb, tfs, cont, eslip, max_sp, min_dp,
        bn_filt, cl_filt, assets):
    trades = []
    details = []
    for s in slugs:
        if s["asset"] not in assets: continue
        if s["tf"] not in tfs: continue
        oc = s["outcome"]
        cc = 0
        entered = False
        for e in s["entries"]:  # sorted by secs desc (earliest first)
            if entered: break
            secs = e["secs"]
            if secs > es or secs < ee: continue
            pct = abs(e["pct_move"])
            direction = "UP" if e["pct_move"] > 0 else "DOWN"
            asset = s["asset"]
            if pct < MD.get(asset, 0.05): continue
            scaled = delta * (STDEV.get(asset, SB) / SB)
            if pct < scaled: continue
            # Momentum filters
            if bn_filt:
                if direction=="UP" and e["bn_mom_5s"] < -0.02: continue
                if direction=="DOWN" and e["bn_mom_5s"] > 0.02: continue
            if cl_filt:
                if direction=="UP" and e["cl_mom_5s"] < -0.03: continue
                if direction=="DOWN" and e["cl_mom_5s"] > 0.03: continue
            # Continuity
            if cont > 0:
                cc += 1
                if cc < cont: continue
            # Book filters
            side = "yes" if direction=="UP" else "no"
            ask = e[f"ask_{side}"]
            if ask < mb or ask > xb: continue
            if e[f"spread_{side}"] > max_sp: continue
            if e[f"depth_{side}"] < min_dp: continue
            # Fill
            fill = round(min(max(ask + eslip, 0.01), 0.99), 3)
            if fill < mb or fill > xb or fill >= 1 or fill <= 0: continue
            entered = True
            # Settle
            ov = 1.0 if oc=="YES" else 0.0
            ep = ov if direction=="UP" else 1.0-ov
            sh = STAKE / fill
            f_ = pmfee(fill)*sh + (0.015*STAKE if eslip>=0 else 0)
            net = (ep - fill)*sh - f_
            trades.append(net)
            details.append({
                "slug": s["slug"], "asset": asset, "tf": s["tf"],
                "side": direction, "fill": fill, "pct": e["pct_move"],
                "secs": secs, "net": round(net, 2), "outcome": oc,
            })

    if len(trades) < 2: return None
    w = sum(1 for t in trades if t > 0)
    pnl = sum(trades)
    # Max drawdown
    cum = 0; pk = 0; dd = 0
    for t in trades:
        cum += t
        if cum > pk: pk = cum
        d = pk - cum
        if d > dd: dd = d
    return {"trades":len(trades),"wins":w,"losses":len(trades)-w,
            "wr":round(w/len(trades)*100,1),"pnl":round(pnl,2),
            "dd":round(dd,2),"details":details}

# ══════════════════════════════════════════════════════════════════════════════
# GRID
# ══════════════════════════════════════════════════════════════════════════════

results = []
cnt = 0
for delta in [0.015, 0.02, 0.025, 0.03, 0.035, 0.04, 0.05, 0.06, 0.07, 0.08]:
    for es in [60, 75, 90, 120, 150, 180, 240, 290]:
        for ee in [10, 15, 20, 30, 40, 60]:
            if ee >= es: continue
            for mb in [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85]:
                for xb in [0.80, 0.85, 0.88, 0.90, 0.92, 0.95]:
                    if mb >= xb: continue
                    for tfs in [[5], [15], [5,15]]:
                        for cont in [0, 1, 2, 3]:
                            for eslip in [-0.01, 0, 0.005]:
                                for max_sp in [0.02, 0.03]:
                                    for min_dp in [500, 2000]:
                                        for bn in [True, False]:
                                            for cl in [True, False]:
                                                for assets in [["btc","eth","sol"], ["btc","eth"], ["btc"], ["eth"], ["sol"]]:
                                                    cnt += 1
                                                    r = run(delta, es, ee, mb, xb, tfs, cont, eslip,
                                                           max_sp, min_dp, bn, cl, assets)
                                                    if r and r["trades"] >= 3:
                                                        r["p"] = (f"d={delta} {es}-{ee}s bk={mb}-{xb} tf={tfs} "
                                                                  f"c={cont} slip={eslip} sp≤{max_sp} dp≥{min_dp} "
                                                                  f"bn={bn} cl={cl} a={assets}")
                                                        results.append(r)
                                                    # Early exit on huge grid
                                                    if cnt % 500000 == 0:
                                                        print(f"  {cnt:,} configs...", flush=True)

elapsed = time.time() - t0
print(f"\n  {cnt:,} configs in {elapsed:.1f}s ({cnt/elapsed:,.0f} configs/sec)")

# ══════════════════════════════════════════════════════════════════════════════
# RESULTS
# ══════════════════════════════════════════════════════════════════════════════

# Filter and sort
g80 = [r for r in results if r["wr"] >= 80]
g80.sort(key=lambda x: (-x["pnl"], -x["wr"]))

g85 = [r for r in results if r["wr"] >= 85]
g85.sort(key=lambda x: (-x["pnl"], -x["wr"]))

g90 = [r for r in results if r["wr"] >= 90]
g90.sort(key=lambda x: (-x["pnl"], -x["wr"]))

all3 = [r for r in results if r["trades"] >= 3]
all3.sort(key=lambda x: (-x["wr"], -x["pnl"]))

print(f"\n{'='*120}")
print(f"  SNIPER GRID: {len(results)} viable configs | ≥80%WR: {len(g80)} | ≥85%WR: {len(g85)} | ≥90%WR: {len(g90)}")
print(f"{'='*120}")

for label, pool in [("≥90% WR", g90), ("≥85% WR", g85), ("≥80% WR (by PnL)", g80)]:
    if not pool: continue
    # Deduplicate by trade count + WR (many configs hit same trades)
    seen = set()
    unique = []
    for r in pool:
        key = (r["trades"], r["wins"], r["losses"], r["pnl"])
        if key in seen: continue
        seen.add(key)
        unique.append(r)

    print(f"\n  ── {label}: {len(unique)} unique results ──")
    print(f"  {'#':>3} {'Tr':>4} {'W':>3} {'L':>3} {'WR':>6} {'PnL':>8} {'DD':>6} {'$/tr':>6}  Config")
    print(f"  {'-'*115}")
    for i, r in enumerate(unique[:30]):
        avg = r["pnl"]/r["trades"]
        print(f"  {i+1:>3} {r['trades']:>4} {r['wins']:>3} {r['losses']:>3} "
              f"{r['wr']:>5.1f}% ${r['pnl']:>+7.2f} ${r['dd']:>5.2f} ${avg:>+5.2f}  {r['p']}")

# ══════════════════════════════════════════════════════════════════════════════
# BEST CONFIG DEEP DIVE
# ══════════════════════════════════════════════════════════════════════════════

# Pick best: highest PnL among ≥80% WR with ≥5 trades
best_pool = [r for r in g80 if r["trades"] >= 5]
best_pool.sort(key=lambda x: -x["pnl"])

if best_pool:
    print(f"\n{'='*120}")
    print(f"  RECOMMENDED LIVE CONFIG (best PnL with ≥80%WR + ≥5 trades)")
    print(f"{'='*120}")
    b = best_pool[0]
    avg = b["pnl"]/b["trades"]
    print(f"\n  {b['p']}")
    print(f"  Trades={b['trades']} W={b['wins']} L={b['losses']} WR={b['wr']}%")
    print(f"  PnL=${b['pnl']:+.2f} DD=${b['dd']:.2f} $/trade=${avg:+.2f}")

    if "slip=-0.01" in b["p"]:
        print(f"\n  Fill: MAKER (ask-1c). Live ~35% fill → ~{max(1,int(b['trades']*0.35))} fills, ~${b['pnl']*0.35:+.2f}")
    elif "slip=0.005" in b["p"]:
        print(f"  Fill: TAKER+0.5c slip. Live ~92% fill → ~{max(1,int(b['trades']*0.92))} fills, ~${b['pnl']*0.92:+.2f}")
    else:
        print(f"  Fill: TAKER at ask. Live ~92% fill → ~{max(1,int(b['trades']*0.92))} fills, ~${b['pnl']*0.92:+.2f}")

    print(f"\n  TRADE LOG:")
    print(f"  {'#':>3} {'W/L':>4} {'Net':>7} {'Asset':>5} {'TF':>3} {'Side':>5} {'Fill':>5} {'Secs':>5} {'Pct':>8}  Slug")
    print(f"  {'-'*100}")
    cum = 0
    for i, d in enumerate(b["details"]):
        wl = "WIN" if d["net"] > 0 else "LOSS"
        cum += d["net"]
        print(f"  {i+1:>3} {wl:>4} ${d['net']:>+5.2f} {d['asset']:>5} {d['tf']:>3}m {d['side']:>5} "
              f"{d['fill']:>5.2f} {d['secs']:>5.0f} {d['pct']:>+7.4f}  {d['slug'][:40]}")
    print(f"\n  Cumulative PnL: ${cum:+.2f}")

# Also show best conservative (≥80%WR, ≥8 trades, taker fills)
cons_pool = [r for r in g80 if r["trades"] >= 8 and "slip=0" in r["p"]]
cons_pool.sort(key=lambda x: -x["pnl"])
if cons_pool:
    print(f"\n  ── CONSERVATIVE ALT (≥8 trades, taker at ask) ──")
    b2 = cons_pool[0]
    avg2 = b2["pnl"]/b2["trades"]
    print(f"  {b2['p']}")
    print(f"  Trades={b2['trades']} W={b2['wins']} L={b2['losses']} WR={b2['wr']}% PnL=${b2['pnl']:+.2f} $/tr=${avg2:+.2f}")

# Best by raw trade count (volume)
vol_pool = [r for r in g80]
vol_pool.sort(key=lambda x: (-x["trades"], -x["pnl"]))
if vol_pool:
    print(f"\n  ── HIGHEST VOLUME 80%+WR ──")
    v = vol_pool[0]
    avgv = v["pnl"]/v["trades"]
    print(f"  {v['p']}")
    print(f"  Trades={v['trades']} W={v['wins']} L={v['losses']} WR={v['wr']}% PnL=${v['pnl']:+.2f} $/tr=${avgv:+.2f}")
