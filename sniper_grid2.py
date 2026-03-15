#!/usr/bin/env python3
"""Sniper grid — lean. ~50K configs max."""

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

def run(delta, es, ee, mb, xb, tfs, cont, eslip, bn_f, cl_f):
    trades = []; details = []
    for s in slugs:
        if s["tf"] not in tfs: continue
        oc = s["outcome"]; asset = s["asset"]; cc = 0; entered = False
        for e in s["entries"]:
            if entered: break
            secs = e["secs"]
            if secs > es or secs < ee: continue
            pct = abs(e["pct_move"])
            d = "UP" if e["pct_move"] > 0 else "DOWN"
            if pct < MD.get(asset, 0.05): continue
            if pct < delta * (STDEV.get(asset, SB) / SB): continue
            if bn_f:
                if d=="UP" and e["bn_mom_5s"]<-0.02: continue
                if d=="DOWN" and e["bn_mom_5s"]>0.02: continue
            if cl_f:
                if d=="UP" and e["cl_mom_5s"]<-0.03: continue
                if d=="DOWN" and e["cl_mom_5s"]>0.03: continue
            if cont > 0:
                cc += 1
                if cc < cont: continue
            side = "yes" if d=="UP" else "no"
            ask = e[f"ask_{side}"]
            if ask < mb or ask > xb: continue
            if e[f"spread_{side}"] > 0.03: continue
            if e[f"depth_{side}"] < 500: continue
            fill = round(min(max(ask+eslip, 0.01), 0.99), 3)
            if fill < mb or fill > xb or fill >= 1 or fill <= 0: continue
            entered = True
            ov = 1.0 if oc=="YES" else 0.0
            ep = ov if d=="UP" else 1.0-ov
            sh = STAKE/fill
            f_ = pmfee(fill)*sh + (0.015*STAKE if eslip>=0 else 0)
            net = (ep-fill)*sh - f_
            trades.append(net)
            details.append({"slug":s["slug"],"asset":asset,"tf":s["tf"],
                           "side":d,"fill":fill,"pct":e["pct_move"],
                           "secs":secs,"net":round(net,2),"outcome":oc})
    if len(trades)<3: return None
    w=sum(1 for t in trades if t>0)
    cum=0;pk=0;dd=0
    for t in trades:
        cum+=t
        if cum>pk:pk=cum
        d2=pk-cum
        if d2>dd:dd=d2
    return {"trades":len(trades),"wins":w,"losses":len(trades)-w,
            "wr":round(w/len(trades)*100,1),"pnl":round(sum(trades),2),
            "dd":round(dd,2),"details":details}

results = []; cnt = 0
for delta in [0.015,0.02,0.025,0.03,0.035,0.04,0.05,0.06,0.07,0.08]:
    for es in [60,75,90,120,150,180,240,290]:
        for ee in [10,15,20,30,40,60]:
            if ee>=es: continue
            for mb in [0.40,0.50,0.55,0.60,0.65,0.70,0.75,0.80,0.85]:
                for xb in [0.85,0.88,0.92,0.95]:
                    if mb>=xb: continue
                    for tfs in [[5],[15],[5,15]]:
                        for cont in [0,1,2,3]:
                            for eslip in [-0.01,0,0.005]:
                                for bn_f in [True,False]:
                                    for cl_f in [True,False]:
                                        cnt+=1
                                        r=run(delta,es,ee,mb,xb,tfs,cont,eslip,bn_f,cl_f)
                                        if r:
                                            r["p"]=(f"d={delta} {es}-{ee}s bk={mb}-{xb} tf={tfs} "
                                                   f"c={cont} slip={eslip} bn={bn_f} cl={cl_f}")
                                            results.append(r)

elapsed=time.time()-t0
print(f"{cnt:,} configs in {elapsed:.1f}s ({cnt/elapsed:,.0f}/sec)\n")

# Deduplicate
seen=set(); unique=[]
for r in results:
    k=(r["trades"],r["wins"],r["pnl"])
    if k not in seen: seen.add(k); unique.append(r)

g80=[r for r in unique if r["wr"]>=80]; g80.sort(key=lambda x:(-x["pnl"],-x["wr"]))
g85=[r for r in unique if r["wr"]>=85]; g85.sort(key=lambda x:(-x["pnl"]))
g90=[r for r in unique if r["wr"]>=90]; g90.sort(key=lambda x:(-x["pnl"]))

print(f"{'='*120}")
print(f"  SNIPER: {len(unique)} unique | ≥80%:{len(g80)} | ≥85%:{len(g85)} | ≥90%:{len(g90)}")
print(f"{'='*120}")

for label,pool in [("≥90%WR",g90),("≥85%WR",g85),("≥80%WR TOP PnL",g80)]:
    if not pool: continue
    print(f"\n  ── {label} ({len(pool)} configs) ──")
    print(f"  {'#':>3} {'Tr':>4} {'W':>3} {'L':>3} {'WR':>6} {'PnL':>8} {'DD':>6} {'$/tr':>6}  Config")
    print(f"  {'-'*115}")
    for i,r in enumerate(pool[:20]):
        avg=r["pnl"]/r["trades"]
        print(f"  {i+1:>3} {r['trades']:>4} {r['wins']:>3} {r['losses']:>3} "
              f"{r['wr']:>5.1f}% ${r['pnl']:>+7.2f} ${r['dd']:>5.2f} ${avg:>+5.2f}  {r['p']}")

# ══════════════════════════════════════════════════════════════════════════════
# BEST LIVE CONFIG
# ══════════════════════════════════════════════════════════════════════════════

# Best = highest PnL among ≥80%WR, ≥5 trades
best_pool=[r for r in g80 if r["trades"]>=5]
best_pool.sort(key=lambda x:-x["pnl"])

# Also find best ≥10 trades
vol_pool=[r for r in g80 if r["trades"]>=10]
vol_pool.sort(key=lambda x:-x["pnl"])

# Best ≥85% WR with ≥5 trades
safe_pool=[r for r in g85 if r["trades"]>=5]
safe_pool.sort(key=lambda x:-x["pnl"])

for label, pool in [("BEST (≥80%WR, ≥5 trades, top PnL)", best_pool),
                     ("SAFE (≥85%WR, ≥5 trades, top PnL)", safe_pool),
                     ("VOLUME (≥80%WR, ≥10 trades, top PnL)", vol_pool)]:
    if not pool: continue
    b=pool[0]; avg=b["pnl"]/b["trades"]
    print(f"\n{'='*120}")
    print(f"  {label}")
    print(f"{'='*120}")
    print(f"  Config: {b['p']}")
    print(f"  Trades={b['trades']} W={b['wins']} L={b['losses']} WR={b['wr']}%")
    print(f"  PnL=${b['pnl']:+.2f} DD=${b['dd']:.2f} $/trade=${avg:+.2f}")
    if "slip=-0.01" in b["p"]:
        print(f"  MAKER (ask-1c). Live ~35%: ~{max(1,int(b['trades']*0.35))} fills, ~${b['pnl']*0.35:+.2f}")
    elif "slip=0.005" in b["p"]:
        print(f"  TAKER+0.5c. Live ~92%: ~{max(1,int(b['trades']*0.92))} fills, ~${b['pnl']*0.92:+.2f}")
    else:
        print(f"  TAKER@ask. Live ~92%: ~{max(1,int(b['trades']*0.92))} fills, ~${b['pnl']*0.92:+.2f}")
    print(f"\n  TRADE LOG:")
    print(f"  {'#':>3} {'W/L':>4} {'$':>7} {'Cum':>7} {'Asset':>5} {'TF':>3} {'Dir':>5} {'Fill':>5} {'Secs':>5} {'Move%':>8}")
    print(f"  {'-'*80}")
    cum=0
    for i,d in enumerate(b["details"]):
        wl="W" if d["net"]>0 else "L"
        cum+=d["net"]
        print(f"  {i+1:>3} {wl:>4} ${d['net']:>+5.2f} ${cum:>+5.2f} {d['asset']:>5} {d['tf']:>3}m "
              f"{d['side']:>5} {d['fill']:>5.2f} {d['secs']:>5.0f} {d['pct']:>+7.4f}")
