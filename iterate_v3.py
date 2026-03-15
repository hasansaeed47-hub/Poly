#!/usr/bin/env python3
"""Ultra-fast grid: max ~5 candidates per slug per strategy → ~350 rows total.
Grid filters over these tiny arrays = millions of configs/sec.
"""

import json, time
from collections import defaultdict

t0 = time.time()
print("Loading...", flush=True)
signals = []
with open("/tmp/signals_lite_full.jsonl") as f:
    for line in f:
        if line.strip(): signals.append(json.loads(line))
signals.sort(key=lambda s: s["ts"])

settlements = {}
with open("/home/user/Poly/settlements_2026-03-15.jsonl") as f:
    for line in f:
        if line.strip():
            s = json.loads(line); settlements[s["slug"]] = s

slug_oc = {slug: s["outcome"] for slug, s in settlements.items()}

t30 = {}
for sig in signals:
    secs = sig["secs_left"]
    if 15 <= secs <= 45:
        slug = sig["slug"]
        if slug not in t30 or abs(secs-30) < abs(t30[slug]["secs_left"]-30):
            t30[slug] = sig

STAKE = 5.0
STDEV = {"btc":0.167,"eth":0.194,"sol":0.247,"xrp":0.440}
SB = 0.167
MD = {"btc":0.015,"eth":0.020,"sol":0.030,"xrp":0.050}

def pmfee(px):
    if px<=0 or px>=1: return 0
    return px*(1-px)*0.0625

def gate_ok(sig):
    return (sig.get("data_quality")=="full" and not sig.get("cl_stale")
            and not sig.get("bn_stale") and not sig.get("book_stale")
            and sig.get("book_age_ms",9999)<=3000)

# ══════════════════════════════════════════════════════════════════════════════
# Pre-compute: For each slug, keep ONE candidate per 30-second time bucket
# This gives us ~10 candidates per slug max (300s / 30s = 10 buckets)
# ══════════════════════════════════════════════════════════════════════════════

print("Pre-computing bucketed candidates...", flush=True)

# Oracle: best edge per slug per 30s bucket
oracle_by_slug_bucket = {}  # (slug, bucket) -> best sig
for sig in signals:
    if not gate_ok(sig): continue
    be = sig.get("best_edge", 0)
    bs = sig.get("best_side", "")
    if be < 0.05 or not bs: continue
    slug = sig["slug"]
    bucket = int(sig["secs_left"] / 30) * 30
    key = (slug, bucket)
    if key not in oracle_by_slug_bucket or be > oracle_by_slug_bucket[key].get("best_edge", 0):
        side = bs.upper()
        oracle_by_slug_bucket[key] = {
            "slug": slug, "secs": sig["secs_left"], "edge": be, "side": side,
            "ask": sig.get(f"ask_{side.lower()}", 0),
            "fair": sig.get(f"fair_{side.lower()}", 0.5),
            "spread": sig.get(f"spread_{side.lower()}", 1),
            "depth": sig.get(f"depth_{side.lower()}", 0),
            "tf": sig["tf"], "best_edge": be,
        }
oracle_pool = sorted(oracle_by_slug_bucket.values(), key=lambda x: -x["secs"])
print(f"  Oracle: {len(oracle_pool)} bucketed candidates")

# S3C: best balanced signal per slug per 30s bucket
s3c_by_sb = {}
for sig in signals:
    if not gate_ok(sig): continue
    ay = sig.get("ask_yes",0); an = sig.get("ask_no",0)
    if ay<=0.01 or an<=0.01 or ay>=0.99 or an>=0.99: continue
    slug = sig["slug"]
    bucket = int(sig["secs_left"]/30)*30
    key = (slug, bucket)
    # Prefer most balanced (closest to 0.50/0.50)
    balance = abs(ay - 0.5) + abs(an - 0.5)
    if key not in s3c_by_sb or balance < s3c_by_sb[key]["_bal"]:
        s3c_by_sb[key] = {
            "slug": slug, "secs": sig["secs_left"],
            "ask_yes": ay, "ask_no": an,
            "spread_yes": sig.get("spread_yes",1), "spread_no": sig.get("spread_no",1),
            "depth_yes": sig.get("depth_yes",0), "depth_no": sig.get("depth_no",0),
            "tf": sig["tf"], "_bal": balance,
        }
s3c_pool = sorted(s3c_by_sb.values(), key=lambda x: -x["secs"])
print(f"  S3C: {len(s3c_pool)} bucketed candidates")

# Sniper: strongest delta per slug per 30s bucket
sni_by_sb = {}
for sig in signals:
    if not gate_ok(sig): continue
    pct = sig.get("pct_move", 0)
    if abs(pct) < 0.01: continue
    slug = sig["slug"]
    bucket = int(sig["secs_left"]/30)*30
    key = (slug, bucket)
    if key not in sni_by_sb or abs(pct) > sni_by_sb[key]["pct"]:
        direction = "UP" if pct > 0 else "DOWN"
        side_str = "YES" if direction == "UP" else "NO"
        ask = sig.get(f"ask_{side_str.lower()}", 0)
        if ask <= 0.01 or ask >= 0.99: continue
        sni_by_sb[key] = {
            "slug": slug, "secs": sig["secs_left"], "pct": abs(pct),
            "direction": direction, "side": side_str, "ask": ask,
            "spread": sig.get(f"spread_{side_str.lower()}", 1),
            "depth": sig.get(f"depth_{side_str.lower()}", 0),
            "tf": sig["tf"], "asset": sig["asset"],
            "bn_mom": sig.get("bn_momentum_5s",0)*100,
            "cl_mom": sig.get("cl_momentum_5s",0)*100,
            "ts": sig["ts"],
        }
sniper_pool = sorted(sni_by_sb.values(), key=lambda x: x["ts"])
print(f"  Sniper: {len(sniper_pool)} bucketed candidates")

# Gap: best edge per slug per 30s bucket
gap_by_sb = {}
for sig in signals:
    if not gate_ok(sig): continue
    fy = sig.get("fair_yes", 0.5)
    if abs(fy-0.5) < 0.02: continue
    slug = sig["slug"]
    bucket = int(sig["secs_left"]/30)*30
    key = (slug, bucket)
    if fy > 0.5:
        d="YES"; fair=fy; ask=sig.get("ask_yes",0); bid=sig.get("bid_yes",0)
    else:
        d="NO"; fair=sig.get("fair_no",0.5); ask=sig.get("ask_no",0); bid=sig.get("bid_no",0)
    edge = fair - ask
    if ask<=0.02 or ask>=0.98 or bid<=0: continue
    if key not in gap_by_sb or edge > gap_by_sb[key]["edge"]:
        gap_by_sb[key] = {
            "slug": slug, "secs": sig["secs_left"], "dir": d,
            "fair": fair, "ask": ask, "bid": bid, "edge": edge,
            "spread": sig.get(f"spread_{d.lower()}", 1),
            "depth": sig.get(f"depth_{d.lower()}", 0),
            "tf": sig["tf"],
        }
gap_pool = sorted(gap_by_sb.values(), key=lambda x: -x["secs"])
print(f"  Gap: {len(gap_pool)} bucketed candidates")
print(f"  Pre-compute: {time.time()-t0:.1f}s\n")


# ══════════════════════════════════════════════════════════════════════════════
# GRID FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def oracle_run(pool, min_edge, max_secs, min_secs, tfs, max_sp, min_dp, eslip):
    used = set(); trades = []
    for c in pool:
        if c["slug"] in used: continue
        if c["edge"] < min_edge: continue
        if c["secs"] > max_secs or c["secs"] < min_secs: continue
        if c["tf"] not in tfs: continue
        if c["spread"] > max_sp or c["depth"] < min_dp: continue
        if c["ask"] <= 0.02 or c["ask"] >= 0.98: continue
        fill = round(min(max(c["ask"]+eslip,0.01),0.99),3)
        if fill <= 0.01 or fill >= 0.99: continue
        used.add(c["slug"])
        if c["slug"] not in slug_oc: continue
        win = (slug_oc[c["slug"]] == c["side"])
        sh = STAKE/fill
        f_ = pmfee(fill)*sh + (0.015*STAKE if eslip>=0 else 0)
        net = ((1.0-fill)*sh - f_) if win else (-fill*sh - f_)
        trades.append(net)
    if len(trades) < 2: return None
    w = sum(1 for t in trades if t > 0)
    return {"trades":len(trades),"wins":w,"losses":len(trades)-w,
            "wr":w/len(trades)*100,"pnl":round(sum(trades),2)}

def s3c_run(pool, es, ee, al, ah, dp_price, sl, tfs, max_sp, min_dp, eslip):
    used = set(); trades = []
    for c in pool:
        if c["slug"] in used: continue
        if c["secs"] > es or c["secs"] < ee: continue
        if c["tf"] not in tfs: continue
        if c["ask_yes"]<al or c["ask_yes"]>ah or c["ask_no"]<al or c["ask_no"]>ah: continue
        if c["spread_yes"]>max_sp or c["spread_no"]>max_sp: continue
        if c["depth_yes"]<min_dp or c["depth_no"]<min_dp: continue
        fy=round(min(max(c["ask_yes"]+eslip,0.01),0.99),3)
        fn=round(min(max(c["ask_no"]+eslip,0.01),0.99),3)
        if fy>=1 or fn>=1: continue
        used.add(c["slug"])
        if c["slug"] not in slug_oc: continue
        up_win = slug_oc[c["slug"]]=="YES"
        su=STAKE/fy; sd=STAKE/fn
        ef=pmfee(fy)*su+pmfee(fn)*sd
        if eslip>=0: ef+=0.015*STAKE*2
        cost=STAKE*2+ef
        dsig=t30.get(c["slug"])
        if dsig:
            lb=dsig.get("bid_no" if up_win else "bid_yes",0)
            dp=max(min(lb,dp_price)-sl,0.01)
            lsh=sd if up_win else su; wsh=su if up_win else sd
            net=lsh*dp+wsh*1.0-cost-pmfee(dp)*lsh
        else:
            up=1.0 if up_win else 0.0
            net=su*up+sd*(1-up)-cost
        trades.append(net)
    if len(trades)<2: return None
    w=sum(1 for t in trades if t>0)
    return {"trades":len(trades),"wins":w,"losses":len(trades)-w,
            "wr":w/len(trades)*100,"pnl":round(sum(trades),2)}

def sniper_run(pool, delta, es, ee, mb, xb, tfs, cont, eslip):
    used = set(); trades = []; cc = defaultdict(int)
    for c in pool:
        if c["slug"] in used: continue
        if c["secs"]>es or c["secs"]<ee: continue
        if c["tf"] not in tfs: continue
        asset=c["asset"]
        if c["pct"]<MD.get(asset,0.05): continue
        scaled=delta*(STDEV.get(asset,SB)/SB)
        if c["pct"]<scaled: continue
        if c["direction"]=="UP" and c["bn_mom"]<-0.02: continue
        if c["direction"]=="DOWN" and c["bn_mom"]>0.02: continue
        if c["direction"]=="UP" and c["cl_mom"]<-0.03: continue
        if c["direction"]=="DOWN" and c["cl_mom"]>0.03: continue
        if cont>0:
            cc[c["slug"]]+=1
            if cc[c["slug"]]<cont: continue
        if c["ask"]<mb or c["ask"]>xb: continue
        if c["spread"]>0.03 or c["depth"]<500: continue
        fill=round(min(max(c["ask"]+eslip,0.01),0.99),3)
        if fill<mb or fill>xb or fill>=1 or fill<=0: continue
        used.add(c["slug"])
        if c["slug"] not in slug_oc: continue
        ov=1.0 if slug_oc[c["slug"]]=="YES" else 0.0
        ep=ov if c["direction"]=="UP" else 1.0-ov
        sh=STAKE/fill
        f_=pmfee(fill)*sh+(0.015*STAKE if eslip>=0 else 0)
        net=(ep-fill)*sh-f_
        trades.append(net)
    if len(trades)<2: return None
    w=sum(1 for t in trades if t>0)
    return {"trades":len(trades),"wins":w,"losses":len(trades)-w,
            "wr":w/len(trades)*100,"pnl":round(sum(trades),2)}

def gap_run(pool, min_edge, max_secs, min_secs, min_fd, max_sf, tfs, mo, eslip, min_dp):
    used=set(); trades=[]
    for c in pool:
        if c["slug"] in used: continue
        if len(trades)>=mo: break
        if c["secs"]>max_secs or c["secs"]<min_secs: continue
        if c["tf"] not in tfs: continue
        if abs(c["fair"]-0.5)<min_fd: continue
        if c["ask"]-c["bid"]>max_sf or c["edge"]<min_edge: continue
        if c["spread"]>max_sf or c["depth"]<min_dp: continue
        fill=round(min(max(c["ask"]+eslip,0.01),0.99),3)
        if fill<=0.01 or fill>=0.99: continue
        used.add(c["slug"])
        if c["slug"] not in slug_oc: continue
        win=(slug_oc[c["slug"]]==c["dir"])
        sh=STAKE/fill
        f_=pmfee(fill)*sh+(0.02*STAKE if eslip>=0 else 0)
        net=((1.0-fill)*STAKE-f_) if win else (-fill*STAKE-f_)
        trades.append(net)
    if len(trades)<2: return None
    w=sum(1 for t in trades if t>0)
    return {"trades":len(trades),"wins":w,"losses":len(trades)-w,
            "wr":w/len(trades)*100,"pnl":round(sum(trades),2)}

def show(name, results, min_wr=80, min_tr=3, top=25):
    good=[r for r in results if r["wr"]>=min_wr and r["trades"]>=min_tr]
    good.sort(key=lambda x:(-x["pnl"],-x["wr"]))
    alll=[r for r in results if r["trades"]>=min_tr]
    alll.sort(key=lambda x:(-x["wr"],-x["pnl"]))
    print(f"\n{'='*120}")
    print(f"  {name}: {len(results)} configs | {len(alll)} ≥{min_tr}tr | {len(good)} ≥{min_wr}%WR")
    print(f"{'='*120}")
    if good:
        print(f"  {'#':>3} {'Tr':>4} {'W':>3} {'L':>3} {'WR':>6} {'PnL':>8} {'$/tr':>6}  Params")
        print(f"  {'-'*110}")
        for i,r in enumerate(good[:top]):
            avg=r["pnl"]/r["trades"]
            print(f"  {i+1:>3} {r['trades']:>4} {r['wins']:>3} {r['losses']:>3} "
                  f"{r['wr']:>5.1f}% ${r['pnl']:>+7.2f} ${avg:>+5.2f}  {r['p']}")
    else:
        print(f"  No {min_wr}%WR configs. Best by WR:")
        for r in alll[:10]:
            avg=r["pnl"]/r["trades"] if r["trades"] else 0
            print(f"    WR={r['wr']:.1f}% Tr={r['trades']} PnL=${r['pnl']:+.2f} $/tr=${avg:+.2f} | {r['p']}")
        g70=[r for r in results if r["wr"]>=70 and r["trades"]>=5]
        g70.sort(key=lambda x:-x["pnl"])
        if g70:
            print(f"\n  ≥70%WR+≥5tr ({len(g70)}):")
            for r in g70[:5]:
                avg=r["pnl"]/r["trades"]
                print(f"    WR={r['wr']:.1f}% Tr={r['trades']} PnL=${r['pnl']:+.2f} $/tr=${avg:+.2f} | {r['p']}")
        g75=[r for r in results if r["wr"]>=75 and r["trades"]>=3]
        g75.sort(key=lambda x:-x["pnl"])
        if g75:
            print(f"\n  ≥75%WR+≥3tr ({len(g75)}):")
            for r in g75[:5]:
                avg=r["pnl"]/r["trades"]
                print(f"    WR={r['wr']:.1f}% Tr={r['trades']} PnL=${r['pnl']:+.2f} $/tr=${avg:+.2f} | {r['p']}")
    return good, alll


# ══════════════════════════════════════════════════════════════════════════════
# RUN GRIDS
# ══════════════════════════════════════════════════════════════════════════════

t1 = time.time()
print("[1/4] ORACLE...", flush=True)
orc=[]; cnt=0
for me in [0.10,0.15,0.20,0.25,0.30,0.35,0.40]:
    for mx in [150,180,240,290]:
        for mn in [30,40,60,80,100,120]:
            for tf in [[5],[15],[5,15]]:
                for msp in [0.02,0.03,0.04]:
                    for mdp in [200,500,1000]:
                        for es in [-0.01,0,0.005]:
                            cnt+=1
                            r=oracle_run(oracle_pool,me,mx,mn,tf,msp,mdp,es)
                            if r:
                                r["p"]=f"e≥{me} {mn}-{mx}s tf={tf} sp≤{msp} dp≥{mdp} slip={es}"
                                orc.append(r)
print(f"  {cnt} configs in {time.time()-t1:.1f}s")
orc_g,orc_a=show("ORACLE",orc)

t1=time.time()
print("\n[2/4] S3C...", flush=True)
s3c=[]; cnt=0
for es in [120,180,240,290]:
    for ee in [30,40,60,80]:
        if ee>=es: continue
        for al in [0.35,0.38,0.40,0.44,0.47]:
            for ah in [0.53,0.56,0.60,0.62,0.65]:
                for dp in [0.15,0.20,0.25,0.30,0.35,0.40,0.45,0.50]:
                    for sl in [0.003,0.005,0.008,0.01,0.015]:
                        for tf in [[5],[5,15]]:
                            for esl in [-0.01,0,0.005]:
                                for msp in [0.02,0.03]:
                                    for mdp in [300,500]:
                                        cnt+=1
                                        r=s3c_run(s3c_pool,es,ee,al,ah,dp,sl,tf,msp,mdp,esl)
                                        if r:
                                            r["p"]=f"{es}-{ee}s ask={al}-{ah} dump={dp} sl={sl} tf={tf} sp≤{msp} dp≥{mdp} entry={esl}"
                                            s3c.append(r)
print(f"  {cnt} configs in {time.time()-t1:.1f}s")
s3c_g,s3c_a=show("S3C",s3c)

t1=time.time()
print("\n[3/4] SNIPER...", flush=True)
sni=[]; cnt=0
for d in [0.02,0.025,0.03,0.035,0.04,0.05,0.06,0.07]:
    for es in [75,90,120,150,180,240]:
        for ee in [15,20,30,40,60]:
            if ee>=es: continue
            for mb in [0.50,0.55,0.60,0.65,0.70,0.75,0.80,0.85]:
                for xb in [0.85,0.88,0.90,0.92,0.95]:
                    for tf in [[5],[5,15]]:
                        for c in [0,2,3,4]:
                            for esl in [-0.01,0,0.005]:
                                cnt+=1
                                r=sniper_run(sniper_pool,d,es,ee,mb,xb,tf,c,esl)
                                if r:
                                    r["p"]=f"d={d} {es}-{ee}s bk={mb}-{xb} tf={tf} c={c} slip={esl}"
                                    sni.append(r)
print(f"  {cnt} configs in {time.time()-t1:.1f}s")
sni_g,sni_a=show("SNIPER",sni)

t1=time.time()
print("\n[4/4] GAP BOT...", flush=True)
gap=[]; cnt=0
for me in [0.05,0.08,0.10,0.15,0.20,0.25,0.30]:
    for mx in [180,240,300]:
        for mn in [30,45,60,90]:
            for mfd in [0.03,0.05,0.10,0.15,0.20]:
                for msf in [0.03,0.04,0.06,0.10]:
                    for tf in [[5],[15],[5,15]]:
                        for mo in [3,6,10,20]:
                            for esl in [-0.01,0,0.005]:
                                for mdp in [200,500]:
                                    cnt+=1
                                    r=gap_run(gap_pool,me,mx,mn,mfd,msf,tf,mo,esl,mdp)
                                    if r:
                                        r["p"]=f"e≥{me} {mn}-{mx}s fd≥{mfd} sf≤{msf} tf={tf} o≤{mo} slip={esl} dp≥{mdp}"
                                        gap.append(r)
print(f"  {cnt} configs in {time.time()-t1:.1f}s")
gap_g,gap_a=show("GAP BOT",gap)


# ══════════════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

print(f"\n{'='*120}")
print(f"  TOTAL TIME: {time.time()-t0:.1f}s")
print(f"{'='*120}")
print(f"\n{'='*120}")
print("  PRODUCTION-READY CONFIG CARDS")
print(f"{'='*120}")

for name,g,a in [("ORACLE",orc_g,orc_a),("S3C",s3c_g,s3c_a),
                  ("SNIPER",sni_g,sni_a),("GAP",gap_g,gap_a)]:
    pool=g if g else a[:3]
    if not pool:
        print(f"\n  [{name}] NO VIABLE CONFIG"); continue
    b=pool[0]; avg=b["pnl"]/b["trades"] if b["trades"] else 0
    st="PASS" if b["wr"]>=80 else ("NEAR" if b["wr"]>=70 else "BELOW")
    print(f"\n  [{name}] {st} — WR={b['wr']:.1f}% Tr={b['trades']} W={b['wins']} L={b['losses']} PnL=${b['pnl']:+.2f} $/tr=${avg:+.2f}")
    print(f"    {b['p']}")
    if "slip=-0.01" in b["p"]: print(f"    MAKER fill. Live ~35%: ~{max(1,int(b['trades']*0.35))} fills, ~${b['pnl']*0.35:+.2f}")
    elif "slip=0.005" in b["p"] or "entry=0.005" in b["p"]: print(f"    TAKER+slip. Live ~92%: ~{max(1,int(b['trades']*0.92))} fills, ~${b['pnl']*0.92:+.2f}")
    else: print(f"    TAKER at ask. Live ~92%: ~{max(1,int(b['trades']*0.92))} fills, ~${b['pnl']*0.92:+.2f}")

    # Higher volume alternative
    hvol=[r for r in (g if g else a) if r["trades"]>=8]
    hvol.sort(key=lambda x:-x["pnl"])
    if hvol and hvol[0]!=b:
        h=hvol[0]; ha=h["pnl"]/h["trades"]
        print(f"    ALT: WR={h['wr']:.1f}% Tr={h['trades']} PnL=${h['pnl']:+.2f} $/tr=${ha:+.2f} | {h['p']}")

print(f"\n{'='*120}")
print("  PRODUCTION CHECKLIST")
print(f"{'='*120}")
gs=sum(1 for s in signals if gate_ok(s))
print(f"""
  ENFORCED:
    [x] Data quality='full', no stale feeds, book_age≤3s
    [x] Spread≤2-4c, depth≥200-1000 (configurable)
    [x] PM fee: price*(1-price)*6.25%
    [x] Taker fee: 1.5-2% when slip≥0
    [x] Maker: ask-1c, no taker fee
    [x] One entry/slug, max open positions
    [x] S3C: real T-30 bid, capped dump, slippage on dump
    [x] Sniper: BN+CL momentum, per-asset delta scaling

  NOT MODELED:
    [ ] Maker fill rate (~35%) — divide trade count by ~3
    [ ] Latency 50-200ms
    [ ] Partial fills
    [ ] Mid-life SL/TP exits (settle-only here)
    [ ] Bot competition

  SIGNAL STATS: {len(signals):,} total, {gs:,} pass gate ({gs/len(signals)*100:.1f}%), {len(slug_oc)} settlements
""")
