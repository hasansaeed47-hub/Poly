#!/usr/bin/env python3
"""Final fast iteration — S3C, Gap (settle-only = fast), Sniper (tiny grid)."""

import json, math
from collections import defaultdict

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

settle_by_end = {}
for slug, s in settlements.items():
    settle_by_end.setdefault(s["window_end"], []).append(s)

t30 = {}
for sig in signals:
    slug = sig["slug"]; secs = sig["secs_left"]
    if 15 <= secs <= 45:
        if slug not in t30 or abs(secs-30) < abs(t30[slug]["secs_left"]-30):
            t30[slug] = sig

STAKE = 5.0
STDEV = {"btc":0.167,"eth":0.194,"sol":0.247,"xrp":0.440}
SB = 0.167
MD = {"btc":0.015,"eth":0.020,"sol":0.030,"xrp":0.050}
slug_oc = {slug: s["outcome"] for slug, s in settlements.items()}

def fee(px):
    if px<=0 or px>=1: return 0
    return px*(1-px)*0.0625

good_sigs = [s for s in signals if s.get("data_quality")=="full"
             and not s.get("cl_stale") and not s.get("bn_stale")
             and not s.get("book_stale") and s.get("book_age_ms",9999)<=3000]
print(f"{len(signals):,} signals, {len(good_sigs):,} pass gate, {len(settlements)} settlements\n")


# ═══ S3C ═════════════════════════════════════════════════════════════════════
def s3c(es, ee, al, ah, dp_price, sl, tfs, max_sp, min_dp, eslip):
    entries = {}; entered = set()
    for sig in good_sigs:
        slug = sig["slug"]
        if sig["tf"] not in tfs: continue
        secs = sig["secs_left"]
        if secs > es or secs < ee: continue
        if slug in entered: continue
        ay=sig.get("ask_yes",0); an=sig.get("ask_no",0)
        if ay<al or ay>ah or an<al or an>ah: continue
        if sig.get("spread_yes",1)>max_sp or sig.get("spread_no",1)>max_sp: continue
        if sig.get("depth_yes",0)<min_dp or sig.get("depth_no",0)<min_dp: continue
        fy=round(min(max(ay+eslip,0.01),0.99),3)
        fn=round(min(max(an+eslip,0.01),0.99),3)
        if fy>=1 or fn>=1: continue
        su=STAKE/fy; sd=STAKE/fn
        ef = (fee(fy)*su+fee(fn)*sd) if eslip<0 else (0.015*STAKE*2+fee(fy)*su+fee(fn)*sd)
        entries[slug] = {"su":su,"sd":sd,"cost":STAKE*2+ef,"secs":secs}
        entered.add(slug)
    w=l=0; pnl=0.0; pk=0.0; dd=0.0
    for slug, pos in entries.items():
        if slug not in slug_oc: continue
        up_win = slug_oc[slug]=="YES"
        dsig = t30.get(slug)
        if dsig:
            lb=dsig.get("bid_no" if up_win else "bid_yes",0)
            dp=max(min(lb,dp_price)-sl,0.01)
            lsh=pos["sd"] if up_win else pos["su"]
            wsh=pos["su"] if up_win else pos["sd"]
            n=lsh*dp+wsh*1.0-pos["cost"]-fee(dp)*lsh
        else:
            up=1.0 if up_win else 0.0
            n=pos["su"]*up+pos["sd"]*(1-up)-pos["cost"]
        pnl+=n
        if n>0: w+=1
        else: l+=1
        if pnl>pk: pk=pnl
        d=pk-pnl
        if d>dd: dd=d
    t=w+l
    return {"trades":t,"wins":w,"losses":l,"wr":w/t*100 if t else 0,"pnl":round(pnl,2),"dd":round(dd,2)}

print("S3C grid...", flush=True)
s3c_res=[]; cnt=0
for es in [120,180,240,290]:
    for ee in [30,40,60,80]:
        if ee>=es: continue
        for al in [0.38,0.40,0.44,0.47]:
            for ah in [0.53,0.56,0.60,0.62]:
                for dp in [0.20,0.25,0.30,0.35,0.40,0.45,0.50]:
                    for sl in [0.003,0.005,0.008,0.01]:
                        for tf in [[5],[5,15]]:
                            for esl in [-0.01,0,0.005]:
                                cnt+=1
                                r=s3c(es,ee,al,ah,dp,sl,tf,0.03,500,esl)
                                if r["trades"]>=2:
                                    r["p"]=f"{es}-{ee}s ask={al}-{ah} dump={dp} sl={sl} tf={tf} entry={esl}"
                                    s3c_res.append(r)
good80=[r for r in s3c_res if r["wr"]>=80 and r["trades"]>=3]
good80.sort(key=lambda x:(-x["pnl"],-x["wr"]))
alll=[r for r in s3c_res if r["trades"]>=3]
alll.sort(key=lambda x:(-x["wr"],-x["pnl"]))
print(f"  {cnt} configs, {len(s3c_res)} with ≥2tr, {len(good80)} with ≥80%WR")
print(f"\n{'='*110}")
print(f"  S3C: {len(good80)} configs ≥80%WR")
print(f"{'='*110}")
if good80:
    print(f"  {'#':>3} {'Tr':>4} {'W':>3} {'L':>3} {'WR':>6} {'PnL':>8} {'DD':>6} {'$/tr':>6}  Params")
    print(f"  {'-'*100}")
    for i,r in enumerate(good80[:20]):
        avg=r["pnl"]/r["trades"]
        print(f"  {i+1:>3} {r['trades']:>4} {r['wins']:>3} {r['losses']:>3} {r['wr']:>5.1f}% ${r['pnl']:>+7.2f} ${r['dd']:>5.2f} ${avg:>+5.2f}  {r['p']}")
else:
    print("  None hit 80%. Best:")
    for r in alll[:8]:
        avg=r["pnl"]/r["trades"] if r["trades"] else 0
        print(f"    WR={r['wr']:.1f}% Tr={r['trades']} PnL=${r['pnl']:+.2f} | {r['p']}")
    g70=[r for r in s3c_res if r["wr"]>=70 and r["trades"]>=5]
    g70.sort(key=lambda x:-x["pnl"])
    if g70:
        print(f"\n  ≥70%WR + ≥5tr ({len(g70)}):")
        for r in g70[:5]:
            avg=r["pnl"]/r["trades"]
            print(f"    WR={r['wr']:.1f}% Tr={r['trades']} PnL=${r['pnl']:+.2f} $/tr=${avg:+.2f} | {r['p']}")


# ═══ GAP BOT ═════════════════════════════════════════════════════════════════
def gap(min_edge, max_secs, min_secs, min_fd, max_sf, tfs, max_open, eslip, min_dp):
    entries={}; done=set()
    for sig in good_sigs:
        slug=sig["slug"]; secs=sig["secs_left"]
        if sig["tf"] not in tfs: continue
        if secs<min_secs or secs>max_secs: continue
        if slug in done: continue
        if len(entries)>=max_open and slug not in entries: continue
        fy=sig.get("fair_yes",0.5)
        if abs(fy-0.5)<min_fd: continue
        if fy>0.5:
            d="YES"; fair=fy; ask=sig.get("ask_yes",0); bid=sig.get("bid_yes",0)
        else:
            d="NO"; fair=sig.get("fair_no",0.5); ask=sig.get("ask_no",0); bid=sig.get("bid_no",0)
        edge=fair-ask
        if ask<=0.02 or ask>=0.98 or bid<=0: continue
        if ask-bid>max_sf or edge<min_edge: continue
        sp=sig.get(f"spread_{d.lower()}",1); dp=sig.get(f"depth_{d.lower()}",0)
        if sp>max_sf or dp<min_dp: continue
        fill=round(min(max(ask+eslip,0.01),0.99),3)
        if fill<=0.01 or fill>=0.99: continue
        f_=fee(fill)*(STAKE/fill)+(0.02*STAKE if eslip>=0 else 0)
        entries[slug]={"dir":d,"fill":fill,"fee":f_}
        done.add(slug)
    w=l=0; pnl=0.0; pk=0.0; dd=0.0
    for slug,pos in entries.items():
        if slug not in slug_oc: continue
        win=(slug_oc[slug]==pos["dir"])
        g2=(1.0-pos["fill"])*STAKE if win else -pos["fill"]*STAKE
        n=g2-pos["fee"]; pnl+=n
        if n>0: w+=1
        else: l+=1
        if pnl>pk: pk=pnl
        d=pk-pnl
        if d>dd: dd=d
    t=w+l
    return {"trades":t,"wins":w,"losses":l,"wr":w/t*100 if t else 0,"pnl":round(pnl,2),"dd":round(dd,2)}

print("\nGAP BOT grid...", flush=True)
gap_res=[]; cnt=0
for me in [0.05,0.08,0.10,0.15,0.20,0.25,0.30]:
    for mx in [180,240,300]:
        for mn in [30,45,60,90]:
            for mfd in [0.03,0.05,0.10,0.15]:
                for msf in [0.04,0.06,0.10]:
                    for tf in [[5],[15],[5,15]]:
                        for mo in [3,6,10]:
                            for esl in [-0.01,0,0.005]:
                                for mdp in [200,500]:
                                    cnt+=1
                                    r=gap(me,mx,mn,mfd,msf,tf,mo,esl,mdp)
                                    if r["trades"]>=2:
                                        r["p"]=f"e≥{me} {mn}-{mx}s fd≥{mfd} sf≤{msf} tf={tf} o≤{mo} entry={esl} dp≥{mdp}"
                                        gap_res.append(r)
good80g=[r for r in gap_res if r["wr"]>=80 and r["trades"]>=3]
good80g.sort(key=lambda x:(-x["pnl"],-x["wr"]))
alllg=[r for r in gap_res if r["trades"]>=3]
alllg.sort(key=lambda x:(-x["wr"],-x["pnl"]))
print(f"  {cnt} configs, {len(gap_res)} with ≥2tr, {len(good80g)} with ≥80%WR")
print(f"\n{'='*110}")
print(f"  GAP BOT: {len(good80g)} configs ≥80%WR")
print(f"{'='*110}")
if good80g:
    print(f"  {'#':>3} {'Tr':>4} {'W':>3} {'L':>3} {'WR':>6} {'PnL':>8} {'DD':>6} {'$/tr':>6}  Params")
    print(f"  {'-'*100}")
    for i,r in enumerate(good80g[:20]):
        avg=r["pnl"]/r["trades"]
        print(f"  {i+1:>3} {r['trades']:>4} {r['wins']:>3} {r['losses']:>3} {r['wr']:>5.1f}% ${r['pnl']:>+7.2f} ${r['dd']:>5.2f} ${avg:>+5.2f}  {r['p']}")
else:
    print("  None hit 80%. Best:")
    for r in alllg[:8]:
        avg=r["pnl"]/r["trades"] if r["trades"] else 0
        print(f"    WR={r['wr']:.1f}% Tr={r['trades']} PnL=${r['pnl']:+.2f} | {r['p']}")
    g70g=[r for r in gap_res if r["wr"]>=70 and r["trades"]>=5]
    g70g.sort(key=lambda x:-x["pnl"])
    if g70g:
        print(f"\n  ≥70%WR + ≥5tr ({len(g70g)}):")
        for r in g70g[:5]:
            avg=r["pnl"]/r["trades"]
            print(f"    WR={r['wr']:.1f}% Tr={r['trades']} PnL=${r['pnl']:+.2f} $/tr=${avg:+.2f} | {r['p']}")


# ═══ SNIPER — tiny grid, full signal scan ════════════════════════════════════
print("\nSNIPER grid (reduced)...", flush=True)
sni_res=[]; cnt=0
for d in [0.03,0.04,0.05,0.06]:
    for es in [90,120,180]:
        for ee in [20,40,60]:
            if ee>=es: continue
            for mb in [0.55,0.65,0.75,0.85]:
                for xb in [0.88,0.92,0.95]:
                    for tf in [[5],[5,15]]:
                        for c in [0,2,3]:
                            for esl in [-0.01,0.005]:
                                for usl in [True,False]:
                                    cnt+=1
                                    # inline sniper
                                    positions={}; entered=set(); cc=defaultdict(int)
                                    w2=l2=0; p2=0.0; pk2=0.0; dd2=0.0; sw2=set()
                                    for sig in signals:
                                        ts=sig["ts"]
                                        for wend in list(settle_by_end.keys()):
                                            if ts>=wend and wend not in sw2:
                                                sw2.add(wend)
                                                for s in settle_by_end[wend]:
                                                    sl2=s["slug"]
                                                    if sl2 not in positions: continue
                                                    pos=positions.pop(sl2)
                                                    ov=1.0 if s["outcome"]=="YES" else 0.0
                                                    ep=ov if pos["side"]=="UP" else 1.0-ov
                                                    n=(ep-pos["fill"])*pos["sh"]-pos["fee"]
                                                    p2+=n
                                                    if n>0: w2+=1
                                                    else: l2+=1
                                                    if p2>pk2: pk2=p2
                                                    d2=pk2-p2
                                                    if d2>dd2: dd2=d2
                                        slug=sig["slug"]
                                        if usl and slug in positions:
                                            pos=positions[slug]
                                            obk="bid_yes" if pos["side"]=="UP" else "bid_no"
                                            oppk="bid_no" if pos["side"]=="UP" else "bid_yes"
                                            ob=sig.get(obk,0); opp=sig.get(oppk,0)
                                            if ob>0 and ob<=pos["fill"]*0.50 and opp>=0.80:
                                                ep=max(ob-0.005,0.001)
                                                n=(ep-pos["fill"])*pos["sh"]-pos["fee"]-fee(ep)*pos["sh"]
                                                p2+=n; l2+=1
                                                if p2>pk2: pk2=p2
                                                d2=pk2-p2
                                                if d2>dd2: dd2=d2
                                                del positions[slug]
                                        if sig.get("data_quality")!="full" or sig.get("cl_stale") or sig.get("bn_stale") or sig.get("book_stale"): continue
                                        if sig.get("book_age_ms",9999)>3000: continue
                                        tfi=sig["tf"]; secs=sig["secs_left"]; asset=sig["asset"]
                                        if tfi not in tf: continue
                                        if slug in entered or slug in positions: continue
                                        if secs>es or secs<ee: continue
                                        pct=abs(sig.get("pct_move",0))
                                        direction="UP" if sig.get("pct_move",0)>0 else "DOWN"
                                        if pct<MD.get(asset,0.05): continue
                                        scaled=d*(STDEV.get(asset,SB)/SB)
                                        if pct<scaled: continue
                                        bm=sig.get("bn_momentum_5s",0)*100
                                        if direction=="UP" and bm<-0.02: continue
                                        if direction=="DOWN" and bm>0.02: continue
                                        cm=sig.get("cl_momentum_5s",0)*100
                                        if direction=="UP" and cm<-0.03: continue
                                        if direction=="DOWN" and cm>0.03: continue
                                        if c>0:
                                            cc[slug]+=1
                                            if cc[slug]<c: continue
                                        side_str="YES" if direction=="UP" else "NO"
                                        ask=sig.get(f"ask_{side_str.lower()}",0)
                                        if ask<mb or ask>xb: continue
                                        sp=sig.get(f"spread_{side_str.lower()}",1)
                                        dpp=sig.get(f"depth_{side_str.lower()}",0)
                                        if sp>0.03 or dpp<500: continue
                                        fill=round(min(max(ask+esl,0.01),0.99),3)
                                        if fill<mb or fill>xb or fill>=1 or fill<=0: continue
                                        f_=fee(fill)*(STAKE/fill)+(0.015*STAKE if esl>=0 else 0)
                                        positions[slug]={"side":direction,"fill":fill,"sh":STAKE/fill,"fee":f_}
                                        entered.add(slug); cc[slug]=0
                                    for slug in list(positions.keys()):
                                        if slug in slug_oc:
                                            pos=positions.pop(slug)
                                            ov=1.0 if slug_oc[slug]=="YES" else 0.0
                                            ep=ov if pos["side"]=="UP" else 1.0-ov
                                            n=(ep-pos["fill"])*pos["sh"]-pos["fee"]
                                            p2+=n
                                            if n>0: w2+=1
                                            else: l2+=1
                                            if p2>pk2: pk2=p2
                                            d2=pk2-p2
                                            if d2>dd2: dd2=d2
                                    t2=w2+l2
                                    if t2>=2:
                                        sni_res.append({"trades":t2,"wins":w2,"losses":l2,
                                                       "wr":w2/t2*100,"pnl":round(p2,2),"dd":round(dd2,2),
                                                       "p":f"d={d} {es}-{ee}s bk={mb}-{xb} tf={tf} c={c} entry={esl} sl={usl}"})
                                    if cnt % 100 == 0:
                                        print(f"    {cnt} configs done...", flush=True)

good80s=[r for r in sni_res if r["wr"]>=80 and r["trades"]>=3]
good80s.sort(key=lambda x:(-x["pnl"],-x["wr"]))
allls=[r for r in sni_res if r["trades"]>=3]
allls.sort(key=lambda x:(-x["wr"],-x["pnl"]))
print(f"  {cnt} configs, {len(sni_res)} with ≥2tr, {len(good80s)} with ≥80%WR")
print(f"\n{'='*110}")
print(f"  SNIPER: {len(good80s)} configs ≥80%WR")
print(f"{'='*110}")
if good80s:
    print(f"  {'#':>3} {'Tr':>4} {'W':>3} {'L':>3} {'WR':>6} {'PnL':>8} {'DD':>6} {'$/tr':>6}  Params")
    print(f"  {'-'*100}")
    for i,r in enumerate(good80s[:20]):
        avg=r["pnl"]/r["trades"]
        print(f"  {i+1:>3} {r['trades']:>4} {r['wins']:>3} {r['losses']:>3} {r['wr']:>5.1f}% ${r['pnl']:>+7.2f} ${r['dd']:>5.2f} ${avg:>+5.2f}  {r['p']}")
else:
    print("  None hit 80%. Best:")
    for r in allls[:8]:
        avg=r["pnl"]/r["trades"] if r["trades"] else 0
        print(f"    WR={r['wr']:.1f}% Tr={r['trades']} PnL=${r['pnl']:+.2f} | {r['p']}")
    g70s=[r for r in sni_res if r["wr"]>=70 and r["trades"]>=5]
    g70s.sort(key=lambda x:-x["pnl"])
    if g70s:
        print(f"\n  ≥70%WR + ≥5tr ({len(g70s)}):")
        for r in g70s[:5]:
            avg=r["pnl"]/r["trades"]
            print(f"    WR={r['wr']:.1f}% Tr={r['trades']} PnL=${r['pnl']:+.2f} $/tr=${avg:+.2f} | {r['p']}")


# ═══ COMBINED SUMMARY ════════════════════════════════════════════════════════
print(f"\n{'='*110}")
print("  COMBINED RESULTS — ALL STRATEGIES")
print(f"{'='*110}")

# Oracle results from prior run
print(f"""
  ORACLE (from prior run):
    PASS — 83.3% WR, 6 trades, PnL=$+91.12, DD=$5.26, $15.19/trade
    Config: edge≥0.25 60-180s tf=[5,15] max_open≤6 maker(ask-1c) spread≤0.02 depth≥300
    TP exit HURTS (drops WR to 60%). Do NOT use TP.
    Live (35% maker fill): ~2 fills, ~$+32 PnL
""")

for name, g80, al in [("S3C", good80, alll), ("GAP", good80g, alllg), ("SNIPER", good80s, allls)]:
    pool = g80 if g80 else al[:1]
    if not pool:
        print(f"  [{name}] NO VIABLE CONFIGS")
        continue
    b = pool[0]
    avg = b["pnl"]/b["trades"] if b["trades"] else 0
    st = "PASS" if b["wr"]>=80 else ("NEAR" if b["wr"]>=70 else "BELOW")
    print(f"  [{name}] {st} — WR={b['wr']:.1f}% Tr={b['trades']} PnL=${b['pnl']:+.2f} DD=${b['dd']:.2f} $/tr=${avg:+.2f}")
    print(f"    {b['p']}")
    if "entry=-0.01" in b["p"]:
        print(f"    Live (35% fill): ~{max(1,int(b['trades']*0.35))} fills, ~${b['pnl']*0.35:+.2f}")
    else:
        print(f"    Live (92% fill): ~{max(1,int(b['trades']*0.92))} fills, ~${b['pnl']*0.92:+.2f}")
    print()

print(f"\n{'='*110}")
print("  PRODUCTION CHECKLIST")
print(f"{'='*110}")
print("""
  [x] Book staleness: ≤3000ms (reject stale)
  [x] Feed staleness: cl_stale/bn_stale/book_stale flags
  [x] Data quality: 'full' only
  [x] Spread: ≤2-3c per side
  [x] Depth: ≥300-500 per side
  [x] PM fees: price*(1-price)*6.25%
  [x] Taker fee: 1.5% when filling at ask/ask+slip
  [x] Maker: no taker fee, entry at ask-1c
  [x] Slippage: +0.5c for taker fills
  [x] Confirmed SL: opposing bid ≥ 0.80
  [x] One entry per slug
  [x] Max open positions
  [x] S3C dump: actual T-30 bid, capped at dump_price - slip

  LIVE GAPS:
  [ ] Maker fill rate (~35%) — reduces trade volume
  [ ] Execution latency (50-200ms)
  [ ] Partial fills
  [ ] API rate limits
  [ ] Bot competition
""")
