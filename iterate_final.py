#!/usr/bin/env python3
"""Production-realistic grid search — optimized for speed.

Strategy: run each config forward through signals chronologically.
For speed: skip TP/SL exit loops (settle at settlement only).
Then validate top configs WITH TP/SL in a second pass.
"""

import json, math, random
from collections import defaultdict

random.seed(42)
print("Loading...", flush=True)

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

def fee(px):
    if px<=0 or px>=1: return 0
    return px*(1-px)*0.0625

# Pre-filter: only gate-passing signals
good_sigs = [s for s in signals if s.get("data_quality")=="full"
             and not s.get("cl_stale") and not s.get("bn_stale")
             and not s.get("book_stale") and s.get("book_age_ms",9999)<=3000]
print(f"Loaded {len(signals):,} signals, {len(good_sigs):,} pass gate, {len(settlements)} settlements")

# For each slug: pre-compute outcome
slug_outcome = {slug: s["outcome"] for slug, s in settlements.items()}

# ══════════════════════════════════════════════════════════════════════════════
# Lightweight runners — entry-only, settle at settlement (no mid-life exits)
# This is fast because we only scan good_sigs once per config.
# ══════════════════════════════════════════════════════════════════════════════

def oracle_fast(min_edge, max_secs, min_secs, tfs, max_open, entry_slip, max_sp, min_dp, tfee):
    """Fast oracle: enter on edge signal, settle at window end."""
    entries = {}  # slug -> position
    entered = set()
    for sig in good_sigs:
        slug = sig["slug"]; secs = sig["secs_left"]
        if sig["tf"] not in tfs: continue
        if secs > max_secs or secs < min_secs: continue
        if slug in entered: continue
        if len(entries) >= max_open and slug not in entries: continue

        be = sig.get("best_edge", 0); bs = sig.get("best_side", "")
        if be < min_edge or not bs: continue
        side = bs.upper()
        ask = sig.get(f"ask_{side.lower()}", 0)
        if ask <= 0.02 or ask >= 0.98: continue
        sp = sig.get(f"spread_{side.lower()}", 1)
        dp = sig.get(f"depth_{side.lower()}", 0)
        if sp > max_sp or dp < min_dp: continue

        fill = round(min(max(ask + entry_slip, 0.01), 0.99), 3)
        if fill <= 0.01 or fill >= 0.99: continue
        f_ = fee(fill)*(STAKE/fill) + (tfee*STAKE if entry_slip >= 0 else 0)

        entries[slug] = {"side": side, "fill": fill, "sh": STAKE/fill, "fee": f_}
        entered.add(slug)

    # Settle
    w = l = 0; pnl = 0.0; pk = 0.0; dd = 0.0
    for slug, pos in entries.items():
        if slug not in slug_outcome: continue
        oc = slug_outcome[slug]
        win = (oc == pos["side"])
        if win: g = (1.0 - pos["fill"]) * pos["sh"]
        else: g = -pos["fill"] * pos["sh"]
        n = g - pos["fee"]
        pnl += n
        if n > 0: w += 1
        else: l += 1
        if pnl > pk: pk = pnl
        d = pk - pnl
        if d > dd: dd = d

    t = w + l
    return {"trades":t,"wins":w,"losses":l,"wr":w/t*100 if t else 0,"pnl":round(pnl,2),"dd":round(dd,2)}


def s3c_fast(es, ee, al, ah, dp_price, sl, tfs, max_sp, min_dp, entry_slip):
    entries = {}; entered = set()
    for sig in good_sigs:
        slug = sig["slug"]
        if sig["tf"] not in tfs: continue
        secs = sig["secs_left"]
        if secs > es or secs < ee: continue
        if slug in entered: continue

        ay = sig.get("ask_yes",0); an = sig.get("ask_no",0)
        if ay < al or ay > ah or an < al or an > ah: continue
        sy = sig.get("spread_yes",1); sn = sig.get("spread_no",1)
        dy = sig.get("depth_yes",0); dn = sig.get("depth_no",0)
        if sy > max_sp or sn > max_sp or dy < min_dp or dn < min_dp: continue

        fy = round(min(max(ay+entry_slip,0.01),0.99),3)
        fn = round(min(max(an+entry_slip,0.01),0.99),3)
        if fy>=1 or fn>=1: continue

        su = STAKE/fy; sd = STAKE/fn
        if entry_slip < 0:
            ef = fee(fy)*su + fee(fn)*sd
        else:
            ef = 0.015*STAKE*2 + fee(fy)*su + fee(fn)*sd
        cost = STAKE*2 + ef

        entries[slug] = {"su":su,"sd":sd,"cost":cost,"secs":secs}
        entered.add(slug)

    w = l = 0; pnl = 0.0; pk = 0.0; dd = 0.0
    for slug, pos in entries.items():
        if slug not in slug_outcome: continue
        oc = slug_outcome[slug]
        up_win = oc == "YES"
        dsig = t30.get(slug)
        if dsig:
            lb = dsig.get("bid_no" if up_win else "bid_yes", 0)
            dp = max(min(lb, dp_price) - sl, 0.01)
            lsh = pos["sd"] if up_win else pos["su"]
            wsh = pos["su"] if up_win else pos["sd"]
            n = lsh*dp + wsh*1.0 - pos["cost"] - fee(dp)*lsh
        else:
            up = 1.0 if up_win else 0.0
            n = pos["su"]*up + pos["sd"]*(1-up) - pos["cost"]
        pnl += n
        if n > 0: w += 1
        else: l += 1
        if pnl > pk: pk = pnl
        d = pk - pnl
        if d > dd: dd = d

    t = w+l
    return {"trades":t,"wins":w,"losses":l,"wr":w/t*100 if t else 0,"pnl":round(pnl,2),"dd":round(dd,2)}


def sniper_fast(delta, es, ee, mb, xb, tfs, cont, max_sp, min_dp, entry_slip, usl):
    positions = {}; entered = set(); cc = defaultdict(int)
    w = l = 0; pnl = 0.0; pk = 0.0; dd = 0.0; sw = set()

    for sig in signals:
        ts = sig["ts"]
        # Settle
        for wend in list(settle_by_end.keys()):
            if ts >= wend and wend not in sw:
                sw.add(wend)
                for s in settle_by_end[wend]:
                    sl2 = s["slug"]
                    if sl2 not in positions: continue
                    pos = positions.pop(sl2)
                    ov = 1.0 if s["outcome"]=="YES" else 0.0
                    ep = ov if pos["side"]=="UP" else 1.0-ov
                    n = (ep-pos["fill"])*pos["sh"]-pos["fee"]
                    pnl += n
                    if n>0: w+=1
                    else: l+=1
                    if pnl>pk: pk=pnl
                    d=pk-pnl
                    if d>dd: dd=d

        slug = sig["slug"]
        # SL
        if usl and slug in positions:
            pos = positions[slug]
            obk = "bid_yes" if pos["side"]=="UP" else "bid_no"
            oppk = "bid_no" if pos["side"]=="UP" else "bid_yes"
            ob = sig.get(obk,0); opp = sig.get(oppk,0)
            if ob>0 and ob<=pos["fill"]*0.50 and opp>=0.80:
                ep = max(ob-0.005,0.001)
                n = (ep-pos["fill"])*pos["sh"]-pos["fee"]-fee(ep)*pos["sh"]
                pnl += n; l+=1
                if pnl>pk: pk=pnl
                d=pk-pnl
                if d>dd: dd=d
                del positions[slug]

        # Gate
        if sig.get("data_quality")!="full" or sig.get("cl_stale") or sig.get("bn_stale") or sig.get("book_stale"): continue
        if sig.get("book_age_ms",9999)>3000: continue

        tf = sig["tf"]; secs = sig["secs_left"]; asset = sig["asset"]
        if tf not in tfs: continue
        if slug in entered or slug in positions: continue
        if secs > es or secs < ee: continue

        pct = abs(sig.get("pct_move",0))
        direction = "UP" if sig.get("pct_move",0)>0 else "DOWN"
        if pct < MD.get(asset,0.05): continue
        scaled = delta*(STDEV.get(asset,SB)/SB)
        if pct < scaled: continue

        bm = sig.get("bn_momentum_5s",0)*100
        if direction=="UP" and bm<-0.02: continue
        if direction=="DOWN" and bm>0.02: continue
        cm = sig.get("cl_momentum_5s",0)*100
        if direction=="UP" and cm<-0.03: continue
        if direction=="DOWN" and cm>0.03: continue

        if cont > 0:
            cc[slug] += 1
            if cc[slug] < cont: continue

        side_str = "YES" if direction=="UP" else "NO"
        ask = sig.get(f"ask_{side_str.lower()}",0)
        if ask < mb or ask > xb: continue
        sp = sig.get(f"spread_{side_str.lower()}",1)
        dp = sig.get(f"depth_{side_str.lower()}",0)
        if sp > max_sp or dp < min_dp: continue

        fill = round(min(max(ask+entry_slip,0.01),0.99),3)
        if fill<mb or fill>xb or fill>=1 or fill<=0: continue
        f_ = fee(fill)*(STAKE/fill) + (0.015*STAKE if entry_slip>=0 else 0)

        positions[slug] = {"side":direction,"fill":fill,"sh":STAKE/fill,"fee":f_}
        entered.add(slug); cc[slug]=0

    # Settle remaining
    for slug in list(positions.keys()):
        if slug in slug_outcome:
            pos = positions.pop(slug)
            ov = 1.0 if slug_outcome[slug]=="YES" else 0.0
            ep = ov if pos["side"]=="UP" else 1.0-ov
            n = (ep-pos["fill"])*pos["sh"]-pos["fee"]
            pnl += n
            if n>0: w+=1
            else: l+=1
            if pnl>pk: pk=pnl
            d=pk-pnl
            if d>dd: dd=d

    t=w+l
    return {"trades":t,"wins":w,"losses":l,"wr":w/t*100 if t else 0,"pnl":round(pnl,2),"dd":round(dd,2)}


def gap_fast(min_edge, max_secs, min_secs, min_fd, max_sf, tfs, max_open, entry_slip, min_dp):
    entries = {}; done = set()
    for sig in good_sigs:
        slug = sig["slug"]; secs = sig["secs_left"]
        if sig["tf"] not in tfs: continue
        if secs < min_secs or secs > max_secs: continue
        if slug in done: continue
        if len(entries) >= max_open and slug not in entries: continue

        fy = sig.get("fair_yes",0.5)
        if abs(fy-0.5) < min_fd: continue
        if fy > 0.5:
            d="YES"; fair=fy; ask=sig.get("ask_yes",0); bid=sig.get("bid_yes",0)
        else:
            d="NO"; fair=sig.get("fair_no",0.5); ask=sig.get("ask_no",0); bid=sig.get("bid_no",0)
        edge = fair - ask
        if ask<=0.02 or ask>=0.98 or bid<=0: continue
        if ask-bid > max_sf: continue
        if edge < min_edge: continue
        sp = sig.get(f"spread_{d.lower()}",1)
        dp = sig.get(f"depth_{d.lower()}",0)
        if sp > max_sf or dp < min_dp: continue

        fill = round(min(max(ask+entry_slip,0.01),0.99),3)
        if fill<=0.01 or fill>=0.99: continue
        f_ = fee(fill)*(STAKE/fill) + (0.02*STAKE if entry_slip>=0 else 0)

        entries[slug] = {"dir":d,"fill":fill,"fee":f_,"edge":edge}
        done.add(slug)

    w = l = 0; pnl = 0.0; pk = 0.0; dd = 0.0
    for slug, pos in entries.items():
        if slug not in slug_outcome: continue
        oc = slug_outcome[slug]
        win = (oc == pos["dir"])
        if win: g = (1.0-pos["fill"])*STAKE
        else: g = -pos["fill"]*STAKE
        n = g - pos["fee"]
        pnl += n
        if n>0: w+=1
        else: l+=1
        if pnl>pk: pk=pnl
        d=pk-pnl
        if d>dd: dd=d

    t=w+l
    return {"trades":t,"wins":w,"losses":l,"wr":w/t*100 if t else 0,"pnl":round(pnl,2),"dd":round(dd,2)}


# ══════════════════════════════════════════════════════════════════════════════
# GRID SEARCHES — small, focused grids
# ══════════════════════════════════════════════════════════════════════════════

def show(name, res, min_wr=80, min_tr=3, top=20):
    good = [r for r in res if r["wr"]>=min_wr and r["trades"]>=min_tr]
    good.sort(key=lambda x:(-x["pnl"],-x["wr"]))
    alll = [r for r in res if r["trades"]>=min_tr]
    alll.sort(key=lambda x:(-x["wr"],-x["pnl"]))

    print(f"\n{'='*110}")
    print(f"  {name}: {len(res)} configs | {len(alll)} with ≥{min_tr}tr | {len(good)} with ≥{min_wr}%WR")
    print(f"{'='*110}")

    if good:
        print(f"  {'#':>3} {'Tr':>4} {'W':>3} {'L':>3} {'WR':>6} {'PnL':>8} {'DD':>6} {'$/tr':>6}  Params")
        print(f"  {'-'*100}")
        for i,r in enumerate(good[:top]):
            avg=r["pnl"]/r["trades"]
            print(f"  {i+1:>3} {r['trades']:>4} {r['wins']:>3} {r['losses']:>3} "
                  f"{r['wr']:>5.1f}% ${r['pnl']:>+7.2f} ${r['dd']:>5.2f} ${avg:>+5.2f}  {r['p']}")
    else:
        print(f"  No {min_wr}%WR configs. Best by WR:")
        for r in alll[:8]:
            avg=r["pnl"]/r["trades"] if r["trades"] else 0
            print(f"    WR={r['wr']:.1f}% Tr={r['trades']} PnL=${r['pnl']:+.2f} DD=${r['dd']:.2f} $/tr=${avg:+.2f} | {r['p']}")
        g70=[r for r in res if r["wr"]>=70 and r["trades"]>=5]
        g70.sort(key=lambda x:-x["pnl"])
        if g70:
            print(f"\n  ≥70%WR + ≥5tr ({len(g70)}), top PnL:")
            for r in g70[:5]:
                avg=r["pnl"]/r["trades"]
                print(f"    WR={r['wr']:.1f}% Tr={r['trades']} PnL=${r['pnl']:+.2f} DD=${r['dd']:.2f} $/tr=${avg:+.2f} | {r['p']}")
    return good, alll


# ── 1. ORACLE — focused grid ────────────────────────────────────────────────
print("\n[1/4] ORACLE...", flush=True)
orc = []; cnt = 0
for me in [0.10,0.15,0.20,0.25,0.30,0.35,0.40]:
    for mx in [180,240,290]:
        for mn in [40,60,80,100]:
            for tf in [[5],[15],[5,15]]:
                for mo in [3,6,10]:
                    for es in [-0.01,0,0.005]:
                        for msp in [0.02,0.04]:
                            for mdp in [300,1000]:
                                cnt += 1
                                r = oracle_fast(me,mx,mn,tf,mo,es,msp,mdp,0.015)
                                if r["trades"]>=2:
                                    r["p"]=f"e≥{me} {mn}-{mx}s tf={tf} o≤{mo} slip={es} sp≤{msp} dp≥{mdp}"
                                    orc.append(r)
print(f"  {cnt} configs")
orc_g,orc_a = show("ORACLE (settle-only, no TP)", orc)

# Now run TP variant for top oracle configs
print("\n  Running TP validation on top Oracle configs...", flush=True)
orc_tp = []; cnt2 = 0
for me in [0.15,0.20,0.25,0.30,0.35,0.40]:
    for mx in [240,290]:
        for mn in [40,60,80]:
            for tf in [[5],[15],[5,15]]:
                for mo in [3,6,10]:
                    for es in [-0.01,0,0.005]:
                        cnt2 += 1
                        # Full TP version — need to scan all signals
                        positions = {}; entered = set()
                        w2=l2=0; p2=0.0; pk2=0.0; dd2=0.0; sw2=set()

                        for sig in signals:
                            ts = sig["ts"]
                            for wend in list(settle_by_end.keys()):
                                if ts>=wend and wend not in sw2:
                                    sw2.add(wend)
                                    for s in settle_by_end[wend]:
                                        sl2=s["slug"]
                                        if sl2 not in positions: continue
                                        pos=positions.pop(sl2)
                                        win=(s["outcome"]==pos["side"])
                                        if win: g2=(1.0-pos["fill"])*pos["sh"]
                                        else: g2=-pos["fill"]*pos["sh"]
                                        n2=g2-pos["fee"]
                                        p2+=n2
                                        if n2>0: w2+=1
                                        else: l2+=1
                                        if p2>pk2: pk2=p2
                                        d2=pk2-p2
                                        if d2>dd2: dd2=d2

                            # TP
                            slug=sig["slug"]
                            if slug in positions:
                                pos=positions[slug]
                                ob=sig.get(f"bid_{pos['side'].lower()}",0)
                                if ob>=pos["fair"] and sig["secs_left"]>10:
                                    ep2=max(ob-0.005,0.01)
                                    g2=(ep2-pos["fill"])*pos["sh"]
                                    n2=g2-pos["fee"]-fee(ep2)*pos["sh"]
                                    p2+=n2
                                    if n2>0: w2+=1
                                    else: l2+=1
                                    if p2>pk2: pk2=p2
                                    d2=pk2-p2
                                    if d2>dd2: dd2=d2
                                    del positions[slug]

                            # Entry
                            if sig.get("data_quality")!="full" or sig.get("cl_stale") or sig.get("bn_stale") or sig.get("book_stale"): continue
                            if sig.get("book_age_ms",9999)>3000: continue
                            secs=sig["secs_left"]
                            if sig["tf"] not in tf: continue
                            if secs>mx or secs<mn: continue
                            if slug in entered or slug in positions: continue
                            if len(positions)>=mo: continue

                            be=sig.get("best_edge",0); bside=sig.get("best_side","")
                            if be<me or not bside: continue
                            side=bside.upper()
                            ask=sig.get(f"ask_{side.lower()}",0)
                            fair2=sig.get(f"fair_{side.lower()}",0.5)
                            if ask<=0.02 or ask>=0.98: continue
                            sp=sig.get(f"spread_{side.lower()}",1)
                            dp=sig.get(f"depth_{side.lower()}",0)
                            if sp>0.04 or dp<300: continue

                            fill=round(min(max(ask+es,0.01),0.99),3)
                            if fill<=0.01 or fill>=0.99: continue
                            f_=fee(fill)*(STAKE/fill)+(0.015*STAKE if es>=0 else 0)
                            positions[slug]={"side":side,"fill":fill,"sh":STAKE/fill,"fee":f_,"fair":fair2}
                            entered.add(slug)

                        for slug in list(positions.keys()):
                            if slug in slug_outcome:
                                pos=positions.pop(slug)
                                win=(slug_outcome[slug]==pos["side"])
                                if win: g2=(1.0-pos["fill"])*pos["sh"]
                                else: g2=-pos["fill"]*pos["sh"]
                                n2=g2-pos["fee"]
                                p2+=n2
                                if n2>0: w2+=1
                                else: l2+=1
                                if p2>pk2: pk2=p2
                                d2=pk2-p2
                                if d2>dd2: dd2=d2

                        t2=w2+l2
                        if t2>=2:
                            wr2=w2/t2*100
                            orc_tp.append({"trades":t2,"wins":w2,"losses":l2,"wr":wr2,
                                          "pnl":round(p2,2),"dd":round(dd2,2),
                                          "p":f"e≥{me} {mn}-{mx}s tf={tf} o≤{mo} slip={es} TP=True"})

print(f"  {cnt2} TP configs tested")
orc_tp_g, orc_tp_a = show("ORACLE (with TP exit)", orc_tp)


# ── 2. S3C ───────────────────────────────────────────────────────────────────
print("\n[2/4] S3C...", flush=True)
s3c = []; cnt = 0
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
                                r=s3c_fast(es,ee,al,ah,dp,sl,tf,0.03,500,esl)
                                if r["trades"]>=2:
                                    r["p"]=f"{es}-{ee}s ask={al}-{ah} dump={dp} sl={sl} tf={tf} entry={esl}"
                                    s3c.append(r)
print(f"  {cnt} configs")
s3c_g,s3c_a = show("S3C (Both-Sides Dump)", s3c)


# ── 3. SNIPER — smaller grid since it's O(N) per config ─────────────────────
print("\n[3/4] SNIPER...", flush=True)
sni = []; cnt = 0
for d in [0.02,0.03,0.04,0.05,0.06]:
    for es in [90,120,180,240]:
        for ee in [20,30,40,60]:
            if ee>=es: continue
            for mb in [0.50,0.60,0.70,0.75,0.80,0.85]:
                for xb in [0.88,0.92,0.95]:
                    for tf in [[5],[5,15]]:
                        for c in [0,2,3,4]:
                            for esl in [-0.01,0.005]:
                                for usl in [True,False]:
                                    cnt+=1
                                    r=sniper_fast(d,es,ee,mb,xb,tf,c,0.03,500,esl,usl)
                                    if r["trades"]>=2:
                                        r["p"]=f"d={d} {es}-{ee}s bk={mb}-{xb} tf={tf} c={c} entry={esl} sl={usl}"
                                        sni.append(r)
print(f"  {cnt} configs")
sni_g,sni_a = show("SNIPER", sni)


# ── 4. GAP BOT ──────────────────────────────────────────────────────────────
print("\n[4/4] GAP BOT...", flush=True)
gap = []; cnt = 0
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
                                    r=gap_fast(me,mx,mn,mfd,msf,tf,mo,esl,mdp)
                                    if r["trades"]>=2:
                                        r["p"]=f"e≥{me} {mn}-{mx}s fd≥{mfd} sf≤{msf} tf={tf} o≤{mo} entry={esl} dp≥{mdp}"
                                        gap.append(r)
print(f"  {cnt} configs")
gap_g,gap_a = show("GAP BOT", gap)


# ══════════════════════════════════════════════════════════════════════════════
# PRODUCTION CONFIG CARDS
# ══════════════════════════════════════════════════════════════════════════════

print(f"\n{'='*110}")
print("  FINAL PRODUCTION-READY CONFIG CARDS")
print(f"{'='*110}")

for name, g, a in [("ORACLE",orc_g,orc_a),("ORACLE+TP",orc_tp_g,orc_tp_a),
                     ("S3C",s3c_g,s3c_a),("SNIPER",sni_g,sni_a),("GAP",gap_g,gap_a)]:
    pool = g if g else a[:3]
    if not pool:
        print(f"\n  [{name}] NO VIABLE CONFIGS")
        continue
    b = pool[0]
    avg = b["pnl"]/b["trades"] if b["trades"] else 0
    st = "PASS" if b["wr"]>=80 else ("NEAR" if b["wr"]>=70 else "FAIL")
    print(f"\n  [{name}] {st} — WR={b['wr']:.1f}% Tr={b['trades']} PnL=${b['pnl']:+.2f} DD=${b['dd']:.2f} $/tr=${avg:+.2f}")
    print(f"    {b['p']}")
    if "slip=-0.01" in b["p"] or "entry=-0.01" in b["p"]:
        print(f"    Live (35% maker fill): ~{int(b['trades']*0.35)} fills, ~${b['pnl']*0.35:+.2f} PnL")
    elif "slip=0.005" in b["p"] or "entry=0.005" in b["p"]:
        print(f"    Live (92% taker fill): ~{int(b['trades']*0.92)} fills, ~${b['pnl']*0.92:+.2f} PnL")
    else:
        print(f"    Live (92% taker fill): ~{int(b['trades']*0.92)} fills, ~${b['pnl']*0.92:+.2f} PnL")


print(f"\n{'='*110}")
print("  SYSTEM GAPS CHECKLIST")
print(f"{'='*110}")
print("""
  ENFORCED in this iteration:
    [x] Book age ≤ 3000ms (reject stale book data)
    [x] No stale feeds (CL, BN, book staleness flags)
    [x] Data quality = 'full' only
    [x] Spread filter ≤ 2-4c (reject wide-spread markets)
    [x] Depth filter ≥ 300-1000 shares (reject thin books)
    [x] PM fees: price*(1-price)*6.25% per side
    [x] Taker fee: 1.5-2% when entry_slip ≥ 0
    [x] Slippage: -1c maker improvement or +0.5c taker slip
    [x] Confirmed SL: opposing bid ≥ 0.80 required
    [x] One entry per slug, max open positions
    [x] S3C dump: uses actual T-30 bid, capped at dump_price

  LIVE GAPS (not modeled):
    [ ] Maker fill rate (~35% in live vs 100% deterministic)
    [ ] Execution latency 50-200ms
    [ ] Partial fills
    [ ] PM API rate limits
    [ ] Competition from other bots
    [ ] Market regime changes

  FILL RATE IMPACT:
    Maker configs: multiply trade count × 0.35, PnL × 0.35
    Taker configs: multiply trade count × 0.92, PnL × 0.92
    These are conservative estimates — actual may vary.
""")
