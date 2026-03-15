#!/usr/bin/env python3
"""Vectorized grid search — pre-compute ONCE, filter with numpy-style ops.

Key insight: instead of looping 71K signals per config, pre-compute one
row per slug with all the fields we need, then configs just mask & sum.
69 slugs × fast = done in seconds.
"""

import json
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

slug_oc = {slug: s["outcome"] for slug, s in settlements.items()}

# T-30 signals for S3C
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
    if px <= 0 or px >= 1: return 0
    return px * (1-px) * 0.0625

def gate_ok(sig):
    return (sig.get("data_quality") == "full"
            and not sig.get("cl_stale") and not sig.get("bn_stale")
            and not sig.get("book_stale") and sig.get("book_age_ms", 9999) <= 3000)

# ══════════════════════════════════════════════════════════════════════════════
# PRE-COMPUTE: For each slug, find the BEST entry signal for each strategy
# at various time points. This gives us ~69 rows to work with.
# ══════════════════════════════════════════════════════════════════════════════

print("Pre-computing per-slug entry candidates...", flush=True)

# For Oracle/Gap: best edge signal per slug
# For Sniper: first strong delta signal per slug
# For S3C: first balanced-ask signal per slug
# We store MULTIPLE candidates per slug at different time points

# Build per-slug signal lists (gate-passing only)
slug_sigs = defaultdict(list)
for sig in signals:
    if gate_ok(sig):
        slug_sigs[sig["slug"]].append(sig)

print(f"  {len(slug_sigs)} slugs with gate-passing signals")

# ══════════════════════════════════════════════════════════════════════════════
# ORACLE: For each slug, pick first signal with best_edge >= threshold in window
# ══════════════════════════════════════════════════════════════════════════════

def oracle_grid():
    """Pre-compute oracle candidates, then grid-filter."""
    # For each slug: list of (secs, edge, side, ask, fair, spread, depth, tf, sig)
    oracle_pool = []
    for slug, sigs in slug_sigs.items():
        for sig in sigs:
            be = sig.get("best_edge", 0)
            bs = sig.get("best_side", "")
            if be < 0.05 or not bs: continue
            side = bs.upper()
            ask = sig.get(f"ask_{side.lower()}", 0)
            if ask <= 0.02 or ask >= 0.98: continue
            oracle_pool.append({
                "slug": slug, "secs": sig["secs_left"], "edge": be,
                "side": side, "ask": ask,
                "fair": sig.get(f"fair_{side.lower()}", 0.5),
                "spread": sig.get(f"spread_{side.lower()}", 1),
                "depth": sig.get(f"depth_{side.lower()}", 0),
                "tf": sig["tf"],
            })

    # Sort by timestamp (secs descending = earliest first in window)
    oracle_pool.sort(key=lambda x: -x["secs"])

    print(f"  Oracle pool: {len(oracle_pool)} candidate signals across {len(slug_sigs)} slugs")

    results = []
    cnt = 0
    for min_edge in [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40]:
        for max_secs in [180, 240, 290]:
            for min_secs in [30, 40, 60, 80, 100, 120]:
                for tfs in [[5], [15], [5,15]]:
                    for max_sp in [0.02, 0.03, 0.04]:
                        for min_dp in [200, 500, 1000]:
                            for eslip in [-0.01, 0, 0.005]:
                                cnt += 1
                                # Filter and pick first per slug
                                used = set()
                                trades = []
                                for c in oracle_pool:
                                    if c["slug"] in used: continue
                                    if c["edge"] < min_edge: continue
                                    if c["secs"] > max_secs or c["secs"] < min_secs: continue
                                    if c["tf"] not in tfs: continue
                                    if c["spread"] > max_sp or c["depth"] < min_dp: continue
                                    fill = round(min(max(c["ask"] + eslip, 0.01), 0.99), 3)
                                    if fill <= 0.01 or fill >= 0.99: continue
                                    used.add(c["slug"])
                                    # Settle
                                    if c["slug"] not in slug_oc: continue
                                    oc = slug_oc[c["slug"]]
                                    win = (oc == c["side"])
                                    sh = STAKE / fill
                                    f_ = pmfee(fill) * sh + (0.015*STAKE if eslip >= 0 else 0)
                                    if win: net = (1.0 - fill) * sh - f_
                                    else: net = -fill * sh - f_
                                    trades.append(net)

                                if len(trades) < 2: continue
                                w = sum(1 for t in trades if t > 0)
                                l = len(trades) - w
                                pnl = sum(trades)
                                wr = w / len(trades) * 100
                                results.append({
                                    "trades": len(trades), "wins": w, "losses": l,
                                    "wr": wr, "pnl": round(pnl, 2),
                                    "p": f"e≥{min_edge} {min_secs}-{max_secs}s tf={tfs} sp≤{max_sp} dp≥{min_dp} slip={eslip}"
                                })
    return results, cnt

# ══════════════════════════════════════════════════════════════════════════════
# S3C: Buy both sides, dump loser at T-30
# ══════════════════════════════════════════════════════════════════════════════

def s3c_grid():
    # Pre-compute candidates: first balanced signal per slug
    s3c_pool = []
    for slug, sigs in slug_sigs.items():
        for sig in sigs:
            ay = sig.get("ask_yes", 0); an = sig.get("ask_no", 0)
            if ay <= 0.01 or an <= 0.01: continue
            if ay >= 0.99 or an >= 0.99: continue
            s3c_pool.append({
                "slug": slug, "secs": sig["secs_left"],
                "ask_yes": ay, "ask_no": an,
                "spread_yes": sig.get("spread_yes", 1), "spread_no": sig.get("spread_no", 1),
                "depth_yes": sig.get("depth_yes", 0), "depth_no": sig.get("depth_no", 0),
                "tf": sig["tf"],
            })
    s3c_pool.sort(key=lambda x: -x["secs"])
    print(f"  S3C pool: {len(s3c_pool)} candidates")

    results = []
    cnt = 0
    for es in [120, 180, 240, 290]:
        for ee in [30, 40, 60, 80]:
            if ee >= es: continue
            for al in [0.35, 0.38, 0.40, 0.44, 0.47]:
                for ah in [0.53, 0.56, 0.60, 0.62, 0.65]:
                    for dp_price in [0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50]:
                        for sl in [0.003, 0.005, 0.008, 0.01, 0.015]:
                            for tfs in [[5], [5,15]]:
                                for eslip in [-0.01, 0, 0.005]:
                                    for max_sp in [0.02, 0.03]:
                                        for min_dp in [300, 500]:
                                            cnt += 1
                                            used = set(); trades = []
                                            for c in s3c_pool:
                                                if c["slug"] in used: continue
                                                if c["secs"] > es or c["secs"] < ee: continue
                                                if c["tf"] not in tfs: continue
                                                if c["ask_yes"] < al or c["ask_yes"] > ah: continue
                                                if c["ask_no"] < al or c["ask_no"] > ah: continue
                                                if c["spread_yes"] > max_sp or c["spread_no"] > max_sp: continue
                                                if c["depth_yes"] < min_dp or c["depth_no"] < min_dp: continue

                                                fy = round(min(max(c["ask_yes"]+eslip,0.01),0.99),3)
                                                fn = round(min(max(c["ask_no"]+eslip,0.01),0.99),3)
                                                if fy>=1 or fn>=1: continue
                                                used.add(c["slug"])

                                                if c["slug"] not in slug_oc: continue
                                                up_win = slug_oc[c["slug"]] == "YES"
                                                su = STAKE/fy; sd = STAKE/fn
                                                ef = pmfee(fy)*su + pmfee(fn)*sd
                                                if eslip >= 0: ef += 0.015*STAKE*2
                                                cost = STAKE*2 + ef

                                                dsig = t30.get(c["slug"])
                                                if dsig:
                                                    lb = dsig.get("bid_no" if up_win else "bid_yes", 0)
                                                    dp = max(min(lb, dp_price) - sl, 0.01)
                                                    lsh = sd if up_win else su
                                                    wsh = su if up_win else sd
                                                    net = lsh*dp + wsh*1.0 - cost - pmfee(dp)*lsh
                                                else:
                                                    up = 1.0 if up_win else 0.0
                                                    net = su*up + sd*(1-up) - cost
                                                trades.append(net)

                                            if len(trades) < 2: continue
                                            w = sum(1 for t in trades if t > 0)
                                            pnl = sum(trades)
                                            results.append({
                                                "trades": len(trades), "wins": w, "losses": len(trades)-w,
                                                "wr": w/len(trades)*100, "pnl": round(pnl,2),
                                                "p": f"{es}-{ee}s ask={al}-{ah} dump={dp_price} sl={sl} tf={tfs} sp≤{max_sp} dp≥{min_dp} entry={eslip}"
                                            })
    return results, cnt

# ══════════════════════════════════════════════════════════════════════════════
# SNIPER: Delta momentum — pre-compute candidates with pct_move
# ══════════════════════════════════════════════════════════════════════════════

def sniper_grid():
    # Pre-compute: first strong-delta signal per slug (pick earliest in each window)
    sniper_pool = []
    for slug, sigs in slug_sigs.items():
        for sig in sigs:
            pct = sig.get("pct_move", 0)
            if abs(pct) < 0.01: continue
            direction = "UP" if pct > 0 else "DOWN"
            side_str = "YES" if direction == "UP" else "NO"
            ask = sig.get(f"ask_{side_str.lower()}", 0)
            if ask <= 0.01 or ask >= 0.99: continue
            sniper_pool.append({
                "slug": slug, "secs": sig["secs_left"], "pct": abs(pct),
                "direction": direction, "side": side_str, "ask": ask,
                "spread": sig.get(f"spread_{side_str.lower()}", 1),
                "depth": sig.get(f"depth_{side_str.lower()}", 0),
                "tf": sig["tf"], "asset": sig["asset"],
                "bn_mom": sig.get("bn_momentum_5s", 0) * 100,
                "cl_mom": sig.get("cl_momentum_5s", 0) * 100,
                "ts": sig["ts"],
            })
    sniper_pool.sort(key=lambda x: x["ts"])  # chronological for continuity
    print(f"  Sniper pool: {len(sniper_pool)} candidates")

    results = []
    cnt = 0
    for delta in [0.02, 0.025, 0.03, 0.035, 0.04, 0.05, 0.06, 0.07]:
        for es in [75, 90, 120, 150, 180, 240]:
            for ee in [15, 20, 30, 40, 60]:
                if ee >= es: continue
                for mb in [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85]:
                    for xb in [0.85, 0.88, 0.90, 0.92, 0.95]:
                        for tfs in [[5], [5,15]]:
                            for cont in [0, 2, 3, 4]:
                                for eslip in [-0.01, 0, 0.005]:
                                    cnt += 1
                                    used = set(); trades = []
                                    cont_counts = defaultdict(int)
                                    for c in sniper_pool:
                                        if c["slug"] in used: continue
                                        if c["secs"] > es or c["secs"] < ee: continue
                                        if c["tf"] not in tfs: continue
                                        asset = c["asset"]
                                        min_d = MD.get(asset, 0.05)
                                        if c["pct"] < min_d: continue
                                        scaled = delta * (STDEV.get(asset, SB) / SB)
                                        if c["pct"] < scaled: continue
                                        # Momentum filters
                                        if c["direction"] == "UP" and c["bn_mom"] < -0.02: continue
                                        if c["direction"] == "DOWN" and c["bn_mom"] > 0.02: continue
                                        if c["direction"] == "UP" and c["cl_mom"] < -0.03: continue
                                        if c["direction"] == "DOWN" and c["cl_mom"] > 0.03: continue
                                        # Continuity
                                        if cont > 0:
                                            cont_counts[c["slug"]] += 1
                                            if cont_counts[c["slug"]] < cont: continue
                                        if c["ask"] < mb or c["ask"] > xb: continue
                                        if c["spread"] > 0.03 or c["depth"] < 500: continue

                                        fill = round(min(max(c["ask"]+eslip,0.01),0.99),3)
                                        if fill < mb or fill > xb or fill >= 1 or fill <= 0: continue
                                        used.add(c["slug"])

                                        if c["slug"] not in slug_oc: continue
                                        oc = slug_oc[c["slug"]]
                                        ov = 1.0 if oc == "YES" else 0.0
                                        ep = ov if c["direction"] == "UP" else 1.0 - ov
                                        sh = STAKE / fill
                                        f_ = pmfee(fill)*sh + (0.015*STAKE if eslip >= 0 else 0)
                                        net = (ep - fill) * sh - f_
                                        trades.append(net)

                                    if len(trades) < 2: continue
                                    w = sum(1 for t in trades if t > 0)
                                    pnl = sum(trades)
                                    results.append({
                                        "trades": len(trades), "wins": w, "losses": len(trades)-w,
                                        "wr": w/len(trades)*100, "pnl": round(pnl,2),
                                        "p": f"d={delta} {es}-{ee}s bk={mb}-{xb} tf={tfs} c={cont} slip={eslip}"
                                    })
    return results, cnt

# ══════════════════════════════════════════════════════════════════════════════
# GAP BOT: Fair value divergence
# ══════════════════════════════════════════════════════════════════════════════

def gap_grid():
    gap_pool = []
    for slug, sigs in slug_sigs.items():
        for sig in sigs:
            fy = sig.get("fair_yes", 0.5)
            if abs(fy - 0.5) < 0.02: continue
            if fy > 0.5:
                d = "YES"; fair = fy; ask = sig.get("ask_yes",0); bid = sig.get("bid_yes",0)
            else:
                d = "NO"; fair = sig.get("fair_no",0.5); ask = sig.get("ask_no",0); bid = sig.get("bid_no",0)
            if ask <= 0.02 or ask >= 0.98 or bid <= 0: continue
            gap_pool.append({
                "slug": slug, "secs": sig["secs_left"], "dir": d,
                "fair": fair, "ask": ask, "bid": bid, "edge": fair - ask,
                "spread": sig.get(f"spread_{d.lower()}", 1),
                "depth": sig.get(f"depth_{d.lower()}", 0),
                "tf": sig["tf"],
            })
    gap_pool.sort(key=lambda x: -x["secs"])
    print(f"  Gap pool: {len(gap_pool)} candidates")

    results = []
    cnt = 0
    for min_edge in [0.05, 0.08, 0.10, 0.15, 0.20, 0.25, 0.30]:
        for max_secs in [180, 240, 300]:
            for min_secs in [30, 45, 60, 90]:
                for min_fd in [0.03, 0.05, 0.10, 0.15, 0.20]:
                    for max_sf in [0.03, 0.04, 0.06, 0.10]:
                        for tfs in [[5], [15], [5,15]]:
                            for mo in [3, 6, 10]:
                                for eslip in [-0.01, 0, 0.005]:
                                    for min_dp in [200, 500]:
                                        cnt += 1
                                        used = set(); trades = []
                                        for c in gap_pool:
                                            if c["slug"] in used: continue
                                            if len(trades) >= mo: break  # max open approx
                                            if c["secs"] > max_secs or c["secs"] < min_secs: continue
                                            if c["tf"] not in tfs: continue
                                            if abs(c["fair"] - 0.5) < min_fd: continue
                                            if c["ask"] - c["bid"] > max_sf: continue
                                            if c["edge"] < min_edge: continue
                                            if c["spread"] > max_sf or c["depth"] < min_dp: continue

                                            fill = round(min(max(c["ask"]+eslip,0.01),0.99),3)
                                            if fill <= 0.01 or fill >= 0.99: continue
                                            used.add(c["slug"])

                                            if c["slug"] not in slug_oc: continue
                                            oc = slug_oc[c["slug"]]
                                            win = (oc == c["dir"])
                                            sh = STAKE / fill
                                            f_ = pmfee(fill)*sh + (0.02*STAKE if eslip >= 0 else 0)
                                            if win: net = (1.0 - fill)*STAKE - f_
                                            else: net = -fill*STAKE - f_
                                            trades.append(net)

                                        if len(trades) < 2: continue
                                        w = sum(1 for t in trades if t > 0)
                                        pnl = sum(trades)
                                        results.append({
                                            "trades": len(trades), "wins": w, "losses": len(trades)-w,
                                            "wr": w/len(trades)*100, "pnl": round(pnl,2),
                                            "p": f"e≥{min_edge} {min_secs}-{max_secs}s fd≥{min_fd} sf≤{max_sf} tf={tfs} o≤{mo} slip={eslip} dp≥{min_dp}"
                                        })
    return results, cnt


# ══════════════════════════════════════════════════════════════════════════════
# RUN ALL GRIDS
# ══════════════════════════════════════════════════════════════════════════════

def show(name, results, min_wr=80, min_tr=3, top=25):
    good = [r for r in results if r["wr"] >= min_wr and r["trades"] >= min_tr]
    good.sort(key=lambda x: (-x["pnl"], -x["wr"]))
    alll = [r for r in results if r["trades"] >= min_tr]
    alll.sort(key=lambda x: (-x["wr"], -x["pnl"]))

    print(f"\n{'='*120}")
    print(f"  {name}: {len(results)} tested | {len(alll)} ≥{min_tr}tr | {len(good)} ≥{min_wr}%WR")
    print(f"{'='*120}")

    if good:
        print(f"  {'#':>3} {'Tr':>4} {'W':>3} {'L':>3} {'WR':>6} {'PnL':>8} {'DD':>6} {'$/tr':>6}  Params")
        print(f"  {'-'*110}")
        for i, r in enumerate(good[:top]):
            avg = r["pnl"]/r["trades"]
            print(f"  {i+1:>3} {r['trades']:>4} {r['wins']:>3} {r['losses']:>3} "
                  f"{r['wr']:>5.1f}% ${r['pnl']:>+7.2f}         ${avg:>+5.2f}  {r['p']}")
    else:
        print(f"  No {min_wr}%WR. Best by WR:")
        for r in alll[:10]:
            avg = r["pnl"]/r["trades"] if r["trades"] else 0
            print(f"    WR={r['wr']:.1f}% Tr={r['trades']} PnL=${r['pnl']:+.2f} $/tr=${avg:+.2f} | {r['p']}")
        g70 = [r for r in results if r["wr"] >= 70 and r["trades"] >= 5]
        g70.sort(key=lambda x: -x["pnl"])
        if g70:
            print(f"\n  Relaxed ≥70%WR + ≥5tr ({len(g70)}):")
            for r in g70[:5]:
                avg = r["pnl"]/r["trades"]
                print(f"    WR={r['wr']:.1f}% Tr={r['trades']} PnL=${r['pnl']:+.2f} $/tr=${avg:+.2f} | {r['p']}")
    return good, alll


print("\n[1/4] ORACLE...", flush=True)
orc_res, orc_cnt = oracle_grid()
print(f"  {orc_cnt} configs tested")
orc_g, orc_a = show("ORACLE SCANNER", orc_res)

print("\n[2/4] S3C...", flush=True)
s3c_res, s3c_cnt = s3c_grid()
print(f"  {s3c_cnt} configs tested")
s3c_g, s3c_a = show("S3C (Both-Sides Dump)", s3c_res)

print("\n[3/4] SNIPER...", flush=True)
sni_res, sni_cnt = sniper_grid()
print(f"  {sni_cnt} configs tested")
sni_g, sni_a = show("SNIPER (Delta Momentum)", sni_res)

print("\n[4/4] GAP BOT...", flush=True)
gap_res, gap_cnt = gap_grid()
print(f"  {gap_cnt} configs tested")
gap_g, gap_a = show("GAP BOT (Fair Value)", gap_res)


# ══════════════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

print(f"\n{'='*120}")
print("  FINAL PRODUCTION-READY CONFIGS")
print(f"{'='*120}")

for name, g, a in [("ORACLE", orc_g, orc_a), ("S3C", s3c_g, s3c_a),
                    ("SNIPER", sni_g, sni_a), ("GAP", gap_g, gap_a)]:
    pool = g if g else a[:3]
    if not pool:
        print(f"\n  [{name}] NO VIABLE CONFIG"); continue
    b = pool[0]
    avg = b["pnl"]/b["trades"] if b["trades"] else 0
    st = "PASS 80%+" if b["wr"]>=80 else ("NEAR 70%+" if b["wr"]>=70 else "BELOW 70%")
    print(f"\n  [{name}] {st}")
    print(f"    WR={b['wr']:.1f}%  Trades={b['trades']}  W={b['wins']}  L={b['losses']}  PnL=${b['pnl']:+.2f}  $/trade=${avg:+.2f}")
    print(f"    Config: {b['p']}")
    if "slip=-0.01" in b["p"]:
        print(f"    Fill mode: MAKER (ask-1c). Live ~35% fill rate → ~{max(1,int(b['trades']*0.35))} fills, ~${b['pnl']*0.35:+.2f}")
    elif "slip=0.005" in b["p"]:
        print(f"    Fill mode: TAKER+SLIP (+0.5c). Live ~92% fill → ~{max(1,int(b['trades']*0.92))} fills, ~${b['pnl']*0.92:+.2f}")
    else:
        print(f"    Fill mode: TAKER (at ask). Live ~92% fill → ~{max(1,int(b['trades']*0.92))} fills, ~${b['pnl']*0.92:+.2f}")

    # Show also a higher-trade-count option if available
    high_vol = [r for r in (g if g else a) if r["trades"] >= 10]
    high_vol.sort(key=lambda x: -x["pnl"])
    if high_vol and high_vol[0] != b:
        h = high_vol[0]
        havg = h["pnl"]/h["trades"]
        print(f"    Alt (more volume): WR={h['wr']:.1f}% Tr={h['trades']} PnL=${h['pnl']:+.2f} $/tr=${havg:+.2f}")
        print(f"      {h['p']}")


# ═══ PRODUCTION CHECKLIST ════════════════════════════════════════════════════
print(f"\n{'='*120}")
print("  PRODUCTION REALISM CHECKLIST")
print(f"{'='*120}")
print(f"""
  ENFORCED IN THIS TEST:
    [x] Data quality = 'full' only
    [x] No stale feeds (cl_stale, bn_stale, book_stale)
    [x] Book age ≤ 3000ms
    [x] Spread filter (per-side, configurable ≤2-4c)
    [x] Depth filter (per-side, configurable ≥200-1000)
    [x] PM fees: price*(1-price)*6.25% on entry
    [x] Taker fee: 1.5-2.0% when entry slip ≥ 0
    [x] Maker: no taker fee, fill at ask-1c
    [x] Taker+slip: fill at ask+0.5c with 1.5% fee
    [x] One entry per slug (no duplicate positions)
    [x] S3C: actual T-30 bid for dump, capped at dump_price - slippage
    [x] Sniper: BN+CL momentum filters, per-asset delta scaling

  NOT MODELED (expected live gaps):
    [ ] Maker fill rate (~35% base) — reduces volume
    [ ] Execution latency (50-200ms WS→order)
    [ ] Partial fills
    [ ] PM API rate limits
    [ ] Bot competition on same signals
    [ ] Mid-life exits (SL/TP) — settle-only in this test
    [ ] Position sizing interaction between concurrent positions

  DATA STATS:
    Total signals: {len(signals):,}
    Gate-passing: {len(good_sigs):,} ({len(good_sigs)/len(signals)*100:.1f}%)
    Settlements: {len(settlements)}
    Slugs with data: {len(slug_sigs)}
""")
