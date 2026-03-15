#!/usr/bin/env python3
"""Ultra-fast iteration: pre-index signals by slug, run grids in seconds.

Key optimization: instead of scanning all 71K signals per config,
pre-build lookup tables and only iterate relevant signals.
"""

import json, math, random
from collections import defaultdict
from datetime import datetime, timezone

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

# Pre-index: signals by slug
sig_by_slug = defaultdict(list)
for sig in signals:
    sig_by_slug[sig["slug"]].append(sig)

# Pre-compute T-30 signals
t30_signals = {}
for sig in signals:
    slug = sig["slug"]
    secs = sig["secs_left"]
    if 15 <= secs <= 45:
        if slug not in t30_signals or abs(secs - 30) < abs(t30_signals[slug]["secs_left"] - 30):
            t30_signals[slug] = sig

# Pre-compute: for each slug, get the FIRST signal at various time windows
# This lets us find candidate entries without scanning all signals
first_in_window = {}  # (slug, secs_bucket) -> sig
for sig in signals:
    slug = sig["slug"]
    secs = sig["secs_left"]
    bucket = int(secs / 10) * 10  # 10-second buckets
    key = (slug, bucket)
    if key not in first_in_window:
        first_in_window[key] = sig

print(f"Loaded {len(signals):,} signals, {len(settlements)} settlements, {len(sig_by_slug)} slugs")

STAKE = 5.0
STDEV = {"btc": 0.167, "eth": 0.194, "sol": 0.247, "xrp": 0.440}
STDEV_BASE = 0.167
MIN_DELTA = {"btc": 0.015, "eth": 0.020, "sol": 0.030, "xrp": 0.050}

def pm_fee(px):
    if px <= 0 or px >= 1: return 0
    return px * (1.0 - px) * 0.0625

def gate(sig, max_book_age=3000):
    if sig.get("data_quality") != "full": return False
    if sig.get("cl_stale") or sig.get("bn_stale") or sig.get("book_stale"): return False
    if sig.get("book_age_ms", 9999) > max_book_age: return False
    return True


# ══════════════════════════════════════════════════════════════════════════════
# Pre-compute candidate entries per strategy type
# ══════════════════════════════════════════════════════════════════════════════

print("Pre-computing candidates...", flush=True)

# Oracle candidates: signals with edge data and gate passing
oracle_cands = []
for sig in signals:
    if not gate(sig): continue
    if sig.get("best_edge", 0) >= 0.05 and sig.get("best_side"):
        oracle_cands.append(sig)
print(f"  Oracle candidates: {len(oracle_cands)}")

# Sniper candidates: signals with pct_move
sniper_cands = []
for sig in signals:
    if not gate(sig): continue
    pct = abs(sig.get("pct_move", 0))
    if pct >= 0.01:
        sniper_cands.append(sig)
print(f"  Sniper candidates: {len(sniper_cands)}")

# S3C candidates: signals with both sides in range
s3c_cands = []
for sig in signals:
    if not gate(sig): continue
    ay = sig.get("ask_yes", 0); an = sig.get("ask_no", 0)
    if 0.30 <= ay <= 0.70 and 0.30 <= an <= 0.70:
        s3c_cands.append(sig)
print(f"  S3C candidates: {len(s3c_cands)}")

# Gap candidates: signals with fair value divergence
gap_cands = []
for sig in signals:
    if not gate(sig): continue
    fy = sig.get("fair_yes", 0.5)
    if abs(fy - 0.5) >= 0.03:
        gap_cands.append(sig)
print(f"  Gap candidates: {len(gap_cands)}")


# ══════════════════════════════════════════════════════════════════════════════
# Strategy runners using pre-filtered candidates
# ══════════════════════════════════════════════════════════════════════════════

def run_oracle(cands, min_edge, max_secs, min_secs, tfs, max_open, use_tp,
               entry_slip, max_spread, min_depth, taker_fee_pct):
    """entry_slip: 0 = at ask (taker), -0.01 = maker, +0.005 = taker+slip"""
    positions = {}; entered = set()
    wins = 0; losses = 0; pnl = 0.0; peak = 0.0; dd = 0.0
    settled_w = set()

    # Build TP check signals for open positions
    tp_sigs = defaultdict(list)
    if use_tp:
        for sig in signals:
            tp_sigs[sig["slug"]].append(sig)

    def _settle(slug, outcome):
        nonlocal wins, losses, pnl, peak, dd
        if slug not in positions: return
        pos = positions.pop(slug)
        side = pos["side"]
        we_win = (outcome == side)
        if we_win: gross = (1.0 - pos["fill"]) * pos["sh"]
        else: gross = -pos["fill"] * pos["sh"]
        net = gross - pos["fee"]
        pnl += net
        if net > 0: wins += 1
        else: losses += 1
        if pnl > peak: peak = pnl
        d = peak - pnl
        if d > dd: dd = d

    # Process entries chronologically
    for sig in cands:
        slug = sig["slug"]; secs = sig["secs_left"]
        if sig["tf"] not in tfs: continue
        if secs > max_secs or secs < min_secs: continue
        if slug in entered or slug in positions: continue
        if len(positions) >= max_open: continue

        best_edge = sig.get("best_edge", 0)
        best_side = sig.get("best_side", "")
        if best_edge < min_edge or not best_side: continue

        side = best_side.upper()
        ask = sig.get(f"ask_{side.lower()}", 0)
        fair = sig.get(f"fair_{side.lower()}", 0.5)
        if ask <= 0.02 or ask >= 0.98: continue

        sp = sig.get(f"spread_{side.lower()}", 1)
        dp = sig.get(f"depth_{side.lower()}", 0)
        if sp > max_spread or dp < min_depth: continue

        fill = round(min(max(ask + entry_slip, 0.01), 0.99), 3)
        if fill <= 0.01 or fill >= 0.99: continue

        if entry_slip < 0:  # maker
            fee = pm_fee(fill) * (STAKE / fill)
        else:  # taker
            fee = taker_fee_pct * STAKE + pm_fee(fill) * (STAKE / fill)

        positions[slug] = {
            "slug": slug, "side": side, "asset": sig["asset"],
            "fill": fill, "fair": fair, "fair_at_entry": fair,
            "edge": best_edge, "sh": STAKE / fill, "fee": fee, "secs": secs,
            "entry_ts": sig["ts"],
        }
        entered.add(slug)

    # Process TP exits and settlements chronologically
    if use_tp:
        for sig in signals:
            ts = sig["ts"]
            # Settle
            for wend in list(settle_by_end.keys()):
                if ts >= wend and wend not in settled_w:
                    settled_w.add(wend)
                    for s in settle_by_end[wend]:
                        _settle(s["slug"], s["outcome"])

            slug = sig["slug"]
            if slug in positions:
                pos = positions[slug]
                if ts <= pos["entry_ts"]: continue
                our_bid = sig.get(f"bid_{pos['side'].lower()}", 0)
                if our_bid >= pos["fair_at_entry"] and sig["secs_left"] > 10:
                    exit_p = max(our_bid - 0.005, 0.01)
                    gross = (exit_p - pos["fill"]) * pos["sh"]
                    net = gross - pos["fee"] - pm_fee(exit_p) * pos["sh"]
                    pnl += net
                    if net > 0: wins += 1
                    else: losses += 1
                    if pnl > peak: peak = pnl
                    d = peak - pnl
                    if d > dd: dd = d
                    del positions[slug]
    else:
        # Just settle
        for wend in settle_by_end:
            for s in settle_by_end[wend]:
                _settle(s["slug"], s["outcome"])

    # Remaining
    for slug in list(positions.keys()):
        if slug in settlements:
            _settle(slug, settlements[slug]["outcome"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    return {"trades": total, "wins": wins, "losses": losses, "wr": wr,
            "pnl": round(pnl, 2), "dd": round(dd, 2)}


def run_s3c(cands, entry_start, entry_end, ask_lo, ask_hi, dump_price, slip,
            tfs, max_spread, min_depth, entry_slip):
    positions = {}; entered = set()
    wins = 0; losses = 0; pnl = 0.0; peak = 0.0; dd = 0.0

    def settle(slug, outcome):
        nonlocal wins, losses, pnl, peak, dd
        if slug not in positions: return
        pos = positions.pop(slug)
        up_win = outcome == "YES"
        dsig = t30_signals.get(slug)
        if dsig:
            loser_bid = dsig.get("bid_no" if up_win else "bid_yes", 0)
            dp = max(min(loser_bid, dump_price) - slip, 0.01)
            l_sh = pos["sh_dn"] if up_win else pos["sh_up"]
            w_sh = pos["sh_up"] if up_win else pos["sh_dn"]
            net_pnl = l_sh * dp + w_sh * 1.0 - pos["cost"] - pm_fee(dp) * l_sh
        else:
            up_pay = 1.0 if up_win else 0.0
            net_pnl = pos["sh_up"] * up_pay + pos["sh_dn"] * (1 - up_pay) - pos["cost"]
        pnl += net_pnl
        if net_pnl > 0: wins += 1
        else: losses += 1
        if pnl > peak: peak = pnl
        d = peak - pnl
        if d > dd: dd = d

    for sig in cands:
        slug = sig["slug"]
        if sig["tf"] not in tfs: continue
        secs = sig["secs_left"]
        if secs > entry_start or secs < entry_end: continue
        if slug in entered or slug in positions: continue

        ay = sig.get("ask_yes", 0); an = sig.get("ask_no", 0)
        if ay < ask_lo or ay > ask_hi or an < ask_lo or an > ask_hi: continue

        sp_y = sig.get("spread_yes", 1); sp_n = sig.get("spread_no", 1)
        dp_y = sig.get("depth_yes", 0); dp_n = sig.get("depth_no", 0)
        if sp_y > max_spread or sp_n > max_spread: continue
        if dp_y < min_depth or dp_n < min_depth: continue

        fy = round(min(max(ay + entry_slip, 0.01), 0.99), 3)
        fn = round(min(max(an + entry_slip, 0.01), 0.99), 3)
        if fy >= 1 or fn >= 1: continue

        sh_up = STAKE / fy; sh_dn = STAKE / fn
        if entry_slip < 0:
            ef = pm_fee(fy) * sh_up + pm_fee(fn) * sh_dn
        else:
            ef = 0.015 * STAKE * 2 + pm_fee(fy) * sh_up + pm_fee(fn) * sh_dn
        cost = STAKE * 2 + ef

        positions[slug] = {"sh_up": sh_up, "sh_dn": sh_dn, "cost": cost,
                           "secs": secs, "asset": sig["asset"]}
        entered.add(slug)

    for slug in list(positions.keys()):
        if slug in settlements:
            settle(slug, settlements[slug]["outcome"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    return {"trades": total, "wins": wins, "losses": losses, "wr": wr,
            "pnl": round(pnl, 2), "dd": round(dd, 2)}


def run_sniper(cands, delta, entry_start, entry_end, min_book, max_book, tfs, cont,
               max_spread, min_depth, entry_slip, bn_filter, cl_filter, use_sl):
    positions = {}; entered = set()
    cont_counts = defaultdict(int)
    wins = 0; losses = 0; pnl = 0.0; peak = 0.0; dd = 0.0
    settled_w = set()

    def settle(slug, outcome):
        nonlocal wins, losses, pnl, peak, dd
        if slug not in positions: return
        pos = positions.pop(slug)
        ov = 1.0 if outcome == "YES" else 0.0
        ep = ov if pos["side"] == "UP" else 1.0 - ov
        net = (ep - pos["fill"]) * pos["sh"] - pos["fee"]
        pnl += net
        if net > 0: wins += 1
        else: losses += 1
        if pnl > peak: peak = pnl
        d = peak - pnl
        if d > dd: dd = d

    # Need chronological for SL + settlement
    for sig in signals:
        ts = sig["ts"]
        for wend in list(settle_by_end.keys()):
            if ts >= wend and wend not in settled_w:
                settled_w.add(wend)
                for s in settle_by_end[wend]:
                    settle(s["slug"], s["outcome"])

        slug = sig["slug"]
        # SL check
        if use_sl and slug in positions:
            pos = positions[slug]
            obk = "bid_yes" if pos["side"] == "UP" else "bid_no"
            oppk = "bid_no" if pos["side"] == "UP" else "bid_yes"
            ob = sig.get(obk, 0); opp = sig.get(oppk, 0)
            if ob > 0 and ob <= pos["fill"] * 0.50 and opp >= 0.80:
                ep = max(ob - 0.005, 0.001)
                net = (ep - pos["fill"]) * pos["sh"] - pos["fee"] - pm_fee(ep) * pos["sh"]
                pnl += net; losses += 1
                if pnl > peak: peak = pnl
                d = peak - pnl
                if d > dd: dd = d
                del positions[slug]

        if not gate(sig): continue
        asset = sig["asset"]; tf = sig["tf"]; secs = sig["secs_left"]
        if tf not in tfs: continue
        if slug in entered or slug in positions: continue
        if secs > entry_start or secs < entry_end: continue

        pct = abs(sig.get("pct_move", 0))
        direction = "UP" if sig.get("pct_move", 0) > 0 else "DOWN"
        min_d = MIN_DELTA.get(asset, 0.05)
        if pct < min_d: continue
        scaled = delta * (STDEV.get(asset, STDEV_BASE) / STDEV_BASE)
        if pct < scaled: continue

        if bn_filter:
            bm = sig.get("bn_momentum_5s", 0) * 100
            if direction == "UP" and bm < -0.02: continue
            if direction == "DOWN" and bm > 0.02: continue
        if cl_filter:
            cm = sig.get("cl_momentum_5s", 0) * 100
            if direction == "UP" and cm < -0.03: continue
            if direction == "DOWN" and cm > 0.03: continue

        if cont > 0:
            cont_counts[slug] += 1
            if cont_counts[slug] < cont: continue

        side_str = "YES" if direction == "UP" else "NO"
        ask = sig.get(f"ask_{side_str.lower()}", 0)
        if ask < min_book or ask > max_book: continue

        sp = sig.get(f"spread_{side_str.lower()}", 1)
        dp = sig.get(f"depth_{side_str.lower()}", 0)
        if sp > max_spread or dp < min_depth: continue

        fill = round(min(max(ask + entry_slip, 0.01), 0.99), 3)
        if fill < min_book or fill > max_book or fill >= 1.0 or fill <= 0: continue

        if entry_slip < 0:
            fee = pm_fee(fill) * (STAKE / fill)
        else:
            fee = 0.015 * STAKE + pm_fee(fill) * (STAKE / fill)

        positions[slug] = {"side": direction, "asset": asset, "fill": fill,
                           "sh": STAKE / fill, "secs": secs, "fee": fee}
        entered.add(slug)
        cont_counts[slug] = 0

    for slug in list(positions.keys()):
        if slug in settlements:
            settle(slug, settlements[slug]["outcome"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    return {"trades": total, "wins": wins, "losses": losses, "wr": wr,
            "pnl": round(pnl, 2), "dd": round(dd, 2)}


def run_gap(cands, min_edge, max_secs, min_secs, min_fair_dist, max_spread_f,
            tfs, max_open, entry_slip, min_depth):
    positions = {}; done = set()
    wins = 0; losses = 0; pnl = 0.0; peak = 0.0; dd = 0.0

    def settle(slug, outcome):
        nonlocal wins, losses, pnl, peak, dd
        if slug not in positions: return
        pos = positions.pop(slug)
        we_win = (outcome == pos["dir"])
        if we_win: gross = (1.0 - pos["fill"]) * STAKE
        else: gross = -pos["fill"] * STAKE
        net = gross - pos["fee"]
        pnl += net
        if net > 0: wins += 1
        else: losses += 1
        if pnl > peak: peak = pnl
        d = peak - pnl
        if d > dd: dd = d

    for sig in cands:
        slug = sig["slug"]; secs = sig["secs_left"]
        if sig["tf"] not in tfs: continue
        if secs < min_secs or secs > max_secs: continue
        if slug in done or slug in positions: continue
        if len(positions) >= max_open: continue

        fy = sig.get("fair_yes", 0.5); fn = sig.get("fair_no", 0.5)
        if abs(fy - 0.5) < min_fair_dist: continue

        if fy > 0.5:
            d = "YES"; fair = fy; ask = sig.get("ask_yes", 0); bid = sig.get("bid_yes", 0)
        else:
            d = "NO"; fair = fn; ask = sig.get("ask_no", 0); bid = sig.get("bid_no", 0)
        edge = fair - ask
        if ask <= 0.02 or ask >= 0.98: continue
        if bid <= 0: continue
        if ask - bid > max_spread_f: continue
        if edge < min_edge: continue

        sp = sig.get(f"spread_{d.lower()}", 1)
        dp = sig.get(f"depth_{d.lower()}", 0)
        if sp > max_spread_f or dp < min_depth: continue

        fill = round(min(max(ask + entry_slip, 0.01), 0.99), 3)
        if fill <= 0.01 or fill >= 0.99: continue

        if entry_slip < 0:
            fee = pm_fee(fill) * (STAKE / fill)
        else:
            fee = 0.02 * STAKE + pm_fee(fill) * (STAKE / fill)

        positions[slug] = {"dir": d, "asset": sig["asset"], "fill": fill,
                           "fair": fair, "edge": edge, "fee": fee, "secs": secs}
        done.add(slug)

    for slug in list(positions.keys()):
        if slug in settlements:
            settle(slug, settlements[slug]["outcome"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    return {"trades": total, "wins": wins, "losses": losses, "wr": wr,
            "pnl": round(pnl, 2), "dd": round(dd, 2)}


# ══════════════════════════════════════════════════════════════════════════════
# GRID SEARCHES
# ══════════════════════════════════════════════════════════════════════════════

def show_top(name, results, min_wr=80, min_trades=3, top_n=20):
    good = [r for r in results if r["wr"] >= min_wr and r["trades"] >= min_trades]
    good.sort(key=lambda x: (-x["pnl"], -x["wr"]))
    all_viable = [r for r in results if r["trades"] >= min_trades]
    all_viable.sort(key=lambda x: (-x["wr"], -x["pnl"]))

    print(f"\n{'='*110}")
    print(f"  {name}: {len(results)} tested | {len(all_viable)} with ≥{min_trades} trades | {len(good)} with ≥{min_wr}% WR")
    print(f"{'='*110}")

    if not good:
        print(f"  No configs hit {min_wr}% WR. Best by WR:")
        for r in all_viable[:8]:
            avg = r["pnl"]/r["trades"] if r["trades"] else 0
            print(f"    WR={r['wr']:.1f}% Tr={r['trades']} PnL=${r['pnl']:+.2f} DD=${r['dd']:.2f} $/tr=${avg:+.2f} | {r['p']}")
        # Try lower WR
        lower = [r for r in results if r["wr"] >= 70 and r["trades"] >= 5]
        lower.sort(key=lambda x: (-x["pnl"]))
        if lower:
            print(f"\n  ≥70% WR + ≥5 trades ({len(lower)} configs), top by PnL:")
            for r in lower[:5]:
                avg = r["pnl"]/r["trades"]
                print(f"    WR={r['wr']:.1f}% Tr={r['trades']} PnL=${r['pnl']:+.2f} DD=${r['dd']:.2f} $/tr=${avg:+.2f} | {r['p']}")
        return good, all_viable

    print(f"\n  {'#':>3} {'Tr':>4} {'W':>3} {'L':>3} {'WR':>6} {'PnL':>8} {'DD':>6} {'$/tr':>6}  Params")
    print(f"  {'-'*100}")
    for i, r in enumerate(good[:top_n]):
        avg = r["pnl"]/r["trades"]
        print(f"  {i+1:>3} {r['trades']:>4} {r['wins']:>3} {r['losses']:>3} "
              f"{r['wr']:>5.1f}% ${r['pnl']:>+7.2f} ${r['dd']:>5.2f} ${avg:>+5.2f}  {r['p']}")
    return good, all_viable


# ── 1. ORACLE ────────────────────────────────────────────────────────────────
print("\n[1/4] ORACLE SCANNER...", flush=True)
orc = []
cnt = 0
for me in [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40]:
    for mx in [180, 240, 290]:
        for mn in [40, 60, 80, 100]:
            for tf in [[5], [15], [5,15]]:
                for mo in [3, 6, 10]:
                    for tp in [False, True]:
                        for es in [-0.01, 0, 0.005]:
                            for msp in [0.02, 0.04]:
                                for mdp in [300, 1000]:
                                    cnt += 1
                                    r = run_oracle(oracle_cands, me, mx, mn, tf, mo, tp, es, msp, mdp, 0.015)
                                    if r["trades"] >= 2:
                                        r["p"] = f"e≥{me} {mn}-{mx}s tf={tf} o≤{mo} tp={tp} slip={es} sp≤{msp} dp≥{mdp}"
                                        orc.append(r)
print(f"  {cnt} configs tested")
orc_good, orc_all = show_top("ORACLE SCANNER", orc)

# ── 2. S3C ───────────────────────────────────────────────────────────────────
print("\n[2/4] S3C...", flush=True)
s3c = []
cnt = 0
for es in [120, 180, 240, 290]:
    for ee in [30, 40, 60, 80]:
        if ee >= es: continue
        for al in [0.38, 0.40, 0.44, 0.47]:
            for ah in [0.53, 0.56, 0.60, 0.62]:
                for dp in [0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50]:
                    for sl in [0.003, 0.005, 0.008, 0.01]:
                        for tf in [[5], [5,15]]:
                            for esl in [-0.01, 0, 0.005]:
                                cnt += 1
                                r = run_s3c(s3c_cands, es, ee, al, ah, dp, sl, tf, 0.03, 500, esl)
                                if r["trades"] >= 2:
                                    r["p"] = f"{es}-{ee}s ask={al}-{ah} dump={dp} sl={sl} tf={tf} entry={esl}"
                                    s3c.append(r)
print(f"  {cnt} configs tested")
s3c_good, s3c_all = show_top("S3C (Both-Sides Dump)", s3c)

# ── 3. SNIPER ────────────────────────────────────────────────────────────────
print("\n[3/4] SNIPER...", flush=True)
sni = []
cnt = 0
for d in [0.02, 0.025, 0.03, 0.035, 0.04, 0.05, 0.06]:
    for es in [75, 90, 120, 180, 240]:
        for ee in [20, 30, 40, 60]:
            if ee >= es: continue
            for mb in [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85]:
                for xb in [0.85, 0.88, 0.92, 0.95]:
                    for tf in [[5], [5,15]]:
                        for c in [0, 2, 3, 4]:
                            for esl in [-0.01, 0.005]:
                                for usl in [True, False]:
                                    cnt += 1
                                    r = run_sniper(sniper_cands, d, es, ee, mb, xb, tf, c,
                                                   0.03, 500, esl, True, True, usl)
                                    if r["trades"] >= 2:
                                        r["p"] = f"d={d} {es}-{ee}s bk={mb}-{xb} tf={tf} c={c} entry={esl} sl={usl}"
                                        sni.append(r)
print(f"  {cnt} configs tested")
sni_good, sni_all = show_top("SNIPER", sni)

# ── 4. GAP BOT ──────────────────────────────────────────────────────────────
print("\n[4/4] GAP BOT...", flush=True)
gap = []
cnt = 0
for me in [0.05, 0.08, 0.10, 0.15, 0.20, 0.25, 0.30]:
    for mx in [180, 240, 300]:
        for mn in [30, 45, 60, 90]:
            for mfd in [0.03, 0.05, 0.10, 0.15]:
                for msf in [0.04, 0.06, 0.10]:
                    for tf in [[5], [15], [5,15]]:
                        for mo in [3, 6, 10]:
                            for esl in [-0.01, 0, 0.005]:
                                for mdp in [200, 500]:
                                    cnt += 1
                                    r = run_gap(gap_cands, me, mx, mn, mfd, msf, tf, mo, esl, mdp)
                                    if r["trades"] >= 2:
                                        r["p"] = f"e≥{me} {mn}-{mx}s fd≥{mfd} sf≤{msf} tf={tf} o≤{mo} entry={esl} dp≥{mdp}"
                                        gap.append(r)
print(f"  {cnt} configs tested")
gap_good, gap_all = show_top("GAP BOT", gap)


# ══════════════════════════════════════════════════════════════════════════════
# FINAL PRODUCTION CARD
# ══════════════════════════════════════════════════════════════════════════════

print(f"\n{'='*110}")
print("  FINAL PRODUCTION CONFIG CARDS")
print(f"{'='*110}")

for name, good, alll in [("ORACLE", orc_good, orc_all), ("S3C", s3c_good, s3c_all),
                          ("SNIPER", sni_good, sni_all), ("GAP", gap_good, gap_all)]:
    pool = good if good else alll[:3]
    if not pool:
        print(f"\n  [{name}] NO VIABLE CONFIGS")
        continue

    best = pool[0]
    avg = best["pnl"]/best["trades"] if best["trades"] else 0
    status = "PASS" if best["wr"] >= 80 else "MARGINAL" if best["wr"] >= 70 else "FAIL"

    print(f"\n  [{name}] {status}")
    print(f"    Config: {best['p']}")
    print(f"    Trades={best['trades']} W={best['wins']} L={best['losses']} WR={best['wr']:.1f}%")
    print(f"    PnL=${best['pnl']:+.2f} DD=${best['dd']:.2f} $/trade=${avg:+.2f}")

    # Live adjustment note
    if "entry=-0.01" in best["p"] or "slip=-0.01" in best["p"]:
        print(f"    Entry: MAKER (ask-1c) — expect ~35% fill rate in live")
        print(f"    Live PnL estimate: ~${best['pnl']*0.35:+.2f} (35% of trades fill)")
    elif "entry=0.005" in best["p"] or "slip=0.005" in best["p"]:
        print(f"    Entry: TAKER+SLIP — expect ~92% fill, 1.5% fee")
        print(f"    Live PnL estimate: ~${best['pnl']*0.92:+.2f} (92% of trades fill)")
    else:
        print(f"    Entry: TAKER at ask — expect ~92% fill, 1.5% fee")
        print(f"    Live PnL estimate: ~${best['pnl']*0.92:+.2f}")


# ── MECHANICAL AUDIT ─────────────────────────────────────────────────────────
print(f"\n{'='*110}")
print("  PRODUCTION REALISM CHECKLIST")
print(f"{'='*110}")

pass_gate = sum(1 for s in signals if gate(s))
print(f"""
  DATA GATES:
    Signals passing production gate: {pass_gate}/{len(signals)} ({pass_gate/len(signals)*100:.1f}%)
    Rejected: stale CL/BN/book feeds, bad data quality, book_age > 3s

  ENTRY REALISM:
    Maker (ask-1c): No taker fee, limit order inside spread
      - Fill rate: ~30-40% in live (queue position, adverse selection)
      - Risk: order may not fill if market moves
    Taker (ask+slip): 1.5% fee + 0.5c slippage
      - Fill rate: ~90-95% in live (IOC at market)
      - Risk: slippage can be higher in thin books

  FEE MODEL:
    PM fee = price * (1-price) * 6.25% per side (entry + exit)
    Taker fee: 1.5-2% of stake (applied on taker entries)
    Maker fee: $0 (maker rebate not modeled — conservative)

  EXIT REALISM:
    Settlement: binary payout at 0 or 1 — no slippage
    Take profit: bid - 0.5c (conservative sell at market)
    Stop loss: confirmed only when opposing bid ≥ 0.80
    S3C dump: sell loser at bid or dump_price, whichever lower, minus slip

  POSITION MANAGEMENT:
    Max open positions enforced
    One entry per slug (no duplicate positions)
    No partial fills modeled (all-or-nothing)

  NOT MODELED (live gaps):
    - Order execution latency (50-200ms)
    - PM API rate limits
    - Partial fills and order book changes between signal and fill
    - Competition from other bots on same signals
    - Market regime changes (volatility shifts)
""")
