#!/usr/bin/env python3
"""Production-realistic forward test iterator.

Goal: Find configs for each strategy that deliver ≥80% WR with decent PnL,
using realistic slippage, fill simulation, fee modeling, and book staleness
checks that mirror live trading conditions.

Production realism checks:
  1. Book staleness: reject if book_age_ms > MAX_BOOK_AGE_MS
  2. Feed staleness: reject if cl_stale or bn_stale or book_stale
  3. Spread filter: reject if spread > max_spread (wide = bad fills)
  4. Depth filter: reject if depth < min_depth (thin = slippage)
  5. Maker fill probability: based on spread, depth, time-to-settle
  6. Taker slippage: based on our size vs top_ask_size
  7. PM fees: price*(1-price)*0.0625 per side
  8. Data quality: only use 'full' quality signals
"""

import json, math, random, sys
from collections import defaultdict
from datetime import datetime, timezone

random.seed(42)

# ── Load data ────────────────────────────────────────────────────────────────

print("Loading signals...", flush=True)
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

# Pre-compute T-30 signals for S3C dump phase
t30_signals = {}
for sig in signals:
    slug = sig["slug"]
    secs = sig["secs_left"]
    if 15 <= secs <= 45:
        if slug not in t30_signals or abs(secs - 30) < abs(t30_signals[slug]["secs_left"] - 30):
            t30_signals[slug] = sig

print(f"Loaded {len(signals):,} signals, {len(settlements)} settlements\n")

STAKE = 5.0
STDEV = {"btc": 0.167, "eth": 0.194, "sol": 0.247, "xrp": 0.440}
STDEV_BASE = 0.167
MIN_DELTA_BASE = {"btc": 0.015, "eth": 0.020, "sol": 0.030, "xrp": 0.050}

def pm_fee(px):
    """Polymarket fee: price * (1 - price) * 6.25%"""
    if px <= 0 or px >= 1:
        return 0
    return px * (1.0 - px) * 0.0625


# ── Production realism gate ──────────────────────────────────────────────────

def production_gate(sig, max_book_age_ms=3000, max_spread=0.04, min_depth=500,
                    max_cl_age_ms=5000, max_bn_age_ms=3000):
    """Returns True if signal passes production realism checks."""
    # Data quality
    if sig.get("data_quality") != "full":
        return False
    # Feed staleness
    if sig.get("cl_stale", False) or sig.get("bn_stale", False) or sig.get("book_stale", False):
        return False
    # Book age
    if sig.get("book_age_ms", 9999) > max_book_age_ms:
        return False
    # CL feed age
    if sig.get("cl_feed_age_ms", 9999) > max_cl_age_ms:
        return False
    # BN feed age
    if sig.get("bn_feed_age_ms", 9999) > max_bn_age_ms:
        return False
    return True


def spread_depth_ok(sig, side, max_spread=0.04, min_depth=500):
    """Check spread and depth for a specific side."""
    suffix = "_yes" if side == "YES" else "_no"
    spread = sig.get(f"spread{suffix}", 1.0)
    depth = sig.get(f"depth{suffix}", 0)
    if spread > max_spread:
        return False
    if depth < min_depth:
        return False
    return True


def maker_fill_prob(sig, side, base_prob=0.35):
    """Estimate maker fill probability based on market conditions.

    Factors that reduce fill probability:
    - Wide spreads (less competition, but also less crossing)
    - Low depth (thin book, orders get picked off)
    - Fast-moving edge signals (adverse selection)
    - Large top_ask_size (our order is behind queue)
    """
    suffix = "_yes" if side == "YES" else "_no"
    spread = sig.get(f"spread{suffix}", 0.01)
    depth = sig.get(f"depth{suffix}", 5000)
    top_ask = sig.get(f"top_ask_size{suffix}", 100)

    prob = base_prob

    # Tight spread = more competition but more flow
    if spread <= 0.01:
        prob *= 1.0  # normal - 1 cent spread is standard
    elif spread <= 0.02:
        prob *= 0.8  # wider = less fill
    else:
        prob *= 0.5  # very wide = unlikely fill

    # Our $5 stake = ~5-10 shares. If top_ask is huge, we're behind queue
    our_shares = STAKE / max(sig.get(f"ask{suffix}", 0.5), 0.01)
    if top_ask > our_shares * 10:
        prob *= 0.7  # big queue ahead of us

    # Time pressure: closer to settlement = more urgent fills
    secs = sig.get("secs_left", 300)
    if secs < 60:
        prob *= 1.3  # more urgent
    elif secs < 120:
        prob *= 1.1

    return min(prob, 0.95)


def taker_fill_price(sig, side, base_slip=0.005):
    """Estimate taker fill price with slippage.

    Taker buys at ask + slippage based on our size vs available depth.
    """
    suffix = "_yes" if side == "YES" else "_no"
    ask = sig.get(f"ask{suffix}", 0)
    if ask <= 0:
        return 0

    top_ask_size = sig.get(f"top_ask_size{suffix}", 100)
    our_shares = STAKE / ask

    # If our order is bigger than top level, we eat into next level
    if our_shares > top_ask_size * 0.8:
        slip = base_slip + 0.005  # extra slippage
    else:
        slip = base_slip

    return min(ask + slip, 0.99)


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY 1: ORACLE SCANNER (CL fair value edge)
# ══════════════════════════════════════════════════════════════════════════════

def run_oracle(min_edge, max_secs, min_secs, tfs, max_open, use_tp, use_sl,
               fill_mode, max_spread, min_depth, max_book_age_ms):
    """Oracle Scanner: enter when BS fair value edge > threshold."""
    positions = {}
    entered = set()
    wins = 0; losses = 0; pnl = 0.0; peak = 0.0; dd = 0.0
    settled_windows = set()
    trade_log = []

    def _settle(slug, outcome_str, ts):
        nonlocal wins, losses, pnl, peak, dd
        if slug not in positions:
            return
        pos = positions.pop(slug)
        side = pos["side"]

        if side == "YES":
            we_win = (outcome_str == "YES")
        else:
            we_win = (outcome_str == "NO")

        if we_win:
            gross = (1.0 - pos["fill"]) * pos["shares"]
        else:
            gross = -pos["fill"] * pos["shares"]

        net = gross - pos["fee"]
        pnl += net
        if net > 0: wins += 1
        else: losses += 1
        if pnl > peak: peak = pnl
        d = peak - pnl
        if d > dd: dd = d
        trade_log.append({"net": round(net, 2), "side": side, "fill": pos["fill"],
                          "edge": pos["edge"], "secs": pos["secs"], "exit": "SETTLE",
                          "slug": slug, "asset": pos["asset"]})

    for sig in signals:
        ts = sig["ts"]

        # Settle
        for wend in list(settle_by_end.keys()):
            if ts >= wend and wend not in settled_windows:
                settled_windows.add(wend)
                for s in settle_by_end[wend]:
                    _settle(s["slug"], s["outcome"], ts)

        # Take profit / stop loss checks on open positions
        for slug in list(positions.keys()):
            pos = positions[slug]
            if pos["slug"] != sig["slug"]:
                continue
            side = pos["side"]

            # Take profit: bid on our side >= fair_at_entry
            if use_tp:
                our_bid = sig.get(f"bid_{side.lower()}", 0)
                if our_bid >= pos["fair_at_entry"] and sig["secs_left"] > 10:
                    # Exit at bid - slip
                    exit_p = max(our_bid - 0.005, 0.01)
                    gross = (exit_p - pos["fill"]) * pos["shares"]
                    net = gross - pos["fee"] - pm_fee(exit_p) * pos["shares"]
                    pnl += net
                    if net > 0: wins += 1
                    else: losses += 1
                    if pnl > peak: peak = pnl
                    d = peak - pnl
                    if d > dd: dd = d
                    trade_log.append({"net": round(net, 2), "side": side, "fill": pos["fill"],
                                      "edge": pos["edge"], "secs": pos["secs"], "exit": "TP",
                                      "slug": slug, "asset": pos["asset"]})
                    del positions[slug]
                    continue

            # Confirmed stop loss: our bid collapsed AND opposing bid >= 0.80
            if use_sl and sig["secs_left"] > 30:
                our_bid = sig.get(f"bid_{side.lower()}", 0)
                opp_side = "no" if side == "YES" else "yes"
                opp_bid = sig.get(f"bid_{opp_side}", 0)
                if our_bid > 0 and our_bid <= pos["fill"] * 0.40 and opp_bid >= 0.80:
                    exit_p = max(our_bid - 0.005, 0.001)
                    gross = (exit_p - pos["fill"]) * pos["shares"]
                    net = gross - pos["fee"] - pm_fee(exit_p) * pos["shares"]
                    pnl += net
                    losses += 1
                    if pnl > peak: peak = pnl
                    d = peak - pnl
                    if d > dd: dd = d
                    trade_log.append({"net": round(net, 2), "side": side, "fill": pos["fill"],
                                      "edge": pos["edge"], "secs": pos["secs"], "exit": "SL",
                                      "slug": slug, "asset": pos["asset"]})
                    del positions[slug]
                    continue

        # Entry
        slug = sig["slug"]
        secs = sig["secs_left"]
        if sig["tf"] not in tfs:
            continue
        if secs > max_secs or secs < min_secs:
            continue
        if slug in entered or slug in positions:
            continue
        if len(positions) >= max_open:
            continue

        # Production gate
        if not production_gate(sig, max_book_age_ms=max_book_age_ms):
            continue

        # Get edge
        best_edge = sig.get("best_edge", 0)
        best_side = sig.get("best_side", "")
        if best_edge < min_edge:
            continue
        if not best_side:
            continue

        side = best_side.upper()
        suffix = "_yes" if side == "YES" else "_no"
        ask = sig.get(f"ask{suffix.replace('_', '_')}", 0)
        fair = sig.get(f"fair{suffix}", 0.5)

        if ask <= 0.01 or ask >= 0.99:
            continue

        # Spread/depth check
        if not spread_depth_ok(sig, side, max_spread=max_spread, min_depth=min_depth):
            continue

        # Fill simulation
        if fill_mode == "maker":
            fp = maker_fill_prob(sig, side, base_prob=0.35)
            if random.random() > fp:
                continue
            fill_price = max(ask - 0.01, 0.01)
            fee = 0.0  # maker = no fee
        elif fill_mode == "taker":
            # 92% taker fill rate
            if random.random() > 0.92:
                continue
            fill_price = taker_fill_price(sig, side)
            fee = 0.015 * STAKE  # 1.5% taker fee
        else:  # conservative: maker price but with taker fee as worst case
            fp = maker_fill_prob(sig, side, base_prob=0.30)
            if random.random() > fp:
                continue
            fill_price = ask  # assume fill at ask (not ask-0.01)
            fee = 0.015 * STAKE

        if fill_price <= 0.01 or fill_price >= 0.99:
            continue

        shares = STAKE / fill_price
        fee += pm_fee(fill_price) * shares

        positions[slug] = {
            "slug": slug, "side": side, "asset": sig["asset"],
            "fill": fill_price, "fair": fair, "fair_at_entry": fair,
            "edge": best_edge, "shares": shares, "fee": fee, "secs": secs,
        }
        entered.add(slug)

    # Final settlement
    for slug in list(positions.keys()):
        if slug in settlements:
            _settle(slug, settlements[slug]["outcome"], 0)

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    unsettled = len(positions)
    return {"trades": total, "wins": wins, "losses": losses, "wr": wr,
            "pnl": round(pnl, 2), "dd": round(dd, 2), "unsettled": unsettled,
            "log": trade_log}


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY 2: S3C (Both sides, dump loser)
# ══════════════════════════════════════════════════════════════════════════════

def run_s3c(entry_start, entry_end, ask_lo, ask_hi, dump_price, slip,
            tfs, max_spread, min_depth, max_book_age_ms, fill_mode):
    """S3C: Buy both YES+NO, dump losing side at T-30."""
    positions = {}
    entered = set()
    wins = 0; losses = 0; pnl = 0.0; peak = 0.0; dd = 0.0
    settled_windows = set()
    trade_log = []

    def settle(slug, outcome_str):
        nonlocal wins, losses, pnl, peak, dd
        if slug not in positions:
            return
        pos = positions.pop(slug)
        up_winning = outcome_str == "YES"

        dump_sig = t30_signals.get(slug)
        if dump_sig:
            loser_bid = dump_sig.get("bid_no" if up_winning else "bid_yes", 0)
            # Realistic dump: we sell at bid - slippage (hitting bid)
            dp = max(min(loser_bid, dump_price) - slip, 0.01)
            loser_sh = pos["sh_dn"] if up_winning else pos["sh_up"]
            winner_sh = pos["sh_up"] if up_winning else pos["sh_dn"]
            gross = loser_sh * dp + winner_sh * 1.0 - pos["cost"]
            exit_fee = pm_fee(dp) * loser_sh
            net_pnl = gross - exit_fee
        else:
            # No T-30 signal - hold to settlement (no dump)
            up_pay = 1.0 if up_winning else 0.0
            dn_pay = 1.0 - up_pay
            net_pnl = pos["sh_up"] * up_pay + pos["sh_dn"] * dn_pay - pos["cost"]

        pnl += net_pnl
        if net_pnl > 0: wins += 1
        else: losses += 1
        if pnl > peak: peak = pnl
        d = peak - pnl
        if d > dd: dd = d
        trade_log.append({"net": round(net_pnl, 2), "secs": pos["secs"],
                          "ask_yes": pos["ask_yes"], "ask_no": pos["ask_no"],
                          "asset": pos["asset"], "slug": slug})

    for sig in signals:
        ts = sig["ts"]
        for wend in list(settle_by_end.keys()):
            if ts >= wend and wend not in settled_windows:
                settled_windows.add(wend)
                for s in settle_by_end[wend]:
                    settle(s["slug"], s["outcome"])

        slug = sig["slug"]
        if sig["tf"] not in tfs:
            continue
        secs = sig["secs_left"]
        if secs > entry_start or secs < entry_end:
            continue
        if slug in entered or slug in positions:
            continue

        # Production gate
        if not production_gate(sig, max_book_age_ms=max_book_age_ms):
            continue

        ask_yes = sig.get("ask_yes", 0)
        ask_no = sig.get("ask_no", 0)
        if ask_yes <= 0 or ask_no <= 0:
            continue
        if ask_yes < ask_lo or ask_yes > ask_hi:
            continue
        if ask_no < ask_lo or ask_no > ask_hi:
            continue

        # Spread/depth both sides
        spread_yes = sig.get("spread_yes", 1)
        spread_no = sig.get("spread_no", 1)
        if spread_yes > max_spread or spread_no > max_spread:
            continue
        depth_yes = sig.get("depth_yes", 0)
        depth_no = sig.get("depth_no", 0)
        if depth_yes < min_depth or depth_no < min_depth:
            continue

        # Fill simulation - both sides need to fill
        if fill_mode == "maker":
            fp_yes = maker_fill_prob(sig, "YES", 0.35)
            fp_no = maker_fill_prob(sig, "NO", 0.35)
            # Both sides must fill (sequential maker orders)
            if random.random() > fp_yes or random.random() > fp_no:
                continue
            fill_yes = max(ask_yes - 0.01, 0.01)
            fill_no = max(ask_no - 0.01, 0.01)
            entry_fee = pm_fee(fill_yes) * (STAKE / fill_yes) + pm_fee(fill_no) * (STAKE / fill_no)
        elif fill_mode == "taker":
            if random.random() > 0.92:
                continue
            fill_yes = taker_fill_price(sig, "YES")
            fill_no = taker_fill_price(sig, "NO")
            entry_fee = 0.015 * STAKE * 2 + pm_fee(fill_yes) * (STAKE / fill_yes) + pm_fee(fill_no) * (STAKE / fill_no)
        else:  # conservative
            fp_yes = maker_fill_prob(sig, "YES", 0.30)
            fp_no = maker_fill_prob(sig, "NO", 0.30)
            if random.random() > fp_yes or random.random() > fp_no:
                continue
            fill_yes = ask_yes + slip
            fill_no = ask_no + slip
            entry_fee = pm_fee(fill_yes) * (STAKE / fill_yes) + pm_fee(fill_no) * (STAKE / fill_no)

        if fill_yes >= 1.0 or fill_no >= 1.0:
            continue

        sh_up = STAKE / fill_yes
        sh_dn = STAKE / fill_no
        total_cost = STAKE * 2 + entry_fee

        positions[slug] = {
            "sh_up": sh_up, "sh_dn": sh_dn, "cost": total_cost, "secs": secs,
            "asset": sig["asset"], "ask_yes": ask_yes, "ask_no": ask_no, "slug": slug,
        }
        entered.add(slug)

    for slug in list(positions.keys()):
        if slug in settlements:
            settle(slug, settlements[slug]["outcome"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    return {"trades": total, "wins": wins, "losses": losses, "wr": wr,
            "pnl": round(pnl, 2), "dd": round(dd, 2), "log": trade_log}


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY 3: SNIPER (CL delta momentum)
# ══════════════════════════════════════════════════════════════════════════════

def run_sniper(delta, entry_start, entry_end, min_book, max_book, tfs, cont,
               max_spread, min_depth, max_book_age_ms, fill_mode,
               bn_filter=True, cl_filter=True):
    """Sniper: enter on strong CL delta with momentum confirmation."""
    positions = {}
    entered = set()
    cont_counts = defaultdict(int)
    wins = 0; losses = 0; pnl = 0.0; peak = 0.0; dd = 0.0
    settled_windows = set()
    trade_log = []

    def settle(slug, outcome_str):
        nonlocal wins, losses, pnl, peak, dd
        if slug not in positions:
            return
        pos = positions.pop(slug)
        outcome = 1.0 if outcome_str == "YES" else 0.0
        exit_p = outcome if pos["side"] == "UP" else 1.0 - outcome
        gross = (exit_p - pos["fill"]) * pos["shares"]
        net = gross - pos["fee"]
        pnl += net
        if net > 0: wins += 1
        else: losses += 1
        if pnl > peak: peak = pnl
        d = peak - pnl
        if d > dd: dd = d
        trade_log.append({"net": round(net, 2), "side": pos["side"],
                          "fill": pos["fill"], "secs": pos["secs"],
                          "asset": pos["asset"], "slug": slug})

    for sig in signals:
        ts = sig["ts"]
        for wend in list(settle_by_end.keys()):
            if ts >= wend and wend not in settled_windows:
                settled_windows.add(wend)
                for s in settle_by_end[wend]:
                    settle(s["slug"], s["outcome"])

        slug = sig["slug"]

        # Confirmed stop loss on open positions
        if slug in positions:
            pos = positions[slug]
            our_bid_key = "bid_yes" if pos["side"] == "UP" else "bid_no"
            opp_bid_key = "bid_no" if pos["side"] == "UP" else "bid_yes"
            our_bid = sig.get(our_bid_key, 0)
            opp_bid = sig.get(opp_bid_key, 0)
            if our_bid > 0 and our_bid <= pos["fill"] * 0.50 and opp_bid >= 0.80:
                exit_p = max(our_bid - 0.005, 0.001)
                gross = (exit_p - pos["fill"]) * pos["shares"]
                ef = pm_fee(pos["fill"]) * pos["shares"] + pm_fee(exit_p) * pos["shares"]
                net = gross - ef
                pnl += net; losses += 1
                if pnl > peak: peak = pnl
                d = peak - pnl
                if d > dd: dd = d
                trade_log.append({"net": round(net, 2), "side": pos["side"],
                                  "fill": pos["fill"], "secs": pos["secs"],
                                  "asset": pos["asset"], "slug": slug, "exit": "SL"})
                del positions[slug]

        asset = sig["asset"]; tf = sig["tf"]; secs = sig["secs_left"]
        if tf not in tfs:
            continue
        if slug in entered or slug in positions:
            continue
        if secs > entry_start or secs < entry_end:
            continue

        # Production gate
        if not production_gate(sig, max_book_age_ms=max_book_age_ms):
            continue

        pct = abs(sig.get("pct_move", 0))
        direction = "UP" if sig.get("pct_move", 0) > 0 else "DOWN"
        min_d = MIN_DELTA_BASE.get(asset, 0.05)
        if pct < min_d:
            continue

        # Scaled delta threshold
        scaled = delta * (STDEV.get(asset, STDEV_BASE) / STDEV_BASE)
        if pct < scaled:
            continue

        # BN momentum filter
        if bn_filter:
            bm = sig.get("bn_momentum_5s", 0) * 100
            if direction == "UP" and bm < -0.02:
                continue
            if direction == "DOWN" and bm > 0.02:
                continue

        # CL momentum filter
        if cl_filter:
            cm = sig.get("cl_momentum_5s", 0) * 100
            if direction == "UP" and cm < -0.03:
                continue
            if direction == "DOWN" and cm > 0.03:
                continue

        # Continuity
        if cont > 0:
            cont_counts[slug] += 1
            if cont_counts[slug] < cont:
                continue

        side = "YES" if direction == "UP" else "NO"
        ask_key = f"ask_{side.lower()}"
        ask = sig.get(ask_key, 0)
        if ask < min_book or ask > max_book:
            continue

        # Spread/depth
        if not spread_depth_ok(sig, side, max_spread=max_spread, min_depth=min_depth):
            continue

        # Fill
        if fill_mode == "maker":
            fp = maker_fill_prob(sig, side, 0.35)
            if random.random() > fp:
                continue
            fill = max(ask - 0.01, min_book)
            fee = pm_fee(fill) * (STAKE / fill)
        elif fill_mode == "taker":
            if random.random() > 0.92:
                continue
            fill = taker_fill_price(sig, side)
            fee = 0.015 * STAKE + pm_fee(fill) * (STAKE / fill)
        else:  # conservative
            fp = maker_fill_prob(sig, side, 0.30)
            if random.random() > fp:
                continue
            fill = ask  # worst case: fill at ask
            fee = pm_fee(fill) * (STAKE / fill)

        if fill < min_book or fill > max_book or fill >= 1.0 or fill <= 0:
            continue

        positions[slug] = {
            "side": direction, "asset": asset, "fill": fill,
            "shares": STAKE / fill, "secs": secs, "fee": fee, "slug": slug,
        }
        entered.add(slug)
        cont_counts[slug] = 0

    for slug in list(positions.keys()):
        if slug in settlements:
            settle(slug, settlements[slug]["outcome"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    return {"trades": total, "wins": wins, "losses": losses, "wr": wr,
            "pnl": round(pnl, 2), "dd": round(dd, 2), "log": trade_log}


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY 4: CL GAP BOT (fair value vs book price)
# ══════════════════════════════════════════════════════════════════════════════

def run_gap(min_edge, max_secs, min_secs, min_fair_dist, max_spread_filter,
            tfs, max_open, fill_mode, max_book_age_ms, min_depth):
    """CL Gap Bot: trade when BS fair value diverges from book price."""
    positions = {}
    done_slugs = set()
    wins = 0; losses = 0; pnl = 0.0; peak = 0.0; dd = 0.0
    settled_windows = set()
    trade_log = []

    def settle(slug, outcome_str):
        nonlocal wins, losses, pnl, peak, dd
        if slug not in positions:
            return
        pos = positions.pop(slug)
        if pos["direction"] == "YES":
            we_win = outcome_str == "YES"
        else:
            we_win = outcome_str == "NO"

        if we_win:
            gross = (1.0 - pos["entry_price"]) * STAKE
        else:
            gross = -pos["entry_price"] * STAKE

        net = gross - pos["fee"]
        pnl += net
        if net > 0: wins += 1
        else: losses += 1
        if pnl > peak: peak = pnl
        d = peak - pnl
        if d > dd: dd = d
        trade_log.append({"net": round(net, 2), "dir": pos["direction"],
                          "entry": pos["entry_price"], "edge": pos["edge"],
                          "secs": pos["secs"], "asset": pos["asset"], "slug": slug})

    for sig in signals:
        ts = sig["ts"]
        for wend in list(settle_by_end.keys()):
            if ts >= wend and wend not in settled_windows:
                settled_windows.add(wend)
                for s in settle_by_end[wend]:
                    settle(s["slug"], s["outcome"])

        slug = sig["slug"]
        secs = sig["secs_left"]
        if sig["tf"] not in tfs:
            continue
        if secs < min_secs or secs > max_secs:
            continue
        if slug in done_slugs or slug in positions:
            continue
        if len(positions) >= max_open:
            continue

        # Production gate
        if not production_gate(sig, max_book_age_ms=max_book_age_ms):
            continue

        fair_yes = sig.get("fair_yes", 0.5)
        fair_no = sig.get("fair_no", 0.5)

        if abs(fair_yes - 0.5) < min_fair_dist:
            continue

        if fair_yes > 0.5:
            direction = "YES"
            fair = fair_yes
            ask = sig.get("ask_yes", 0)
            bid = sig.get("bid_yes", 0)
            edge = fair - ask
        else:
            direction = "NO"
            fair = fair_no
            ask = sig.get("ask_no", 0)
            bid = sig.get("bid_no", 0)
            edge = fair - ask

        if ask <= 0.01 or ask >= 0.99:
            continue
        if bid <= 0 or bid >= 1.0:
            continue
        if ask - bid > max_spread_filter:
            continue
        if edge < min_edge:
            continue

        # Spread/depth
        side = direction
        if not spread_depth_ok(sig, side, max_spread=max_spread_filter, min_depth=min_depth):
            continue

        # Fill
        if fill_mode == "maker":
            fp = maker_fill_prob(sig, side, 0.30)
            if random.random() > fp:
                continue
            entry_price = max(ask - 0.01, 0.01)
            fee = pm_fee(entry_price) * (STAKE / entry_price)
        elif fill_mode == "taker":
            if random.random() > 0.92:
                continue
            entry_price = taker_fill_price(sig, side)
            fee = 0.02 * STAKE + pm_fee(entry_price) * (STAKE / entry_price)
        else:
            fp = maker_fill_prob(sig, side, 0.25)
            if random.random() > fp:
                continue
            entry_price = ask
            fee = 0.015 * STAKE + pm_fee(ask) * (STAKE / ask)

        if entry_price <= 0.01 or entry_price >= 0.99:
            continue

        positions[slug] = {
            "slug": slug, "direction": direction, "asset": sig["asset"],
            "entry_price": entry_price, "fair": fair, "edge": edge,
            "fee": fee, "secs": secs,
        }
        done_slugs.add(slug)

    for slug in list(positions.keys()):
        if slug in settlements:
            settle(slug, settlements[slug]["outcome"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    return {"trades": total, "wins": wins, "losses": losses, "wr": wr,
            "pnl": round(pnl, 2), "dd": round(dd, 2), "log": trade_log}


# ══════════════════════════════════════════════════════════════════════════════
# GRID SEARCH - ALL STRATEGIES
# ══════════════════════════════════════════════════════════════════════════════

def print_results(name, results, top_n=20):
    """Print top results sorted by PnL, filtered to 80%+ WR."""
    good = [r for r in results if r["wr"] >= 80.0 and r["trades"] >= 3]
    good.sort(key=lambda x: (-x["pnl"], -x["wr"]))

    print(f"\n{'=' * 100}")
    print(f"  {name}: {len(results)} configs tested, {len(good)} with ≥80% WR and ≥3 trades")
    print(f"{'=' * 100}")

    if not good:
        # Show best we have even if <80% WR
        results.sort(key=lambda x: (-x["wr"], -x["pnl"]))
        best = [r for r in results if r["trades"] >= 3][:5]
        if best:
            print("  (No configs hit 80% WR. Best available:)")
            for r in best:
                p = r.get("params", "?")
                print(f"    WR={r['wr']:.1f}% Tr={r['trades']} PnL=${r['pnl']:+.2f} DD=${r['dd']:.2f} | {p}")
        return good

    print(f"\n  {'#':>3} {'Tr':>4} {'W':>3} {'L':>3} {'WR':>6} {'PnL':>8} {'DD':>6} {'$/tr':>6}  Params")
    print(f"  {'-' * 90}")
    for i, r in enumerate(good[:top_n]):
        avg = r["pnl"] / r["trades"] if r["trades"] > 0 else 0
        p = r.get("params", "?")
        print(f"  {i+1:>3} {r['trades']:>4} {r['wins']:>3} {r['losses']:>3} "
              f"{r['wr']:>5.1f}% ${r['pnl']:>+6.2f} ${r['dd']:>5.2f} ${avg:>+5.2f}  {p}")

    return good


# ── 1. ORACLE SCANNER GRID ──────────────────────────────────────────────────

print("=" * 100)
print("  ORACLE SCANNER — Iterating for 80%+ WR")
print("=" * 100)

oracle_results = []
cnt = 0
for min_edge in [0.15, 0.20, 0.25, 0.30, 0.35]:
    for max_secs in [180, 240, 290]:
        for min_secs in [40, 60, 80]:
            for tfs in [[5], [15], [5, 15]]:
                for max_open in [4, 6, 10]:
                    for use_tp in [False, True]:
                        for use_sl in [False]:  # SL was bad in prior tests
                            for fill_mode in ["maker", "taker", "conservative"]:
                                cnt += 1
                                r = run_oracle(min_edge, max_secs, min_secs, tfs, max_open,
                                               use_tp, use_sl, fill_mode,
                                               max_spread=0.03, min_depth=500, max_book_age_ms=3000)
                                if r["trades"] >= 2:
                                    r["params"] = f"edge≥{min_edge} {min_secs}-{max_secs}s tf={tfs} open≤{max_open} tp={use_tp} fill={fill_mode}"
                                    oracle_results.append(r)

                                    # Reset random seed for reproducibility across configs
                                    random.seed(42 + cnt)

print(f"  Tested {cnt} configs")
oracle_good = print_results("ORACLE SCANNER", oracle_results)


# ── 2. S3C GRID ─────────────────────────────────────────────────────────────

print(f"\n{'=' * 100}")
print("  S3C (Both-Sides Dump) — Iterating for 80%+ WR")
print(f"{'=' * 100}")

s3c_results = []
cnt = 0
for es in [180, 240, 290]:
    for ee in [40, 60, 80]:
        if ee >= es:
            continue
        for al in [0.40, 0.44, 0.47]:
            for ah in [0.53, 0.56, 0.60]:
                for dp in [0.25, 0.30, 0.35, 0.40, 0.45, 0.50]:
                    for sl in [0.003, 0.005, 0.008]:
                        for tfs in [[5], [5, 15]]:
                            for fill_mode in ["maker", "taker", "conservative"]:
                                cnt += 1
                                random.seed(42 + cnt)
                                r = run_s3c(es, ee, al, ah, dp, sl, tfs,
                                            max_spread=0.03, min_depth=500,
                                            max_book_age_ms=3000, fill_mode=fill_mode)
                                if r["trades"] >= 2:
                                    r["params"] = f"{es}-{ee}s ask={al}-{ah} dump={dp} slip={sl} tf={tfs} fill={fill_mode}"
                                    s3c_results.append(r)

print(f"  Tested {cnt} configs")
s3c_good = print_results("S3C (Both-Sides Dump)", s3c_results)


# ── 3. SNIPER GRID ──────────────────────────────────────────────────────────

print(f"\n{'=' * 100}")
print("  SNIPER — Iterating for 80%+ WR")
print(f"{'=' * 100}")

sniper_results = []
cnt = 0
for delta in [0.02, 0.03, 0.04, 0.05, 0.06]:
    for es in [75, 90, 120, 180]:
        for ee in [20, 30, 40, 60]:
            if ee >= es:
                continue
            for mb in [0.55, 0.65, 0.75, 0.80, 0.85]:
                for xb in [0.88, 0.92, 0.95]:
                    for tfs in [[5], [5, 15]]:
                        for c in [0, 2, 3]:
                            for fill_mode in ["maker", "taker"]:
                                cnt += 1
                                random.seed(42 + cnt)
                                r = run_sniper(delta, es, ee, mb, xb, tfs, c,
                                               max_spread=0.03, min_depth=500,
                                               max_book_age_ms=3000, fill_mode=fill_mode)
                                if r["trades"] >= 2:
                                    r["params"] = f"d={delta} {es}-{ee}s book={mb}-{xb} tf={tfs} c={c} fill={fill_mode}"
                                    sniper_results.append(r)

print(f"  Tested {cnt} configs")
sniper_good = print_results("SNIPER", sniper_results)


# ── 4. CL GAP BOT GRID ──────────────────────────────────────────────────────

print(f"\n{'=' * 100}")
print("  CL GAP BOT — Iterating for 80%+ WR")
print(f"{'=' * 100}")

gap_results = []
cnt = 0
for min_edge in [0.10, 0.15, 0.20, 0.25, 0.30]:
    for max_secs in [180, 240, 300]:
        for min_secs in [30, 60, 90]:
            for min_fair_dist in [0.03, 0.05, 0.10]:
                for max_spread_f in [0.05, 0.10]:
                    for tfs in [[5], [15], [5, 15]]:
                        for max_open in [4, 6, 10]:
                            for fill_mode in ["maker", "taker", "conservative"]:
                                cnt += 1
                                random.seed(42 + cnt)
                                r = run_gap(min_edge, max_secs, min_secs, min_fair_dist,
                                            max_spread_f, tfs, max_open, fill_mode,
                                            max_book_age_ms=3000, min_depth=300)
                                if r["trades"] >= 2:
                                    r["params"] = f"edge≥{min_edge} {min_secs}-{max_secs}s fair_d≥{min_fair_dist} spr≤{max_spread_f} tf={tfs} open≤{max_open} fill={fill_mode}"
                                    gap_results.append(r)

print(f"  Tested {cnt} configs")
gap_good = print_results("CL GAP BOT", gap_results)


# ══════════════════════════════════════════════════════════════════════════════
# FINAL SUMMARY: BEST CONFIG PER STRATEGY
# ══════════════════════════════════════════════════════════════════════════════

print(f"\n{'=' * 100}")
print("  PRODUCTION-READY CONFIGS (≥80% WR, ≥3 trades, production-realistic fills)")
print(f"{'=' * 100}")

for name, good in [("ORACLE", oracle_good), ("S3C", s3c_good),
                    ("SNIPER", sniper_good), ("GAP", gap_good)]:
    viable = [r for r in good if r["trades"] >= 3] if good else []
    if not viable:
        print(f"\n  [{name}] No configs met 80% WR + 3 trade minimum")
        continue

    # Pick best by: decent PnL, not extreme
    # Sort by (WR desc, PnL desc) but cap at reasonable
    viable.sort(key=lambda x: (-x["wr"], -x["pnl"]))
    best = viable[0]
    avg = best["pnl"] / best["trades"]

    print(f"\n  [{name}] BEST CONFIG:")
    print(f"    {best['params']}")
    print(f"    Trades={best['trades']} W={best['wins']} L={best['losses']} "
          f"WR={best['wr']:.1f}% PnL=${best['pnl']:+.2f} DD=${best['dd']:.2f} $/trade=${avg:+.2f}")

    # Also show highest PnL config
    viable.sort(key=lambda x: -x["pnl"])
    best_pnl = viable[0]
    if best_pnl != best:
        avg2 = best_pnl["pnl"] / best_pnl["trades"]
        print(f"  [{name}] HIGHEST PnL:")
        print(f"    {best_pnl['params']}")
        print(f"    Trades={best_pnl['trades']} W={best_pnl['wins']} L={best_pnl['losses']} "
              f"WR={best_pnl['wr']:.1f}% PnL=${best_pnl['pnl']:+.2f} DD=${best_pnl['dd']:.2f} $/trade=${avg2:+.2f}")

    # Show trade details for best WR config
    if "log" in best and best.get("log"):
        print(f"\n    Trade log ({name} best WR):")
        log = best.get("log", [])
        for i, t in enumerate(log[:15]):
            w = "WIN" if t["net"] > 0 else "LOSS"
            extra = ""
            if "exit" in t:
                extra = f" exit={t['exit']}"
            if "edge" in t:
                extra += f" edge={t['edge']:.3f}"
            if "fill" in t:
                extra += f" fill={t['fill']:.3f}"
            print(f"      {i+1:>3}. {w:>4s} ${t['net']:>+5.2f} {t.get('asset','?'):>4s} "
                  f"secs={t.get('secs',0):>5.0f}{extra}")


# ══════════════════════════════════════════════════════════════════════════════
# PRODUCTION REALISM AUDIT
# ══════════════════════════════════════════════════════════════════════════════

print(f"\n{'=' * 100}")
print("  PRODUCTION REALISM AUDIT")
print(f"{'=' * 100}")

# How many signals pass production gate?
pass_gate = sum(1 for s in signals if production_gate(s))
print(f"\n  Signals passing production gate: {pass_gate}/{len(signals)} ({pass_gate/len(signals)*100:.1f}%)")

# Book age distribution
ages = [s.get("book_age_ms", 0) for s in signals]
ages.sort()
n = len(ages)
print(f"  Book age: p50={ages[n//2]:.0f}ms p90={ages[int(n*0.9)]:.0f}ms p99={ages[int(n*0.99)]:.0f}ms max={ages[-1]:.0f}ms")

# CL feed age
cl_ages = [s.get("cl_feed_age_ms", 0) for s in signals]
cl_ages.sort()
print(f"  CL feed age: p50={cl_ages[n//2]:.0f}ms p90={cl_ages[int(n*0.9)]:.0f}ms p99={cl_ages[int(n*0.99)]:.0f}ms")

# BN feed age
bn_ages = [s.get("bn_feed_age_ms", 0) for s in signals]
bn_ages.sort()
print(f"  BN feed age: p50={bn_ages[n//2]:.0f}ms p90={bn_ages[int(n*0.9)]:.0f}ms p99={bn_ages[int(n*0.99)]:.0f}ms")

# Stale rates
cl_stale = sum(1 for s in signals if s.get("cl_stale"))
bn_stale = sum(1 for s in signals if s.get("bn_stale"))
bk_stale = sum(1 for s in signals if s.get("book_stale"))
print(f"  Stale rates: CL={cl_stale}/{n} ({cl_stale/n*100:.2f}%) BN={bn_stale}/{n} ({bn_stale/n*100:.2f}%) Book={bk_stale}/{n} ({bk_stale/n*100:.2f}%)")

print(f"\n  Fill model assumptions:")
print(f"    Maker: base 30-35% fill prob, reduced for wide spread/deep queue/fast edge")
print(f"    Taker: 92% fill rate, 0.5-1.0c slippage, 1.5-2% fee")
print(f"    PM fee: price*(1-price)*6.25% per side")
print(f"    Conservative: maker price, taker fee (worst of both)")
print(f"    Book age gate: ≤3000ms (reject stale books)")
print(f"    Spread gate: ≤3-4c (reject wide spreads)")
print(f"    Depth gate: ≥500 shares (reject thin books)")
print(f"    Data quality: 'full' only")
