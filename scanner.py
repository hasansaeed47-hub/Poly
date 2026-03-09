#!/usr/bin/env python3
"""
Both-Sides Scanner v2 — Bulletproof Edition

Strategy: Maker bid at TARGET_BID on BOTH sides (UP + DN) of 5-min binary markets.

Execution flow:
  1. ENTRY GATES must all pass before posting bids
  2. Both fill as maker → hold to settlement → guaranteed profit
  3. One fills → taker hedge other side (limit MAX_HEDGE_ASK)
     → hedge fills → small profit or breakeven
     → hedge fails → sell filled side as maker at entry price (breakeven)
     → sell not filling → taker dump at bid (tiny capped loss)
  4. Neither fills → $0.00

Fees: Maker=0% | Taker=p*(1-p)*3.14% | Settlement=0%
"""

import time
import json
import requests
from datetime import datetime, timezone

# ── Strategy Config ────────────────────────────────────────────────
ASSETS = ["btc", "eth", "sol", "xrp"]
WINDOW_MIN = 5
GAMMA = "https://gamma-api.polymarket.com"
CLOB  = "https://clob.polymarket.com"
STAKE = 5.0
REFRESH_SEC = 5
HEADERS = {"User-Agent": "scanner/2"}

TARGET_BID = 0.485           # maker bid price on both sides

# ── Entry Gates ────────────────────────────────────────────────────
GATE_MAX_ASK       = 0.505   # both asks must be ≤ this
GATE_MAX_SPREAD    = 0.030   # both spreads must be ≤ this
GATE_MIN_DEPTH     = 10.0    # both sides must have ≥ $10 top-of-book size
GATE_MIN_LEFT_S    = 240     # must have ≥ 4 min remaining
GATE_MAX_BID       = 0.50    # bids must be ≤ this (near 50/50)
GATE_MIN_BID       = 0.47    # bids must be ≥ this (market not already decided)

# ── Fee Model ─────────────────────────────────────────────────────
# Maker = 0%, Taker = p*(1-p)*0.0314, Settlement = 0%
def taker_fee(px: float) -> float:
    return px * (1.0 - px) * 0.0314

# ── Max hedge ask: solve ask + taker_fee(ask) = 1.0 - TARGET_BID ──
def calc_max_hedge_ask(maker_px: float) -> float:
    budget = 1.0 - maker_px
    lo, hi = 0.40, 0.60
    for _ in range(50):
        mid = (lo + hi) / 2.0
        if mid + mid * (1.0 - mid) * 0.0314 < budget:
            lo = mid
        else:
            hi = mid
    return lo

MAX_HEDGE_ASK = calc_max_hedge_ask(TARGET_BID)

# ── Discover active 5-min windows ─────────────────────────────────
def discover_windows() -> list[dict]:
    now = int(time.time())
    iv = WINDOW_MIN * 60
    s0 = (now // iv) * iv
    windows = []
    for asset in ASSETS:
        for start_ts in [s0, s0 + iv]:
            end_ts = start_ts + iv
            if end_ts <= now:
                continue
            slug = f"{asset}-updown-{WINDOW_MIN}m-{start_ts}"
            try:
                r = requests.get(f"{GAMMA}/markets", params={"slug": slug},
                                 headers=HEADERS, timeout=5)
                if r.status_code != 200:
                    continue
                d = r.json()
                m = d[0] if isinstance(d, list) and len(d) > 0 else d
                if not m or not m.get("clobTokenIds"):
                    continue
                tids_raw = m["clobTokenIds"]
                tids = json.loads(tids_raw) if isinstance(tids_raw, str) else tids_raw
                if len(tids) < 2:
                    continue
                outs_raw = m.get("outcomes", "[]")
                outs = json.loads(outs_raw) if isinstance(outs_raw, str) else outs_raw
                if len(outs) >= 2 and outs[0] == "Down":
                    tid_up, tid_dn = tids[1], tids[0]
                else:
                    tid_up, tid_dn = tids[0], tids[1]
                windows.append({
                    "asset": asset.upper(), "slug": slug,
                    "tid_up": tid_up, "tid_dn": tid_dn,
                    "start_ts": start_ts, "end_ts": end_ts,
                    "left_s": end_ts - now,
                })
            except Exception:
                continue
    return windows

# ── Fetch orderbooks ───────────────────────────────────────────────
def fetch_books(token_ids: list[str]) -> dict:
    if not token_ids:
        return {}
    body = [{"token_id": tid} for tid in token_ids]
    try:
        r = requests.post(f"{CLOB}/books", json=body, headers=HEADERS, timeout=5)
        if r.status_code != 200:
            return {}
        res = r.json()
    except Exception:
        return {}
    books = {}
    for item in res:
        tid = item.get("asset_id", "")
        if not tid:
            continue
        bk = {"bid": 0.0, "ask": 0.0, "bid_sz": 0.0, "ask_sz": 0.0,
               "n_bids": 0, "n_asks": 0, "spread": 999.0}
        bids = item.get("bids", [])
        if bids:
            bk["n_bids"] = len(bids)
            best = max(bids, key=lambda b: float(b.get("price", "0")))
            bk["bid"] = float(best.get("price", "0"))
            bk["bid_sz"] = float(best.get("size", "0"))
        asks = item.get("asks", [])
        if asks:
            bk["n_asks"] = len(asks)
            best = min(asks, key=lambda a: float(a.get("price", "999")))
            bk["ask"] = float(best.get("price", "0"))
            bk["ask_sz"] = float(best.get("size", "0"))
        if bk["bid"] > 0 and bk["ask"] > 0:
            bk["spread"] = bk["ask"] - bk["bid"]
        books[tid] = bk
    return books

# ── Entry Gate Checks ──────────────────────────────────────────────
def check_gates(w: dict, up: dict, dn: dict) -> tuple[bool, list[str]]:
    """Returns (pass, [fail_reasons])"""
    fails = []

    # Gate 1: Both asks must be ≤ MAX_ASK
    if up["ask"] <= 0 or up["ask"] > GATE_MAX_ASK:
        fails.append(f"UP ask {up['ask']:.3f} > {GATE_MAX_ASK}")
    if dn["ask"] <= 0 or dn["ask"] > GATE_MAX_ASK:
        fails.append(f"DN ask {dn['ask']:.3f} > {GATE_MAX_ASK}")

    # Gate 2: Both spreads must be ≤ MAX_SPREAD
    if up["spread"] > GATE_MAX_SPREAD:
        fails.append(f"UP spread {up['spread']:.3f} > {GATE_MAX_SPREAD}")
    if dn["spread"] > GATE_MAX_SPREAD:
        fails.append(f"DN spread {dn['spread']:.3f} > {GATE_MAX_SPREAD}")

    # Gate 3: Both sides must have depth ≥ MIN_DEPTH
    if up["ask_sz"] < GATE_MIN_DEPTH:
        fails.append(f"UP ask depth ${up['ask_sz']:.0f} < ${GATE_MIN_DEPTH:.0f}")
    if dn["ask_sz"] < GATE_MIN_DEPTH:
        fails.append(f"DN ask depth ${dn['ask_sz']:.0f} < ${GATE_MIN_DEPTH:.0f}")

    # Gate 4: Enough time remaining
    if w["left_s"] < GATE_MIN_LEFT_S:
        fails.append(f"Only {w['left_s']}s left < {GATE_MIN_LEFT_S}s")

    # Gate 5: Market still near 50/50 (not already decided)
    if up["bid"] > 0 and (up["bid"] > GATE_MAX_BID or up["bid"] < GATE_MIN_BID):
        fails.append(f"UP bid {up['bid']:.3f} outside {GATE_MIN_BID}-{GATE_MAX_BID}")
    if dn["bid"] > 0 and (dn["bid"] > GATE_MAX_BID or dn["bid"] < GATE_MIN_BID):
        fails.append(f"DN bid {dn['bid']:.3f} outside {GATE_MIN_BID}-{GATE_MAX_BID}")

    return (len(fails) == 0, fails)

# ── Evaluate all scenarios for a window ────────────────────────────
def evaluate(w: dict, up_bk: dict, dn_bk: dict) -> dict:
    gates_pass, gate_fails = check_gates(w, up_bk, dn_bk)

    result = {
        "asset": w["asset"], "slug": w["slug"], "left_s": w["left_s"],
        "up_bid": up_bk["bid"], "up_ask": up_bk["ask"], "up_spread": up_bk["spread"],
        "up_bid_sz": up_bk["bid_sz"], "up_ask_sz": up_bk["ask_sz"],
        "up_n_bids": up_bk["n_bids"], "up_n_asks": up_bk["n_asks"],
        "dn_bid": dn_bk["bid"], "dn_ask": dn_bk["ask"], "dn_spread": dn_bk["spread"],
        "dn_bid_sz": dn_bk["bid_sz"], "dn_ask_sz": dn_bk["ask_sz"],
        "dn_n_bids": dn_bk["n_bids"], "dn_n_asks": dn_bk["n_asks"],
        "gates_pass": gates_pass, "gate_fails": gate_fails,
        "signal": "SKIP",
        # Scenario results
        "s1_both_maker": {},
        "s2_hedge_taker": {},
        "s3_sellback_maker": {},
        "s4_dump_taker": {},
    }

    # ── S1: Both sides fill as MAKER at TARGET_BID (best case) ─────
    cost_s1 = TARGET_BID * 2
    profit_s1 = 1.0 - cost_s1  # maker fee = 0%
    shares = STAKE / TARGET_BID
    result["s1_both_maker"] = {
        "cost": cost_s1,
        "profit_per_sh": profit_s1,
        "profit_dollar": profit_s1 * shares,
        "verdict": "PROFIT" if profit_s1 > 0 else "LOSS",
    }

    # ── S2: One fills as maker, hedge other as taker at current ask ─
    for label, mkr_side, tkr_ask in [
        ("UP maker + DN taker", "UP", dn_bk["ask"]),
        ("DN maker + UP taker", "DN", up_bk["ask"]),
    ]:
        if tkr_ask <= 0:
            continue
        cost = TARGET_BID + tkr_ask
        fee = taker_fee(tkr_ask)
        total = cost + fee
        profit = 1.0 - total
        result["s2_hedge_taker"][label] = {
            "maker_px": TARGET_BID, "taker_px": tkr_ask,
            "taker_fee": fee, "total": total,
            "profit_per_sh": profit,
            "profit_dollar": profit * shares,
            "verdict": "PROFIT" if profit > 0 else ("BREAK" if abs(profit) < 0.001 else "LOSS"),
        }

    # ── S3: Hedge fails → sell filled side back as MAKER at entry ──
    # Buy at TARGET_BID, sell at TARGET_BID, both maker = 0% fee
    result["s3_sellback_maker"] = {
        "buy_px": TARGET_BID, "sell_px": TARGET_BID,
        "cost": 0.0, "profit_per_sh": 0.0, "profit_dollar": 0.0,
        "verdict": "BREAKEVEN",
    }

    # ── S4: Sell-back not filling → taker dump at current bid ──────
    for label, bid in [("Dump UP side", up_bk["bid"]), ("Dump DN side", dn_bk["bid"])]:
        if bid <= 0:
            continue
        loss_per_sh = TARGET_BID - bid - taker_fee(bid)
        result["s4_dump_taker"][label] = {
            "entry_px": TARGET_BID, "dump_bid": bid,
            "dump_fee": taker_fee(bid),
            "loss_per_sh": loss_per_sh,
            "loss_dollar": loss_per_sh * shares,
            "verdict": "TINY_LOSS" if loss_per_sh < 0.02 else "LOSS",
        }

    # ── Signal (only if gates pass) ──
    if not gates_pass:
        result["signal"] = "GATE_FAIL"
    else:
        # Check worst-case hedge
        s2_profits = [v["profit_per_sh"] for v in result["s2_hedge_taker"].values()]
        worst_hedge = min(s2_profits) if s2_profits else -1

        if worst_hedge >= 0:
            result["signal"] = "GO"         # all paths ≥ breakeven
        elif worst_hedge >= -0.005:
            result["signal"] = "NEAR_GO"    # worst hedge barely negative, sellback covers
        else:
            result["signal"] = "CAUTION"    # hedge too expensive, rely on sellback

    return result

# ── Display ────────────────────────────────────────────────────────
SIG_COL = {
    "GO":        "\033[1;32m",   # bright green
    "NEAR_GO":   "\033[1;36m",   # cyan
    "CAUTION":   "\033[0;33m",   # yellow
    "GATE_FAIL": "\033[0;90m",   # gray
    "SKIP":      "\033[0;90m",   # gray
}
RST = "\033[0m"
BOLD = "\033[1m"

def display(results: list[dict]):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print("\033[2J\033[H")
    print(f"{'='*100}")
    print(f"  BOTH-SIDES SCANNER v2  |  {ts} UTC  |  Bid ${TARGET_BID}  |  "
          f"Max hedge ask ${MAX_HEDGE_ASK:.4f}  |  Stake ${STAKE:.0f}")
    print(f"{'='*100}")
    print()

    # ── Summary table ──
    print(f"  {'ASSET':<5} {'LEFT':>5}  "
          f"{'UP bid':>7} {'UP ask':>7} {'UP spr':>6}  "
          f"{'DN bid':>7} {'DN ask':>7} {'DN spr':>6}  "
          f"{'SIGNAL':<10} {'S1 M+M':>8} {'S2 Hedge':>8}")
    print(f"  {'─'*5} {'─'*5}  {'─'*7} {'─'*7} {'─'*6}  {'─'*7} {'─'*7} {'─'*6}  {'─'*10} {'─'*8} {'─'*8}")

    for r in results:
        col = SIG_COL.get(r["signal"], "")
        s1 = r["s1_both_maker"]
        s2_worst = min((v["profit_per_sh"] for v in r["s2_hedge_taker"].values()), default=0)

        left_m, left_s = r["left_s"] // 60, r["left_s"] % 60
        print(f"  {col}{r['asset']:<5} {left_m}:{left_s:02d}  "
              f"{r['up_bid']:7.3f} {r['up_ask']:7.3f} {r['up_spread']:6.3f}  "
              f"{r['dn_bid']:7.3f} {r['dn_ask']:7.3f} {r['dn_spread']:6.3f}  "
              f"{r['signal']:<10} {s1['profit_per_sh']:+7.4f} {s2_worst:+7.4f}{RST}")

    # ── Detail for GO / NEAR_GO ──
    actionable = [r for r in results if r["signal"] in ("GO", "NEAR_GO", "CAUTION")]
    if actionable:
        print()
        print(f"  {'─'*100}")
        for r in actionable:
            col = SIG_COL.get(r["signal"], "")
            print()
            print(f"  {BOLD}{r['asset']} ({r['left_s']}s left){RST}  "
                  f"Signal: {col}{r['signal']}{RST}")

            # S1
            s1 = r["s1_both_maker"]
            print(f"    S1 Both maker:    {TARGET_BID} + {TARGET_BID} = {s1['cost']:.3f}  "
                  f"→ {s1['verdict']}  {s1['profit_per_sh']:+.4f}/sh  (${s1['profit_dollar']:+.2f})")

            # S2
            for label, v in r["s2_hedge_taker"].items():
                status_col = "\033[1;32m" if v["profit_per_sh"] >= 0 else "\033[1;31m"
                print(f"    S2 {label}: {v['maker_px']:.3f} + {v['taker_px']:.3f} + fee {v['taker_fee']:.4f} = {v['total']:.4f}  "
                      f"→ {status_col}{v['verdict']}{RST}  {v['profit_per_sh']:+.4f}/sh  (${v['profit_dollar']:+.2f})")

            # S3
            s3 = r["s3_sellback_maker"]
            print(f"    S3 Sell-back:     buy {s3['buy_px']:.3f} → sell {s3['sell_px']:.3f} (maker 0% fee)  "
                  f"→ \033[1;33m{s3['verdict']}{RST}")

            # S4
            for label, v in r["s4_dump_taker"].items():
                status_col = "\033[0;33m" if v["loss_per_sh"] < 0.02 else "\033[1;31m"
                print(f"    S4 {label}:  {v['entry_px']:.3f} → dump at bid {v['dump_bid']:.3f} - fee {v['dump_fee']:.4f}  "
                      f"→ {status_col}{v['verdict']}{RST}  {v['loss_per_sh']:+.4f}/sh  (${v['loss_dollar']:+.2f})")

            # Max hedge ask
            print(f"    Max taker ask for hedge: ≤ ${MAX_HEDGE_ASK:.4f}  "
                  f"(UP ask {'✅' if r['up_ask'] <= MAX_HEDGE_ASK else '❌'} {r['up_ask']:.3f}  "
                  f"DN ask {'✅' if r['dn_ask'] <= MAX_HEDGE_ASK else '❌'} {r['dn_ask']:.3f})")

            # Depth
            print(f"    Depth: UP [${r['up_ask_sz']:.0f} ask / ${r['up_bid_sz']:.0f} bid]  "
                  f"DN [${r['dn_ask_sz']:.0f} ask / ${r['dn_bid_sz']:.0f} bid]  "
                  f"(need ≥${GATE_MIN_DEPTH:.0f})")

            # Gate status
            if r["gate_fails"]:
                print(f"    \033[0;90mGate warnings: {', '.join(r['gate_fails'])}{RST}")

    else:
        print()
        print(f"  \033[0;90mNo actionable windows. Waiting for clean setup...{RST}")

    # ── Footer ──
    print()
    print(f"  {'─'*100}")
    print(f"  EXECUTION FLOW:")
    print(f"  1. Post maker bid ${TARGET_BID} on BOTH sides")
    print(f"  2. Both fill → hold → S1 profit ${1.0 - TARGET_BID*2:+.3f}/sh")
    print(f"  3. One fills → taker hedge other (limit ${MAX_HEDGE_ASK:.4f}) → S2 profit/breakeven")
    print(f"  4. Hedge fails → sell filled side as maker at ${TARGET_BID} → S3 breakeven")
    print(f"  5. Sell not filling → taker dump at bid → S4 tiny capped loss")
    print()
    print(f"  ENTRY GATES: asks≤{GATE_MAX_ASK} | spread≤{GATE_MAX_SPREAD} | "
          f"depth≥${GATE_MIN_DEPTH:.0f} | time≥{GATE_MIN_LEFT_S}s | "
          f"bids {GATE_MIN_BID}-{GATE_MAX_BID}")
    print(f"  FEES: Maker=0% | Taker=p*(1-p)*3.14% | Settlement=0%")
    print()

# ── Main loop ──────────────────────────────────────────────────────
def main():
    print(f"Both-Sides Scanner v2 — Bulletproof Edition")
    print(f"Assets: {', '.join(a.upper() for a in ASSETS)}  |  Bid: ${TARGET_BID}  |  "
          f"Max hedge: ${MAX_HEDGE_ASK:.4f}")
    print()

    while True:
        try:
            windows = discover_windows()
            if not windows:
                print("\033[2J\033[H")
                print("No active 5-min windows found. Waiting...")
                time.sleep(REFRESH_SEC)
                continue

            all_tids = []
            for w in windows:
                all_tids.extend([w["tid_up"], w["tid_dn"]])
            books = fetch_books(list(set(all_tids)))

            results = []
            for w in windows:
                w["left_s"] = w["end_ts"] - int(time.time())
                if w["left_s"] <= 0:
                    continue
                up_bk = books.get(w["tid_up"], {"bid":0,"ask":0,"bid_sz":0,"ask_sz":0,"n_bids":0,"n_asks":0,"spread":999})
                dn_bk = books.get(w["tid_dn"], {"bid":0,"ask":0,"bid_sz":0,"ask_sz":0,"n_bids":0,"n_asks":0,"spread":999})
                results.append(evaluate(w, up_bk, dn_bk))

            sig_order = {"GO": 0, "NEAR_GO": 1, "CAUTION": 2, "GATE_FAIL": 3, "SKIP": 4}
            results.sort(key=lambda r: (sig_order.get(r["signal"], 9), r["asset"]))
            display(results)
            time.sleep(REFRESH_SEC)

        except KeyboardInterrupt:
            print("\nScanner stopped.")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(REFRESH_SEC)

if __name__ == "__main__":
    main()
