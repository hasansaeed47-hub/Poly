#!/usr/bin/env python3
"""
run_15m.py — BTC 15m single-window live run
=============================================
Uses Chainlink RTDS momentum to select which leg to post first,
then chases the second leg through passive → aggressive → FAK.

Target: BTC 15m window opening 3:30 PM PKT (10:30 UTC) 2026-04-12

Required env vars (live mode):
    POLY_PRIVATE_KEY      — Polygon wallet private key (0x...)
    POLY_FUNDER_ADDRESS   — Proxy/funder wallet address (0x...)

Usage:
    python run_15m.py              # live
    python run_15m.py --paper      # paper (no real orders, for testing)
"""

from __future__ import annotations

import os
import sys
import time
import signal
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple

import requests

from infra import (
    Config, log, POOL,
    BinanceFeed, ChainlinkFeed, BookFetcher, ExecutionLayer,
    MarketWindow, Book,
)

# ─── Session ──────────────────────────────────────────────────────────────────

_S = requests.Session()
_S.headers.update({"Connection": "keep-alive"})
_adapter = requests.adapters.HTTPAdapter(
    pool_connections=4, pool_maxsize=8, max_retries=2
)
_S.mount("https://", _adapter)


# ─── Target window ────────────────────────────────────────────────────────────

PKT = timezone(timedelta(hours=5))  # Pakistan Standard Time = UTC+5

TARGET_START_PKT = datetime(2026, 4, 12, 15, 30, 0, tzinfo=PKT)
TARGET_START_TS  = int(TARGET_START_PKT.timestamp())   # 10:30:00 UTC
TARGET_END_TS    = TARGET_START_TS + 15 * 60           # 10:45:00 UTC


# ─── Strategy params ──────────────────────────────────────────────────────────

STAKE          = 1.0    # $ per leg (USDC)
MIN_EDGE       = 0.03   # minimum combined edge to enter (3¢ = $0.03 on $1.00 pair)
MOM_LOOKBACK   = 45     # seconds of CL history for momentum calculation
ENTRY_OFFSET   = 0.02   # post leg1 this far above best bid (inside spread)
PASSIVE_SEC    = 30     # stay passive on leg2 for this long
AGGRESSIVE_SEC = 45     # start walking price up after this
FAK_SEC        = 60     # cross the spread (FAK) after this
MIN_EDGE_AGG   = 0.01   # floor edge when aggressive (1¢)
SCAN_RETRY_S   = 15     # seconds between Gamma retries
SCAN_DEADLINE  = TARGET_START_TS + 120  # give up scanning 2min after target start


# ─── Gamma window scanner ─────────────────────────────────────────────────────

def _parse_items(raw) -> list:
    """Normalise Gamma API response to a flat list of market dicts."""
    if isinstance(raw, list):
        return raw
    for key in ("markets", "events", "data", "results"):
        val = raw.get(key)
        if isinstance(val, list):
            return val
    return []


def _ts_from_iso(s: str) -> int:
    """Parse ISO-8601 date string → unix timestamp."""
    if not s:
        return 0
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return 0


def _extract_tokens(m: dict) -> Tuple[str, str]:
    """Return (tid_up, tid_down) from a Gamma market object."""
    tid_up = tid_dn = ""
    tokens = m.get("tokens") or m.get("clobTokenIds") or []

    if isinstance(tokens, list):
        for t in tokens:
            if isinstance(t, dict):
                outcome = (t.get("outcome") or "").lower()
                tid     = t.get("token_id") or t.get("tokenId") or ""
                if "up" in outcome and not tid_up:
                    tid_up = tid
                elif "down" in outcome and not tid_dn:
                    tid_dn = tid
            elif isinstance(t, str):
                # Some endpoints return bare token ID strings [up_id, down_id]
                if not tid_up:
                    tid_up = t
                elif not tid_dn:
                    tid_dn = t

    return tid_up, tid_dn


def find_window(target_ts: int, tol: int = 90) -> Optional[MarketWindow]:
    """
    Query Gamma API for the BTC 15m window whose start is within ±tol
    seconds of target_ts.  Tries multiple endpoint patterns.
    """
    urls = [
        f"{Config.GAMMA}/markets?tag_slug=crypto&active=true&limit=200",
        f"{Config.GAMMA}/markets?active=true&limit=200",
        f"{Config.GAMMA}/events?active=true&limit=100",
    ]

    for url in urls:
        try:
            r = _S.get(url, timeout=10)
            if r.status_code != 200:
                log.debug(f"[SCAN] {url} → HTTP {r.status_code}")
                continue

            items = _parse_items(r.json())

            # Events endpoint nests markets inside each event
            expanded = []
            for item in items:
                nested = item.get("markets")
                if isinstance(nested, list):
                    expanded.extend(nested)
                else:
                    expanded.append(item)

            for m in expanded:
                slug = (m.get("slug") or m.get("marketSlug") or "").lower()

                # Must be a BTC 15m window
                if "btc" not in slug or "15m" not in slug:
                    continue

                s_ts = _ts_from_iso(m.get("startDate") or m.get("startDateIso") or "")
                e_ts = _ts_from_iso(m.get("endDate")   or m.get("endDateIso")   or "")

                if not e_ts and s_ts:
                    e_ts = s_ts + 900  # default 15 min

                # Match by start timestamp
                if s_ts and abs(s_ts - target_ts) > tol:
                    continue

                tid_up, tid_dn = _extract_tokens(m)
                if not tid_up or not tid_dn:
                    log.debug(f"[SCAN] {slug} — token IDs missing, skipping")
                    continue

                cid = m.get("conditionId") or ""

                return MarketWindow(
                    eid      = m.get("id") or m.get("marketId") or slug,
                    title    = m.get("question") or m.get("title") or slug,
                    slug     = slug,
                    asset    = "btc",
                    wmin     = 15,
                    cid_up   = cid,
                    cid_down = cid,
                    tid_up   = tid_up,
                    tid_down = tid_dn,
                    start_ts = s_ts or target_ts,
                    end_ts   = e_ts  or (target_ts + 900),
                )

        except Exception as e:
            log.debug(f"[SCAN] {url} error: {e}")

    return None


# ─── Chainlink momentum ───────────────────────────────────────────────────────

def cl_momentum(cl: ChainlinkFeed, secs: int = MOM_LOOKBACK) -> Tuple[str, float]:
    """
    Compute momentum from CL RTDS price history.

    Returns:
        direction  — "UP" or "DOWN"
        magnitude  — absolute % move over last `secs` seconds

    Logic:
        - Look back `secs` seconds in CL history
        - Positive drift  → bias leg1 on UP side   (more UP sellers coming in)
        - Negative drift  → bias leg1 on DOWN side
        - Near-zero drift → pick cheaper ask side
    """
    with cl._lock:
        h = list(cl.hist.get("btc", []))

    if len(h) < 2:
        log.warning("[MOM] Insufficient CL history — defaulting neutral")
        return ("UP", 0.0)

    now    = time.time()
    cutoff = now - secs

    # Oldest price at or after cutoff
    old_p: Optional[float] = None
    for ts, p in h:
        if ts >= cutoff:
            old_p = p
            break

    if old_p is None:
        old_p = h[0][1]  # use absolute oldest if history is short

    cur_p = h[-1][1]

    if old_p == 0:
        return ("UP", 0.0)

    pct       = ((cur_p - old_p) / old_p) * 100
    direction = "UP" if pct >= 0 else "DOWN"

    log.info(
        f"[MOM] CL BTC ${old_p:,.2f} → ${cur_p:,.2f}  |  "
        f"move={pct:+.4f}%  →  leg1={direction}"
    )
    return (direction, abs(pct))


# ─── Entry price ──────────────────────────────────────────────────────────────

def maker_price(book: Book) -> float:
    """
    Competitive maker bid price: best_bid + ENTRY_OFFSET, capped 1¢ below ask.
    Keeps us as a maker (below ask) with queue priority over the current best bid.
    """
    if not book.bids or not book.asks:
        return 0.0
    bb = book.bb
    ba = book.ba
    px = round(min(bb + ENTRY_OFFSET, ba - 0.01), 2)
    return max(0.01, min(px, 0.99))


# ─── Window runner ────────────────────────────────────────────────────────────

class WindowRunner:
    """
    Single-window pair accumulator.

    State machine:
        WAIT → LEG1_POSTED → LEG1_FILLED → LEG2_CHASE → DONE / ORPHAN
    """

    def __init__(self, paper: bool = False):
        self.paper  = paper
        self.bn     = BinanceFeed()
        self.cl     = ChainlinkFeed()
        self.books  = BookFetcher()
        self.exec   = ExecutionLayer(paper=paper)

        # Trade state
        self.window:       Optional[MarketWindow] = None
        self.leg1_side:    str   = ""     # "UP" or "DOWN"
        self.leg1_oid:     str   = ""
        self.leg1_px:      float = 0.0
        self.leg1_fill_px: float = 0.0
        self.leg1_fill_ts: float = 0.0
        self.leg2_side:    str   = ""
        self.leg2_fill_px: float = 0.0

        self._running = True

    # ── Entry point ────────────────────────────────────────────────────────────

    def run(self):
        self._banner()

        signal.signal(signal.SIGTERM, lambda s, f: self._stop())
        signal.signal(signal.SIGINT,  lambda s, f: self._stop())

        # Start price feeds
        self.bn.start()
        self.cl.set_bn_fallback(self.bn)
        self.cl.start()
        log.info("[INIT] Feeds starting — warming 5s...")
        time.sleep(5)

        # Live CLOB init
        if not self.paper:
            creds = self.exec.init_live()
            if not creds:
                log.error("[INIT] Live CLOB init failed — aborting")
                self._cleanup()
                return
            log.info(f"[INIT] CLOB ready: key={creds.api_key[:12]}...")
        else:
            log.info("[INIT] PAPER mode — no real orders")

        # Find window
        self._find_window()
        if not self.window or not self._running:
            log.error("[INIT] Window not found — aborting")
            self._cleanup()
            return

        self._log_window()

        # Wait until open
        self._wait_for_open()
        if not self._running:
            self._cleanup()
            return

        # Trade
        self._enter_leg1()
        if self._running and self.leg1_fill_ts > 0:
            self._chase_leg2()

        self._summary()
        self._cleanup()

    # ── Window discovery ───────────────────────────────────────────────────────

    def _find_window(self):
        log.info(
            f"[SCAN] Searching for BTC 15m window at "
            f"{TARGET_START_PKT.strftime('%H:%M PKT')} "
            f"(ts={TARGET_START_TS})"
        )
        while self._running and time.time() < SCAN_DEADLINE:
            w = find_window(TARGET_START_TS)
            if w:
                self.window = w
                return
            remaining = max(0, SCAN_DEADLINE - time.time())
            log.info(f"[SCAN] Not found — retry in {SCAN_RETRY_S}s ({remaining:.0f}s left)")
            time.sleep(SCAN_RETRY_S)

        if not self.window:
            log.error("[SCAN] Window not found before deadline")

    def _log_window(self):
        w = self.window
        log.info(
            f"[WINDOW] {w.title}\n"
            f"  slug  : {w.slug}\n"
            f"  UP    : {w.tid_up[:24]}...\n"
            f"  DOWN  : {w.tid_down[:24]}...\n"
            f"  open  : {datetime.fromtimestamp(w.start_ts, tz=PKT).strftime('%H:%M:%S PKT')}\n"
            f"  close : {datetime.fromtimestamp(w.end_ts,   tz=PKT).strftime('%H:%M:%S PKT')}"
        )

    # ── Timing ─────────────────────────────────────────────────────────────────

    def _wait_for_open(self):
        w   = self.window
        now = time.time()

        # Pre-fetch books 10s before open
        pre_fetch_at = w.start_ts - 10
        if now < pre_fetch_at:
            wait = pre_fetch_at - now
            log.info(f"[WAIT] {wait:.0f}s until book pre-fetch")
            time.sleep(max(0, wait))

        log.info("[WAIT] Pre-fetching order books...")
        self.books.fetch_batch([w.tid_up, w.tid_down])

        now = time.time()
        if now < w.start_ts:
            wait = w.start_ts - now
            log.info(f"[WAIT] {wait:.1f}s until window opens — standing by")
            time.sleep(max(0, wait))

        log.info("[WAIT] Window is OPEN")

    # ── Leg 1 — momentum-selected passive GTC ─────────────────────────────────

    def _enter_leg1(self):
        w = self.window

        # Fresh books
        self.books.fetch_batch([w.tid_up, w.tid_down])
        up_book = self.books.get(w.tid_up)
        dn_book = self.books.get(w.tid_down)

        if not up_book or not dn_book:
            log.error("[LEG1] Book fetch failed — aborting")
            self._running = False
            return

        # Edge check (worst-case cost = ask on both sides)
        combined_ask = up_book.ba + dn_book.ba
        edge         = 1.0 - combined_ask

        log.info(
            f"[EDGE] UP ask={up_book.ba:.2f}  DN ask={dn_book.ba:.2f}  "
            f"combined={combined_ask:.2f}  edge={edge:+.4f}"
        )

        if edge < MIN_EDGE:
            log.warning(
                f"[LEG1] Edge {edge:.4f} < {MIN_EDGE} threshold — "
                f"spread too tight, skipping entry"
            )
            self._running = False
            return

        # ── Momentum → leg1 side ──────────────────────────────────────────
        direction, mag = cl_momentum(self.cl)

        if mag < 0.005:
            # Near-zero drift — pick cheaper ask side
            direction = "UP" if up_book.ba <= dn_book.ba else "DOWN"
            log.info(f"[LEG1] Flat momentum — cheapest side: {direction}")

        if direction == "UP":
            tid1, book1         = w.tid_up,   up_book
            self.leg1_side      = "UP"
            self.leg2_side      = "DOWN"
        else:
            tid1, book1         = w.tid_down, dn_book
            self.leg1_side      = "DOWN"
            self.leg2_side      = "UP"

        px1 = maker_price(book1)
        if px1 <= 0:
            log.error(f"[LEG1] Cannot compute entry price (book empty?) — aborting")
            self._running = False
            return

        log.info(
            f"[LEG1] Posting {self.leg1_side} @ ${px1:.2f}  |  "
            f"stake=${STAKE:.2f}  bb={book1.bb:.2f}  ba={book1.ba:.2f}"
        )

        oid = self.exec.buy_gtc(tid1, STAKE, px1)
        if not oid:
            log.error("[LEG1] Order placement failed")
            self._running = False
            return

        self.leg1_oid = oid
        self.leg1_px  = px1
        log.info(f"[LEG1] Order live: {oid}")

        self._wait_leg1_fill()

    def _wait_leg1_fill(self):
        w        = self.window
        oid      = self.leg1_oid
        deadline = w.end_ts - Config.CANCEL_ALL_LEFT
        last_log = 0.0

        while self._running and time.time() < deadline:
            filled, fill_px = self.exec.check_fill(oid)

            if filled:
                self.leg1_fill_px = fill_px if fill_px > 0 else self.leg1_px
                self.leg1_fill_ts = time.time()
                log.info(
                    f"[LEG1] FILLED {self.leg1_side} @ ${self.leg1_fill_px:.2f}  |  "
                    f"shares={STAKE / self.leg1_fill_px:.4f}  "
                    f"cost=${STAKE:.2f}"
                )
                return

            now = time.time()
            if now - last_log >= 10:
                left = w.end_ts - now
                log.info(
                    f"[LEG1] Waiting for fill... {left:.0f}s left  |  "
                    f"CL=${self.cl.get('btc') or 0:,.0f}"
                )
                last_log = now

            time.sleep(0.4)

        # Deadline hit — cancel and stop
        log.warning("[LEG1] No fill before deadline — cancelling order")
        self.exec.cancel_order(oid)
        self.leg1_oid = ""
        self._running = False

    # ── Leg 2 — passive → aggressive → FAK ────────────────────────────────────

    def _chase_leg2(self):
        w     = self.window
        side2 = self.leg2_side
        tid2  = w.tid_down if side2 == "DOWN" else w.tid_up

        log.info(f"[LEG2] Chasing {side2} leg  |  leg1 cost=${self.leg1_fill_px:.2f}")

        started = time.time()
        phase   = "PASSIVE"
        oid2    = ""

        while self._running:
            now     = time.time()
            elapsed = now - started
            left    = w.end_ts - now

            # ── Window closing guard ──────────────────────────────────────
            if left < Config.CANCEL_ALL_LEFT:
                log.warning(f"[LEG2] <{Config.CANCEL_ALL_LEFT}s left — bailing")
                if oid2:
                    self.exec.cancel_order(oid2)
                break

            # ── Phase transitions ─────────────────────────────────────────
            if elapsed >= FAK_SEC and phase != "FAK":
                log.info("[LEG2] → FAK phase")
                phase = "FAK"
                if oid2:
                    self.exec.cancel_order(oid2)
                    oid2 = ""

            elif elapsed >= AGGRESSIVE_SEC and phase == "PASSIVE":
                log.info("[LEG2] → AGGRESSIVE phase")
                phase = "AGGRESSIVE"
                if oid2:
                    self.exec.cancel_order(oid2)
                    oid2 = ""

            # ── Refresh book ──────────────────────────────────────────────
            self.books.fetch_batch([tid2])
            book2 = self.books.get(tid2)
            if not book2:
                time.sleep(1)
                continue

            # ─────────────────────────────────────────────────────────────
            # FAK: cross the spread to guarantee a fill
            # ─────────────────────────────────────────────────────────────
            if phase == "FAK":
                fak_px = round(min(book2.ba + 0.01, 0.99), 2)
                log.info(
                    f"[LEG2] FAK @ ${fak_px:.2f}  |  "
                    f"ask={book2.ba:.2f}  left={left:.0f}s"
                )
                oid2 = self.exec.buy_fak(tid2, STAKE, fak_px)
                if oid2:
                    filled, fill_px = self.exec.check_fill(oid2)
                    if filled:
                        self.leg2_fill_px = fill_px if fill_px > 0 else fak_px
                        log.info(f"[LEG2] FAK FILLED {side2} @ ${self.leg2_fill_px:.2f}")
                        return
                log.warning("[LEG2] FAK missed — window closing soon")
                break

            # ─────────────────────────────────────────────────────────────
            # AGGRESSIVE: walk price toward ask, stay above edge floor
            # ─────────────────────────────────────────────────────────────
            elif phase == "AGGRESSIVE":
                max_leg2 = round(1.0 - self.leg1_fill_px - MIN_EDGE_AGG, 2)
                # Walk toward best bid+3¢ but never past max_leg2
                agg_px = round(min(book2.bb + 0.03, max_leg2, book2.ba - 0.01), 2)
                agg_px = max(0.01, min(agg_px, 0.99))

                if agg_px < 0.01:
                    log.warning("[LEG2] Aggressive price below floor")
                    time.sleep(1)
                    continue

                if not oid2:
                    log.info(
                        f"[LEG2] AGGRESSIVE @ ${agg_px:.2f}  |  "
                        f"max=${max_leg2:.2f}  left={left:.0f}s"
                    )
                    oid2 = self.exec.buy_gtc(tid2, STAKE, agg_px)
                else:
                    filled, fill_px = self.exec.check_fill(oid2)
                    if filled:
                        self.leg2_fill_px = fill_px if fill_px > 0 else agg_px
                        log.info(f"[LEG2] AGGRESSIVE FILLED {side2} @ ${self.leg2_fill_px:.2f}")
                        return
                    # Reprice if spread moved >1¢
                    posted = self.exec._orders.get(oid2, {}).get("price", agg_px)
                    if abs(posted - agg_px) > Config.QUOTE_REPOST_THRESHOLD:
                        log.debug(f"[LEG2] Reprice {posted:.2f} → {agg_px:.2f}")
                        self.exec.cancel_order(oid2)
                        oid2 = ""

            # ─────────────────────────────────────────────────────────────
            # PASSIVE: rest inside spread, reprice if stale
            # ─────────────────────────────────────────────────────────────
            else:
                passive_px = maker_price(book2)
                if passive_px <= 0:
                    time.sleep(0.5)
                    continue

                if not oid2:
                    log.info(
                        f"[LEG2] PASSIVE @ ${passive_px:.2f}  |  "
                        f"bb={book2.bb:.2f}  ba={book2.ba:.2f}  left={left:.0f}s"
                    )
                    oid2 = self.exec.buy_gtc(tid2, STAKE, passive_px)
                else:
                    filled, fill_px = self.exec.check_fill(oid2)
                    if filled:
                        self.leg2_fill_px = fill_px if fill_px > 0 else passive_px
                        log.info(f"[LEG2] PASSIVE FILLED {side2} @ ${self.leg2_fill_px:.2f}")
                        return
                    posted = self.exec._orders.get(oid2, {}).get("price", passive_px)
                    if abs(posted - passive_px) > Config.QUOTE_REPOST_THRESHOLD:
                        log.debug(f"[LEG2] Reprice {posted:.2f} → {passive_px:.2f}")
                        self.exec.cancel_order(oid2)
                        oid2 = ""

            time.sleep(0.4)

        # Exited loop without fill → orphan
        if oid2:
            self.exec.cancel_order(oid2)
        log.warning(
            f"[LEG2] {side2} leg UNFILLED  |  "
            f"ORPHAN: holding {self.leg1_side} @ ${self.leg1_fill_px:.2f}"
        )

    # ── Output ─────────────────────────────────────────────────────────────────

    def _banner(self):
        mode = "PAPER" if self.paper else "LIVE"
        print("=" * 64)
        print(f"  BTC 15m WINDOW RUN — {mode}")
        print(f"  Target : {TARGET_START_PKT.strftime('%Y-%m-%d %H:%M PKT')} "
              f"({TARGET_START_PKT.astimezone(timezone.utc).strftime('%H:%M UTC')})")
        print(f"  Stake  : ${STAKE}/leg  |  Min edge: {MIN_EDGE*100:.0f}¢")
        print(f"  Momentum lookback: {MOM_LOOKBACK}s  |  Entry offset: {ENTRY_OFFSET*100:.0f}¢")
        print(f"  Leg2: passive {PASSIVE_SEC}s → aggressive {AGGRESSIVE_SEC}s → FAK {FAK_SEC}s")
        print("=" * 64)

    def _summary(self):
        w = self.window
        print()
        print("=" * 64)
        print("  TRADE SUMMARY")
        print("=" * 64)

        if w:
            print(f"  Window  : {w.title}")
            print(
                f"  Closed  : {datetime.fromtimestamp(w.end_ts, tz=PKT).strftime('%H:%M:%S PKT')}"
            )

        if self.leg1_fill_px > 0:
            print(f"  Leg 1   : {self.leg1_side:4s}  filled @ ${self.leg1_fill_px:.4f}")
        else:
            print(f"  Leg 1   : {self.leg1_side or '----'}  NO FILL")

        if self.leg2_fill_px > 0:
            combined = self.leg1_fill_px + self.leg2_fill_px
            pnl      = 1.0 - combined
            print(f"  Leg 2   : {self.leg2_side:4s}  filled @ ${self.leg2_fill_px:.4f}")
            print(f"  Combined: ${combined:.4f}")
            print(f"  PnL lock: ${pnl:+.4f}  "
                  f"({'profit' if pnl > 0 else 'LOSS'} on settlement)")
        else:
            if self.leg1_fill_px > 0:
                print(f"  Leg 2   : {self.leg2_side or '----'}  NO FILL — ORPHAN")
                print(f"  Holding : {self.leg1_side} @ ${self.leg1_fill_px:.4f}  "
                      f"(cost ${STAKE:.2f})")
            else:
                print("  Result  : No position taken")

        print("=" * 64)

    # ── Cleanup ────────────────────────────────────────────────────────────────

    def _stop(self):
        log.info("[RUNNER] Signal received — stopping")
        self._running = False

    def _cleanup(self):
        log.info("[CLEANUP] Cancelling all open orders...")
        self.exec.cancel_all()
        time.sleep(0.5)
        self.bn.stop()
        self.cl.stop()
        POOL.shutdown(wait=True)
        log.info("[CLEANUP] Done")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    paper = "--paper" in sys.argv

    if not paper:
        # Verify credentials before starting
        missing = []
        if not os.getenv("POLY_PRIVATE_KEY"):
            missing.append("POLY_PRIVATE_KEY")
        if not os.getenv("POLY_FUNDER_ADDRESS"):
            missing.append("POLY_FUNDER_ADDRESS")
        if missing:
            print("ERROR: Missing required environment variables:")
            for m in missing:
                print(f"  export {m}=...")
            print()
            print("Or run in paper mode:")
            print("  python run_15m.py --paper")
            sys.exit(1)

    WindowRunner(paper=paper).run()


if __name__ == "__main__":
    main()
