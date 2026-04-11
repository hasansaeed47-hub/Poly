#!/usr/bin/env python3
"""
DSA v1.0 — Dual-Sided Accumulation Bot
=======================================
profit/pair = $1.00 - (vwap_up + vwap_dn), realised at settlement.
State updated only on confirmed WS fills — never on order post.

Decision stack (every tick):
  1. SAFETY      — session limits
  2. IMBALANCE   — headroom = MAX_IMBALANCE - imbalance; 0 → frozen
  3. PROV CHUNK  — floor(min(BASE_CHUNK, headroom))
  4. FEASIBILITY — proj combined ≤ target_net (bootstrap-safe)
  5. REGIME      — final = floor(prov × range_scaler); < 1 → skip
  6. EXECUTE     — post GTC limit, register oid for fill routing
"""
from __future__ import annotations
import asyncio, logging, math, os, signal, sys, threading, time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Deque, Dict, List, Optional, Set, Tuple

try:
    import uvloop; asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

import aiohttp

try:
    import orjson
    _loads = orjson.loads
    def _dumps(o): return orjson.dumps(o).decode()
except ImportError:
    import json
    _loads, _dumps = json.loads, json.dumps

from infra import Config as IC, ExecutionLayer, BinanceFeed, ChainlinkFeed, HeartbeatThread, slug_ts, slug_wmin

log = logging.getLogger("DSA")
log.setLevel(logging.DEBUG)
_h = logging.StreamHandler()
_h.setFormatter(logging.Formatter("%(asctime)s.%(msecs)03d DSA %(levelname)s %(message)s", "%H:%M:%S"))
log.addHandler(_h)


# =============================================================================
# CONFIG
# =============================================================================
class Cfg:
    TARGET_HIGH, TARGET_MID, TARGET_LOW = 0.95, 0.96, 0.97
    MIN_PROFIT        = 0.025
    BASE_CHUNK        = 5
    MAX_IMBALANCE     = 5
    MIN_NOTIONAL      = 2.00
    MIN_VOL_ANCHOR    = 30
    PHASE_ESCALATE    = 0.70
    PHASE_CLEANUP     = 0.90
    FLOOR_ACC, CEIL_ACC = 0.25, 0.75
    FLOOR_ESC, CEIL_ESC = 0.20, 0.80
    MIN_OSC           = 0.04
    MAX_SPREAD        = 1.03
    MIN_DEPTH         = 125
    REGIME_TICKS      = 10
    REGIME_FULL       = 0.06
    REGIME_PAUSE      = 0.01
    MAX_VELOCITY      = 0.04
    MAX_BOOK_AGE      = 3.0
    CLEANUP_SLIP      = 0.03
    CLEANUP_T_LIMIT   = 30
    CLEANUP_T_MARKET  = 10
    PENDING_TIMEOUT   = 90
    SESSION_RESIDUAL  = 50
    MAX_DAILY_LOSS    = 1_000.0
    MAX_CONSEC_LOSS   = 3
    PAUSE_SECS        = 1_800
    CLOB_WS  = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    USER_WS  = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    SCAN_SEC = 60
    ASSETS, TIMEFRAMES = {"btc", "eth", "sol"}, {5, 15}


# =============================================================================
# STATE
# =============================================================================
class Phase(Enum):
    ACCUMULATE = "acc"
    ESCALATE   = "esc"
    CLEANUP    = "cln"
    DONE       = "done"


@dataclass
class Side:
    confirmed_qty:  float = 0.0
    confirmed_cost: float = 0.0
    _pending: Dict[str, Tuple[float, float, float]] = field(default_factory=dict)

    @property
    def vwap(self) -> float:
        return self.confirmed_cost / self.confirmed_qty if self.confirmed_qty > 0 else 0.0

    def add_pending(self, oid: str, price: float, shares: float):
        self._pending[oid] = (price, shares, time.time())

    def on_confirm(self, oid: str, px: float, qty: float):
        self._pending.pop(oid, None)
        self.confirmed_qty  += qty
        self.confirmed_cost += px * qty

    def on_cancel(self, oid: str):
        self._pending.pop(oid, None)

    def expire_pending(self):
        now = time.time()
        for oid in [o for o, (_, _, ts) in self._pending.items()
                    if now - ts > Cfg.PENDING_TIMEOUT]:
            log.debug(f"[SIDE] expire pending {oid[:12]}")
            del self._pending[oid]


@dataclass
class WindowState:
    market_id:  str
    tid_up:     str
    tid_dn:     str
    open_ts:    float
    close_ts:   float
    target_net: float
    wmin:       int
    up: Side = field(default_factory=Side)
    dn: Side = field(default_factory=Side)
    cleanup_t30_fired: bool = False
    cleanup_t10_fired: bool = False
    cleanup_oid_up:    Optional[str] = None
    cleanup_oid_dn:    Optional[str] = None
    up_asks: Deque[float] = field(default_factory=lambda: deque(maxlen=20))
    dn_asks: Deque[float] = field(default_factory=lambda: deque(maxlen=20))

    @property
    def paired(self) -> float:
        return min(self.up.confirmed_qty, self.dn.confirmed_qty)

    @property
    def imbalance(self) -> float:
        return abs(self.up.confirmed_qty - self.dn.confirmed_qty)

    @property
    def combined(self) -> float:
        return self.up.vwap + self.dn.vwap

    @property
    def time_left(self) -> float:
        return max(0.0, self.close_ts - time.time())

    @property
    def elapsed(self) -> float:
        span = self.close_ts - self.open_ts
        return min(1.0, (time.time() - self.open_ts) / span) if span > 0 else 1.0

    @property
    def phase(self) -> Phase:
        e = self.elapsed
        if e >= 1.0:                return Phase.DONE
        if e >= Cfg.PHASE_CLEANUP:  return Phase.CLEANUP
        if e >= Cfg.PHASE_ESCALATE: return Phase.ESCALATE
        return Phase.ACCUMULATE


# =============================================================================
# ENGINE
# =============================================================================
class Engine:
    def __init__(self, st: WindowState, exec_layer: ExecutionLayer,
                 loop: asyncio.AbstractEventLoop, paper: bool,
                 register_oid: Callable[[str, str, str], None]):
        self.st           = st
        self._exec        = exec_layer
        self._loop        = loop
        self._paper       = paper
        self._register    = register_oid   # (oid, market_id, "up"|"dn")
        self._lock        = asyncio.Lock()

    # ── Book helpers ──
    @staticmethod
    def _ask(b: Optional[dict]) -> float:
        asks = (b or {}).get("asks", [])
        return min(float(a["price"]) for a in asks) if asks else 1.0

    @staticmethod
    def _bid(b: Optional[dict]) -> float:
        bids = (b or {}).get("bids", [])
        return max(float(b["price"]) for b in bids) if bids else 0.0

    @staticmethod
    def _mid(b: Optional[dict]) -> float:
        asks = (b or {}).get("asks", [])
        bids = (b or {}).get("bids", [])
        ba = min(float(a["price"]) for a in asks) if asks else 1.0
        bb = max(float(x["price"]) for x in bids) if bids else 0.0
        return (ba + bb) / 2.0 if bb > 0 else 0.5

    @staticmethod
    def _depth(b: Optional[dict], max_px: float) -> float:
        return sum(float(a["size"]) for a in (b or {}).get("asks", [])
                   if float(a["price"]) <= max_px + 0.005)

    @staticmethod
    def _age(b: Optional[dict]) -> float:
        return time.time() - (b or {}).get("_ts", 0.0)

    # ── Budget ceiling ──
    def _max_px(self, me: Side, other: Side, other_mid: float) -> float:
        ref = other.vwap if other.confirmed_qty >= Cfg.MIN_VOL_ANCHOR else other_mid
        ceil = self.st.target_net - ref
        f, c = (Cfg.FLOOR_ACC, Cfg.CEIL_ACC) if self.st.phase == Phase.ACCUMULATE \
               else (Cfg.FLOOR_ESC, Cfg.CEIL_ESC)
        return round(min(max(ceil, f), c), 2)

    # ── Regime scaler ──
    @staticmethod
    def _scaler(hist: Deque[float]) -> float:
        if len(hist) < 3:
            return 0.5
        recent = list(hist)[-Cfg.REGIME_TICKS:]
        rng = max(recent) - min(recent)
        if len(recent) >= 5 and abs(recent[-1] - recent[0]) > Cfg.MAX_VELOCITY:
            return 0.0
        if rng < Cfg.REGIME_PAUSE:
            return 0.0
        return min(1.0, rng / Cfg.REGIME_FULL)

    # ── Sync execution (run in executor) ──
    def _post_buy(self, tid: str, price: float, shares: float) -> Optional[str]:
        return self._exec.buy_gtc(tid, shares * price, price)

    def _post_sell(self, tid: str, price: float, shares: float) -> Optional[str]:
        return self._exec.sell_gtc(tid, shares, price)

    def _cancel(self, oid: str):
        self._exec.cancel_order(oid)

    # ── Main tick ──
    async def tick(self, up_book: Optional[dict], dn_book: Optional[dict]):
        async with self._lock:
            st = self.st
            phase = st.phase

            if phase == Phase.DONE:
                return
            if phase == Phase.CLEANUP:
                await self._cleanup(up_book, dn_book)
                return

            # Book freshness
            if self._age(up_book) > Cfg.MAX_BOOK_AGE or self._age(dn_book) > Cfg.MAX_BOOK_AGE:
                return

            up_ask = self._ask(up_book); dn_ask = self._ask(dn_book)
            up_mid = self._mid(up_book); dn_mid = self._mid(dn_book)
            st.up_asks.append(up_ask);   st.dn_asks.append(dn_ask)

            # Step 2: IMBALANCE → headroom
            up_head = Cfg.MAX_IMBALANCE - max(0.0, st.up.confirmed_qty - st.dn.confirmed_qty)
            dn_head = Cfg.MAX_IMBALANCE - max(0.0, st.dn.confirmed_qty - st.up.confirmed_qty)
            if phase == Phase.ESCALATE:
                if st.up.confirmed_qty >= st.dn.confirmed_qty: up_head = 0
                if st.dn.confirmed_qty >= st.up.confirmed_qty: dn_head = 0

            # Step 3: PROVISIONAL CHUNK
            prov_up = math.floor(min(Cfg.BASE_CHUNK, up_head))
            prov_dn = math.floor(min(Cfg.BASE_CHUNK, dn_head))

            # Step 4: FEASIBILITY
            up_max = self._max_px(st.up, st.dn, dn_mid)
            dn_max = self._max_px(st.dn, st.up, up_mid)
            dn_ref = st.dn.vwap if st.dn.confirmed_qty >= Cfg.MIN_VOL_ANCHOR else dn_mid
            up_ref = st.up.vwap if st.up.confirmed_qty >= Cfg.MIN_VOL_ANCHOR else up_mid

            band_f = Cfg.FLOOR_ACC if phase == Phase.ACCUMULATE else Cfg.FLOOR_ESC
            band_c = Cfg.CEIL_ACC  if phase == Phase.ACCUMULATE else Cfg.CEIL_ESC

            def _proj(side: Side, px: float, qty: float, other_ref: float) -> float:
                return (side.confirmed_cost + px * qty) / (side.confirmed_qty + qty) + other_ref

            buy_up = (prov_up >= 1
                      and band_f <= up_ask <= band_c
                      and up_ask <= up_max
                      and _proj(st.up, up_max, prov_up, dn_ref) <= st.target_net
                      and self._depth(up_book, up_max) >= prov_up)

            buy_dn = (prov_dn >= 1
                      and band_f <= dn_ask <= band_c
                      and dn_ask <= dn_max
                      and _proj(st.dn, dn_max, prov_dn, up_ref) <= st.target_net
                      and self._depth(dn_book, dn_max) >= prov_dn)

            if not buy_up and not buy_dn:
                return

            # Step 5: REGIME
            up_sc = self._scaler(st.up_asks)
            dn_sc = self._scaler(st.dn_asks)
            final_up = math.floor(prov_up * up_sc) if buy_up else 0
            final_dn = math.floor(prov_dn * dn_sc) if buy_dn else 0
            if buy_up and (final_up < 1 or final_up * up_max < Cfg.MIN_NOTIONAL): buy_up = False
            if buy_dn and (final_dn < 1 or final_dn * dn_max < Cfg.MIN_NOTIONAL): buy_dn = False
            if not buy_up and not buy_dn:
                return

            # Step 6: EXECUTE — both legs in parallel
            tasks, meta = [], []
            if buy_up:
                tasks.append(self._loop.run_in_executor(None, self._post_buy, st.tid_up, up_max, final_up))
                meta.append(("up", up_max, final_up))
            if buy_dn:
                tasks.append(self._loop.run_in_executor(None, self._post_buy, st.tid_dn, dn_max, final_dn))
                meta.append(("dn", dn_max, final_dn))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for (side, px, qty), res in zip(meta, results):
                if isinstance(res, Exception) or not res:
                    continue
                oid = res
                (st.up if side == "up" else st.dn).add_pending(oid, px, qty)
                self._register(oid, st.market_id, side)
                log.debug(f"[{st.market_id}] {side} posted {oid[:12]} @{px} x{qty}")

            log.info(f"[{st.market_id}] {phase.value} "
                     f"up={st.up.confirmed_qty:.0f}@{st.up.vwap:.3f} "
                     f"dn={st.dn.confirmed_qty:.0f}@{st.dn.vwap:.3f} "
                     f"paired={st.paired:.0f} comb={st.combined:.3f}")

    # ── Cleanup ──
    async def _cleanup(self, up_book: Optional[dict], dn_book: Optional[dict]):
        st      = self.st
        t_left  = st.time_left
        paired  = st.paired
        exc_up  = max(0.0, st.up.confirmed_qty - paired)
        exc_dn  = max(0.0, st.dn.confirmed_qty - paired)

        if paired > 0 and not st.cleanup_t30_fired:
            log.info(f"[{st.market_id}] LOCKED {paired:.0f} pairs "
                     f"@ {st.combined:.3f} → +${paired*(1.0-st.combined):.2f}")

        # T-30: post limit sell on excess only
        if t_left <= Cfg.CLEANUP_T_LIMIT and not st.cleanup_t30_fired:
            st.cleanup_t30_fired = True
            for exc, tid, side_st, attr in [
                (exc_up, st.tid_up, st.up, "cleanup_oid_up"),
                (exc_dn, st.tid_dn, st.dn, "cleanup_oid_dn"),
            ]:
                if exc < 1: continue
                lim = round(side_st.vwap - Cfg.CLEANUP_SLIP, 2)
                if lim <= 0: continue
                oid = await self._loop.run_in_executor(None, self._post_sell, tid, lim, exc)
                if oid: setattr(st, attr, oid)
                log.info(f"[{st.market_id}] T-30 sell {exc:.0f} @{lim:.2f}")

        # T-10: cancel limit, market sell excess
        if t_left <= Cfg.CLEANUP_T_MARKET and not st.cleanup_t10_fired:
            st.cleanup_t10_fired = True
            for exc, tid, attr, book in [
                (exc_up, st.tid_up, "cleanup_oid_up", up_book),
                (exc_dn, st.tid_dn, "cleanup_oid_dn", dn_book),
            ]:
                if exc < 1: continue
                lim_oid = getattr(st, attr)
                if lim_oid:
                    await self._loop.run_in_executor(None, self._cancel, lim_oid)
                best_bid = self._bid(book)
                if best_bid > 0:
                    await self._loop.run_in_executor(None, self._post_sell, tid, best_bid, exc)
                    log.info(f"[{st.market_id}] T-10 market sell {exc:.0f} @{best_bid:.2f}")


# =============================================================================
# RISK
# =============================================================================
class Risk:
    def __init__(self):
        self.daily_loss = 0.0; self.consec = 0; self._pause_until = 0.0
        self.residual: Dict[str, float] = {"up": 0.0, "dn": 0.0}

    def paused(self) -> bool: return time.time() < self._pause_until

    def can_enter(self) -> Tuple[bool, str]:
        if self.paused():                         return False, "paused"
        if self.daily_loss >= Cfg.MAX_DAILY_LOSS: return False, "daily_loss"
        return True, "ok"

    def residual_ok(self, side: str, qty: float) -> bool:
        return self.residual.get(side, 0) + qty <= Cfg.SESSION_RESIDUAL

    def record(self, pnl: float, r_up: float = 0, r_dn: float = 0):
        self.residual["up"] += r_up; self.residual["dn"] += r_dn
        if pnl < 0:
            self.daily_loss += abs(pnl); self.consec += 1
            if self.consec >= Cfg.MAX_CONSEC_LOSS:
                self._pause_until = time.time() + Cfg.PAUSE_SECS
                log.warning(f"[RISK] {self.consec} losses → 30m pause")
        else:
            self.consec = 0


# =============================================================================
# ENTRY FILTER
# =============================================================================
class EntryFilter:
    def __init__(self):
        self._hist: Dict[str, deque] = {}

    def record(self, mid: str, up_ask: float):
        if mid not in self._hist: self._hist[mid] = deque(maxlen=200)
        self._hist[mid].append((time.time(), up_ask))

    def osc(self, mid: str, secs: float = 900) -> float:
        h = self._hist.get(mid, deque()); cutoff = time.time() - secs
        r = [p for ts, p in h if ts >= cutoff]
        return (max(r) - min(r)) if len(r) >= 2 else 0.0

    def target_for(self, mid: str) -> float:
        o = self.osc(mid)
        return Cfg.TARGET_HIGH if o > 0.10 else Cfg.TARGET_MID if o > 0.06 else Cfg.TARGET_LOW

    def check(self, mid: str, up_ask: float, dn_ask: float,
              up_depth: float, dn_depth: float) -> Tuple[bool, str]:
        if not (Cfg.FLOOR_ACC <= up_ask <= Cfg.CEIL_ACC): return False, f"up {up_ask:.2f}"
        if not (Cfg.FLOOR_ACC <= dn_ask <= Cfg.CEIL_ACC): return False, f"dn {dn_ask:.2f}"
        if up_ask + dn_ask > Cfg.MAX_SPREAD:              return False, f"spread {up_ask+dn_ask:.3f}"
        if self.osc(mid) < Cfg.MIN_OSC:                   return False, f"osc {self.osc(mid):.3f}"
        if up_depth < Cfg.MIN_DEPTH:                       return False, f"up_depth {up_depth:.0f}"
        if dn_depth < Cfg.MIN_DEPTH:                       return False, f"dn_depth {dn_depth:.0f}"
        return True, "ok"


# =============================================================================
# BOT
# =============================================================================
class DsaBot:
    def __init__(self, paper: bool = True):
        self.paper          = paper
        self._engines:      Dict[str, Engine] = {}
        self._tid_to_mid:   Dict[str, str]    = {}
        self._oid_to_key:   Dict[str, Tuple[str, str]] = {}
        self._subscribed:   Set[str] = set()
        self._filter        = EntryFilter()
        self._risk          = Risk()
        self._exec:         Optional[ExecutionLayer]   = None
        self._bn:           Optional[BinanceFeed]      = None
        self._cl:           Optional[ChainlinkFeed]    = None
        self._hb:           Optional[HeartbeatThread]  = None
        self._sess:         Optional[aiohttp.ClientSession] = None
        self._ws_books_conn: Optional[aiohttp.ClientWebSocketResponse] = None
        self._loop:         Optional[asyncio.AbstractEventLoop] = None
        self._running       = True
        self._api_key = self._api_secret = self._api_pass = ""

    def _register_oid(self, oid: str, market_id: str, side: str):
        self._oid_to_key[oid] = (market_id, side)

    # ── Start ──
    async def _start(self):
        self._loop = asyncio.get_running_loop()
        conn = aiohttp.TCPConnector(limit=30, keepalive_timeout=60)
        self._sess = aiohttp.ClientSession(connector=conn)

        self._bn = BinanceFeed(); self._bn.start()
        self._cl = ChainlinkFeed(); self._cl.set_bn_fallback(self._bn); self._cl.start()

        self._exec = ExecutionLayer(paper=self.paper)
        if not self.paper:
            creds = await self._loop.run_in_executor(None, self._exec.init_live)
            if creds:
                self._api_key  = getattr(creds, "api_key", "")
                self._api_secret = getattr(creds, "api_secret", "")
                self._api_pass = getattr(creds, "api_passphrase", "")

        self._hb = HeartbeatThread(self._exec); self._hb.start()

        asyncio.create_task(self._scan_loop(),        name="scan")
        asyncio.create_task(self._ws_books_loop(),    name="ws_books")
        asyncio.create_task(self._ws_user_loop(),     name="ws_user")
        asyncio.create_task(self._maintenance_loop(), name="maint")
        log.info(f"[DSA] started ({'PAPER' if self.paper else 'LIVE'})")

    # ── Scan ──
    async def _scan_loop(self):
        while self._running:
            try: await self._scan()
            except Exception as e: log.error(f"[SCAN] {e}")
            await asyncio.sleep(Cfg.SCAN_SEC)

    async def _scan(self):
        try:
            async with self._sess.get(f"{IC.GAMMA}/markets",
                params={"active": "true", "closed": "false", "limit": "300"},
                timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200: return
                raw = await r.json(content_type=None)
        except Exception as e:
            log.debug(f"[SCAN] {e}"); return

        markets = raw if isinstance(raw, list) else raw.get("data", [])
        now = time.time()

        for m in markets:
            slug  = m.get("slug", "")
            asset = next((a for a in Cfg.ASSETS if f"{a}-" in slug.lower()), None)
            if not asset: continue
            wmin = slug_wmin(slug)
            if wmin not in Cfg.TIMEFRAMES: continue

            tokens = m.get("clobTokenIds") or m.get("tokens") or []
            if len(tokens) < 2: continue
            def _tid(t): return t if isinstance(t, str) else t.get("token_id", t.get("id", ""))
            tid_up, tid_dn = _tid(tokens[0]), _tid(tokens[1])
            if not tid_up or not tid_dn: continue

            end_ts = slug_ts(slug)
            if not end_ts:
                end_str = m.get("endDateIso") or m.get("end_date_iso", "")
                try:
                    from datetime import datetime
                    end_ts = int(datetime.fromisoformat(end_str.replace("Z","+00:00")).timestamp())
                except Exception: continue

            if not end_ts or end_ts <= now: continue

            market_id = m.get("id") or m.get("conditionId") or slug
            if market_id in self._engines: continue

            ok, reason = self._risk.can_enter()
            if not ok: continue

            st = WindowState(
                market_id=market_id, tid_up=tid_up, tid_dn=tid_dn,
                open_ts=float(end_ts - wmin * 60), close_ts=float(end_ts),
                target_net=Cfg.TARGET_MID, wmin=wmin,
            )
            eng = Engine(st, self._exec, self._loop, self.paper, self._register_oid)
            self._engines[market_id] = eng
            self._tid_to_mid[tid_up] = market_id
            self._tid_to_mid[tid_dn] = market_id
            log.info(f"[SCAN] +{slug} closes in {end_ts-now:.0f}s")

        # Subscribe new tids
        new_tids = set(self._tid_to_mid) - self._subscribed
        if new_tids and self._ws_books_conn:
            try:
                await self._ws_books_conn.send_str(_dumps({"assets_ids": list(new_tids), "type": "market"}))
                self._subscribed.update(new_tids)
            except Exception as e:
                log.debug(f"[SCAN] ws sub: {e}")

    # ── Book WS ──
    async def _ws_books_loop(self):
        while self._running:
            try:
                async with self._sess.ws_connect(Cfg.CLOB_WS, heartbeat=20) as ws:
                    self._ws_books_conn = ws
                    log.info("[WS:BOOKS] connected")
                    if self._tid_to_mid:
                        await ws.send_str(_dumps({"assets_ids": list(self._tid_to_mid), "type": "market"}))
                        self._subscribed = set(self._tid_to_mid)
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            asyncio.create_task(self._on_book(msg.data))
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break
            except Exception as e: log.warning(f"[WS:BOOKS] {e}")
            finally: self._ws_books_conn = None
            if self._running: await asyncio.sleep(2)

    async def _on_book(self, raw: str):
        try:
            d   = _loads(raw)
            tid = d.get("asset_id", "")
            if not tid: return
            mid = self._tid_to_mid.get(tid)
            if not mid: return
            eng = self._engines.get(mid)
            if not eng: return
            if eng.st.phase == Phase.DONE:
                self._drop(mid); return

            # WS is trigger — fetch both books in parallel
            up_book, dn_book = await asyncio.gather(
                self._fetch_book(eng.st.tid_up),
                self._fetch_book(eng.st.tid_dn),
            )
            if not up_book or not dn_book: return

            up_ask = Engine._ask(up_book)
            self._filter.record(mid, up_ask)

            # First-entry gate
            if eng.st.up.confirmed_qty == 0 and eng.st.dn.confirmed_qty == 0:
                dn_ask = Engine._ask(dn_book)
                ok, reason = self._filter.check(
                    mid, up_ask, dn_ask,
                    Engine._depth(up_book, up_ask + 0.05),
                    Engine._depth(dn_book, dn_ask + 0.05),
                )
                if not ok: return
                eng.st.target_net = self._filter.target_for(mid)
                log.info(f"[ENTRY] {mid} target={eng.st.target_net}")

            ok, _ = self._risk.can_enter()
            if not ok and eng.st.phase == Phase.ACCUMULATE: return

            asyncio.create_task(eng.tick(up_book, dn_book))
        except Exception as e:
            log.debug(f"[ON_BOOK] {e}")

    async def _fetch_book(self, tid: str) -> Optional[dict]:
        try:
            async with self._sess.get(f"{IC.CLOB}/book", params={"token_id": tid},
                                      timeout=aiohttp.ClientTimeout(total=3)) as r:
                if r.status == 200:
                    d = await r.json(content_type=None)
                    d["_ts"] = time.time()
                    return d
        except Exception as e:
            log.debug(f"[BOOK] {tid[:12]}: {e}")
        return None

    # ── User WS (fill confirms) ──
    async def _ws_user_loop(self):
        if self.paper:
            await self._paper_fills(); return

        while self._running:
            try:
                async with self._sess.ws_connect(Cfg.USER_WS, heartbeat=20) as ws:
                    log.info("[WS:USER] connected")
                    await ws.send_str(_dumps({"type": "user", "auth": {
                        "apiKey": self._api_key,
                        "secret": self._api_secret,
                        "passphrase": self._api_pass,
                    }}))
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self._on_fill(msg.data)
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break
            except Exception as e: log.warning(f"[WS:USER] {e}")
            if self._running: await asyncio.sleep(2)

    def _on_fill(self, raw: str):
        try:
            d     = _loads(raw)
            event = d.get("event_type", d.get("type", ""))
            if event not in ("order_matched", "trade", "fill"): return
            oid   = d.get("order_id", d.get("orderId", ""))
            px    = float(d.get("price", d.get("fill_price", 0)) or 0)
            qty   = float(d.get("size", d.get("fill_size", 0)) or 0)
            if not oid or qty <= 0: return
            key = self._oid_to_key.pop(oid, None)
            if not key: return
            mid, side = key
            eng = self._engines.get(mid)
            if not eng: return
            target = eng.st.up if side == "up" else eng.st.dn
            target.on_confirm(oid, px, qty)
            log.info(f"[FILL] {mid} {side} @{px:.3f} x{qty:.1f} "
                     f"confirmed={target.confirmed_qty:.0f}")
        except Exception as e:
            log.debug(f"[ON_FILL] {e}")

    async def _paper_fills(self):
        while self._running:
            await asyncio.sleep(2)
            for eng in list(self._engines.values()):
                for side_char, side in [("up", eng.st.up), ("dn", eng.st.dn)]:
                    for oid, (px, qty, ts) in list(side._pending.items()):
                        if time.time() - ts >= 2.0:
                            side.on_confirm(oid, px, qty)
                            self._oid_to_key.pop(oid, None)
                            log.debug(f"[PAPER] fill {eng.st.market_id} {side_char} @{px} x{qty}")

    # ── Maintenance ──
    async def _maintenance_loop(self):
        while self._running:
            await asyncio.sleep(10)
            for mid in list(self._engines):
                eng = self._engines[mid]; st = eng.st
                st.up.expire_pending(); st.dn.expire_pending()
                if st.phase == Phase.DONE or st.time_left <= 0:
                    pnl   = st.paired * (1.0 - st.combined) if st.paired > 0 else 0.0
                    r_up  = max(0.0, st.up.confirmed_qty - st.paired)
                    r_dn  = max(0.0, st.dn.confirmed_qty - st.paired)
                    self._risk.record(pnl, r_up, r_dn)
                    log.info(f"[CLOSE] {mid} pairs={st.paired:.0f} pnl=${pnl:.3f} "
                             f"residual=({r_up:.0f}up {r_dn:.0f}dn)")
                    self._drop(mid)

    def _drop(self, mid: str):
        eng = self._engines.pop(mid, None)
        if eng:
            self._tid_to_mid.pop(eng.st.tid_up, None)
            self._tid_to_mid.pop(eng.st.tid_dn, None)
            self._subscribed.discard(eng.st.tid_up)
            self._subscribed.discard(eng.st.tid_dn)

    # ── Stop ──
    async def _stop(self):
        self._running = False
        if self._hb:   self._hb.stop()
        if self._bn:   self._bn.stop()
        if self._cl:   self._cl.stop()
        if self._exec: self._exec.cancel_all()
        if self._sess: await self._sess.close()
        log.info("[DSA] stopped")

    def run(self):
        async def _main():
            await self._start()
            ev = asyncio.Event()
            for sig in (signal.SIGINT, signal.SIGTERM):
                asyncio.get_running_loop().add_signal_handler(sig, ev.set)
            await ev.wait()
            await self._stop()
        asyncio.run(_main())


if __name__ == "__main__":
    DsaBot(paper="--live" not in sys.argv).run()
