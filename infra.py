#!/usr/bin/env python3
"""
POLYMARKET v4.1 — INFRA LAYER
===============================
Config, models, feeds, books, execution, heartbeat, user WS.

v4.0 base + targeted upgrades:
  CL:  Chainlink RTDS WebSocket primary, Binance fallback (CryptoCompare stripped)
       Snap dict for second-level precision at() lookups
  EX1: Consecutive failure backoff (from v3.7 — prevents API ban)
  EX2: Post-only cross detection on buy_gtc (from v3.8 — prevents taker fees)

Preserved from v4.0:
  B3:  FOK → FAK order type (partial fills OK)
  B4:  Native merge/redeem endpoint ($1.00, not sell @$0.99)
  B5:  WS cancel cleans order tracking + is_order_live() helper
  M1:  HMAC signature format: method\npath\nbody\ntimestamp
  M2:  WS auth format verified
  M3:  Heartbeat fallback uses correct sig format
  M4:  Paper fill delay configurable
  A3:  Binance stale-price protection (BN_STALE_MAX_SEC)
  A4:  Edge floor raised 0.5¢ → 1.0¢
  S1:  Batch HTTP book fetches via persistent pool
  S6:  Persistent ThreadPoolExecutor (shared, no per-tick creation)
  S7:  orjson preferred (single parse per response)
"""

# ─── JSON (S7: orjson preferred) ─────────────────────────────────────────────

try:
    import orjson as _orjson

    class json:
        @staticmethod
        def loads(s):
            return _orjson.loads(s)

        @staticmethod
        def dumps(o, **kw):
            return _orjson.dumps(o).decode()

        @staticmethod
        def load(f):
            return _orjson.loads(f.read())

        @staticmethod
        def dump(o, f, **kw):
            f.write(_orjson.dumps(o, option=_orjson.OPT_INDENT_2).decode())
except ImportError:
    import json

# ─── STDLIB ───────────────────────────────────────────────────────────────────

import time, sys, io, os, threading, logging, hmac, hashlib, base64
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple, Deque, Set
from enum import Enum

try:
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace"
    )
except Exception:
    pass

import requests

_SESSION = requests.Session()
_SESSION.headers.update({"Connection": "keep-alive"})
_adapter = requests.adapters.HTTPAdapter(
    pool_connections=10, pool_maxsize=20, max_retries=2
)
_SESSION.mount("https://", _adapter)
_SESSION.mount("http://", _adapter)

try:
    import websocket as ws_lib

    HAS_WS = True
except ImportError:
    HAS_WS = False

# S6: Persistent thread pool — shared across all tick cycles, never recreated
POOL = ThreadPoolExecutor(max_workers=8)


# =============================================================================
# UTILITY
# =============================================================================


def slug_ts(slug: str) -> Optional[int]:
    """Extract unix timestamp from slug like 'btc-updown-15m-1234567890'."""
    parts = slug.rsplit("-", 1)
    if len(parts) == 2:
        try:
            return int(parts[1])
        except ValueError:
            pass
    return None


def slug_wmin(slug: str) -> int:
    """Extract window minutes from slug like 'btc-updown-15m-...'."""
    for p in slug.split("-"):
        if p.endswith("m") and p[:-1].isdigit():
            return int(p[:-1])
    return 15


# =============================================================================
# CONFIG
# =============================================================================


class Config:
    # ── QUOTING ──
    QUOTE_STAKE = 1.0               # $ per quote order
    QUOTE_EDGE_MIN = 0.01           # A4: raised 0.005 → 0.01 (1¢ floor)
    QUOTE_EDGE_MAX = 0.04           # 4¢ ceiling
    QUOTE_EDGE_DEFAULT = 0.02       # default when no spread data
    QUOTE_REPOST_THRESHOLD = 0.01   # repost if stale by >1¢
    QUOTE_INTERVAL_MS = 400         # ms between quote updates
    QUOTE_MAX_PER_SIDE = 10.0       # max $ accumulated per side per window

    # ── SEQUENTIAL PAIR ENTRY ──
    COMPLETION_PASSIVE_SEC = 10     # passive after leg1 fill
    COMPLETION_AGGRESSIVE_SEC = 30  # walk up to min-edge
    COMPLETION_FAK_SEC = 45         # switch to FAK (cross spread)
    COMPLETION_MIN_EDGE = 0.01      # min edge when aggressive (1¢)

    # ── BALANCE ──
    MAX_IMBALANCE = 0.25
    MAX_ENTRIES_PER_WINDOW = 20

    # ── PRICING ──
    MIN_TICK = 0.01                 # Polymarket minimum price increment

    # ── CROSS-WINDOW INTELLIGENCE ──
    XW_WEIGHTS = [0.5, 0.3, 0.2, 0.1, 0.05]  # recency weights for last 5 windows
    XW_NOISE_FLOOR = 0.05           # min weighted move% to trigger any bias
    XW_MAX_STRENGTH = 0.3           # max bias influence on leg1 selection
    XW_NORMALIZER = 0.5             # move% that maps to full strength
    XW_MIN_APPLY = 0.1              # min strength to actually apply bias

    # ── DYNAMIC EDGE ──
    VOL_PREMIUM_FACTOR = 0.5        # BN 1-min move contribution to edge calc
    EDGE_SPREAD_FRACTION = 0.25     # target edge = this fraction of avg spread

    # ── ORPHAN HEDGING ──
    HEDGE_MIN_RECOVERY = 0.5        # sell orphan only if recovering >50% of cost
    ESCALATION_COOLDOWN_SEC = 5.0   # B6: min seconds between leg2 escalations

    # ── WINDOWS ──
    ASSETS = {"btc", "eth", "sol"}
    TIMEFRAMES = {5, 15}
    STOP_QUOTING_LEFT = 30          # stop new quotes when <30s left
    CANCEL_ALL_LEFT = 10            # cancel all when <10s left

    # ── RISK ──
    MAX_POSITIONS = 50
    MAX_EXPOSURE = 200.0
    MAX_DAILY_LOSS = 50.0
    MAX_DAILY_TRADES = 500

    # ── MERGE ──
    MERGE_ENABLED = True
    MERGE_INTERVAL = 30
    MERGE_SELL_FALLBACK_PRICE = 0.99  # price when native merge unavailable
    MIN_SHARES = 0.5

    # ── REGIME ──
    REGIME_VOL_THRESHOLD = 0.02
    REGIME_SPREAD_MAX = 0.10
    REGIME_CASCADE_COOLDOWN = 30

    # ── HEARTBEAT ──
    HEARTBEAT_INTERVAL = 5
    HEARTBEAT_STALE_WARN = 12

    # ── BINANCE STALE PROTECTION (A3) ──
    BN_STALE_MAX_SEC = 10

    # ── PAPER MODE ──
    PAPER_FILL_DELAY_SEC = 3.0      # M4: simulated fill delay in paper mode

    # ── EXECUTION ──
    CROSS_RETRY_OFFSET = 0.01       # EX2: retry this far below best ask on cross

    # ── EX1: CONSECUTIVE FAILURE BACKOFF (from v3.7) ──
    EXEC_BACKOFF_BASE = 1.0         # initial backoff (seconds)
    EXEC_BACKOFF_MAX = 30.0         # max backoff (seconds)
    EXEC_BACKOFF_THRESHOLD = 3      # consecutive failures before backoff kicks in
    EXEC_BACKOFF_MULTIPLIER = 2.0   # exponential multiplier

    # ── INFRASTRUCTURE ──
    CLOB = "https://clob.polymarket.com"
    GAMMA = "https://gamma-api.polymarket.com"

    SYM_BN = {"btc": "BTCUSDT", "eth": "ETHUSDT", "sol": "SOLUSDT"}

    BN_REST = "https://api.binance.com"
    BN_WS = "wss://stream.binance.com:9443/stream?streams="

    # CL RTDS WebSocket — primary oracle (what Polymarket settles against)
    CL_RTDS_WS = "wss://ws-live-data.polymarket.com"
    CL_RTDS_TOPIC = "crypto_prices_chainlink"
    # RTDS symbol format → internal asset key
    CL_RTDS_SYMBOLS = {"btc/usd": "btc", "eth/usd": "eth", "sol/usd": "sol"}
    CL_RTDS_RECONNECT_SEC = 2.0     # reconnect delay on disconnect
    CL_RTDS_STALE_SEC = 10          # max age before considered stale

    WS_USER = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

    LOG_LEVEL = "INFO"
    BATCH_ORDER_MAX = 15


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("V4.1")
for _quiet in ("httpx", "httpcore", "urllib3"):
    logging.getLogger(_quiet).setLevel(logging.WARNING)


# =============================================================================
# DATA CLASSES
# =============================================================================


class PairState(Enum):
    IDLE = "IDLE"
    LEG1_POSTED = "LEG1_POSTED"
    LEG1_FILLED = "LEG1_FILLED"
    LEG2_POSTED = "LEG2_POSTED"
    PAIR_COMPLETE = "PAIR_COMPLETE"


@dataclass
class BookLevel:
    price: float
    size: float


@dataclass
class Book:
    bids: List[BookLevel] = field(default_factory=list)
    asks: List[BookLevel] = field(default_factory=list)
    ts: float = 0

    @property
    def bb(self) -> float:
        return self.bids[0].price if self.bids else 0

    @property
    def ba(self) -> float:
        return self.asks[0].price if self.asks else 1

    @property
    def spread(self) -> float:
        return self.ba - self.bb if self.bids and self.asks else 1

    def depth(self, side: str, levels: int = 10) -> float:
        ls = self.bids[:levels] if side == "bid" else self.asks[:levels]
        return sum(lv.price * lv.size for lv in ls)

    def ask_size_at(self, price: float, tol: float = 0.005) -> float:
        """Total ask size at or below price (for FAK fill estimation)."""
        return sum(a.size for a in self.asks if a.price <= price + tol)


@dataclass
class MarketWindow:
    eid: str
    title: str
    slug: str
    asset: str
    wmin: int
    cid_up: str
    cid_down: str
    tid_up: str
    tid_down: str
    start_ts: int
    end_ts: int

    @property
    def left(self) -> int:
        return self.end_ts - int(time.time())

    @property
    def orphan_hedge_left(self) -> int:
        """A5/B7: Dynamic orphan hedge timing: max(30, wmin*60*0.15)."""
        return max(30, int(self.wmin * 60 * 0.15))


@dataclass
class WindowResult:
    asset: str
    window: int
    outcome: str       # "UP" or "DOWN"
    move_pct: float
    ts: float


# =============================================================================
# BINANCE FEED
# =============================================================================


class BinanceFeed:
    def __init__(self):
        self.px: Dict[str, float] = {}
        self.px_ts: Dict[str, float] = {}          # A3: per-symbol update ts
        self.hist: Dict[str, Deque[Tuple[float, float]]] = defaultdict(
            lambda: deque(maxlen=7200)
        )
        self._lock = threading.Lock()
        self.running = False

    def start(self):
        self._rest_fetch()
        self.running = True
        if HAS_WS:
            threading.Thread(target=self._ws, daemon=True, name="bn_ws").start()
            log.info("[BN] aggTrade WebSocket started")
        else:
            threading.Thread(target=self._poll, daemon=True, name="bn_poll").start()

    def stop(self):
        self.running = False

    def get(self, sym: str) -> Optional[float]:
        with self._lock:
            return self.px.get(sym)

    def get_asset(self, asset: str) -> Optional[float]:
        sym = Config.SYM_BN.get(asset)
        return self.get(sym) if sym else None

    def is_stale(self, asset: str, max_age_s: float = None) -> bool:
        """A3: True if Binance price for *asset* is older than threshold."""
        if max_age_s is None:
            max_age_s = Config.BN_STALE_MAX_SEC
        sym = Config.SYM_BN.get(asset)
        if not sym:
            return True
        with self._lock:
            ts = self.px_ts.get(sym, 0)
        return (time.time() - ts) > max_age_s

    def move(self, sym_or_asset: str, secs: int) -> Optional[float]:
        sym = Config.SYM_BN.get(sym_or_asset, sym_or_asset)
        with self._lock:
            h = list(self.hist.get(sym, []))
        if not h or len(h) < 2:
            return None
        cutoff = time.time() - secs
        old = next((p for ts, p in h if ts >= cutoff), None)
        cur = h[-1][1]
        if not old or old == 0:
            return None
        return ((cur - old) / old) * 100

    def _upd(self, sym: str, px: float):
        now = time.time()
        with self._lock:
            self.px[sym] = px
            self.px_ts[sym] = now
            self.hist[sym].append((now, px))

    def _rest_fetch(self):
        syms = set(Config.SYM_BN.values())
        try:
            for item in _SESSION.get(
                f"{Config.BN_REST}/api/v3/ticker/price", timeout=5
            ).json():
                if item["symbol"] in syms:
                    self._upd(item["symbol"], float(item["price"]))
        except Exception as e:
            log.debug(f"[BN] REST fetch failed: {e}")

    def _poll(self):
        while self.running:
            self._rest_fetch()
            time.sleep(0.5)

    def _ws(self):
        streams = "/".join(
            f"{s.lower()}@aggTrade" for s in set(Config.SYM_BN.values())
        )
        url = f"{Config.BN_WS}{streams}"
        while self.running:
            try:
                ws = ws_lib.WebSocketApp(
                    url, on_message=lambda w, m: self._on_msg(m)
                )
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                log.debug(f"[BN] WS error: {e}")
            if self.running:
                time.sleep(1)

    def _on_msg(self, msg):
        try:
            d = json.loads(msg)
            if "data" in d:
                d = d["data"]
            self._upd(d["s"], float(d["p"]))
        except Exception as e:
            log.debug(f"[BN] WS parse: {e}")


# =============================================================================
# CHAINLINK FEED  (RTDS WebSocket primary, Binance fallback)
# =============================================================================


class ChainlinkFeed:
    """Chainlink RTDS WebSocket — the oracle Polymarket settles against.

    Primary: wss://ws-live-data.polymarket.com (topic crypto_prices_chainlink)
      - Parses {symbol, timestamp (ms), value (float), full_accuracy_value (18-dec)}
      - ~1 tick/sec per symbol, ~4/sec total
      - Stores second-level snapshots for precise at() lookups

    Fallback: BinanceFeed.get_asset() — already running, close enough for
      quoting decisions when RTDS is temporarily disconnected.
      NOT used for settlement — only CL prices settle markets.

    Stripped in v4.1: CryptoCompare, on-chain Multicall3, web3, eth_abi.
    """

    def __init__(self):
        self.px: Dict[str, float] = {}
        self.px_ts: Dict[str, float] = {}          # asset → last update unix seconds
        self.hist: Dict[str, Deque[Tuple[float, float]]] = defaultdict(
            lambda: deque(maxlen=7200)
        )
        self._lock = threading.Lock()
        self.running = False
        self._snap: Dict[str, Dict[int, float]] = defaultdict(dict)
        self._ws = None
        self._ws_connected = False
        self._bn_fallback: Optional["BinanceFeed"] = None  # set by Bot after init

    def start(self):
        self.running = True
        if HAS_WS:
            threading.Thread(
                target=self._ws_loop, daemon=True, name="cl_rtds"
            ).start()
            log.info("[CL] RTDS WebSocket starting")
        else:
            log.warning("[CL] websocket-client not installed — Binance fallback only")

    def stop(self):
        self.running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception as e:
                log.debug(f"[CL] WS close: {e}")

    def set_bn_fallback(self, bn: "BinanceFeed"):
        """Wire Binance as fallback price source (called by Bot after init)."""
        self._bn_fallback = bn

    def get(self, asset: str) -> Optional[float]:
        """Get current price for asset (e.g. 'btc').
        Returns RTDS price if fresh, Binance fallback if RTDS stale."""
        with self._lock:
            p = self.px.get(asset)
            ts = self.px_ts.get(asset, 0)

        # RTDS price is fresh — use it
        if p is not None and (time.time() - ts) < Config.CL_RTDS_STALE_SEC:
            return p

        # RTDS stale or absent — Binance fallback for quoting only
        if self._bn_fallback:
            bn_px = self._bn_fallback.get_asset(asset)
            if bn_px is not None:
                return bn_px

        # Return stale RTDS if we have it (better than nothing)
        return p

    def at(self, asset: str, ts: int, tol: int = 5) -> Optional[float]:
        """Price at a given timestamp.

        Args:
            asset: lowercase asset key (e.g. 'btc')
            ts: unix timestamp to look up
            tol: tolerance in MINUTES (strategy.py calls cl.at(asset, ts, tol=5)
                 meaning 5 minutes)

        Uses second-level snap dict first (RTDS gives per-second timestamps),
        falls back to hist deque search within ±tol minutes.
        """
        tol_sec = tol * 60

        with self._lock:
            snaps = dict(self._snap.get(asset, {}))
            h = list(self.hist.get(asset, []))

        # Snap dict: check ±5 seconds for exact RTDS match
        snap_tol = min(5, tol_sec)
        for t in range(ts - snap_tol, ts + snap_tol + 1):
            if t in snaps:
                return snaps[t]

        # Fallback: search history deque within ±tol minutes
        if not h:
            return None
        best, best_dt = None, float("inf")
        for t, p in h:
            dt = abs(t - ts)
            if dt < best_dt:
                best, best_dt = p, dt
        return best if best_dt <= tol_sec else None

    def is_stale(self, max_age_s: int = 15) -> bool:
        """True if ALL assets are stale (no update within max_age_s)."""
        now = time.time()
        with self._lock:
            if not self.px_ts:
                return True
            for asset, ts in self.px_ts.items():
                if (now - ts) < max_age_s:
                    return False
        return True

    def is_rtds_connected(self) -> bool:
        """True if RTDS WebSocket is currently connected."""
        return self._ws_connected

    # ── RTDS WebSocket ──

    def _ws_loop(self):
        """Reconnect loop for RTDS WebSocket."""
        while self.running:
            try:
                url = Config.CL_RTDS_WS
                self._ws = ws_lib.WebSocketApp(
                    url,
                    on_open=self._on_rtds_open,
                    on_message=lambda w, m: self._on_rtds_msg(m),
                    on_error=lambda w, e: self._on_rtds_error(e),
                    on_close=lambda w, c, m: self._on_rtds_close(c, m),
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                log.debug(f"[CL] RTDS WS error: {e}")
            self._ws_connected = False
            if self.running:
                log.info(
                    f"[CL] RTDS disconnected — reconnecting in "
                    f"{Config.CL_RTDS_RECONNECT_SEC}s"
                )
                time.sleep(Config.CL_RTDS_RECONNECT_SEC)

    def _on_rtds_open(self, ws):
        """Subscribe to crypto_prices_chainlink topic on connect."""
        sub_msg = json.dumps({
            "type": "subscribe",
            "channel": Config.CL_RTDS_TOPIC,
        })
        ws.send(sub_msg)
        self._ws_connected = True
        log.info("[CL] RTDS WebSocket connected — subscribed to "
                 f"{Config.CL_RTDS_TOPIC}")

    def _on_rtds_msg(self, msg: str):
        """Parse RTDS update message.

        Expected format:
        {
          "topic": "crypto_prices_chainlink",
          "type": "update",
          "timestamp": 1772533839000,  (Unix ms)
          "payload": {
            "symbol": "btc/usd",
            "timestamp": 1772533839000,
            "value": 66483.43,
            "full_accuracy_value": "66483434010500000000000"
          }
        }
        """
        try:
            d = json.loads(msg)

            # Skip non-update messages (connection ack, heartbeats, etc.)
            if d.get("type") != "update":
                return
            if d.get("topic") != Config.CL_RTDS_TOPIC:
                return

            payload = d.get("payload")
            if not isinstance(payload, dict):
                return

            symbol = payload.get("symbol", "")
            asset = Config.CL_RTDS_SYMBOLS.get(symbol)
            if not asset:
                return

            price = payload.get("value")
            if price is None or not isinstance(price, (int, float)) or price <= 0:
                return

            # RTDS timestamp is Unix milliseconds
            ts_ms = payload.get("timestamp", 0)
            ts_sec = ts_ms / 1000.0 if ts_ms > 1_000_000_000_000 else float(ts_ms)
            ts_int = int(ts_sec)

            price = float(price)

            with self._lock:
                self.px[asset] = price
                self.px_ts[asset] = ts_sec
                self.hist[asset].append((ts_sec, price))
                self._snap[asset][ts_int] = price

            # Prune snap dict periodically (every ~60s worth of ticks)
            if ts_int % 60 == 0:
                self._prune(ts_int)

        except Exception as e:
            log.debug(f"[CL] RTDS parse error: {e}")

    def _on_rtds_error(self, error):
        log.debug(f"[CL] RTDS WS error: {error}")

    def _on_rtds_close(self, close_code, close_msg):
        self._ws_connected = False
        log.debug(f"[CL] RTDS WS closed: {close_code} {close_msg}")

    def _prune(self, inow: int):
        cutoff = inow - 3600
        with self._lock:
            for asset in list(self._snap):
                self._snap[asset] = {
                    k: v for k, v in self._snap[asset].items() if k > cutoff
                }


# =============================================================================
# BOOK FETCHER  (S1: batch fetch via persistent pool)
# =============================================================================


class BookFetcher:
    def __init__(self):
        self._books: Dict[str, Book] = {}
        self._lock = threading.Lock()

    def update(self, tid: str, book: Book):
        with self._lock:
            self._books[tid] = book

    def get(self, tid: str) -> Optional[Book]:
        with self._lock:
            return self._books.get(tid)

    def fetch_http(self, tid: str) -> Optional[Book]:
        try:
            r = _SESSION.get(
                f"{Config.CLOB}/book", params={"token_id": tid}, timeout=3
            )
            if r.status_code == 200:
                data = r.json()
                bids = sorted(
                    [BookLevel(float(b["price"]), float(b["size"]))
                     for b in data.get("bids", [])],
                    key=lambda x: -x.price,
                )
                asks = sorted(
                    [BookLevel(float(a["price"]), float(a["size"]))
                     for a in data.get("asks", [])],
                    key=lambda x: x.price,
                )
                book = Book(bids=bids, asks=asks, ts=time.time())
                self.update(tid, book)
                return book
        except Exception as e:
            log.debug(f"[BOOKS] fetch_http {tid[:16]}.. failed: {e}")
        return None

    def fetch_batch(self, tids: List[str]) -> Dict[str, Optional[Book]]:
        """S1: Parallel book refresh using persistent pool."""
        results: Dict[str, Optional[Book]] = {}
        futs = {POOL.submit(self.fetch_http, tid): tid for tid in tids}
        for fut in as_completed(futs):
            tid = futs[fut]
            try:
                results[tid] = fut.result()
            except Exception:
                results[tid] = None
        return results

    def start_ws(self, tids: Set[str]):
        try:
            from ws_books import WSBooks

            self._ws = WSBooks(tids, self._on_ws_book)
            self._ws.start()
            log.info(f"[BOOKS] WS started for {len(tids)} tokens")
        except Exception as e:
            log.warning(f"[BOOKS] WS failed: {e}")

    def _on_ws_book(self, tid: str, bids: list, asks: list):
        book = Book(
            bids=[BookLevel(float(b[0]), float(b[1])) for b in bids],
            asks=[BookLevel(float(a[0]), float(a[1])) for a in asks],
            ts=time.time(),
        )
        self.update(tid, book)


# =============================================================================
# HEARTBEAT THREAD  (M1/M3: verified HMAC format)
# =============================================================================


class HeartbeatThread:
    """POST /heartbeat every 5s — Polymarket cancels all orders if missed."""

    def __init__(self, exec_layer: "ExecutionLayer"):
        self.exec = exec_layer
        self._running = False
        self._last_beat = 0.0
        self._last_id: Optional[str] = None
        self._beat_count = 0
        self._fail_count = 0
        self._lock = threading.Lock()

    def start(self):
        self._running = True
        threading.Thread(target=self._loop, daemon=True, name="heartbeat").start()
        log.info("[HB] Heartbeat started (interval=%ds)", Config.HEARTBEAT_INTERVAL)

    def stop(self):
        self._running = False

    @property
    def healthy(self) -> bool:
        with self._lock:
            if self._last_beat == 0:
                return False
            return (time.time() - self._last_beat) < Config.HEARTBEAT_STALE_WARN

    @property
    def stats(self) -> str:
        with self._lock:
            age = time.time() - self._last_beat if self._last_beat else 999
            return f"beats={self._beat_count} fails={self._fail_count} age={age:.1f}s"

    def _loop(self):
        while self._running:
            try:
                self._send()
            except Exception as e:
                with self._lock:
                    self._fail_count += 1
                log.error(f"[HB] Error: {e}")
            time.sleep(Config.HEARTBEAT_INTERVAL)

    def _send(self):
        if self.exec.paper or not self.exec._clob:
            with self._lock:
                self._last_beat = time.time()
                self._beat_count += 1
            return

        # Try py-clob-client method first
        try:
            if hasattr(self.exec._clob, "heartbeat"):
                with self.exec._clob_lock:
                    resp = self.exec._clob.heartbeat()
                with self._lock:
                    self._last_beat = time.time()
                    self._beat_count += 1
                    if isinstance(resp, dict):
                        self._last_id = resp.get("heartbeat_id", self._last_id)
                return
        except AttributeError:
            pass

        # M1/M3: Manual HTTP with verified sig format
        try:
            headers = {}
            if hasattr(self.exec._clob, "creds") and self.exec._clob.creds:
                creds = self.exec._clob.creds
                ts_str = str(int(time.time()))
                sig_payload = f"POST\n/heartbeat\n\n{ts_str}"
                sig = hmac.new(
                    base64.b64decode(creds.api_secret),
                    sig_payload.encode("utf-8"),
                    hashlib.sha256,
                ).hexdigest()
                headers = {
                    "POLY_API_KEY": creds.api_key,
                    "POLY_PASSPHRASE": creds.api_passphrase,
                    "POLY_SIGNATURE": sig,
                    "POLY_TIMESTAMP": ts_str,
                }

            resp = _SESSION.post(
                f"{Config.CLOB}/heartbeat", headers=headers, json={}, timeout=5
            )
            if resp.status_code == 200:
                data = resp.json()
                with self._lock:
                    self._last_beat = time.time()
                    self._beat_count += 1
                    self._last_id = data.get("heartbeat_id", self._last_id)
            else:
                with self._lock:
                    self._fail_count += 1
                log.warning(f"[HB] HTTP {resp.status_code}: {resp.text[:100]}")
        except Exception as e:
            with self._lock:
                self._fail_count += 1
            log.error(f"[HB] Manual heartbeat failed: {e}")


# =============================================================================
# USER WS FEED  (M2: verified WS auth format)
# =============================================================================


class UserWSFeed:
    """Real-time fill/cancel push from Polymarket user channel."""

    def __init__(self, on_fill=None, on_cancel=None):
        self._on_fill = on_fill
        self._on_cancel = on_cancel
        self._running = False
        self._connected = False
        self._msg_count = 0
        self._lock = threading.Lock()
        self._api_key = ""
        self._api_secret = ""
        self._api_passphrase = ""

    def configure(self, api_key: str, api_secret: str, api_passphrase: str):
        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase

    def start(self):
        if not self._api_key:
            log.warning("[USER_WS] No API creds — skipping")
            return
        self._running = True
        threading.Thread(target=self._ws_loop, daemon=True, name="user_ws").start()
        log.info("[USER_WS] Starting")

    def stop(self):
        self._running = False

    @property
    def connected(self) -> bool:
        with self._lock:
            return self._connected

    def _ws_loop(self):
        while self._running:
            try:
                ts_str = str(int(time.time()))
                sig_payload = f"GET\n/ws/user\n\n{ts_str}"
                sig = hmac.new(
                    base64.b64decode(self._api_secret),
                    sig_payload.encode("utf-8"),
                    hashlib.sha256,
                ).hexdigest()

                headers = {
                    "POLY_API_KEY": self._api_key,
                    "POLY_PASSPHRASE": self._api_passphrase,
                    "POLY_SIGNATURE": sig,
                    "POLY_TIMESTAMP": ts_str,
                }

                ws = ws_lib.WebSocketApp(
                    Config.WS_USER,
                    header=[f"{k}: {v}" for k, v in headers.items()],
                    on_message=lambda w, m: self._on_msg(m),
                    on_open=lambda w: self._set_connected(True),
                    on_close=lambda w, c, m: self._set_connected(False),
                    on_error=lambda w, e: log.warning(f"[USER_WS] {e}"),
                )
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                log.error(f"[USER_WS] Connection error: {e}")
            self._set_connected(False)
            if self._running:
                time.sleep(2)

    def _set_connected(self, val: bool):
        with self._lock:
            self._connected = val
        log.info(f"[USER_WS] {'Connected' if val else 'Disconnected'}")

    def _on_msg(self, msg):
        try:
            d = json.loads(msg)
            with self._lock:
                self._msg_count += 1

            event_type = d.get("type", d.get("event_type", ""))

            if event_type in ("order_matched", "trade", "fill"):
                oid = d.get("order_id", d.get("orderId", ""))
                px = float(d.get("price", d.get("fill_price", 0)) or 0)
                sz = float(d.get("size", d.get("fill_size", 0)) or 0)
                if oid and self._on_fill:
                    self._on_fill(oid, px, sz)

            elif event_type in ("order_cancelled", "cancel"):
                oid = d.get("order_id", d.get("orderId", ""))
                if oid and self._on_cancel:
                    self._on_cancel(oid)

        except Exception as e:
            log.debug(f"[USER_WS] Parse error: {e}")


# =============================================================================
# EXECUTION LAYER  (EX1: failure backoff, EX2: cross detection, B3/B4/B5)
# =============================================================================


class ExecutionLayer:
    def __init__(self, paper: bool = True):
        self.paper = paper
        self._next_oid = 1000
        self._orders: Dict[str, dict] = {}
        self._clob = None
        self._clob_lock = threading.Lock()
        self._fill_check_ts: Dict[str, float] = {}
        # WS-driven tracking
        self._ws_fills: Dict[str, Tuple[float, float]] = {}
        self._ws_cancels: Set[str] = set()
        self._ws_lock = threading.Lock()
        # EX1: Consecutive failure backoff
        self._consec_fails = 0
        self._backoff_until = 0.0

    # ── WS callbacks ──

    def on_ws_fill(self, oid: str, px: float, sz: float):
        with self._ws_lock:
            self._ws_fills[oid] = (px, sz)

    def on_ws_cancel(self, oid: str):
        """B5: Immediately clean order tracking on WS cancel."""
        with self._ws_lock:
            self._ws_cancels.add(oid)
        self._orders.pop(oid, None)

    # ── EX1: Backoff check ──

    def _check_backoff(self) -> bool:
        """EX1: Returns True if currently in backoff (should not send orders)."""
        if self._consec_fails < Config.EXEC_BACKOFF_THRESHOLD:
            return False
        now = time.time()
        if now < self._backoff_until:
            return True
        return False

    def _record_success(self):
        """EX1: Reset failure counter on successful API call."""
        self._consec_fails = 0
        self._backoff_until = 0.0

    def _record_failure(self):
        """EX1: Increment failure counter and compute backoff."""
        self._consec_fails += 1
        if self._consec_fails >= Config.EXEC_BACKOFF_THRESHOLD:
            delay = min(
                Config.EXEC_BACKOFF_BASE * (
                    Config.EXEC_BACKOFF_MULTIPLIER
                    ** (self._consec_fails - Config.EXEC_BACKOFF_THRESHOLD)
                ),
                Config.EXEC_BACKOFF_MAX,
            )
            self._backoff_until = time.time() + delay
            log.warning(
                f"[EXEC] Backoff: {self._consec_fails} consecutive failures, "
                f"pausing {delay:.1f}s"
            )

    # ── Init ──

    def init_live(self):
        try:
            from py_clob_client.client import ClobClient

            pk = os.getenv("POLY_PRIVATE_KEY", "")
            funder = os.getenv("POLY_FUNDER_ADDRESS", "")
            if not pk:
                raise ValueError("POLY_PRIVATE_KEY required")
            self._clob = ClobClient(
                "https://clob.polymarket.com",
                key=pk, chain_id=137,
                signature_type=1, funder=funder,
            )
            creds = self._clob.create_or_derive_api_creds()
            self._clob.set_api_creds(creds)
            log.info(f"[EXEC] Derived API creds: {creds.api_key[:12]}...")
            self._clob.get_orders()
            self.paper = False
            log.info(f"[EXEC] LIVE — sig_type=1, funder={funder[:10]}...")
            return creds
        except Exception as e:
            log.error(f"[EXEC] Live init failed: {e} — staying paper")
            return None

    # ── GTC buy (maker-only, EX2: cross detection) ──

    def buy_gtc(self, tid: str, stake: float, price: float) -> Optional[str]:
        if price <= 0 or price >= 1.0 or stake <= 0:
            return None
        price = round(price, 2)

        if self.paper:
            oid = f"P{self._next_oid}"; self._next_oid += 1
            self._orders[oid] = {
                "tid": tid, "price": price, "stake": stake,
                "shares": stake / price, "posted_at": time.time(),
                "filled": False, "fill_px": 0, "type": "GTC",
            }
            return oid

        # EX1: Check backoff before sending
        if self._check_backoff():
            return None

        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        try:
            with self._clob_lock:
                order = OrderArgs(
                    price=price, size=round(stake / price, 2),
                    side=BUY, token_id=tid,
                )
                signed = self._clob.create_order(order)
                try:
                    resp = self._clob.post_order(signed, OrderType.GTC)
                except Exception as po_err:
                    if "crosses" not in str(po_err).lower():
                        raise
                    # EX2: Crossed best ask — retry below (from v3.8)
                    try:
                        br = _SESSION.get(
                            f"{Config.CLOB}/book",
                            params={"token_id": tid}, timeout=3,
                        )
                        asks = br.json().get("asks", []) if br.status_code == 200 else []
                        new_price = round(
                            min(float(a["price"]) for a in asks) - Config.CROSS_RETRY_OFFSET, 2
                        ) if asks else round(price - Config.CROSS_RETRY_OFFSET, 2)
                    except Exception:
                        new_price = round(price - Config.CROSS_RETRY_OFFSET, 2)
                    if new_price < 0.01:
                        return None
                    log.debug(f"[EXEC] Crossed, retry @{new_price:.2f}")
                    order = OrderArgs(
                        price=new_price, size=round(stake / new_price, 2),
                        side=BUY, token_id=tid,
                    )
                    signed = self._clob.create_order(order)
                    resp = self._clob.post_order(signed, OrderType.GTC)
                    price = new_price

            if resp.get("errorMsg"):
                log.debug(f"[EXEC] BUY error: {resp['errorMsg']}")
                self._record_failure()  # EX1
                return None
            oid = resp.get("orderID", "")
            if not oid:
                self._record_failure()  # EX1
                return None
            self._record_success()  # EX1
            filled_now = resp.get("status") == "matched"
            self._orders[oid] = {
                "tid": tid, "price": price, "stake": stake,
                "shares": stake / price, "posted_at": time.time(),
                "filled": filled_now,
                "fill_px": price if filled_now else 0,
                "type": "GTC",
            }
            return oid
        except Exception as e:
            log.error(f"[EXEC] buy_gtc: {e}")
            self._record_failure()  # EX1
            return None

    # ── FAK buy  (B3: was FOK, now FAK — allows partial fills) ──

    def buy_fak(self, tid: str, stake: float, price: float) -> Optional[str]:
        """B3 CRITICAL FIX: Uses OrderType.FAK (partial fill OK)."""
        if price <= 0 or price >= 1.0 or stake <= 0:
            return None
        price = round(price, 2)

        if self.paper:
            oid = f"PF{self._next_oid}"; self._next_oid += 1
            self._orders[oid] = {
                "tid": tid, "price": price, "stake": stake,
                "shares": stake / price, "posted_at": time.time(),
                "filled": True, "fill_px": price, "type": "FAK",
            }
            return oid

        # EX1: Check backoff
        if self._check_backoff():
            return None

        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        try:
            with self._clob_lock:
                order = OrderArgs(
                    price=price, size=round(stake / price, 2),
                    side=BUY, token_id=tid,
                )
                signed = self._clob.create_order(order)
                resp = self._clob.post_order(signed, OrderType.FAK)

            if resp.get("errorMsg"):
                log.debug(f"[EXEC] FAK error: {resp['errorMsg']}")
                self._record_failure()  # EX1
                return None
            oid = resp.get("orderID", "")
            if not oid:
                self._record_failure()  # EX1
                return None

            status = resp.get("status", "")
            filled = status in ("matched", "filled")
            fill_size = float(
                resp.get("filledSize", resp.get("filled_size", 0)) or 0
            )
            fill_price = float(
                resp.get("avgPrice", resp.get("price", price)) or price
            )

            if filled or fill_size > 0:
                self._record_success()  # EX1
                actual_shares = fill_size if fill_size > 0 else stake / price
                self._orders[oid] = {
                    "tid": tid, "price": fill_price,
                    "stake": actual_shares * fill_price,
                    "shares": actual_shares, "posted_at": time.time(),
                    "filled": True, "fill_px": fill_price, "type": "FAK",
                }
                log.info(
                    f"[EXEC] FAK FILL {oid[:12]}.. @{fill_price:.2f} "
                    f"sz={actual_shares:.2f}"
                )
                return oid
            else:
                self._record_success()  # EX1: not a failure, just no liquidity
                log.debug(f"[EXEC] FAK MISS {oid[:12]}.. @{price:.2f}")
                return None
        except Exception as e:
            log.error(f"[EXEC] buy_fak: {e}")
            self._record_failure()  # EX1
            return None

    # ── Sell GTC ──

    def sell_gtc(self, tid: str, shares: float, price: float) -> Optional[str]:
        if price <= 0 or shares <= 0:
            return None
        price = round(price, 2)

        if self.paper:
            oid = f"PS{self._next_oid}"; self._next_oid += 1
            self._orders[oid] = {
                "tid": tid, "side": "SELL", "price": price, "shares": shares,
                "posted_at": time.time(), "filled": False, "fill_px": 0,
            }
            return oid

        # EX1: Check backoff
        if self._check_backoff():
            return None

        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import SELL

        try:
            with self._clob_lock:
                order = OrderArgs(
                    price=price, size=round(shares, 2),
                    side=SELL, token_id=tid,
                )
                signed = self._clob.create_order(order)
                resp = self._clob.post_order(signed, OrderType.GTC)
            if resp.get("errorMsg"):
                self._record_failure()  # EX1
                return None
            oid = resp.get("orderID", "")
            self._record_success()  # EX1
            self._orders[oid] = {
                "tid": tid, "side": "SELL", "price": price, "shares": shares,
                "posted_at": time.time(),
                "filled": resp.get("status") == "matched",
                "fill_px": price if resp.get("status") == "matched" else 0,
            }
            return oid
        except Exception as e:
            log.error(f"[EXEC] sell_gtc: {e}")
            self._record_failure()  # EX1
            return None

    # ── S3: Batch buy ──

    def buy_batch(self, orders: List[dict]) -> List[Optional[str]]:
        """S3: Up to 15 orders in one POST."""
        if not orders:
            return []
        if self.paper:
            return [self.buy_gtc(o["tid"], o["stake"], o["price"]) for o in orders]

        # EX1: Check backoff
        if self._check_backoff():
            return [None] * len(orders)

        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        results: List[Optional[str]] = []
        try:
            signed_orders = []
            for o in orders[: Config.BATCH_ORDER_MAX]:
                price = round(o["price"], 2)
                if price <= 0 or price >= 1.0:
                    continue
                with self._clob_lock:
                    arg = OrderArgs(
                        price=price, size=round(o["stake"] / price, 2),
                        side=BUY, token_id=o["tid"],
                    )
                    signed_orders.append((self._clob.create_order(arg), o))

            if not signed_orders:
                return [None] * len(orders)

            with self._clob_lock:
                if hasattr(self._clob, "post_orders"):
                    resp = self._clob.post_orders(
                        [s for s, _ in signed_orders], OrderType.GTC
                    )
                else:
                    resp = []
                    for signed, _ in signed_orders:
                        try:
                            resp.append(self._clob.post_order(signed, OrderType.GTC))
                        except Exception:
                            resp.append({"errorMsg": "failed"})

            resp_list = resp if isinstance(resp, list) else [resp]
            any_success = False
            for r, (_, o) in zip(resp_list, signed_orders):
                if isinstance(r, dict) and r.get("orderID") and not r.get("errorMsg"):
                    oid = r["orderID"]
                    price = round(o["price"], 2)
                    self._orders[oid] = {
                        "tid": o["tid"], "price": price, "stake": o["stake"],
                        "shares": o["stake"] / price, "posted_at": time.time(),
                        "filled": r.get("status") == "matched",
                        "fill_px": price if r.get("status") == "matched" else 0,
                        "type": "GTC",
                    }
                    results.append(oid)
                    any_success = True
                else:
                    results.append(None)
            if any_success:
                self._record_success()  # EX1
            else:
                self._record_failure()  # EX1
            return results
        except Exception as e:
            log.error(f"[EXEC] buy_batch: {e}")
            self._record_failure()  # EX1
            return [None] * len(orders)

    # ── B4: Native merge/redeem ($1.00 per pair, no counterparty) ──

    def merge_redeem(self, condition_id: str, shares: float) -> bool:
        """B4: Surrender UP+DOWN shares for $1.00 directly."""
        if shares <= 0:
            return False
        if self.paper:
            return True

        try:
            with self._clob_lock:
                if hasattr(self._clob, "merge_positions"):
                    resp = self._clob.merge_positions(
                        condition_id, round(shares, 2)
                    )
                    return bool(resp)

                headers = {}
                if hasattr(self._clob, "creds") and self._clob.creds:
                    creds = self._clob.creds
                    ts_str = str(int(time.time()))
                    body = json.dumps({
                        "conditionId": condition_id,
                        "amount": str(round(shares, 2)),
                    })
                    sig_payload = f"POST\n/merge\n{body}\n{ts_str}"
                    sig = hmac.new(
                        base64.b64decode(creds.api_secret),
                        sig_payload.encode("utf-8"),
                        hashlib.sha256,
                    ).hexdigest()
                    headers = {
                        "POLY_API_KEY": creds.api_key,
                        "POLY_PASSPHRASE": creds.api_passphrase,
                        "POLY_SIGNATURE": sig,
                        "POLY_TIMESTAMP": ts_str,
                        "Content-Type": "application/json",
                    }

                resp = _SESSION.post(
                    f"{Config.CLOB}/merge",
                    data=json.dumps({
                        "conditionId": condition_id,
                        "amount": str(round(shares, 2)),
                    }),
                    headers=headers,
                    timeout=10,
                )
                if resp.status_code == 200:
                    log.info(
                        f"[EXEC] Merge/redeem {condition_id[:16]}.. "
                        f"{shares:.1f}sh → $1.00/ea"
                    )
                    return True
                else:
                    log.warning(
                        f"[EXEC] Merge failed HTTP {resp.status_code}: "
                        f"{resp.text[:100]}"
                    )
                    return False
        except Exception as e:
            log.error(f"[EXEC] merge_redeem: {e}")
            return False

    # ── Cancel ──

    def cancel_order(self, oid: str) -> bool:
        if not oid:
            return False
        if self.paper:
            self._orders.pop(oid, None)
            return True
        try:
            with self._clob_lock:
                self._clob.cancel(oid)
            self._orders.pop(oid, None)
            return True
        except Exception as e:
            log.debug(f"[EXEC] cancel: {e}")
            self._orders.pop(oid, None)
            return False

    def cancel_batch(self, oids: List[str]) -> int:
        """S4: Batch cancel."""
        oids = [o for o in oids if o]
        if not oids:
            return 0
        if self.paper:
            for oid in oids:
                self._orders.pop(oid, None)
            return len(oids)
        try:
            with self._clob_lock:
                if hasattr(self._clob, "cancel_orders"):
                    self._clob.cancel_orders(oids)
                else:
                    for oid in oids:
                        try:
                            self._clob.cancel(oid)
                        except Exception as e:
                            log.debug(f"[EXEC] cancel {oid[:12]}.. failed: {e}")
            for oid in oids:
                self._orders.pop(oid, None)
            return len(oids)
        except Exception as e:
            log.error(f"[EXEC] cancel_batch: {e}")
            return 0

    def cancel_all(self) -> int:
        if self.paper:
            n = len(self._orders)
            self._orders.clear()
            return n
        try:
            with self._clob_lock:
                self._clob.cancel_all()
            n = len(self._orders)
            self._orders.clear()
            return n
        except Exception as e:
            log.error(f"[EXEC] cancel_all: {e}")
            return 0

    # ── Fill check ──

    def check_fill(self, oid: str) -> Tuple[bool, float]:
        """WS push first, polling fallback."""
        if not oid:
            return (False, 0)

        # B5: WS-driven fills (instant)
        with self._ws_lock:
            if oid in self._ws_fills:
                px, _ = self._ws_fills.pop(oid)
                self._orders.pop(oid, None)
                return (True, px)
            if oid in self._ws_cancels:
                self._ws_cancels.discard(oid)
                self._orders.pop(oid, None)
                return (False, 0)

        if self.paper:
            o = self._orders.get(oid)
            if not o:
                return (False, 0)
            if o.get("filled"):
                return (True, o["fill_px"])
            if time.time() - o["posted_at"] > Config.PAPER_FILL_DELAY_SEC:
                o["filled"] = True
                o["fill_px"] = o["price"]
                return (True, o["price"])
            return (False, 0)

        # Polling fallback
        now = time.time()
        if now - self._fill_check_ts.get(oid, 0) < 1.0:
            return (False, 0)
        self._fill_check_ts[oid] = now
        try:
            with self._clob_lock:
                resp = self._clob.get_order(oid)
            status = resp.get("status", "")
            if status in ("matched", "filled"):
                trades = resp.get("associate_trades", [{}])
                px = float(trades[0].get("price", 0) or 0) if trades else 0
                if px == 0:
                    px = float(resp.get("price", 0) or 0)
                self._orders.pop(oid, None)
                self._fill_check_ts.pop(oid, None)
                return (True, px)
            return (False, 0)
        except Exception:
            return (False, 0)

    def is_order_live(self, oid: str) -> bool:
        """B5: Check if order still tracked (not WS-cancelled)."""
        if not oid:
            return False
        with self._ws_lock:
            if oid in self._ws_cancels:
                return False
        return oid in self._orders
