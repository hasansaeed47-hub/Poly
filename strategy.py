#!/usr/bin/env python3
"""
POLYMARKET v4.1 — STRATEGY LAYER
===================================
RegimeGuard, RiskManager, PairTracker, CrossWindowIntel,
Scanner, MergeEngine, Settlement, QuoteEngine

Gamma API: GET /events/slug/{slug}
  Response: { markets: [{ conditionId, clobTokenIds, outcomes, ... }] }
  YES token → tid_up  (price went UP)
  NO  token → tid_down (price did NOT go up, i.e. DOWN)
  Single condition per binary window.
"""

from __future__ import annotations

import time
import json
import math
import threading
import logging
from collections import deque, defaultdict
from typing import Dict, List, Optional, Tuple

from infra import (
    Config, log, POOL, _SESSION,
    BinanceFeed, ChainlinkFeed, BookFetcher, ExecutionLayer,
    MarketWindow, WindowResult, Book, PairState,
    slug_ts, slug_wmin,
)


# =============================================================================
# SCANNER
# =============================================================================


class Scanner:
    """Discovers active Polymarket UP/DOWN windows via Gamma API.

    Slug format  : {asset}-updown-{N}m-{window_start_unix}
    Gamma endpoint: GET /events/slug/{slug}
    Response shape: { markets: [{ conditionId, clobTokenIds, outcomes }] }
    """

    CACHE_TTL = 10.0   # seconds between full re-scans
    LOOK_AHEAD = 1     # how many future windows to check (0 = current only)

    def __init__(self, books: BookFetcher):
        self.books = books
        self._cache: List[MarketWindow] = []
        self._cache_ts = 0.0
        self._lock = threading.Lock()

    # ── Public ──────────────────────────────────────────────────────────────

    def scan(self) -> List[MarketWindow]:
        """Return active windows. Caches results for CACHE_TTL seconds."""
        now = time.time()
        with self._lock:
            if now - self._cache_ts < self.CACHE_TTL:
                return [w for w in self._cache
                        if w.left > Config.CANCEL_ALL_LEFT]

        windows = []
        futs = {}
        for asset in sorted(Config.ASSETS):
            for tf in sorted(Config.TIMEFRAMES):
                for slug in self._candidate_slugs(asset, tf):
                    futs[POOL.submit(self._fetch_window, slug, asset, tf)] = slug

        from concurrent.futures import as_completed
        for fut in as_completed(futs):
            w = fut.result()
            if w:
                windows.append(w)

        with self._lock:
            self._cache = windows
            self._cache_ts = now

        return [w for w in windows if w.left > Config.CANCEL_ALL_LEFT]

    # ── Helpers ─────────────────────────────────────────────────────────────

    def _candidate_slugs(self, asset: str, wmin: int) -> List[str]:
        """Current window + next window slugs."""
        interval = wmin * 60
        now = int(time.time())
        base = (now // interval) * interval
        return [
            f"{asset}-updown-{wmin}m-{base + i * interval}"
            for i in range(self.LOOK_AHEAD + 1)
        ]

    def _fetch_window(self, slug: str, asset: str, wmin: int
                      ) -> Optional[MarketWindow]:
        url = f"{Config.GAMMA}/events/slug/{slug}"
        try:
            r = _SESSION.get(url, timeout=5)
            if r.status_code == 404:
                return None
            if r.status_code != 200:
                log.debug(f"[SCAN] {slug} HTTP {r.status_code}")
                return None
            return self._parse(r.json(), slug, asset, wmin)
        except Exception as e:
            log.debug(f"[SCAN] {slug}: {e}")
            return None

    def _parse(self, ev: dict, slug: str, asset: str, wmin: int
               ) -> Optional[MarketWindow]:
        try:
            markets = ev.get("markets", [])
            if not markets:
                return None

            # Find market with YES/NO outcomes
            market = None
            for m in markets:
                outs = [o.upper() for o in (m.get("outcomes") or [])]
                if "YES" in outs and "NO" in outs:
                    market = m
                    break
            if not market:
                return None

            tokens  = market.get("clobTokenIds") or []
            outcomes = [o.upper() for o in (market.get("outcomes") or [])]
            cid      = market.get("conditionId", "")

            if len(tokens) < 2 or len(outcomes) < 2:
                return None

            yes_i = outcomes.index("YES")
            no_i  = outcomes.index("NO")
            tid_up   = tokens[yes_i]
            tid_down = tokens[no_i]

            if not tid_up or not tid_down or not cid:
                return None

            start_ts = slug_ts(slug) or 0
            if not start_ts:
                return None
            end_ts = start_ts + wmin * 60

            now = int(time.time())
            if end_ts <= now + Config.CANCEL_ALL_LEFT:
                return None   # already expired

            eid   = str(ev.get("id", slug))
            title = ev.get("title", slug)

            log.debug(
                f"[SCAN] {slug} YES={tid_up[:12]}.. NO={tid_down[:12]}.. "
                f"left={end_ts - now}s"
            )
            return MarketWindow(
                eid=eid, title=title, slug=slug,
                asset=asset, wmin=wmin,
                cid_up=cid, cid_down=cid,
                tid_up=tid_up, tid_down=tid_down,
                start_ts=start_ts, end_ts=end_ts,
            )
        except Exception as e:
            log.debug(f"[SCAN] parse {slug}: {e}")
            return None


# =============================================================================
# REGIME GUARD
# =============================================================================


class RegimeGuard:
    """Detects adverse market conditions: cascades, high vol, wide spreads."""

    def __init__(self, bn: BinanceFeed, books: BookFetcher):
        self.bn    = bn
        self.books = books
        self._casc_ts  = 0.0
        self._casc_dir = ""
        self._casc_mag = 0.0
        self._lock = threading.Lock()

    def check_cascade(self) -> Tuple[bool, str, float]:
        """Return (is_cascade, direction, magnitude_pct).
        Cascade = any asset moved >= REGIME_VOL_THRESHOLD% in 60s.
        Holds for REGIME_CASCADE_COOLDOWN seconds after detection.
        """
        now = time.time()
        with self._lock:
            if now - self._casc_ts < Config.REGIME_CASCADE_COOLDOWN:
                return (True, self._casc_dir, self._casc_mag)

        for asset in Config.ASSETS:
            sym = Config.SYM_BN.get(asset, "")
            move = self.bn.move(sym, 60)
            if move is None:
                continue
            if abs(move) >= Config.REGIME_VOL_THRESHOLD * 100:
                direction = "UP" if move > 0 else "DOWN"
                with self._lock:
                    self._casc_ts  = now
                    self._casc_dir = direction
                    self._casc_mag = abs(move)
                return (True, direction, abs(move))

        return (False, "", 0.0)

    def is_vol_ok(self, asset: str) -> bool:
        """True if 1-min move is below vol threshold."""
        sym  = Config.SYM_BN.get(asset, "")
        move = self.bn.move(sym, 60)
        if move is None:
            return True
        return abs(move) < Config.REGIME_VOL_THRESHOLD * 100

    def is_spread_ok(self, book: Book) -> bool:
        """True if bid-ask spread is within quoting range."""
        return book.spread <= Config.REGIME_SPREAD_MAX


# =============================================================================
# RISK MANAGER
# =============================================================================


class RiskManager:
    """P&L accounting, position sizing, daily limits."""

    def __init__(self):
        self._lock         = threading.Lock()
        self._wins         = 0
        self._losses       = 0
        self._pnl          = 0.0
        self._maker_pnl    = 0.0
        self._merge_pnl    = 0.0
        self._hedge_pnl    = 0.0
        self._trades       = 0
        self._maker_trades = 0
        self._merges       = 0
        self._hedges       = 0
        self._deployed     = 0.0
        self._daily_loss   = 0.0
        self._day_start    = int(time.time() // 86400) * 86400
        self._trades_log: List[dict] = []
        self.positions: Dict[str, dict] = {}   # slug → {up_cost, dn_cost, ...}

    # ── Stats ────────────────────────────────────────────────────────────────

    @property
    def stats(self) -> dict:
        with self._lock:
            return {
                "w":            self._wins,
                "l":            self._losses,
                "pnl":          self._pnl,
                "maker_pnl":    self._maker_pnl,
                "merge_pnl":    self._merge_pnl,
                "hedge_pnl":    self._hedge_pnl,
                "trades":       self._trades,
                "maker_trades": self._maker_trades,
                "merges":       self._merges,
                "hedges":       self._hedges,
            }

    # ── Guards ───────────────────────────────────────────────────────────────

    def can_enter(self, slug: str, cost: float) -> bool:
        self._maybe_reset_daily()
        with self._lock:
            if self._daily_loss   >= Config.MAX_DAILY_LOSS:   return False
            if self._deployed     >= Config.MAX_EXPOSURE:     return False
            if len(self.positions) >= Config.MAX_POSITIONS:   return False
            if self._trades       >= Config.MAX_DAILY_TRADES: return False
        return True

    # ── Mutations ────────────────────────────────────────────────────────────

    def open_position(self, slug: str, side: str, cost: float, shares: float):
        with self._lock:
            if slug not in self.positions:
                self.positions[slug] = {
                    "up_cost": 0.0, "dn_cost": 0.0,
                    "up_shares": 0.0, "dn_shares": 0.0,
                }
            if side == "up":
                self.positions[slug]["up_cost"]   += cost
                self.positions[slug]["up_shares"] += shares
            else:
                self.positions[slug]["dn_cost"]   += cost
                self.positions[slug]["dn_shares"] += shares
            self._deployed     += cost
            self._maker_trades += 1
            self._trades       += 1

    def record_merge(self, slug: str, shares: float, total_cost: float):
        proceeds = shares * 1.0
        profit   = proceeds - total_cost
        with self._lock:
            self._merge_pnl += profit
            self._pnl       += profit
            self._merges    += 1
            if profit >= 0:
                self._wins += 1
            else:
                self._losses     += 1
                self._daily_loss += abs(profit)
            self._deployed = max(0.0, self._deployed - total_cost)
            self.positions.pop(slug, None)
            self._trades_log.append({
                "ts": time.time(), "slug": slug, "type": "merge",
                "shares": shares, "cost": total_cost, "pnl": profit,
            })

    def record_hedge(self, slug: str, shares: float, cost: float,
                     proceeds: float):
        profit = proceeds - cost
        with self._lock:
            self._hedge_pnl  += profit
            self._pnl        += profit
            self._hedges     += 1
            if profit >= 0:
                self._wins += 1
            else:
                self._losses     += 1
                self._daily_loss += abs(profit)
            self._deployed = max(0.0, self._deployed - cost)
            self.positions.pop(slug, None)
            self._trades_log.append({
                "ts": time.time(), "slug": slug, "type": "hedge",
                "shares": shares, "cost": cost, "proceeds": proceeds,
                "pnl": profit,
            })

    def close_position(self, slug: str):
        with self._lock:
            pos  = self.positions.pop(slug, {})
            cost = pos.get("up_cost", 0.0) + pos.get("dn_cost", 0.0)
            self._deployed = max(0.0, self._deployed - cost)

    def save_trades(self):
        with self._lock:
            if not self._trades_log:
                return
            log_copy = list(self._trades_log)
            self._trades_log.clear()
        try:
            with open("trades.jsonl", "a") as f:
                for t in log_copy:
                    f.write(json.dumps(t) + "\n")
        except Exception as e:
            log.debug(f"[RISK] save_trades: {e}")

    def _maybe_reset_daily(self):
        day = int(time.time() // 86400) * 86400
        with self._lock:
            if day > self._day_start:
                self._day_start  = day
                self._daily_loss = 0.0
                self._trades     = 0


# =============================================================================
# PAIR TRACKER
# =============================================================================


class PairTracker:
    """Window slug → pair state machine dict.

    State dict keys (all accessed externally by bot.py):
      state, up_oid, dn_oid, up_filled, dn_filled,
      up_shares, dn_shares, up_cost, dn_cost,
      up_fill_px, dn_fill_px, leg1_side, leg1_fill_ts,
      last_escalation, entries
    """

    def __init__(self):
        self._windows: Dict[str, dict] = {}
        self._lock = threading.Lock()

    def get(self, slug: str) -> dict:
        with self._lock:
            if slug not in self._windows:
                self._windows[slug] = _new_pair()
            return dict(self._windows[slug])

    def update(self, slug: str, **kwargs):
        with self._lock:
            if slug not in self._windows:
                self._windows[slug] = _new_pair()
            self._windows[slug].update(kwargs)

    def cleanup(self, now_ts: int):
        """Remove COMPLETE windows older than 1 hour."""
        with self._lock:
            dead = [
                sl for sl, d in self._windows.items()
                if d["state"] == PairState.PAIR_COMPLETE
                and (now_ts - (slug_ts(sl) or now_ts)) > 3600
            ]
            for sl in dead:
                del self._windows[sl]


def _new_pair() -> dict:
    return {
        "state":           PairState.IDLE,
        "up_oid":          None,
        "dn_oid":          None,
        "up_filled":       False,
        "dn_filled":       False,
        "up_shares":       0.0,
        "dn_shares":       0.0,
        "up_cost":         0.0,
        "dn_cost":         0.0,
        "up_fill_px":      0.0,
        "dn_fill_px":      0.0,
        "leg1_side":       None,
        "leg1_fill_ts":    0.0,
        "last_escalation": 0.0,
        "entries":         0,
    }


# =============================================================================
# CROSS-WINDOW INTELLIGENCE
# =============================================================================


class CrossWindowIntel:
    """Tracks recent window outcomes to bias leg1 side selection."""

    def __init__(self):
        self._results: deque = deque(maxlen=20)
        self._xw_counts: Dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def record(self, result: WindowResult):
        with self._lock:
            self._results.append(result)

    def note_xw(self, slug: str):
        with self._lock:
            self._xw_counts[slug] += 1

    def get_bias(self, asset: str) -> Tuple[float, str]:
        """Return (strength 0..1, 'UP'|'DOWN') for asset.
        strength=0 means no bias (don't apply).
        """
        weights = Config.XW_WEIGHTS
        with self._lock:
            relevant = [
                r for r in reversed(self._results)
                if r.asset == asset
            ][:len(weights)]

        if not relevant:
            return (0.0, "")

        weighted = 0.0
        total_w  = 0.0
        for i, r in enumerate(relevant):
            w = weights[i] if i < len(weights) else 0.05
            weighted += w * r.move_pct
            total_w  += w

        if total_w == 0:
            return (0.0, "")

        avg = weighted / total_w
        mag = abs(avg)

        if mag < Config.XW_NOISE_FLOOR:
            return (0.0, "")

        strength = min(mag / Config.XW_NORMALIZER, 1.0) * Config.XW_MAX_STRENGTH
        if strength < Config.XW_MIN_APPLY:
            return (0.0, "")

        direction = "UP" if avg > 0 else "DOWN"
        return (strength, direction)


# =============================================================================
# MERGE ENGINE
# =============================================================================


class MergeEngine:
    """Execute or log native merge/redeem for completed pairs."""

    @staticmethod
    def log_merges(pairs: PairTracker, windows: List[MarketWindow]):
        """Paper: log potential merges without executing."""
        for w in windows:
            pt = pairs.get(w.slug)
            if not (pt["up_filled"] and pt["dn_filled"]):
                continue
            shares = min(pt["up_shares"], pt["dn_shares"])
            if shares < Config.MIN_SHARES:
                continue
            cost   = pt["up_cost"] + pt["dn_cost"]
            profit = shares - cost
            log.info(
                f"[MERGE] {w.slug} paper: {shares:.2f}sh "
                f"cost=${cost:.4f} → ${shares:.4f} pnl=${profit:+.4f}"
            )

    @staticmethod
    def execute_merges(
        pairs: PairTracker,
        windows: List[MarketWindow],
        exec_layer: ExecutionLayer,
        risk: RiskManager,
    ):
        for w in windows:
            pt = pairs.get(w.slug)
            if not (pt["up_filled"] and pt["dn_filled"]):
                continue
            shares = min(pt["up_shares"], pt["dn_shares"])
            if shares < Config.MIN_SHARES:
                continue

            cid = w.cid_up or w.cid_down
            ok  = exec_layer.merge_redeem(cid, shares)

            if not ok:
                # Fallback: sell both sides at near-$1
                exec_layer.sell_gtc(
                    w.tid_up,   shares,
                    Config.MERGE_SELL_FALLBACK_PRICE
                )
                exec_layer.sell_gtc(
                    w.tid_down, shares,
                    Config.MERGE_SELL_FALLBACK_PRICE
                )

            # Prorate cost to merged shares
            up_sh  = max(pt["up_shares"],  1e-9)
            dn_sh  = max(pt["dn_shares"],  1e-9)
            merged_cost = (
                pt["up_cost"]  * (shares / up_sh) +
                pt["dn_cost"]  * (shares / dn_sh)
            )
            risk.record_merge(w.slug, shares, merged_cost)
            pairs.update(
                w.slug,
                up_shares=pt["up_shares"] - shares,
                dn_shares=pt["dn_shares"] - shares,
                state=PairState.PAIR_COMPLETE,
            )


# =============================================================================
# SETTLEMENT
# =============================================================================


class Settlement:
    """Resolve expired windows: record outcome, close any open positions."""

    @staticmethod
    def resolve(
        pairs:   PairTracker,
        risk:    RiskManager,
        cl:      ChainlinkFeed,
        xw:      CrossWindowIntel,
        windows: List[MarketWindow],
    ):
        now = time.time()
        for w in windows:
            if w.left > 0:
                continue  # not expired yet

            pt = pairs.get(w.slug)
            if pt["state"] == PairState.PAIR_COMPLETE:
                continue

            # Record oracle outcome for CrossWindowIntel
            open_px  = cl.at(w.asset, w.start_ts, tol=5)
            close_px = cl.at(w.asset, w.end_ts,   tol=5)
            if open_px and close_px and open_px > 0:
                move_pct = (close_px - open_px) / open_px * 100
                outcome  = "UP" if close_px >= open_px else "DOWN"
                xw.record(WindowResult(
                    asset=w.asset, window=w.wmin,
                    outcome=outcome, move_pct=move_pct, ts=now,
                ))

            # Close any open position
            if pt["state"] != PairState.IDLE:
                risk.close_position(w.slug)
                pairs.update(w.slug, state=PairState.PAIR_COMPLETE)


# =============================================================================
# QUOTE ENGINE
# =============================================================================


class QuoteEngine:
    """Core state-machine: IDLE → LEG1_POSTED → LEG1_FILLED
                                → LEG2_POSTED → PAIR_COMPLETE
    """

    def __init__(
        self,
        bn:     BinanceFeed,
        cl:     ChainlinkFeed,
        books:  BookFetcher,
        exec:   ExecutionLayer,
        risk:   RiskManager,
        regime: RegimeGuard,
        pairs:  PairTracker,
        xw:     CrossWindowIntel,
    ):
        self.bn     = bn
        self.cl     = cl
        self.books  = books
        self.exec   = exec
        self.risk   = risk
        self.regime = regime
        self.pairs  = pairs
        self.xw     = xw

    # bot.py reads len(engine.quotes) and engine.quotes[sl]["state"]
    @property
    def quotes(self) -> Dict[str, dict]:
        return self.pairs._windows

    # ── Main tick ────────────────────────────────────────────────────────────

    def tick(self, windows: List[MarketWindow]):
        for w in windows:
            try:
                self._process(w)
            except Exception as e:
                log.debug(f"[QE] {w.slug}: {e}")
                import traceback; traceback.print_exc()

    # ── Per-window dispatch ──────────────────────────────────────────────────

    def _process(self, w: MarketWindow):
        left  = w.left
        pt    = self.pairs.get(w.slug)
        state = pt["state"]

        if left <= Config.CANCEL_ALL_LEFT:
            self._expire(w, pt)
            return

        if state == PairState.PAIR_COMPLETE:
            return

        if state == PairState.IDLE:
            if left > Config.STOP_QUOTING_LEFT:
                self._try_enter(w, pt)

        elif state == PairState.LEG1_POSTED:
            self._check_leg1(w, pt)

        elif state == PairState.LEG1_FILLED:
            self._try_leg2(w, pt)

        elif state == PairState.LEG2_POSTED:
            self._check_leg2(w, pt)

    # ── Edge / fair-value ────────────────────────────────────────────────────

    def _edge(self, w: MarketWindow) -> float:
        sym  = Config.SYM_BN.get(w.asset, "")
        move = self.bn.move(sym, 60) or 0.0
        vol_premium = abs(move) * Config.VOL_PREMIUM_FACTOR / 100.0

        spreads, n = 0.0, 0
        for tid in (w.tid_up, w.tid_down):
            b = self.books.get(tid)
            if b and b.spread < 1.0:
                spreads += b.spread; n += 1
        avg_spread = spreads / n if n else Config.QUOTE_EDGE_DEFAULT * 2

        edge = vol_premium + avg_spread * Config.EDGE_SPREAD_FRACTION
        return max(Config.QUOTE_EDGE_MIN, min(edge, Config.QUOTE_EDGE_MAX))

    def _fv(self, book: Optional[Book]) -> float:
        if not book or not book.bids or not book.asks:
            return 0.5
        return round(max(0.02, min(0.98, (book.bb + book.ba) / 2)), 2)

    # ── IDLE → LEG1_POSTED ──────────────────────────────────────────────────

    def _try_enter(self, w: MarketWindow, pt: dict):
        if not self.risk.can_enter(w.slug, Config.QUOTE_STAKE):
            return

        casc, _, _ = self.regime.check_cascade()
        if casc:
            return

        if not self.regime.is_vol_ok(w.asset):
            return

        b_up = self.books.get(w.tid_up)
        b_dn = self.books.get(w.tid_down)
        if not b_up or not b_dn:
            return
        if not self.regime.is_spread_ok(b_up) or not self.regime.is_spread_ok(b_dn):
            return

        fv_up = self._fv(b_up)
        fv_dn = self._fv(b_dn)

        # Imbalance guard
        total = fv_up + fv_dn
        if total > 0 and abs(fv_up - fv_dn) / total > Config.MAX_IMBALANCE:
            return

        edge = self._edge(w)

        # Cross-window bias: enter contra the recent trend
        strength, bias_dir = self.xw.get_bias(w.asset)
        if strength > 0 and bias_dir:
            leg1_side = "dn" if bias_dir == "UP" else "up"
        else:
            # Enter the cheaper side (lower fair value = more discounted)
            leg1_side = "up" if fv_up <= fv_dn else "dn"

        tid   = w.tid_up   if leg1_side == "up" else w.tid_down
        fv    = fv_up      if leg1_side == "up" else fv_dn
        price = round(fv - edge, 2)

        if price < 0.01 or price >= 1.0:
            return

        oid = self.exec.buy_gtc(tid, Config.QUOTE_STAKE, price)
        if not oid:
            return

        self.pairs.update(
            w.slug,
            state     = PairState.LEG1_POSTED,
            leg1_side = leg1_side,
            **{"up_oid": oid} if leg1_side == "up" else {"dn_oid": oid},
        )
        log.debug(
            f"[QE] LEG1 {w.slug} {leg1_side.upper()} "
            f"@{price:.2f} edge={edge*100:.1f}¢ oid={oid[:12]}.."
        )

    # ── LEG1_POSTED → LEG1_FILLED ───────────────────────────────────────────

    def _check_leg1(self, w: MarketWindow, pt: dict):
        side = pt["leg1_side"]
        oid  = pt["up_oid"] if side == "up" else pt["dn_oid"]
        if not oid:
            self.pairs.update(w.slug, state=PairState.IDLE)
            return

        filled, fill_px = self.exec.check_fill(oid)
        if not filled:
            self._maybe_repost(w, pt, side, oid)
            return

        if fill_px <= 0:
            fill_px = (self.books.get(
                w.tid_up if side == "up" else w.tid_down
            ) or Book()).ba or 0.5

        shares = Config.QUOTE_STAKE / fill_px
        cost   = Config.QUOTE_STAKE
        self.risk.open_position(w.slug, side, cost, shares)

        updates: dict = {
            "state":        PairState.LEG1_FILLED,
            "leg1_fill_ts": time.time(),
            "entries":      pt["entries"] + 1,
        }
        if side == "up":
            updates.update(up_filled=True, up_shares=shares,
                           up_cost=cost, up_fill_px=fill_px)
        else:
            updates.update(dn_filled=True, dn_shares=shares,
                           dn_cost=cost, dn_fill_px=fill_px)

        self.pairs.update(w.slug, **updates)
        log.info(
            f"[QE] LEG1 FILL {w.slug} {side.upper()} "
            f"@{fill_px:.2f} {shares:.2f}sh"
        )

    def _maybe_repost(self, w: MarketWindow, pt: dict, side: str, oid: str):
        """Cancel + repost leg1 if price drifted > REPOST_THRESHOLD."""
        tid  = w.tid_up if side == "up" else w.tid_down
        book = self.books.get(tid)
        if not book:
            return

        o = self.exec._orders.get(oid)
        if not o:
            return
        order_px = o.get("price", 0.0)

        target = round(self._fv(book) - self._edge(w), 2)
        if abs(target - order_px) <= Config.QUOTE_REPOST_THRESHOLD:
            return
        if target < 0.01 or target >= 1.0:
            return

        self.exec.cancel_order(oid)
        new_oid = self.exec.buy_gtc(tid, Config.QUOTE_STAKE, target)
        if new_oid:
            key = "up_oid" if side == "up" else "dn_oid"
            self.pairs.update(w.slug, **{key: new_oid})

    # ── LEG1_FILLED → LEG2_POSTED ───────────────────────────────────────────

    def _try_leg2(self, w: MarketWindow, pt: dict):
        now  = time.time()
        left = w.left
        leg1 = pt["leg1_side"]
        leg2 = "dn" if leg1 == "up" else "up"
        tid  = w.tid_up if leg2 == "up" else w.tid_down
        book = self.books.get(tid)
        if not book:
            return

        elapsed = now - pt["leg1_fill_ts"]
        edge    = self._edge(w)
        fv      = self._fv(book)

        # Orphan hedge: time running out and leg2 not filled
        if left <= w.orphan_hedge_left:
            self._orphan_hedge(w, pt, leg1)
            return

        # Escalation phases
        if elapsed < Config.COMPLETION_PASSIVE_SEC:
            price = round(fv - edge, 2)
        elif elapsed < Config.COMPLETION_AGGRESSIVE_SEC:
            price = round(fv - Config.COMPLETION_MIN_EDGE, 2)
        elif elapsed >= Config.COMPLETION_FAK_SEC:
            self._fak_leg2(w, pt, leg2, tid, book)
            return
        else:
            price = round(fv - Config.COMPLETION_MIN_EDGE, 2)

        # Cooldown between escalations (B6)
        if now - pt["last_escalation"] < Config.ESCALATION_COOLDOWN_SEC:
            return

        if price < 0.01 or price >= 1.0:
            return

        oid = self.exec.buy_gtc(tid, Config.QUOTE_STAKE, price)
        if not oid:
            return

        key = "up_oid" if leg2 == "up" else "dn_oid"
        self.pairs.update(
            w.slug,
            state=PairState.LEG2_POSTED,
            last_escalation=now,
            **{key: oid},
        )
        log.debug(
            f"[QE] LEG2 {w.slug} {leg2.upper()} @{price:.2f} "
            f"elapsed={elapsed:.0f}s"
        )

    def _fak_leg2(self, w: MarketWindow, pt: dict, leg2: str,
                  tid: str, book: Book):
        """FAK escalation — cross spread to complete the pair immediately."""
        price = min(round(book.ba + 0.01, 2), 0.99)
        oid   = self.exec.buy_fak(tid, Config.QUOTE_STAKE, price)
        if not oid:
            return

        filled, fill_px = self.exec.check_fill(oid)
        if not filled:
            return

        if fill_px <= 0:
            fill_px = price
        shares = Config.QUOTE_STAKE / fill_px
        self.risk.open_position(w.slug, leg2, Config.QUOTE_STAKE, shares)

        updates: dict = {"state": PairState.PAIR_COMPLETE}
        if leg2 == "up":
            updates.update(up_filled=True, up_shares=shares,
                           up_cost=Config.QUOTE_STAKE, up_fill_px=fill_px)
        else:
            updates.update(dn_filled=True, dn_shares=shares,
                           dn_cost=Config.QUOTE_STAKE, dn_fill_px=fill_px)
        self.pairs.update(w.slug, **updates)
        log.info(
            f"[QE] FAK FILL {w.slug} {leg2.upper()} @{fill_px:.2f} → COMPLETE"
        )

    # ── LEG2_POSTED → PAIR_COMPLETE ─────────────────────────────────────────

    def _check_leg2(self, w: MarketWindow, pt: dict):
        leg1 = pt["leg1_side"]
        leg2 = "dn" if leg1 == "up" else "up"
        oid  = pt["up_oid"] if leg2 == "up" else pt["dn_oid"]
        if not oid:
            self.pairs.update(w.slug, state=PairState.LEG1_FILLED)
            return

        filled, fill_px = self.exec.check_fill(oid)
        if not filled:
            # Timed out → cancel and re-evaluate
            now     = time.time()
            elapsed = now - pt["last_escalation"]
            if elapsed > Config.COMPLETION_AGGRESSIVE_SEC:
                self.exec.cancel_order(oid)
                key = "up_oid" if leg2 == "up" else "dn_oid"
                self.pairs.update(
                    w.slug, state=PairState.LEG1_FILLED,
                    **{key: None},
                )
            return

        if fill_px <= 0:
            fill_px = 0.5
        shares = Config.QUOTE_STAKE / fill_px
        self.risk.open_position(w.slug, leg2, Config.QUOTE_STAKE, shares)

        updates: dict = {"state": PairState.PAIR_COMPLETE}
        if leg2 == "up":
            updates.update(up_filled=True, up_shares=shares,
                           up_cost=Config.QUOTE_STAKE, up_fill_px=fill_px)
        else:
            updates.update(dn_filled=True, dn_shares=shares,
                           dn_cost=Config.QUOTE_STAKE, dn_fill_px=fill_px)
        self.pairs.update(w.slug, **updates)
        log.info(
            f"[QE] LEG2 FILL {w.slug} {leg2.upper()} @{fill_px:.2f} → COMPLETE"
        )

    # ── Orphan hedge ─────────────────────────────────────────────────────────

    def _orphan_hedge(self, w: MarketWindow, pt: dict, leg1_side: str):
        """Near expiry with only one leg filled — sell to recover capital."""
        if leg1_side == "up":
            shares, cost, tid = pt["up_shares"], pt["up_cost"], w.tid_up
        else:
            shares, cost, tid = pt["dn_shares"], pt["dn_cost"], w.tid_down

        if shares < Config.MIN_SHARES:
            self.pairs.update(w.slug, state=PairState.PAIR_COMPLETE)
            return

        book = self.books.get(tid)
        if not book or not book.bids:
            self.pairs.update(w.slug, state=PairState.PAIR_COMPLETE)
            return

        sell_px    = round(book.bb - 0.01, 2)
        min_accept = cost / max(shares, 1e-9) * Config.HEDGE_MIN_RECOVERY

        if sell_px < min_accept or sell_px < 0.01:
            log.debug(
                f"[QE] HEDGE SKIP {w.slug} {leg1_side} "
                f"sell={sell_px:.2f} < min={min_accept:.2f}"
            )
            self.pairs.update(w.slug, state=PairState.PAIR_COMPLETE)
            return

        oid = self.exec.sell_gtc(tid, shares, sell_px)
        if oid:
            proceeds = shares * sell_px
            self.risk.record_hedge(w.slug, shares, cost, proceeds)
            log.info(
                f"[QE] HEDGE {w.slug} {leg1_side.upper()} "
                f"{shares:.2f}sh @{sell_px:.2f} pnl=${proceeds - cost:+.4f}"
            )
        self.pairs.update(w.slug, state=PairState.PAIR_COMPLETE)

    # ── Expire window ────────────────────────────────────────────────────────

    def _expire(self, w: MarketWindow, pt: dict):
        """Cancel any resting orders when window is about to close."""
        oids = [o for o in (pt["up_oid"], pt["dn_oid"]) if o]
        if oids:
            self.exec.cancel_batch(oids)
            self.pairs.update(w.slug, up_oid=None, dn_oid=None)
