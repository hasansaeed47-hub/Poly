#!/usr/bin/env python3
"""
Split-Sell Arbitrage Paper Trading Bot for Polymarket

Strategy: When YES_bid + NO_bid > $1.00 (after fees), simulate:
  1. Split $X USDC → X YES tokens + X NO tokens  (gasless via relayer)
  2. Sell X YES tokens at YES_bid
  3. Sell X NO tokens at NO_bid
  4. Profit = sell_proceeds - split_cost - fees

WebSocket-driven: Real-time book updates via CLOB WS + 500ms arb scan tick.
REST discovery only for finding new markets (~60s interval).
Paper trading only — no wallet, no signing, no real orders.
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

import aiohttp
import tomli

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config(path: str = "config.toml") -> dict:
    with open(path, "rb") as f:
        return tomli.load(f)

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class BookSide:
    """One side of an order book — list of (price, size) levels."""
    levels: list[tuple[float, float]] = field(default_factory=list)

    @property
    def best(self) -> float:
        return self.levels[0][0] if self.levels else 0.0

    def fillable_at(self, depth_usd: float) -> tuple[float, float]:
        """Walk the book to fill `depth_usd` worth. Returns (avg_price, filled_qty)."""
        remaining = depth_usd
        total_cost = 0.0
        total_qty = 0.0
        for price, size in self.levels:
            fill_qty = min(size, remaining / price) if price > 0 else 0
            if fill_qty <= 0:
                break
            total_cost += fill_qty * price
            total_qty += fill_qty
            remaining -= fill_qty * price
            if remaining <= 0.01:
                break
        avg = total_cost / total_qty if total_qty > 0 else 0.0
        return avg, total_qty

    def apply_snapshot(self, levels: list[tuple[float, float]], is_bids: bool):
        """Replace entire side from a full snapshot."""
        self.levels = [(p, s) for p, s in levels if s > 0]
        if is_bids:
            self.levels.sort(key=lambda x: -x[0])  # highest first
        else:
            self.levels.sort(key=lambda x: x[0])    # lowest first

    def apply_delta(self, price: float, size: float, is_bids: bool):
        """Apply a single level delta: size=0 removes, size>0 upserts."""
        # Remove existing level at this price
        self.levels = [(p, s) for p, s in self.levels if abs(p - price) > 1e-10]
        if size > 0:
            self.levels.append((price, size))
            if is_bids:
                self.levels.sort(key=lambda x: -x[0])
            else:
                self.levels.sort(key=lambda x: x[0])


@dataclass
class Market:
    """A binary market with YES/NO token pair."""
    condition_id: str
    question: str
    slug: str
    token_yes: str
    token_no: str
    yes_bids: BookSide = field(default_factory=BookSide)
    no_bids: BookSide = field(default_factory=BookSide)
    yes_asks: BookSide = field(default_factory=BookSide)
    no_asks: BookSide = field(default_factory=BookSide)
    last_update: float = 0.0


@dataclass
class PaperTrade:
    """Record of a simulated split-sell arbitrage."""
    ts: float
    market: str
    question: str
    stake: float          # USDC split
    yes_sell_price: float  # avg fill price
    no_sell_price: float
    yes_fee: float
    no_fee: float
    gross: float          # yes_proceeds + no_proceeds
    net: float            # gross - stake
    profit: float         # net - fees
    entry_num: int = 0    # which entry this is on this market (1st, 2nd, etc.)
    capture_latency_ms: float = 0.0  # ms from first sum>1.0 detection to execution

# ---------------------------------------------------------------------------
# Fee model — Polymarket's actual fee curve
# ---------------------------------------------------------------------------

def taker_fee(price: float, max_rate: float) -> float:
    """
    Polymarket fee: rate * price * (1 - price)
    Peaks at 50c (max_rate * 0.25), zero at 0c and 100c.
    """
    effective_rate = min(max_rate, 2 * max_rate * min(price, 1 - price))
    return price * effective_rate


def compute_arb(
    yes_bid: float,
    no_bid: float,
    stake: float,
    max_fee_rate: float,
    slippage_bps: int,
) -> Optional[PaperTrade]:
    """Check if a split-sell arb is profitable."""
    if yes_bid <= 0 or no_bid <= 0:
        return None

    slip = slippage_bps / 10000
    yes_fill = yes_bid * (1 - slip)
    no_fill = no_bid * (1 - slip)

    yes_proceeds = stake * yes_fill
    no_proceeds = stake * no_fill
    gross = yes_proceeds + no_proceeds

    yes_fee = stake * taker_fee(yes_fill, max_fee_rate)
    no_fee = stake * taker_fee(no_fill, max_fee_rate)
    total_fees = yes_fee + no_fee

    net = gross - stake
    profit = net - total_fees

    if profit <= 0:
        return None

    return PaperTrade(
        ts=time.time(),
        market="",
        question="",
        stake=stake,
        yes_sell_price=yes_fill,
        no_sell_price=no_fill,
        yes_fee=yes_fee,
        no_fee=no_fee,
        gross=gross,
        net=net,
        profit=profit,
    )

# ---------------------------------------------------------------------------
# WebSocket book feed — real-time order book updates
# ---------------------------------------------------------------------------

class BookFeed:
    """
    WebSocket connection to Polymarket CLOB for real-time book updates.
    Mirrors the Rust feeds.rs implementation:
      - Subscribes via market channel with custom_feature_enabled
      - Handles best_bid_ask, book (snapshot), and price_change (delta) events
      - Maintains full order book state per token with proper delta application
      - Automatic reconnection with backoff
    """

    def __init__(self, ws_url: str, markets: dict[str, "Market"]):
        self.ws_url = ws_url
        self.markets = markets  # condition_id → Market (shared ref)
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._token_to_market: dict[str, tuple[str, str]] = {}  # token → (cond_id, "yes"/"no")
        self._subscribed_tokens: set[str] = set()
        self._msg_count = 0
        self._connected = False
        self._update_count = 0  # total book updates received

    def _rebuild_token_map(self):
        """Rebuild token→market mapping from current markets dict."""
        self._token_to_market.clear()
        for cid, m in self.markets.items():
            self._token_to_market[m.token_yes] = (cid, "yes")
            self._token_to_market[m.token_no] = (cid, "no")

    async def run(self):
        """Main loop: connect, subscribe, process messages. Auto-reconnects."""
        while True:
            try:
                await self._connect_and_run()
            except Exception as e:
                logging.error("[BOOK-WS] Error: %s, reconnecting in 5s", e)
            self._connected = False
            await asyncio.sleep(5)

    async def _connect_and_run(self):
        """Single connection lifecycle."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()

        logging.info("[BOOK-WS] Connecting to %s", self.ws_url)
        self._ws = await self._session.ws_connect(
            self.ws_url,
            timeout=10.0,
            heartbeat=30,
        )
        self._connected = True
        self._msg_count = 0
        logging.info("[BOOK-WS] Connected")

        # Subscribe to all known tokens
        await self._subscribe_all()

        # Ping every 10s (Polymarket CLOB WS keepalive)
        ping_task = asyncio.create_task(self._ping_loop())

        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    text = msg.data
                    if text in ("pong", "PONG"):
                        continue
                    self._msg_count += 1
                    if self._msg_count <= 3:
                        logging.debug("[BOOK-WS] msg #%d: %s", self._msg_count, text[:500])

                    # CLOB WS may send JSON arrays
                    if text.startswith("["):
                        try:
                            for v in json.loads(text):
                                self._process_message(v)
                        except json.JSONDecodeError:
                            pass
                    else:
                        try:
                            self._process_message(json.loads(text))
                        except json.JSONDecodeError:
                            pass

                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    logging.warning("[BOOK-WS] Connection closed/error")
                    break
        finally:
            ping_task.cancel()
            try:
                await ping_task
            except asyncio.CancelledError:
                pass

    async def _ping_loop(self):
        """Send ping every 10s to keep connection alive."""
        while True:
            await asyncio.sleep(10)
            if self._ws and not self._ws.closed:
                try:
                    await self._ws.send_str("ping")
                except Exception:
                    break

    async def _subscribe_all(self):
        """Subscribe to all token IDs from tracked markets."""
        self._rebuild_token_map()
        all_tokens = list(self._token_to_market.keys())
        if not all_tokens:
            return

        # Polymarket limits ~500 per subscription
        for i in range(0, len(all_tokens), 500):
            batch = all_tokens[i:i+500]
            sub = {
                "type": "market",
                "assets_ids": batch,
                "custom_feature_enabled": True,
            }
            if self._ws and not self._ws.closed:
                await self._ws.send_json(sub)
                logging.info("[BOOK-WS] Subscribed to %d tokens (batch %d)", len(batch), i // 500 + 1)

        self._subscribed_tokens = set(all_tokens)

    async def subscribe_new_tokens(self, new_tokens: list[str]):
        """Subscribe to newly discovered tokens without reconnecting."""
        if not new_tokens or not self._ws or self._ws.closed:
            return
        self._rebuild_token_map()
        unsub = [t for t in new_tokens if t not in self._subscribed_tokens]
        if not unsub:
            return

        for i in range(0, len(unsub), 500):
            batch = unsub[i:i+500]
            sub = {
                "type": "market",
                "assets_ids": batch,
                "custom_feature_enabled": True,
            }
            await self._ws.send_json(sub)
            logging.info("[BOOK-WS] Subscribed to %d new tokens", len(batch))

        self._subscribed_tokens.update(unsub)

    def _process_message(self, v: dict):
        """Process a single WS message — mirrors Rust process_book_message."""
        event_type = v.get("event_type", "")
        now = time.time()

        if event_type == "best_bid_ask":
            # Fast top-of-book update (custom_feature_enabled)
            # IMPORTANT: Only update the best price at level[0], do NOT inject
            # fake sizes — that corrupts fillable_at() depth calculations.
            # Full book depth is maintained by "book" + "price_change" events.
            asset_id = v.get("asset_id", "")
            if not asset_id or asset_id not in self._token_to_market:
                return

            best_bid = self._parse_float(v.get("best_bid"))
            best_ask = self._parse_float(v.get("best_ask"))

            cid, side = self._token_to_market[asset_id]
            m = self.markets.get(cid)
            if not m:
                return

            if side == "yes":
                if best_bid and best_bid > 0:
                    self._update_top_of_book(m.yes_bids, best_bid, is_bids=True)
                if best_ask and best_ask > 0:
                    self._update_top_of_book(m.yes_asks, best_ask, is_bids=False)
            else:
                if best_bid and best_bid > 0:
                    self._update_top_of_book(m.no_bids, best_bid, is_bids=True)
                if best_ask and best_ask > 0:
                    self._update_top_of_book(m.no_asks, best_ask, is_bids=False)

            m.last_update = now
            self._update_count += 1

        elif event_type == "book":
            # Full snapshot
            asset_id = v.get("asset_id", "")
            if not asset_id or asset_id not in self._token_to_market:
                return

            cid, side = self._token_to_market[asset_id]
            m = self.markets.get(cid)
            if not m:
                return

            asks = self._parse_levels(v.get("asks"))
            bids = self._parse_levels(v.get("bids"))

            if side == "yes":
                m.yes_bids.apply_snapshot(bids, is_bids=True)
                m.yes_asks.apply_snapshot(asks, is_bids=False)
            else:
                m.no_bids.apply_snapshot(bids, is_bids=True)
                m.no_asks.apply_snapshot(asks, is_bids=False)

            m.last_update = now
            self._update_count += 1

        elif event_type == "price_change":
            # Delta updates — handle both new and old Polymarket formats

            # New format: price_changes array with per-entry asset_id
            price_changes = v.get("price_changes")
            if price_changes and isinstance(price_changes, list):
                for change in price_changes:
                    asset_id = change.get("asset_id", "")
                    if asset_id and asset_id in self._token_to_market:
                        self._apply_change(asset_id, change, now)
                return

            # Old format: changes array with message-level asset_id
            changes = v.get("changes")
            asset_id = v.get("asset_id", "")
            if changes and isinstance(changes, list) and asset_id and asset_id in self._token_to_market:
                for change in changes:
                    self._apply_change(asset_id, change, now)

    @staticmethod
    def _update_top_of_book(book_side: BookSide, new_best: float, is_bids: bool):
        """Update only the best price without injecting fake depth.

        If the book already has levels, update the price at level[0] while
        preserving its real size. If the book is empty (no snapshot yet),
        insert a minimal placeholder — the next 'book' or 'price_change'
        event will replace it with real depth.
        """
        if book_side.levels:
            old_best = book_side.levels[0][0]
            if abs(old_best - new_best) > 1e-10:
                # Price changed — update level[0] price, keep its real size
                old_size = book_side.levels[0][1]
                book_side.levels[0] = (new_best, old_size)
                # Re-sort in case the new best crossed other levels
                if is_bids:
                    book_side.levels.sort(key=lambda x: -x[0])
                else:
                    book_side.levels.sort(key=lambda x: x[0])
        else:
            # No book yet — insert placeholder with small size so .best works
            # but fillable_at() won't think there's real depth
            book_side.levels = [(new_best, 0.01)]

    def _apply_change(self, asset_id: str, change: dict, now: float):
        """Apply a single price_change entry to book state."""
        price = self._parse_float(change.get("price"))
        size = self._parse_float(change.get("size"))
        side = change.get("side", "")

        if price is None or size is None:
            return

        is_ask = side.upper() in ("SELL", "ASK")
        cid, token_side = self._token_to_market[asset_id]
        m = self.markets.get(cid)
        if not m:
            return

        if token_side == "yes":
            if is_ask:
                m.yes_asks.apply_delta(price, size, is_bids=False)
            else:
                m.yes_bids.apply_delta(price, size, is_bids=True)
        else:
            if is_ask:
                m.no_asks.apply_delta(price, size, is_bids=False)
            else:
                m.no_bids.apply_delta(price, size, is_bids=True)

        m.last_update = now
        self._update_count += 1

    @staticmethod
    def _parse_float(val) -> Optional[float]:
        if val is None:
            return None
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            try:
                return float(val)
            except ValueError:
                return None
        return None

    @staticmethod
    def _parse_levels(val) -> list[tuple[float, float]]:
        if not val or not isinstance(val, list):
            return []
        levels = []
        for l in val:
            try:
                p = float(l.get("price", 0)) if isinstance(l.get("price"), (int, float)) else float(l.get("price", "0"))
                s = float(l.get("size", 0)) if isinstance(l.get("size"), (int, float)) else float(l.get("size", "0"))
                if p > 0 and s > 0:
                    levels.append((p, s))
            except (ValueError, TypeError, AttributeError):
                continue
        return levels


# ---------------------------------------------------------------------------
# API client (REST — discovery only)
# ---------------------------------------------------------------------------

class PolyClient:
    """REST client for market discovery via Gamma API + initial book snapshots."""

    def __init__(self, cfg: dict):
        self.gamma_api = cfg["api"]["gamma_api"]
        self.clob_rest = cfg["api"]["clob_rest"]
        self.throttle = cfg["api"]["throttle_ms"] / 1000.0
        self._last_call = 0.0
        self._session: Optional[aiohttp.ClientSession] = None

        # Market filters (from [scan] section)
        self.slug_keywords = [k.lower() for k in cfg["scan"].get("slug_keywords", [])]
        self.time_keywords = [k.lower() for k in cfg["scan"].get("time_keywords", [])]

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
                headers={"Accept": "application/json"},
            )
        return self._session

    async def _throttle(self):
        elapsed = time.time() - self._last_call
        if elapsed < self.throttle:
            await asyncio.sleep(self.throttle - elapsed)
        self._last_call = time.time()

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def discover_markets(self, limit: int = 50, offset: int = 0) -> list[Market]:
        """Fetch active binary markets from Gamma API."""
        await self._throttle()
        session = await self._get_session()

        params = {
            "limit": str(limit),
            "offset": str(offset),
            "active": "true",
            "closed": "false",
            "order": "volume24hr",
            "ascending": "false",
        }

        try:
            async with session.get(
                f"{self.gamma_api}/events", params=params
            ) as resp:
                if resp.status != 200:
                    logging.warning("Gamma /events returned %d", resp.status)
                    return []
                events = await resp.json()
        except Exception as e:
            logging.error("Gamma /events failed: %s", e)
            return []

        markets = []
        for event in events:
            event_markets = event.get("markets", [])
            for m in event_markets:
                outcomes = m.get("outcomes", "")
                if isinstance(outcomes, str):
                    try:
                        outcomes = json.loads(outcomes)
                    except (json.JSONDecodeError, TypeError):
                        continue

                if not isinstance(outcomes, list) or len(outcomes) != 2:
                    continue

                tokens = m.get("clobTokenIds", "")
                if isinstance(tokens, str):
                    try:
                        tokens = json.loads(tokens)
                    except (json.JSONDecodeError, TypeError):
                        continue

                if not isinstance(tokens, list) or len(tokens) != 2:
                    continue

                cond_id = m.get("conditionId", "")
                question = m.get("question", event.get("title", ""))
                slug = event.get("slug", m.get("slug", ""))

                if not cond_id or not tokens[0] or not tokens[1]:
                    continue

                # -- Filter by slug/question keywords --
                match_text = f"{slug} {question}".lower()
                if self.slug_keywords:
                    if not any(kw in match_text for kw in self.slug_keywords):
                        if not hasattr(self, '_logged_skips'):
                            self._logged_skips = 0
                        if self._logged_skips < 20:
                            logging.debug("SKIP (no crypto kw): %s | %s", slug[:50], question[:60])
                            self._logged_skips += 1
                        continue
                if self.time_keywords:
                    if not any(kw in match_text for kw in self.time_keywords):
                        continue

                o_lower = [o.lower() for o in outcomes]
                if "yes" in o_lower:
                    yes_idx = o_lower.index("yes")
                    no_idx = 1 - yes_idx
                elif "up" in o_lower:
                    yes_idx = o_lower.index("up")
                    no_idx = 1 - yes_idx
                else:
                    yes_idx, no_idx = 0, 1

                logging.info("MATCH: %s | %s", slug[:50], question[:60])
                markets.append(Market(
                    condition_id=cond_id,
                    question=question[:120],
                    slug=slug,
                    token_yes=tokens[yes_idx],
                    token_no=tokens[no_idx],
                ))

        return markets

    async def discover_crypto_windows(self, quiet: bool = False) -> list[Market]:
        """
        Discover active crypto Up/Down time-window markets.
        Fires all slug lookups concurrently — ~1 request round-trip instead of 84 sequential.
        Only queries assets that actually have updown markets on Polymarket.
        """
        session = await self._get_session()
        now = int(time.time())
        # Only these assets have updown markets on Polymarket
        assets = ["btc", "eth", "sol", "xrp"]
        windows = [5, 15, 60]  # minutes

        # Build all slugs to query
        slugs = []
        for asset in assets:
            for wm in windows:
                iv = wm * 60
                s0 = (now // iv) * iv
                # s0 = current open window; s0+iv = next (not yet started)
                # Include current + next so we subscribe early
                for st in [s0, s0 + iv]:
                    if st + iv < now:       # already ended
                        continue
                    slugs.append(f"{asset}-updown-{wm}m-{st}")

        # Fire all requests concurrently
        async def _fetch_slug(slug: str):
            try:
                async with session.get(
                    f"{self.gamma_api}/markets",
                    params={"slug": slug},
                ) as resp:
                    if resp.status != 200:
                        return None
                    return (slug, await resp.json())
            except Exception:
                return None

        results = await asyncio.gather(*[_fetch_slug(s) for s in slugs])

        markets = []
        for result in results:
            if result is None:
                continue
            slug, data = result

            if isinstance(data, list):
                if not data:
                    continue
                m = data[0]
            elif isinstance(data, dict):
                m = data
            else:
                continue

            tokens = m.get("clobTokenIds", "")
            if isinstance(tokens, str):
                try:
                    tokens = json.loads(tokens)
                except (json.JSONDecodeError, TypeError):
                    continue

            if not isinstance(tokens, list) or len(tokens) < 2:
                continue

            outcomes = m.get("outcomes", "")
            if isinstance(outcomes, str):
                try:
                    outcomes = json.loads(outcomes)
                except (json.JSONDecodeError, TypeError):
                    continue

            if not isinstance(outcomes, list) or len(outcomes) != 2:
                continue

            cond_id = m.get("conditionId", "")
            question = m.get("question", "")
            if not cond_id or not tokens[0] or not tokens[1]:
                continue

            o_lower = [o.lower() for o in outcomes]
            if "up" in o_lower:
                yes_idx = o_lower.index("up")
                no_idx = 1 - yes_idx
            elif "yes" in o_lower:
                yes_idx = o_lower.index("yes")
                no_idx = 1 - yes_idx
            elif "down" in o_lower:
                no_idx = o_lower.index("down")
                yes_idx = 1 - no_idx
            else:
                yes_idx, no_idx = 0, 1

            if not quiet:
                logging.info("WINDOW: %s | %s", slug, question[:60])
            markets.append(Market(
                condition_id=cond_id,
                question=question[:120],
                slug=slug,
                token_yes=tokens[yes_idx],
                token_no=tokens[no_idx],
            ))

        return markets

    async def fetch_books(self, token_ids: list[str]) -> dict[str, dict]:
        """Fetch order books for a batch of token IDs (REST fallback)."""
        if not token_ids:
            return {}

        await self._throttle()
        session = await self._get_session()

        body = [{"token_id": tid} for tid in token_ids]

        try:
            async with session.post(
                f"{self.clob_rest}/books", json=body
            ) as resp:
                if resp.status != 200:
                    logging.warning("CLOB /books returned %d", resp.status)
                    return {}
                data = await resp.json()
        except Exception as e:
            logging.error("CLOB /books failed: %s", e)
            return {}

        result = {}
        if not isinstance(data, list):
            return {}

        for item in data:
            tid = item.get("asset_id", "")
            if not tid:
                continue

            bids = []
            for level in item.get("bids", []):
                try:
                    p = float(level.get("price", 0))
                    s = float(level.get("size", 0))
                    if p > 0 and s > 0:
                        bids.append((p, s))
                except (ValueError, TypeError):
                    continue
            bids.sort(key=lambda x: -x[0])

            asks = []
            for level in item.get("asks", []):
                try:
                    p = float(level.get("price", 0))
                    s = float(level.get("size", 0))
                    if p > 0 and s > 0:
                        asks.append((p, s))
                except (ValueError, TypeError):
                    continue
            asks.sort(key=lambda x: x[0])

            result[tid] = {"bids": bids, "asks": asks}

        return result


# ---------------------------------------------------------------------------
# Paper trading engine
# ---------------------------------------------------------------------------

class PaperEngine:
    """Tracks bankroll, executes paper trades, logs everything."""

    def __init__(self, cfg: dict):
        self.bankroll = cfg["arb"]["bankroll"]
        self.max_stake = cfg["arb"]["max_stake"]
        self.min_profit_cents = cfg["arb"]["min_profit_cents"]
        self.max_fee_rate = cfg["arb"]["max_taker_fee_rate"]
        self.slippage_bps = cfg["arb"]["slippage_bps"]
        self.exec_delay = cfg["arb"]["exec_delay_secs"]
        self.min_depth = cfg["scan"]["min_depth_usd"]

        self.total_trades = 0
        self.total_profit = 0.0
        self.total_volume = 0.0
        self.session_start = time.time()

        # Cooldown: don't re-arb the same market within N seconds
        self.cooldown_secs = cfg["arb"].get("cooldown_secs", 5.0)
        self._last_trade_ts: dict[str, float] = {}  # condition_id → last trade time

        # Capture latency: track when sum first exceeded 1.0 per market
        self._first_seen: dict[str, float] = {}  # condition_id → ts of first arb signal

        # Multi-entry tracking per market
        self._market_entries: dict[str, int] = defaultdict(int)    # condition_id → entry count
        self._market_profit: dict[str, float] = defaultdict(float) # condition_id → cumulative profit
        self.max_entries_per_market = cfg["arb"].get("max_entries_per_market", 10)  # safety cap

        # Log files
        trade_path = Path(cfg["logging"]["trade_log"])
        scan_path = Path(cfg["logging"]["scan_log"])
        trade_path.parent.mkdir(parents=True, exist_ok=True)
        scan_path.parent.mkdir(parents=True, exist_ok=True)
        self._trade_log = open(trade_path, "a")
        self._scan_log = open(scan_path, "a")

    def close(self):
        self._trade_log.close()
        self._scan_log.close()

    def _log_scan(self, record: dict):
        self._scan_log.write(json.dumps(record) + "\n")
        self._scan_log.flush()

    def _log_trade(self, trade: PaperTrade):
        self._trade_log.write(json.dumps(asdict(trade)) + "\n")
        self._trade_log.flush()

    def on_cooldown(self, condition_id: str) -> bool:
        last = self._last_trade_ts.get(condition_id, 0)
        if (time.time() - last) < self.cooldown_secs:
            return True
        if self._market_entries[condition_id] >= self.max_entries_per_market:
            return True
        return False

    def evaluate(self, market: Market) -> Optional[PaperTrade]:
        """Check a market for split-sell arb. Returns trade if profitable."""
        yes_bid = market.yes_bids.best
        no_bid = market.no_bids.best

        if yes_bid <= 0 or no_bid <= 0:
            return None

        raw_sum = yes_bid + no_bid

        if raw_sum <= 1.0:
            return None

        # Record first time this market showed sum > 1.0 — used for latency tracking
        if market.condition_id not in self._first_seen:
            self._first_seen[market.condition_id] = time.time()

        # Check fillable depth
        yes_avg, yes_qty = market.yes_bids.fillable_at(self.min_depth)
        no_avg, no_qty = market.no_bids.fillable_at(self.min_depth)

        if yes_avg <= 0 or no_avg <= 0:
            return None

        depth_limited = min(yes_qty * yes_avg, no_qty * no_avg)
        stake = min(self.max_stake, self.bankroll, depth_limited)
        if stake < 1.0:
            return None

        trade = compute_arb(yes_avg, no_avg, stake, self.max_fee_rate, self.slippage_bps)

        self._log_scan({
            "ts": time.time(),
            "slug": market.slug,
            "question": market.question,
            "yes_bid": yes_bid,
            "no_bid": no_bid,
            "sum": round(raw_sum, 4),
            "yes_avg": round(yes_avg, 4),
            "no_avg": round(no_avg, 4),
            "stake": round(stake, 2),
            "profit": round(trade.profit, 4) if trade else 0,
            "triggered": trade is not None and trade.profit >= self.min_profit_cents / 100,
        })

        if trade is None:
            return None

        if trade.profit < self.min_profit_cents / 100:
            return None

        return trade

    def execute(self, market: Market, trade: PaperTrade):
        """
        Record a paper trade immediately. The exec_delay is modeled as a
        cooldown on the market (already handled by cooldown_secs) rather
        than blocking the scan loop — blocking would miss arb opportunities
        on other markets during the 2s relayer wait.
        """
        trade.market = market.slug
        trade.question = market.question

        # Capture latency: ms from first sum>1.0 detection to this execution
        now = time.time()
        first_seen = self._first_seen.get(market.condition_id, now)
        trade.capture_latency_ms = round((now - first_seen) * 1000, 1)

        # Track multi-entry
        self._market_entries[market.condition_id] += 1
        trade.entry_num = self._market_entries[market.condition_id]

        self.bankroll += trade.profit
        self.total_trades += 1
        self.total_profit += trade.profit
        self.total_volume += trade.stake
        self._market_profit[market.condition_id] += trade.profit
        self._last_trade_ts[market.condition_id] = now

        self._log_trade(trade)

        logging.info(
            "TRADE #%d | %s | entry=%d | stake=$%.2f | YES@%.3f NO@%.3f | "
            "profit=$%.4f (mkt_total=$%.4f) | bankroll=$%.2f | latency=%.0fms",
            self.total_trades,
            market.slug[:50],
            trade.entry_num,
            trade.stake,
            trade.yes_sell_price,
            trade.no_sell_price,
            trade.profit,
            self._market_profit[market.condition_id],
            self.bankroll,
            trade.capture_latency_ms,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _slug_end_ts(slug: str) -> int:
    """Extract end timestamp from slug like 'btc-updown-5m-1741651200'."""
    parts = slug.rsplit("-", 1)
    if len(parts) < 2:
        return 0
    try:
        start_ts = int(parts[1])
    except ValueError:
        return 0
    # Parse window minutes from slug: ...-updown-{N}m-{ts}
    mid = slug.replace(parts[1], "").rstrip("-")
    for seg in mid.split("-"):
        if seg.endswith("m") and seg[:-1].isdigit():
            return start_ts + int(seg[:-1]) * 60
    return start_ts + 300  # default 5m


# ---------------------------------------------------------------------------
# Main loop — 500ms scan tick with WebSocket book feed
# ---------------------------------------------------------------------------

async def run(cfg: dict, cfg_path: str = "config.toml"):
    from optimizer import run_optimizer_task
    from pathlib import Path as _Path

    client = PolyClient(cfg)
    engine = PaperEngine(cfg)

    scan_interval_ms = cfg["scan"]["interval_ms"]  # 500ms scan tick
    max_markets = cfg["scan"]["max_markets"]
    gamma_page_size = cfg["scan"]["gamma_page_size"]
    gamma_pages = cfg["scan"]["gamma_pages"]
    discovery_interval = cfg["scan"]["discovery_interval_secs"]
    book_batch_size = cfg["api"].get("book_batch_size", 100)

    clob_ws_url = cfg["api"]["clob_ws"]

    tracked: dict[str, Market] = {}  # condition_id → Market

    logging.info("=" * 60)
    logging.info("Split-Sell Arb Bot — WebSocket + 500ms Scan")
    logging.info("Bankroll: $%.2f | Max stake: $%.2f", engine.bankroll, engine.max_stake)
    logging.info("Min profit: %.1f¢ | Slippage: %d bps", engine.min_profit_cents, engine.slippage_bps)
    logging.info("Min depth: $%.0f | Exec delay: %.1fs", engine.min_depth, engine.exec_delay)
    logging.info("Scan tick: %dms | Discovery: %ds", scan_interval_ms, discovery_interval)
    logging.info("CLOB WS: %s", clob_ws_url)
    logging.info("=" * 60)

    # -- Phase 1: Initial discovery — crypto Up/Down windows -------------------
    logging.info("Discovering crypto time-window markets...")
    crypto_batch = await client.discover_crypto_windows()
    for m in crypto_batch:
        if m.condition_id not in tracked and len(tracked) < max_markets:
            tracked[m.condition_id] = m

    logging.info("Discovered %d crypto window markets", len(tracked))

    if not tracked:
        logging.warning("No crypto windows found — falling back to general discovery")
        for page in range(gamma_pages):
            batch = await client.discover_markets(
                limit=gamma_page_size,
                offset=page * gamma_page_size,
            )
            for m in batch:
                if m.condition_id not in tracked and len(tracked) < max_markets:
                    tracked[m.condition_id] = m
            if len(batch) < gamma_page_size:
                break
        logging.info("Fallback discovered %d markets", len(tracked))

    if not tracked:
        logging.error("No markets found, exiting")
        await client.close()
        return

    # -- Phase 2: Fetch initial book snapshots via REST ------------------------
    logging.info("Fetching initial book snapshots...")
    all_tokens = []
    token_to_market: dict[str, tuple[str, str]] = {}

    for cid, m in tracked.items():
        all_tokens.append(m.token_yes)
        all_tokens.append(m.token_no)
        token_to_market[m.token_yes] = (cid, "yes")
        token_to_market[m.token_no] = (cid, "no")

    for i in range(0, len(all_tokens), book_batch_size):
        batch = all_tokens[i:i + book_batch_size]
        books = await client.fetch_books(batch)
        now = time.time()
        for tid, book in books.items():
            if tid not in token_to_market:
                continue
            cid, side = token_to_market[tid]
            m = tracked.get(cid)
            if m is None:
                continue
            if side == "yes":
                m.yes_bids = BookSide(levels=book["bids"])
                m.yes_asks = BookSide(levels=book["asks"])
            else:
                m.no_bids = BookSide(levels=book["bids"])
                m.no_asks = BookSide(levels=book["asks"])
            m.last_update = now

    logging.info("Initial snapshots loaded for %d tokens", len(all_tokens))

    # -- Phase 3: Start WebSocket book feed + optimizer (parallel tasks) ------
    book_feed = BookFeed(clob_ws_url, tracked)
    ws_task  = asyncio.create_task(book_feed.run(), name="book-feed")
    opt_task = asyncio.create_task(
        run_optimizer_task(_Path(cfg_path), dry_run=False, interval=300, window=600),
        name="optimizer",
    )
    logging.info("Optimizer running as parallel async task (interval=300s)")

    # -- Graceful shutdown on SIGTERM/SIGINT ------------------------------------
    shutdown_event = asyncio.Event()

    def _signal_handler(sig, _frame):
        logging.info("Received %s — shutting down gracefully...", signal.Signals(sig).name)
        shutdown_event.set()

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    # -- Phase 4: 500ms scan loop + periodic REST discovery --------------------
    last_discovery = time.time()
    scan_count = 0
    scan_tick = scan_interval_ms / 1000.0  # 0.5s

    try:
        while not shutdown_event.is_set():
            loop_start = time.time()
            scan_count += 1

            # -- Periodic discovery (every discovery_interval secs) --------
            # Also prune expired windows (end_ts passed)
            if time.time() - last_discovery >= discovery_interval:
                last_discovery = time.time()

                # Prune expired windows
                now_ts = int(time.time())
                expired = [cid for cid, m in tracked.items()
                           if "-updown-" in m.slug and _slug_end_ts(m.slug) < now_ts]
                for cid in expired:
                    del tracked[cid]
                if expired:
                    logging.info("Pruned %d expired windows", len(expired))

                new_tokens = []
                crypto_batch = await client.discover_crypto_windows(quiet=True)
                for m in crypto_batch:
                    if m.condition_id not in tracked and len(tracked) < max_markets:
                        tracked[m.condition_id] = m
                        new_tokens.extend([m.token_yes, m.token_no])
                        logging.info("NEW window: %s", m.slug)

                if new_tokens:
                    logging.info("Discovery: +%d new markets (%d total)", len(new_tokens) // 2, len(tracked))
                    # Fetch initial snapshots for new markets
                    for i in range(0, len(new_tokens), book_batch_size):
                        tbatch = new_tokens[i:i + book_batch_size]
                        books = await client.fetch_books(tbatch)
                        now = time.time()
                        for tid, book in books.items():
                            cid_side = token_to_market.get(tid)
                            if not cid_side:
                                # Build mapping for new tokens
                                for cid, m in tracked.items():
                                    if m.token_yes == tid:
                                        token_to_market[tid] = (cid, "yes")
                                        cid_side = (cid, "yes")
                                        break
                                    elif m.token_no == tid:
                                        token_to_market[tid] = (cid, "no")
                                        cid_side = (cid, "no")
                                        break
                            if not cid_side:
                                continue
                            cid, side = cid_side
                            m = tracked.get(cid)
                            if m is None:
                                continue
                            if side == "yes":
                                m.yes_bids = BookSide(levels=book["bids"])
                                m.yes_asks = BookSide(levels=book["asks"])
                            else:
                                m.no_bids = BookSide(levels=book["bids"])
                                m.no_asks = BookSide(levels=book["asks"])
                            m.last_update = now

                    # Subscribe new tokens on WS
                    await book_feed.subscribe_new_tokens(new_tokens)

            # -- Scan all markets for arb opportunities -------------------------
            # Collect all candidates this tick, then execute highest-profit first.
            # Prevents dict-order luck from burying the best opportunity when
            # multiple markets fire simultaneously in the same 500ms window.
            candidates: list[tuple[float, "Market", "PaperTrade"]] = []
            for m in tracked.values():
                if engine.on_cooldown(m.condition_id):
                    continue
                trade = engine.evaluate(m)
                if trade is not None:
                    candidates.append((trade.profit, m, trade))

            candidates.sort(key=lambda x: -x[0])  # highest profit first
            opportunities = len(candidates)
            for _, m, trade in candidates:
                engine.execute(m, trade)

            # -- Status line (every 20 scans = ~10s) ----------------------------
            if scan_count % 20 == 0:
                elapsed_session = time.time() - engine.session_start
                hours = elapsed_session / 3600
                daily_rate = (engine.total_profit / hours * 24) if hours > 0.01 else 0
                # Multi-entry stats
                multi_markets = sum(1 for v in engine._market_entries.values() if v >= 2)
                max_entries = max(engine._market_entries.values()) if engine._market_entries else 0
                avg_entries = (sum(engine._market_entries.values()) / len(engine._market_entries)) if engine._market_entries else 0
                logging.info(
                    "Scan #%d | %d mkts | ws=%d | "
                    "trades=%d (multi=%d, max=%d, avg=%.1f/mkt) | "
                    "profit=$%.4f daily=$%.2f | bankroll=$%.2f",
                    scan_count,
                    len(tracked),
                    book_feed._update_count,
                    engine.total_trades,
                    multi_markets,
                    max_entries,
                    avg_entries,
                    engine.total_profit,
                    daily_rate,
                    engine.bankroll,
                )

                # -- Top 5 closest markets to arb threshold -------------------------
                sums = []
                for m in tracked.values():
                    yb = m.yes_bids.best
                    nb = m.no_bids.best
                    if yb > 0 and nb > 0:
                        sums.append((yb + nb, yb, nb, m.slug[:40]))
                sums.sort(key=lambda x: -x[0])
                top5 = sums[:5]
                if top5:
                    parts = [f"{s[3]}={s[0]:.4f}(Y{s[1]:.3f}+N{s[2]:.3f})" for s in top5]
                    logging.info("  TOP5 sums: %s", " | ".join(parts))

            # -- Wait for next 500ms tick --------------------------------------
            elapsed = time.time() - loop_start
            sleep_time = max(0, scan_tick - elapsed)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    except KeyboardInterrupt:
        logging.info("Shutting down...")
    finally:
        for task in (ws_task, opt_task):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        engine.close()
        await client.close()
        if book_feed._session and not book_feed._session.closed:
            await book_feed._session.close()

    # -- Final summary ----------------------------------------------------------
    elapsed_session = time.time() - engine.session_start
    hours = elapsed_session / 3600
    daily_rate = (engine.total_profit / hours * 24) if hours > 0.01 else 0

    logging.info("=" * 60)
    logging.info("SESSION SUMMARY")
    logging.info("Runtime:         %.1f minutes", elapsed_session / 60)
    logging.info("Total scans:     %d", scan_count)
    logging.info("WS book updates: %d", book_feed._update_count)
    logging.info("Total trades:    %d", engine.total_trades)
    logging.info("Total volume:    $%.2f", engine.total_volume)
    logging.info("Total profit:    $%.4f", engine.total_profit)
    logging.info("Daily rate:      $%.2f/day", daily_rate)
    logging.info("Final bankroll:  $%.2f", engine.bankroll)
    if engine.total_trades > 0:
        logging.info("Avg profit/trade: $%.4f", engine.total_profit / engine.total_trades)

    # Multi-entry breakdown
    if engine._market_entries:
        multi = {k: v for k, v in engine._market_entries.items() if v >= 2}
        logging.info("--- Multi-entry markets: %d / %d traded ---", len(multi), len(engine._market_entries))
        # Top 5 most-arbed markets
        top = sorted(engine._market_entries.items(), key=lambda x: -x[1])[:5]
        for cid, count in top:
            profit = engine._market_profit.get(cid, 0)
            slug = tracked.get(cid, Market("", "", "", "", "")).slug or cid[:20]
            logging.info("  %s: %d entries, profit=$%.4f", slug[:50], count, profit)
    logging.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "config.toml"
    if not os.path.exists(cfg_path):
        print(f"Config not found: {cfg_path}")
        sys.exit(1)

    cfg = load_config(cfg_path)

    log_level = getattr(logging, cfg["logging"]["level"].upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )

    asyncio.run(run(cfg, cfg_path))


if __name__ == "__main__":
    main()
