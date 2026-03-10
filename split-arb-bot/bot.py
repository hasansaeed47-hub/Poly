#!/usr/bin/env python3
"""
Split-Sell Arbitrage Paper Trading Bot for Polymarket

Strategy: When YES_bid + NO_bid > $1.00 (after fees), simulate:
  1. Split $X USDC → X YES tokens + X NO tokens  (gasless via relayer)
  2. Sell X YES tokens at YES_bid
  3. Sell X NO tokens at NO_bid
  4. Profit = sell_proceeds - split_cost - fees

This is paper trading only — no wallet, no signing, no real orders.
All execution is simulated with realistic timing, slippage, and fees.
"""

import asyncio
import json
import logging
import os
import sys
import time
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

# ---------------------------------------------------------------------------
# Fee model — Polymarket's actual fee curve
# ---------------------------------------------------------------------------

def taker_fee(price: float, max_rate: float) -> float:
    """
    Polymarket fee: rate * price * (1 - price)
    Peaks at 50c (max_rate * 0.25), zero at 0c and 100c.
    The rate is ~6.25% to produce ~1.56% effective fee at 50c.
    We use max_rate as the cap on the effective fee.
    """
    # Effective fee = price * q * (1-q) where q is the price
    # But Polymarket docs say: fee = price * fee_rate, capped.
    # Simpler realistic model: fee% = min(max_rate, 2 * max_rate * min(price, 1-price))
    effective_rate = min(max_rate, 2 * max_rate * min(price, 1 - price))
    return price * effective_rate


def compute_arb(
    yes_bid: float,
    no_bid: float,
    stake: float,
    max_fee_rate: float,
    slippage_bps: int,
) -> Optional[PaperTrade]:
    """
    Check if a split-sell arb is profitable.

    Split $stake USDC → stake YES + stake NO tokens.
    Sell YES at yes_bid (minus slippage), sell NO at no_bid (minus slippage).
    Pay taker fees on each sell.
    """
    if yes_bid <= 0 or no_bid <= 0:
        return None

    # Apply slippage
    slip = slippage_bps / 10000
    yes_fill = yes_bid * (1 - slip)
    no_fill = no_bid * (1 - slip)

    # Proceeds from selling
    yes_proceeds = stake * yes_fill
    no_proceeds = stake * no_fill
    gross = yes_proceeds + no_proceeds

    # Fees on each leg
    yes_fee = stake * taker_fee(yes_fill, max_fee_rate)
    no_fee = stake * taker_fee(no_fill, max_fee_rate)
    total_fees = yes_fee + no_fee

    # P&L
    net = gross - stake  # before fees
    profit = net - total_fees

    if profit <= 0:
        return None

    return PaperTrade(
        ts=time.time(),
        market="",      # filled by caller
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
# API client
# ---------------------------------------------------------------------------

class PolyClient:
    """Thin async wrapper around Gamma + CLOB REST APIs."""

    def __init__(self, cfg: dict):
        self.gamma_api = cfg["api"]["gamma_api"]
        self.clob_rest = cfg["api"]["clob_rest"]
        self.throttle = cfg["api"]["throttle_ms"] / 1000.0
        self._last_call = 0.0
        self._session: Optional[aiohttp.ClientSession] = None

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

    # -- Gamma: discover binary markets ----------------------------------------

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
                # Only binary (2-outcome) markets
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

                # Map outcomes to YES/NO token positions
                o_lower = [o.lower() for o in outcomes]
                if "yes" in o_lower:
                    yes_idx = o_lower.index("yes")
                    no_idx = 1 - yes_idx
                elif "up" in o_lower:
                    yes_idx = o_lower.index("up")
                    no_idx = 1 - yes_idx
                else:
                    # Default: first = YES, second = NO
                    yes_idx, no_idx = 0, 1

                markets.append(Market(
                    condition_id=cond_id,
                    question=question[:120],
                    slug=slug,
                    token_yes=tokens[yes_idx],
                    token_no=tokens[no_idx],
                ))

        return markets

    # -- CLOB: fetch order books -----------------------------------------------

    async def fetch_books(self, token_ids: list[str]) -> dict[str, dict]:
        """Fetch order books for a batch of token IDs. Returns {token_id: {bids, asks}}."""
        if not token_ids:
            return {}

        await self._throttle()
        session = await self._get_session()

        # CLOB /books accepts POST with array of {token_id: ...}
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
            bids.sort(key=lambda x: -x[0])  # highest first

            asks = []
            for level in item.get("asks", []):
                try:
                    p = float(level.get("price", 0))
                    s = float(level.get("size", 0))
                    if p > 0 and s > 0:
                        asks.append((p, s))
                except (ValueError, TypeError):
                    continue
            asks.sort(key=lambda x: x[0])  # lowest first

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

    async def evaluate(self, market: Market) -> Optional[PaperTrade]:
        """Check a market for split-sell arb. Returns trade if profitable."""
        yes_bid = market.yes_bids.best
        no_bid = market.no_bids.best

        if yes_bid <= 0 or no_bid <= 0:
            return None

        raw_sum = yes_bid + no_bid

        # Quick check: bids must sum above $1.00
        if raw_sum <= 1.0:
            return None

        # Check fillable depth
        yes_avg, yes_qty = market.yes_bids.fillable_at(self.min_depth)
        no_avg, no_qty = market.no_bids.fillable_at(self.min_depth)

        if yes_avg <= 0 or no_avg <= 0:
            return None

        # Stake: limited by bankroll, max_stake, and available depth
        depth_limited = min(yes_qty * yes_avg, no_qty * no_avg)
        stake = min(self.max_stake, self.bankroll, depth_limited)
        if stake < 1.0:
            return None

        # Compute arb using depth-weighted avg prices
        trade = compute_arb(yes_avg, no_avg, stake, self.max_fee_rate, self.slippage_bps)

        # Log the scan regardless
        self._log_scan({
            "ts": time.time(),
            "slug": market.slug,
            "question": market.question,
            "yes_bid": yes_bid,
            "no_bid": no_bid,
            "sum": raw_sum,
            "yes_avg": round(yes_avg, 4),
            "no_avg": round(no_avg, 4),
            "stake": round(stake, 2),
            "profit": round(trade.profit, 4) if trade else 0,
            "triggered": trade is not None and trade.profit >= self.min_profit_cents / 100,
        })

        if trade is None:
            return None

        # Must meet minimum profit threshold
        if trade.profit < self.min_profit_cents / 100:
            return None

        return trade

    async def execute(self, market: Market, trade: PaperTrade):
        """Simulate execution: split + sell both sides via relayer."""
        # Simulate relayer delay
        await asyncio.sleep(self.exec_delay)

        trade.market = market.slug
        trade.question = market.question

        # Update bankroll
        self.bankroll += trade.profit
        self.total_trades += 1
        self.total_profit += trade.profit
        self.total_volume += trade.stake

        self._log_trade(trade)

        logging.info(
            "TRADE #%d | %s | stake=$%.2f | YES@%.3f NO@%.3f | "
            "gross=$%.4f fees=$%.4f profit=$%.4f | bankroll=$%.2f",
            self.total_trades,
            market.slug[:50],
            trade.stake,
            trade.yes_sell_price,
            trade.no_sell_price,
            trade.gross,
            trade.yes_fee + trade.no_fee,
            trade.profit,
            self.bankroll,
        )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def run(cfg: dict):
    client = PolyClient(cfg)
    engine = PaperEngine(cfg)

    scan_interval = cfg["scan"]["interval_secs"]
    max_markets = cfg["scan"]["max_markets"]
    gamma_page_size = cfg["scan"]["gamma_page_size"]
    gamma_pages = cfg["scan"]["gamma_pages"]
    book_batch_size = 20  # CLOB limit per request

    tracked: dict[str, Market] = {}  # condition_id → Market
    discovery_counter = 0

    logging.info("=" * 60)
    logging.info("Split-Sell Arb Bot — Paper Trading")
    logging.info("Bankroll: $%.2f | Max stake: $%.2f", engine.bankroll, engine.max_stake)
    logging.info("Min profit: %.1f¢ | Slippage: %d bps", engine.min_profit_cents, engine.slippage_bps)
    logging.info("Min depth: $%.0f | Exec delay: %.1fs", engine.min_depth, engine.exec_delay)
    logging.info("=" * 60)

    try:
        while True:
            loop_start = time.time()

            # -- Discovery: refresh market list every 12 cycles (~1 min) -------
            if discovery_counter % 12 == 0:
                logging.info("Discovering markets...")
                new_markets = []
                for page in range(gamma_pages):
                    batch = await client.discover_markets(
                        limit=gamma_page_size,
                        offset=page * gamma_page_size,
                    )
                    new_markets.extend(batch)
                    if len(batch) < gamma_page_size:
                        break

                added = 0
                for m in new_markets:
                    if m.condition_id not in tracked and len(tracked) < max_markets:
                        tracked[m.condition_id] = m
                        added += 1

                logging.info(
                    "Tracking %d markets (+%d new, %d discovered)",
                    len(tracked), added, len(new_markets),
                )
            discovery_counter += 1

            if not tracked:
                logging.warning("No markets tracked, waiting...")
                await asyncio.sleep(scan_interval)
                continue

            # -- Fetch order books in batches -----------------------------------
            all_tokens = []
            token_to_market: dict[str, tuple[str, str]] = {}  # token → (cond_id, "yes"/"no")

            for cid, m in tracked.items():
                all_tokens.append(m.token_yes)
                all_tokens.append(m.token_no)
                token_to_market[m.token_yes] = (cid, "yes")
                token_to_market[m.token_no] = (cid, "no")

            # Batch fetch
            all_books: dict[str, dict] = {}
            for i in range(0, len(all_tokens), book_batch_size):
                batch = all_tokens[i : i + book_batch_size]
                books = await client.fetch_books(batch)
                all_books.update(books)

            # -- Update market book state ---------------------------------------
            now = time.time()
            for tid, book in all_books.items():
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

            # -- Scan for arb opportunities ------------------------------------
            opportunities = 0
            for m in tracked.values():
                trade = await engine.evaluate(m)
                if trade is not None:
                    opportunities += 1
                    await engine.execute(m, trade)

            # -- Status line ----------------------------------------------------
            elapsed = time.time() - loop_start
            logging.info(
                "Scan %d | %d markets | %d opps | "
                "trades=%d profit=$%.4f volume=$%.2f | %.1fs",
                discovery_counter,
                len(tracked),
                opportunities,
                engine.total_trades,
                engine.total_profit,
                engine.total_volume,
                elapsed,
            )

            # -- Wait for next tick --------------------------------------------
            sleep_time = max(0, scan_interval - elapsed)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    except KeyboardInterrupt:
        logging.info("Shutting down...")
    finally:
        engine.close()
        await client.close()

    # -- Final summary ----------------------------------------------------------
    logging.info("=" * 60)
    logging.info("SESSION SUMMARY")
    logging.info("Total trades:  %d", engine.total_trades)
    logging.info("Total volume:  $%.2f", engine.total_volume)
    logging.info("Total profit:  $%.4f", engine.total_profit)
    logging.info("Final bankroll: $%.2f", engine.bankroll)
    if engine.total_trades > 0:
        logging.info("Avg profit/trade: $%.4f", engine.total_profit / engine.total_trades)
        logging.info("Win rate: 100%% (arb is structurally profitable)")
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

    # Setup logging
    log_level = getattr(logging, cfg["logging"]["level"].upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )

    asyncio.run(run(cfg))


if __name__ == "__main__":
    main()
