"""
Order execution: paper fills + live CLOB orders with cancel support.
"""

import os
import time
from typing import Optional, List

from weatherbot.config import log, CLOB, ORDER_CANCEL_TIMEOUT


class OrderManager:
    """Paper or live order execution with stale order cancellation."""

    def __init__(self, paper: bool = True):
        self.paper = paper
        self._clob = None
        self._paper_id = 1000
        self._live_orders: List[dict] = []  # {"order_id", "token_id", "posted_at"}

    def init_live(self) -> bool:
        """Initialize live CLOB client. Returns True on success."""
        api_key = os.environ.get("POLY_API_KEY", "")
        if not api_key:
            log.warning("[EXEC] No POLY_API_KEY -- paper only")
            return False
        try:
            from py_clob_client.client import ClobClient
            self._clob = ClobClient(
                CLOB,
                key=os.environ.get("POLY_FUNDER", api_key),
                chain_id=137,
                creds={
                    "api_key": api_key,
                    "api_secret": os.environ.get("POLY_API_SECRET", ""),
                    "api_passphrase": os.environ.get("POLY_API_PASSPHRASE", ""),
                },
            )
            log.info("[EXEC] Live CLOB client ready")
            return True
        except Exception as e:
            log.error(f"[EXEC] Init failed: {e}")
            return False

    def buy(self, token_id: str, price: float, stake: float) -> Optional[str]:
        """
        Post a BUY order. Paper: instant fill. Live: GTC limit order.
        Returns order ID or None.
        """
        price = round(price, 2)
        if price <= 0.0 or price >= 1.0 or stake <= 0.0:
            return None
        shares = round(stake / price, 2)
        if shares < 1.0:
            return None

        if self.paper:
            self._paper_id += 1
            return f"P{self._paper_id}"

        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        try:
            order = OrderArgs(
                price=price, size=shares, side=BUY, token_id=token_id,
            )
            signed = self._clob.create_order(order)
            resp = self._clob.post_order(signed, OrderType.GTC)
            if resp.get("errorMsg"):
                log.warning(f"[EXEC] {resp['errorMsg']}")
                return None
            oid = resp.get("orderID")
            if oid:
                self._live_orders.append({
                    "order_id": oid,
                    "token_id": token_id,
                    "posted_at": time.time(),
                })
            return oid or None
        except Exception as e:
            log.error(f"[EXEC] buy failed: {e}")
            return None

    def sell(
        self, token_id: str, price: float, shares: float, taker: bool = False,
    ) -> Optional[str]:
        """
        Post a SELL order. taker=True uses FOK for guaranteed fill.
        Paper: instant fill. Live: GTC or FOK limit order.
        """
        price = round(price, 2)
        if price <= 0.0 or price >= 1.0 or shares <= 0.0:
            return None

        if self.paper:
            self._paper_id += 1
            return f"P{self._paper_id}"

        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import SELL

        try:
            order = OrderArgs(
                price=price, size=round(shares, 2), side=SELL, token_id=token_id,
            )
            signed = self._clob.create_order(order)
            otype = OrderType.FOK if taker else OrderType.GTC
            resp = self._clob.post_order(signed, otype)
            if resp.get("errorMsg"):
                log.warning(f"[EXEC] {resp['errorMsg']}")
                return None
            oid = resp.get("orderID")
            if oid and not taker:
                self._live_orders.append({
                    "order_id": oid,
                    "token_id": token_id,
                    "posted_at": time.time(),
                })
            return oid or None
        except Exception as e:
            log.error(f"[EXEC] sell failed: {e}")
            return None

    def cancel_stale_orders(self) -> int:
        """Cancel GTC orders older than ORDER_CANCEL_TIMEOUT. Returns count."""
        if self.paper or not self._clob:
            return 0

        now = time.time()
        cancelled = 0
        remaining = []

        for entry in self._live_orders:
            age = now - entry["posted_at"]
            if age > ORDER_CANCEL_TIMEOUT:
                try:
                    self._clob.cancel(entry["order_id"])
                    cancelled += 1
                    log.info(f"[EXEC] Cancelled stale order {entry['order_id']} ({age:.0f}s old)")
                except Exception as e:
                    log.debug(f"[EXEC] Cancel failed {entry['order_id']}: {e}")
                    remaining.append(entry)  # retry next tick
            else:
                remaining.append(entry)

        self._live_orders = remaining
        return cancelled

    def cancel_all(self) -> int:
        """Cancel all resting orders. Called on shutdown."""
        if self.paper or not self._clob:
            return 0
        try:
            self._clob.cancel_all()
            n = len(self._live_orders)
            self._live_orders.clear()
            log.info(f"[EXEC] Cancelled all ({n}) resting orders")
            return n
        except Exception as e:
            log.error(f"[EXEC] Cancel all failed: {e}")
            return 0

    def order_filled(self, order_id: str):
        """Remove an order from tracking (called when fill confirmed)."""
        self._live_orders = [
            o for o in self._live_orders if o["order_id"] != order_id
        ]
