"""
paper_sim.py — Simplified paper simulation for DSA bot.

Design:
  - Order books oscillate realistically around 0.50 so entry filter passes.
  - Fill events fire every FILL_INTERVAL seconds per side at cheap prices,
    simulating a maker bid getting hit by a seller.
  - Combined VWAP of fills stays well below 0.97 target → guaranteed profit.
  - No network access needed.
"""
from __future__ import annotations
import asyncio, json, math, random, time, uuid, logging
from typing import Dict, Optional

import dsa_bot
import infra
from dsa_bot import Cfg, DsaBot, Engine, Phase, WindowState, log

# Silence urllib3 retry noise from BinanceFeed / ChainlinkFeed
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)


# ── Synthetic market ──────────────────────────────────────────────────────────

class SimMarket:
    """
    One BTC up/dn binary window with:
      - Oscillating books (sinusoidal + noise) so the entry filter sees
        enough price variation (osc > MIN_OSC = 0.04).
      - Fill events every FILL_INTERVAL seconds per side, at a price
        drawn from [FILL_LO, FILL_HI] — cheap enough that combined
        VWAP stays well below the 0.97 target.
    """
    FILL_INTERVAL = 20          # seconds between fill events per side
    FILL_LO, FILL_HI = 0.38, 0.46   # fill price range (cheap maker bids)
    OSC_PERIOD   = 90           # book oscillation period in seconds
    OSC_AMP      = 0.06         # oscillation amplitude around 0.50

    def __init__(self, wmin: int):
        self.wmin     = wmin
        self.tid_up   = "SIM_UP_" + uuid.uuid4().hex[:12].upper()
        self.tid_dn   = "SIM_DN_" + uuid.uuid4().hex[:12].upper()
        self.mid      = f"sim-btc-{wmin}m-{uuid.uuid4().hex[:6]}"

        now = time.time()
        period = wmin * 60
        self.close_ts = math.ceil(now / period) * period
        self.open_ts  = self.close_ts - period

        # Stagger fill timers so UP and DN don't always fire together
        self._next_fill_up = now + random.uniform(8, self.FILL_INTERVAL)
        self._next_fill_dn = now + random.uniform(8, self.FILL_INTERVAL) + 5

        # Phase offset so UP and DN books oscillate with slight offset
        self._phase_up = random.uniform(0, 2 * math.pi)
        self._phase_dn = self._phase_up + math.pi * 0.4   # offset by ~72°

    # ── Book ──

    def _book_mid(self, phase_offset: float) -> float:
        """Sinusoidal oscillation + small random tick noise."""
        t = time.time()
        osc = self.OSC_AMP * math.sin(2 * math.pi * t / self.OSC_PERIOD + phase_offset)
        return max(0.30, min(0.70, 0.50 + osc + random.gauss(0, 0.005)))

    @staticmethod
    def _make_book(mid_p: float, half: float = 0.013) -> dict:
        asks, bids = [], []
        for i in range(4):
            asks.append({"price": f"{min(0.97, mid_p + half + i*0.02):.2f}",
                         "size":  f"{random.randint(150, 400)}"})
            bids.append({"price": f"{max(0.03, mid_p - half - i*0.02):.2f}",
                         "size":  f"{random.randint(150, 400)}"})
        return {"asks": asks, "bids": bids, "_ts": time.time()}

    def get_book(self, tid: str) -> Optional[dict]:
        if tid == self.tid_up:
            return self._make_book(self._book_mid(self._phase_up))
        if tid == self.tid_dn:
            return self._make_book(self._book_mid(self._phase_dn))
        return None

    # ── Fill events ──

    def pop_fill(self, side: str) -> Optional[float]:
        """
        Returns a fill price if the fill timer has expired for this side,
        otherwise None.  Resets the timer on each fire.
        """
        now = time.time()
        if side == "up" and now >= self._next_fill_up:
            self._next_fill_up = now + self.FILL_INTERVAL + random.uniform(-3, 3)
            return round(random.uniform(self.FILL_LO, self.FILL_HI), 2)
        if side == "dn" and now >= self._next_fill_dn:
            self._next_fill_dn = now + self.FILL_INTERVAL + random.uniform(-3, 3)
            return round(random.uniform(self.FILL_LO, self.FILL_HI), 2)
        return None


# ── Simulator ─────────────────────────────────────────────────────────────────

class PaperSimulator:
    TICK_SEC = 1.0

    def __init__(self, bot: DsaBot):
        self.bot     = bot
        self.markets: Dict[str, SimMarket] = {}

    def _spawn(self, wmin: int) -> SimMarket:
        m  = SimMarket(wmin)
        self.markets[m.mid] = m

        st = WindowState(
            market_id  = m.mid,
            tid_up     = m.tid_up,
            tid_dn     = m.tid_dn,
            open_ts    = m.open_ts,
            close_ts   = m.close_ts,
            target_net = Cfg.TARGET_MID,
            wmin       = wmin,
        )
        eng = Engine(st, self.bot._exec, self.bot._loop,
                     paper=True, register_oid=self.bot._register_oid)
        self.bot._engines[m.mid]       = eng
        self.bot._tid_to_mid[m.tid_up] = m.mid
        self.bot._tid_to_mid[m.tid_dn] = m.mid

        log.info(f"[SIM] +{m.mid}  closes in {m.close_ts - time.time():.0f}s")
        return m

    # ── Tick loop — drives engine ticks via fake WS events ──

    async def tick_loop(self):
        await asyncio.sleep(1.5)
        for wmin in sorted(Cfg.TIMEFRAMES):
            self._spawn(wmin)

        while self.bot._running:
            await asyncio.sleep(self.TICK_SEC)
            for mid in list(self.markets):
                m = self.markets[mid]
                if mid not in self.bot._engines:
                    del self.markets[mid]
                    self._spawn(m.wmin)
                    continue
                fake = json.dumps({"asset_id": m.tid_up, "event_type": "book"})
                asyncio.create_task(self.bot._on_book(fake))

    # ── Fill loop — fire fills every FILL_INTERVAL seconds ──

    async def fill_loop(self):
        while self.bot._running:
            await asyncio.sleep(0.5)
            for mid, eng in list(self.bot._engines.items()):
                m = self.markets.get(mid)
                if not m:
                    continue
                for side_char in ("up", "dn"):
                    side = eng.st.up if side_char == "up" else eng.st.dn
                    if not side._pending:
                        continue
                    fill_px = m.pop_fill(side_char)
                    if fill_px is None:
                        continue
                    # Fill ONE order per event (highest-bid wins), cancel stale rest
                    best_oid = best_px = None
                    for oid, (px, qty, ts) in side._pending.items():
                        if fill_px <= px and (best_px is None or px > best_px):
                            best_oid, best_px = oid, px
                    if best_oid:
                        _, qty, _ = side._pending[best_oid]
                        side.on_confirm(best_oid, fill_px, qty)
                        self.bot._oid_to_key.pop(best_oid, None)
                        # Cancel stale accumulated orders so they don't pile up
                        for stale in list(side._pending.keys()):
                            side.on_cancel(stale)
                            self.bot._oid_to_key.pop(stale, None)
                        log.info(
                            f"[FILL] {mid[:22]} {side_char}"
                            f" @{fill_px:.2f}×{qty:.0f}"
                            f"  vwap={side.vwap:.3f}"
                            f"  comb={eng.st.combined:.3f}"
                        )


# ── Patch ─────────────────────────────────────────────────────────────────────

def apply(bot: DsaBot) -> PaperSimulator:
    sim = PaperSimulator(bot)

    # Kill BinanceFeed / ChainlinkFeed network threads
    def _noop_start(self): self.running = True
    infra.BinanceFeed.start   = _noop_start
    infra.ChainlinkFeed.start = _noop_start

    # No-op scan loop
    async def _noop_scan(self):
        while self._running:
            await asyncio.sleep(60)
    dsa_bot.DsaBot._scan_loop = _noop_scan

    # Book WS loop → sim tick loop
    async def _sim_books(self):
        await sim.tick_loop()
    dsa_bot.DsaBot._ws_books_loop = _sim_books

    # Fetch book → in-memory sim book
    async def _sim_fetch(self, tid: str):
        for m in sim.markets.values():
            b = m.get_book(tid)
            if b:
                return b
        return None
    dsa_bot.DsaBot._fetch_book = _sim_fetch

    # User WS / fill loop → timer-based fills
    async def _sim_fills(self):
        await sim.fill_loop()
    dsa_bot.DsaBot._ws_user_loop = _sim_fills

    return sim
