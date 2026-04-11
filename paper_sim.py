"""
paper_sim.py — Max-realism paper simulation layer for DSA bot.

Replaces ALL network deps with synthetic data:
  - BTC price: geometric Brownian motion (70% annualised vol)
  - Binary market probability: derived from rolling BTC momentum
  - Order books: realistic bid/ask ladders with varying depth/spread
  - Fill logic: only fills when market ask actually reaches the bot's bid
  - Window lifecycle: respawns on next clock boundary when a window closes

No real network calls are made. All order IDs are synthetic.
"""
from __future__ import annotations
import asyncio, json, math, random, time, uuid
from typing import Dict, Optional

import logging
import dsa_bot
import infra
from dsa_bot import Cfg, DsaBot, Engine, Phase, WindowState, log

# Silence urllib3 retries — BinanceFeed/ChainlinkFeed network calls
# are irrelevant in paper sim; they fail gracefully but spam warnings.
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)


# ── BTC price simulation (GBM) ───────────────────────────────────────────────

class BTCPriceSim:
    """
    Geometric Brownian Motion for BTC price.

    σ_annual = 1.20  (120% — high-vol BTC regime, observed during volatile periods)
    σ_per_sec ≈ 0.000214  →  σ_5min ≈ 0.37%  →  σ_15min ≈ 0.64%

    Seeded with 900-step burn-in so rolling_return has meaningful history
    from tick 1 rather than starting flat.
    """
    SIGMA_ANNUAL = 1.20

    def __init__(self, start: float = 83_000.0):
        σ = self.SIGMA_ANNUAL / math.sqrt(365 * 24 * 3600)
        self._hist: list[float] = []
        p = start
        # 900-step (15-min) burn-in random walk so momentum is non-trivial at t=0
        for _ in range(900):
            p *= math.exp(random.gauss(-0.5 * σ**2, σ))
            self._hist.append(p)
        self.price = p

    def step(self, dt: float = 1.0):
        σ = self.SIGMA_ANNUAL / math.sqrt(365 * 24 * 3600 / dt)
        self.price *= math.exp(random.gauss(-0.5 * σ**2, σ))
        self._hist.append(self.price)
        if len(self._hist) > 7200:           # keep 2h
            del self._hist[0]

    def rolling_return(self, secs: int) -> float:
        n = min(secs, len(self._hist) - 1)
        if n < 1:
            return 0.0
        return (self._hist[-1] - self._hist[-n - 1]) / self._hist[-n - 1]

    def momentum(self, wmin: int) -> float:
        """
        Normalised momentum in [-1, 1] for the given window.
        ref_vol calibrated so 1-sigma BTC move → |momentum| ≈ 1.0.
        """
        ref_vol = 0.0037 if wmin <= 5 else 0.0064
        ret = self.rolling_return(wmin * 60)
        return max(-1.0, min(1.0, ret / ref_vol))


# ── Single synthetic Polymarket binary market ────────────────────────────────

class SimMarket:
    """
    Simulates one BTC up/dn binary window.

    UP probability = 0.50 + 0.18 × momentum
    Prices follow Ornstein-Uhlenbeck (mean-reverting) processes so that
    tick-to-tick changes are smooth (≤1¢/tick typical), matching real
    Polymarket book dynamics and avoiding false MAX_VELOCITY triggers.

    Combined fair mean-reverts toward 0.965; ~30% of ticks it dips
    below 0.958, opening genuine arb windows for the bot.
    """
    HALF_SPREAD  = 0.012
    KAPPA_PRICE  = 0.80   # fast reversion → oscillates → 10-tick range ≥ 0.06
    KAPPA_COMB   = 0.08   # slower reversion for combined fair
    SIGMA_PRICE  = 0.025  # per-tick noise: σ_stat = 0.025/√1.6 ≈ 0.020
    SIGMA_COMB   = 0.007  # per-tick noise for combined fair
    COMB_TARGET  = 0.965  # long-run mean for combined fair value

    def __init__(self, wmin: int, btc: BTCPriceSim):
        self.wmin     = wmin
        self.btc      = btc
        self.tid_up   = "SIM_UP_" + uuid.uuid4().hex[:12].upper()
        self.tid_dn   = "SIM_DN_" + uuid.uuid4().hex[:12].upper()
        self.mid      = f"sim-btc-{wmin}m-{uuid.uuid4().hex[:6]}"

        now = time.time()
        period = wmin * 60
        self.close_ts = math.ceil(now / period) * period
        self.open_ts  = self.close_ts - period

        # State — initialised near equilibrium
        mom = self.btc.momentum(wmin)
        self._up_p          = max(0.30, min(0.70, 0.50 + 0.18 * mom))
        self._combined_fair = self.COMB_TARGET + random.gauss(0, 0.010)

        self._up_book: dict = {}
        self._dn_book: dict = {}
        self._regen()

    # ── Internal ──

    @staticmethod
    def _make_book(mid_p: float, half: float) -> dict:
        asks, bids = [], []
        for i in range(4):
            a_px = round(min(0.97, mid_p + half + i * 0.02), 2)
            b_px = round(max(0.03, mid_p - half - i * 0.02), 2)
            sz   = random.randint(140 + i * 40, 420 + i * 80)
            asks.append({"price": f"{a_px:.2f}", "size": f"{sz}"})
            bids.append({"price": f"{b_px:.2f}", "size": f"{sz}"})
        return {"asks": asks, "bids": bids, "_ts": time.time()}

    def _regen(self):
        # OU step: UP prob reverts toward BTC momentum target
        target_up  = max(0.30, min(0.70, 0.50 + 0.18 * self.btc.momentum(self.wmin)))
        self._up_p = ((1 - self.KAPPA_PRICE) * self._up_p
                      + self.KAPPA_PRICE * target_up
                      + random.gauss(0, self.SIGMA_PRICE))
        self._up_p = max(0.28, min(0.72, self._up_p))

        # OU step: combined fair reverts toward long-run mean
        self._combined_fair = ((1 - self.KAPPA_COMB) * self._combined_fair
                               + self.KAPPA_COMB * self.COMB_TARGET
                               + random.gauss(0, self.SIGMA_COMB))
        self._combined_fair = max(0.80, self._combined_fair)

        # DN priced as remainder; small independent noise for realistic splitting
        dn_p = max(0.25, min(0.72,
               self._combined_fair - self._up_p + random.gauss(0, 0.003)))

        self._up_book = self._make_book(self._up_p, self.HALF_SPREAD)
        self._dn_book = self._make_book(dn_p,       self.HALF_SPREAD)

    # ── Public ──

    def tick(self):
        self._regen()

    def get_book(self, tid: str) -> Optional[dict]:
        if tid == self.tid_up:
            b = dict(self._up_book); b["_ts"] = time.time(); return b
        if tid == self.tid_dn:
            b = dict(self._dn_book); b["_ts"] = time.time(); return b
        return None

    def best_ask(self, tid: str) -> float:
        b = self.get_book(tid) or {}
        asks = b.get("asks", [])
        return float(asks[0]["price"]) if asks else 1.0

    def best_bid(self, tid: str) -> float:
        b = self.get_book(tid) or {}
        bids = b.get("bids", [])
        return float(bids[0]["price"]) if bids else 0.0


# ── Simulator ────────────────────────────────────────────────────────────────

class PaperSimulator:
    TICK_SEC = 1.0

    def __init__(self, bot: DsaBot):
        self.bot     = bot
        self.btc     = BTCPriceSim()
        self.markets: Dict[str, SimMarket] = {}

    # ── Market injection ──

    def _spawn(self, wmin: int) -> SimMarket:
        m  = SimMarket(wmin, self.btc)
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

        log.info(
            f"[SIM] +{m.mid}  closes in {m.close_ts - time.time():.0f}s"
            f"  BTC=${self.btc.price:,.0f}"
        )
        return m

    # ── Tick loop (drives the engine) ──

    async def tick_loop(self):
        await asyncio.sleep(1.5)          # let bot fully initialise

        for wmin in sorted(Cfg.TIMEFRAMES):
            self._spawn(wmin)

        while self.bot._running:
            await asyncio.sleep(self.TICK_SEC)
            self.btc.step()

            for mid in list(self.markets):
                m = self.markets[mid]
                m.tick()

                # Window closed and bot dropped the engine — spawn fresh
                if mid not in self.bot._engines:
                    del self.markets[mid]
                    self._spawn(m.wmin)
                    continue

                # Mimic a WS book-change event on the UP token
                fake = json.dumps({"asset_id": m.tid_up, "event_type": "book"})
                asyncio.create_task(self.bot._on_book(fake))

    # ── Fill loop (max-realism: fill only when market comes to bid) ──

    async def fill_loop(self):
        while self.bot._running:
            await asyncio.sleep(0.25)

            for mid, eng in list(self.bot._engines.items()):
                m = self.markets.get(mid)
                if not m:
                    continue

                for side_char in ("up", "dn"):
                    side = eng.st.up if side_char == "up" else eng.st.dn
                    tid  = eng.st.tid_up if side_char == "up" else eng.st.tid_dn
                    ask  = m.best_ask(tid)

                    for oid, (px, qty, ts) in list(side._pending.items()):
                        if ask <= px:
                            fill_px = round(min(px, ask), 4)
                            side.on_confirm(oid, fill_px, qty)
                            self.bot._oid_to_key.pop(oid, None)
                            log.info(
                                f"[FILL] {mid[:20]} {side_char}"
                                f" @{fill_px:.3f}×{qty:.0f}"
                                f"  ask={ask:.3f} bid={px:.3f}"
                                f"  vwap={side.vwap:.3f}"
                            )


# ── Patch function ────────────────────────────────────────────────────────────

def apply(bot: DsaBot) -> PaperSimulator:
    """
    Monkey-patch DsaBot to use PaperSimulator instead of live network feeds.
    Call before bot.run().
    """
    sim = PaperSimulator(bot)

    # 0. Kill noisy network threads in BinanceFeed / ChainlinkFeed
    #    They start daemon threads that retry forever — patch to no-op.
    def _bn_start_noop(self):
        self.running = True          # mark running so .stop() works
    def _cl_start_noop(self):
        self.running = True
    infra.BinanceFeed.start      = _bn_start_noop
    infra.ChainlinkFeed.start    = _cl_start_noop

    # 1. Scan loop — no-op (we inject markets directly via sim)
    async def _noop_scan_loop(self):
        while self._running:
            await asyncio.sleep(60)
    dsa_bot.DsaBot._scan_loop = _noop_scan_loop

    # 2. Book WS loop — replaced by sim tick loop
    async def _sim_books_loop(self):
        await sim.tick_loop()
    dsa_bot.DsaBot._ws_books_loop = _sim_books_loop

    # 3. Book REST fetch — served from in-memory sim books
    async def _sim_fetch_book(self, tid: str) -> Optional[dict]:
        for m in sim.markets.values():
            b = m.get_book(tid)
            if b:
                return b
        return None
    dsa_bot.DsaBot._fetch_book = _sim_fetch_book

    # 4. User/fill WS — conditional fills (market-price-driven)
    async def _sim_user_loop(self):
        await sim.fill_loop()
    dsa_bot.DsaBot._ws_user_loop = _sim_user_loop

    return sim
