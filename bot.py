#!/usr/bin/env python3
"""
POLYMARKET v4.1 — BOT ORCHESTRATOR
=====================================
Wires infra + strategy layers. Runs the main loop.

v4.1 upgrades (all in infra.py):
  CL: Chainlink RTDS WebSocket primary, Binance fallback (settlement accuracy)
  EX1: Consecutive failure backoff (prevents API ban)
  EX2: Post-only cross detection (prevents taker fees)

Preserved from v4.0:
  M5:  Clean shutdown — cancel all, 1s drain for WS confirms, save trades
  S1:  Batch book refresh for all active windows each tick
  S6:  POOL.shutdown() on exit
"""

from __future__ import annotations

import sys
import signal
import time
import logging
from collections import defaultdict
from typing import List, Set

from infra import (
    Config, log, POOL,
    BinanceFeed, ChainlinkFeed, BookFetcher,
    ExecutionLayer, HeartbeatThread, UserWSFeed,
    MarketWindow,
)
from strategy import (
    RegimeGuard, RiskManager, PairTracker, CrossWindowIntel,
    Scanner, MergeEngine, Settlement, QuoteEngine,
)


class Bot:
    def __init__(self, paper: bool = True):
        self.paper = paper

        # Infrastructure
        self.books = BookFetcher()
        self.bn = BinanceFeed()
        self.cl = ChainlinkFeed()
        self.exec = ExecutionLayer(paper=paper)
        self.heartbeat = HeartbeatThread(self.exec)
        self.user_ws = UserWSFeed(
            on_fill=self.exec.on_ws_fill,
            on_cancel=self.exec.on_ws_cancel,
        )

        # Strategy
        self.risk = RiskManager()
        self.regime = RegimeGuard(self.bn, self.books)
        self.pairs = PairTracker()
        self.xw = CrossWindowIntel()
        self.scanner = Scanner(self.books)
        self.engine = QuoteEngine(
            self.bn, self.cl, self.books, self.exec,
            self.risk, self.regime, self.pairs, self.xw,
        )

        # State
        self._tick = 0
        self._ws_tids: Set[str] = set()
        self._last_merge = 0.0
        self._last_status = 0.0
        self._last_clean = 0.0
        self._running = True

    # ── Main loop ──

    def run(self):
        self._banner()

        def _shutdown(signum, frame):
            log.info(f"[BOT] Signal {signum}, shutting down...")
            self._running = False

        signal.signal(signal.SIGTERM, _shutdown)
        signal.signal(signal.SIGINT, _shutdown)

        # Start feeds — BN first (CL uses it as fallback)
        self.bn.start()
        self.cl.set_bn_fallback(self.bn)
        self.cl.start()
        time.sleep(2)

        # Live init
        creds = None
        if not self.paper:
            creds = self.exec.init_live()

        # Start heartbeat
        self.heartbeat.start()

        # Start user WS feed
        if creds and hasattr(creds, "api_key"):
            self.user_ws.configure(
                creds.api_key,
                creds.api_secret,
                creds.api_passphrase,
            )
            self.user_ws.start()

        # Print initial prices
        for asset in sorted(Config.ASSETS):
            cl_px = self.cl.get(asset)
            bn_px = self.bn.get_asset(asset)
            log.info(
                f"  {asset.upper()}: CL=${(cl_px or 0):,.2f} "
                f"BN=${(bn_px or 0):,.2f}"
            )

        log.info(
            f"[BOT] Running {'PAPER' if self.paper else 'LIVE'} | "
            f"TFs={sorted(Config.TIMEFRAMES)} | "
            f"Assets={sorted(Config.ASSETS)}"
        )

        # ── MAIN LOOP ──
        while self._running:
            try:
                self._tick += 1
                now = time.time()
                inow = int(now)

                # Feed staleness check — warn but don't block
                # cl.get() already falls back to BN when RTDS is stale
                cl_stale = self.cl.is_stale(15)
                bn_stale = all(
                    self.bn.is_stale(a) for a in Config.ASSETS
                )
                if cl_stale and bn_stale:
                    # Both feeds dead — skip, nothing safe to do
                    if self._tick <= 1 or self._tick % 50 == 0:
                        log.warning(
                            f"[FEEDS] Both stale — CL: RTDS "
                            f"{'up' if self.cl.is_rtds_connected() else 'DOWN'}"
                            f" | BN: {sum(1 for a in Config.ASSETS if not self.bn.is_stale(a))}"
                            f"/{len(Config.ASSETS)} fresh"
                        )
                    time.sleep(1)
                    continue
                if cl_stale and self._tick % 600 == 0:
                    rtds = "connected" if self.cl.is_rtds_connected() else "DISCONNECTED"
                    log.warning(f"[FEEDS] CL RTDS stale (WS {rtds}) — using BN fallback")

                # Heartbeat health
                if not self.paper and not self.heartbeat.healthy:
                    if self._tick % 100 == 0:
                        log.warning(
                            f"[HB] Heartbeat unhealthy! "
                            f"{self.heartbeat.stats}"
                        )

                # Cascade detection
                casc, cdir, cmag = self.regime.check_cascade()
                if casc:
                    log.warning(
                        f"[CASCADE] {cdir} {cmag:.2f}% — pausing quotes"
                    )

                # Scan markets
                windows = self.scanner.scan()

                # S1: Batch-refresh stale books for all active windows
                if windows:
                    self._refresh_books(windows, now)

                # WS subscriptions for new tokens
                if windows:
                    self._subscribe_ws(windows)

                # Merge-first — free capital before new entries
                if (Config.MERGE_ENABLED
                        and now - self._last_merge > Config.MERGE_INTERVAL):
                    if self.paper:
                        MergeEngine.log_merges(self.pairs, windows)
                    else:
                        MergeEngine.execute_merges(
                            self.pairs, windows, self.exec, self.risk,
                        )
                    self._last_merge = now

                # Quote engine tick
                self.engine.tick(windows)

                # Settlement
                Settlement.resolve(
                    self.pairs, self.risk, self.cl, self.xw, windows,
                )

                # Status line
                if now - self._last_status > 30:
                    self._status(windows)
                    self.risk.save_trades()
                    self._last_status = now

                # Cleanup stale pair tracker entries
                if now - self._last_clean > 300:
                    self.pairs.cleanup(inow)
                    self._last_clean = now

                time.sleep(Config.QUOTE_INTERVAL_MS / 1000)

            except KeyboardInterrupt:
                break
            except Exception as e:
                log.error(f"[BOT] Loop: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(2)

        # ── M5: CLEAN SHUTDOWN ──
        self._shutdown()

    # ── Helpers ──

    def _refresh_books(self, windows: List[MarketWindow], now: float):
        """S1: Batch-refresh stale books for all active windows."""
        stale_tids = []
        for w in windows:
            b_up = self.books.get(w.tid_up)
            if not b_up or (now - b_up.ts) > 3:
                stale_tids.append(w.tid_up)
            b_dn = self.books.get(w.tid_down)
            if not b_dn or (now - b_dn.ts) > 3:
                stale_tids.append(w.tid_down)
        if stale_tids:
            self.books.fetch_batch(list(set(stale_tids)))

    def _subscribe_ws(self, windows: List[MarketWindow]):
        """Subscribe WS for new token IDs."""
        tids = set()
        for w in windows:
            if w.tid_up:
                tids.add(w.tid_up)
            if w.tid_down:
                tids.add(w.tid_down)
        new_tids = tids - self._ws_tids
        if new_tids:
            if not self._ws_tids:
                self.books.start_ws(tids)
            elif hasattr(self.books, "_ws"):
                try:
                    self.books._ws.subscribe(new_tids)
                except Exception as e:
                    log.debug(f"[BOT] WS subscribe failed: {e}")
            self._ws_tids.update(tids)

    def _shutdown(self):
        """M5: Cancel all → drain 1s for WS confirms → save → stop feeds."""
        log.info("[BOT] Cancelling all resting orders...")
        n = self.exec.cancel_all()
        log.info(f"[BOT] Cancelled {n} orders")

        time.sleep(1)  # M5: drain time for WS confirmations

        self.heartbeat.stop()
        self.user_ws.stop()
        self.risk.save_trades()
        self.bn.stop()
        self.cl.stop()

        # S6: Shutdown persistent thread pool
        POOL.shutdown(wait=False)

        self._summary()

    def _status(self, windows: List[MarketWindow]):
        s = self.risk.stats
        w, l = s["w"], s["l"]
        wr = w / (w + l) * 100 if (w + l) > 0 else 0
        active = len(self.engine.quotes)
        total_fills = sum(
            self.pairs.get(sl)["entries"]
            for sl in self.pairs._windows
        )
        deployed = sum(
            self.pairs.get(sl)["up_cost"] + self.pairs.get(sl)["dn_cost"]
            for sl in self.pairs._windows
        )

        # Count states
        states = defaultdict(int)
        for qs in self.engine.quotes.values():
            states[qs["state"].value] += 1
        state_str = " ".join(f"{k}={v}" for k, v in sorted(states.items()))

        btc_bn = self.bn.get_asset("btc") or 0
        hb_str = self.heartbeat.stats if not self.paper else "paper"
        user_ws_str = "\u2713" if self.user_ws.connected else "\u2717"

        log.info(
            f"--- #{self._tick} | BTC=${btc_bn:,.0f} | "
            f"{w}W/{l}L {wr:.0f}% PnL=${s['pnl']:+.4f} "
            f"(maker=${s['maker_pnl']:+.3f} merge=${s['merge_pnl']:+.3f} "
            f"hedge=${s['hedge_pnl']:+.3f}) | "
            f"fills={total_fills} merges={s['merges']} "
            f"hedges={s['hedges']} | "
            f"active={active} deployed=${deployed:.1f} | "
            f"states=[{state_str}] | "
            f"windows={len(windows)} | HB:{hb_str} WS:{user_ws_str} ---"
        )

    def _banner(self):
        tfs = ",".join(f"{t}m" for t in sorted(Config.TIMEFRAMES))
        mode = "PAPER" if self.paper else "LIVE (CLOB API)"
        cl_mode = "RTDS WebSocket" if self.cl.is_rtds_connected() else "Binance fallback"
        print("=" * 72)
        print("  POLYMARKET v4.1 — SEQUENTIAL PAIR ACCUMULATOR (CORRECTED)")
        print("=" * 72)
        print(f"  {mode} | {tfs} windows | "
              f"{', '.join(sorted(Config.ASSETS))}")
        print()
        print("  v4.1 UPGRADES:")
        print(f"    CL: Chainlink oracle = {cl_mode}")
        print("    EX1: Consecutive failure backoff "
              f"(threshold={Config.EXEC_BACKOFF_THRESHOLD}, "
              f"max={Config.EXEC_BACKOFF_MAX}s)")
        print("    EX2: Post-only cross detection (retry 1\u00a2 below ask)")
        print()
        print("  QUOTING:")
        print(f"    ${Config.QUOTE_STAKE}/quote | "
              f"Edge: {Config.QUOTE_EDGE_MIN*100:.1f}\u00a2\u2013"
              f"{Config.QUOTE_EDGE_MAX*100:.0f}\u00a2 (dynamic)")
        print(f"    Repost: >{Config.QUOTE_REPOST_THRESHOLD*100:.0f}\u00a2 | "
              f"Max: ${Config.QUOTE_MAX_PER_SIDE}/side")
        print(f"    Imbalance cap: {Config.MAX_IMBALANCE*100:.0f}%")
        print()
        print("  PAIR STATE MACHINE:")
        print("    IDLE \u2192 LEG1 \u2192 LEG2 \u2192 COMPLETE")
        print(f"    Passive: {Config.COMPLETION_PASSIVE_SEC}s \u2192 "
              f"Aggressive: {Config.COMPLETION_AGGRESSIVE_SEC}s \u2192 "
              f"FAK: {Config.COMPLETION_FAK_SEC}s")
        print(f"    Min edge: {Config.COMPLETION_MIN_EDGE*100:.0f}\u00a2")
        print()
        print("  RISK:")
        print(f"    Max exposure: ${Config.MAX_EXPOSURE} | "
              f"Daily loss: ${Config.MAX_DAILY_LOSS}")
        print(f"    Regime: vol>{Config.REGIME_VOL_THRESHOLD*100:.0f}% pause | "
              f"spread>{Config.REGIME_SPREAD_MAX*100:.0f}\u00a2 block")
        print(f"    Orphan hedge: dynamic max(30,wmin*60*0.15) | "
              f"Stop: T-{Config.STOP_QUOTING_LEFT}s | "
              f"Cancel: T-{Config.CANCEL_ALL_LEFT}s")
        print()
        print("  INFRA (v4.1):")
        print(f"    Heartbeat: {Config.HEARTBEAT_INTERVAL}s | "
              f"Batch: up to {Config.BATCH_ORDER_MAX}")
        print("    User WS: real-time fills | Merge-first: ON")
        print(f"    FAK order type (partial fills) | "
              f"Native merge/redeem ($1.00)")
        print(f"    BN stale protect: {Config.BN_STALE_MAX_SEC}s | "
              f"Edge floor: {Config.QUOTE_EDGE_MIN*100:.1f}\u00a2")
        print()
        print(f"  MERGE: {'ON' if Config.MERGE_ENABLED else 'OFF'} | "
              f"interval={Config.MERGE_INTERVAL}s | "
              f"native redeem preferred")
        print("=" * 72)

    def _summary(self):
        s = self.risk.stats
        w, l = s["w"], s["l"]
        wr = w / (w + l) * 100 if (w + l) > 0 else 0
        print()
        print("=" * 72)
        print("  SESSION SUMMARY \u2014 v4.1")
        print(f"  Trades: {s['trades']} | {w}W/{l}L {wr:.0f}%")
        print(f"  PnL: ${s['pnl']:+.4f}")
        print(f"  Maker: {s['maker_trades']} trades, "
              f"${s['maker_pnl']:+.4f}")
        print(f"  Merges: {s['merges']} \u2192 ${s['merge_pnl']:+.4f}")
        print(f"  Hedges: {s['hedges']} \u2192 ${s['hedge_pnl']:+.4f}")
        print(f"  Open positions: {len(self.risk.positions)}")
        print(f"  Heartbeat: {self.heartbeat.stats}")
        print("=" * 72)


# =============================================================================
# MAIN
# =============================================================================

def main():
    paper = "--live" not in sys.argv
    Bot(paper=paper).run()


if __name__ == "__main__":
    main()
