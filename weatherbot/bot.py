"""
WeatherBot: main loop orchestrating feeds, engine, execution, state.
"""

import os
import signal
import time
import traceback
from datetime import datetime, timezone
from typing import Dict, List

from weatherbot.config import (
    log, CITIES,
    MAX_DEPLOYED, MAX_POSITIONS, MIN_POSITION,
    MAX_LOSS_PER_CITY, DAILY_LOSS_LIMIT,
    SCAN_INTERVAL, ENSEMBLE_INTERVAL, EVENT_CACHE_INTERVAL,
    WHALE_REFRESH_INTERVAL,
    SCALP_TARGET, SCALP_TIMEOUT, KELLY_FRACTION, MAX_POSITION,
    STATE_FILE,
)
from weatherbot.models import Forecast, Position
from weatherbot.feeds import get_forecast
from weatherbot.market import (
    gamma_find_weather_events, extract_city_buckets,
    get_live_prices, fetch_weather_leaderboard, fetch_recent_trades,
)
from weatherbot.engine import (
    forecast_to_probs, compute_prev_probs,
    is_model_run_window,
    play1_eod_lock, play2_shift_scalp, play3_no_grind,
    play4_whale_flow, scalp_exit,
)
from weatherbot.execution import OrderManager
from weatherbot.state import save_state, load_state, restore_positions


class WeatherBot:
    def __init__(self, paper: bool = True):
        self.paper = paper
        self.exec = OrderManager(paper=paper)
        self.positions: List[Position] = []
        self.forecasts: Dict[str, Forecast] = {}
        self.observed_highs: Dict[str, float] = {}
        self.whale_wallets: set = set()
        self.pnl = 0.0
        self.trades_count = 0
        self.daily_pnl = 0.0
        self.daily_no_profit = 0.0
        self.city_pnl: Dict[str, float] = {}
        self._running = True
        self._last_day = ""
        self._last_whale_refresh = 0.0
        self._play_stats: Dict[str, dict] = {}
        self._cached_events: List[dict] = []
        self._events_fetched_at = 0.0

    # ── Public entry point ──

    def run(self):
        self._banner()
        self._restore()

        if not self.paper:
            if not self.exec.init_live():
                log.error("Live init failed -- falling back to paper")
                self.paper = True
                self.exec.paper = True

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        self._refresh_whales()
        self._update_forecasts()

        tick = 0
        while self._running:
            try:
                tick += 1

                # Day rollover
                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                if today != self._last_day:
                    self._last_day = today
                    self.daily_pnl = 0.0
                    self.daily_no_profit = 0.0
                    self.observed_highs.clear()
                    log.info(f"[BOT] New day: {today}")

                # Periodic updates
                if tick == 1 or tick % (ENSEMBLE_INTERVAL // SCAN_INTERVAL) == 0:
                    self._update_forecasts()

                if time.time() - self._last_whale_refresh > WHALE_REFRESH_INTERVAL:
                    self._refresh_whales()

                # Core tick
                self._tick()
                self._status()
                self._save()

                # Cancel stale live orders
                self.exec.cancel_stale_orders()

                # Sleep with signal check
                for _ in range(SCAN_INTERVAL):
                    if not self._running:
                        break
                    time.sleep(1)

            except KeyboardInterrupt:
                break
            except Exception:
                log.error(f"[BOT] Tick error:\n{traceback.format_exc()}")
                time.sleep(10)

        # Clean shutdown
        self.exec.cancel_all()
        self._save()
        self._summary()

    # ── Signal handler ──

    def _handle_signal(self, signum, frame):
        log.info(f"[BOT] Signal {signum}, shutting down...")
        self._running = False

    # ── State management ──

    def _restore(self):
        state = load_state()
        if not state:
            return
        self.positions = restore_positions(state)
        self.pnl = state.get("pnl", 0.0)
        self.daily_pnl = state.get("daily_pnl", 0.0)
        self.city_pnl = state.get("city_pnl", {})
        self._play_stats = state.get("play_stats", {})
        self.observed_highs = state.get("observed_highs", {})
        open_count = sum(1 for p in self.positions if not p.settled and not p.sold)
        log.info(f"[STATE] Restored {open_count} open positions, pnl=${self.pnl:+.2f}")

    def _save(self):
        save_state(
            self.positions, self.observed_highs,
            self.pnl, self.daily_pnl, self.city_pnl, self._play_stats,
        )

    # ── Data refresh ──

    def _refresh_whales(self):
        wallets = fetch_weather_leaderboard()
        if wallets:
            self.whale_wallets = set(wallets)
        self._last_whale_refresh = time.time()

    def _update_forecasts(self):
        for city, info in CITIES.items():
            prev_fc = self.forecasts.get(city)
            fc = get_forecast(
                city, info, prev_forecast=prev_fc,
                observed_highs=self.observed_highs,
            )
            if fc:
                self.forecasts[city] = fc
            time.sleep(0.5)

    def _get_events(self) -> List[dict]:
        now = time.time()
        if self._cached_events and (now - self._events_fetched_at) < EVENT_CACHE_INTERVAL:
            return self._cached_events
        events = gamma_find_weather_events()
        if events:
            self._cached_events = events
            self._events_fetched_at = now
        return self._cached_events or events

    # ── Core tick ──

    def _tick(self):
        """
        One scan + trade cycle:
          0. Circuit breaker
          1. Scalp exits
          2. Play 1: EOD Lock
          3. Play 2: Shift Scalp
          4. Play 3: NO Grind
          5. Play 4: Whale Flow
          6. Execute sells, then buys
          7. Settlement check
        """
        if self.daily_pnl < -DAILY_LOSS_LIMIT:
            log.warning(f"[CIRCUIT BREAKER] Daily loss ${self.daily_pnl:.2f}. Halted.")
            return

        events = self._get_events()
        if not events:
            log.info("[TICK] No weather markets found")
            return

        in_window = is_model_run_window()
        if in_window:
            log.info("[TIMING] GFS model run window -- optimal for Play 2")

        sell_trades = []
        buy_trades = []
        deployed = sum(p.cost for p in self.positions if not p.settled and not p.sold)
        available = MAX_DEPLOYED - deployed
        held_tids = {p.token_id for p in self.positions if not p.settled and not p.sold}
        blocked_cities = {
            c for c, pnl in self.city_pnl.items() if pnl < -MAX_LOSS_PER_CITY
        }

        for ev in events:
            city, buckets = extract_city_buckets(ev)
            if not buckets:
                continue

            # Live prices
            all_tids = []
            for b in buckets:
                all_tids.extend([b.token_yes, b.token_no])
            live = get_live_prices(all_tids)
            for b in buckets:
                if b.token_yes in live:
                    b.yes_price = live[b.token_yes]
                if b.token_no in live:
                    b.no_price = live[b.token_no]

            fc = self.forecasts.get(city)
            if not fc:
                continue

            buckets = forecast_to_probs(fc, buckets)
            prev_probs = compute_prev_probs(fc, buckets)

            # Log market state
            title = ev.get("title", "?")
            etag = f"ens={len(fc.ensemble_highs)}m" if fc.has_ensemble else "fallback"
            stag = "shift" if fc.has_prev_ensemble else "first-run"
            obs_tag = f" obs_high={fc.observed_high_f:.0f}F" if fc.has_observation else ""
            log.info(
                f"[{city}] {title} -- {len(buckets)}b high={fc.high_f:.0f}F "
                f"({fc.source},{etag},{stag}){obs_tag}"
            )

            for b in sorted(buckets, key=lambda x: x.low_temp):
                old_p = prev_probs.get(b.token_yes, b.our_prob)
                shift = b.our_prob - old_p
                marker = " *" if b.our_prob > 0.15 else (" ." if b.our_prob < 0.02 else "")
                st = f" [{shift:+.0%}]" if abs(shift) >= 0.03 else ""
                log.info(
                    f"  {b.label:20s} Y={b.yes_price:.2f} N={b.no_price:.2f} "
                    f"p={b.our_prob:.1%}{marker}{st}"
                )

            # Scalp exits
            se = scalp_exit(self.positions, buckets)
            for t in se:
                t["city"] = city
            sell_trades.extend(se)

            if city in blocked_cities:
                continue

            info = CITIES.get(city, {})

            # Play 1: EOD Lock
            p1 = play1_eod_lock(buckets, fc, city, info)
            for t in p1:
                t["city"] = city
            buy_trades.extend(p1)

            # Play 2: Shift Scalp
            p2 = play2_shift_scalp(buckets, fc, prev_probs, available)
            for t in p2:
                t["city"] = city
            buy_trades.extend(p2)

            # Play 3: NO Grind
            p3 = play3_no_grind(buckets)
            for t in p3:
                t["city"] = city
            buy_trades.extend(p3)

            # Play 4: Whale Flow
            if self.whale_wallets:
                cids = list({b.condition_id for b in buckets if b.condition_id})
                recent = []
                for cid in cids[:3]:
                    recent.extend(fetch_recent_trades(cid))
                    time.sleep(0.2)
                p4 = play4_whale_flow(buckets, self.whale_wallets, recent)
                for t in p4:
                    t["city"] = city
                buy_trades.extend(p4)

        # Execute sells
        for t in sell_trades:
            tid = t["token_id"]
            shares = t.get("shares", 0)
            price = t["price"]
            reason = t.get("reason", "")
            city = t.get("city", "?")
            taker = t.get("taker", False)

            tag = "TAKE" if taker else "SELL"
            log.info(f"  -> {tag} {t['label'][:35]} @ {price:.2f} ({shares:.0f}sh) -- {reason}")
            oid = self.exec.sell(tid, price, shares, taker=taker)
            if oid:
                for pos in self.positions:
                    if pos.token_id == tid and not pos.sold and not pos.settled:
                        pos.sold = True
                        pos.sell_price = price
                        revenue = shares * price
                        profit = revenue - pos.cost
                        self.pnl += profit
                        self.daily_pnl += profit
                        self.city_pnl[city] = self.city_pnl.get(city, 0.0) + profit
                        deployed -= pos.cost
                        available += pos.cost
                        held_tids.discard(tid)
                        self._track_play(pos.play, profit)
                        log.info(f"    SOLD: cost=${pos.cost:.2f} rev=${revenue:.2f} pnl=${profit:+.2f}")
                        break

        # Execute buys
        open_count = sum(1 for p in self.positions if not p.settled and not p.sold)
        for t in buy_trades:
            tid = t["token_id"]
            if tid in held_tids:
                continue
            if deployed >= MAX_DEPLOYED:
                continue
            if open_count >= MAX_POSITIONS:
                continue

            price = t["price"]
            stake = t.get("stake", MIN_POSITION)
            side = t["side"]
            prob = t.get("our_prob", 0.0)
            play = t["play"]
            city = t.get("city", "?")

            log.info(f"  -> [{play}] BUY {side} {t['label'][:35]} @ {price:.2f} ${stake:.2f}")
            oid = self.exec.buy(tid, price, stake)
            if oid:
                shares = stake / price
                pos = Position(
                    token_id=tid, label=t["label"], side=side,
                    buy_price=price, shares=shares, cost=stake,
                    bought_at=time.time(), play=play, city=city,
                    entry_prob=prob, order_id=oid,
                )
                self.positions.append(pos)
                self.trades_count += 1
                deployed += stake
                available -= stake
                held_tids.add(tid)
                open_count += 1

        self._check_settlements()

    # ── Play stats ──

    def _track_play(self, play: str, profit: float):
        if play not in self._play_stats:
            self._play_stats[play] = {"trades": 0, "pnl": 0.0, "wins": 0, "losses": 0}
        self._play_stats[play]["trades"] += 1
        self._play_stats[play]["pnl"] += profit
        if profit >= 0.0:
            self._play_stats[play]["wins"] += 1
        else:
            self._play_stats[play]["losses"] += 1

    # ── Settlement ──

    def _check_settlements(self):
        """
        Check for settled positions via token price heuristic.
        Token near $1.00 = our side won. Token near $0.00 = our side lost.
        """
        open_pos = [p for p in self.positions if not p.settled and not p.sold]
        if not open_pos:
            return

        tids = [p.token_id for p in open_pos]
        prices = get_live_prices(tids)

        for pos in open_pos:
            price = prices.get(pos.token_id)
            if price is None:
                continue

            if price >= 0.95:
                pos.settled = True
                pos.payout = pos.shares * 1.0
                profit = pos.payout - pos.cost
                self.pnl += profit
                self.daily_pnl += profit
                self.city_pnl[pos.city] = self.city_pnl.get(pos.city, 0.0) + profit
                self._track_play(pos.play, profit)
                if pos.play == "no_grind":
                    self.daily_no_profit += profit
                log.info(f"[WIN] {pos.label[:35]} ({pos.play}) +${profit:.2f}")

            elif price <= 0.05:
                pos.settled = True
                pos.payout = 0.0
                profit = -pos.cost
                self.pnl += profit
                self.daily_pnl += profit
                self.city_pnl[pos.city] = self.city_pnl.get(pos.city, 0.0) + profit
                self._track_play(pos.play, profit)
                log.info(f"[LOSS] {pos.label[:35]} ({pos.play}) -${pos.cost:.2f}")

    # ── Display ──

    def _status(self):
        op = [p for p in self.positions if not p.settled and not p.sold]
        st = [p for p in self.positions if p.settled]
        w = sum(1 for p in st if p.payout > 0)
        l = sum(1 for p in st if p.payout == 0)
        dep = sum(p.cost for p in op)

        plays = {}
        for p in op:
            plays[p.play] = plays.get(p.play, 0) + 1
        ps = " ".join(f"{k}={v}" for k, v in sorted(plays.items()))

        log.info(
            f"[STATUS] open={len(op)} ${dep:.0f} | {w}W/{l}L pnl=${self.pnl:+.2f} "
            f"daily=${self.daily_pnl:+.2f} | whales={len(self.whale_wallets)} | {ps}"
        )

    def _banner(self):
        print("=" * 70)
        print("  WEATHER BOT v6 -- 4-PLAY SYSTEM")
        print("=" * 70)
        print(f"  Mode: {'PAPER' if self.paper else 'LIVE'}")
        print(f"  Cities: {', '.join(CITIES.keys())}")
        print()
        print("  PLAYS:")
        print("    1. EOD LOCK     -- Buy known winner (obs), hold to settlement")
        print("    2. SHIFT SCALP  -- Buy forecast shift, sell fast (30min max)")
        print("    3. NO GRIND     -- Buy NO on dead buckets, hold to settlement")
        print("    4. WHALE FLOW   -- Copy top weather traders, sell fast")
        print()
        print("  DATA FEEDS:")
        print("    METAR (aviationweather.gov)  -- airport obs, free")
        print("    Open-Meteo deterministic     -- point forecast, free")
        print("    Open-Meteo GFS ensemble      -- 31-member probs, free")
        print("    NOAA api.weather.gov         -- US forecast backup, free")
        wu = "YES" if os.environ.get("WU_API_KEY") else "NO (METAR fallback)"
        print(f"    Weather Underground          -- {wu}")
        print()
        print(f"  Scalp: {SCALP_TARGET * 100:.0f}c/sh target | {SCALP_TIMEOUT // 60}min timeout")
        print(f"  Kelly: {KELLY_FRACTION:.0%} | Max pos: ${MAX_POSITION} | Daily limit: ${DAILY_LOSS_LIMIT}")
        sr = "YES" if STATE_FILE.exists() else "NO (fresh start)"
        print(f"  State recovery: {sr}")
        print("=" * 70)

    def _summary(self):
        settled = [p for p in self.positions if p.settled]
        open_pos = [p for p in self.positions if not p.settled and not p.sold]
        sold = [p for p in self.positions if p.sold]

        print()
        print("=" * 70)
        print("  SESSION SUMMARY")
        print("=" * 70)
        print(f"  Trades: {self.trades_count} | Scalp exits: {len(sold)}")
        print(f"  PnL: ${self.pnl:+.2f} | NO grind: ${self.daily_no_profit:+.2f}")
        print(f"  Whale wallets tracked: {len(self.whale_wallets)}")

        if self._play_stats:
            print()
            print("  BY PLAY:")
            for play in ["eod_lock", "shift_scalp", "no_grind", "whale_flow"]:
                s = self._play_stats.get(play)
                if not s:
                    continue
                print(
                    f"    {play:15s} {s['trades']}t "
                    f"{s['wins']}W/{s['losses']}L pnl=${s['pnl']:+.2f}"
                )

        if self.city_pnl:
            print()
            print("  BY CITY:")
            for city, pnl in sorted(self.city_pnl.items()):
                print(f"    {city:15s} ${pnl:+.2f}")

        if open_pos:
            print()
            print(f"  OPEN ({len(open_pos)}):")
            for p in open_pos:
                age = (time.time() - p.bought_at) / 60.0
                print(
                    f"    [{p.play:12s}] {p.side} {p.label[:25]} "
                    f"@{p.buy_price:.2f} ${p.cost:.2f} {age:.0f}min"
                )

        print("=" * 70)
