#!/usr/bin/env python3
"""
Live test: BTC 15-min window only. Max $10 capital. Single window then exit.
Run at 3:00 AM PKT (22:00 UTC).
"""
import dsa_bot
from dsa_bot import Cfg, DsaBot, Phase

Cfg.ASSETS         = {"btc"}
Cfg.TIMEFRAMES     = {15}
Cfg.MAX_CAPITAL    = 10.0
Cfg.BASE_CHUNK     = 5
Cfg.MAX_IMBALANCE  = 5

_orig_tick = dsa_bot.Engine.tick

async def _capped_tick(self, up_book, dn_book):
    st = self.st
    if st.up.confirmed_cost + st.dn.confirmed_cost >= Cfg.MAX_CAPITAL:
        return
    await _orig_tick(self, up_book, dn_book)

dsa_bot.Engine.tick = _capped_tick

async def _single_window_maint(self):
    import asyncio
    completed = False
    while self._running:
        await asyncio.sleep(10)
        for mid in list(self._engines):
            eng = self._engines[mid]
            st  = eng.st
            st.up.expire_pending(); st.dn.expire_pending()
            if st.phase == dsa_bot.Phase.DONE or st.time_left <= 0:
                pnl  = st.paired * (1.0 - st.combined) if st.paired > 0 else 0.0
                r_up = max(0.0, st.up.confirmed_qty - st.paired)
                r_dn = max(0.0, st.dn.confirmed_qty - st.paired)
                self._risk.record(pnl, r_up, r_dn)
                dsa_bot.log.info(
                    f"[TEST] DONE {mid} pairs={st.paired:.0f} "
                    f"pnl=${pnl:.3f} residual=({r_up:.0f}up {r_dn:.0f}dn)"
                )
                self._drop(mid)
                completed = True
        if completed:
            dsa_bot.log.info("[TEST] Window complete — exiting.")
            self._running = False
            return

dsa_bot.DsaBot._maintenance_loop = _single_window_maint

if __name__ == "__main__":
    dsa_bot.log.info("[TEST] BTC 15m — max $10 — live")
    DsaBot(paper=False).run()
