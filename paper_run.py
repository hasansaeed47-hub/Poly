#!/usr/bin/env python3
"""
paper_run.py — DSA bot, continuous paper mode, max-realism simulation.

Runs BTC 5m + 15m windows indefinitely.
Fills only when market ask reaches the posted bid (realistic maker fills).
Spawns fresh windows on next clock boundary after each close.
No network access required.
"""
import dsa_bot
import paper_sim
from dsa_bot import Cfg, DsaBot

# ── Config ──────────────────────────────────────────────────────
Cfg.ASSETS        = {"btc"}
Cfg.TIMEFRAMES    = {5, 15}
Cfg.BASE_CHUNK    = 5
Cfg.MAX_IMBALANCE = 5

# ── Capital cap per window (monkey-patch Engine.tick) ────────────
MAX_CAPITAL = 10.0
_orig_tick  = dsa_bot.Engine.tick

async def _capped_tick(self, up_book, dn_book):
    deployed = self.st.up.confirmed_cost + self.st.dn.confirmed_cost
    if deployed >= MAX_CAPITAL:
        return
    await _orig_tick(self, up_book, dn_book)

dsa_bot.Engine.tick = _capped_tick

# ── Run ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    dsa_bot.log.info("[PAPER] BTC 5m+15m — max-realism simulation — $%.0f cap/window", MAX_CAPITAL)
    bot = DsaBot(paper=True)
    paper_sim.apply(bot)
    bot.run()
