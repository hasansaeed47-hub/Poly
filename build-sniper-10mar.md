# Build Sniper — 10 March 2026 Live Deployment

## Status: LIVE
## Mode: Paper with real feeds (no execution)

---

## Global Rules

| Rule | Value |
|------|-------|
| **Stake** | $5.00 per trade per engine |
| **Max Drawdown** | $50.00 — hard kill switch, all engines halt if cumulative net P&L across all engines hits -$50 |
| **Position Management** | Force close every position — no orphans. Every entry must resolve via SL, TP, or settlement. No position survives past window close + settle delay |
| **Stop Loss (ALL engines)** | If share bid drops to ≤ 50% of entry price → immediate taker exit |
| **Taker Fee** | 1.5% on entry (always). 1.5% on exit (SL/TP only). 0% on settlement |

---

## Data Feeds — Zero Tolerance

All feeds must be live and streaming real prices **continuously** from window open to window close. No gaps, no stale data tolerance, no REST fallback during active trading.

| Feed | Source | Protocol | Requirement |
|------|--------|----------|-------------|
| **CL Oracle** | Polymarket RTDS | `wss://ws-live-data.polymarket.com` | Continuous from open to close. If CL feed drops mid-window, **freeze all entries** until reconnected. No 30s staleness grace period — if last CL tick is >2s old during active scan, skip tick |
| **CLOB Order Book** | Polymarket CLOB | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | Full book maintained via incremental deltas. If WS disconnects, **freeze all entries**. REST is bootstrap only, not a trading fallback |
| **Binance Spot** | Binance | `wss://stream.binance.com:9443/ws` | BTC/USDT, ETH/USDT, SOL/USDT streams. Cross-reference against CL to detect CL lag magnitude. Tracked continuously — used for signal validation, not settlement |
| **Market Discovery** | Gamma API | REST (60s poll) | Discovery only. Not latency-sensitive |

**Price Tracking:** CL and Binance prices logged every tick (500ms) from the moment window_start is captured until settlement resolves. Full price path stored for post-session analysis.

---

## Engine Master Table

| | **Engine A** | **Engine B** | **Engine C** | **Engine D** | **Engine E** |
|---|---|---|---|---|---|
| **Name** | 5M_SNIPER | 5M_D1 | 15M_SNIPER | 15M_D1 | LATE_SCALPER |
| **Timeframe** | 5 min | 5 min | 15 min | 15 min | 5 min + 15 min |
| **Role** | Test | Control | Test | Control | Opportunistic |
| **Min Edge (Delta)** | 0.04 (4%) | 0.08 (8%) | 0.04 (4%) | 0.12 (12%) | N/A — price-based |
| **Continuity** | 4 ticks (2.0s) | 0 (instant) | 4 ticks (2.0s) | 0 (instant) | 0 (instant) |
| **Entry Window** | 20s – 270s left | 20s – 270s left | 60s – 840s left | 60s – 840s left | **≤ 25s left only** |
| **Entry Trigger** | BS edge ≥ 4% for 4 consecutive ticks | BS edge ≥ 8% on any tick | BS edge ≥ 4% for 4 consecutive ticks | BS edge ≥ 12% on any tick | **Book price ≥ 0.95 on higher-probability side** |
| **Entry Method** | Taker (buy best ask) | Taker (buy best ask) | Taker (buy best ask) | Taker (buy best ask) | **Maker first → chase to 0.975 → taker if unfilled** |
| **Stop Loss** | **Yes — bid ≤ 50% of entry** | **Yes — bid ≤ 50% of entry** | **Yes — bid ≤ 50% of entry** | **Yes — bid ≤ 50% of entry** | **Yes — bid ≤ 50% of entry** |
| **Take Profit** | No — hold to settlement | No — hold to settlement | No — hold to settlement | No — hold to settlement | No — hold to settlement (25s max) |
| **Max Hold Time** | Up to 270s | Up to 270s | Up to 840s | Up to 840s | **≤ 25 seconds** |
| **Settlement** | Binary (CL close > open → 1.0) | Binary (CL close > open → 1.0) | Binary (CL close > open → 1.0) | Binary (CL close > open → 1.0) | Binary (CL close > open → 1.0) |
| **Sigma Window** | 180s | 180s | 600s | 600s | N/A — no BS model |

---

## Engine E — `LATE_SCALPER` Detail

Engine E is fundamentally different from A-D. It doesn't use the Black-Scholes model. It's a late-window conviction scalper.

**Logic:**
1. When **≤ 25 seconds** remain in any window (5m or 15m)
2. Check which side (YES or NO) has **book best ask ≥ 0.95**
3. That side is the market's implied winner with 95%+ confidence
4. **Place a maker (limit) order** at the current best bid + 1 tick on that side
5. If not filled, **chase**: improve bid every tick up to a maximum entry price of **0.975**
6. If still unfilled at 0.975, **cross the spread** — buy taker at best ask (up to 0.975)
7. If best ask > 0.975, **skip** — edge too thin after fees
8. Hold to settlement (max ~25s)

**Why it works (in theory):**
- At 25s left, CL has essentially already determined the outcome
- A 0.95 book price means the market agrees the outcome is nearly certain
- Buying at 0.95-0.975 and settling at 1.00 = 2.5-5% gross return in 25 seconds
- After 1.5% taker fee: 1.0-3.5% net return
- Maker entry saves the 1.5% fee entirely — pure profit on the spread

**SL still applies:** If a sudden CL reversal crashes the book from 0.95 to below 0.475 (50% of 0.95), exit immediately. This is the black swan protection.

---

## Stop Loss Detail (All Engines)

```
Every 500ms tick while position is open:
  current_bid = best bid on our side (YES or NO)
  sl_trigger  = entry_price × 0.50

  if current_bid ≤ sl_trigger:
    → Immediate taker sell at current_bid
    → Pay 1.5% exit fee
    → Log as STOP_LOSS exit
```

**Key difference from previous SL design:** The old SL checked *fair value* vs entry price with a 30% window cutoff. The new SL checks the **actual bid** (what we can sell for) at **50% of entry price**, with **no time cutoff** — it fires at any point in the window.

**Example:**
- Enter YES at 0.60. SL trigger = 0.30
- If bid drops to 0.30 at any point → sell at 0.30, lose $2.50 + fees
- Without SL: if settlement = NO (0.00), lose $5.00 + fees
- SL caps max loss per trade at ~52.5% of stake vs 101.5% without SL

---

## Max Drawdown Kill Switch

```
After every trade close (SL, TP, or settlement):
  cumulative_net_pnl = sum of all net P&L across ALL 5 engines

  if cumulative_net_pnl ≤ -50.00:
    → HALT all engines
    → Cancel any open maker orders (Engine E)
    → Force-close any open positions at market (taker)
    → Log KILL_SWITCH event
    → Exit process
```

**$50 budget = 10 full losses at $5 stake.** This is the absolute floor. No recovery trading.

---

## Risk Matrix

| Metric | Value |
|--------|-------|
| Stake per trade | $5.00 |
| Max engines | 5 |
| Max concurrent positions | 5 engines × ~6 markets = up to 30 theoretical (realistically 5-10) |
| Max single-trade loss (with SL) | ~$2.65 (50% of stake + 1.5% entry fee + 1.5% exit fee) |
| Max single-trade loss (no SL, settlement) | ~$5.075 (full stake + entry fee) |
| Max drawdown | $50.00 (hard kill) |
| Worst case to hit DD | ~19 consecutive SL exits, or ~10 full settlement losses |

---

## Strengths

| # | Strength | Detail |
|---|----------|--------|
| 1 | **A/B test design is clean** | A vs B (5m) and C vs D (15m) isolate exactly two variables: delta threshold + continuity. Easy to measure which matters |
| 2 | **Continuity filter attacks the right problem** | V1's 0% win rate was driven by false entries on transient signals. 2s confirmation directly targets this failure mode |
| 3 | **Engine E exploits a different edge entirely** | Late-window scalping doesn't depend on BS model accuracy at all — pure market microstructure play. If A-D fail because the model is still wrong, E can still win |
| 4 | **Universal SL caps tail risk** | 50% bid SL means max loss per trade is ~$2.65 instead of ~$5.08. Doubles the runway before hitting DD limit |
| 5 | **Maker-first on Engine E** | Saving 1.5% fee on a 2.5-5% gross trade is massive — it's 30-60% of the profit. Maker entry is the correct approach for a conviction play |
| 6 | **Binance cross-reference** | BN feed validates whether CL lag is real or if CL has already caught up. Prevents entering on stale signals where CL is about to snap back |
| 7 | **Zero-tolerance feed policy** | Previous version had 30s staleness grace → entered on stale book data. No tolerance = no garbage entries |
| 8 | **$5 stake is appropriately small** | At $5, even 10 losses = $50 = kill switch. Low enough to learn real market dynamics without material financial damage |

---

## Weaknesses

| # | Weakness | Detail | Severity |
|---|----------|--------|----------|
| 1 | **SL at 50% is extremely wide for binary options** | A binary option at 0.60 that drops to 0.30 is already a massive adverse move. By the time bid hits 50% of entry, the trade is already catastrophically wrong. SL saves ~50% of stake but still realizes a large loss | **Medium** — better than no SL, but the damage is done |
| 2 | **Engine E maker order may never fill** | With ≤25s left and price at 0.95+, the spread is tight and competitive. Maker orders may sit unfilled for 10-15s, then chasing to 0.975 eats most of the edge. In practice E may frequently enter at 0.97+ for only 1.5% gross after fees | **High** — fill rate is the make-or-break metric for E |
| 3 | **Engine E has no model — pure price level** | "Price ≥ 0.95" doesn't account for *why* it's at 0.95. A 0.95 book with CL barely above open (volatile) is far riskier than 0.95 with CL 2% above open. No volatility adjustment | **Medium** — could add CL delta filter to E |
| 4 | **Continuity filter adds latency to A/C** | 2s delay means entering 2s later into a move that may already be fading. If CL mean-reverts quickly, A/C enter at worse prices than B/D would have. Continuity protects against noise but taxes real signals | **Medium** — net effect unknown until live data |
| 5 | **4% edge threshold (A/C) is aggressive** | After 1.5% fee, a 4% raw edge is only 2.5% net. This is slim margin — small model errors in sigma estimation or BS assumptions eat it entirely. D1's 8%/12% thresholds had more buffer for model error | **High** — if BS model is even slightly miscalibrated, A/C trade on phantom edge |
| 6 | **No position sizing by confidence** | $5 flat on a 4% edge and a 25% edge. Engine A's marginal signals get the same capital as Engine D's high-conviction signals. Risk-adjusted sizing would improve capital efficiency | **Low** — acceptable for paper/validation phase |
| 7 | **5 engines on same markets = correlated losses** | All 5 engines trade the same BTC/ETH/SOL windows. A bad window (CL whipsaw at settlement) hits all 5 simultaneously. DD limit can be consumed in a single bad cycle | **High** — $50 DD / 5 engines = only $10 per-engine budget |

---

## What's Being Overlooked

| # | Blind Spot | Why It Matters |
|---|------------|----------------|
| 1 | **CL oracle update frequency is variable** | CL doesn't update every second — it updates on deviation thresholds (~0.1% for BTC) or heartbeat (~60s). During low-vol periods, CL may not update for 30-60s. The scanner sees "no movement" but actually has no data. Sigma estimation on sparse data is unreliable. **No current handling for CL update drought.** |
| 2 | **Binance feed is tracked but not used in any engine's entry logic** | The paper says "cross-reference" but no engine actually gates on BN-CL divergence. BN is just logged. To be useful, engines should reject entries where BN has already reversed but CL hasn't updated yet (CL lag is closing, not opening) |
| 3 | **Book depth is ignored — only best ask/bid matters** | A 0.95 best ask with 1 share behind it is very different from 0.95 with $500 behind it. Engine E especially needs depth — if the 0.95 level is thin, a single $50 market order can crash it to 0.80. The SL at 50% doesn't help if the move is a single tick |
| 4 | **No handling for settlement ties (CL close = CL open)** | Current code: `if settle > open → YES else NO`. If settle == open, NO wins. But the probability model (BS) assigns ~50% to this case. Rare but can happen on low-vol windows — systematic bias toward NO on flat markets |
| 5 | **Engine E has no protection against last-second CL reversal** | With 25s left, CL can still update 5-10 more times. A single 0.15% CL move against the position can flip the 0.95 to 0.70 in seconds. The 50% SL trigger (0.475) may not even fire before settlement resolves the loss. **25s is not "safe" — it's just shorter exposure** |
| 6 | **Maker order management for Engine E is complex** | Place → monitor fill → cancel if not filled → resubmit higher → repeat → eventually cross. This is an order management state machine that doesn't exist in the current codebase. The scanner has zero order placement logic — it's paper-only. Building this for live is a significant engineering effort |
| 7 | **Fee model assumes 1.5% flat** | Polymarket uses a tiered fee schedule based on price. Near 0.50: ~2% fee. Near 0.95: ~0.5% fee. Engine E's edge calculation should use the actual fee at the 0.95+ price point (much lower than 1.5%), which actually *improves* the economics. Engines A-D entering at 0.40-0.60 face higher fees than modeled |
| 8 | **No window-to-window correlation analysis** | If BTC 5m window at 14:00 loses, what's the probability the 14:05 window also loses? CL lag patterns may be autocorrelated. Entering the next window after a loss may be doubling down on the same regime. No cooldown logic exists |
| 9 | **Max DD ($50) is across all engines but SL is per-trade** | Between SL checks (500ms), a flash crash can gap through the SL level. If BTC drops 5% in 500ms, all 5 engines' positions gap to 0.00 simultaneously. That's 5 × $5.08 = $25.40 in a single tick — half the DD budget gone before kill switch fires |
| 10 | **Open price capture is still racy** | Max 5s delay is allowed. On 5m windows, 5s is 1.7% of the window. A stale open price shifts the entire BS model — every fair value is biased. This was flagged in v1 review and the tolerance was kept. For engines with 4% edge threshold, a 1-2% open price error is catastrophic |

---

## Engine Comparison: When Each Wins and Loses

| Scenario | A (5m Sniper) | B (5m D1) | C (15m Sniper) | D (15m D1) | E (Late Scalp) |
|----------|---|---|---|---|---|
| **Strong CL trend, book lagging** | Wins (enters after 2s confirmation) | Wins (enters immediately, better price) | Wins (enters after 2s) | Wins (if edge > 12%) | Wins (price > 0.95 with 25s left) |
| **Noisy CL, book tracking well** | Skips (continuity filter saves) | **Loses (enters on noise)** | Skips | Skips (12% rarely hit) | Skips (price ~0.50, < 0.95) |
| **CL whipsaw at settlement** | Loses (entered on real signal, CL reversed) | Loses | Loses | Loses | **Loses hard (bought 0.95, settles 0.00)** |
| **Low vol, CL barely moves** | Skips (no edge) | Skips | Skips | Skips | Skips (price stays ~0.50) |
| **Large CL move, fast book reprice** | Skips (by tick 4, book has caught up, edge gone) | **Wins (entered on tick 1 before reprice)** | Skips | Maybe enters | Wins (already at 0.95+) |
| **CL lag only on one asset** | Wins on that asset | Wins on that asset | Wins on that asset | Maybe | Wins on that asset |

**Key tension:** Engine A's continuity filter and Engine B's instant entry are **anti-correlated in success scenarios**. When signals are real but brief, B wins. When signals are noisy, A wins. This is why running both is the correct test.

---

## Config (Reference Only — No Code Changes)

```toml
[configs.a]
name           = "5M_SNIPER"
tf             = 5
min_edge       = 0.04
max_secs_left  = 270
min_secs       = 20
stop_loss      = true
sl_threshold   = 0.50       # bid ≤ 50% of entry
take_profit    = false
continuity     = 4

[configs.b]
name           = "5M_D1"
tf             = 5
min_edge       = 0.08
max_secs_left  = 270
min_secs       = 20
stop_loss      = true
sl_threshold   = 0.50
take_profit    = false
continuity     = 0

[configs.c]
name           = "15M_SNIPER"
tf             = 15
min_edge       = 0.04
max_secs_left  = 840
min_secs       = 60
stop_loss      = true
sl_threshold   = 0.50
take_profit    = false
continuity     = 4

[configs.d]
name           = "15M_D1"
tf             = 15
min_edge       = 0.12
max_secs_left  = 840
min_secs       = 60
stop_loss      = true
sl_threshold   = 0.50
take_profit    = false
continuity     = 0

[configs.e]
name           = "LATE_SCALPER"
tf             = 0           # 0 = both 5m and 15m
min_edge       = 0.0         # not used — price-based entry
max_secs_left  = 25
min_secs       = 3           # need at least 3s for maker attempt
stop_loss      = true
sl_threshold   = 0.50
take_profit    = false
continuity     = 0
entry_mode     = "maker_chase"
min_book_price = 0.95
max_entry_price = 0.975
```

---

*Build Sniper — 10 March 2026*
*5 Engines | $5 Stake | $50 Max DD | Universal 50% SL*
*Paper session with real feeds — no execution*
