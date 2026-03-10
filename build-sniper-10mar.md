# Build Sniper — 10 March 2026 Live Deployment

## Status: LIVE
## Mode: Paper with real feeds (no execution)

---

## Global Rules

| Rule | Value |
|------|-------|
| **Stake** | $5.00 per trade per engine |
| **Max Drawdown** | $50.00 cumulative across all engines — hard kill, process exits |
| **Stop Loss** | ALL engines: bid ≤ 50% of entry price → immediate taker exit |
| **Window Skip Post-SL** | If SL fires on a window, that engine skips the remainder of that window — no re-entry on the same slug |
| **Force Close** | Every position must close. No orphans. SL, settlement, or force-close at window_end + settle_delay — whichever comes first |
| **Fee Model** | Polymarket actual: `fee(px) = px × (1 - px) × 0.0625` — maker fee is 0% |
| **Order Approach** | ALL engines (A-E): maker-first for 3 seconds, then taker if unfilled |

---

## Fee Schedule (Actual Polymarket Formula)

```
fee(price) = price × (1 - price) × 0.0625
```

| Entry Price | Fee | Fee % of Stake | Notes |
|-------------|-----|----------------|-------|
| 0.50 | $0.0156 per share | 1.56% | Worst case — peak of curve |
| 0.40 | $0.0150 per share | 1.50% | Near-peak |
| 0.60 | $0.0150 per share | 1.50% | Near-peak |
| 0.70 | $0.0131 per share | 1.31% | Moderate |
| 0.80 | $0.0100 per share | 1.00% | Lower |
| 0.90 | $0.0056 per share | 0.56% | Low |
| 0.95 | $0.0030 per share | 0.30% | Engine E territory — very cheap |
| **Maker** | **$0.00** | **0%** | **All engines attempt maker first** |

Engines A-D typically enter at 0.40-0.65 range → 1.0-1.56% taker fee. Engine E enters at 0.95+ → 0.30% taker fee. Maker fill saves the fee entirely.

---

## Data Feeds — Zero Tolerance

All feeds must be live and streaming real prices **continuously** from window open to window close. No staleness grace. No REST fallback during active scanning.

| Feed | Source | Requirement |
|------|--------|-------------|
| **CL Oracle** | `wss://ws-live-data.polymarket.com` (RTDS) | Continuous. If last CL tick > 2s old on any scan tick → **freeze entries, keep existing positions on SL watch** |
| **CLOB Book** | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | Continuous. Full book via incremental deltas. If WS disconnects → **freeze entries immediately**. REST is bootstrap only |
| **Binance Spot** | `wss://stream.binance.com:9443/ws` (btcusdt/ethusdt/solusdt) | Continuous. Cross-reference vs CL for lag direction validation. Logged every tick for post-analysis |
| **Gamma API** | `https://gamma-api.polymarket.com` | REST 60s poll — discovery only |

**Price logging:** CL, Binance, and CLOB best bid/ask logged every 500ms tick from window_start to settlement. Full price path stored.

---

## Order Approach — All Engines

Every engine uses the same 3-phase entry sequence:

```
Phase 1: MAKER (0.0 - 3.0 seconds)
  Place limit order at best_bid + 0.01 on target side
  Every 500ms: if unfilled and book moved, re-place at new best_bid + 0.01
  If filled → done (0% fee)

Phase 2: TAKER (at 3.0 seconds if unfilled)
  Cancel maker order
  Buy at best_ask (taker)
  Pay actual fee: px × (1-px) × 0.0625

Phase 3: ABORT (if best_ask has moved beyond max entry)
  If best_ask > fair_value (A-D) or > 0.975 (E) → skip, no entry
```

**Why maker-first:** At 0.50 entry, maker saves $0.078 per $5 trade (1.56%). Over 100 trades that's $7.80 saved — meaningful against a $50 DD budget. Even partial maker fills reduce fee drag.

**Why 3 seconds:** Enough time for 6 maker attempts (500ms tick). Long enough for fills on liquid books. Short enough that signal doesn't decay — A/C with continuity already confirmed 2s of signal persistence, so 3s more is 5s total from first qualifying tick.

---

## Continuity Filter — How It's Tracked

Engines A and C use a continuity gate. Here's the exact state machine:

```
Per engine, per market slug:
  streak: HashMap<String, u32>   // slug → consecutive qualifying tick count

On each 500ms scan tick for a market:
  1. Compute signal (BS fair value, edge)
  2. Check: edge ≥ min_edge AND min_secs ≤ secs_left ≤ max_secs_left?

  If YES (qualifying tick):
    streak[slug] += 1
    If streak[slug] >= continuity_required (4):
      → Trigger entry (enter Phase 1: maker order)
      → Reset streak[slug] = 0

  If NO (non-qualifying tick):
    streak[slug] = 0   // hard reset, must restart from scratch

  On window close or settlement:
    streak.remove(slug)  // cleanup
```

**Key properties:**
- 4 consecutive qualifying ticks = 2.0 seconds of sustained edge
- Any single non-qualifying tick resets to zero — no "3 out of 5" logic
- Counter is per-engine, per-market — Engine A's counter is independent of Engine B's
- Counter resets on entry — prevents double-entry on the same sustained signal
- Engines B, D, E have continuity = 0 → enter on first qualifying tick (no counter)

---

## Stop Loss — All Engines

```
Every 500ms tick while position is open:
  current_bid = best_bid on our side (from CLOB WS book)
  sl_trigger  = entry_price × 0.50

  if current_bid ≤ sl_trigger:
    → Taker sell at current_bid immediately
    → Pay exit fee: current_bid × (1 - current_bid) × 0.0625
    → Mark slug as SKIP for this engine (no re-entry this window)
    → Log STOP_LOSS with full price path
```

**No time cutoff.** SL fires at any point — 10s in or 250s in. Previous design disabled SL in last 30% of window. That's removed.

**Window skip:** After SL, this engine will not re-enter the same market/slug for the remainder of that window. Prevents entering a second time into the same adverse move. Next window is clean — no cross-window cooldown.

---

## Flash Crash Management

A flash crash can gap through SL between 500ms ticks. Three layers of protection:

**Layer 1: SL on every tick (500ms)**
Standard check. Catches normal adverse moves.

**Layer 2: Book-driven SL (event-based)**
On every CLOB WebSocket `price_change` or `best_bid_ask` event (not just scan ticks), check SL condition for all open positions on that token. This fires on the WS event loop, not the 500ms scan — catches moves between ticks.

**Layer 3: Max drawdown kill switch**
```
After every position close:
  if cumulative_net_pnl ≤ -$50.00:
    → Cancel all open maker orders
    → Force-sell all open positions at market (taker)
    → HALT — exit process
```

**What this does NOT protect against:**
- If book gaps from 0.60 to 0.00 in a single WS event (e.g., book completely empties), the SL "sell at current_bid" gets nothing. Loss = full stake + entry fee.
- If all 5 engines hold positions on the same asset and it gaps, worst case = 5 × $5.08 = $25.40 in one event.

---

## Engine Master Table

| | **Engine A** | **Engine B** | **Engine C** | **Engine D** | **Engine E** |
|---|---|---|---|---|---|
| **Name** | 5M_SNIPER | 5M_D1 | 15M_SNIPER | 15M_D1 | LATE_SCALPER |
| **Timeframe** | 5m | 5m | 15m | 15m | 5m + 15m |
| **Role** | Test | Control | Test | Control | Opportunistic |
| | | | | | |
| **ENTRY** | | | | | |
| Min Edge | 0.04 (4%) | 0.08 (8%) | 0.04 (4%) | 0.12 (12%) | N/A |
| Entry Trigger | BS edge ≥ 4% | BS edge ≥ 8% | BS edge ≥ 4% | BS edge ≥ 12% | Book price ≥ 0.95 on leading side |
| Continuity | 4 ticks (2.0s) | 0 (instant) | 4 ticks (2.0s) | 0 (instant) | 0 (instant) |
| Entry Window (secs left) | 20 – 270 | 20 – 270 | 60 – 840 | 60 – 840 | 3 – 25 |
| Entry Window (after open) | 30s – 280s | 30s – 280s | 60s – 840s | 60s – 840s | Last 25s of window |
| Order Method | Maker 3s → taker | Maker 3s → taker | Maker 3s → taker | Maker 3s → taker | Maker 3s → taker (max 0.975) |
| Max Entry Price | Fair value | Fair value | Fair value | Fair value | 0.975 |
| Typical Entry Range | 0.40 – 0.65 | 0.40 – 0.65 | 0.40 – 0.65 | 0.40 – 0.65 | 0.95 – 0.975 |
| Typical Entry Fee | 1.0 – 1.5% | 1.0 – 1.5% | 1.0 – 1.5% | 1.0 – 1.5% | 0.15 – 0.30% |
| | | | | | |
| **EXIT** | | | | | |
| Stop Loss | Bid ≤ 50% entry | Bid ≤ 50% entry | Bid ≤ 50% entry | Bid ≤ 50% entry | Bid ≤ 50% entry |
| SL Example | Enter 0.55 → SL at bid ≤ 0.275 | Same | Same | Same | Enter 0.96 → SL at bid ≤ 0.48 |
| Take Profit | No — hold to settle | No — hold to settle | No — hold to settle | No — hold to settle | No — hold to settle |
| Settlement | Binary (CL close vs open) | Binary | Binary | Binary | Binary |
| Max Hold Time | ~270s | ~270s | ~840s | ~840s | ~25s |
| Window Skip Post-SL | Yes | Yes | Yes | Yes | Yes |
| | | | | | |
| **RISK** | | | | | |
| Max loss per trade (SL) | ~$2.65 | ~$2.65 | ~$2.65 | ~$2.65 | ~$2.58 |
| Max loss per trade (settle) | ~$5.08 | ~$5.08 | ~$5.08 | ~$5.08 | ~$5.02 |
| Sigma Window | 180s | 180s | 600s | 600s | N/A |
| BS Model Used | Yes | Yes | Yes | Yes | No |

---

## Engine E — `LATE_SCALPER` Detail

```
When secs_left ≤ 25 (any window, 5m or 15m):
  1. Check both sides: which has best_ask ≥ 0.95?
  2. That side = market's implied winner (95%+ confidence)
  3. Phase 1: Maker at best_bid + 0.01 on that side
     - Chase every 500ms up to 0.975
  4. Phase 2: At 3.0s, if unfilled → taker at best_ask
     - Reject if best_ask > 0.975
  5. Hold to settlement (≤25s)
  6. SL: if bid drops to ≤ 50% of entry → taker exit
```

**P&L scenarios at 0.96 entry (taker):**
- Win (settles 1.00): profit = (1.00 - 0.96) × shares - fee = +$0.133 per $5 (+2.7%)
- Win (maker entry at 0.95): profit = (1.00 - 0.95) × shares - 0 fee = +$0.263 per $5 (+5.3%)
- Lose (settles 0.00): loss = -$5.00 - fee = -$5.015 per $5 (-100.3%)
- SL at 0.48: loss = (0.48 - 0.96) × shares - fees = -$2.52 per $5 (-50.4%)

**Break-even win rate (taker at 0.96):** Need ~97.4% wins to break even. The market prices this at 96% (0.96 ask). Engine E bets the market is underpricing certainty by 1-2% in the final 25 seconds.

---

## Strengths

| # | Strength | Detail |
|---|----------|--------|
| 1 | **Maker-first saves real money** | At 0.50 entry: 1.56% saved = $0.078/trade. At 0.95: 0.30% saved. Over 100 trades on A-D: ~$7-8 saved. Maker fills are free alpha |
| 2 | **Actual fee model improves Engine E economics** | Old paper used flat 1.5%. Real fee at 0.95 = 0.30%. Engine E's edge is 5x better than modeled before |
| 3 | **Window skip post-SL prevents revenge trading** | Old design allowed immediate re-entry into the same adverse window. Now: SL fires → engine sits out that window. Prevents doubling down on a wrong signal |
| 4 | **Event-driven SL (Layer 2) plugs the 500ms gap** | Flash crashes between tick scans are caught by the book WS event handler. Doesn't eliminate gap risk but reduces it from 500ms to WS event latency (~10-50ms) |
| 5 | **Clean A/B isolation** | A vs B: delta + continuity only. C vs D: same. E is independent. Clear attribution of what works |
| 6 | **Continuity state machine is simple and debuggable** | One counter per slug, hard reset on miss, cleanup on settlement. No complex rolling windows or weighted scores. Easy to log and validate |
| 7 | **Binance cross-reference detects closing lag** | If BN already reversed but CL hasn't updated → the lag is closing, not opening. Prevents entering on stale signals (once wired into entry logic) |
| 8 | **$50 DD is 10 full losses — enough for statistical signal** | At $5/trade, 10 complete wipeouts before kill. If 7/10 first trades lose, that's strong evidence to stop. If 5/10 win, that's signal to continue |

---

## Weaknesses

| # | Weakness | Severity | Detail |
|---|----------|----------|--------|
| 1 | **50% SL is catastrophically wide for binaries** | **Critical** | A binary at 0.55 that drops to bid 0.275 is already a near-total loss scenario. The book doesn't smoothly decline from 0.55 to 0.275 — it gaps. Binary books are thin; they jump from 0.55 to 0.15 in one event. SL at 50% may rarely trigger before settlement resolves the position anyway |
| 2 | **Maker-first adds 3s delay to all entries** | **High** | On A/C with continuity: total delay = 2s continuity + 3s maker = **5 seconds from first signal to worst-case fill**. On a 5m window with 270s entry zone, 5s is small. But the *edge* may decay in those 5s — book reprices while we're posting makers |
| 3 | **Engine E break-even at ~97.4% win rate** | **High** | Market prices 0.96 as "96% likely to win." Engine E needs actual win rate > 97.4%. That 1.4% edge is razor-thin. One unexpected CL reversal in 25s costs 38 winning trades to recover. Risk/reward is deeply asymmetric |
| 4 | **4% edge (A/C) is below BS model error margin** | **High** | BS d2 model assumes GBM, correct sigma, and accurate open price. Sigma estimation on 180s of sparse CL ticks has wide confidence intervals. A 4% "edge" could be entirely model noise. D1's 8%/12% had buffer for this |
| 5 | **Maker fills on binary books are unreliable** | **Medium** | Binary option books are thin (often $20-100 per level). Maker orders at best_bid + 0.01 compete with other makers. Fill rate may be <30%, meaning 70%+ of entries still pay taker fee. 3s maker window may just waste time |
| 6 | **All 5 engines trade same asset universe** | **High** | BTC/ETH/SOL only. A single bad CL oracle event (e.g., CL stale for 30s then snaps) hits all 5 engines simultaneously. Maximum correlated loss = 5 × $5.08 = $25.40 — half the DD budget in one event |
| 7 | **No position sizing by conviction** | **Low** | $5 flat regardless of edge magnitude. A 25% edge trade gets same size as a 4% edge trade |

---

## What's Being Overlooked

| # | Blind Spot | Impact |
|---|------------|--------|
| 1 | **Binance feed is tracked but NOT wired into any engine's entry gate** | BN is logged but doesn't influence decisions. If BN shows BTC reversed 0.3% but CL hasn't updated, engines A-D still see "stale CL lag = edge" and enter. BN should gate entries: reject if BN-CL divergence is narrowing |
| 2 | **Book depth is completely ignored** | Only best_ask/best_bid used. A 0.95 ask with $2 behind it vs $500 behind it are treated identically. Engine E especially vulnerable — thin books at 0.95 can vanish instantly. Maker orders into thin books have no real counterparty |
| 3 | **CL oracle drought periods** | CL updates on deviation thresholds (~0.1% for BTC) or 60s heartbeat. During low vol, CL may not update for 30-60s. Scanner sees "no movement" but has no data. Sigma on sparse data is noise. No logic to detect "CL is stale" vs "CL is stable" |
| 4 | **Settlement tie (CL close = CL open)** | Code: `if settle > open → YES else NO`. Ties → NO always wins. BS model assigns ~50% to each side at open. Systematic NO bias on flat-market windows. Rare but creates hidden edge on NO side that's not modeled |
| 5 | **Open price is still racy (up to 5s late)** | max_open_delay = 5.0s. On 5m windows, BTC can move $50-200 in 5s. A stale open biases every BS fair value calculation for the entire window. For 4% edge threshold, a 2% open error creates phantom edge or hides real edge. This was flagged in v1 review and unchanged |
| 6 | **Maker order management doesn't exist in the codebase** | The scanner has zero order placement logic. It's a paper trading simulator. Building maker → cancel → re-place → taker fallback is an order management state machine that needs: CLOB API auth, order signing (Polymarket uses EIP-712), nonce management, fill tracking. This is a major engineering gap between paper and live |
| 7 | **No cross-engine awareness** | Engine A and Engine B can both enter the same market on the same tick (different thresholds, same signal). That's $10 exposure on one binary outcome. With C and D on 15m, and E on late windows, up to $25 on a single asset direction. No aggregate position limit per asset |
| 8 | **SL exit assumes book has a bid** | If the book empties completely (no bids), current_bid = 0.0. SL condition (0.0 ≤ 0.275) fires but sell at 0.0 means total loss. No handling for empty-book scenario — should force-hold to settlement instead of selling into nothing |
| 9 | **Engine E "leading side" detection is just price** | "Which side has ask ≥ 0.95" doesn't distinguish between genuine conviction (CL 2% above open with 25s left) and stale/manipulated book (someone posts a $5 ask at 0.95 to bait entries). No CL delta validation on E's trigger |
| 10 | **3s maker chase + continuity = real edge decay** | Engine A: 2s continuity + 3s maker = 5s total. In 5s, the PM book can fully reprice to fair value. The "edge" detected at t=0 may be gone by t=5 when the taker fill happens. Engines may systematically enter at worse prices than the edge that triggered them |

---

## Scenario Analysis

| Scenario | A (5m Sniper) | B (5m D1) | C (15m Sniper) | D (15m D1) | E (Late Scalp) |
|----------|---|---|---|---|---|
| **Strong CL trend, stale book** | Enters after 5s (2s cont + 3s maker). Edge may partially decay | Enters after 3s (maker). Better price than A | Enters after 5s. 15m window — plenty of time | Enters after 3s if edge > 12% (rare) | If price at 0.95+ with 25s left: enters. Otherwise skips |
| **Noisy CL, book tracking** | Continuity kills it — good | **Enters on noise → loses** | Continuity kills it — good | 12% threshold kills it | Book at ~0.50, skips |
| **CL whipsaw at settlement** | Loses. SL unlikely to fire (whipsaw is at close, not during hold) | Loses | Loses | Loses | **Loses hard**: bought 0.96, settles 0.00 = -$5.02. SL at 0.48 may not fire in 25s |
| **Low vol, flat CL** | No edge, no entry | No edge, no entry | No edge, no entry | No edge, no entry | Book at ~0.50, no entry |
| **Fast book reprice after CL move** | Continuity confirms edge but maker phase finds book already repriced → abort | Maker phase, book already moved → abort or enter at worse price | Same as A | Same as B | Depends on whether reprice reached 0.95 |
| **Flash crash (BTC -3% in 1s)** | If holding YES: book gaps to 0.00, SL fires but sells at ~0. Full loss $5.08 | Same | Same | Same | If holding: same catastrophic loss |
| **SL fires mid-window** | Exits, skips rest of window. Loses ~$2.65 | Same | Same | Same | Exits, 25s window is essentially over anyway |

---

## Config (Reference Only)

```toml
[global]
stake             = 5.0
max_drawdown      = 50.0
window_skip_on_sl = true
force_close       = true

[order]
maker_chase_secs  = 3.0
maker_tick_step   = 0.01

[feeds]
cl_stale_max_secs = 2.0
book_stale_max_secs = 0    # zero tolerance — must be live WS
binance_enabled   = true
binance_assets    = ["btcusdt", "ethusdt", "solusdt"]

[configs.a]
name           = "5M_SNIPER"
tf             = 5
min_edge       = 0.04
max_secs_left  = 270
min_secs       = 20
stop_loss      = true
sl_threshold   = 0.50
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
tf             = 0
min_edge       = 0.0
max_secs_left  = 25
min_secs       = 3
stop_loss      = true
sl_threshold   = 0.50
take_profit    = false
continuity     = 0
min_book_price = 0.95
max_entry_price = 0.975
```

---

*Build Sniper — 10 March 2026*
*5 Engines | $5 Stake | $50 Max DD | Universal 50% SL | Maker-First All Engines*
*Fee: `px × (1-px) × 0.0625` (actual Polymarket) | Maker = 0%*
