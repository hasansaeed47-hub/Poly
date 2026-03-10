# Build Sniper — 10 March 2026 Live Deployment

## Status: LIVE
## Base: cl-sniper-9mar (v8.0.0 — 96% WR, +$21.44 over 109 trades)

---

## 9th March Results (Baseline)

| Metric | Value |
|--------|-------|
| Trades | 109 |
| Wins | ~105 |
| Losses | 1 |
| Stop Losses | 2 |
| Win Rate | ~96.3% |
| Cumulative P&L | +$21.44 |
| Entry prices (9 Mar) | 0.85 – 0.985 |
| Entry range (10 Mar A-D) | **0.88 – 0.98** (raised from 0.85) |
| Entry range (10 Mar E) | 0.95 – 0.975 (unchanged) |
| SL example | BTC enter 0.85, bid dropped to 0.33 → SL at 0.425, loss -$3.09 |
| SL example | ETH enter 0.92, bid dropped to 0.27 → SL at 0.460, loss -$3.56 |

---

## Global Rules

| Rule | Value |
|------|-------|
| **Stake** | $5.00 per trade per engine |
| **Max Drawdown** | $50.00 cumulative across all engines — hard kill |
| **Stop Loss** | ALL engines: bid ≤ 50% of entry price → immediate taker exit |
| **Window Skip Post-SL** | SL fires → that engine skips remainder of that window slug. No re-entry |
| **Force Close** | Every position must close via SL, settlement, or force-close at end_ts + 3s |
| **Fee Model** | Polymarket actual: `fee(px) = px × (1 - px) × 0.0625` — maker = 0% |

---

## Fee Schedule (Actual Polymarket)

```
fee(price) = price × (1 - price) × 0.0625
```

| Entry Price | Fee per share | Fee % | Engines |
|-------------|---------------|-------|---------|
| 0.88 | $0.0066 | 0.66% | A-D low end |
| 0.90 | $0.0056 | 0.56% | A-D typical |
| 0.95 | $0.0030 | 0.30% | A-D/E high end |
| 0.975 | $0.0015 | 0.15% | E max entry |
| **Maker** | **$0.00** | **0%** | **All engines attempt maker first** |

---

## Data Feeds — Zero Tolerance

All feeds continuous from window open to close. No staleness grace. No REST fallback during scanning.

| Feed | Source | Requirement |
|------|--------|-------------|
| **CL Oracle** | `wss://ws-live-data.polymarket.com` (RTDS) | Continuous. Subscribe `crypto_prices_chainlink`. CL snapshots stored per-asset with timestamps for open price capture and settlement |
| **Binance Spot** | `wss://stream.binance.com:9443/ws` | `btcusdt@aggTrade`, `ethusdt@aggTrade`, `solusdt@aggTrade`, `xrpusdt@aggTrade`. 1h rolling history for regime detection + 15s trend for BN contra filter |
| **CLOB Book** | `https://clob.polymarket.com/books` (REST) | Batch refresh every 400ms for active windows (last 120s of window) + held positions. No WS book — REST only per 9th March design |
| **Gamma API** | `https://gamma-api.polymarket.com/markets` | Discovery every 10s. Slug format: `{asset}-updown-{tf}m-{start_ts}` |

**CL open price capture:** First CL tick at or after `window_start`. Stored in `cl_opens` map. If no CL tick available when window starts, retry each scan tick until captured. No max delay tolerance — just capture when available.

**CL settlement:** `cl_at(asset, end_ts)` with ±1s tolerance. Fallback to latest CL price. Cross-checked against CLOB book (bid > 0.80 on winning side).

---

## Entry Window — Per 9th March Sniper

**The sniper does NOT trade the entire window.** It only enters in the final seconds when CL lag creates directional certainty.

```
ENTRY_START  = 57 seconds left    (earliest entry)
TAKER_DEADLINE = 44 seconds left  (taker fallback cutoff / latest entry)

5m window (300s):  entry from 243s to 256s after open  (13-second window)
15m window (900s): entry from 843s to 856s after open  (13-second window)
```

**Why 57s to 44s:** By this point CL has been updating for 4+ minutes. If CL has moved enough to push book price to 0.85+, the move is real and confirmed. The 13-second entry window is tight enough that mean reversion hasn't started.

**Book price range (A-D):** Entry only if `0.88 ≤ best_ask ≤ 0.98`. Raised from 0.85 — the two SL events on 9th March both entered at 0.85. Raising the floor to 0.88 requires higher market conviction before entry, filtering out the weakest signals that produced the worst losses.

**Book price range (E):** `0.95 ≤ best_ask ≤ 0.975`. Unchanged.

---

## Delta — How It's Actually Checked

Delta is **raw CL percentage change from window open**, NOT a Black-Scholes fair value edge.

```
cl_open  = CL price at window_start (captured and stored)
cl_now   = latest CL price
delta    = (cl_now - cl_open) / cl_open × 100.0    (percentage)
direction = if delta > 0 → "UP" else → "DOWN"
```

**Scaled by asset volatility:**
```
STDEV: BTC=0.167, ETH=0.194, SOL=0.247, XRP=0.440
STDEV_BASE = 0.167 (BTC)

scaled_threshold = engine.delta × (stdev(asset) / STDEV_BASE)

Example — Engine A (delta=0.04) on ETH:
  threshold = 0.04 × (0.194 / 0.167) = 0.04 × 1.16 = 0.0465%

Example — Engine D (delta=0.15) on SOL:
  threshold = 0.15 × (0.247 / 0.167) = 0.15 × 1.48 = 0.222%
```

**If `|delta| < scaled_threshold` → reset continuity counter, skip.**

---

## Continuity — How It's Tracked

```
Per tracker, per market slug:
  delta_ticks: HashMap<String, u32>   // slug → consecutive qualifying tick count

On each scan tick (500ms):
  1. Compute delta = (cl_now - cl_open) / cl_open × 100%
  2. Scale threshold by asset stdev
  3. If |delta| < threshold:
       → delta_ticks.remove(slug)     // HARD RESET
       → skip
  4. If continuity > 0:
       → delta_ticks[slug] += 1
       → if ticks < continuity: skip  // not enough sustained ticks yet
  5. If continuity == 0:
       → enter immediately (no counter needed)
  6. On entry:
       → delta_ticks.remove(slug)     // cleanup after fill
```

**9th March used continuity=3 (1.5s). 10th March uses continuity=4 (2.0s).** One extra tick of confirmation. Cost: 0.5s more latency. Benefit: filters one more class of transient CL noise.

---

## Order Approach — All Engines (Maker 2s → Taker)

Per 9th March sniper logic, adapted with 2s maker chase:

```
Signal qualifies (delta + continuity + filters pass):

Phase 1: MAKER (ticks 1-4, up to 2.0 seconds)
  maker_price = round_down(best_ask - 0.01, 2 decimals)
  If maker_price >= best_ask → fill at maker_price (already crossing)
  Else: post maker, wait for fill
  Track elapsed ticks via delta_ticks counter

Phase 2: TAKER FALLBACK (at secs_left ≤ 45 OR after 4+ ticks unfilled)
  fill_price = best_ask + 0.005 (SLIP)
  If fill_price > 0.98 (MAX_ENTRY) → abort, no entry
  If fill_price < 0.88 (MIN_ENTRY, A-D) or < 0.95 (E) → abort

Phase 3: ENTRY
  shares = $5.00 / fill_price
  sl_price = fill_price × 0.50
  Record trade, mark slug as done for this engine
```

**Maker at ask - 0.01, NOT bid + 0.01.** This is the 9th March approach: maker just below the ask, hoping to get filled by someone selling into our limit. More aggressive than mid-book posting.

**2 seconds (4 ticks):** Enough for 4 maker attempts. Short enough to not lose the 13s entry window. With continuity=4 engines: 2s continuity + 2s maker = 4s total before worst-case taker. Still within the 13s window.

---

## Additional Filters (from 9th March)

### BN Contra (Binance trend validation)
```
bn_trend = BN price change over last 15 seconds

If direction = "UP" and bn_trend < -0.02%: SKIP
   (BN is falling — CL lag may be closing, not opening)
If direction = "DOWN" and bn_trend > +0.02%: SKIP
   (BN is rising — same logic)
```
**Applied to:** D1 engines (B, D) — the "1" variants from 9th March had this. Engines A/C keep it per "keep filters."

### CL Fade (CL momentum check)
```
cl_trend = CL price change over last 10 seconds

If direction = "UP" and cl_trend < -0.03%: SKIP
   (CL itself is reversing — delta is stale)
If direction = "DOWN" and cl_trend > +0.03%: SKIP
```
**Applied to:** D1 engines (B, D). A/C keep it per "keep filters."

### Regime Check (hourly volatility gate)
```
hour_range = (1h high - 1h low) / 1h low × 100%

If hour_range < 0.3%: SKIP (market is chopping, CL lag is noise)
```
**Applied to:** D1 engines (B, D). Optional on A/C.

---

## Stop Loss — All Engines

```
Every 500ms tick while position is open:
  bk = book_state(held_token_id)

  if bk.has_bids AND bk.best_bid ≤ sl_price (= entry × 0.50):
    recovery = shares × (bk.best_bid - 0.005).max(0.0)
    pnl = recovery - $5.00
    → Close position, log SL
    → Mark slug as done (skip rest of window)
```

**No time cutoff.** SL fires any time during the hold (from entry through settlement wait).

**SL from 9th March data:**
- BTC: entered 0.85, SL at 0.425, bid hit 0.33, recovery = $1.91, loss = -$3.09
- ETH: entered 0.92, SL at 0.46, bid hit 0.27, recovery = $1.44, loss = -$3.56

---

## Flash Crash Management

**Layer 1: SL every 500ms tick** — standard check on book refresh.

**Layer 2: Book refresh frequency** — CLOB REST polls every 400ms for active positions (`tick_count` based). Faster than the 9th March default during active windows.

**Layer 3: Max DD kill switch** — cumulative P&L ≤ -$50 → halt all engines, force-close.

**What this does NOT protect against:**
- Book gaps between 400ms REST polls — binary books can jump from 0.80 to 0.10 in one REST cycle
- All engines on same asset: up to 5 × $5.08 = $25.40 correlated loss
- REST latency spikes (2s timeout) create blind spots where SL can't fire

---

## Engine Master Table

| | **Engine A** | **Engine B** | **Engine C** | **Engine D** | **Engine E** |
|---|---|---|---|---|---|
| **Name** | 5M_SNIPER | 5M_D1 | 15M_SNIPER | 15M_D1 | LATE_SCALPER |
| **Timeframe** | 5m | 5m | 15m | 15m | 5m + 15m |
| **Role** | Test | Control (D1 clone) | Test | Control (D1 clone) | Opportunistic |
| | | | | | |
| **SIGNAL** | | | | | |
| Delta (base) | **0.04%** | 0.15% | **0.04%** | 0.15% | N/A |
| Delta (BTC actual) | 0.04% | 0.15% | 0.04% | 0.15% | N/A |
| Delta (ETH actual) | 0.046% | 0.174% | 0.046% | 0.174% | N/A |
| Delta (SOL actual) | 0.059% | 0.222% | 0.059% | 0.222% | N/A |
| Continuity | **4 ticks (2.0s)** | 0 (instant) | **4 ticks (2.0s)** | 0 (instant) | 0 (instant) |
| BN Contra | Yes (keep filters) | Yes (D1 has it) | Yes (keep filters) | Yes (D1 has it) | No |
| CL Fade | Yes (keep filters) | Yes (D1 has it) | Yes (keep filters) | Yes (D1 has it) | No |
| Regime Check | Yes (keep filters) | Yes (D1 has it) | Yes (keep filters) | Yes (D1 has it) | No |
| | | | | | |
| **ENTRY** | | | | | |
| Entry Window | **57s – 44s left** | **57s – 44s left** | **57s – 44s left** | **57s – 44s left** | **≤ 25s left** |
| Entry Window Width | 13 seconds | 13 seconds | 13 seconds | 13 seconds | 25 seconds |
| Min Book Price | **0.88** | **0.88** | **0.88** | **0.88** | 0.95 |
| Max Book Price | 0.98 | 0.98 | 0.98 | 0.98 | 0.975 |
| Order Method | Maker 2s → taker | Maker 2s → taker | Maker 2s → taker | Maker 2s → taker | Maker 2s → taker |
| Maker Price | ask - 0.01 | ask - 0.01 | ask - 0.01 | ask - 0.01 | ask - 0.01 (chase to 0.975) |
| Taker Slippage | +0.005 | +0.005 | +0.005 | +0.005 | +0.005 |
| Typical Entry Fee (taker) | 0.3 – 0.66% | 0.3 – 0.66% | 0.3 – 0.66% | 0.3 – 0.66% | 0.15 – 0.30% |
| | | | | | |
| **EXIT** | | | | | |
| Stop Loss | Bid ≤ 50% entry | Bid ≤ 50% entry | Bid ≤ 50% entry | Bid ≤ 50% entry | Bid ≤ 50% entry |
| SL Example | Enter 0.90 → SL bid ≤ 0.45 | Same | Same | Same | Enter 0.96 → SL bid ≤ 0.48 |
| SL Max Loss | ~$2.85 (at 0.88 entry) | Same | Same | Same | ~$2.58 (at 0.96 entry) |
| Take Profit | No — hold to settle | No — hold to settle | No — hold to settle | No — hold to settle | No — hold to settle |
| Settlement | end_ts + 3s, binary | end_ts + 3s, binary | end_ts + 3s, binary | end_ts + 3s, binary | end_ts + 3s, binary |
| Max Hold Time | ~57 seconds | ~57 seconds | ~57 seconds | ~57 seconds | ~25 seconds |
| Window Skip Post-SL | Yes | Yes | Yes | Yes | Yes |
| | | | | | |
| **RISK** | | | | | |
| Max loss (SL fires) | ~$2.85 (at 0.88 floor) | ~$2.85 | ~$2.85 | ~$2.85 | ~$2.58 |
| Max loss (settle at 0) | -$5.00 | -$5.00 | -$5.00 | -$5.00 | -$5.00 |
| Stdev scaling | Yes | Yes | Yes | Yes | No |
| BS Model | **No** | **No** | **No** | **No** | **No** |

---

## Engine E — `LATE_SCALPER`

```
When secs_left ≤ 25 (any window, 5m or 15m):
  1. Check which side has best_ask ≥ 0.95
  2. That side = market's confirmed winner
  3. Maker at ask - 0.01, chase every tick up to 0.975
  4. Taker fallback at 2s if unfilled (ask + 0.005)
  5. Reject if fill > 0.975
  6. Hold to settlement
  7. SL: bid ≤ 50% of entry → exit
```

**Break-even at 0.96 entry (taker):** Win pays +$0.21, loss pays -$5.00. Need 96.0% WR to break even. Market prices at 96%. Engine E bets the last 25s add 1-2% certainty beyond what market prices.

---

## Timing Budget (Worst Case)

| Engine | Continuity | Maker Chase | Total Latency | Remaining Entry Window |
|--------|-----------|-------------|---------------|----------------------|
| A (5m Sniper) | 2.0s | 2.0s | **4.0s** | 9s of 13s window |
| B (5m D1) | 0s | 2.0s | **2.0s** | 11s of 13s window |
| C (15m Sniper) | 2.0s | 2.0s | **4.0s** | 9s of 13s window |
| D (15m D1) | 0s | 2.0s | **2.0s** | 11s of 13s window |
| E (Late Scalp) | 0s | 2.0s | **2.0s** | 23s of 25s window |

**A/C use 4s of their 13s entry window on confirmation + maker.** Still have 9s for fill. Acceptable.

---

## Strengths

| # | Strength | Detail |
|---|----------|--------|
| 1 | **Based on proven live system** | 9th March sniper ran 109 trades at 96% WR, +$21.44. Not a theoretical model — this logic made money |
| 2 | **No BS model dependency** | Raw CL delta is observable and verifiable. No sigma estimation noise, no d1/d2 bugs, no model risk. Delta is just `(cl_now - open) / open` |
| 3 | **13-second entry window is intentionally tight** | Enters only when outcome is near-certain (book at 0.85-0.98). The v1 scanner tried to predict outcomes early in the window — and lost 100%. Late-entry is the proven approach |
| 4 | **Stdev scaling adapts delta per asset** | SOL needs 1.48x higher delta than BTC because SOL is 1.48x more volatile. Prevents garbage entries on high-vol assets while capturing real moves on BTC |
| 5 | **BN contra catches closing lag** | If Binance shows the price reversing while CL hasn't updated, the "delta" is stale. BN contra filter skips these. This was a key blind spot identified previously — now it's wired into entry logic |
| 6 | **CL fade catches CL reversions** | If CL itself started reverting in the last 10s, the delta that qualified the entry may be closing. CL fade filter skips this |
| 7 | **Regime filter skips chop** | When 1h range < 0.3%, CL delta is noise, not signal. Regime filter prevents trading in dead markets |
| 8 | **Maker at ask-0.01 is realistic** | 9th March used this approach. $0.01 below ask means getting filled by incoming market sells. More aggressive and higher fill rate than posting at bid |

---

## Weaknesses

| # | Weakness | Severity | Detail |
|---|----------|----------|--------|
| 1 | **Delta 0.04% is 3.75x lower than D1's 0.15%** | **High** | D1 at 0.15% produced 96% WR. Dropping to 0.04% captures much weaker signals. On BTC (stdev=0.167), 0.04% = $27 move on $67k BTC. That's noise-level. Continuity helps, but 4 ticks of 0.04% noise is still noise |
| 2 | **50% SL is wide but fires too late** | **Medium** | 9th March SL data: BTC entered 0.85, bid hit 0.33 (61% drop). ETH entered 0.92, bid hit 0.27 (71% drop). Book gaps through SL level. **Raising MIN_ENTRY to 0.88 eliminates the BTC 0.85 entry that triggered the worst SL.** ETH at 0.92 would still occur. SL at 50% of 0.88 = 0.44 — marginally tighter than 0.425 |
| 3 | **REST-only book (no WS) means 400ms blind spots** | **Medium** | 9th March used REST polling, not WS. Between polls, book can move. SL checks only happen on poll refresh. For Engine E at 0.95+, a 400ms gap is significant — book can crash to 0.50 between polls |
| 4 | **Engine A/C continuity=4 eats 2s of a 13s window** | **Medium** | 4s total (2s cont + 2s maker) of a 13s window = 31% of the entry time consumed by confirmation. If the edge is real but brief, A/C miss it while B/D catch it |
| 5 | **Engine E has no delta filter at all** | **Medium** | Only checks book price ≥ 0.95. A thin 0.95 ask with CL barely above open is far riskier than 0.95 with CL 0.5% above open. No CL validation on E |
| 6 | **All 5 engines on same 4 assets** | **High** | Correlated loss on a bad window hits all engines. Flash crash on BTC at settlement = 5 × $5 = $25 gone |
| 7 | **No maker fill tracking** | **Low** | 9th March used probabilistic fill simulation (FILL_PROB=0.60). Live needs actual order management. Current paper approach just assumes fill after 2+ ticks |

---

## What's Being Overlooked

| # | Blind Spot | Impact |
|---|------------|--------|
| 1 | **0.04% delta on high-stdev assets (SOL, XRP) scales to 0.059% and 0.106%** | After stdev scaling, SOL threshold is 0.059% (~$0.049 on $83 SOL) and XRP is 0.106%. SOL is still very low. XRP is reasonable. The stdev scaling helps but doesn't fully protect against noise on low-cap assets |
| 2 | **13s entry window + 4s latency = 9s effective window for A/C** | If the qualifying delta only appears at 50s left (7s into the 13s window), A/C have 2s continuity then need to fill within 1s before TAKER_DEADLINE. Tight. B/D have the full 13s |
| 3 | **Settlement uses cl_at(asset, end_ts) ±1s with latest fallback** | If CL hasn't updated in 10s near settlement, the "close" price is stale. Settlement may not reflect the actual end-of-window price. This affected 0% of 9th March trades but is a known race |
| 4 | **CL open price capture has no max delay** | 9th March just takes the first available CL tick after window_start. If CL doesn't update for 30s, the open is 30s stale. All subsequent delta calculations are biased |
| 5 | **Engine E and Engines A-D can both enter same market** | E enters at ≤25s left. A-D enter at 57-44s left. Different windows — no overlap on same slug. But E on a 5m window and A on the same 5m window IS possible if E triggers after A's position settles. Cross-engine position limit per asset doesn't exist |
| 6 | **9th March win rate may not replicate** | 96% WR over 109 trades in ~12 hours during a specific BTC volatility regime. Different regime (chop, extreme vol, CL feed delays) could produce very different results. 109 trades is a small sample for 96% confidence |
| 7 | **No handling for book with no bids** | If CLOB returns empty bids, `bk.hb = false`, SL check skips (`if bk.hb && bk.bb <= sl_px`). Position held to settlement even if book has collapsed. May be correct (hold to settle) or may be a $5 loss that could have been $2.50 |
| 8 | **XRP added but not in 10 March Engine E** | 9th March had XRP. Engine E checks both 5m and 15m but doesn't specify XRP inclusion. If XRP is included, its 0.440 stdev means the 0.04% base delta scales to 0.106% — actually reasonable. But XRP books are thinner |
| 9 | **Maker at ask-0.01 may be below MIN_ENTRY** | If ask is 0.88 (new MIN_ENTRY), maker at 0.87 is below the range. The code handles this (`if mk >= bk.ba → fill at mk`) but maker fill at 0.87 would be below 0.88 floor. Should clamp maker price to `max(ask-0.01, MIN_ENTRY)` |
| 10 | **D1 used delta 0.15 and still had 1 loss + 2 SLs in 109 trades** | Even with the higher threshold, 3/109 = 2.8% adverse rate. At delta 0.04, the adverse rate will likely be higher. **Raising MIN_ENTRY to 0.88 helps** — both SL entries were at 0.85 (BTC) and 0.92 (ETH). The 0.85 entry would now be filtered. But the 0.92 entry still occurs |

---

## Scenario Analysis

| Scenario | A (5m, δ=0.04, cont=4) | B (5m D1, δ=0.15) | C (15m, δ=0.04, cont=4) | D (15m D1, δ=0.15) | E (≤25s, ≥0.95) |
|----------|---|---|---|---|---|
| **BTC trends 0.2%, book at 0.93** | ✅ Enters after 2s confirm + 2s maker. Delta 0.2% >> 0.04% threshold | ✅ Enters after 2s maker. Delta 0.2% > 0.15% | ✅ Same as A but 15m window | ✅ Same as B | ❌ Book < 0.95, skips |
| **BTC trends 0.08%, book at 0.88** | ✅ Enters — 0.08% > 0.04%, book 0.88 = MIN_ENTRY | ❌ Skips — 0.08% < 0.15% | ✅ Enters | ❌ Skips | ❌ Book < 0.95 |
| **BTC trends 0.05%, book at 0.86** | ❌ Skips — book 0.86 < 0.88 MIN_ENTRY | ❌ Skips | ❌ Skips — book below range | ❌ Skips | ❌ Book < 0.95 |
| **BTC barely moves 0.03%** | ❌ Skips — 0.03% < 0.04% | ❌ Skips | ❌ Skips | ❌ Skips | ❌ |
| **BTC trends but BN reverses** | ❌ BN contra blocks | ❌ BN contra blocks | ❌ Blocked | ❌ Blocked | ✅ No BN filter on E |
| **CL fading (reverting last 10s)** | ❌ CL fade blocks | ❌ CL fade blocks | ❌ Blocked | ❌ Blocked | ✅ No CL fade on E |
| **Chop market (1h range < 0.3%)** | ❌ Regime blocks | ❌ Regime blocks | ❌ Blocked | ❌ Blocked | ✅ No regime on E |
| **Book at 0.96, 20s left** | ❌ Outside 57-44s window | ❌ Outside window | ❌ Outside window | ❌ Outside window | ✅ Enters |
| **Flash crash at settlement** | ❌ Full loss $5 | ❌ Full loss $5 | ❌ Full loss $5 | ❌ Full loss $5 | ❌ Full loss $5 |
| **Transient 1-tick CL spike** | ❌ Continuity kills it | ✅/❌ Enters on spike → risky | ❌ Continuity kills | ✅/❌ Enters on spike | Depends on book |

**Key tension:** A/C with delta=0.04% will trigger on moves B/D ignore. If those moves are real → A/C captures alpha B/D misses. If those moves are noise → A/C enters bad trades B/D correctly skips. The continuity filter is the only defense at 0.04%.

---

## Config (Reference)

```toml
[global]
stake             = 5.0
max_drawdown      = 50.0
sl_share_pct      = 0.50
min_entry         = 0.88      # raised from 0.85 — 9Mar SLs both entered at 0.85
max_entry         = 0.98
entry_start       = 57        # seconds left — earliest entry
taker_deadline    = 44        # seconds left — taker fallback
slip              = 0.005
maker_chase_secs  = 2.0       # reduced from 3.0
assets            = ["btc", "eth", "sol", "xrp"]

[stdev]
btc  = 0.167
eth  = 0.194
sol  = 0.247
xrp  = 0.440
base = 0.167

[engines.a]
id         = "A"
delta      = 0.04
continuity = 4
bn_contra  = true
cl_fade    = true
regime     = true
wmin       = 5

[engines.b]
id         = "B"
delta      = 0.15
continuity = 0
bn_contra  = true
cl_fade    = true
regime     = true
wmin       = 5

[engines.c]
id         = "C"
delta      = 0.04
continuity = 4
bn_contra  = true
cl_fade    = true
regime     = true
wmin       = 15

[engines.d]
id         = "D"
delta      = 0.15
continuity = 0
bn_contra  = true
cl_fade    = true
regime     = true
wmin       = 15

[engines.e]
id              = "E"
delta           = 0.0
continuity      = 0
bn_contra       = false
cl_fade         = false
regime          = false
wmin            = 0           # both 5m and 15m
min_book_price  = 0.95
max_entry_price = 0.975
entry_start     = 25          # override — last 25s only
taker_deadline  = 3           # need at least 3s
```

---

*Build Sniper — 10 March 2026*
*5 Engines | $5 Stake | $50 Max DD | 50% Bid SL | Maker 2s → Taker*
*Based on cl-sniper-9mar v8.0.0 (96% WR, +$21.44 / 109 trades)*
*Entry: last 57-44s of window | Delta: raw CL % change, stdev-scaled | No BS model*
*A-D book range: 0.88-0.98 (raised from 0.85) | E book range: 0.95-0.975*
