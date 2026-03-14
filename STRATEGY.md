# Weather Bot Strategy — v4

## The 3 Plays

### Play 1: END-OF-DAY LOCK (highest conviction) → HOLD TO WIN

The temperature is already recorded. Weather Underground shows today's high.
The winning bucket is KNOWN or nearly known. But the market hasn't settled yet
(UMA oracle takes 2+ hours). So the winning bucket might still be at 60-80¢
instead of $1.00.

**How it works:**
- Late afternoon / evening: fetch ACTUAL high from Weather Underground
- The high is already set — it's an observation, not a forecast
- Find the matching bucket. If it's trading below 90¢, buy YES.
- HOLD TO SETTLEMENT. Collect the spread to $1.00.

**Why this is the best play:**
- Zero forecast risk. The temperature already happened.
- The only risk is: WU revises the data (rare), or we misread the bucket.
- This is basically free money limited only by liquidity and timing.

**When:** Last 4-6 hours of the day (after the daily high is likely set).
Earlier if the weather pattern is simple (clear day, high hit by 2pm).

**Edge:** 5-40¢ per dollar depending on how fast the market converges.

**Exit:** HOLD TO SETTLEMENT. The outcome is already determined. No reason
to sell early — every cent below $1.00 is profit we'd leave on the table.

---

### Play 2: FORECAST SHIFT SCALP (main active trading) → FAST SCALP

GFS model updates 4x/day. When it shifts, market prices lag 10-30 min.
Buy the shift, sell as soon as the market reprices. Fast in, fast out.

**How it works:**
- Store previous ensemble probabilities
- When new GFS data drops (every ~6h), compute new probabilities
- Buy buckets that GAINED probability where market price is stale
- Sell AS SOON AS market price moves toward our probability
- NEVER hold to settlement. Scalp the reprice only.

**Fresh run detection:**
We can't directly see which GFS init time Open-Meteo is serving. But we CAN:
1. Compare ensemble member values between fetches. If 5+ members changed by
   1°F+, a new run dropped.
2. Track the GFS schedule (data avail ~03:30, 09:30, 15:30, 21:30 UTC).
   Fetch more frequently during these windows.
3. Cross-reference: if our ensemble shifted AND our WU/NOAA point forecast
   also moved, it's a real model update, not API noise.

**Sell FAST:**
- Don't wait for full convergence. If we bought at 15¢ and it's now 18¢,
  sell. That's a 20% return in minutes. Take it.
- Target: 3-5¢ profit per share, exit immediately.
- Place limit sell at entry + 3-5¢ right after buy fills.
- Hard timeout: exit at market after 30 min regardless of P&L.
  If the market didn't move in 30 min, the shift wasn't tradeable.
- NEVER hold past the next model run (6h max). Stale shift = no edge.

**First boot:** NO shift trades until second ensemble fetch. Need history.

---

### Play 3: NO GRIND (safe income, always on)

Buy NO on dead buckets (extreme tails). Ensemble says <5% probability,
NO price is 85¢+. Collect 5-15¢ per dollar when bucket resolves to 0.

This doesn't require any edge or timing. Dead buckets stay dead.
Win rate ~95%. Small profit per trade but extremely consistent.

**Sell:** Never. Hold to settlement. These almost always win.

---

## What We Do NOT Do

- **Buy YES on random buckets hoping they hit.** A 20% bucket loses 80% of the time.
- **Hold shift trades to settlement.** The edge is in the reprice, not the outcome.
- **Trade without a shift signal or end-of-day observation.** No signal = no trade.
- **Buy YES above 50¢.** The $2M loss trader bought at 51-67¢. We don't.
- **Trade on first boot.** Wait for second ensemble fetch to have shift data.

---

## Model Calibration — SKIP

Decided against ongoing model calibration. Reasoning:

- **Play 1:** Uses actual observations, not forecasts. Calibration irrelevant.
- **Play 2:** The edge is SPEED (trading the shift delta), not ACCURACY.
  When GFS shifts 3°F, the market reprices regardless of which model is
  "more accurate." We sell in minutes. We don't need to be right about
  the final temperature.
- **Play 3:** Dead buckets are dead. A 2°F model bias doesn't matter.

Research backs this: profitable bots (suislanchez, solship, gopfan2) use
simple threshold rules, not calibrated models. The $2M loss trader probably
had the most sophisticated model.

**The one thing that matters (one-time, not ongoing):**
Verify that Open-Meteo ensemble coordinates match the WU settlement station.
If ensemble is for central Manhattan but PM settles on LaGuardia airport,
there could be a systematic 1-2°F offset. Check once, adjust lat/lon if
needed, done. Already handled — CITIES config uses airport coordinates.

---

## Priority Order

1. **End-of-day lock** — highest conviction, implement first
2. **NO grind** — always running, no timing dependency
3. **Forecast shift scalp** — most complex, implement after 1 & 2 prove out

---

## Decisions Made

- **Hold rules:** Play 1 (end-of-day lock) → hold to settlement.
  Play 2 (shift scalp) → sell fast, 30 min hard timeout.
  Play 3 (NO grind) → hold to settlement.
- **Model calibration:** Skip. Edge is speed not accuracy.
- **First boot:** NO shift trades. Only NO grind and end-of-day lock.

## Open Questions

1. **WU API key:** Do we have one? Without it, Play 1 needs an alternative
   for actual observations. NOAA hourly actuals are close but may not
   match WU exactly (different station, different rounding).

2. **Bucket matching precision:** WU shows "high: 72°F" — is the winning
   bucket "72-73°F" or "71-72°F"? Need to verify PM's rounding rules.
   Market rules say "whole degrees" from the specific WU station page.

3. **End-of-day liquidity:** When the winning bucket is obvious, will there
   be asks left below 95¢? If the book is empty, Play 1 doesn't work.
   Need to observe actual book depth near settlement time.

4. **Sell mechanics for scalps:** Place limit sell at entry+3-5¢ immediately
   after buy confirms? Or watch the book and sell into bids? Need to test
   CLOB latency for round-trip speed.
