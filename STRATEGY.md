# Weather Bot Strategy — v4 Draft

## The 3 Plays

### Play 1: END-OF-DAY LOCK (highest conviction)

The temperature is already recorded. Weather Underground shows today's high.
The winning bucket is KNOWN or nearly known. But the market hasn't settled yet
(UMA oracle takes 2+ hours). So the winning bucket might still be at 60-80¢
instead of $1.00.

**How it works:**
- Late afternoon / evening: fetch ACTUAL high from Weather Underground
- The high is already set — it's an observation, not a forecast
- Find the matching bucket. If it's trading below 90¢, buy YES.
- Wait for settlement. Collect the spread to $1.00.

**Why this is the best play:**
- Zero forecast risk. The temperature already happened.
- The only risk is: WU revises the data (rare), or we misread the bucket.
- This is basically free money limited only by liquidity and timing.

**When:** Last 4-6 hours of the day (after the daily high is likely set).
Earlier if the weather pattern is simple (clear day, high hit by 2pm).

**Edge:** 5-40¢ per dollar depending on how fast the market converges.

**Exit:** Hold to settlement. This is one case where holding IS correct
because the outcome is already determined.

---

### Play 2: FORECAST SHIFT SCALP (main active trading)

GFS model updates 4x/day. When it shifts, market prices lag 10-30 min.
Buy the shift, sell as soon as the market reprices. Fast in, fast out.

**How it works:**
- Store previous ensemble probabilities
- When new GFS data drops (every ~6h), compute new probabilities
- Buy buckets that GAINED probability where market price is stale
- Sell AS SOON AS market price approaches our new probability
- Don't hold. Don't wait for settlement. Scalp the reprice.

**Fresh run detection:**
We can't directly see which GFS init time Open-Meteo is serving. But we CAN:
1. Compare ensemble member values between fetches. If 5+ members changed by
   1°F+, a new run dropped.
2. Track the GFS schedule (data avail ~03:30, 09:30, 15:30, 21:30 UTC).
   Fetch more frequently during these windows.
3. Cross-reference: if our ensemble shifted AND our WU/NOAA point forecast
   also moved, it's a real model update, not API noise.

**Sell aggressively:**
- Don't wait for full convergence. If we bought at 15¢ and it's now 20¢,
  sell. That's a 33% return in minutes.
- Target: 3-5¢ profit per share, exit immediately.
- Never hold a shift trade past the next model run (6h max).
- If the market doesn't move within 30 min, the shift wasn't big enough
  to matter — exit at breakeven or small loss.

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

## Model Calibration (Play 2 improvement)

Over the first 2-3 days, track which forecast source (GFS ensemble, NOAA,
WU forecast) was closest to the actual WU settlement temperature.

Build a simple accuracy table:

```
City     | GFS mean error | NOAA error | WU forecast error | Best source
---------|---------------|------------|-------------------|------------
NYC      | 1.8°F         | 1.5°F      | 1.2°F             | WU
Seoul    | 2.5°F         | N/A        | 2.1°F             | WU
Chicago  | 2.0°F         | 1.7°F      | 1.3°F             | WU
```

Then weight the ensemble probabilities toward the more accurate source.
This is the "which model is closer to the settlement oracle" insight.

But honestly — for Play 1 (end-of-day), we don't need forecast accuracy
at all. We're using ACTUAL observations.

---

## Priority Order

1. **End-of-day lock** — highest conviction, implement first
2. **NO grind** — always running, no timing dependency
3. **Forecast shift scalp** — most complex, implement after 1 & 2 prove out

---

## Open Questions

1. WU API: do we have a key? Without it, Play 1 is harder (need to scrape
   or use NOAA hourly actuals as proxy, which may not match WU exactly).

2. Bucket matching: WU shows "high: 72°F" — but is the bucket "72-73°F"
   or "71-72°F"? Need to verify how PM rounds. The market rules say
   "whole degrees" from the specific WU station page.

3. Liquidity on winning bucket: if everyone knows the answer, will there
   be any asks left below 95¢? Need to check how fast books thin out
   near settlement. If the book is empty, Play 1 doesn't work.

4. Sell speed for Play 2: can we place limit sells immediately after buying,
   or do we need to wait for CLOB confirmation? Latency matters for scalps.
