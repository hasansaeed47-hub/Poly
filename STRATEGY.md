# Weather Bot Strategy — v5 (verified data feeds)

## Data Feeds

All free, no API keys required (except WU which is optional):

| Feed | Endpoint | What | Used For |
|------|----------|------|----------|
| **METAR** | `aviationweather.gov/api/data/metar?ids=KLGA&format=json` | Airport observations (°C, hourly) | Play 1: observed high tracking |
| **Open-Meteo** | `api.open-meteo.com/v1/forecast` | Point forecast (worldwide) | Fallback forecast |
| **GFS Ensemble** | `ensemble-api.open-meteo.com/v1/ensemble` | 31-member ensemble (member00-30) | Probability engine |
| **NOAA** | `api.weather.gov/gridpoints/{office}/{grid}/forecast` | US forecast | Backup forecast |
| **WU** (optional) | `api.weather.com/v2/pws/observations/current` | Settlement source obs | Higher confidence obs |
| **Gamma API** | `gamma-api.polymarket.com/events` | Market discovery | Find temperature markets |
| **CLOB** | `clob.polymarket.com/midpoints`, `/book` | Prices + orderbook | Live pricing |
| **Data API** | `data-api.polymarket.com/trades`, `/v1/leaderboard` | Trades + whales | Play 4 whale tracking |

### Key API details (verified)

- **GFS ensemble fields**: `temperature_2m_max_member00` through `_member30` (zero-indexed, zero-padded)
- **Gamma clobTokenIds/outcomePrices**: returned as JSON **strings**, must `json.loads()` to parse
- **Trade fields**: `asset` = token ID, `size` = shares (not USD), `side` = `"BUY"`/`"SELL"` (uppercase), `timestamp` = unix epoch
- **Leaderboard fields**: `proxyWallet`, `pnl`, `userName` — category `WEATHER` confirmed
- **METAR fields**: `temp` in °C, `obsTime` = unix epoch, `maxT` exists but usually null (must track running max ourselves)

---

## The 4 Plays

### Play 1: END-OF-DAY LOCK → HOLD TO WIN

**What:** After the daily high is set, buy YES on the winning bucket.

**Data source:** METAR airport observation (free, same stations WU uses).
We fetch current temp hourly and track the **running maximum** — that's
today's observed high. WU observations used if WU_API_KEY is set.

**Time gate:** Only activates after 20:00 UTC for US cities (~4pm ET),
after 10:00 UTC for Asian cities (~7pm local). By this time the daily
high is almost always locked in.

**Single bucket:** Only buys the ONE bucket where the observed high falls
most centrally. Previous version bought all adjacent buckets — guaranteed
loss on all but one.

**Edge:** 5-40¢ per dollar. Zero forecast risk. The temp already happened.

**Exit:** HOLD TO SETTLEMENT.

---

### Play 2: FORECAST SHIFT SCALP → FAST SCALP

**What:** GFS updates 4x/day. Buy buckets that gained probability. Sell fast.

**Shift detection:** Compare ensemble members between fetches. A real model
update changes 5+ of 31 members by 1°F+. If members are identical, it's
the same GFS run and we DON'T trade (prevents false signals from API noise).

**Kelly sizing:** Uses AVAILABLE capital (MAX_DEPLOYED minus deployed), not
the fixed MAX_DEPLOYED. Prevents over-allocation as positions accumulate.

**Exit:** 4¢ take profit, 30 min timeout, or 8%+ probability reversal.

---

### Play 3: NO GRIND → HOLD TO WIN

**What:** Buy NO on dead buckets. Ensemble says <5% probability, NO price >85¢.

**No timing dependency.** Always valid.

**Exit:** HOLD TO SETTLEMENT.

---

### Play 4: WHALE FLOW → FAST SCALP

**What:** Copy the top 50 weather traders from Polymarket's weather leaderboard.

**Trade filtering (fixed):**
- Timestamp filter: only follow trades from the last hour (unix epoch comparison)
- `asset` field confirmed = token ID (matches `bucket.token_yes`)
- `size` field = shares, so USD = `size * price`

**Exit:** Same as Play 2 — fast scalp.

---

## Fixes in v5 (from v4 weakness analysis)

1. **Play 1 time gate** — only fires after 20 UTC (US) / 10 UTC (Asia)
2. **Play 1 uses observations** — METAR + WU running max, not forecast high
3. **Play 1 single bucket** — picks the one best bucket, not all adjacent
4. **Ensemble member00 fix** — was looking for wrong key name, now `member{i:02d}`
5. **Whale timestamp filter** — cutoff variable actually used now
6. **Kelly uses available capital** — `MAX_DEPLOYED - deployed` not `MAX_DEPLOYED`
7. **Position persistence** — saves to `bot_state.json` every tick, restores on restart
8. **Event caching** — Gamma API called every 15 min, not every 60s
9. **Settlement PnL fix** — uses `profit = -pos.cost` consistently for losses
10. **Shift detection** — verifies ensemble members actually changed (new GFS run)
11. **City coordinates** — updated to actual airport coords, not city centers
12. **METAR data feed** — free alternative to WU for airport observations

---

## Running

```bash
# Paper mode (default)
python simple.py

# Live mode (requires POLY_API_KEY)
POLY_API_KEY=xxx POLY_API_SECRET=xxx POLY_API_PASSPHRASE=xxx python simple.py --live

# With Weather Underground (optional, improves Play 1)
WU_API_KEY=xxx python simple.py
```

---

## Open Questions

1. **METAR vs WU discrepancy:** METAR observations are every ~1 hour. WU may
   update more frequently. The daily high from METAR should be within 1°F of WU
   but edge cases exist (temp spike between METAR reports).

2. **Bucket rounding:** WU shows "high: 72°F" — is the winning bucket "72-73°F"
   or "71-72°F"? PM market rules say "whole degrees" from the station page.

3. **Liquidity at EOD:** When the winning bucket is obvious, asks may dry up
   above 90¢. Play 1 won't fire if no asks below 90¢.
