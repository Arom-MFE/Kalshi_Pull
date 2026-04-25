# Kalshi Data Pipeline (Quick Reference)

## Core Data Structure
Series → Event → Market

- **Series** = recurring template (e.g., CPI, Recession)
- **Event** = one occurrence (e.g., CPI April 2026)
- **Market** = one tradable binary contract (YES/NO)

---
## Two Entry Cases

### 1. If you KNOW the market ticker
Example: `KXRECSSNBER-26`

Pipeline:
1. **get_market**
   - validate ticker
   - get `event_ticker`

2. **get_event**
   - get `series_ticker`

3. **get_market_candlesticks**
   - fetch price history (daily = `period_interval=1440`)

4. **check historical cutoff**
   - if old → use historical endpoint

---

### 2. If you DO NOT know the ticker

Pipeline:
1. **get_series / get_series_list**
2. **get_events**
3. **get_markets**
4. **get_market (optional inspect)**
5. **get_market_candlesticks**

---

## Key Commands (by purpose)

### Discovery
- `get_series` → find category
- `get_events` → find occurrences
- `get_markets` → find contracts

### Inspection
- `get_market` → single contract details
- `get_event` → event + hierarchy

### Data (what you actually use)
- `get_market_candlesticks` → OHLC data
- `get_market_orderbook` → bid depth
- `get_trades` → trade tape

### Historical
- `get_historical_cutoff`
- `/historical/markets/...`

---

## Daily Data Pull (Target Flow)

1. identify **market_ticker**
2. call **get_market**
3. extract **event_ticker**
4. call **get_event**
5. extract **series_ticker**
6. check **historical cutoff**
7. call:
   - live → `get_market_candlesticks`
   - old → `/historical/.../candlesticks`
8. normalize output

---

## Key Differences

- **get_event**
  - container (context)
  - multiple markets

- **get_market**
  - tradable unit
  - price / volume / history

 Rule:
- discovery → event
- data → market

---

## Output Structure (typical dataset)

- date
- open
- high
- low
- close
- mean
- volume
- open_interest
- market_ticker
- event_ticker
- series_ticker

---

## Minimal Mental Model

known ticker → market → event → series → cutoff → candles