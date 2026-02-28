# IMCity Algothon Challenge — Strategy Plan

## Challenge Summary

You're trading **8 products** on a custom exchange (CMI), settling based on real-world London data over a 24-hour window (Saturday 12pm → Sunday 12pm). Products are grouped for scoring, and PnL is normalized per group. Position limits are ±100 per product. API rate limit: 1 request/second.

---

## The 8 Markets at a Glance

| # | Symbol | Settles To | Data Source | Type |
|---|--------|-----------|-------------|------|
| 1 | **TIDE_SPOT** | `abs(water_level_mAOD) × 1000` at 12pm Sunday | EA Flood Monitoring | Spot |
| 2 | **TIDE_SWING** | Sum of strangle payoffs on 15-min tidal diffs | EA Flood Monitoring | Accumulator |
| 3 | **WX_SPOT** | `temp_F × humidity_%` at 12pm Sunday | Open-Meteo | Spot |
| 4 | **WX_SUM** | `Σ(temp_F × humidity_%) / 100` over all 15-min intervals | Open-Meteo | Accumulator |
| 5 | **LHR_COUNT** | Total flights (arrivals + departures) in 24h | AeroDataBox | Accumulator |
| 6 | **LHR_INDEX** | `abs(Σ 100×(arr-dep)/max(arr+dep,1))` per 30-min bin | AeroDataBox | Accumulator |
| 7 | **LON_ETF** | `TIDE_SPOT + WX_SPOT + LHR_COUNT` | Derived | Composite |
| 8 | **LON_FLY** | Options structure: `2×P(6200) + C(6200) - 2×C(6600) + 3×C(7000)` on ETF | Derived | Options |

### Scoring Groups
- **Group A** (4× weight): Markets 1, 3, 5, 7 (the spots + ETF)
- **Group B**: Market 2 (TIDE_SWING)
- **Group C**: Market 4 (WX_SUM)
- **Group D**: Market 6 (LHR_INDEX)
- **Group E**: Market 8 (LON_FLY)

---

## Strategic Framework: Three Pillars

### Pillar 1: Alpha Signal (Directional Trading via Fair Value Estimation)

This is your **biggest edge**. The settlement values are deterministic functions of real-world data that you can observe and forecast. Most competitors will not have good models. Focus on computing accurate **theoretical fair values (theo)** for each product and trading when the market price diverges from your theo.

#### Market 1 — TIDE_SPOT
**Algorithm: Tidal Harmonic Analysis**

Thames tides are extremely predictable — they're driven by astronomical forces (lunar/solar cycles). Use the **Python `utide` library** or `pytides` to perform harmonic constituent analysis on historical EA data:

1. **Fetch 7+ days of historical readings** from `environment.data.gov.uk` (the API is free, no key needed).
2. **Fit harmonic constituents** (M2, S2, N2, K1, O1, etc.) using least-squares. The dominant constituents for the Thames at Westminster are well-known.
3. **Predict the water level at Sunday 12:00pm** by reconstructing the tidal signal.
4. Settlement = `abs(predicted_level) × 1000`.

**Expected accuracy:** Tidal predictions are typically accurate to within a few cm for short-term forecasts. Your theo should be very close to settlement. This gives a strong alpha signal.

**Refinement:** As more 15-min readings arrive during the session, update your forecast. The real-time residual (observation minus harmonic prediction) can be modelled with a simple AR(1) or exponential smoothing to capture weather-driven surge.

#### Market 2 — TIDE_SWING
**Algorithm: Monte Carlo Simulation on Tidal Model**

This is the **strangle payoff** on 15-min absolute differences with strikes at 20cm and 25cm:
- Payoff per interval = `max(0, 20 - diff_cm) + max(0, diff_cm - 25)`

Strategy:
1. Use your harmonic tidal model to predict the full 24h tidal curve at 15-min resolution.
2. Compute the expected absolute differences between consecutive readings.
3. Sum the strangle payoffs → this is your theo.
4. **Key insight:** Most 15-min changes in the Thames are either very small (during slack water) or moderate (during tide change). The strangle pays when diffs are < 20cm (put leg) or > 25cm (call leg). The "dead zone" is 20–25cm. Most intervals will be in the put-payoff region (small changes), so the settlement will be dominated by `Σ max(0, 20 - diff_cm)`.
5. To refine, add a volatility term for the uncertainty around each predicted diff (from residual analysis), and compute the expected payoff as `E[max(0, 20 - X) + max(0, X - 25)]` for X ~ N(μ_predicted, σ_residual).

#### Market 3 — WX_SPOT
**Algorithm: Weather Forecast Consumption**

Open-Meteo provides **free 15-minute forecasts**. This is straightforward:

1. Query the Open-Meteo API for the forecast at Sunday 12:00pm London time.
2. Convert temperature to Fahrenheit: `temp_F = temp_C × 9/5 + 32` (round as specified).
3. Multiply by humidity.
4. **Track forecast evolution:** Weather forecasts improve as the target time approaches. Re-fetch every 15–30 minutes and update your theo.

**Edge:** The forecast converges. As Saturday evening arrives, your 12-hour forecast for Sunday noon will be quite accurate. If the market hasn't adjusted, trade into it.

#### Market 4 — WX_SUM
**Algorithm: Running Sum + Forecast Extrapolation**

1. **Observed portion:** As data arrives during the session, compute the running sum of `(temp_F × humidity) / 100` for all observed intervals.
2. **Forecast portion:** For remaining unobserved intervals, use the Open-Meteo forecast to project the remaining sum.
3. **Theo = observed_sum + projected_remaining_sum.**
4. As the session progresses, the observed portion grows and uncertainty shrinks — your theo gets more accurate over time.

#### Market 5 — LHR_COUNT
**Algorithm: Historical Average + Schedule Estimation**

Heathrow flight counts over 24 hours are relatively stable for the same day-of-week:

1. **Historical baseline:** Weekend Saturday→Sunday counts at LHR are typically in the range of ~800–1200 flights. Research typical Saturday/Sunday schedules.
2. **Live tracking:** During the session, count observed flights and extrapolate.
3. **AeroDataBox API is rate-limited (150 req/month free)** — be strategic. Pre-fetch historical baselines before the competition, and during the session only poll every 1–2 hours.
4. As time passes, your linear extrapolation from observed counts becomes more accurate.

#### Market 6 — LHR_INDEX
**Algorithm: Running Metric + Historical Pattern**

The imbalance metric `Σ 100×(arr-dep)/max(arr+dep,1)` per 30-min bin, settled as absolute value:

1. This tends to oscillate around zero (arrivals ≈ departures on average), so the absolute value of the running sum stays relatively small.
2. Track the running metric as data comes in.
3. Forecast remaining bins using historical arrival/departure patterns for the time of day.

#### Market 7 — LON_ETF
**Algorithm: Composite Fair Value**

`ETF = TIDE_SPOT + WX_SPOT + LHR_COUNT`

Your ETF theo is simply the sum of your individual theos for Markets 1, 3, and 5. This is in **Group A** (4× weight), so getting this right is very valuable.

**Key cross-product arbitrage opportunity:** If the ETF market price diverges from the sum of its components' market prices, you can arbitrage. Buy the cheap side, sell the expensive side.

#### Market 8 — LON_FLY
**Algorithm: Options Pricing via ETF Distribution**

The options structure `2×P(6200) + C(6200) - 2×C(6600) + 3×C(7000)` depends on the ETF settlement value.

1. Estimate the **probability distribution** of the ETF settlement from your model uncertainties.
2. Compute the expected payoff: `E[2×max(0, 6200-S) + max(0, S-6200) - 2×max(0, S-6600) + 3×max(0, S-7000)]`
3. If your ETF estimate is, say, S = 6500 with tight confidence intervals, you can compute this directly.
4. As your ETF estimate sharpens, the options theo narrows in range.

**Insight on the payoff shape:**
- Below 6200: Dominated by the 2× put → payoff = 2×(6200-S) + 0 - 0 + 0 = 2×(6200-S)
- At 6200: Payoff = 0
- Between 6200–6600: `(S-6200)` rising linearly
- At 6600: Payoff = 400 (peak of the butterfly portion)
- Between 6600–7000: Payoff = (S-6200) - 2(S-6600) = 11000 - S, declining
- Above 7000: +3(S-7000) adds positive convexity → payoff rises again

---

### Pillar 2: Market Making (Spread Capture)

Use a modified **Avellaneda-Stoikov** style market making strategy:

#### Core Algorithm
```
reservation_price = theo - gamma × position × sigma²
bid = reservation_price - spread/2
ask = reservation_price + spread/2
```

Where:
- `theo` = your fair value estimate from Pillar 1
- `gamma` = inventory risk aversion parameter (tune this)
- `position` = your current net position in the product
- `sigma²` = estimated variance of the product price
- `spread` = optimal spread (wider when uncertain, tighter when confident)

#### Implementation Details

1. **Asymmetric quoting based on inventory:** When you're long, lower your ask (to sell) and raise your bid (to discourage buying more). Vice versa when short.
2. **Skew quotes toward your alpha signal:** If your theo is above the market mid, bias your quotes upward (place ask higher, keep bid closer to mid) to accumulate a long position.
3. **Respect the ±100 position limit:** Stop quoting on one side as you approach the limit.
4. **Smart requoting:** Only cancel and replace orders when the mid or your theo has moved enough to justify it. This preserves **queue priority** (price-time priority matching).
5. **Width adjustment:** Quote wider on markets where your theo has high uncertainty (early in the session), and tighter as your theo converges (later in the session).

#### Practical Tips
- The simple quoter template gives you a starting point — enhance it with your theo.
- Don't blindly follow the market mid. Your edge is that you know approximate fair value from data.
- Use the SSE stream for real-time orderbook updates to react quickly.

---

### Pillar 3: Cross-Product Arbitrage

Several products are structurally linked. Exploit mispricings between them.

#### Arbitrage 1: ETF vs. Components
`LON_ETF = TIDE_SPOT + WX_SPOT + LHR_COUNT`

If `ETF_market_price > TIDE_SPOT_ask + WX_SPOT_ask + LHR_COUNT_ask`:
→ **Sell ETF, Buy components** (or vice versa)

Monitor this spread continuously. Even small dislocations can be captured.

#### Arbitrage 2: LON_FLY vs. LON_ETF
The options payoff is a deterministic function of the ETF settlement. If the ETF is trading at a known level and the FLY is mispriced relative to the implied payoff, trade into it.

#### Arbitrage 3: Spot vs. Accumulator Consistency
TIDE_SPOT and TIDE_SWING are both driven by the same tidal data. If your tidal model is good, both theos are consistent. If one market is pricing in a different scenario than the other, trade the mispriced one.

---

## Recommended Bot Architecture

```
┌─────────────────────────────────────────────┐
│                  MAIN LOOP                   │
│                                              │
│  ┌──────────┐   ┌──────────┐   ┌─────────┐  │
│  │  Data     │   │  Fair    │   │ Trading │  │
│  │  Fetcher  │──▶│  Value   │──▶│ Engine  │  │
│  │           │   │  Model   │   │         │  │
│  └──────────┘   └──────────┘   └─────────┘  │
│       │              │              │        │
│       ▼              ▼              ▼        │
│  ┌──────────┐   ┌──────────┐   ┌─────────┐  │
│  │  Weather  │   │  Theo    │   │ Order   │  │
│  │  Tides    │   │  Store   │   │ Manager │  │
│  │  Flights  │   │  (per    │   │ (quote, │  │
│  │  APIs     │   │  product)│   │ cancel, │  │
│  └──────────┘   └──────────┘   │ arb)    │  │
│                                 └─────────┘  │
│                                              │
│  ┌──────────────────────────────────────┐    │
│  │  Risk Manager                        │    │
│  │  - Position limits (±100)            │    │
│  │  - PnL tracking                      │    │
│  │  - Quote width adjustment            │    │
│  └──────────────────────────────────────┘    │
└─────────────────────────────────────────────┘
```

### Key Components

1. **DataFetcher** — Periodically (every 15–30 min) fetch real-world data from APIs.
2. **FairValueModel** — Compute theo for each product based on latest data + forecasts.
3. **TradingEngine** — Two modes:
   - **Quoter:** Market-make around theo (Avellaneda-Stoikov style)
   - **Hitter:** Aggressively take liquidity when market price diverges significantly from theo
4. **RiskManager** — Enforce position limits, track PnL, adjust aggressiveness.

---

## Prioritization & Time Allocation

Given that Group A has **4× weight**, prioritize these markets:

| Priority | Market | Why | Effort |
|----------|--------|-----|--------|
| 🥇 1 | TIDE_SPOT (1) | Highly predictable via harmonic analysis, 4× weight | High — build tidal model |
| 🥇 2 | WX_SPOT (3) | Free forecast API, 4× weight | Medium — consume forecast |
| 🥇 3 | LHR_COUNT (5) | Relatively stable, 4× weight | Medium — historical + extrapolation |
| 🥇 4 | LON_ETF (7) | Composite of above three, 4× weight | Low — sum of component theos |
| 🥈 5 | LON_FLY (8) | Options payoff from ETF, standalone group | Medium — expected value calc |
| 🥈 6 | TIDE_SWING (2) | Uses same tidal model | Medium — simulate strangle |
| 🥉 7 | WX_SUM (4) | Running sum, straightforward | Low — running accumulator |
| 🥉 8 | LHR_INDEX (6) | Noisy, hard to predict | Low — basic tracking |

---

## Key Libraries & Tools

| Library | Purpose |
|---------|---------|
| `utide` (Python) | Tidal harmonic analysis and prediction |
| `pytides` | Simpler alternative for tidal forecasting |
| `requests` | API calls (EA, Open-Meteo, AeroDataBox) |
| `numpy` / `pandas` | Data manipulation and time series |
| `scipy.stats` | Distribution fitting for options pricing |
| `sseclient-py` | SSE stream for real-time exchange updates |

---

## Implementation Roadmap

### Phase 1: Data Infrastructure (2–3 hours)
- [ ] Build data fetchers for all three sources (tides, weather, flights)
- [ ] Fetch historical data and explore patterns
- [ ] Set up tidal harmonic analysis with `utide` or `pytides`

### Phase 2: Fair Value Models (2–3 hours)
- [ ] TIDE_SPOT: Harmonic prediction for settlement time
- [ ] WX_SPOT: Weather forecast consumption + C→F conversion
- [ ] LHR_COUNT: Historical average + live extrapolation
- [ ] Derived products: ETF sum, options expected value

### Phase 3: Trading Bot (2–3 hours)
- [ ] Extend `BaseBot` with theo-informed quoting
- [ ] Implement Avellaneda-Stoikov inventory management
- [ ] Add cross-product arbitrage detection (ETF vs components)
- [ ] Add aggressive hitting when market is far from theo

### Phase 4: Testing & Tuning (1–2 hours)
- [ ] Test on the test exchange
- [ ] Tune parameters: spread width, inventory aversion, requote threshold
- [ ] Stress test: what if data APIs go down? Fallback to last known theo.

---

## Risk Considerations

1. **Model risk:** Your tidal/weather model could be wrong. Don't bet everything on one theo — maintain reasonable spread widths.
2. **Position limits:** Hard cap at ±100. Monitor carefully and stop quoting one side before you hit the wall.
3. **API failures:** Have fallback logic if data sources go down. Cache the last known theo.
4. **Other participants:** In a competition, other teams' bots create the market. Expect thin liquidity and potentially adversarial order flow.
5. **Scoring system quirk:** Losses are normalized against other losers. It's better to be flat (0 PnL) than to have a loss in a group where others also lost — because your negative score scales with how much of the total losses you represent. **Avoid big losses in any single group.**
