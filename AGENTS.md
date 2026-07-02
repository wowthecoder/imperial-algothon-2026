# IMCity Trading Bot MCP Server - Agent Guide

This document provides instructions for AI agents on how to use the local MCP server to interact with the IMCity Challenge APIs. By connecting to this MCP server, you can access live market data and real-world inputs to inform your algorithmic trading strategies.

## Available Tools

The MCP server exposes the following 4 tools. All data returned is structured as JSON.

### 1. `get_orderbook(product: str)`
Fetches the current live orderbook (bids and asks) for a given product on the CMI exchange.

*   **Arguments**:
    *   `product` (string): The symbol of the product to query. Valid options include: `TIDE_SPOT`, `TIDE_SWING`, `WX_SPOT`, `WX_SUM`, `LHR_COUNT`, `LHR_INDEX`, `LON_ETF`, `LON_FLY`.
*   **Use Case**: Use this to check the current market spread, mid-price, and available volume before deciding to place a trade. For example, if you calculate the fair value of `LON_ETF` to be 6500, check the orderbook to see if you can buy it cheaper.

### 2. `get_weather(past_steps: int, forecast_steps: int)`
Fetches 15-minute resolution weather data (historical and forecast) for London using the Open-Meteo API.

*   **Arguments**:
    *   `past_steps` (integer, default 96): Number of past 15-minute intervals to retrieve. 96 = 24 hours.
    *   `forecast_steps` (integer, default 96): Number of future 15-minute intervals to retrieve.
*   **Use Case**: Critical for pricing `WX_SPOT` and `WX_SUM`. Remember that `WX_SPOT` settles at `temperature_F × humidity_%` at 12pm London time, and `WX_SUM` aggregates these values.

### 3. `get_thames_tides(limit: int)`
Fetches recent tidal levels at the Westminster gauge from the EA Flood Monitoring API.

*   **Arguments**:
    *   `limit` (integer, default 200): Number of recent 15-minute readings to fetch. Max useful limit is generally around 400 (~4 days).
*   **Use Case**: Required for pricing `TIDE_SPOT` and `TIDE_SWING`. The readings are in metres Above Ordnance Datum (mAOD). Remember `TIDE_SPOT` settles on the absolute value in **mm AOD** (multiply by 1000).

### 4. `get_flights(offset_minutes: int, duration_minutes: int, filters: dict)`
Fetches arrivals and departures data for London Heathrow (LHR) via the AeroDataBox API.

*   **Arguments**:
    *   `offset_minutes` (integer, default -360): The start of the query window relative to now (negative for past). Max 12 hours total window size.
    *   `duration_minutes` (integer, default 720): The length of the window in minutes (max 720).
    *   `filters` (dict, optional): Boolean filters for the API.
*   **Use Case**: Essential for pricing `LHR_COUNT` (total flights) and `LHR_INDEX` (imbalance). **Important:** RapidAPI free tier is strictly rate-limited (~150 requests/month). Use this tool sparingly and cache data when possible.

## Strategy Guidance for Agents

When building and iterating on strategies, keep the following workflows in mind:

1.  **Calculate Fair Value (The "Theo")**:
    *   Call `get_weather()`, `get_thames_tides()`, or `get_flights()` to gather the raw underlying data.
    *   Apply the settlement formulas specific to the product (e.g., converting Celsius to Fahrenheit for weather, or summing strangle payoffs for `TIDE_SWING`).
    *   This gives you the theoretical fair value (theo) of the product.

2.  **Evaluate the Market**:
    *   Call `get_orderbook("PRODUCT_NAME")`.
    *   Compare the best bid and best ask against your calculated fair value.

3.  **Identify Opportunities**:
    *   **Directional/Alpha**: If your calculated fair value is significantly higher than the best ask, it implies the market is underpricing the asset (opportunity to buy).
    *   **Arbitrage**: Because `LON_ETF = TIDE_SPOT + WX_SPOT + LHR_COUNT`, you can fetch the orderbooks for all 4 products. If the ETF's price deviates from the sum of the underlying components, there is a risk-free arbitrage opportunity.

# Other tools
The .alpha_but_state_Stack_Overslept.json contains the state of the alpha bot and some past market data. 