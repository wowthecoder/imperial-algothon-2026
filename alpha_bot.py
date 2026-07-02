"""AlphaBot — data-driven trading bot for CMI Exchange.

Strategy (3 layers):
  1. Fair Value Engine: blends external data (weather/tides/flights) with live
     exchange orderbook mids. Live mids are incorporated as they arrive via SSE.
  2. LON_ETF Arbitrage: pure arb when LON_ETF != TIDE_SPOT + WX_SPOT + LHR_COUNT
  3. Data-driven Market Making: quotes bid/ask around blended FV, adaptive spread

Key design choices:
  - SSE orderbooks are cached → _check_etf_arb never calls get_orderbook()
  - Active order IDs tracked from responses → _requote never calls get_orders()
    Together these eliminate ~12 API calls/cycle and avoid the 1 req/s rate limit.
  - Flights fetched in two 12h windows to cover the full 24h session (≤3 API calls total)
  - Hit cooldown prevents duplicate fill orders when SSE fires repeatedly
  - Settlement proximity boost: confidence ramps to ~1.0 in final hours before noon
"""

from __future__ import annotations

import json
import math
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock, Thread
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv

from bot_template import BaseBot, OrderBook, OrderRequest, Side, Trade
from fair_value import (
    FairValueEstimate,
    compute_all_fair_values,
    fetch_flights_range,
    get_thames,
    get_weather,
    lon_fly_payoff,
    lon_fly_expected_payoff,
    lon_fly_expected_delta,
)

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GROUP_A_PRODUCTS: frozenset[str] = frozenset({
    "TIDE_SPOT", "WX_SPOT", "LHR_COUNT", "LON_ETF",
})

ETF_ARB_ENABLED = False  # All 4 legs are Group A — disabled

MAX_POSITION: dict[str, int] = {
    "TIDE_SPOT": 0,      # Group A — disabled
    "TIDE_SWING": 50,    # Group B — increased from 30
    "WX_SPOT":   0,      # Group A — disabled
    "WX_SUM":    50,     # Group C — increased from 30
    "LHR_COUNT": 0,      # Group A — disabled
    "LHR_INDEX": 50,     # Group D — increased from 30
    "LON_ETF":   0,      # Group A — disabled
    "LON_FLY":   12,     # Group E — reduced for safety (nonlinear payoff risk)
}

QUOTE_VOLUME = 5
# High-confidence volume boost: when conf > 0.7, quote up to this many lots
HIGH_CONF_QUOTE_VOLUME = 9
HIGH_CONF_THRESHOLD = 0.70

# Spread half-width (ticks): narrow at high confidence, wide at low
MIN_SPREAD_TICKS = 2.0
MAX_SPREAD_TICKS = 8.0

# Minimum FV move to trigger a requote (preserves queue priority)
REQUOTE_THRESHOLD_TICKS = 1.0

# Hit mispriced orders when they cross our FV by at least this many ticks
HIT_THRESHOLD_TICKS = 2.0

# Don't re-hit same product within this window (SSE fires on every book change)
HIT_COOLDOWN_SECS = 4.0  # reduced from 10.0 — capture mispricings faster
HIT_MIN_CONFIDENCE = 0.20
HIT_POS_LIMIT_FRAC = 0.6

# LON_ETF arb: minimum profit in ticks to trigger
ARB_TRIGGER_TICKS = 5.0

# Fishing/trap orders: extreme resting limits to catch undisciplined bots
TRAP_SELL_MULTIPLES = [1.6, 2.0, 3.0]   # sell at 160%, 200%, 300% of FV
TRAP_BUY_FRACTIONS  = [0.5, 0.2, 0.05]  # buy at 50%, 20%, 5% of FV
TRAP_VOLUME = 5                          # contracts per trap level
TRAP_REFRESH_INTERVAL_SECS = 300        # re-place every 5 minutes

DATA_REFRESH_INTERVAL_SECS = 60     # weather + tides
QUOTING_INTERVAL_SECS = 3           # reduced from 5 for higher throughput

# LON_FLY delta hedging: max LON_ETF units held as hedge (|delta| ≤ 2 × max_fly_pos = 24)
MAX_HEDGE_ETF_POS = 24

# LON_FLY arb: minimum seconds between arb attempts (arb is reactive via SSE)
FLY_ARB_COOLDOWN_SECS = 5.0

SESSION_HOURS = 24
INVENTORY_SOFT_LIMIT_FRAC = 0.8
SESSION_START_HOUR = 14
SESSION_START_MINUTE = 30
LONDON_TZ = ZoneInfo("Europe/London")
MID_FILTER_DEBUG = False
MID_FILTER_DEBUG_EVERY_N = 25
STATE_SAVE_INTERVAL_SECS = 10.0
STATE_FILE_TEMPLATE = ".alpha_bot_state_{username}.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_mid(ob: OrderBook) -> float | None:
    bids = [o.price for o in ob.buy_orders if o.volume - o.own_volume > 0]
    asks = [o.price for o in ob.sell_orders if o.volume - o.own_volume > 0]
    if bids and asks:
        return (max(bids) + min(asks)) / 2.0
    return None


def _book_level_counts(ob: OrderBook) -> tuple[int, int, int, int]:
    total_bids = len(ob.buy_orders)
    total_asks = len(ob.sell_orders)
    filtered_bids = sum(1 for o in ob.buy_orders if o.volume - o.own_volume > 0)
    filtered_asks = sum(1 for o in ob.sell_orders if o.volume - o.own_volume > 0)
    return total_bids, total_asks, filtered_bids, filtered_asks


def _best_bid(ob: OrderBook) -> float | None:
    bids = [o.price for o in ob.buy_orders if o.volume - o.own_volume > 0]
    return max(bids) if bids else None


def _best_ask(ob: OrderBook) -> float | None:
    asks = [o.price for o in ob.sell_orders if o.volume - o.own_volume > 0]
    return min(asks) if asks else None


def _blend(model: float | None, market: float | None, model_weight: float) -> float | None:
    """Weighted blend: model_weight to model, (1-model_weight) to market.
    Model weight capped at 0.9 — always give at least 10% to market.
    """
    if model is None and market is None:
        return None
    if model is None:
        return market
    if market is None:
        return model
    w = max(0.0, min(0.9, model_weight))   # floor: 10% to market always
    return w * model + (1.0 - w) * market


def _side_volume(pos: int, max_pos: int, side: Side, max_volume: int = QUOTE_VOLUME) -> int:
    """Inventory-aware size: reduce size when trading further into risk."""
    if max_pos <= 0:
        return 0

    if side == Side.BUY:
        remaining = max_pos - pos
        push_frac = max(0.0, pos / max_pos)
    else:
        remaining = max_pos + pos
        push_frac = max(0.0, -pos / max_pos)

    if remaining <= 0:
        return 0

    scale = max(0.2, 1.0 - push_frac)
    sized = max(1, math.ceil(max_volume * scale))
    return int(min(max_volume, sized, remaining))


def current_time(tzinfo) -> datetime:
    """Centralized wall-clock access for consistent time handling."""
    return datetime.now(tz=tzinfo)


def resolve_session_start(now_dt: datetime | None = None) -> datetime:
    """Derive the most recent London session start at HH:MM."""
    now_london = now_dt or current_time(LONDON_TZ)
    candidate = now_london.replace(
        hour=SESSION_START_HOUR,
        minute=SESSION_START_MINUTE,
        second=0,
        microsecond=0,
    )
    if now_london < candidate:
        candidate -= timedelta(days=1)
    return candidate


# ---------------------------------------------------------------------------
# AlphaBot
# ---------------------------------------------------------------------------

class AlphaBot(BaseBot):

    def __init__(
        self,
        cmi_url: str,
        username: str,
        password: str,
        aero_api_key: str,
        session_start: datetime,
    ):
        super().__init__(cmi_url, username, password)
        self._aero_api_key  = aero_api_key
        self._session_start = session_start
        self._session_end   = session_start + timedelta(hours=SESSION_HOURS)
        # Settlement is noon on the day following session_start (Saturday 14:30 → Sunday 12:00)
        self._settlement_dt = (
            session_start.replace(hour=12, minute=0, second=0, microsecond=0)
            + timedelta(days=1)
        )

        self._lock = Lock()

        # External model FV (slow updates)
        self._fv_estimate = FairValueEstimate()

        # Live market data from SSE (fast updates)
        self._market_mids: dict[str, float]   = {}
        self._orderbooks:  dict[str, OrderBook] = {}   # full book cached from SSE

        # Position + order tracking
        self._positions:     dict[str, int]        = {}
        self._active_orders: dict[str, list[str]]  = {}  # product → [order_id, ...]
        self._last_quoted_fv: dict[str, float]     = {}
        self._last_hit:       dict[str, float]     = {}  # product → monotonic of last hit

        # Cached external data
        self._weather_df: pd.DataFrame = pd.DataFrame()
        self._tidal_df:   pd.DataFrame = pd.DataFrame()
        # Two 12h flight windows covering the full 24h session
        self._flights_w1: dict | None = None   # session_start → session_start+12h
        self._flights_w2: dict | None = None   # session_start+12h → session_end

        self._products: dict[str, any] = {}
        self._mid_debug_counts: dict[str, int] = {}
        self._trap_orders: dict[str, list[str]] = {}   # product → [order_id, ...]
        self._last_trap_placed: float = 0.0            # monotonic time of last trap refresh
        self._last_fly_arb: float = 0.0                # cooldown for LON_FLY arb attempts
        safe_user = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in username)
        self._state_path = Path(STATE_FILE_TEMPLATE.format(username=safe_user))
        self._state_dirty = False
        self._last_state_save = time.monotonic()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def run(self):
        print("AlphaBot starting...")
        self._products = {p.symbol: p for p in self.get_products()}
        print(f"Products: {list(self._products.keys())}")
        self._load_state()

        self.start()   # SSE stream → on_orderbook / on_trades
        self._unwind_disabled_positions()

        Thread(target=self._data_loop, daemon=True).start()

        try:
            self._quoting_loop()
        except KeyboardInterrupt:
            print("\nShutting down...")
        finally:
            self._save_state(force=True)
            self.cancel_all_orders()
            self.stop()
            print("AlphaBot stopped.")

    def _unwind_disabled_positions(self):
        """Cancel orders and flatten positions for disabled Group A products."""
        print("[UNWIND] Checking Group A positions to flatten...")
        positions = self.get_positions()

        for sym in GROUP_A_PRODUCTS:
            orders = self.get_orders(product=sym)
            for order in orders:
                self.cancel_order(order["id"])
            if orders:
                print(f"[UNWIND] Cancelled {len(orders)} orders on {sym}")

            pos = positions.get(sym, 0)
            if pos == 0:
                continue

            try:
                ob = self.get_orderbook(sym)
                if pos > 0:
                    bid = _best_bid(ob)
                    if bid is not None:
                        self.send_order(OrderRequest(sym, bid, Side.SELL, abs(pos)))
                        print(f"[UNWIND] Flattening {sym}: SELL {abs(pos)}@{bid}")
                else:
                    ask = _best_ask(ob)
                    if ask is not None:
                        self.send_order(OrderRequest(sym, ask, Side.BUY, abs(pos)))
                        print(f"[UNWIND] Flattening {sym}: BUY {abs(pos)}@{ask}")
            except Exception as e:
                print(f"[UNWIND] Failed to flatten {sym}: {e}")

        print("[UNWIND] Done.")

    # ------------------------------------------------------------------
    # SSE callbacks
    # ------------------------------------------------------------------

    def on_orderbook(self, ob: OrderBook) -> None:
        """Cache orderbook + market mid; instantly hit obvious mispricings."""
        mid = _safe_mid(ob)
        total_bids, total_asks, filtered_bids, filtered_asks = _book_level_counts(ob)
        raw_best_bid = max((o.price for o in ob.buy_orders), default=None)
        raw_best_ask = min((o.price for o in ob.sell_orders), default=None)
        filt_best_bid = _best_bid(ob)
        filt_best_ask = _best_ask(ob)
        tick = 1.0
        pos = 0
        max_pos = MAX_POSITION.get(ob.product, 50)
        conf = 0.0
        debug_count = 0
        with self._lock:
            self._orderbooks[ob.product] = ob          # ← cache full book
            if mid is not None:
                self._market_mids[ob.product] = mid
                self._state_dirty = True
            p = self._products.get(ob.product)
            if p:
                tick = p.tickSize
            pos = self._positions.get(ob.product, 0)
            max_pos = MAX_POSITION.get(ob.product, 50)
            conf = self._fv_estimate.confidence.get(ob.product, 0.0)
            debug_count = self._mid_debug_counts.get(ob.product, 0) + 1
            self._mid_debug_counts[ob.product] = debug_count

        if MID_FILTER_DEBUG and (mid is None or debug_count % MID_FILTER_DEBUG_EVERY_N == 0):
            mid_str = "n/a" if mid is None else f"{mid:.1f}"
            raw_bid_str = "n/a" if raw_best_bid is None else f"{raw_best_bid:.1f}"
            raw_ask_str = "n/a" if raw_best_ask is None else f"{raw_best_ask:.1f}"
            filt_bid_str = "n/a" if filt_best_bid is None else f"{filt_best_bid:.1f}"
            filt_ask_str = "n/a" if filt_best_ask is None else f"{filt_best_ask:.1f}"
            print(
                f"[MID FILTER] {ob.product} #{debug_count} "
                f"levels(raw bid/ask={total_bids}/{total_asks}, filtered={filtered_bids}/{filtered_asks}) "
                f"top(raw={raw_bid_str}/{raw_ask_str}, filtered={filt_bid_str}/{filt_ask_str}) "
                f"mid={mid_str}"
            )

        live_fv = self._compute_live_fv()

        # React instantly to LON_FLY/LON_ETF book changes for arb detection
        if ob.product in ("LON_FLY", "LON_ETF"):
            self._check_fly_arb()

        fv = live_fv.get(ob.product)
        if fv is None:
            return

        now = time.monotonic()

        # Cooldown: don't hit same product repeatedly as SSE fires on every book change
        if now - self._last_hit.get(ob.product, 0) < HIT_COOLDOWN_SECS:
            return
        if conf < HIT_MIN_CONFIDENCE:
            return

        best_ask = _best_ask(ob)
        if best_ask is not None and fv - best_ask >= HIT_THRESHOLD_TICKS * tick:
            if pos < max_pos * HIT_POS_LIMIT_FRAC:
                vol = _side_volume(pos, max_pos, Side.BUY)
                if vol > 0:
                    result = self.send_order(OrderRequest(ob.product, best_ask, Side.BUY, vol))
                    if result:
                        self._last_hit[ob.product] = now
                        print(f"[HIT BUY]  {ob.product}: {vol}@{best_ask:.0f}  live_FV={fv:.1f}")

        best_bid = _best_bid(ob)
        if best_bid is not None and best_bid - fv >= HIT_THRESHOLD_TICKS * tick:
            if pos > -max_pos * HIT_POS_LIMIT_FRAC:
                vol = _side_volume(pos, max_pos, Side.SELL)
                if vol > 0:
                    result = self.send_order(OrderRequest(ob.product, best_bid, Side.SELL, vol))
                    if result:
                        self._last_hit[ob.product] = now
                        print(f"[HIT SELL] {ob.product}: {vol}@{best_bid:.0f}  live_FV={fv:.1f}")

    def on_trades(self, trade: Trade) -> None:
        if trade.buyer == self.username:
            side = "BOUGHT"
            sign = +1
        elif trade.seller == self.username:
            side = "SOLD"
            sign = -1
        else:
            return
        print(f"  FILL: {side} {trade.volume}x {trade.product} @ {trade.price:.1f}")
        with self._lock:
            self._positions[trade.product] = (
                self._positions.get(trade.product, 0) + sign * trade.volume
            )
            self._state_dirty = True

    # ------------------------------------------------------------------
    # Fair value: blend model + live market data
    # ------------------------------------------------------------------

    def _settlement_proximity(self) -> float:
        """0.0 early in session → 1.0 at settlement (noon Sunday).
        Ramps linearly over the final 6 hours before settlement.
        """
        now = current_time(self._settlement_dt.tzinfo)
        hours_left = (self._settlement_dt - now).total_seconds() / 3600
        if hours_left <= 0:
            return 1.0
        if hours_left >= 6:
            return 0.0
        return (6.0 - hours_left) / 6.0

    def _compute_live_fv(self) -> dict[str, float]:
        """Blend external model FV with live SSE market mids.

        Near settlement the model gets higher weight because:
        - More actual data has been observed (tides, weather, flights)
        - Forecast errors shrink to near zero
        """
        with self._lock:
            model = self._fv_estimate
            mids  = dict(self._market_mids)

        prox = self._settlement_proximity()
        # Extra model weight boost as we approach settlement (max +0.3)
        prox_boost = prox * 0.3

        live: dict[str, float] = {}

        # --- Base products ---
        for sym in ["TIDE_SPOT", "TIDE_SWING", "WX_SPOT", "WX_SUM", "LHR_COUNT", "LHR_INDEX"]:
            conf = min(0.9, model.confidence.get(sym, 0.5) + prox_boost)
            blended = _blend(model.fv.get(sym), mids.get(sym), model_weight=conf)
            if blended is not None:
                live[sym] = blended

        # --- LON_ETF (three signals; component sum is best because it's exact at settlement) ---
        comp_tide = live.get("TIDE_SPOT") or mids.get("TIDE_SPOT")
        comp_wx   = live.get("WX_SPOT")   or mids.get("WX_SPOT")
        comp_lhr  = live.get("LHR_COUNT") or mids.get("LHR_COUNT")

        etf_from_components = (
            comp_tide + comp_wx + comp_lhr
            if (comp_tide and comp_wx and comp_lhr) else None
        )
        etf_model  = model.fv.get("LON_ETF")
        etf_market = mids.get("LON_ETF")

        if etf_from_components and etf_market and etf_model:
            blended_etf = (0.5 * etf_from_components
                           + 0.3 * etf_market
                           + 0.2 * etf_model)
        elif etf_from_components and etf_market:
            blended_etf = 0.6 * etf_from_components + 0.4 * etf_market
        elif etf_from_components and etf_model:
            blended_etf = 0.6 * etf_from_components + 0.4 * etf_model
        elif etf_from_components:
            blended_etf = etf_from_components
        elif etf_market and etf_model:
            conf = min(0.9, model.confidence.get("LON_ETF", 0.3) + prox_boost)
            blended_etf = _blend(etf_model, etf_market, model_weight=conf)
        else:
            blended_etf = etf_market or etf_model

        if blended_etf is not None:
            live["LON_ETF"] = blended_etf
            # Use distribution-aware expected payoff for LON_FLY.
            # ETF std estimated from model confidence: low confidence → high uncertainty.
            etf_conf_raw = model.confidence.get("LON_ETF", 0.3)
            etf_std = max(50.0, blended_etf * 0.15 * (1.0 - etf_conf_raw))
            live["LON_FLY"] = lon_fly_expected_payoff(blended_etf, etf_std)

        return live

    # ------------------------------------------------------------------
    # External data loop
    # ------------------------------------------------------------------

    def _data_loop(self):
        while True:
            try:
                self._refresh_all_data()
            except Exception as e:
                print(f"[DATA ERROR] {e}")
            time.sleep(DATA_REFRESH_INTERVAL_SECS)

    def _refresh_all_data(self):
        # Weather + tides (free, unlimited — refresh every cycle)
        print("[DATA] Refreshing weather and tides...")
        try:
            weather_df = get_weather(past_steps=96, forecast_steps=96)
            tidal_df   = get_thames(limit=200)
        except Exception as e:
            print(f"[DATA] Weather/tides fetch failed: {e}")
            weather_df = self._weather_df
            tidal_df   = self._tidal_df

        # Flights: two 12h windows covering full 24h session.
        # Window 1 is fetched once (immediately). Window 2 is fetched once
        # after session midpoint (when it becomes historical). ~2 API calls total.
        self._refresh_flights_smart()
        flights_data = self._merged_flights()

        with self._lock:
            self._weather_df = weather_df
            self._tidal_df   = tidal_df

        try:
            fv_est = compute_all_fair_values(
                weather_df=weather_df,
                tidal_df=tidal_df,
                flights_data=flights_data,
                session_start=self._session_start,
            )
            with self._lock:
                self._fv_estimate = fv_est
                self._state_dirty = True
        except Exception as e:
            print(f"[DATA] Fair value computation failed: {e}")
            return

        live_fv = self._compute_live_fv()
        self._print_fv_table(fv_est, live_fv)

    def _refresh_flights_smart(self):
        """Fetch flights in two 12h windows. Each window fetched only once."""
        fmt = "%Y-%m-%dT%H:%M"
        session_mid = self._session_start + timedelta(hours=12)
        now = current_time(self._session_start.tzinfo)

        # Window 1: session_start → session_start+12h (fetch immediately, once)
        if self._flights_w1 is None:
            try:
                print("[DATA] Fetching flight window 1 (first 12h)...")
                raw_w1 = fetch_flights_range(
                    api_key=self._aero_api_key,
                    from_local=self._session_start.strftime(fmt),
                    to_local=session_mid.strftime(fmt),
                )
                self._flights_w1 = self._compact_flights_payload(raw_w1) or {
                    "arrivals": [], "departures": []
                }
                arr = len(self._flights_w1.get("arrivals", []))
                dep = len(self._flights_w1.get("departures", []))
                print(f"[DATA] Window 1: {arr} arr, {dep} dep")
                self._state_dirty = True
            except Exception as e:
                print(f"[DATA] Flight window 1 failed: {e}")

        # Window 2: session_start+12h → session_end (fetch once past midpoint)
        if self._flights_w2 is None and now >= session_mid:
            try:
                print("[DATA] Fetching flight window 2 (second 12h)...")
                raw_w2 = fetch_flights_range(
                    api_key=self._aero_api_key,
                    from_local=session_mid.strftime(fmt),
                    to_local=self._session_end.strftime(fmt),
                )
                self._flights_w2 = self._compact_flights_payload(raw_w2) or {
                    "arrivals": [], "departures": []
                }
                arr = len(self._flights_w2.get("arrivals", []))
                dep = len(self._flights_w2.get("departures", []))
                print(f"[DATA] Window 2: {arr} arr, {dep} dep")
                self._state_dirty = True
            except Exception as e:
                print(f"[DATA] Flight window 2 failed: {e}")

    def _extract_flight_time(self, flight: dict[str, Any], stem: str) -> str | None:
        """Extract timestamp string from multiple AeroDataBox payload shapes."""
        if stem not in {"scheduled", "revised", "actual"}:
            return None
        for section_key in ("movement", "departure", "arrival"):
            section = flight.get(section_key)
            if not isinstance(section, dict):
                continue
            # Shape A: movement.scheduledTime.local / .utc
            nested = section.get(f"{stem}Time")
            if isinstance(nested, dict):
                val = nested.get("local") or nested.get("utc")
                if isinstance(val, str) and val:
                    return val
            # Shape B: movement.scheduledTimeLocal
            flat = (
                section.get(f"{stem}TimeLocal")
                or section.get(f"{stem}TimeUTC")
                or section.get(f"{stem}TimeUtc")
            )
            if isinstance(flat, str) and flat:
                return flat
        return None

    def _compact_flight_entry(self, flight: dict[str, Any]) -> dict[str, Any] | None:
        """Keep only flight fields used by fair value logic."""
        if not isinstance(flight, dict):
            return None

        movement: dict[str, str] = {}
        for stem in ("scheduled", "revised", "actual"):
            t = self._extract_flight_time(flight, stem)
            if t is not None:
                movement[f"{stem}TimeLocal"] = t

        if not movement:
            return None

        # Drop entries that are clearly outside this session window.
        ref_t = (
            movement.get("revisedTimeLocal")
            or movement.get("actualTimeLocal")
            or movement.get("scheduledTimeLocal")
        )
        if ref_t is not None:
            try:
                ts = pd.to_datetime(ref_t).to_pydatetime()
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=self._session_start.tzinfo)
                if ts < self._session_start or ts > self._session_end:
                    return None
            except Exception:
                pass

        compact: dict[str, Any] = {"movement": movement}
        for k in ("number", "status", "codeshareStatus"):
            v = flight.get(k)
            if isinstance(v, str) and v:
                compact[k] = v
        return compact

    def _compact_flights_payload(self, flights: dict | None) -> dict | None:
        """Normalize and compact flights payload to avoid oversized state files."""
        if not isinstance(flights, dict):
            return None

        out: dict[str, list[dict[str, Any]]] = {"arrivals": [], "departures": []}
        for side in ("arrivals", "departures"):
            items = flights.get(side, [])
            if not isinstance(items, list):
                continue
            seen: set[tuple[str | None, str | None, str | None, str | None]] = set()
            compact_items: list[dict[str, Any]] = []
            for f in items:
                cf = self._compact_flight_entry(f)
                if cf is None:
                    continue
                m = cf.get("movement", {})
                dedupe_key = (
                    cf.get("number"),
                    m.get("scheduledTimeLocal"),
                    m.get("revisedTimeLocal"),
                    cf.get("status"),
                )
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                compact_items.append(cf)

            compact_items.sort(
                key=lambda x: (
                    (x.get("movement", {}) or {}).get("scheduledTimeLocal", ""),
                    x.get("number", ""),
                )
            )
            out[side] = compact_items

        if not out["arrivals"] and not out["departures"]:
            return None
        return out

    def _merged_flights(self) -> dict | None:
        """Combine both flight windows into one dict."""
        w1 = self._flights_w1 or {}
        w2 = self._flights_w2 or {}
        merged = {
            "arrivals":   w1.get("arrivals", [])   + w2.get("arrivals", []),
            "departures": w1.get("departures", []) + w2.get("departures", []),
        }
        return merged if (merged["arrivals"] or merged["departures"]) else None

    def _print_fv_table(self, model: FairValueEstimate, live_fv: dict[str, float]):
        prox = self._settlement_proximity()
        with self._lock:
            mids = dict(self._market_mids)
        print(f"\n[FV] settlement_proximity={prox:.2f}")
        print(f"{'Symbol':<12} {'ModelFV':>9} {'MarketMid':>10} {'LiveFV':>9} {'Conf':>6}")
        print("-" * 52)
        for sym in sorted(model.fv):
            mfv_s = f"{model.fv[sym]:9.1f}"      if sym in model.fv   else "      n/a"
            mid_s = f"{mids[sym]:10.1f}"          if sym in mids       else "       n/a"
            lfv_s = f"{live_fv[sym]:9.1f}"        if sym in live_fv    else "      n/a"
            conf  = model.confidence.get(sym, 0.0)
            print(f"{sym:<12} {mfv_s} {mid_s} {lfv_s} {conf:6.2f}")
        print()

    # ------------------------------------------------------------------
    # Quoting loop
    # ------------------------------------------------------------------

    def _quoting_loop(self):
        while True:
            try:
                # Sync positions from exchange (ground truth, one API call)
                live_positions = self.get_positions()
                with self._lock:
                    self._positions = live_positions
                    model = self._fv_estimate
                    self._state_dirty = True

                live_fv = self._compute_live_fv()

                for symbol, product in self._products.items():
                    self._requote_product(symbol, product, live_fv, model)

                self._check_etf_arb()
                self._check_fly_arb()
                self._hedge_lon_fly()

                now = time.monotonic()
                if now - self._last_trap_placed >= TRAP_REFRESH_INTERVAL_SECS:
                    self._refresh_traps(live_fv)
                    self._last_trap_placed = now

            except Exception as e:
                print(f"[QUOTE ERROR] {e}")

            self._save_state()
            time.sleep(QUOTING_INTERVAL_SECS)

    def _requote_product(
        self,
        symbol: str,
        product,
        live_fv: dict[str, float],
        model: FairValueEstimate,
    ):
        if symbol in GROUP_A_PRODUCTS:
            return

        fv = live_fv.get(symbol)
        if fv is None or fv <= 0:
            return

        tick = product.tickSize
        prox = self._settlement_proximity()

        # Spread: narrower with higher model confidence and near settlement
        model_fv = model.fv.get(symbol)
        conf = min(1.0, model.confidence.get(symbol, 0.5) + prox * 0.3)
        base_spread = MAX_SPREAD_TICKS - conf * (MAX_SPREAD_TICKS - MIN_SPREAD_TICKS)

        # Widen if model and live disagree (extra uncertainty), scaled by (1-conf)
        # so disagreement penalty vanishes as confidence rises
        if model_fv is not None:
            extra = min(abs(fv - model_fv) / tick * 0.5 * (1.0 - conf), 10.0)
        else:
            extra = 5.0 * (1.0 - conf)
        spread = base_spread + extra

        # Gamma-aware spread widening for LON_FLY near option strikes
        if symbol == "LON_FLY":
            etf_fv_for_gamma = live_fv.get("LON_ETF")
            if etf_fv_for_gamma is not None:
                spread += self._fly_gamma_spread_adjustment(etf_fv_for_gamma)

        # Skip requote if FV barely moved (preserve queue priority)
        last_fv = self._last_quoted_fv.get(symbol)
        if last_fv is not None and abs(fv - last_fv) < REQUOTE_THRESHOLD_TICKS * tick:
            return

        pos     = self._positions.get(symbol, 0)
        max_pos = MAX_POSITION.get(symbol, 50)

        # Inventory skew: shift quotes toward flattening by up to 2 ticks
        # (long position → lower both bid & ask to sell more aggressively, etc.)
        inventory_skew = (pos / max_pos) * 2.0 * tick if max_pos > 0 else 0.0

        bid = math.floor((fv - spread * tick - inventory_skew) / tick) * tick
        ask = math.ceil( (fv + spread * tick - inventory_skew) / tick) * tick

        if bid <= 0 or bid >= ask:
            return

        orders: list[OrderRequest] = []
        # Use higher volume when confidence is high
        q_vol = HIGH_CONF_QUOTE_VOLUME if conf >= HIGH_CONF_THRESHOLD else QUOTE_VOLUME
        buy_vol = _side_volume(pos, max_pos, Side.BUY, q_vol)
        sell_vol = _side_volume(pos, max_pos, Side.SELL, q_vol)
        if pos < max_pos * INVENTORY_SOFT_LIMIT_FRAC and buy_vol > 0:
            orders.append(OrderRequest(symbol, bid, Side.BUY, buy_vol))
        if pos > -max_pos * INVENTORY_SOFT_LIMIT_FRAC and sell_vol > 0:
            orders.append(OrderRequest(symbol, ask, Side.SELL, sell_vol))

        # LON_FLY deep inventory flattening: when |pos| > 50% of max, post an
        # aggressive flattening order near the market mid to speed up unwinding.
        if symbol == "LON_FLY" and max_pos > 0 and abs(pos) > max_pos * 0.5:
            with self._lock:
                ob = self._orderbooks.get(symbol)
            if ob is not None:
                if pos > 0:
                    # Long: post an extra sell 1 tick above current bid
                    best_b = _best_bid(ob)
                    if best_b is not None:
                        flatten_price = best_b + tick
                        flatten_vol = min(3, pos)
                        orders.append(OrderRequest(symbol, flatten_price, Side.SELL, flatten_vol))
                        print(f"  [FLATTEN] {symbol}: extra SELL {flatten_vol}@{flatten_price:.0f} (pos={pos})")
                else:
                    # Short: post an extra buy 1 tick below current ask
                    best_a = _best_ask(ob)
                    if best_a is not None:
                        flatten_price = best_a - tick
                        flatten_vol = min(3, -pos)
                        orders.append(OrderRequest(symbol, flatten_price, Side.BUY, flatten_vol))
                        print(f"  [FLATTEN] {symbol}: extra BUY {flatten_vol}@{flatten_price:.0f} (pos={pos})")

        if not orders:
            return

        # Cancel using tracked order IDs — no get_orders() API call needed
        old_ids = self._active_orders.get(symbol, [])
        if old_ids:
            threads = [Thread(target=self.cancel_order, args=(oid,)) for oid in old_ids]
            for t in threads: t.start()
            for t in threads: t.join()

        results = self.send_orders(orders)
        # Track new order IDs for next-cycle cancellation
        self._active_orders[symbol] = [r.id for r in results if r is not None]

        if results:
            sides_str = " / ".join(f"{o.volume}@{o.price:.0f} {o.side}" for o in orders)
            print(f"  [QUOTE] {symbol:<12} live_FV={fv:.1f}  spread=±{spread:.1f}  {sides_str}")
            self._last_quoted_fv[symbol] = fv
            self._state_dirty = True

    # ------------------------------------------------------------------
    # LON_FLY helpers: gamma spread, arb, delta hedging
    # ------------------------------------------------------------------

    def _fly_gamma_spread_adjustment(self, etf_fv: float) -> float:
        """Widen LON_FLY spread when ETF is near option strikes (high gamma zones)."""
        strikes = [6200, 6600, 7000]
        min_dist = min(abs(etf_fv - k) for k in strikes)
        # Within 100 of a strike → add up to 5 extra ticks of spread
        if min_dist < 100:
            return 5.0 * (1.0 - min_dist / 100.0)
        return 0.0

    def _check_fly_arb(self):
        """Arb LON_FLY against its theoretical value implied by LON_ETF mid."""
        now = time.monotonic()
        if now - self._last_fly_arb < FLY_ARB_COOLDOWN_SECS:
            return

        with self._lock:
            fly_ob = self._orderbooks.get("LON_FLY")
            etf_ob = self._orderbooks.get("LON_ETF")

        if fly_ob is None or etf_ob is None:
            return

        etf_mid = _safe_mid(etf_ob)
        if etf_mid is None:
            return

        theo_fly = lon_fly_payoff(etf_mid)
        fly_ask = _best_ask(fly_ob)
        fly_bid = _best_bid(fly_ob)

        pos = self._positions.get("LON_FLY", 0)
        max_pos = MAX_POSITION["LON_FLY"]

        if fly_ask is not None and theo_fly - fly_ask > 5:
            if pos < max_pos:
                vol = min(3, max_pos - pos)
                self.send_order(OrderRequest("LON_FLY", fly_ask, Side.BUY, vol))
                self._last_fly_arb = now
                print(f"[FLY ARB] BUY {vol}@{fly_ask} (theo={theo_fly:.0f}, edge={theo_fly-fly_ask:.0f})")

        if fly_bid is not None and fly_bid - theo_fly > 5:
            if pos > -max_pos:
                vol = min(3, max_pos + pos)
                self.send_order(OrderRequest("LON_FLY", fly_bid, Side.SELL, vol))
                self._last_fly_arb = now
                print(f"[FLY ARB] SELL {vol}@{fly_bid} (theo={theo_fly:.0f}, edge={fly_bid-theo_fly:.0f})")

    def _hedge_lon_fly(self):
        """Delta-hedge LON_FLY position using LON_ETF."""
        fly_pos = self._positions.get("LON_FLY", 0)
        if fly_pos == 0:
            return

        live_fv = self._compute_live_fv()
        etf_fv = live_fv.get("LON_ETF")
        if etf_fv is None:
            return

        etf_conf = self._fv_estimate.confidence.get("LON_ETF", 0.3)
        etf_std = max(50.0, etf_fv * 0.15 * (1.0 - etf_conf))
        delta = lon_fly_expected_delta(etf_fv, etf_std)

        fly_etf_delta = fly_pos * delta
        etf_pos = self._positions.get("LON_ETF", 0)

        # Desired hedge: offset the fly delta, capped to MAX_HEDGE_ETF_POS
        raw_target = -round(fly_etf_delta)
        target_etf_pos = max(-MAX_HEDGE_ETF_POS, min(MAX_HEDGE_ETF_POS, raw_target))
        hedge_needed = target_etf_pos - etf_pos

        if abs(hedge_needed) < 1:
            return

        with self._lock:
            ob = self._orderbooks.get("LON_ETF")
        if ob is None:
            return

        if hedge_needed > 0:
            ask = _best_ask(ob)
            if ask:
                self.send_order(OrderRequest("LON_ETF", ask, Side.BUY, abs(hedge_needed)))
                print(f"[HEDGE] BUY {abs(hedge_needed)} LON_ETF @{ask} (fly delta={delta:.2f}, fly_pos={fly_pos})")
        else:
            bid = _best_bid(ob)
            if bid:
                self.send_order(OrderRequest("LON_ETF", bid, Side.SELL, abs(hedge_needed)))
                print(f"[HEDGE] SELL {abs(hedge_needed)} LON_ETF @{bid} (fly delta={delta:.2f}, fly_pos={fly_pos})")

    # ------------------------------------------------------------------
    # LON_ETF Arbitrage — uses SSE-cached orderbooks (no API calls)
    # ------------------------------------------------------------------

    def _check_etf_arb(self):
        """Pure arb: LON_ETF should equal TIDE_SPOT + WX_SPOT + LHR_COUNT at settlement."""
        if not ETF_ARB_ENABLED:
            return
        with self._lock:
            obs = {sym: self._orderbooks.get(sym)
                   for sym in ["LON_ETF", "TIDE_SPOT", "WX_SPOT", "LHR_COUNT"]}

        if any(ob is None for ob in obs.values()):
            return  # orderbooks not yet populated via SSE

        etf_ob, tide_ob, wx_ob, lhr_ob = (
            obs["LON_ETF"], obs["TIDE_SPOT"], obs["WX_SPOT"], obs["LHR_COUNT"]
        )

        # Case 1: ETF bid > sum(component asks) → SELL ETF, BUY components
        etf_bid, tide_ask, wx_ask, lhr_ask = (
            _best_bid(etf_ob), _best_ask(tide_ob), _best_ask(wx_ob), _best_ask(lhr_ob)
        )
        if None not in (etf_bid, tide_ask, wx_ask, lhr_ask):
            profit = etf_bid - (tide_ask + wx_ask + lhr_ask)
            if profit >= ARB_TRIGGER_TICKS:
                print(f"[ARB] SELL ETF@{etf_bid:.0f} / BUY components  profit={profit:.0f}")
                self._send_arb(
                    (Side.SELL, "LON_ETF",   etf_bid),
                    (Side.BUY,  "TIDE_SPOT", tide_ask),
                    (Side.BUY,  "WX_SPOT",   wx_ask),
                    (Side.BUY,  "LHR_COUNT", lhr_ask),
                )

        # Case 2: sum(component bids) > ETF ask → BUY ETF, SELL components
        etf_ask, tide_bid, wx_bid, lhr_bid = (
            _best_ask(etf_ob), _best_bid(tide_ob), _best_bid(wx_ob), _best_bid(lhr_ob)
        )
        if None not in (etf_ask, tide_bid, wx_bid, lhr_bid):
            profit = (tide_bid + wx_bid + lhr_bid) - etf_ask
            if profit >= ARB_TRIGGER_TICKS:
                print(f"[ARB] BUY ETF@{etf_ask:.0f} / SELL components  profit={profit:.0f}")
                self._send_arb(
                    (Side.BUY,  "LON_ETF",   etf_ask),
                    (Side.SELL, "TIDE_SPOT", tide_bid),
                    (Side.SELL, "WX_SPOT",   wx_bid),
                    (Side.SELL, "LHR_COUNT", lhr_bid),
                )

    def _send_arb(self, *legs: tuple[Side, str, float], volume: int = 5):
        max_pos = 30
        positions = self._positions

        def ok(sym: str, side: Side) -> bool:
            pos = positions.get(sym, 0)
            return not (side == Side.BUY and pos >= max_pos
                        or side == Side.SELL and pos <= -max_pos)

        orders = [
            OrderRequest(sym, price, side, volume)
            for side, sym, price in legs
            if ok(sym, side)
        ]
        if len(orders) == 4:
            self.send_orders(orders)
        else:
            print("[ARB] Skipped: position limit on one or more legs")

    # ------------------------------------------------------------------
    # Trap orders — extreme resting limits to catch undisciplined bots
    # ------------------------------------------------------------------

    def _refresh_traps(self, live_fv: dict[str, float]) -> None:
        """Cancel and re-place extreme resting limit orders for active products.

        Any bot that crosses the spread without price discipline will fill against
        these, handing us large instant PnL. Normal bots with proper risk management
        will never touch them.
        """
        with self._lock:
            positions = dict(self._positions)

        for sym, product in self._products.items():
            if sym in GROUP_A_PRODUCTS:
                continue
            fv = live_fv.get(sym)
            if not fv or fv <= 0:
                continue

            max_pos = MAX_POSITION.get(sym, 0)
            if max_pos == 0:
                continue

            tick = product.tickSize
            pos = positions.get(sym, 0)

            # Cancel old trap orders (no-op if already filled)
            for oid in self._trap_orders.get(sym, []):
                self.cancel_order(oid)

            trap_orders: list[OrderRequest] = []

            # Extreme SELL traps — catch buyers with no price cap
            sell_capacity = max_pos + pos   # how much further short we can go
            if sell_capacity > 0:
                for mult in TRAP_SELL_MULTIPLES:
                    price = round(fv * mult / tick) * tick
                    vol = min(TRAP_VOLUME, sell_capacity)
                    trap_orders.append(OrderRequest(sym, price, Side.SELL, vol))

            # Extreme BUY traps — catch sellers with no price floor
            buy_capacity = max_pos - pos    # how much further long we can go
            if buy_capacity > 0:
                for frac in TRAP_BUY_FRACTIONS:
                    price = max(tick, round(fv * frac / tick) * tick)
                    vol = min(TRAP_VOLUME, buy_capacity)
                    trap_orders.append(OrderRequest(sym, price, Side.BUY, vol))

            if not trap_orders:
                self._trap_orders[sym] = []
                continue

            results = self.send_orders(trap_orders)
            self._trap_orders[sym] = [r.id for r in results if r is not None]
            print(
                f"[TRAP] {sym}: {len(self._trap_orders[sym])} orders placed "
                f"(sells @{[round(fv*m) for m in TRAP_SELL_MULTIPLES]}, "
                f"buys @{[max(1, round(fv*f)) for f in TRAP_BUY_FRACTIONS]})"
            )

    # ------------------------------------------------------------------
    # Local state persistence
    # ------------------------------------------------------------------

    def _load_state(self):
        if not self._state_path.exists():
            return
        try:
            with self._state_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"[STATE] Load failed: {e}")
            return

        if data.get("session_start") != self._session_start.isoformat():
            print("[STATE] Session changed; ignoring previous persisted state")
            return

        try:
            positions = {k: int(v) for k, v in data.get("positions", {}).items()}
            last_quoted_fv = {k: float(v) for k, v in data.get("last_quoted_fv", {}).items()}
            market_mids = {k: float(v) for k, v in data.get("market_mids", {}).items()}
            fv_data = data.get("fv_estimate", {})
            fv = {k: float(v) for k, v in fv_data.get("fv", {}).items()}
            conf = {k: float(v) for k, v in fv_data.get("confidence", {}).items()}

            with self._lock:
                self._positions.update(positions)
                self._last_quoted_fv.update(last_quoted_fv)
                self._market_mids.update(market_mids)
                # Compact legacy/full payloads on load so future saves stay small.
                self._flights_w1 = self._compact_flights_payload(data.get("flights_w1"))
                self._flights_w2 = self._compact_flights_payload(data.get("flights_w2"))
                if fv or conf:
                    self._fv_estimate = FairValueEstimate(
                        fv=fv,
                        confidence=conf,
                        last_data_update=time.monotonic(),
                    )
                self._state_dirty = False

            print(
                f"[STATE] Restored mids={len(market_mids)} "
                f"fv={len(fv)} flights={'yes' if self._flights_w1 or self._flights_w2 else 'no'}"
            )
        except Exception as e:
            print(f"[STATE] Invalid state format: {e}")

    def _save_state(self, force: bool = False):
        compact_w1: dict | None = None
        compact_w2: dict | None = None
        with self._lock:
            if not force:
                if not self._state_dirty:
                    return
                if time.monotonic() - self._last_state_save < STATE_SAVE_INTERVAL_SECS:
                    return
            positions = dict(self._positions)
            last_quoted_fv = dict(self._last_quoted_fv)
            market_mids = dict(self._market_mids)
            fv = dict(self._fv_estimate.fv)
            conf = dict(self._fv_estimate.confidence)
            flights_w1 = self._flights_w1
            flights_w2 = self._flights_w2

        # Keep state compact: only persist fields used by the model.
        compact_w1 = self._compact_flights_payload(flights_w1)
        compact_w2 = self._compact_flights_payload(flights_w2)
        payload = {
            "version": 2,
            "saved_at": datetime.now(tz=self._session_start.tzinfo).isoformat(),
            "session_start": self._session_start.isoformat(),
            "positions": positions,
            "last_quoted_fv": last_quoted_fv,
            "market_mids": market_mids,
            "fv_estimate": {
                "fv": fv,
                "confidence": conf,
            },
            "flights_w1": compact_w1,
            "flights_w2": compact_w2,
        }

        try:
            tmp_path = self._state_path.with_suffix(self._state_path.suffix + ".tmp")
            with tmp_path.open("w", encoding="utf-8") as f:
                json.dump(payload, f, separators=(",", ":"), ensure_ascii=True)
            tmp_path.replace(self._state_path)
            with self._lock:
                self._state_dirty = False
                self._last_state_save = time.monotonic()
        except Exception as e:
            print(f"[STATE] Save failed: {e}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    EXCHANGE_URL = os.getenv("CMI_URL_CHALLENGE", "")
    USERNAME = os.getenv("CMI_USERNAME_CHALLENGE", "")
    PASSWORD = os.getenv("CMI_PASSWORD_CHALLENGE", "")
    AERODATABOX_KEY = os.getenv("AERODATABOX_KEY", "")

    SESSION_START = resolve_session_start()

    bot = AlphaBot(
        cmi_url=EXCHANGE_URL,
        username=USERNAME,
        password=PASSWORD,
        aero_api_key=AERODATABOX_KEY,
        session_start=SESSION_START,
    )
    bot.run()
