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

import math
import time
from datetime import datetime, timedelta
from threading import Lock, Thread

import pandas as pd

from bot_template import BaseBot, OrderBook, OrderRequest, Side, Trade
from fair_value import (
    FairValueEstimate,
    compute_all_fair_values,
    fetch_flights_range,
    get_thames,
    get_weather,
    lon_fly_payoff,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MAX_POSITION: dict[str, int] = {
    "TIDE_SPOT": 50,
    "TIDE_SWING": 30,
    "WX_SPOT":   50,
    "WX_SUM":    30,
    "LHR_COUNT": 50,
    "LHR_INDEX": 30,
    "LON_ETF":   50,
    "LON_FLY":   20,
}

QUOTE_VOLUME = 5

# Spread half-width (ticks): narrow at high confidence, wide at low
MIN_SPREAD_TICKS = 3.0
MAX_SPREAD_TICKS = 10.0

# Minimum FV move to trigger a requote (preserves queue priority)
REQUOTE_THRESHOLD_TICKS = 1.0

# Hit mispriced orders when they cross our FV by at least this many ticks
HIT_THRESHOLD_TICKS = 3.0

# Don't re-hit same product within this window (SSE fires on every book change)
HIT_COOLDOWN_SECS = 10.0

# LON_ETF arb: minimum profit in ticks to trigger
ARB_TRIGGER_TICKS = 5.0

DATA_REFRESH_INTERVAL_SECS = 60     # weather + tides
QUOTING_INTERVAL_SECS = 5

SESSION_HOURS = 24


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_mid(ob: OrderBook) -> float | None:
    bids = [o.price for o in ob.buy_orders if o.volume - o.own_volume > 0]
    asks = [o.price for o in ob.sell_orders if o.volume - o.own_volume > 0]
    if bids and asks:
        return (max(bids) + min(asks)) / 2.0
    return None


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

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def run(self):
        print("AlphaBot starting...")
        self._products = {p.symbol: p for p in self.get_products()}
        print(f"Products: {list(self._products.keys())}")

        self.start()   # SSE stream → on_orderbook / on_trades
        self._refresh_all_data()

        Thread(target=self._data_loop, daemon=True).start()

        try:
            self._quoting_loop()
        except KeyboardInterrupt:
            print("\nShutting down...")
        finally:
            self.cancel_all_orders()
            self.stop()
            print("AlphaBot stopped.")

    # ------------------------------------------------------------------
    # SSE callbacks
    # ------------------------------------------------------------------

    def on_orderbook(self, ob: OrderBook) -> None:
        """Cache orderbook + market mid; instantly hit obvious mispricings."""
        mid = _safe_mid(ob)
        tick = 1.0
        with self._lock:
            self._orderbooks[ob.product] = ob          # ← cache full book
            if mid is not None:
                self._market_mids[ob.product] = mid
            p = self._products.get(ob.product)
            if p:
                tick = p.tickSize

        live_fv = self._compute_live_fv()
        fv = live_fv.get(ob.product)
        if fv is None:
            return

        now = time.monotonic()

        # Cooldown: don't hit same product repeatedly as SSE fires on every book change
        if now - self._last_hit.get(ob.product, 0) < HIT_COOLDOWN_SECS:
            return

        best_ask = _best_ask(ob)
        if best_ask is not None and fv - best_ask >= HIT_THRESHOLD_TICKS * tick:
            pos = self._positions.get(ob.product, 0)
            max_pos = MAX_POSITION.get(ob.product, 50)
            if pos < max_pos:
                vol = min(QUOTE_VOLUME, max_pos - pos)
                result = self.send_order(OrderRequest(ob.product, best_ask, Side.BUY, vol))
                if result:
                    self._last_hit[ob.product] = now
                    print(f"[HIT BUY]  {ob.product}: {vol}@{best_ask:.0f}  live_FV={fv:.1f}")

        best_bid = _best_bid(ob)
        if best_bid is not None and best_bid - fv >= HIT_THRESHOLD_TICKS * tick:
            pos = self._positions.get(ob.product, 0)
            max_pos = MAX_POSITION.get(ob.product, 50)
            if pos > -max_pos:
                vol = min(QUOTE_VOLUME, max_pos + pos)
                result = self.send_order(OrderRequest(ob.product, best_bid, Side.SELL, vol))
                if result:
                    self._last_hit[ob.product] = now
                    print(f"[HIT SELL] {ob.product}: {vol}@{best_bid:.0f}  live_FV={fv:.1f}")

    def on_trades(self, trade: Trade) -> None:
        side = "BOUGHT" if trade.buyer == self.username else "SOLD"
        sign = +1 if side == "BOUGHT" else -1
        print(f"  FILL: {side} {trade.volume}x {trade.product} @ {trade.price:.1f}")
        with self._lock:
            self._positions[trade.product] = (
                self._positions.get(trade.product, 0) + sign * trade.volume
            )

    # ------------------------------------------------------------------
    # Fair value: blend model + live market data
    # ------------------------------------------------------------------

    def _settlement_proximity(self) -> float:
        """0.0 early in session → 1.0 at settlement (noon Sunday).
        Ramps linearly over the final 6 hours before settlement.
        """
        now = datetime.now(tz=self._settlement_dt.tzinfo)
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
            live["LON_FLY"] = lon_fly_payoff(blended_etf)

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
        except Exception as e:
            print(f"[DATA] Fair value computation failed: {e}")
            return

        live_fv = self._compute_live_fv()
        self._print_fv_table(fv_est, live_fv)

    def _refresh_flights_smart(self):
        """Fetch flights in two 12h windows. Each window fetched only once."""
        fmt = "%Y-%m-%dT%H:%M"
        session_mid = self._session_start + timedelta(hours=12)
        now = datetime.now(tz=self._session_start.tzinfo)

        # Window 1: session_start → session_start+12h (fetch immediately, once)
        if self._flights_w1 is None:
            try:
                print("[DATA] Fetching flight window 1 (first 12h)...")
                self._flights_w1 = fetch_flights_range(
                    api_key=self._aero_api_key,
                    from_local=self._session_start.strftime(fmt),
                    to_local=session_mid.strftime(fmt),
                )
                arr = len(self._flights_w1.get("arrivals", []))
                dep = len(self._flights_w1.get("departures", []))
                print(f"[DATA] Window 1: {arr} arr, {dep} dep")
            except Exception as e:
                print(f"[DATA] Flight window 1 failed: {e}")

        # Window 2: session_start+12h → session_end (fetch once past midpoint)
        if self._flights_w2 is None and now >= session_mid:
            try:
                print("[DATA] Fetching flight window 2 (second 12h)...")
                self._flights_w2 = fetch_flights_range(
                    api_key=self._aero_api_key,
                    from_local=session_mid.strftime(fmt),
                    to_local=self._session_end.strftime(fmt),
                )
                arr = len(self._flights_w2.get("arrivals", []))
                dep = len(self._flights_w2.get("departures", []))
                print(f"[DATA] Window 2: {arr} arr, {dep} dep")
            except Exception as e:
                print(f"[DATA] Flight window 2 failed: {e}")

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

                live_fv = self._compute_live_fv()

                for symbol, product in self._products.items():
                    self._requote_product(symbol, product, live_fv, model)

                self._check_etf_arb()

            except Exception as e:
                print(f"[QUOTE ERROR] {e}")

            time.sleep(QUOTING_INTERVAL_SECS)

    def _requote_product(
        self,
        symbol: str,
        product,
        live_fv: dict[str, float],
        model: FairValueEstimate,
    ):
        fv = live_fv.get(symbol)
        if not fv or fv <= 0:
            return

        tick = product.tickSize
        prox = self._settlement_proximity()

        # Spread: narrower with higher model confidence and near settlement
        model_fv = model.fv.get(symbol)
        conf = min(1.0, model.confidence.get(symbol, 0.5) + prox * 0.3)
        base_spread = MAX_SPREAD_TICKS - conf * (MAX_SPREAD_TICKS - MIN_SPREAD_TICKS)

        # Widen if model and live disagree (extra uncertainty)
        if model_fv is not None:
            extra = min(abs(fv - model_fv) / tick * 0.5, 10.0)
        else:
            extra = 5.0
        spread = base_spread + extra

        # Skip requote if FV barely moved (preserve queue priority)
        last_fv = self._last_quoted_fv.get(symbol)
        if last_fv is not None and abs(fv - last_fv) < REQUOTE_THRESHOLD_TICKS * tick:
            return

        pos     = self._positions.get(symbol, 0)
        max_pos = MAX_POSITION.get(symbol, 50)

        bid = math.floor((fv - spread * tick) / tick) * tick
        ask = math.ceil( (fv + spread * tick) / tick) * tick

        if bid <= 0 or bid >= ask:
            return

        orders: list[OrderRequest] = []
        if pos < max_pos * 0.8:
            orders.append(OrderRequest(symbol, bid, Side.BUY, QUOTE_VOLUME))
        if pos > -max_pos * 0.8:
            orders.append(OrderRequest(symbol, ask, Side.SELL, QUOTE_VOLUME))

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

    # ------------------------------------------------------------------
    # LON_ETF Arbitrage — uses SSE-cached orderbooks (no API calls)
    # ------------------------------------------------------------------

    def _check_etf_arb(self):
        """Pure arb: LON_ETF should equal TIDE_SPOT + WX_SPOT + LHR_COUNT at settlement."""
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


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import pytz

    EXCHANGE_URL    = "http://ec2-52-49-69-152.eu-west-1.compute.amazonaws.com/"
    # EXCHANGE_URL  = "REPLACE_WITH_CHALLENGE_URL"

    USERNAME        = "Stack_Overslept"
    PASSWORD        = "JKVM2026"
    AERODATABOX_KEY = "07ee7d05eamshc9db0bf5c17f962p13157ejsn762d96e7aa7e"

    london_tz     = pytz.timezone("Europe/London")
    SESSION_START = london_tz.localize(datetime(2026, 3, 1, 14, 30, 0))

    bot = AlphaBot(
        cmi_url=EXCHANGE_URL,
        username=USERNAME,
        password=PASSWORD,
        aero_api_key=AERODATABOX_KEY,
        session_start=SESSION_START,
    )
    bot.run()
