"""LiquidityProviderBot — inventory-aware market maker for CMI Exchange.

This bot is intentionally execution-focused:
  - Quotes around live orderbook prices (no external alpha/fair-value model)
  - Skews quotes based on inventory to mean-revert positions
  - Uses SSE-cached books to avoid frequent REST orderbook polling
"""

from __future__ import annotations

import math
import os
import time
from threading import Lock, Thread
from typing import Any

from bot_template import BaseBot, OrderBook, OrderRequest, Side, Trade

# ---------------------------------------------------------------------------
# Strategy configuration
# ---------------------------------------------------------------------------

QUOTE_SIZE = 4
QUOTING_INTERVAL_SECS = 3.0
REQUOTE_THRESHOLD_TICKS = 1.0

MIN_HALF_SPREAD_TICKS = 2.0
MAX_HALF_SPREAD_TICKS = 8.0
INVENTORY_SKEW_TICKS = 6.0
INVENTORY_SOFT_LIMIT_FRAC = 0.85
STALE_BOOK_SECS = 20.0

DEFAULT_MAX_POSITION = 40
MAX_POSITION: dict[str, int] = {
    "TIDE_SPOT": 50,
    "TIDE_SWING": 40,
    "WX_SPOT": 50,
    "WX_SUM": 40,
    "LHR_COUNT": 50,
    "LHR_INDEX": 40,
    "LON_ETF": 50,
    "LON_FLY": 25,
}


def _best_bid(ob: OrderBook) -> tuple[float | None, int]:
    for level in ob.buy_orders:
        available = level.volume - level.own_volume
        if available > 0:
            return level.price, available
    return None, 0


def _best_ask(ob: OrderBook) -> tuple[float | None, int]:
    for level in ob.sell_orders:
        available = level.volume - level.own_volume
        if available > 0:
            return level.price, available
    return None, 0


def _side_volume(pos: int, max_pos: int, side: Side, base_size: int) -> int:
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
    sized = max(1, math.ceil(base_size * scale))
    return int(min(base_size, sized, remaining))


class LiquidityProviderBot(BaseBot):
    def __init__(
        self,
        cmi_url: str,
        username: str,
        password: str,
        quote_size: int = QUOTE_SIZE,
        quote_interval_secs: float = QUOTING_INTERVAL_SECS,
        target_symbols: set[str] | None = None,
    ):
        super().__init__(cmi_url, username, password)
        self._quote_size = quote_size
        self._quote_interval_secs = max(1.0, quote_interval_secs)
        self._target_symbols = target_symbols

        self._lock = Lock()
        self._products: dict[str, Any] = {}
        self._positions: dict[str, int] = {}
        self._orderbooks: dict[str, OrderBook] = {}
        self._book_updated_at: dict[str, float] = {}
        self._active_orders: dict[str, list[str]] = {}
        self._last_quotes: dict[str, tuple[float, float]] = {}

    def run(self) -> None:
        print("LiquidityProviderBot starting...")
        all_products = {p.symbol: p for p in self.get_products()}
        if self._target_symbols:
            self._products = {s: p for s, p in all_products.items() if s in self._target_symbols}
        else:
            self._products = all_products

        print(f"Quoting symbols: {list(self._products.keys())}")
        self.start()

        try:
            self._quoting_loop()
        except KeyboardInterrupt:
            print("\nShutting down...")
        finally:
            self.cancel_all_orders()
            self.stop()
            print("LiquidityProviderBot stopped.")

    # ------------------------------------------------------------------
    # SSE callbacks
    # ------------------------------------------------------------------

    def on_orderbook(self, orderbook: OrderBook) -> None:
        if self._products and orderbook.product not in self._products:
            return
        with self._lock:
            self._orderbooks[orderbook.product] = orderbook
            self._book_updated_at[orderbook.product] = time.monotonic()

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
            self._positions[trade.product] = self._positions.get(trade.product, 0) + sign * trade.volume

    # ------------------------------------------------------------------
    # Quoting
    # ------------------------------------------------------------------

    def _quoting_loop(self) -> None:
        while True:
            try:
                live_positions = self.get_positions()
                with self._lock:
                    self._positions = live_positions

                for symbol, product in self._products.items():
                    self._requote_symbol(symbol, product)
            except Exception as e:
                print(f"[QUOTE ERROR] {e}")
            time.sleep(self._quote_interval_secs)

    def _requote_symbol(self, symbol: str, product) -> None:
        with self._lock:
            ob = self._orderbooks.get(symbol)
            last_book_update = self._book_updated_at.get(symbol, 0.0)
            pos = self._positions.get(symbol, 0)
            old_ids = list(self._active_orders.get(symbol, []))
            old_quote = self._last_quotes.get(symbol)

        if ob is None:
            return
        if time.monotonic() - last_book_update > STALE_BOOK_SECS:
            self._cancel_symbol_orders(symbol, old_ids)
            return

        best_bid, bid_size = _best_bid(ob)
        best_ask, ask_size = _best_ask(ob)
        if best_bid is None or best_ask is None or best_bid >= best_ask:
            self._cancel_symbol_orders(symbol, old_ids)
            return

        tick = product.tickSize
        max_pos = MAX_POSITION.get(symbol, DEFAULT_MAX_POSITION)
        if max_pos <= 0:
            return

        mid = (best_bid + best_ask) / 2.0
        micro = (
            (best_ask * bid_size + best_bid * ask_size) / (bid_size + ask_size)
            if (bid_size + ask_size) > 0
            else mid
        )
        reference = 0.6 * micro + 0.4 * mid

        market_spread_ticks = (best_ask - best_bid) / tick
        half_spread_ticks = max(
            MIN_HALF_SPREAD_TICKS,
            min(MAX_HALF_SPREAD_TICKS, max(1.0, 0.75 * market_spread_ticks)),
        )

        inv_frac = max(-1.0, min(1.0, pos / max_pos))
        reservation = reference - inv_frac * INVENTORY_SKEW_TICKS * tick

        bid_px = math.floor((reservation - half_spread_ticks * tick) / tick) * tick
        ask_px = math.ceil((reservation + half_spread_ticks * tick) / tick) * tick

        bid_px = min(bid_px, best_ask - tick)
        ask_px = max(ask_px, best_bid + tick)
        if bid_px <= 0 or bid_px >= ask_px:
            self._cancel_symbol_orders(symbol, old_ids)
            return

        buy_vol = _side_volume(pos, max_pos, Side.BUY, self._quote_size)
        sell_vol = _side_volume(pos, max_pos, Side.SELL, self._quote_size)
        if pos >= max_pos * INVENTORY_SOFT_LIMIT_FRAC:
            buy_vol = 0
        if pos <= -max_pos * INVENTORY_SOFT_LIMIT_FRAC:
            sell_vol = 0

        orders: list[OrderRequest] = []
        if buy_vol > 0:
            orders.append(OrderRequest(symbol, bid_px, Side.BUY, buy_vol))
        if sell_vol > 0:
            orders.append(OrderRequest(symbol, ask_px, Side.SELL, sell_vol))

        if not orders:
            self._cancel_symbol_orders(symbol, old_ids)
            return

        if old_quote is not None:
            old_bid, old_ask = old_quote
            if (
                abs(old_bid - bid_px) < REQUOTE_THRESHOLD_TICKS * tick
                and abs(old_ask - ask_px) < REQUOTE_THRESHOLD_TICKS * tick
            ):
                return

        if old_ids:
            cancel_threads = [Thread(target=self.cancel_order, args=(order_id,)) for order_id in old_ids]
            for t in cancel_threads:
                t.start()
            for t in cancel_threads:
                t.join()

        results = self.send_orders(orders)
        new_ids = [r.id for r in results if r is not None]
        with self._lock:
            self._active_orders[symbol] = new_ids
            self._last_quotes[symbol] = (bid_px, ask_px)

        sides = " / ".join(f"{o.volume}@{o.price:.0f} {o.side}" for o in orders)
        print(
            f"[LP] {symbol:<12} pos={pos:>4} ref={reference:7.1f} "
            f"bid={bid_px:7.1f} ask={ask_px:7.1f} {sides}"
        )

    def _cancel_symbol_orders(self, symbol: str, order_ids: list[str]) -> None:
        if not order_ids:
            return
        threads = [Thread(target=self.cancel_order, args=(order_id,)) for order_id in order_ids]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        with self._lock:
            self._active_orders[symbol] = []


if __name__ == "__main__":
    # Set via environment variables for safety:
    #   export CMI_URL=http://ec2-xx-xx-xx-xx.eu-west-1.compute.amazonaws.com/
    #   export CMI_USERNAME=your_username
    #   export CMI_PASSWORD=your_password
    exchange_url = os.getenv("CMI_URL", "http://ec2-52-19-74-159.eu-west-1.compute.amazonaws.com/")
    username = os.getenv("CMI_USERNAME", "your_username")
    password = os.getenv("CMI_PASSWORD", "your_password")

    bot = LiquidityProviderBot(
        cmi_url=exchange_url,
        username=username,
        password=password,
        quote_size=QUOTE_SIZE,
        quote_interval_secs=QUOTING_INTERVAL_SECS,
        target_symbols=None,
    )
    bot.run()
