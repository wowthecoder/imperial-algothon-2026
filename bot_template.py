"""CMI Exchange bot framework.

Lightweight bot base class for connecting to the CMI simulated exchange.
"""

import json
import time
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from enum import Enum
try:
    from enum import StrEnum
except ImportError:  # Python <3.11
    class StrEnum(str, Enum):
        pass
from functools import cached_property
from threading import Thread
from traceback import format_exc
from typing import Any, Callable, Literal

import requests
import sseclient

STANDARD_HEADERS = {"Content-Type": "application/json; charset=utf-8"}
# SSE debug controls (set once here; no environment variables needed)
SSE_DEBUG = False
SSE_DEBUG_MAX_CHARS = 320
SSE_DEBUG_EVERY_N = 1
SSE_DEBUG_RAW_LINES = False


class DictLikeFrozenDataclassMapping(Mapping):
    """Mixin class to allow frozen dataclasses behave like a dict."""

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __iter__(self):
        return iter(self.__annotations__)

    def __len__(self) -> int:
        return len(self.__annotations__)

    def to_dict(self) -> dict:
        return asdict(self)

    def keys(self):
        return self.__annotations__.keys()

    def values(self):
        return [getattr(self, k) for k in self.keys()]

    def items(self):
        return [(k, getattr(self, k)) for k in self.keys()]


@dataclass(frozen=True)
class Product(DictLikeFrozenDataclassMapping):
    symbol: str
    tickSize: float
    startingPrice: int
    contractSize: int


@dataclass(frozen=True)
class Trade(DictLikeFrozenDataclassMapping):
    timestamp: str
    product: str
    buyer: str
    seller: str
    volume: int
    price: float


@dataclass(frozen=True)
class Order(DictLikeFrozenDataclassMapping):
    price: float
    volume: int
    own_volume: int


@dataclass(frozen=True)
class OrderBook(DictLikeFrozenDataclassMapping):
    product: str
    tick_size: float
    buy_orders: list[Order]
    sell_orders: list[Order]


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True)
class OrderRequest:
    product: str
    price: float
    side: Side
    volume: int


@dataclass(frozen=True)
class OrderResponse:
    id: str
    status: Literal["ACTIVE", "PART_FILLED"]
    product: str
    side: Side
    price: float
    volume: int
    filled: int
    user: str
    timestamp: str
    targetUser: str | None = None
    message: str | None = None


class _SSEThread(Thread):
    """Background thread that consumes the CMI SSE stream and dispatches events."""

    def __init__(
        self,
        bearer: str,
        url: str,
        handle_orderbook: Callable[[OrderBook], Any],
        handle_trade_event: Callable[[Trade], Any],
    ):
        super().__init__(daemon=True)
        self._bearer = bearer
        self._url = url
        self._handle_orderbook = handle_orderbook
        self._handle_trade_event = handle_trade_event
        self._http_stream: requests.Response | None = None
        self._client: sseclient.SSEClient | None = None
        self._closed = False
        self._debug_event_count = 0

    def run(self):
        while not self._closed:
            try:
                self._consume()
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
                pass
            except Exception:
                if not self._closed:
                    print("SSE error, reconnecting...")
                    print(format_exc())

    def close(self):
        self._closed = True
        if self._http_stream:
            self._http_stream.close()
        if self._client:
            # sseclient 0.0.27 has no close(); close underlying response instead.
            if hasattr(self._client, "close"):
                self._client.close()
            elif hasattr(self._client, "resp"):
                self._client.resp.close()

    def _consume(self):
        headers = {
            "Authorization": self._bearer,
            "Accept": "text/event-stream; charset=utf-8",
        }
        self._http_stream = requests.get(self._url, stream=True, headers=headers, timeout=30)
        self._http_stream.raise_for_status()
        self._client = None

        event_name = "message"
        data_lines: list[str] = []

        def flush_event() -> None:
            nonlocal event_name, data_lines
            self._debug_event_count += 1
            do_debug = SSE_DEBUG and (self._debug_event_count % SSE_DEBUG_EVERY_N == 0)
            raw_data = "\n".join(data_lines).strip()
            if do_debug:
                preview = raw_data.replace("\n", "\\n")[:SSE_DEBUG_MAX_CHARS] if raw_data else "<empty>"
                print(
                    f"[SSE DEBUG] #{self._debug_event_count} event={event_name!r} "
                    f"data_len={len(raw_data)} preview={preview}"
                )
            if not raw_data:
                event_name = "message"
                data_lines = []
                return

            try:
                payload = json.loads(raw_data)
            except json.JSONDecodeError:
                if do_debug:
                    print("[SSE DEBUG] JSON decode failed; skipping frame")
                event_name = "message"
                data_lines = []
                return

            event_name_l = event_name.strip().lower()
            is_order_payload = (
                isinstance(payload, dict)
                and "productsymbol" in payload
                and "buyOrders" in payload
                and "sellOrders" in payload
            )
            if do_debug:
                if isinstance(payload, dict):
                    keys_preview = list(payload.keys())[:8]
                    print(f"[SSE DEBUG] payload=dict keys={keys_preview}")
                elif isinstance(payload, list):
                    print(f"[SSE DEBUG] payload=list len={len(payload)}")
                else:
                    print(f"[SSE DEBUG] payload_type={type(payload).__name__}")

            if event_name_l == "order" or is_order_payload:
                if isinstance(payload, dict):
                    if do_debug:
                        print("[SSE DEBUG] dispatch=orderbook")
                    self._on_order_event(payload)
                event_name = "message"
                data_lines = []
                return

            trades = payload if isinstance(payload, list) else [payload]
            trade_fields = set(Trade.__dataclass_fields__.keys())
            valid_trades = [
                t for t in trades
                if isinstance(t, dict) and {"timestamp", "product", "buyer", "seller", "volume", "price"} <= set(t.keys())
            ]
            if event_name_l == "trade" or valid_trades:
                if do_debug:
                    print(f"[SSE DEBUG] dispatch=trade count={len(valid_trades)}")
                for t in valid_trades:
                    self._handle_trade_event(Trade(**{k: v for k, v in t.items() if k in trade_fields}))
                event_name = "message"
                data_lines = []
                return

            if do_debug:
                print("[SSE DEBUG] dispatch=ignored (unknown payload shape)")
            event_name = "message"
            data_lines = []

        for raw_line in self._http_stream.iter_lines(decode_unicode=True):
            if self._closed:
                break

            line = raw_line.rstrip("\r") if isinstance(raw_line, str) else ""
            if SSE_DEBUG and SSE_DEBUG_RAW_LINES and line:
                print(f"[SSE DEBUG RAW] {line[:SSE_DEBUG_MAX_CHARS]}")

            # Empty line terminates one SSE event.
            if line == "":
                flush_event()
                continue

            # Comment/heartbeat
            if line.startswith(":"):
                continue

            field, sep, value = line.partition(":")
            field_l = field.strip().lower()
            if sep and value.startswith(" "):
                value = value[1:]

            if field_l == "event":
                event_name = value.strip() or "message"
            elif field_l == "data":
                data_lines.append(value)
            elif not sep and line[:1] in "{[":
                # Non-standard stream: raw JSON line with no "data:" prefix.
                data_lines.append(line)

        # If stream closes without trailing blank line, flush buffered payload.
        if data_lines:
            flush_event()

    def _on_order_event(self, data: dict[str, Any]):
        buy_orders = sorted(
            [
                Order(price=float(price), volume=v["marketVolume"], own_volume=v["userVolume"])
                for price, v in data["buyOrders"].items()
            ],
            key=lambda o: -o.price,
        )
        sell_orders = sorted(
            [
                Order(price=float(price), volume=v["marketVolume"], own_volume=v["userVolume"])
                for price, v in data["sellOrders"].items()
            ],
            key=lambda o: o.price,
        )
        self._handle_orderbook(OrderBook(data["productsymbol"], data["tickSize"], buy_orders, sell_orders))


class BaseBot(ABC):
    """Base bot for CMI Exchange.
    """

    def __init__(self, cmi_url: str, username: str, password: str):
        if not isinstance(cmi_url, str):
            raise TypeError(f"cmi_url must be a string URL, got {type(cmi_url).__name__}")
        self._cmi_url = cmi_url.rstrip("/")
        self.username = username
        self._password = password
        self._sse_thread: _SSEThread | None = None

        # Incremental trade state
        self.trades: list[Trade] = []
        self._trade_watermark: str | None = None
        self._last_trade_fetch: float | None = None

    @cached_property
    def auth_token(self) -> str:
        response = requests.post(
            f"{self._cmi_url}/api/user/authenticate",
            headers=STANDARD_HEADERS,
            json={"username": self.username, "password": self._password},
        )
        response.raise_for_status()
        return response.headers["Authorization"]

    # -- lifecycle --

    def start(self) -> None:
        if self._sse_thread:
            raise RuntimeError("Bot already running. Call stop() first.")
        self._sse_thread = _SSEThread(
            bearer=self.auth_token,
            url=f"{self._cmi_url}/api/market/stream",
            handle_orderbook=self.on_orderbook,
            handle_trade_event=self.on_trades,
        )
        self._sse_thread.start()

    def stop(self) -> None:
        if self._sse_thread:
            self._sse_thread.close()
            self._sse_thread.join(timeout=5)
            self._sse_thread = None

    # -- callbacks --

    @abstractmethod
    def on_orderbook(self, orderbook: OrderBook) -> None: ...

    @abstractmethod
    def on_trades(self, trade: Trade) -> None: ...

    # -- market trades (incremental) --

    def get_market_trades(self) -> list[Trade]:
        """Fetch new market trades from the exchange and append to self.trades.

        Uses incremental loading: only requests trades newer than the last
        seen timestamp. Returns the full accumulated list.
        """
        params: dict[str, str] = {}
        if self._trade_watermark:
            params["from"] = self._trade_watermark
        response = requests.get(
            f"{self._cmi_url}/api/trade",
            params=params,
            headers=self._auth_headers(),
        )
        self._last_trade_fetch = time.monotonic()
        if not response.ok:
            print(f"Failed to fetch trades: {response.status_code}")
            return self.trades

        new_trades = []
        for raw in response.json():
            trade = Trade(**raw)
            if self._trade_watermark is None or trade.timestamp > self._trade_watermark:
                new_trades.append(trade)

        if new_trades:
            self.trades.extend(new_trades)
            self._trade_watermark = new_trades[-1].timestamp

        return self.trades

    @property
    def last_trade_fetch_age(self) -> float | None:
        """Seconds since last get_market_trades() call, or None if never called."""
        if self._last_trade_fetch is None:
            return None
        return time.monotonic() - self._last_trade_fetch

    # -- trading helpers --

    def send_order(self, order: OrderRequest) -> OrderResponse | None:
        response = requests.post(
            f"{self._cmi_url}/api/order",
            json=asdict(order),
            headers=self._auth_headers(),
        )
        if response.ok:
            return OrderResponse(**response.json())
        print(f"Order failed: {response.text}")
        return None

    def send_orders(self, orders: list[OrderRequest]) -> list[OrderResponse]:
        results: list[OrderResponse] = []

        def _send(o: OrderRequest):
            r = self.send_order(o)
            if r:
                results.append(r)

        threads = [Thread(target=_send, args=(o,)) for o in orders]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return results

    def cancel_order(self, order_id: str) -> None:
        requests.delete(f"{self._cmi_url}/api/order/{order_id}", headers=self._auth_headers())

    def cancel_all_orders(self) -> None:
        orders = self.get_orders()
        threads = [Thread(target=self.cancel_order, args=(o["id"],)) for o in orders]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def get_orders(self, product: str | None = None) -> list[dict]:
        params = {"productsymbol": product} if product else {}
        response = requests.get(
            f"{self._cmi_url}/api/order/current-user",
            params=params,
            headers=self._auth_headers(),
        )
        return response.json() if response.ok else []

    def get_products(self) -> list[Product]:
        response = requests.get(f"{self._cmi_url}/api/product", headers=self._auth_headers())
        response.raise_for_status()
        return [Product(**p) for p in response.json()]

    def get_positions(self) -> dict[str, int]:
        response = requests.get(
            f"{self._cmi_url}/api/position/current-user",
            headers=self._auth_headers(),
        )
        if response.ok:
            return {p["product"]: p["netPosition"] for p in response.json()}
        return {}

    def get_orderbook(self, product: str) -> OrderBook:
        response = requests.get(
            f"{self._cmi_url}/api/product/{product}/order-book/current-user",
            headers=self._auth_headers(),
        )
        response.raise_for_status()
        data = response.json()
        buy_orders = sorted(
            [Order(price=e["price"], volume=e["volume"], own_volume=e["userOrderVolume"]) for e in data.get("buy", [])],
            key=lambda o: -o.price,
        )
        sell_orders = sorted(
            [Order(price=e["price"], volume=e["volume"], own_volume=e["userOrderVolume"]) for e in data.get("sell", [])],
            key=lambda o: o.price,
        )
        return OrderBook(data["product"], data["tickSize"], buy_orders, sell_orders)

    def get_pnl(self) -> dict:
        response = requests.get(
            f"{self._cmi_url}/api/profit/current-user",
            headers=self._auth_headers(),
        )
        return response.json() if response.ok else {}

    # -- internals --

    def _auth_headers(self) -> dict[str, str]:
        return {**STANDARD_HEADERS, "Authorization": self.auth_token}
