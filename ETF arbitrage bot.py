"""ETF Arbitrage Bot — IMCity Hackathon

Strategy:
    LON_ETF = TIDE_SPOT + WX_SPOT + LHR_COUNT

    If ETF is OVERPRICED vs components:  SELL ETF, BUY TIDE_SPOT + WX_SPOT + LHR_COUNT
    If ETF is UNDERPRICED vs components: BUY ETF, SELL TIDE_SPOT + WX_SPOT + LHR_COUNT

Arb check uses REAL fill prices (best ask to buy, best bid to sell), not mids.
This means an opportunity is only flagged when it's actually executable at a profit.

Leg risk handling: "complete the hedge" — if we get filled on the ETF leg,
we always chase the remaining component legs at market to avoid naked exposure.

Tunable parameters (see ArbBot.__init__):
    MIN_EDGE       — minimum profit per unit before firing (filters noise)
    VOLUME         — contracts per trade
    POLL_INTERVAL  — seconds between arb checks (min 1.0 per exchange rules)
    COMPONENT_SYMBOLS — the 3 components (adjust if symbols differ)
"""

import time
import threading
from dataclasses import dataclass
from threading import Thread
from typing import Optional

from bot_template import BaseBot, OrderBook, OrderRequest, OrderResponse, Trade, Side


# ── Product symbols ────────────────────────────────────────────────────────────
ETF_SYMBOL = "LON_ETF"
COMPONENT_SYMBOLS = ["TIDE_SPOT", "WX_SPOT", "LHR_COUNT"]
ALL_SYMBOLS = [ETF_SYMBOL] + COMPONENT_SYMBOLS


@dataclass
class ArbOpportunity:
    direction: str          # "BUY_ETF" or "SELL_ETF"
    edge_per_unit: float    # profit per contract if fully filled
    etf_price: float        # price we'd trade the ETF at
    component_prices: dict  # {symbol: price} for each component
    volume: int             # max fillable volume given book depth


class ArbBot(BaseBot):
    """
    Polls all 4 orderbooks and fires when a profitable arb exists.
    """

    def __init__(
        self,
        cmi_url: str,
        username: str,
        password: str,
        min_edge: float = 3.0,       # minimum profit per unit (tune this!)
        volume: int = 5,             # contracts per arb trade
        poll_interval: float = 2.0,  # seconds between checks (>= 1.0)
    ):
        super().__init__(cmi_url, username, password)
        self.min_edge = min_edge
        self.volume = volume
        self.poll_interval = max(poll_interval, 1.0)

        self._lock = threading.Lock()
        self._trading = False        # guard against concurrent arb fires
        self._running = False

        self.stats = {
            "checks": 0,
            "opportunities_found": 0,
            "trades_fired": 0,
            "pnl_estimate": 0.0,
        }

    # ── BaseBot callbacks (required) ───────────────────────────────────────────

    def on_orderbook(self, orderbook: OrderBook) -> None:
        pass  # we poll rather than react to avoid feedback loops

    def on_trades(self, trade: Trade) -> None:
        side = "BOUGHT" if trade.buyer == self.username else "SOLD"
        print(f"  [FILL] {side} {trade.volume}x {trade.product} @ {trade.price}")

    # ── Main loop ──────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Blocking main loop. Ctrl+C to stop."""
        self.start()  # start SSE for fill notifications
        self._running = True
        print(f"[ArbBot] Starting. min_edge={self.min_edge}, volume={self.volume}, "
              f"poll_interval={self.poll_interval}s")
        print(f"[ArbBot] Watching: {ETF_SYMBOL} vs {' + '.join(COMPONENT_SYMBOLS)}\n")

        try:
            while self._running:
                self._check_and_trade()
                time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            print("\n[ArbBot] Interrupted — cancelling all orders...")
        finally:
            self.cancel_all_orders()
            self.stop()
            self._print_stats()

    def stop_loop(self) -> None:
        self._running = False

    # ── Core logic ─────────────────────────────────────────────────────────────

    def _fetch_all_orderbooks(self) -> Optional[dict[str, OrderBook]]:
        """Fetch all 4 orderbooks in parallel. Returns None if any fetch fails."""
        results: dict[str, OrderBook] = {}
        errors: list[str] = []
        lock = threading.Lock()

        def fetch(symbol: str):
            try:
                ob = self.get_orderbook(symbol)
                with lock:
                    results[symbol] = ob
            except Exception as e:
                with lock:
                    errors.append(f"{symbol}: {e}")

        threads = [Thread(target=fetch, args=(s,)) for s in ALL_SYMBOLS]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        if errors:
            print(f"[ArbBot] Fetch errors: {errors}")
            return None
        return results

    def _find_opportunity(self, books: dict[str, OrderBook]) -> Optional[ArbOpportunity]:
        """
        Check both arb directions. Returns the best opportunity if edge >= min_edge.

        We use REAL fill prices:
          - To BUY something: pay the best ASK
          - To SELL something: receive the best BID

        This means we only flag an arb if it's profitable at actual execution prices.
        """
        etf_book = books[ETF_SYMBOL]
        comp_books = {s: books[s] for s in COMPONENT_SYMBOLS}

        # Best prices available
        etf_best_bid = etf_book.buy_orders[0].price if etf_book.buy_orders else None
        etf_best_ask = etf_book.sell_orders[0].price if etf_book.sell_orders else None
        comp_best_bids = {s: b.buy_orders[0].price if b.buy_orders else None for s, b in comp_books.items()}
        comp_best_asks = {s: b.sell_orders[0].price if b.sell_orders else None for s, b in comp_books.items()}

        # ── Direction 1: SELL ETF, BUY components ─────────────────────────────
        # Revenue: sell ETF at best_bid
        # Cost: buy each component at best_ask
        if etf_best_bid and all(comp_best_asks[s] for s in COMPONENT_SYMBOLS):
            component_buy_cost = sum(comp_best_asks[s] for s in COMPONENT_SYMBOLS)
            edge_sell_etf = etf_best_bid - component_buy_cost

            if edge_sell_etf >= self.min_edge:
                # Check volume: limited by depth on all sides
                vol = min(
                    self.volume,
                    etf_book.buy_orders[0].volume - etf_book.buy_orders[0].own_volume,
                    *[comp_books[s].sell_orders[0].volume - comp_books[s].sell_orders[0].own_volume
                      for s in COMPONENT_SYMBOLS],
                )
                if vol > 0:
                    return ArbOpportunity(
                        direction="SELL_ETF",
                        edge_per_unit=edge_sell_etf,
                        etf_price=etf_best_bid,
                        component_prices={s: comp_best_asks[s] for s in COMPONENT_SYMBOLS},
                        volume=vol,
                    )

        # ── Direction 2: BUY ETF, SELL components ─────────────────────────────
        # Cost: buy ETF at best_ask
        # Revenue: sell each component at best_bid
        if etf_best_ask and all(comp_best_bids[s] for s in COMPONENT_SYMBOLS):
            component_sell_revenue = sum(comp_best_bids[s] for s in COMPONENT_SYMBOLS)
            edge_buy_etf = component_sell_revenue - etf_best_ask

            if edge_buy_etf >= self.min_edge:
                vol = min(
                    self.volume,
                    etf_book.sell_orders[0].volume - etf_book.sell_orders[0].own_volume,
                    *[comp_books[s].buy_orders[0].volume - comp_books[s].buy_orders[0].own_volume
                      for s in COMPONENT_SYMBOLS],
                )
                if vol > 0:
                    return ArbOpportunity(
                        direction="BUY_ETF",
                        edge_per_unit=edge_buy_etf,
                        etf_price=etf_best_ask,
                        component_prices={s: comp_best_bids[s] for s in COMPONENT_SYMBOLS},
                        volume=vol,
                    )

        return None

    def _execute_arb(self, opp: ArbOpportunity) -> None:
        """
        Execute the arb. Strategy:
          1. Fire ETF leg first (hardest to leg out of if missed).
          2. Fire all 3 component legs in parallel immediately after.
          3. If ETF filled but a component leg fails: complete hedge at market
             (accept a worse price rather than carry naked exposure).
        """
        print(f"\n[ARB] {'='*60}")
        print(f"[ARB] Direction : {opp.direction}")
        print(f"[ARB] Edge/unit : {opp.edge_per_unit:.2f}")
        print(f"[ARB] Volume    : {opp.volume}")
        print(f"[ARB] ETF price : {opp.etf_price}")
        print(f"[ARB] Comp prices: {opp.component_prices}")

        etf_side = Side.SELL if opp.direction == "SELL_ETF" else Side.BUY
        comp_side = Side.BUY if opp.direction == "SELL_ETF" else Side.SELL

        # Step 1: ETF leg
        etf_order = OrderRequest(
            product=ETF_SYMBOL,
            price=opp.etf_price,
            side=etf_side,
            volume=opp.volume,
        )
        etf_resp = self.send_order(etf_order)
        if not etf_resp or etf_resp.filled == 0:
            print("[ARB] ETF leg not filled — aborting.")
            if etf_resp:
                self.cancel_order(etf_resp.id)
            return

        filled_vol = etf_resp.filled
        if etf_resp.volume > etf_resp.filled:
            # Partial fill on ETF — cancel remainder, hedge what filled
            self.cancel_order(etf_resp.id)
            print(f"[ARB] ETF partial fill: {filled_vol}/{opp.volume}")

        print(f"[ARB] ETF leg filled: {filled_vol}x @ {opp.etf_price}")

        # Step 2: Component legs (parallel, chase market if needed)
        def execute_component(symbol: str, price: float):
            order = OrderRequest(
                product=symbol,
                price=price,
                side=comp_side,
                volume=filled_vol,
            )
            resp = self.send_order(order)
            if not resp or resp.filled == 0:
                print(f"[ARB] WARNING: {symbol} leg failed — fetching market to hedge...")
                # Complete hedge at market (leg risk mitigation)
                self._hedge_at_market(symbol, comp_side, filled_vol)
            elif resp.filled < filled_vol:
                self.cancel_order(resp.id)
                remaining = filled_vol - resp.filled
                print(f"[ARB] {symbol} partial ({resp.filled}/{filled_vol}) — hedging {remaining} at market")
                self._hedge_at_market(symbol, comp_side, remaining)
            else:
                print(f"[ARB] {symbol} leg filled: {resp.filled}x @ {price}")

        comp_threads = [
            Thread(target=execute_component, args=(symbol, price))
            for symbol, price in opp.component_prices.items()
        ]
        for t in comp_threads:
            t.start()
        for t in comp_threads:
            t.join()

        estimated_pnl = opp.edge_per_unit * filled_vol
        self.stats["trades_fired"] += 1
        self.stats["pnl_estimate"] += estimated_pnl
        print(f"[ARB] Done. Estimated PnL this trade: {estimated_pnl:.2f}")
        print(f"[ARB] Cumulative estimated PnL: {self.stats['pnl_estimate']:.2f}")
        print(f"[ARB] {'='*60}\n")

    def _hedge_at_market(self, symbol: str, side: Side, volume: int, retries: int = 3) -> None:
        """Chase a leg at the current best market price. Retries if needed."""
        for attempt in range(retries):
            try:
                ob = self.get_orderbook(symbol)
                price = ob.sell_orders[0].price if side == Side.BUY else ob.buy_orders[0].price
                resp = self.send_order(OrderRequest(symbol, price, side, volume))
                if resp and resp.filled > 0:
                    print(f"[HEDGE] {symbol} hedged {resp.filled}x @ {price} (attempt {attempt+1})")
                    if resp.filled < volume:
                        self.cancel_order(resp.id)
                        volume -= resp.filled
                        continue
                    return
            except Exception as e:
                print(f"[HEDGE] Error on attempt {attempt+1}: {e}")
            time.sleep(0.5)
        print(f"[HEDGE] WARNING: Could not fully hedge {symbol} — check positions!")

    def _check_and_trade(self) -> None:
        self.stats["checks"] += 1

        books = self._fetch_all_orderbooks()
        if not books:
            return

        # Log mid prices periodically
        if self.stats["checks"] % 10 == 0:
            self._log_mids(books)

        opp = self._find_opportunity(books)
        if not opp:
            return

        self.stats["opportunities_found"] += 1
        print(f"[ArbBot] Opportunity: {opp.direction} | edge={opp.edge_per_unit:.2f} | vol={opp.volume}")

        # Guard: only one trade at a time
        with self._lock:
            if self._trading:
                print("[ArbBot] Already trading — skipping.")
                return
            self._trading = True

        try:
            self._execute_arb(opp)
        finally:
            with self._lock:
                self._trading = False

    def _log_mids(self, books: dict[str, OrderBook]) -> None:
        parts = []
        for sym in ALL_SYMBOLS:
            ob = books[sym]
            if ob.buy_orders and ob.sell_orders:
                mid = (ob.buy_orders[0].price + ob.sell_orders[0].price) / 2
                parts.append(f"{sym}={mid:.1f}")
        etf_mid = None
        comp_mid_sum = 0.0
        for sym in ALL_SYMBOLS:
            ob = books[sym]
            if ob.buy_orders and ob.sell_orders:
                mid = (ob.buy_orders[0].price + ob.sell_orders[0].price) / 2
                if sym == ETF_SYMBOL:
                    etf_mid = mid
                else:
                    comp_mid_sum += mid
        if etf_mid is not None:
            implied_diff = etf_mid - comp_mid_sum
            print(f"[Mids] {' | '.join(parts)} | ETF-components={implied_diff:+.1f}")

    def _print_stats(self) -> None:
        print("\n[ArbBot] ── Final Stats ──────────────────────")
        print(f"  Checks run          : {self.stats['checks']}")
        print(f"  Opportunities found : {self.stats['opportunities_found']}")
        print(f"  Trades fired        : {self.stats['trades_fired']}")
        print(f"  Estimated PnL       : {self.stats['pnl_estimate']:.2f}")
        print("[ArbBot] ─────────────────────────────────────")


# ── Entry point ────────────────────────────────────────────────────────────────

TEST_URL = "http://ec2-52-49-69-152.eu-west-1.compute.amazonaws.com/"   # TODO: test exchange URL (use this for practice)
CHALLENGE_URL = "REPLACE_WITH_CHALLENGE_URL"   # TODO: challenge exchange URL (all trades on this exchange matter for the challenge!)

# We'll use the test exchange throughout this notebook.
# Switch to CHALLENGE_URL when you are ready to compete.
EXCHANGE_URL = TEST_URL

USERNAME = "Stack_Overslept"  # TODO: your username
PASSWORD = "JKVM2026"  # TODO: your password

if __name__ == "__main__":

    bot = ArbBot(
        cmi_url=EXCHANGE_URL,
        username=USERNAME,
        password=PASSWORD,
        min_edge=3.0,       # ← tune: raise if too noisy, lower if missing opportunities
        volume=5,           # ← tune: contracts per arb trade
        poll_interval=2.0,  # ← tune: check every 2s (must be >= 1.0 per exchange rules)
    )

    bot.run()