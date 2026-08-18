"""
Order book data source for Canonic MAOB.
Polls getMidPrice / getDepth every ~2s (MVP; upgrade to WS later).
"""

import asyncio
import logging
import time
from decimal import Decimal
from typing import Dict, List, Optional

from hummingbot.connector.exchange.canonic import canonic_constants as CONSTANTS
from hummingbot.connector.exchange.canonic import canonic_web_utils as web_utils
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.logger import HummingbotLogger


class CanonicAPIOrderBookDataSource(OrderBookTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None
    POLL_INTERVAL = 2.0

    def __init__(self, trading_pairs: List[str], connector, domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._domain = domain
        self._rpc = web_utils.MegaETHRPCClient()

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        prices = {}
        mid = await self._fetch_mid()
        for pair in trading_pairs:
            prices[pair] = mid if mid else 0.0
        return prices

    async def _fetch_mid(self) -> float:
        try:
            data = web_utils.encode_get_mid_price()
            result = await self._rpc.eth_call(CONSTANTS.CLOB_CONTRACT, data)
            price, precision, _ = web_utils.decode_get_mid_price(result)
            if precision and precision > 0:
                return price / precision
            # If contract returns human-scale already
            if price > 1e6:
                return price / 1e18
            return price
        except Exception as e:
            self.logger().warning(f"getMidPrice failed: {e}")
            return 0.0

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        mid = await self._fetch_mid()
        # MVP synthetic thin book around mid (±1–5 bps) until getDepth ABI is locked
        tick = mid * 0.0001 if mid else 0.01  # 1 bps
        bids = [[str(mid - i * tick), "0.01"] for i in range(1, 6)] if mid else []
        asks = [[str(mid + i * tick), "0.01"] for i in range(1, 6)] if mid else []
        ts = time.time()
        return OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": int(ts * 1e3),
                "bids": bids,
                "asks": asks,
            },
            timestamp=ts,
        )

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        while True:
            try:
                for pair in self._trading_pairs:
                    msg = await self._order_book_snapshot(pair)
                    output.put_nowait(msg)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Order book snapshot error: {e}", exc_info=True)
            await asyncio.sleep(self.POLL_INTERVAL)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        # MAOB has no diff stream yet — snapshots only
        while True:
            await asyncio.sleep(3600)

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        while True:
            await asyncio.sleep(3600)
