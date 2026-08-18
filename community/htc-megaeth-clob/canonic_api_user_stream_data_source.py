"""
User stream for Canonic MAOB.

MVP: polling-based (no live WS yet). Production path in COMPLETE.md
subscribes to wss://mainnet.megaeth.com/ws logs for the CLOB address.
"""

import asyncio
import logging
import time
from typing import Optional

from hummingbot.connector.exchange.canonic import canonic_constants as CONSTANTS
from hummingbot.connector.exchange.canonic.canonic_auth import CanonicAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger


class CanonicAPIUserStreamDataSource(UserStreamTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None
    POLL_INTERVAL = 5.0

    def __init__(self, auth: CanonicAuth, domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth = auth
        self._domain = domain
        self._last_recv_time: float = 0.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Placeholder poll loop. Emits heartbeat so the connector stays alive.
        Upgrade: eth_subscribe logs on CLOB_CONTRACT via WS_URL.
        """
        while True:
            try:
                self._last_recv_time = time.time()
                # Heartbeat — exchange reconciles via previewOrder polling
                output.put_nowait({"type": "heartbeat", "ts": self._last_recv_time})
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"User stream error: {e}", exc_info=True)
            await asyncio.sleep(self.POLL_INTERVAL)
