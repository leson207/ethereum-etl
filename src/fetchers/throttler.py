import asyncio
import time
from collections import deque
from typing import Deque

from src.logger import logger


class Throttler:
    def __init__(
        self,
        rate_limit: int,
        period: float,
        spacing: float = None,
        padding: float = 0.0,
    ):
        self.rate_limit = rate_limit
        self.period = period
        self.padding = padding
        self.spacing = spacing or (period / rate_limit)

        self._lock = asyncio.Lock()
        self._task_logs: Deque[float] = deque()

    def flush(self):
        """Remove timestamps older than the period window."""
        now = time.monotonic()
        while self._task_logs and now - self._task_logs[0] > self.period:
            self._task_logs.popleft()

    async def acquire(self):
        """Wait until it's safe to proceed with another request."""
        while True:
            self.flush()

            if len(self._task_logs) < self.rate_limit:
                # spacing request
                if self._task_logs:
                    duration = self.spacing - (time.monotonic() - self._task_logs[-1])
                    if duration > 0:
                        await asyncio.sleep(duration)

                self._task_logs.append(time.monotonic())
                break

            # Wait until the oldest timestamp exits the time window
            sleep_duration = self._task_logs[0] + self.period - time.monotonic()
            if sleep_duration > 0:
                logger.info(
                    f"Throttler sleeping for {sleep_duration + self.padding:.3f} seconds..."
                )
                await asyncio.sleep(sleep_duration + self.padding)

    async def __aenter__(self):
        async with self._lock:
            await self.acquire()

        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass
