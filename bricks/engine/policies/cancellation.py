"""协作式取消原语。"""

from __future__ import annotations

import threading
import asyncio

from ..errors import CancellationError


class CancellationToken:
    def __init__(self) -> None:
        self._event = threading.Event()
        self._lock = threading.RLock()
        self._async_waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = []

    def cancel(self) -> None:
        self._event.set()
        with self._lock:
            waiters = tuple(self._async_waiters)
        for loop, waiter in waiters:
            loop.call_soon_threadsafe(waiter.set)

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    def raise_if_cancelled(self) -> None:
        if self.cancelled:
            raise CancellationError("operation cancelled")

    async def wait_async(self) -> None:
        """等待取消信号，供异步编排器传播取消。"""
        if self.cancelled:
            return
        loop = asyncio.get_running_loop()
        waiter = asyncio.Event()
        with self._lock:
            if self.cancelled:
                return
            self._async_waiters.append((loop, waiter))
        try:
            await waiter.wait()
        finally:
            with self._lock:
                try:
                    self._async_waiters.remove((loop, waiter))
                except ValueError:
                    pass
