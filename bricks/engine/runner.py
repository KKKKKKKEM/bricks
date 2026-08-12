"""在同步 Engine 边界内执行同步和异步调用。"""

from __future__ import annotations

import asyncio
import inspect
from threading import Event as ThreadEvent
from threading import RLock, Thread, current_thread
from typing import Any


class LocalRunner:
    """同步调用普通函数，并在常驻后台 event loop 上解析 awaitable。"""

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._ready = ThreadEvent()
        self._lock = RLock()
        self._closed = False
        self._thread = Thread(
            target=self._run_loop,
            name="bricks-async-runner",
            daemon=True,
        )
        self._thread.start()
        self._ready.wait()

    def resolve(self, value: Any) -> Any:
        """原样返回同步值，或同步等待后台 loop 上的 awaitable。"""

        if not inspect.isawaitable(value):
            return value
        with self._lock:
            if self._closed:
                if inspect.iscoroutine(value):
                    value.close()
                raise RuntimeError("runner is closed")
            if current_thread() is self._thread:
                if inspect.iscoroutine(value):
                    value.close()
                raise RuntimeError(
                    "cannot synchronously resolve an awaitable on the runner thread"
                )
            future = asyncio.run_coroutine_threadsafe(
                self._await(value), self._loop
            )
        return future.result()

    def close(self) -> None:
        """停止后台 loop，并等待专用线程退出。"""

        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()

    async def _await(self, value: Any) -> Any:
        return await value

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._ready.set()
        try:
            self._loop.run_forever()
        finally:
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            self._loop.close()
