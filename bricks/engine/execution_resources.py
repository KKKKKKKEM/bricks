"""Execution 的可替换输出存储与等待通知能力。"""

from __future__ import annotations

import asyncio
from threading import Condition
from typing import Protocol, runtime_checkable

from .core import Output


@runtime_checkable
class OutputStore(Protocol):
    """单次执行的追加式输出存储；索引稳定，已接受输出必须可重放。"""

    def __len__(self) -> int: ...

    def __getitem__(self, index: int) -> Output: ...

    def append(self, output: Output) -> None:
        """原子追加；抛错表示未接受该项输出。"""


class MemoryOutputStore:
    def __init__(self) -> None:
        self._items: list[Output] = []

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, index: int) -> Output:
        return self._items[index]

    def append(self, output: Output) -> None:
        self._items.append(output)


@runtime_checkable
class ExecutionNotifier(Protocol):
    """线程安全的版本化通知；等待旧版本必须立即完成，避免丢失唤醒。"""

    @property
    def version(self) -> int: ...

    def notify(self) -> None:
        """递增版本并唤醒全部同步和异步等待者。"""

    def wait(self, version: int, timeout: float | None = None) -> None: ...

    async def wait_async(self, version: int) -> None: ...


class LocalExecutionNotifier:
    """使用 Condition 和事件循环 Future 唤醒等待者，无轮询或等待线程。"""

    def __init__(self) -> None:
        self._condition = Condition()
        self._version = 0
        self._waiters: dict[asyncio.Future[None], asyncio.AbstractEventLoop] = {}

    @property
    def version(self) -> int:
        with self._condition:
            return self._version

    def notify(self) -> None:
        with self._condition:
            self._version += 1
            self._condition.notify_all()
            waiters = tuple(self._waiters.items())
            self._waiters.clear()
        for future, loop in waiters:
            try:
                loop.call_soon_threadsafe(self._wake, future)
            except RuntimeError:
                # A waiting application's event loop may already have closed.
                pass

    def wait(self, version: int, timeout: float | None = None) -> None:
        with self._condition:
            self._condition.wait_for(lambda: self._version != version, timeout)

    async def wait_async(self, version: int) -> None:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        with self._condition:
            if self._version != version:
                return
            self._waiters[future] = loop
        try:
            await future
        finally:
            with self._condition:
                self._waiters.pop(future, None)

    @staticmethod
    def _wake(future: asyncio.Future[None]) -> None:
        if not future.done():
            future.set_result(None)
