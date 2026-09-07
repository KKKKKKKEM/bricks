"""Execution 的可替换输出存储与等待通知能力。"""

from __future__ import annotations

import asyncio
from threading import Condition
from typing import Protocol, runtime_checkable

from .core import Output


@runtime_checkable
class OutputStore(Protocol):
    """单次执行的追加式输出存储；索引稳定，已接受输出必须可重放。"""

    def __len__(self) -> int:
        """返回当前容器中保存的条目数量。

        Returns:
            当前容器条目数量。
        """
        ...

    def __getitem__(self, index: int) -> Output:
        """按追加索引读取已接受的 Output。

        Args:
            index: 已追加输出的整数索引。

        Returns:
            指定追加索引对应的 Output。
        """
        ...

    def append(self, output: Output) -> None:
        """原子追加；抛错表示未接受该项输出。

        Args:
            output: 需要校验、保存或交付的一项 Output。
        """


class MemoryOutputStore:
    """按追加顺序保留并重放输出的内存存储。

    Attributes:
        _items: 按追加顺序保存的输出条目。
    """

    def __init__(self) -> None:
        """创建按追加顺序保留输出的独立内存存储。"""

        self._items: list[Output] = []

    def __len__(self) -> int:
        """返回当前容器中保存的条目数量。

        Returns:
            当前容器条目数量。
        """

        return len(self._items)

    def __getitem__(self, index: int) -> Output:
        """按追加索引读取已保存的 Output。

        Args:
            index: 已追加输出的整数索引。

        Returns:
            指定追加索引对应的 Output。
        """

        return self._items[index]

    def append(self, output: Output) -> None:
        """按追加顺序保存一项输出。

        Args:
            output: 需要校验、保存或交付的一项 Output。
        """

        self._items.append(output)


@runtime_checkable
class ExecutionNotifier(Protocol):
    """线程安全的版本化通知；等待旧版本必须立即完成，避免丢失唤醒。"""

    @property
    def version(self) -> int:
        """返回通知器当前的单调递增版本。

        Returns:
            当前通知版本，可用于识别是否发生了新的通知。
        """
        ...

    def notify(self) -> None:
        """递增版本并唤醒全部同步和异步等待者。"""

    def wait(self, version: int, timeout: float | None = None) -> None:
        """等待通知版本变化或等待时限结束。

        Args:
            version: 等待开始前观察到的通知版本。
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
        """
        ...

    async def wait_async(self, version: int) -> None:
        """异步等待通知版本变化，不占用同步等待线程。

        Args:
            version: 等待开始前观察到的通知版本。
        """
        ...


class LocalExecutionNotifier:
    """使用 Condition 和事件循环 Future 唤醒等待者，无轮询或等待线程。

    Attributes:
        _condition: 协调共享状态访问及同步等待的锁或条件变量。
        _version: 每次通知递增的版本，避免等待方丢失唤醒。
        _waiters: 按异步等待方登记的事件循环和 Future。
    """

    def __init__(self) -> None:
        """创建版本化通知器及独立的同步和异步等待集合。"""

        self._condition = Condition()
        self._version = 0
        self._waiters: dict[asyncio.Future[None], asyncio.AbstractEventLoop] = {}

    @property
    def version(self) -> int:
        """返回通知器当前的单调递增版本。

        Returns:
            当前通知版本，可用于识别是否发生了新的通知。
        """

        with self._condition:
            return self._version

    def notify(self) -> None:
        """推进通知版本并唤醒同步及异步等待方。"""

        with self._condition:
            self._version += 1
            self._condition.notify_all()
            waiters = tuple(self._waiters.items())
            self._waiters.clear()
        for future, loop in waiters:
            try:
                loop.call_soon_threadsafe(self._wake, future)
            except RuntimeError:
                # 等待方应用的事件循环可能已经关闭。
                pass

    def wait(self, version: int, timeout: float | None = None) -> None:
        """等待通知版本变化或等待时限结束。

        Args:
            version: 等待开始前观察到的通知版本。
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
        """

        with self._condition:
            self._condition.wait_for(lambda: self._version != version, timeout)

    async def wait_async(self, version: int) -> None:
        """异步等待通知版本变化，不占用同步等待线程。

        Args:
            version: 等待开始前观察到的通知版本。
        """

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
        """在等待方事件循环中完成通知 Future。

        Args:
            future: 线程池或事件循环提交返回的结果句柄。
        """

        if not future.done():
            future.set_result(None)
