"""Action 超时策略。"""

from __future__ import annotations

import asyncio
import inspect
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable
from typing import Optional

from ..errors import ActionTimeout


@dataclass(frozen=True, slots=True)
class TimeoutPolicy:
    seconds: Optional[float] = None

    def __post_init__(self) -> None:
        if self.seconds is not None and self.seconds <= 0:
            raise ValueError("seconds must be positive or None")

    def run(self, operation: Callable[[], Any]) -> Any:
        """同步执行并在返回后检查是否超过期限。

        同步 Python 调用无法安全地强制终止正在运行的线程，因此这里只做协作式
        超时检测；需要真正取消的长任务应使用异步 Action 或外部执行器。
        """
        started = time.monotonic()
        value = operation()
        self._check(time.monotonic() - started)
        return value

    async def run_async(self, operation: Callable[[], Awaitable[Any] | Any]) -> Any:
        started = time.monotonic()
        value = operation()
        if inspect.isawaitable(value):
            if self.seconds is None:
                value = await value
            else:
                try:
                    value = await asyncio.wait_for(value, timeout=self.seconds)
                except asyncio.TimeoutError as exc:
                    raise ActionTimeout(
                        f"action exceeded timeout of {self.seconds} seconds"
                    ) from exc
        self._check(time.monotonic() - started)
        return value

    def _check(self, elapsed: float) -> None:
        if self.seconds is not None and elapsed > self.seconds:
            raise ActionTimeout(
                f"action exceeded timeout of {self.seconds} seconds"
            )
