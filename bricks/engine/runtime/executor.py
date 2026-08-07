"""Machine 使用的 Action 执行协议。"""

from __future__ import annotations

import inspect
from typing import Any, Protocol

from ..errors import AsyncActionRequired
from ..policies.cancellation import CancellationToken
from ..policies.timeout import TimeoutPolicy


class ActionExecutor(Protocol):
    def execute(self, action: Any, context: Any, event: Any) -> Any: ...

    async def execute_async(self, action: Any, context: Any, event: Any) -> Any: ...


class InlineExecutor:
    """引擎核心使用的最小进程内执行器。"""

    def execute(self, action: Any, context: Any, event: Any) -> Any:
        value = action(context, event)
        if inspect.isawaitable(value):
            close = getattr(value, "close", None)
            close and close()
            raise AsyncActionRequired(
                f"async action requires execute_async(): {action!r}"
            )
        return value

    async def execute_async(self, action: Any, context: Any, event: Any) -> Any:
        value = action(context, event)
        if inspect.isawaitable(value):
            return await value
        return value


class PolicyExecutor:
    """在基础执行器外组合取消和超时策略。"""

    def __init__(
        self,
        executor: ActionExecutor,
        *,
        cancellation: CancellationToken | None = None,
        timeout: TimeoutPolicy | None = None,
    ):
        self.executor = executor
        self.cancellation = cancellation
        self.timeout = timeout

    def execute(self, action: Any, context: Any, event: Any) -> Any:
        self._check_cancelled()
        operation = lambda: self.executor.execute(action, context, event)
        value = self.timeout.run(operation) if self.timeout else operation()
        self._check_cancelled()
        return value

    async def execute_async(self, action: Any, context: Any, event: Any) -> Any:
        self._check_cancelled()
        operation = lambda: self.executor.execute_async(action, context, event)
        if self.timeout:
            value = await self.timeout.run_async(operation)
        else:
            value = operation()
            if inspect.isawaitable(value):
                value = await value
        self._check_cancelled()
        return value

    def _check_cancelled(self) -> None:
        if self.cancellation is not None:
            self.cancellation.raise_if_cancelled()
