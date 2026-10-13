"""按声明顺序执行一个领域插槽，保留同步线程与原生异步等待边界。"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Iterable
from functools import partial
from typing import Any

from bricks.hooks import Task

from ._calls import invoke


def _none(value: Any) -> None:
    """确认普通 Hook 返回 None。

    Args:
        value: Hook 结果。

    Raises:
        TypeError: 返回值违反约定。
    """
    if value is not None:
        raise TypeError("domain hooks must return None or raise a flow signal")


def run_hooks(
    owner: object, tasks: Iterable[Task], context: Any, *, allow_await: bool = True
) -> None | Awaitable[None]:
    """执行一个插槽；signals 原样退出整组，交给节点或边界处理。

    Args:
        owner: 当前 Spider，用于生命周期重入保护。
        tasks: 按 index 排序的固定声明。
        context: 当前图内或边界上下文。
        allow_await: 图内允许原生异步；非图边界只接受同步回调。

    Returns:
        同步完成时为 None，遇到异步 Hook 时返回 Awaitable。

    Raises:
        TypeError: 条件或普通返回值违反协议。
        BaseException: Hook 错误、流程信号和取消继续传播。
    """
    iterator = iter(tasks)

    def call(task: Task) -> Any:
        """检查条件并在当前线程启动回调。

        Args:
            task: 当前声明。

        Returns:
            None 或回调 Awaitable。
        """
        if task.match is not None:
            match = partial(
                task.match,
                *((context,) if task._match_with_context is not False else ()),
            )
            value = invoke(
                owner,
                lambda _: match(),
                context,
                lambda result: result,
                allow_await=False,
            )
            if type(value) is not bool:
                raise TypeError("hook condition must return bool synchronously")
            if not value:
                return None
        callback = partial(
            task.func,
            *((context,) if task._with_context is not False else ()),
            *task.args,
            **task.kwargs,
        )
        return invoke(
            owner, lambda _: callback(), context, _none, allow_await=allow_await
        )

    for task in iterator:
        value = call(task)
        if inspect.isawaitable(value):

            async def finish(first: Awaitable[None]) -> None:
                """等待当前回调，再按同一顺序推进剩余回调。

                Args:
                    first: 已启动的异步回调。
                """
                await first
                for following in iterator:
                    result = call(following)
                    if inspect.isawaitable(result):
                        await result

            return finish(value)
    return None
