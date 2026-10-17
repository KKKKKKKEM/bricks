"""保留同步调用线程与异步等待语义的业务调用适配。"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import Any, TypeVar

from .context import CurrentContext, activate_context, reset_context

_T = TypeVar("_T")  # 保留调用后的转换结果类型。
_CALLER: ContextVar[object | None] = ContextVar(
    "bricks.spider.caller", default=None
)  # 仅防止生命周期回调重入，不保存请求数据。


def external(owner: object) -> None:
    """拒绝从当前 Spider 的业务回调中重入阻塞生命周期方法。

    Args:
        owner: 当前 Spider。

    Raises:
        RuntimeError: 当前调用来自自身业务或 Hook。
    """
    if _CALLER.get() is owner:
        raise RuntimeError("spider lifecycle methods cannot run inside its callbacks")


def invoke(
    owner: object,
    callback: Callable[..., Any],
    context: CurrentContext,
    finish: Callable[[Any], _T],
    *,
    allow_await: bool = True,
) -> _T | Awaitable[_T]:
    """在调用线程执行同步函数，仅将 Awaitable 交给引擎等待。

    Args:
        owner: 拥有回调的 Spider。
        callback: 单参数业务回调。
        context: 当前领域上下文。
        finish: 返回值的同步类型校验与输出转换。
        allow_await: 是否接受 Awaitable，条件回调设为 False。

    Returns:
        同步转换结果或等待异步结果的 Awaitable。

    Raises:
        BaseException: 回调、取消或返回值转换失败。
    """
    caller_token = _CALLER.set(owner)
    context_token = activate_context(context)
    try:
        value = callback(context)
    finally:
        reset_context(context_token)
        _CALLER.reset(caller_token)
    if not inspect.isawaitable(value):
        return finish(value)
    if not allow_await:
        if inspect.iscoroutine(value):
            value.close()
        raise TypeError("callback must return synchronously")

    async def awaited() -> _T:
        """在异步函数真正运行时保留生命周期重入保护。

        Returns:
            经过同步转换的结果。
        """
        caller_token = _CALLER.set(owner)
        context_token = activate_context(context)
        try:
            return finish(await value)
        finally:
            reset_context(context_token)
            _CALLER.reset(caller_token)

    return awaited()
