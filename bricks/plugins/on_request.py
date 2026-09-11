"""可复用的请求准备、响应展示及成功检查插件。"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from bricks import signals
from bricks.proxies import Manager

from ._utils import condition, sync

if TYPE_CHECKING:
    from bricks.spider.context import Context

_LOGGER = logging.getLogger(__name__)  # 默认使用标准日志，不创建额外 Handler。

__all__ = ["After", "Before"]


class Before:
    """请求前的可复用插件。"""

    @staticmethod
    def set_proxy(context: Context, timeout: float | None = 30) -> None:
        """选择固定代理或当前 Slot 的代理，重试保留已经选中的地址。

        Args:
            context: 当前请求上下文，target 提供默认 proxy。
            timeout: 代理供应源的最大等待秒数，默认 30。

        Raises:
            RuntimeError: 代理管理器未获得 Slot。
            TypeError: 认证回调不返回 None。
        """
        if context.request.proxy is not None:
            return
        source = context.target.proxy
        if isinstance(source, Manager):
            if context.slot is None:
                raise RuntimeError("managed proxies require a Slot")
            proxy = source.get(context.slot, timeout=timeout)
            if proxy.auth is not None and sync(proxy.auth, context.request) is not None:
                raise TypeError("proxy auth callback must return None")
            context.request.proxy = proxy.proxy
            source.use(context.slot)
        else:
            context.request.proxy = source

    @staticmethod
    def fake_ua(
        context: Context, *, factory: Callable[[], str], overwrite: bool = False
    ) -> None:
        """显式生成 User-Agent，默认保留已有值及同一请求的重试值。

        Args:
            context: 当前请求上下文。
            factory: 同步 UA 工厂，可组合现有 UA 库或 random.choice。
            overwrite: 是否替换已有 User-Agent，默认 False。

        Raises:
            TypeError: 工厂未返回非空字符串或 overwrite 不是布尔值。
        """
        if type(overwrite) is not bool:
            raise TypeError("overwrite must be boolean")
        if not overwrite and context.request.headers.get("User-Agent"):
            return
        value = sync(factory)
        if not isinstance(value, str) or not value.strip():
            raise TypeError("UA factory must return a non-empty string")
        context.request.headers["User-Agent"] = value


class After:
    """响应后的可复用插件。"""

    @staticmethod
    def conditional_scripts(
        context: Context,
        *,
        rules: Sequence[tuple[Callable[[Context], bool], Callable[[Context], None]]],
        first: bool = False,
    ) -> None:
        """按声明顺序执行命中条件的动作，不执行 Request 中的脚本文本。

        Args:
            context: 显式领域上下文。
            rules: 同步条件与同步动作的二元组序列。
            first: True 只执行首个命中动作，默认执行全部命中项。

        Raises:
            TypeError: 条件、动作或 first 违反契约。
        """
        if type(first) is not bool:
            raise TypeError("first must be boolean")
        for match, action in rules:
            if condition(match, context):
                if sync(action, context) is not None:
                    raise TypeError("conditional action must return None")
                if first:
                    return

    @staticmethod
    def show_response(
        context: Context, handle: Callable[[str], Any] | None = None, fmt: int = 0
    ) -> None:
        """展示当前响应信息，保留 handle 和 fmt 参数。

        Args:
            context: 当前响应上下文。
            handle: 同步日志输出函数，None 使用模块 logger.debug。
            fmt: 0 输出单行摘要，1 输出多行元数据。

        Raises:
            ValueError: 格式不是 0 或 1。
            TypeError: 输出函数返回 Awaitable。
        """
        if type(fmt) is not int or fmt not in (0, 1):
            raise ValueError("fmt must be 0 or 1")
        request, response = context.request, context.response
        fields = [
            request.method,
            f"attempt={context.attempt}",
            f"status={response.status_code}",
            f"bytes={len(response.content)}",
            response.url or request.url,
        ]
        message = " ".join(fields) if fmt == 0 else "\n".join(fields)
        result = (_LOGGER.debug if handle is None else handle)(message)
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError("show_response handle must be synchronous")

    @staticmethod
    def bypass(context: Context, ok: Callable[[Context], bool] | None = None) -> None:
        """通过正常返回或 signals 控制响应后的阶段节点。

        默认接受 2xx，重试传输失败、429 和 5xx，其余状态拒绝。自定义 ok
        返回 False 时抛出 Retry，True 抛出 Pass 跳过插槽剩余默认校验。

        Args:
            context: 当前响应上下文。
            ok: 可复用的同步成功判断，None 使用默认 HTTP 策略。

        Raises:
            TypeError: ok 不是同步布尔判断。
            signals.Retry: 可重试响应，由图内预算控制。
            signals.Failure: 响应被拒绝。
            signals.Pass: 自定义条件接受响应，停止当前插槽剩余回调。
        """
        if ok is not None:
            value = ok(context)
            if type(value) is not bool:
                if inspect.iscoroutine(value):
                    value.close()
                raise TypeError("ok must return bool synchronously")
            if value:
                raise signals.Pass()
            raise signals.Retry()
        status = context.response.status_code
        if 200 <= status < 300:
            return
        elif status == -1 or status == 429 or 500 <= status < 600:
            raise signals.Retry()
        else:
            raise signals.Failure(f"response rejected: status={status}")
