"""可直接 use 的请求准备、动态脚本、响应展示及成功检查插件。"""

from __future__ import annotations

import inspect
import logging
import random
import threading
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit, urlunsplit

from bricks import hooks, signals
from bricks.proxies import Manager

from . import scripts
from ._utils import sync

if TYPE_CHECKING:
    from bricks.spider.context import Context

_LOGGER = logging.getLogger(__name__)  # 默认使用标准日志，不创建额外 Handler。
_USER_AGENTS = {
    "chrome": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "edge": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
    ),
    "mobile": (
        "Mozilla/5.0 (Linux; Android 13; Mobile) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"
    ),
    "firefox": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) "
        "Gecko/20100101 Firefox/133.0"
    ),
    "safari": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15"
    ),
    "opera": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 OPR/116.0.0.0"
    ),
    "wechat": (
        "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Version/4.0 Chrome/131.0.0.0 Mobile "
        "Safari/537.36 MicroMessenger/8.0"
    ),
}  # 内置常用 UA；复杂生成策略通过 factory 注入。
_USER_AGENTS["pc"] = _USER_AGENTS["chrome"]
_USER_AGENTS["android"] = _USER_AGENTS["mobile"]

__all__ = ["After", "Before"]


def _context() -> Context:
    """取得当前消费 Context。

    Returns:
        当前 Hook 正在处理的消费 Context。

    Raises:
        RuntimeError: 插件不在消费回调中运行。
    """
    from bricks.spider.context import Context

    return Context.get_context()


def _script_match(value: Any, context: Context) -> bool:
    """使用动态脚本引擎计算一个业务条件。

    Args:
        value: 字符串、callable 或普通条件值。
        context: 当前消费 Context。

    Returns:
        条件的布尔结果。
    """
    scope = scripts.build_namespace(context)
    scripts.Generator([(scripts.Type.define, ("MATCHED", value))]).run(scope)
    return bool(scope["MATCHED"])


def _safe_url(value: str | None) -> str:
    """隐藏诊断 URL 中的凭据、查询值和片段。

    Args:
        value: 原始请求或代理 URL。

    Returns:
        不含认证信息和敏感参数值的 URL；无法解析时返回固定占位，不回显原值。
    """
    if not value:
        return ""
    candidate = value if "://" in value or value.startswith("//") else "//" + value
    try:
        parts = urlsplit(candidate)
    except ValueError:
        return "[INVALID URL]"
    query = "[REDACTED]" if parts.query else ""
    netloc = parts.netloc.rsplit("@", 1)[-1]
    return urlunsplit((parts.scheme, netloc, parts.path, query, ""))


class Before:
    """请求前可直接注册的插件集合。"""

    @staticmethod
    @hooks.current
    def set_proxy(timeout: float | None = 30) -> None:
        """选择固定代理或当前 Slot 的代理。

        Args:
            timeout: 代理供应源最大等待秒数，默认 30 秒。

        Raises:
            RuntimeError: 代理管理器未获得 Slot。
            TypeError: 认证回调返回非 None。
        """
        context = _context()
        if context.request.proxy is not None:
            return
        reuse = context.options.get("crawler.session.reuse", True)
        if (
            reuse is True
            and context.slot is not None
            and "crawler.session.proxy" in context.slot
        ):
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
    @hooks.current
    def fake_ua(
        factory: Callable[[], str] | None = None, *, overwrite: bool = False
    ) -> None:
        """展开 User-Agent 的 @ 预设，也允许注入任意同步 UA 工厂。

        Args:
            factory: 可选同步 UA 工厂；缺省解析请求头中的 @chrome 等预设。
            overwrite: 工厂模式是否替换已有普通 UA，默认 False。

        Raises:
            ValueError: @ 预设不存在。
            TypeError: 工厂结果或 overwrite 非法。
        """
        if type(overwrite) is not bool:
            raise TypeError("overwrite must be boolean")
        request = _context().request
        raw = request.headers.get("User-Agent", "")
        if factory is not None:
            if raw and not overwrite:
                return
            value = sync(factory)
        elif raw.startswith("@"):
            name = raw[1:].split("(", 1)[0]
            if name in {"random", "get"}:
                value = random.choice(tuple(_USER_AGENTS.values()))
            else:
                try:
                    value = _USER_AGENTS[name]
                except KeyError as exc:
                    raise ValueError(f"unknown User-Agent preset: {name}") from exc
        else:
            return
        if not isinstance(value, str) or not value.strip():
            raise TypeError("UA factory must return a non-empty string")
        request.headers["User-Agent"] = value


class After:
    """响应后可直接注册的插件集合。"""

    @staticmethod
    @hooks.current
    def conditional_scripts(
        rules: Mapping[Any, Any] | None = None,
        *,
        namespace: Mapping[str, Any] | None = None,
    ) -> None:
        """执行全部命中的动态条件动作，缺省读取执行配置 crawler.response.scripts。

        Args:
            rules: 条件到动作的有序映射；None 从当前 execution options 读取。
            namespace: 追加到标准脚本命名空间的可信对象。

        Raises:
            TypeError: rules 不是映射。
            BaseException: 脚本错误和领域信号原样传播。
        """
        context = _context()
        selected = (
            context.options.get("crawler.response.scripts", {})
            if rules is None
            else rules
        )
        if not isinstance(selected, Mapping):
            raise TypeError("conditional scripts must be a mapping")
        if not selected:
            return
        scope = scripts.build_namespace(context, namespace)
        scripts.Generator([(scripts.Type.condition, selected)]).run(scope)

    @staticmethod
    @hooks.current
    def show_response(handle: Callable[[str], Any] | None = None, fmt: int = 0) -> None:
        """展示已脱敏的响应信息，参数顺序与 dev 保持一致。

        Args:
            handle: 同步日志输出函数，None 使用模块 logger.debug。
            fmt: 0 输出单行摘要，1 输出多行元数据。

        Raises:
            ValueError: fmt 不是 0 或 1。
            TypeError: 输出函数返回 Awaitable。
        """
        if type(fmt) is not int or fmt not in (0, 1):
            raise ValueError("fmt must be 0 or 1")
        context = _context()
        request, response = context.request, context.response
        url = _safe_url(response.url or request.url)
        proxy = _safe_url(request.proxy)
        if fmt == 0:
            message = " ".join(
                [
                    request.method,
                    f"attempt={context.attempt}",
                    f"status={response.status_code}",
                    f"proxy={proxy or threading.current_thread().name}",
                    f"bytes={response.size()}",
                    url,
                ]
            )
        else:
            message = "\n".join(
                [
                    f"url: {url}",
                    f"method: {request.method}",
                    f"headers: {list(request.headers)}",
                    f"body: {'[REDACTED]' if request.body is not None else None}",
                    f"status={response.status_code}",
                    f"proxy: {proxy}",
                    f"thread: {threading.current_thread().name}",
                ]
            )
        result = (_LOGGER.debug if handle is None else handle)(message)
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError("show_response handle must be synchronous")

    @staticmethod
    @hooks.current
    def bypass(ok: Any = None) -> None:
        """按 dev 形式执行默认、表达式、映射或 callable 成功判断。

        Args:
            ok: 检查条件；缺省读取 context.ok_rule（执行配置 crawler.response.ok，form 另有段级 ok），
                仍缺省时采用默认状态码规则。

        Raises:
            signals.Retry: 检查未通过。
            BaseException: 自定义动作、脚本或其他领域信号原样传播。
        """
        context = _context()
        selected = context.ok_rule if ok is None else ok
        if selected is None:
            status = context.response.status_code
            if 200 <= status < 300:
                return
            if status == -1 or status == 429 or 500 <= status < 600:
                raise signals.Retry()
            raise signals.Failure(f"response rejected: status={status}")
        elif isinstance(selected, Mapping):
            for match, action in selected.items():
                if not _script_match(match, context):
                    continue
                if inspect.isclass(action) and issubclass(action, signals.Signal):
                    if action is signals.Pass:
                        return
                    raise action()
                if isinstance(action, str):
                    scope = scripts.build_namespace(context)
                    scripts.Generator([(scripts.Type.code, action)]).run(scope)
                elif callable(action):
                    scope = scripts.build_namespace(context)
                    scripts.Generator([(scripts.Type.code, action)]).run(scope)
                else:
                    raise TypeError(
                        "bypass actions must be signals, scripts or callables"
                    )
                return
            status = context.response.status_code
            if 200 <= status < 300:
                accepted = True
            elif status == -1 or status == 429 or 500 <= status < 600:
                raise signals.Retry()
            else:
                raise signals.Failure(f"response rejected: status={status}")
        else:
            accepted = _script_match(selected, context)
        if not accepted:
            raise signals.Retry()
