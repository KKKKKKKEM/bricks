"""为可信业务配置提供动态条件、动作、成功判断与翻页插件。"""

from __future__ import annotations

import inspect
import logging
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from bricks import hooks, signals

if TYPE_CHECKING:
    from bricks.spider.context import Context

from ._utils import seed

_LOGGER = logging.getLogger(__name__)  # 脚本默认日志目标，不创建额外 Handler。


class Type(str, Enum):
    """动态 flow 的步骤类别，与 dev 插件配置名称保持一致。"""

    define = "define"  # 定义一个供后续步骤使用的变量。
    choice = "choice"  # 按顺序只执行首个命中的分支。
    condition = "condition"  # 按顺序执行所有命中的分支。
    code = "code"  # 直接执行一个或多个动作。


@lru_cache(maxsize=512)
def _compile(source: str, mode: str) -> Any:
    """编译并缓存可信业务表达式或动作代码。

    Args:
        source: Python 表达式或语句。
        mode: compile 的 eval 或 exec 模式。

    Returns:
        可重复执行的 code 对象。

    Raises:
        SyntaxError: 脚本语法非法。
    """
    return compile(source, "<bricks.plugins.scripts>", mode)


def _items(value: Any) -> tuple[Any, ...]:
    """把单项或序列统一为保持顺序的元组。

    Args:
        value: 单项、序列或 None。

    Returns:
        None 对应空元组；字符串、字节和映射保持为单项。
    """
    if value is None:
        return ()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(value)
    return (value,)


def _invoke(function: Callable[..., Any], context: Context, *args: Any) -> Any:
    """调用显式接收 Context 或使用 get_context 的同步函数。

    Args:
        function: 条件、动作或翻页工厂。
        context: 当前消费 Context。
        args: Context 之后的业务参数。

    Returns:
        函数的同步结果。

    Raises:
        TypeError: 签名无法绑定或返回异步对象。
    """
    if getattr(function, "__bricks_current_context__", False):
        values = args
    else:
        try:
            signature = inspect.signature(function)
        except (TypeError, ValueError):
            values = (context, *args)
        else:
            try:
                signature.bind(context, *args)
            except TypeError as with_context:
                try:
                    signature.bind(*args)
                except TypeError:
                    raise with_context from None
                values = args
            else:
                values = (context, *args)
    result = function(*values)
    if inspect.isawaitable(result) or inspect.isasyncgen(result):
        if inspect.iscoroutine(result):
            result.close()
        raise TypeError("script callbacks must be synchronous")
    return result


def build_namespace(
    context: Context, values: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """建立与 dev 动态插件相近的当前执行命名空间。

    Args:
        context: 当前消费 Context。
        values: 调用方追加或覆盖的可信对象。

    Returns:
        包含 context、request、response、items、seeds、signals 和 logger 的字典。
    """

    def available(name: str) -> Any:
        """读取当前阶段已经产生的领域值。

        Args:
            name: Context 公开属性名。

        Returns:
            当前值；尚未进入对应阶段时为 None。
        """
        try:
            return getattr(context, name)
        except RuntimeError:
            return None

    request = available("request")
    response = available("response")
    items = available("items")
    result: dict[str, Any] = {
        "context": context,
        "ctx": context,
        "request": request,
        "response": response,
        "items": items,
        "seeds": context.seeds,
        "signals": signals,
        "logger": _LOGGER,
        "time": time,
    }
    if values is not None:
        result.update(values)
    return result


def _evaluate(value: Any, scope: dict[str, Any], context: Context) -> Any:
    """在当前命名空间计算表达式、条件函数或普通值。

    Args:
        value: 字符串表达式、同步函数或普通值。
        scope: 可被 define 和代码更新的命名空间。
        context: 当前消费 Context。

    Returns:
        原值或计算结果。
    """
    if isinstance(value, str):
        return eval(_compile(value, "eval"), scope)
    if callable(value):
        return _invoke(value, context)
    return value


def _matches(value: Any, scope: dict[str, Any], context: Context) -> bool:
    """按 dev 的 and 语义计算一组条件。

    Args:
        value: 单个条件或条件序列。
        scope: 当前脚本命名空间。
        context: 当前消费 Context。

    Returns:
        空条件为 True，否则为所有条件结果的布尔合取。
    """
    return all(bool(_evaluate(item, scope, context)) for item in _items(value))


def _actions(value: Any, scope: dict[str, Any], context: Context) -> None:
    """按声明顺序执行字符串或 callable 动作。

    Args:
        value: 单个动作或动作序列。
        scope: 当前脚本命名空间。
        context: 当前消费 Context。

    Raises:
        TypeError: callable 动作返回非 None。
    """
    for action in _items(value):
        if isinstance(action, str):
            exec(_compile(action, "exec"), scope)
        elif callable(action):
            if _invoke(action, context) is not None:
                raise TypeError("script actions must return None")
        else:
            raise TypeError("script actions must be strings or synchronous callables")


@dataclass
class Generator:
    """按 define、choice、condition 和 code 解释可信 flow。

    Attributes:
        flows: 按声明顺序执行的步骤及其值。
    """

    flows: Sequence[tuple[Type | str, Any]]

    def run(self, values: dict[str, Any]) -> dict[str, Any]:
        """在给定命名空间执行 flow，并保留定义结果。

        Args:
            values: 调用方持有的可变可信命名空间。

        Returns:
            执行后的同一命名空间。

        Raises:
            ValueError: 步骤类别不存在或 define 结构非法。
            TypeError: 条件或动作回调违反同步契约。
        """
        from bricks.spider.context import Context

        context = values.get("context")
        if not isinstance(context, Context):
            raise TypeError("script namespace requires the current Context")
        for raw_type, value in self.flows:
            try:
                kind = Type(raw_type)
            except ValueError as exc:
                raise ValueError(f"unsupported script flow type: {raw_type}") from exc
            if kind is Type.code:
                _actions(value, values, context)
            elif kind is Type.define:
                if (
                    not isinstance(value, (tuple, list))
                    or len(value) != 2
                    or not isinstance(value[0], str)
                    or not value[0].isidentifier()
                ):
                    raise ValueError("define requires a variable name and conditions")
                values[value[0]] = _matches(value[1], values, context)
            else:
                if not isinstance(value, Mapping):
                    raise TypeError("choice and condition flows require a mapping")
                for match, actions in value.items():
                    if _matches(match, values, context):
                        _actions(actions, values, context)
                        if kind is Type.choice:
                            break
        return values


@hooks.current
def is_success(
    match: Any,
    pre: Any = None,
    post: Any = None,
    flow: Mapping[Any, Any] | None = None,
    *,
    namespace: Mapping[str, Any] | None = None,
) -> None:
    """运行成功条件及前后脚本，参数顺序与 dev 插件保持一致。

    Args:
        match: 字符串、callable 或其序列；多个条件使用 and 语义。
        pre: 判断前执行的字符串或 callable 动作。
        post: 判断后执行的字符串或 callable 动作。
        flow: 有序条件到动作映射；缺省在失败时抛出 Retry。
        namespace: 追加到标准脚本命名空间的可信对象。

    Raises:
        signals.Retry: 缺省条件不通过。
        BaseException: 脚本、回调和其他领域信号原样传播。
    """
    from bricks.spider.context import Context

    context = Context.get_context()
    scope = build_namespace(context, namespace)
    selected = dict(flow or {})
    selected.setdefault("not ISPASS", "raise signals.Retry")
    Generator(
        [
            (Type.code, pre),
            (Type.define, ("ISPASS", match)),
            (Type.code, post),
            (Type.choice, selected),
        ]
    ).run(scope)


@hooks.current
def turn_page(
    match: Any,
    pre: Any = None,
    post: Any = None,
    flow: Mapping[Any, Any] | None = None,
    key: str = "page",
    action: str | Callable[..., dict[str, Any]] = "+1",
    call_later: bool = False,
    success: bool = False,
    *,
    namespace: Mapping[str, Any] | None = None,
) -> None:
    """按旧参数顺序判断并提交下一页，交付仍使用新队列契约。

    Args:
        match: 字符串、callable 或条件序列。
        pre: 判断前动作。
        post: 下一页建立后的动作。
        flow: 自定义条件到动作的首项命中映射。
        key: 整数页码字段，默认 page。
        action: 旧式页码运算片段，或返回完整下一页种子的 callable。
        call_later: True 持久投递独立 Work；False 在当前 Slot 中继续。
        success: 执行 flow 后是否提前成功结束当前任务。
        namespace: 追加到标准脚本命名空间的可信对象。

    Raises:
        ValueError: key、action 或布尔参数非法。
        signals.Success: success 为 True。
        BaseException: 动态脚本、提交及其他信号原样传播。
    """
    if not isinstance(key, str) or not key:
        raise ValueError("key must be a non-empty string")
    if type(call_later) is not bool or type(success) is not bool:
        raise ValueError("call_later and success must be boolean")
    from bricks.spider.context import Context

    context = Context.get_context()
    scope = build_namespace(context, namespace)
    scope["call_later"] = call_later
    _actions(pre, scope, context)
    scope["ISPASS"] = _matches(match, scope, context)
    if scope["ISPASS"]:
        if callable(action):
            next_seeds = seed(_invoke(action, context))
        elif isinstance(action, str) and action.strip():
            current = context.seeds.get(key)
            value = eval(_compile("value " + action, "eval"), {"value": current})
            next_seeds = seed({**context.seeds, key: value})
        else:
            raise ValueError("action must be a non-empty expression or seed factory")
        scope["NEXT_SEEDS"] = next_seeds
    _actions(post, scope, context)
    if flow is None:
        if scope["ISPASS"]:
            _LOGGER.debug(
                "[开始翻页] 当前页面: %r, 种子: %r",
                context.seeds.get(key),
                context.seeds,
            )
            submitted = context.submit(
                scope["NEXT_SEEDS"], affinity="any" if call_later else "current"
            )
            if success and submitted:
                raise signals.Success()
        else:
            _LOGGER.debug(
                "[停止翻页] 当前页面: %r, 种子: %r",
                context.seeds.get(key),
                context.seeds,
            )
    else:
        Generator([(Type.choice, flow)]).run(scope)


@hooks.current
def inject(
    flows: Sequence[tuple[Type | str, Any]],
    *,
    namespace: Mapping[str, Any] | None = None,
) -> None:
    """在当前 Context 中执行可组合的可信动态 flow。

    Args:
        flows: define、choice、condition 和 code 步骤序列。
        namespace: 追加到标准脚本命名空间的可信对象。
    """
    from bricks.spider.context import Context

    context = Context.get_context()
    Generator(flows).run(build_namespace(context, namespace))


__all__ = ["Generator", "Type", "build_namespace", "inject", "is_success", "turn_page"]
