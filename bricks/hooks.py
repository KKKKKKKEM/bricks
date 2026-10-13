"""爬虫共用的事件声明与参数快照，不建立全局注册表。"""

from __future__ import annotations

import inspect
import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

BEFORE_START = "BEFORE_START"  # Spider 首次打开并完成装配前。
BEFORE_CLOSE = "BEFORE_CLOSE"  # 停止执行后、释放自有资源前。
ERROR_OCCURRED = "ERROR_OCCURRED"  # 业务或插槽普通异常；不允许恢复原失败。
BEFORE_GET_SEEDS = "BEFORE_GET_SEEDS"  # QueueBackend 实际尝试领取之前。
AFTER_GET_SEEDS = "AFTER_GET_SEEDS"  # 已领取并取得本地 Slot 后，运行消费图之前。
BEFORE_PUT_SEEDS = "BEFORE_PUT_SEEDS"  # 初始化批次入队前。
AFTER_PUT_SEEDS = "AFTER_PUT_SEEDS"  # 初始化批次已入队后、提交恢复进度前。
BEFORE_MAKE_SEEDS = "BEFORE_MAKE_SEEDS"  # 调用种子源之前。
AFTER_MAKE_SEEDS = "AFTER_MAKE_SEEDS"  # 取得种子源之后。
BEFORE_MAKE_REQUEST = "BEFORE_MAKE_REQUEST"  # 根据种子构造请求之前。
AFTER_MAKE_REQUEST = "AFTER_MAKE_REQUEST"  # 请求构造完成之后。
BEFORE_REQUEST = "BEFORE_REQUEST"  # 下载之前。
AFTER_REQUEST = "AFTER_REQUEST"  # 下载之后，检查与展示响应在此注册。
BEFORE_RETRY = "BEFORE_RETRY"  # 下载重试之前。
AFTER_RETRY = "AFTER_RETRY"  # 重试准备完成之后。
BEFORE_PARSE = "BEFORE_PARSE"  # 响应解析之前。
AFTER_PARSE = "AFTER_PARSE"  # 解析结果产生之后。
BEFORE_PIPELINE = "BEFORE_PIPELINE"  # 存储之前。
AFTER_PIPELINE = "AFTER_PIPELINE"  # 存储返回之后。


def current(callback: Any) -> Any:
    """声明插件不接收 Context，并在内部使用 Context.get_context()。

    Args:
        callback: 同步或异步插件函数。

    Returns:
        未包装的原函数，便于 staticmethod、继承和测试复用。

    Raises:
        TypeError: 装饰对象不是函数。
    """
    function = (
        callback.__func__
        if isinstance(callback, (staticmethod, classmethod))
        else callback
    )
    if not inspect.isfunction(function):
        raise TypeError("hooks.current decorates functions")
    setattr(function, "__bricks_current_context__", True)
    return callback


@dataclass(frozen=True)
class Task:
    """可重复注册的插件调用描述，支持显式或当前 Context 写法。

    Attributes:
        func: 可显式首参接收 Context，或通过 Context.get_context() 读取。
        args: 追加的位置参数，固定外层元组，内部值归调用方管理。
        kwargs: 追加的关键字参数，只读外层快照，内部资源归调用方管理。
        index: 同阶段升序执行的索引，None 等同默认顺序 0。
        match: 可显式接收或通过 get_context() 读取 Context 的同步布尔条件。
        _with_context: 是否向 func 注入 Context；None 表示签名无法静态判断。
        _match_with_context: 是否向 match 注入 Context；无条件时为 None。
    """

    func: Callable[..., Any]
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)
    index: int | float | None = None
    match: Callable[..., bool] | None = None
    _with_context: bool | None = field(init=False, repr=False, compare=False)
    _match_with_context: bool | None = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        """校验声明并固定参数容器，不解析函数路径或表达式字符串。

        Raises:
            TypeError: 函数、参数、索引或条件类型非法。
            ValueError: 索引为 NaN。
        """
        if not callable(self.func):
            raise TypeError("plugin func must be callable")
        if not isinstance(self.args, (list, tuple)) or not isinstance(
            self.kwargs, Mapping
        ):
            raise TypeError("plugin args and kwargs must be a sequence and mapping")
        if any(type(key) is not str for key in self.kwargs):
            raise TypeError("plugin kwargs keys must be strings")
        if self.index is not None and type(self.index) not in (int, float):
            raise TypeError("plugin index must be numeric or None")
        if self.index is not None and math.isnan(self.index):
            raise ValueError("plugin index must not be NaN")
        if self.match is not None and (
            not callable(self.match)
            or any(
                test(candidate)
                for candidate in (self.match, getattr(self.match, "__call__", None))
                for test in (inspect.iscoroutinefunction, inspect.isasyncgenfunction)
            )
        ):
            raise TypeError("plugin match must be a synchronous callable")
        object.__setattr__(self, "args", tuple(self.args))
        object.__setattr__(self, "kwargs", MappingProxyType(dict(self.kwargs)))
        object.__setattr__(
            self,
            "_with_context",
            self._resolve_context_binding(self.func, self.args, self.kwargs),
        )
        object.__setattr__(
            self,
            "_match_with_context",
            None
            if self.match is None
            else self._resolve_context_binding(self.match, (), {}),
        )

    @staticmethod
    def _resolve_context_binding(
        function: Callable[..., Any], args: tuple[Any, ...], kwargs: Mapping[str, Any]
    ) -> bool | None:
        """根据最终调用参数判断是否需要注入当前 Context。

        显式 Context 与无 Context 两种形式都能绑定时优先显式形式，保持
        当前 API 的既有行为。装饰器收集阶段的未绑定方法可能暂时无法判断，
        实例绑定后创建的新 Task 会重新计算。

        Returns:
            True 注入 Context，False 直接调用，None 表示签名不可检查或不匹配。
        """
        target = getattr(function, "__func__", function)
        if getattr(target, "__bricks_current_context__", False):
            return False
        try:
            signature = inspect.signature(function)
        except (TypeError, ValueError):
            return None
        marker = object()
        try:
            signature.bind(marker, *args, **kwargs)
        except TypeError:
            pass
        else:
            return True
        try:
            signature.bind(*args, **kwargs)
        except TypeError:
            return None
        return False


@dataclass(frozen=True)
class _Binding:
    """当前实例待绑定的一项阶段声明。

    Attributes:
        form: 原样保存的阶段名称，由领域框架验证支持范围。
        task: 可复用的调用描述。
    """

    form: str
    task: Task


def on(
    form: str,
    *,
    index: int | float | None = None,
    args: tuple[Any, ...] = (),
    kwargs: Mapping[str, Any] | None = None,
    match: Callable[..., bool] | None = None,
) -> Callable[..., Any]:
    """在方法上声明插件调用，保持原函数及静态／类方法描述符。

    Args:
        form: 阶段名称，如 AFTER_REQUEST。
        index: 执行索引，较小值优先。
        args: 追加位置参数。
        kwargs: 追加关键字参数。
        match: 可选同步条件。

    Returns:
        保留原方法的装饰器。
    """

    def decorate(callback: Any) -> Any:
        """把声明附加在实际函数上，不包装调用。

        Args:
            callback: 普通方法或静态／类方法描述符。

        Returns:
            原对象。

        Raises:
            TypeError: 对象不是函数。
        """
        function = (
            callback.__func__
            if isinstance(callback, (staticmethod, classmethod))
            else callback
        )
        if not inspect.isfunction(function):
            raise TypeError("hooks.on decorates methods")
        task = Task(function, args, {} if kwargs is None else kwargs, index, match)
        setattr(
            function,
            "__bricks_hooks__",
            (_Binding(form, task), *getattr(function, "__bricks_hooks__", ())),
        )
        return callback

    return decorate


def collect(instance: object) -> tuple[_Binding, ...]:
    """收集绑定到当前实例的声明，子类同名方法替换父类声明。

    Args:
        instance: 当前领域框架实例。

    Returns:
        独立的调用绑定快照。
    """
    attributes: dict[str, Any] = {}
    for cls in reversed(type(instance).__mro__):
        attributes.update(vars(cls))
    result = []
    for name, value in attributes.items():
        function = (
            value.__func__ if isinstance(value, (staticmethod, classmethod)) else value
        )
        if inspect.isfunction(function):
            for declaration in getattr(function, "__bricks_hooks__", ()):
                task = declaration.task
                result.append(
                    _Binding(
                        declaration.form,
                        Task(
                            getattr(instance, name),
                            task.args,
                            task.kwargs,
                            task.index,
                            task.match,
                        ),
                    )
                )
    return tuple(result)
