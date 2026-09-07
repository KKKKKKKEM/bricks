"""Graph 节点模型：Ports、InputPolicy、Node 和 Output。"""

from __future__ import annotations

import enum
import math
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, get_origin

if TYPE_CHECKING:
    from .events import Context
    from .policies import PolicyRef


def require_non_empty_string(value: object, label: str) -> str:
    """校验一个值是非空字符串。

    Args:
        value: 待校验的字符串，不能是空串或全空白。
        label: 校验失败时用于指明字段的说明名称。

    Returns:
        通过非空字符串校验的原始值。

    Raises:
        TypeError: 参数类型或接口实现不符合当前契约。
        ValueError: 参数值或字段组合不合法。
    """

    if not isinstance(value, str):
        raise TypeError(f"{label} must be a string")
    if not value.strip():
        raise ValueError(f"{label} must not be empty")
    return value


def _validate_timeout(timeout: float | None) -> None:
    """校验等待接口使用的超时参数。

    Args:
        timeout: 等待或执行时限，单位秒；None 表示不设置时限。

    Raises:
        TypeError: 参数类型或接口实现不符合当前契约。
        ValueError: 参数值或字段组合不合法。
    """

    if timeout is None:
        return
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise TypeError("timeout must be a number or None")
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("timeout must be a finite non-negative number")


class InputPolicy(str, enum.Enum):
    """Node 的固定输入触发策略。

    Attributes:
        ALL: 所有声明输入端口都有 token 时触发。
        ANY: 任一输入端口有 token 时触发。
        ON_START: Graph 开始时触发一次，不消费端口 token。
    """

    ALL = "all"
    ANY = "any"
    ON_START = "on_start"


class Ports(Mapping[str, type[Any]]):
    """保存不可变的端口名称与 Python 类型映射。

    Attributes:
        __slots__: 实例允许保存的字段名称，限制动态增加属性。
        _types: 端口名称到数据类型的只读映射。
    """

    __slots__ = ("_types",)

    def __init__(self, **ports: type[Any]) -> None:
        """创建端口集合。

        Args:
            **ports: 保持声明顺序的输入端口名称。
        """

        normalized: dict[str, type[Any]] = {}
        for name, data_type in ports.items():
            require_non_empty_string(name, "port name")
            normalized[name] = self.require_type(
                data_type,
                f"port {name!r} type",
            )
        self._types = MappingProxyType(normalized)

    @staticmethod
    def require_type(value: object, label: str) -> type[Any]:
        """校验端口声明使用普通 Python class。

        Args:
            value: 待校验的端口数据类型，必须是普通 Python 类。
            label: 校验失败时用于指明字段的说明名称。

        Returns:
            可作为端口数据类型的普通 Python 类。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if (
            value is Any
            or not isinstance(value, type)
            or get_origin(value) is not None
            or getattr(value, "_is_protocol", False)
        ):
            raise TypeError(f"{label} must be a Python class")
        return value

    @staticmethod
    def is_type_compatible(
        source_type: type[Any],
        target_type: type[Any],
    ) -> bool:
        """判断输出类型能否安全传给输入端口。

        Args:
            source_type: 源端口声明的数据类型。
            target_type: 目标端口要求的数据类型。

        Returns:
            满足当前操作的判断条件时返回 True，否则返回 False。
        """

        source_type = Ports.require_type(source_type, "source_type")
        target_type = Ports.require_type(target_type, "target_type")
        return issubclass(source_type, target_type)

    def __getitem__(self, port: str) -> type[Any]:
        """返回指定端口声明的数据类型。

        Args:
            port: 需要读取或输出的端口名称。

        Returns:
            指定端口声明的 Python 数据类型。
        """

        return self._types[port]

    def __iter__(self) -> Iterator[str]:
        """按声明顺序遍历端口名称。

        Returns:
            遍历当前对象内容的独立迭代入口。
        """

        return iter(self._types)

    def __len__(self) -> int:
        """返回端口数量。

        Returns:
            当前容器条目数量。
        """

        return len(self._types)


@dataclass(frozen=True, slots=True)
class Output:
    """Node 在当前 Graph 内从命名端口产生的一项值。

    Attributes:
        value: 当前记录携带的数据值。
        port: Output 在当前 Graph 内发送到的输出端口。
    """

    value: Any
    port: str = "default"

    def __post_init__(self) -> None:
        """校验输出端口名称。"""

        require_non_empty_string(self.port, "output port")


class Node(ABC):
    """默认在 Runtime 同步 worker 中执行的图节点。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
        input_policy: 仅依据端口和 token 数量生效的输入策略。
        timeout: 超时秒数，None 表示不限制。
    """

    input_ports = Ports(default=object)
    output_ports = Ports(default=object)
    input_policy: InputPolicy | PolicyRef = InputPolicy.ALL
    timeout: float | None = None

    @abstractmethod
    def execute(
        self,
        inputs: Mapping[str, Any],
        context: Context,
    ) -> Output | Iterable[Output] | Awaitable[Output | Iterable[Output] | None] | None:
        """执行一次节点行为。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            符合声明端口契约的 Output 集合。
        """


class AsyncNode(Node):
    """显式在 asyncio 环境中执行的异步图节点。"""

    @abstractmethod
    async def execute(
        self,
        inputs: Mapping[str, Any],
        context: Context,
    ) -> Output | Iterable[Output] | None:
        """异步执行一次节点行为。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            符合声明端口契约的 Output 集合。
        """
