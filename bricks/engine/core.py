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
    """校验一个值是非空字符串。"""

    if not isinstance(value, str):
        raise TypeError(f"{label} must be a string")
    if not value.strip():
        raise ValueError(f"{label} must not be empty")
    return value


def _validate_timeout(timeout: float | None) -> None:
    """校验等待接口使用的超时参数。"""

    if timeout is None:
        return
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise TypeError("timeout must be a number or None")
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("timeout must be a finite non-negative number")


class InputPolicy(str, enum.Enum):
    """Node 的固定输入触发策略。"""

    ALL = "all"
    ANY = "any"
    ON_START = "on_start"


class Ports(Mapping[str, type[Any]]):
    """保存不可变的端口名称与 Python 类型映射。"""

    __slots__ = ("_types",)

    def __init__(self, **ports: type[Any]) -> None:
        """创建端口集合。"""

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
        """校验端口声明使用普通 Python class。"""

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
        """判断输出类型能否安全传给输入端口。"""

        source_type = Ports.require_type(source_type, "source_type")
        target_type = Ports.require_type(target_type, "target_type")
        return issubclass(source_type, target_type)

    def __getitem__(self, port: str) -> type[Any]:
        """返回指定端口声明的数据类型。"""

        return self._types[port]

    def __iter__(self) -> Iterator[str]:
        """按声明顺序遍历端口名称。"""

        return iter(self._types)

    def __len__(self) -> int:
        """返回端口数量。"""

        return len(self._types)


@dataclass(frozen=True, slots=True)
class Output:
    """Node 在当前 Graph 内从命名端口产生的一项值。"""

    value: Any
    port: str = "default"

    def __post_init__(self) -> None:
        """校验输出端口名称。"""

        require_non_empty_string(self.port, "output port")


class Node(ABC):
    """默认在 Runtime 同步 worker 中执行的图节点。"""

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
        """执行一次节点行为。"""


class AsyncNode(Node):
    """显式在 asyncio 环境中执行的异步图节点。"""

    @abstractmethod
    async def execute(
        self,
        inputs: Mapping[str, Any],
        context: Context,
    ) -> Output | Iterable[Output] | None:
        """异步执行一次节点行为。"""
