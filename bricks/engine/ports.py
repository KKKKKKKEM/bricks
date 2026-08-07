"""Node 的类型化端口声明。"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from types import MappingProxyType
from typing import Any

from ._validation import require_non_empty_string


class Ports(Mapping[str, type[Any]]):
    """保存不可变的端口名称与 Python 类型映射。"""

    __slots__ = ("_types",)

    def __init__(self, **ports: type[Any]) -> None:
        """创建端口集合。

        参数：
            **ports: 端口名称到允许数据类型的映射。

        异常：
            TypeError: 端口名称不是字符串，或端口类型不是 Python class。
            ValueError: 端口名称为空。
        """

        normalized: dict[str, type[Any]] = {}
        for name, data_type in ports.items():
            require_non_empty_string(name, "port name")
            normalized[name] = _require_port_type(
                data_type,
                f"port {name!r} type",
            )
        self._types = MappingProxyType(normalized)

    def __getitem__(self, port: str) -> type[Any]:
        """返回指定端口声明的数据类型。

        参数：
            port: 需要查询的端口名称。

        返回：
            端口声明的 Python class。
        """

        return self._types[port]

    def __iter__(self) -> Iterator[str]:
        """按声明顺序遍历端口名称。

        返回：
            端口名称迭代器。
        """

        return iter(self._types)

    def __len__(self) -> int:
        """返回端口数量。

        返回：
            当前集合中的端口数量。
        """

        return len(self._types)


def is_type_compatible(
    source_type: type[Any],
    target_type: type[Any],
) -> bool:
    """判断输出类型能否安全传给输入端口。

    参数：
        source_type: 上游 output port 声明的数据类型。
        target_type: 下游 input port 声明的数据类型。

    返回：
        source_type 是 target_type 或其子类时返回 True。
    """

    source_type = _require_port_type(source_type, "source_type")
    target_type = _require_port_type(target_type, "target_type")
    return issubclass(source_type, target_type)


def _require_port_type(value: object, label: str) -> type[Any]:
    """校验端口类型是普通 Python class。

    参数：
        value: 需要校验的端口类型。
        label: 用于异常信息的参数名称。

    返回：
        校验通过的 Python class。

    异常：
        TypeError: value 不是普通 Python class，或使用了 typing.Any。
    """

    if value is Any or not isinstance(value, type):
        raise TypeError(f"{label} must be a Python class")
    return value
