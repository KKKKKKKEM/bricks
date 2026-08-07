"""图引擎共用的类型别名。

本模块不包含运行时实现。将协议类型集中在这里，可以让图定义层独立于具体执行器、队列和领域适配器。
"""

from __future__ import annotations

import copy
from collections.abc import Mapping as MappingABC
from typing import Any, Awaitable, Iterator, Mapping, Protocol, TypeAlias

NodeId: TypeAlias = str
EventName: TypeAlias = str
Payload: TypeAlias = Any
Metadata: TypeAlias = Mapping[str, Any]


class FrozenDict(MappingABC):
    """支持深复制和结构相等比较的递归只读映射。"""

    __slots__ = ("_data",)

    def __init__(self, value: Mapping[Any, Any]) -> None:
        self._data = {key: freeze_value(item) for key, item in value.items()}

    def __getitem__(self, key: Any) -> Any:
        return self._data[key]

    def __iter__(self) -> Iterator[Any]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __deepcopy__(self, memo: dict[int, Any]) -> "FrozenDict":
        return self

    def __repr__(self) -> str:
        return repr(self._data)


class FrozenList(tuple):
    """保留与 list 的结构相等语义，同时禁止原地修改。"""

    def __new__(cls, values: Any = ()) -> "FrozenList":
        return super().__new__(cls, (freeze_value(item) for item in values))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, (list, tuple)):
            return tuple(self) == tuple(other)
        return False

    def __ne__(self, other: object) -> bool:
        return not self == other

    def __deepcopy__(self, memo: dict[int, Any]) -> "FrozenList":
        return self


def freeze_value(value: Any) -> Any:
    """递归冻结常见 JSON 容器，并复制其它可变对象以隔离调用方。"""
    if isinstance(value, (FrozenDict, FrozenList)):
        return value
    if isinstance(value, MappingABC):
        return FrozenDict(value)
    if isinstance(value, list):
        return FrozenList(value)
    if isinstance(value, tuple):
        return tuple(freeze_value(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(freeze_value(item) for item in value)
    return copy.deepcopy(value)


def thaw_value(value: Any) -> Any:
    """把冻结容器转换为适合快照和 JSON 编码的普通容器。"""
    if isinstance(value, MappingABC):
        return {key: thaw_value(item) for key, item in value.items()}
    if isinstance(value, (FrozenList, list)):
        return [thaw_value(item) for item in value]
    if isinstance(value, tuple):
        return [thaw_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return [thaw_value(item) for item in value]
    return copy.deepcopy(value)


class Action(Protocol):
    def __call__(self, context: Any, event: Any) -> Any | Awaitable[Any]: ...


class Guard(Protocol):
    def __call__(self, context: Any, event: Any) -> bool | Awaitable[bool]: ...


class AsyncAction(Protocol):
    async def __call__(self, context: Any, event: Any) -> Any: ...
