"""可热插拔的 Node 执行钩子和流程控制信号。"""

from __future__ import annotations

import enum
import itertools
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from threading import RLock
from types import MappingProxyType
from typing import Any

from .core import Node, Output
from .events import Context

Outputs = tuple[Output, ...]


@dataclass(frozen=True, slots=True)
class NodeCall:
    """一次 Node 调用中允许 Hook 查看和修改的参数。"""

    graph: str
    node_id: str
    node: Node
    inputs: Mapping[str, Any]
    context: Context

    def with_inputs(self, inputs: Mapping[str, Any]) -> NodeCall:
        """保留调用身份，只替换传给 Node 的输入。"""

        if not isinstance(inputs, Mapping):
            raise TypeError("hook inputs must be a mapping")
        return replace(self, inputs=MappingProxyType(dict(inputs)))


class NodeHook:
    """通过 enter、exit 和 error 介入一次 Node 执行。"""

    def enter(self, call: NodeCall) -> NodeCall:
        """在 Node 执行前转换调用参数。"""

        return call

    def exit(self, call: NodeCall, outputs: Outputs) -> Outputs:
        """在 Node 成功或被短路后转换结果。"""

        del call
        return outputs

    def error(self, call: NodeCall, error: Exception) -> Outputs:
        """处理 Node 异常；默认继续抛出。"""

        del call
        raise error


class HookPhase(str, enum.Enum):
    """单函数 Hook 对应的生命周期阶段。"""

    ENTER = "enter"
    EXIT = "exit"
    ERROR = "error"


class HookSignal(BaseException):
    """由 Engine 捕获并解释的 Hook 控制信号。"""

    def __init__(self, *outputs: Output) -> None:
        super().__init__(*outputs)
        self.outputs = tuple(outputs)


class ShortCircuit(HookSignal):
    """跳过当前 Node，并把携带结果作为该 Node 的输出继续路由。"""


class StopGraph(HookSignal):
    """立即停止整张 Graph，并把携带结果作为 Graph 的终端输出。"""


@dataclass(frozen=True, slots=True)
class _Registration:
    id: int
    hook: NodeHook
    graph: str | None
    node: str | None


class HookHandle:
    """控制一项动态 Hook 注册。"""

    __slots__ = ("_registration_id", "_registry")

    def __init__(self, registry: HookRegistry, registration_id: int) -> None:
        self._registry = registry
        self._registration_id = registration_id

    def detach(self) -> None:
        """幂等卸载；已经开始的 Graph execution 继续使用旧快照。"""

        self._registry._detach(self._registration_id)

    def __enter__(self) -> HookHandle:  # noqa: PYI034
        return self

    def __exit__(self, *args: object) -> None:
        del args
        self.detach()


class _FunctionHook(NodeHook):
    """把单阶段函数适配为完整 NodeHook。"""

    def __init__(self, function: Callable[..., object], phase: HookPhase) -> None:
        self._function = function
        self._phase = phase

    def enter(self, call: NodeCall) -> Any:
        if self._phase is HookPhase.ENTER:
            return self._function(call)
        return call

    def exit(self, call: NodeCall, outputs: Outputs) -> Any:
        if self._phase is HookPhase.EXIT:
            return self._function(call, outputs)
        return outputs

    def error(self, call: NodeCall, error: Exception) -> Any:
        if self._phase is HookPhase.ERROR:
            return self._function(call, error)
        raise error


class HookRegistry:
    """线程安全地注册 Hook，并为 Graph execution 提供不可变快照。"""

    def __init__(self) -> None:
        self._lock = RLock()
        self._counter = itertools.count()
        self._registrations: tuple[_Registration, ...] = ()
        self._closed = False

    def attach(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle:
        """按注册顺序挂载对象 Hook 或单阶段函数 Hook。"""

        if node is not None and graph is None:
            raise ValueError("node-scoped hook requires graph")
        if isinstance(hook, NodeHook):
            if phase is not None:
                raise TypeError("phase is only valid for a function hook")
            adapted = hook
        elif callable(hook):
            selected = HookPhase.ENTER if phase is None else HookPhase(phase)
            adapted = _FunctionHook(hook, selected)
        else:
            raise TypeError("hook must be a NodeHook or callable")

        with self._lock:
            if self._closed:
                raise RuntimeError("hook registry is closed")
            registration = _Registration(
                next(self._counter), adapted, graph, node
            )
            self._registrations = (*self._registrations, registration)
        return HookHandle(self, registration.id)

    def snapshot(self, graph: str) -> tuple[_Registration, ...]:
        """固定一次 Graph execution 可见的全部注册。"""

        with self._lock:
            return tuple(
                registration
                for registration in self._registrations
                if registration.graph is None or registration.graph == graph
            )

    @staticmethod
    def for_node(
        snapshot: tuple[_Registration, ...],
        node_id: str,
    ) -> tuple[NodeHook, ...]:
        """从 execution 快照中选择当前 Node 可见的 Hook。"""

        return tuple(
            registration.hook
            for registration in snapshot
            if registration.node is None or registration.node == node_id
        )

    def close(self) -> None:
        """停止接受新 Hook；现有句柄仍可安全 detach。"""

        with self._lock:
            self._closed = True

    def _detach(self, registration_id: int) -> None:
        with self._lock:
            self._registrations = tuple(
                item
                for item in self._registrations
                if item.id != registration_id
            )
