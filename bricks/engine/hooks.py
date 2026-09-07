"""可热插拔的 Node 执行钩子和流程控制信号。"""

from __future__ import annotations

import enum
import itertools
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from threading import RLock
from types import MappingProxyType
from typing import Any

from .core import Node, Output
from .events import Context

Outputs = tuple[Output, ...]


@dataclass(frozen=True, slots=True)
class NodeCall:
    """一次 Node 调用中允许 Hook 查看和修改的参数。

    Attributes:
        graph: 关联的 Graph 定义或注册名称。
        node_id: Graph 内当前节点的绑定 ID。
        node: 关联的节点实例或 Graph 内节点 ID。
        inputs: 节点调用或工作请求的输入数据。
        context: 当前节点调用允许访问的执行上下文。
    """

    graph: str
    node_id: str
    node: Node
    inputs: Mapping[str, Any]
    context: Context

    def with_inputs(self, inputs: Mapping[str, Any]) -> NodeCall:
        """保留调用身份，只替换传给 Node 的输入。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。

        Returns:
            身份信息不变、输入已替换的 NodeCall。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if not isinstance(inputs, Mapping):
            raise TypeError("hook inputs must be a mapping")
        return replace(self, inputs=MappingProxyType(dict(inputs)))


class NodeHook:
    """通过 enter、exit 和 error 介入一次 Node 执行。"""

    def enter(self, call: NodeCall) -> NodeCall | Awaitable[NodeCall]:
        """在 Node 执行前转换调用参数。

        Args:
            call: Hook 当前处理的节点调用记录。

        Returns:
            同步调用或异步等待完成后的处理结果。
        """

        return call

    def exit(self, call: NodeCall, outputs: Outputs) -> Outputs | Awaitable[Outputs]:
        """在 Node 成功或被短路后转换结果。

        Args:
            call: Hook 当前处理的节点调用记录。
            outputs: 按交付顺序组织的节点输出。

        Returns:
            符合声明端口契约的 Output。
        """

        del call
        return outputs

    def error(self, call: NodeCall, error: Exception) -> Outputs | Awaitable[Outputs]:
        """处理 Node 异常；默认继续抛出。

        Args:
            call: Hook 当前处理的节点调用记录。
            error: 需要传播、记录或用于恢复的异常。

        Returns:
            符合声明端口契约的 Output。
        """

        del call
        raise error


class HookPhase(str, enum.Enum):
    """单函数 Hook 对应的生命周期阶段。

    Attributes:
        ENTER: 节点执行前的 Hook 阶段。
        EXIT: 节点完成后的 Hook 阶段。
        ERROR: 节点异常后的 Hook 阶段。
    """

    ENTER = "enter"
    EXIT = "exit"
    ERROR = "error"


class HookSignal(BaseException):
    """由 Engine 捕获并解释的 Hook 控制信号。

    Attributes:
        outputs: 控制信号携带的节点输出集合。
    """

    def __init__(self, *outputs: Output) -> None:
        """保存控制信号携带的输出，供 Engine 解释后交付。

        Args:
            *outputs: 按交付顺序组织的节点输出。
        """

        super().__init__(*outputs)
        self.outputs = tuple(outputs)


class ShortCircuit(HookSignal):
    """跳过当前 Node，并把携带结果作为该 Node 的输出继续路由。"""


class StopGraph(HookSignal):
    """停止后续 Node，并把携带结果追加到 Graph 已产生的终端输出。"""


@dataclass(frozen=True, slots=True)
class _Registration:
    """一个具有 Graph 和节点作用域的 Hook 注册记录。

    Attributes:
        id: 当前对象的唯一标识。
        hook: 注册的节点 Hook 实例。
        graph: 关联的 Graph 定义或注册名称。
        node: 关联的节点实例或 Graph 内节点 ID。
    """

    id: int
    hook: NodeHook
    graph: str | None
    node: str | None


class HookHandle:
    """控制一项动态 Hook 注册。

    Attributes:
        __slots__: 实例允许保存的字段名称，限制动态增加属性。
        _registry: 管理 Hook 注册和卸载的容器。
        _registration_id: 句柄对应的 Hook 注册标识。
    """

    __slots__ = ("_registration_id", "_registry")

    def __init__(self, registry: HookRegistry, registration_id: int) -> None:
        """绑定 Hook 注册容器及需要卸载的注册 ID。

        Args:
            registry: 管理可卸载注册项的容器。
            registration_id: 需要定位或卸载的注册项标识。
        """

        self._registry = registry
        self._registration_id = registration_id

    def detach(self) -> None:
        """幂等卸载；已经开始的 Graph execution 继续使用旧快照。"""

        self._registry._detach(self._registration_id)

    def __enter__(self) -> HookHandle:  # noqa: PYI034
        """进入资源作用域并返回当前句柄。

        Returns:
            当前资源管理对象。
        """

        return self

    def __exit__(self, *args: object) -> None:
        """退出资源作用域，执行对应的关闭或卸载操作。

        Args:
            *args: 调用协议传入的位置参数。
        """

        del args
        self.detach()


class _FunctionHook(NodeHook):
    """把单阶段函数适配为完整 NodeHook。

    Attributes:
        _function: 调用方注入的单阶段 Hook 函数。
        _phase: 函数 Hook 生效的调用阶段。
    """

    def __init__(self, function: Callable[..., object], phase: HookPhase) -> None:
        """将单阶段函数绑定到对应的 Hook 生命周期阶段。

        Args:
            function: 调用方提供的转换或处理函数。
            phase: 函数 Hook 对应的执行阶段。
        """

        self._function = function
        self._phase = phase

    def enter(self, call: NodeCall) -> Any:
        """在节点执行前调用入口 Hook 并取得调用参数。

        Args:
            call: Hook 当前处理的节点调用记录。

        Returns:
            同步调用或异步等待完成后的处理结果。
        """

        if self._phase is HookPhase.ENTER:
            return self._function(call)
        return call

    def exit(self, call: NodeCall, outputs: Outputs) -> Any:
        """在节点完成后调用出口 Hook 并取得输出。

        Args:
            call: Hook 当前处理的节点调用记录。
            outputs: 按交付顺序组织的节点输出。

        Returns:
            同步调用或异步等待完成后的处理结果。
        """

        if self._phase is HookPhase.EXIT:
            return self._function(call, outputs)
        return outputs

    def error(self, call: NodeCall, error: Exception) -> Any:
        """在错误阶段执行回调，未恢复的异常继续传播。

        Args:
            call: Hook 当前处理的节点调用记录。
            error: 需要传播、记录或用于恢复的异常。

        Returns:
            同步调用或异步等待完成后的处理结果。
        """

        if self._phase is HookPhase.ERROR:
            return self._function(call, error)
        raise error


class HookRegistry:
    """线程安全地注册 Hook，并为 Graph execution 提供不可变快照。

    Attributes:
        _lock: 保护当前组件共享状态的进程内互斥锁。
        _counter: 分配注册项 ID 的递增计数器。
        _registrations: 按注册顺序排列的不可变 Hook 注册快照。
        _closed: 当前组件是否已停止接受新工作。
    """

    def __init__(self) -> None:
        """创建独立的 Hook 注册快照、计数器和互斥锁。"""

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
        """按注册顺序挂载对象 Hook 或单阶段函数 Hook。

        Args:
            hook: 节点 Hook 对象或单阶段回调。
            phase: 函数 Hook 对应的执行阶段。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            node: 节点实例或作用域中的节点 ID，以接口类型为准。

        Returns:
            用于卸载本次注册的句柄。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

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
            registration = _Registration(next(self._counter), adapted, graph, node)
            self._registrations = (*self._registrations, registration)
        return HookHandle(self, registration.id)

    def snapshot(self, graph: str) -> tuple[_Registration, ...]:
        """固定一次 Graph execution 可见的全部注册。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。

        Returns:
            当前 Graph 可见的不可变 Hook 注册集合。
        """

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
        """从 execution 快照中选择当前 Node 可见的 Hook。

        Args:
            snapshot: 本次执行固定使用的定义或注册快照。
            node_id: Graph 内绑定的节点 ID。

        Returns:
            按注册顺序筛选出的当前节点 Hook 集合。
        """

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
        """从当前注册集合中移除指定注册项。

        Args:
            registration_id: 需要定位或卸载的注册项标识。
        """

        with self._lock:
            self._registrations = tuple(
                item for item in self._registrations if item.id != registration_id
            )
