"""Runtime 依赖的角色协议；便利等待接口由门面组合 Execution 实现。"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any, Protocol, runtime_checkable

from ..engine.core import Output
from ..engine.events import Event
from ..engine.execution import Execution, ExecutionLimits
from ..engine.graph import ExecutionPlan, Graph
from ..engine.hooks import HookHandle, HookPhase, NodeHook
from ..engine.observation import ObserverHandle, RuntimeObserver
from ..engine.policies import InputSelector
from ..engine.slots import SlotProvider
from .runtime import EventHandler


@runtime_checkable
class RouterRole(Protocol):
    @property
    def idle(self) -> bool:
        """判断当前组件是否没有尚未完成的工作。

        Returns:
            没有未完成工作时为 True，否则为 False。
        """
        ...

    def observe(self, event_type: str, handler: EventHandler) -> object:
        """注册只读事件观察者，不建立目标 Graph 路由。

        Args:
            event_type: 用于订阅或路由匹配的事件类型。
            handler: 接收事件或投递的处理函数。

        Returns:
            实现自行选择的组合结果，调用方不依赖其具体类型。
        """
        ...

    def route(
        self,
        event_type: str,
        *,
        graph: str,
        queue: str,
        subscription: str | None = None,
        limits: ExecutionLimits | None = None,
    ) -> object:
        """将事件类型连接到目标 Graph 和命名消费通道。

        Args:
            event_type: 用于订阅或路由匹配的事件类型。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            queue: 命名消费通道。
            subscription: 竞争消费组名称，None 创建独立订阅。
            limits: 本次执行独立使用的步数和时长限制。

        Returns:
            实现自行选择的组合结果，调用方不依赖其具体类型。
        """
        ...

    def emit(self, event_or_type: Event | str, payload: Any = None) -> Event:
        """通过事件发布能力提交领域事件并返回已接受的事件。

        Args:
            event_or_type: 已有事件实例，或用于构造事件的类型字符串。
            payload: 事件携带的领域数据。

        Returns:
            事件传输已经接受的 Event 实例。
        """
        ...

    def publish(self, event: Event) -> None:
        """发布事件并推进对应的投递或观察流程。

        Args:
            event: 需要发布、观察或处理的事件。
        """
        ...

    def observe_runtime(self, observer: RuntimeObserver) -> ObserverHandle:
        """注册只读运行时生命周期观察者。

        Args:
            observer: 接收只读生命周期事件的观察者。

        Returns:
            用于卸载本次注册的句柄。
        """
        ...

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待已接受的工作完成，并传播已记录的失败。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
        """
        ...

    def close(self) -> None:
        """结束当前组件的生命周期并释放其拥有的资源。"""
        ...


@runtime_checkable
class WorkerRole(Protocol):
    @property
    def idle(self) -> bool:
        """判断当前组件是否没有尚未完成的工作。

        Returns:
            没有未完成工作时为 True，否则为 False。
        """
        ...

    def register(self, name: str, graph: Graph) -> object:
        """注册具名定义，供后续装配或执行查找。

        Args:
            name: 注册或查找使用的名称。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。

        Returns:
            实现自行选择的组合结果，调用方不依赖其具体类型。
        """
        ...

    def consume(
        self,
        queue: str,
        *,
        concurrency: int = 1,
        slots: SlotProvider | None = None,
    ) -> object:
        """为命名通道注册本地消费者与独立的执行并发限制。

        Args:
            queue: 命名消费通道。
            concurrency: 当前消费者允许并行执行的完整 Graph 数量。
            slots: 提供本地执行槽的资源池能力。

        Returns:
            实现自行选择的组合结果，调用方不依赖其具体类型。
        """
        ...

    def start(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> Execution:
        """提交 Graph 执行并返回统一 Execution 句柄。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            inputs: 入口数据或按端口名称组织的输入映射。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            max_steps: 节点触发次数上限，0 表示不限制。
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
            output_buffer: 每个活跃输出流允许积压的输出条数。

        Returns:
            本次操作得到的 Execution 实例。
        """
        ...

    def iter(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> Iterator[Output]:
        """启动 Graph 并返回同步终端输出流。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            inputs: 入口数据或按端口名称组织的输入映射。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            max_steps: 节点触发次数上限，0 表示不限制。
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
            output_buffer: 每个活跃输出流允许积压的输出条数。

        Returns:
            按产生顺序交付终端 Output 的迭代入口。
        """
        ...

    def aiter(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> AsyncIterator[Output]:
        """启动 Graph 并返回异步终端输出流。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            inputs: 入口数据或按端口名称组织的输入映射。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            max_steps: 节点触发次数上限，0 表示不限制。
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
            output_buffer: 每个活跃输出流允许积压的输出条数。

        Returns:
            按产生顺序交付终端 Output 的迭代入口。
        """
        ...

    def get_execution(self, execution_id: str) -> Execution:
        """按执行 ID 取得已保存的执行句柄。

        Args:
            execution_id: 已登记执行记录的唯一标识。

        Returns:
            本次操作得到的 Execution 实例。
        """
        ...

    def executions(self) -> tuple[Execution, ...]:
        """返回当前保存的执行记录快照。

        Returns:
            当前保存的 Execution 句柄快照。
        """
        ...

    def attach(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle:
        """注册扩展回调并返回可卸载的句柄。

        Args:
            hook: 节点 Hook 对象或单阶段回调。
            phase: 函数 Hook 对应的执行阶段。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            node: 节点实例或作用域中的节点 ID，以接口类型为准。

        Returns:
            用于卸载本次注册的句柄。
        """
        ...

    def contribute_hook(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle | None:
        """通过执行器公开扩展能力贡献节点 Hook。

        Args:
            hook: 节点 Hook 对象或单阶段回调。
            phase: 函数 Hook 对应的执行阶段。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            node: 节点实例或作用域中的节点 ID，以接口类型为准。

        Returns:
            用于卸载本次注册的句柄。
        """
        ...

    def register_policy(self, name: str, selector: InputSelector) -> object:
        """注册带命名空间的输入选择策略。

        Args:
            name: 注册或查找使用的名称。
            selector: 仅依据端口和 token 数量选择输入的实现。

        Returns:
            实现自行选择的组合结果，调用方不依赖其具体类型。
        """
        ...

    def observe_runtime(self, observer: RuntimeObserver) -> ObserverHandle:
        """注册只读运行时生命周期观察者。

        Args:
            observer: 接收只读生命周期事件的观察者。

        Returns:
            用于卸载本次注册的句柄。
        """
        ...

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待已接受的工作完成，并传播已记录的失败。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
        """
        ...

    def close(self) -> None:
        """结束当前组件的生命周期并释放其拥有的资源。"""
        ...
