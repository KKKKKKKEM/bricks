"""组合 EventRouter 与 GraphWorker 的 Runtime 门面。"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator, Callable, Iterable, Iterator
from threading import RLock
from typing import Any

from ..engine.core import Output, _validate_timeout
from ..engine.errors import RuntimeClosedError
from ..engine.events import Event
from ..engine.execution import Execution, ExecutionLimits
from ..engine.graph import ExecutionPlan, Graph
from ..engine.hooks import HookHandle, HookPhase, NodeHook
from ..engine.observation import (
    CompositeObserverHandle,
    RuntimeObserver,
)
from ..engine.policies import InputSelector
from ..plugins import (
    CAP_EVENT_BUS,
    CAP_EVENT_ROUTER,
    CAP_GRAPH_EXECUTOR,
    CAP_EXECUTION_FACTORY,
    CAP_GRAPH_WORKER,
    CAP_TASK_BACKEND,
    Plugin,
    PluginHost,
)
from ..spi import EventHandler, RouterRole, SlotProvider, WorkerRole
from ._utils import _close_components, _remaining
from .plugin import LocalRuntimePlugin

_LOGGER = logging.getLogger(__name__)


class Runtime:
    """组合 EventRouter 与 GraphWorker 的单进程便利门面。

    Attributes:
        router: 负责事件发布和工作路由的角色。
        worker: 负责消费工作和执行 Graph 的角色。
        _plugin_host: 管理默认装配生命周期的宿主，显式角色注入时为 None。
        _closed: 当前组件是否已停止接受新工作。
        _lock: 保护当前组件共享状态的进程内互斥锁。
    """

    def __init__(
        self,
        *,
        router: RouterRole | None = None,
        worker: WorkerRole | None = None,
        plugins: Iterable[Plugin] | None = None,
    ) -> None:
        """通过插件宿主装配默认角色，或接管显式注入的路由与执行角色。

        Args:
            router: 事件路由角色，由 Runtime 管理生命周期。
            worker: 任务消费与 Graph 执行角色，由 Runtime 管理生命周期。
            plugins: 待装配的插件实例集合。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if plugins is not None and (router is not None or worker is not None):
            raise TypeError("plugins cannot be combined with router or worker")
        if (router is None) != (worker is None):
            raise TypeError("Runtime requires both router and worker")
        host: PluginHost | None = None
        if router is None:
            selected = () if plugins is None else tuple(plugins)
            provides = {
                capability
                for plugin in selected
                for capability in plugin.descriptor.provides
            }
            role_capabilities = {CAP_EVENT_ROUTER, CAP_GRAPH_WORKER}
            if provides & role_capabilities and not role_capabilities <= provides:
                raise TypeError("plugins must provide both EventRouter and GraphWorker")
            if not role_capabilities <= provides:
                infrastructure = {
                    CAP_EVENT_BUS,
                    CAP_TASK_BACKEND,
                    CAP_GRAPH_EXECUTOR,
                    CAP_EXECUTION_FACTORY,
                }
                selected = (
                    LocalRuntimePlugin(
                        _provide_capabilities=frozenset(infrastructure - provides)
                    ),
                    *selected,
                )
            host = PluginHost(selected)
            try:
                host.start()
                router = host.require(CAP_EVENT_ROUTER)
                worker = host.require(CAP_GRAPH_WORKER)
                if not isinstance(router, RouterRole):
                    raise TypeError("router must implement RouterRole")
                if not isinstance(worker, WorkerRole):
                    raise TypeError("worker must implement WorkerRole")
            except BaseException:
                try:
                    host.close()
                except BaseException:
                    _LOGGER.exception("failed to roll back Runtime plugin host")
                raise
        else:
            if not isinstance(router, RouterRole):
                raise TypeError("router must implement RouterRole")
            if not isinstance(worker, WorkerRole):
                raise TypeError("worker must implement WorkerRole")
        self.router = router
        self.worker = worker
        self._plugin_host = host
        self._closed = False
        self._lock = RLock()

    @property
    def plugin_host(self) -> PluginHost | None:
        """返回插件装配宿主；显式角色组合没有宿主。

        Returns:
            管理默认装配生命周期的宿主，显式角色注入时为 None。
        """

        return self._plugin_host

    def register(self, name: str, graph: Graph) -> Runtime:
        """注册并冻结 Graph，返回 Runtime 以便继续组合。

        Args:
            name: 注册或查找使用的名称。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。

        Returns:
            当前实例，可继续进行链式组合。
        """

        self.worker.register(name, graph)
        return self

    def observe(self, event_type: str, handler: EventHandler) -> Runtime:
        """注册只读事件观察者，不建立目标 Graph 路由。

        Args:
            event_type: 用于订阅或路由匹配的事件类型。
            handler: 接收事件或投递的处理函数。

        Returns:
            当前实例，可继续进行链式组合。
        """

        self.router.observe(event_type, handler)
        return self

    def route(
        self,
        event_type: str,
        *,
        graph: str,
        queue: str,
        subscription: str | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
    ) -> Runtime:
        """将事件类型连接到目标 Graph 和命名消费通道。

        Args:
            event_type: 用于订阅或路由匹配的事件类型。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            queue: 命名消费通道。
            subscription: 竞争消费组名称，None 创建独立订阅。
            max_steps: 节点触发次数上限，0 表示不限制。
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。

        Returns:
            当前实例，可继续进行链式组合。
        """

        limits = ExecutionLimits(max_steps, timeout)
        self.router.route(
            event_type,
            graph=graph,
            queue=queue,
            subscription=subscription,
            limits=limits,
        )
        return self

    def consume(
        self,
        queue: str,
        *,
        concurrency: int = 1,
        slots: SlotProvider | None = None,
    ) -> Runtime:
        """为命名通道注册本地消费者与独立的执行并发限制。

        Args:
            queue: 命名消费通道。
            concurrency: 当前消费者允许并行执行的完整 Graph 数量。
            slots: 提供本地执行槽的资源池能力。

        Returns:
            当前实例，可继续进行链式组合。
        """

        self.worker.consume(queue, concurrency=concurrency, slots=slots)
        return self

    def emit(self, event_or_type: Event | str, payload: Any = None) -> Event:
        """通过事件发布能力提交领域事件并返回已接受的事件。

        Args:
            event_or_type: 已有事件实例，或用于构造事件的类型字符串。
            payload: 事件携带的领域数据。

        Returns:
            事件传输已经接受的 Event 实例。
        """

        return self.router.emit(event_or_type, payload)

    def run(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> tuple[Output, ...]:
        """同步执行 Graph 并返回终端输出。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            inputs: 入口数据或按端口名称组织的输入映射。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            max_steps: 节点触发次数上限，0 表示不限制。
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
            output_buffer: 每个活跃输出流允许积压的输出条数。

        Returns:
            符合声明端口契约的 Output 集合。
        """

        return self.worker.start(
            graph,
            inputs,
            plan=plan,
            max_steps=max_steps,
            timeout=timeout,
            output_buffer=output_buffer,
        ).result()

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

        return self.worker.iter(
            graph,
            inputs,
            plan=plan,
            max_steps=max_steps,
            timeout=timeout,
            output_buffer=output_buffer,
        )

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

        return self.worker.aiter(
            graph,
            inputs,
            plan=plan,
            max_steps=max_steps,
            timeout=timeout,
            output_buffer=output_buffer,
        )

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
        """提交 Graph 执行并立即返回统一 Execution 句柄。

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

        return self.worker.start(
            graph,
            inputs,
            plan=plan,
            max_steps=max_steps,
            timeout=timeout,
            output_buffer=output_buffer,
        )

    async def arun(
        self,
        graph: str,
        inputs: Any = None,
        *,
        plan: ExecutionPlan | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
        output_buffer: int = 64,
    ) -> tuple[Output, ...]:
        """异步执行 Graph 并等待完整终端输出。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            inputs: 入口数据或按端口名称组织的输入映射。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            max_steps: 节点触发次数上限，0 表示不限制。
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
            output_buffer: 每个活跃输出流允许积压的输出条数。

        Returns:
            符合声明端口契约的 Output 集合。
        """

        return await self.worker.start(
            graph,
            inputs,
            plan=plan,
            max_steps=max_steps,
            timeout=timeout,
            output_buffer=output_buffer,
        )

    def get_execution(self, execution_id: str) -> Execution:
        """按执行 ID 取得已保存的执行句柄。

        Args:
            execution_id: 已登记执行记录的唯一标识。

        Returns:
            本次操作得到的 Execution 实例。
        """

        return self.worker.get_execution(execution_id)

    def executions(self) -> tuple[Execution, ...]:
        """返回当前保存的执行记录快照。

        Returns:
            当前保存的 Execution 句柄快照。
        """

        return self.worker.executions()

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

        return self.worker.attach(hook, phase=phase, graph=graph, node=node)

    def register_policy(self, name: str, selector: InputSelector) -> Runtime:
        """注册带命名空间的输入选择策略。

        Args:
            name: 注册或查找使用的名称。
            selector: 仅依据端口和 token 数量选择输入的实现。

        Returns:
            当前实例，可继续进行链式组合。
        """

        self.worker.register_policy(name, selector)
        return self

    def observe_runtime(self, observer: RuntimeObserver) -> CompositeObserverHandle:
        """订阅只读执行、Node、Event 和 Work 生命周期事件。

        Args:
            observer: 接收只读生命周期事件的观察者。

        Returns:
            用于卸载本次注册的句柄。
        """

        return CompositeObserverHandle(
            (
                self.router.observe_runtime(observer),
                self.worker.observe_runtime(observer),
            )
        )

    def on(
        self,
        event_type: str,
        *,
        graph: str,
        queue: str,
        concurrency: int = 1,
        slots: SlotProvider | None = None,
        subscription: str | None = None,
        max_steps: int = 0,
        timeout: float | None = None,
    ) -> Runtime:
        """组合注册 Event route，并启动对应 queue 的本地消费者。

        Args:
            event_type: 用于订阅或路由匹配的事件类型。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            queue: 命名消费通道。
            concurrency: 当前消费者允许并行执行的完整 Graph 数量。
            slots: 提供本地执行槽的资源池能力。
            subscription: 竞争消费组名称，None 创建独立订阅。
            max_steps: 节点触发次数上限，0 表示不限制。
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。

        Returns:
            本次操作得到的 Runtime 实例。
        """

        # 先启动本地消费者，再开放对应的事件路由。
        self.consume(queue, concurrency=concurrency, slots=slots)
        return self.route(
            event_type,
            graph=graph,
            queue=queue,
            subscription=subscription,
            max_steps=max_steps,
            timeout=timeout,
        )

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待 Router 与 Worker 的级联网络在当前实例内静止。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。

        Raises:
            TimeoutError: 等待未在指定时限内完成。
        """

        _validate_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            self.router.wait_idle(_remaining(deadline))
            self.worker.wait_idle(_remaining(deadline))
            if self.router.idle and self.worker.idle:
                return
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("Runtime did not become idle before timeout")

    def close(self) -> None:
        """排空组合角色，并关闭 Runtime 拥有的底层组件。"""

        with self._lock:
            if self._closed:
                return
        failure: BaseException | None = None
        try:
            self.wait_idle()
        except Exception as exc:  # noqa: BLE001
            failure = exc
        with self._lock:
            self._closed = True
        if self._plugin_host is None:
            failure = _close_components((self.worker, self.router), failure)
        else:
            failure = _close_components((self._plugin_host,), failure)
        if failure is not None:
            raise failure

    def __enter__(self):
        """进入资源作用域并返回当前句柄。

        Returns:
            当前资源管理对象。

        Raises:
            RuntimeClosedError: 当前运行时角色已经关闭。
        """

        with self._lock:
            if self._closed:
                raise RuntimeClosedError("Runtime is closed")
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """退出资源作用域，执行对应的关闭或卸载操作。

        Args:
            exc_type: 离开上下文时的异常类型，没有异常时为 None。
            exc_value: 离开上下文时的异常实例，没有异常时为 None。
            traceback: 离开上下文时的异常栈，没有异常时为 None。
        """

        del exc_type, traceback
        try:
            self.close()
        except Exception:
            if exc_value is None:
                raise
