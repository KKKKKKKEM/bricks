"""角色与资源池无需继承默认实现即可替换的结构化契约测试。"""

from collections import deque
from contextlib import contextmanager
from threading import Condition, RLock
import time

import pytest

from bricks import Graph, Node, Output, Runtime, Slot, SlotPool
from bricks.adapters import memory
from bricks.engine.observation import RuntimeEvent
from bricks.plugins import CAP_EVENT_ROUTER, CAP_GRAPH_WORKER, PluginDescriptor
from bricks.runtime import EventRouter, GraphWorker
from bricks.spi import RouterRole, SlotProvider, WorkerRole


class RouterWrapper:
    """当前契约测试使用的 RouterWrapper 替代实现。

    Attributes:
        router: 负责事件发布和工作路由的角色。
        closed: 测试组件是否已经关闭。
    """

    def __init__(self, router):
        """初始化实例及其依赖，建立当前对象独立维护的状态。

        Args:
            router: 事件路由角色，由 Runtime 管理生命周期。
        """

        self.router = router
        self.closed = False

    @property
    def idle(self):
        """判断当前组件是否没有尚未完成的工作。

        Returns:
            没有未完成工作时为 True，否则为 False。
        """

        return self.router.idle

    def observe(self, *args, **kwargs):
        """注册只读事件观察者，不建立目标 Graph 路由。

        Args:
            *args: 调用协议传入的位置参数。
            **kwargs: 传给目标接口的关键字参数。

        Returns:
            当前测试回调收集或构造的结果。
        """

        return self.router.observe(*args, **kwargs)

    def route(self, *args, **kwargs):
        """将事件类型连接到目标 Graph 和命名消费通道。

        Args:
            *args: 调用协议传入的位置参数。
            **kwargs: 传给目标接口的关键字参数。

        Returns:
            当前测试回调收集或构造的结果。
        """

        return self.router.route(*args, **kwargs)

    def emit(self, *args, **kwargs):
        """通过事件发布能力提交领域事件并返回已接受的事件。

        Args:
            *args: 调用协议传入的位置参数。
            **kwargs: 传给目标接口的关键字参数。

        Returns:
            事件传输已经接受的 Event 实例。
        """

        return self.router.emit(*args, **kwargs)

    def publish(self, event):
        """发布事件并推进对应的投递或观察流程。

        Args:
            event: 需要发布、观察或处理的事件。
        """

        self.router.publish(event)

    def observe_runtime(self, observer):
        """注册只读运行时生命周期观察者。

        Args:
            observer: 接收只读生命周期事件的观察者。

        Returns:
            当前测试回调收集或构造的结果。
        """

        return self.router.observe_runtime(observer)

    def wait_idle(self, timeout=None):
        """等待已接受的工作完成，并传播已记录的失败。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
        """

        self.router.wait_idle(timeout)

    def close(self):
        """结束当前组件的生命周期并释放其拥有的资源。"""

        self.closed = True
        self.router.close()


class WorkerWrapper:
    """当前契约测试使用的 WorkerWrapper 替代实现。

    Attributes:
        worker: 负责消费工作和执行 Graph 的角色。
        closed: 测试组件是否已经关闭。
    """

    def __init__(self, worker):
        """初始化实例及其依赖，建立当前对象独立维护的状态。

        Args:
            worker: 任务消费与 Graph 执行角色，由 Runtime 管理生命周期。
        """

        self.worker = worker
        self.closed = False

    @property
    def idle(self):
        """判断当前组件是否没有尚未完成的工作。

        Returns:
            没有未完成工作时为 True，否则为 False。
        """

        return self.worker.idle

    def register(self, *args, **kwargs):
        """注册具名定义，供后续装配或执行查找。

        Args:
            *args: 调用协议传入的位置参数。
            **kwargs: 传给目标接口的关键字参数。

        Returns:
            当前测试回调收集或构造的结果。
        """

        return self.worker.register(*args, **kwargs)

    def consume(self, *args, **kwargs):
        """为命名通道注册本地消费者与独立的执行并发限制。

        Args:
            *args: 调用协议传入的位置参数。
            **kwargs: 传给目标接口的关键字参数。

        Returns:
            当前测试回调收集或构造的结果。
        """

        return self.worker.consume(*args, **kwargs)

    def start(self, *args, **kwargs):
        """启动已经装配的组件或提交一次新的执行。

        Args:
            *args: 调用协议传入的位置参数。
            **kwargs: 传给目标接口的关键字参数。

        Returns:
            当前测试回调收集或构造的结果。
        """

        return self.worker.start(*args, **kwargs)

    def iter(self, *args, **kwargs):
        """启动 Graph 并返回同步终端输出流。

        Args:
            *args: 调用协议传入的位置参数。
            **kwargs: 传给目标接口的关键字参数。

        Returns:
            当前测试回调收集或构造的结果。
        """

        return self.worker.iter(*args, **kwargs)

    def aiter(self, *args, **kwargs):
        """启动 Graph 并返回异步终端输出流。

        Args:
            *args: 调用协议传入的位置参数。
            **kwargs: 传给目标接口的关键字参数。

        Returns:
            当前测试回调收集或构造的结果。
        """

        return self.worker.aiter(*args, **kwargs)

    def get_execution(self, execution_id):
        """按执行 ID 取得已保存的执行句柄。

        Args:
            execution_id: 已登记执行记录的唯一标识。

        Returns:
            当前测试回调收集或构造的结果。
        """

        return self.worker.get_execution(execution_id)

    def executions(self):
        """返回当前保存的执行记录快照。

        Returns:
            当前保存的 Execution 句柄快照。
        """

        return self.worker.executions()

    def attach(self, *args, **kwargs):
        """注册扩展回调并返回可卸载的句柄。

        Args:
            *args: 调用协议传入的位置参数。
            **kwargs: 传给目标接口的关键字参数。

        Returns:
            当前测试回调收集或构造的结果。
        """

        return self.worker.attach(*args, **kwargs)

    def contribute_hook(self, *args, **kwargs):
        """通过执行器公开扩展能力贡献节点 Hook。

        Args:
            *args: 调用协议传入的位置参数。
            **kwargs: 传给目标接口的关键字参数。

        Returns:
            当前测试回调收集或构造的结果。
        """

        return self.worker.contribute_hook(*args, **kwargs)

    def register_policy(self, *args, **kwargs):
        """注册带命名空间的输入选择策略。

        Args:
            *args: 调用协议传入的位置参数。
            **kwargs: 传给目标接口的关键字参数。

        Returns:
            当前测试回调收集或构造的结果。
        """

        return self.worker.register_policy(*args, **kwargs)

    def observe_runtime(self, observer):
        """注册只读运行时生命周期观察者。

        Args:
            observer: 接收只读生命周期事件的观察者。

        Returns:
            当前测试回调收集或构造的结果。
        """

        return self.worker.observe_runtime(observer)

    def wait_idle(self, timeout=None):
        """等待已接受的工作完成，并传播已记录的失败。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
        """

        self.worker.wait_idle(timeout)

    def close(self):
        """结束当前组件的生命周期并释放其拥有的资源。"""

        self.closed = True
        self.worker.close()


class Echo(Node):
    def execute(self, inputs, context):
        """执行当前测试场景的节点行为，供外层契约断言检查。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            测试节点或替代执行器产生的返回值。
        """

        return Output(inputs["default"])


@pytest.mark.parametrize("wrapped", ["router", "worker", "both"])
@pytest.mark.parametrize("plugin", [False, True])
def test_roles_can_be_independently_decorated_and_composed_without_inheritance(
    wrapped, plugin
):
    """验证角色无需继承默认类即可独立包装和组合。

    Args:
        wrapped: 当前用例使用的 wrapped 夹具或参数化输入。
        plugin: 当前插件实例或其声明身份。
    """

    tasks = memory.TaskBackend()
    router = EventRouter(publisher=tasks)
    worker = GraphWorker(
        consumer=tasks, emit=router.publish, emit_local=router.publish_local
    )
    selected_router = RouterWrapper(router) if wrapped in ("router", "both") else router
    selected_worker = WorkerWrapper(worker) if wrapped in ("worker", "both") else worker
    assert isinstance(selected_router, RouterRole)
    assert isinstance(selected_worker, WorkerRole)

    class Roles:
        """当前契约测试使用的 Roles 替代实现。

        Attributes:
            descriptor: 插件身份、依赖和能力声明。
        """

        descriptor = PluginDescriptor(
            "example/roles", "1", provides=(CAP_EVENT_ROUTER, CAP_GRAPH_WORKER)
        )

        def setup(self, context):
            """在插件装配阶段登记声明的能力或贡献。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            context.provide(CAP_EVENT_ROUTER, selected_router)
            context.provide(CAP_GRAPH_WORKER, selected_worker)

        def start(self, context):
            """启动已经装配的组件或提交一次新的执行。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            pass

        def stop(self, context):
            """停止插件并释放其拥有的资源。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            selected_worker.close()
            selected_router.close()

    runtime = (
        Runtime(plugins=(Roles(),))
        if plugin
        else Runtime(router=selected_router, worker=selected_worker)
    )
    observations: list[RuntimeEvent] = []
    try:
        runtime.observe_runtime(lambda event: observations.append(event))
        runtime.register("echo", Graph(entrypoint="node").add(node=Echo()))
        assert runtime.run("echo", 1) == (Output(1),)
        assert tuple(runtime.iter("echo", 2)) == (Output(2),)
        runtime.on("event", graph="echo", queue="work")
        runtime.emit("event", 3)
        runtime.wait_idle(2)
        assert runtime.executions()[-1].result() == (Output(3),)
        assert observations
    finally:
        runtime.close()
        tasks.close()
    if wrapped in ("router", "both"):
        assert isinstance(selected_router, RouterWrapper)
        assert selected_router.closed
    if wrapped in ("worker", "both"):
        assert isinstance(selected_worker, WorkerWrapper)
        assert selected_worker.closed


class ResourceLease:
    """当前契约测试使用的 ResourceLease 替代实现。

    Attributes:
        provider: lease 归还资源时使用的测试资源提供者。
        slot: 测试执行链共享的本地资源槽。
        references: 测试 lease 尚未归还的引用数量。
        lock: 保护测试组件共享状态的锁。
        execution_lock: 测试 lease 保证串行执行所用的锁。
    """

    def __init__(self, provider, slot):
        """初始化实例及其依赖，建立当前对象独立维护的状态。

        Args:
            provider: 当前用例使用的 provider 夹具或参数化输入。
            slot: 当前逻辑执行链使用的本地执行槽。
        """

        self.provider = provider
        self.slot = slot
        self.references = 1
        self.lock = RLock()
        self.execution_lock = RLock()

    def retain(self):
        """为同一逻辑链新增一个必须由接管方释放的引用。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
        """

        with self.lock:
            if not self.references:
                raise RuntimeError("released")
            self.references += 1

    def release(self):
        """释放当前持有的 lease 引用，最后一个引用结束时归还 Slot。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
        """

        with self.lock:
            if not self.references:
                raise RuntimeError("released")
            self.references -= 1
            last = not self.references
        if last:
            self.provider.put(self.slot)

    @contextmanager
    def execution(self):
        """在 lease 保护下串行使用同一个 Slot 的执行资源。

        Yields:
            在引用保护与串行锁内使用的测试 Slot。
        """

        self.retain()
        try:
            with self.execution_lock:
                yield self.slot
        finally:
            self.release()


class ResourcePool:
    """当前契约测试使用的 ResourcePool 替代实现。

    Attributes:
        size: 测试资源池的固定容量。
        slots: 消费者申请根执行链资源所用的 SlotProvider。
        condition: 测试组件用于等待通知的条件变量。
        listeners: 测试资源提供者登记的可用通知回调。
        closed: 测试组件是否已经关闭。
    """

    def __init__(self, size):
        """初始化实例及其依赖，建立当前对象独立维护的状态。

        Args:
            size: 资源池容量或本次断言使用的预期字节数。
        """

        self.size = size
        self.slots = deque(Slot() for _ in range(size))
        self.condition = Condition()
        self.listeners = []
        self.closed = False

    @property
    def available(self):
        """返回当前可申请的执行槽数量。

        Returns:
            当前可供申请的空闲 Slot 数量。
        """

        with self.condition:
            return len(self.slots)

    def try_acquire(self):
        """尝试立即取得 Slot lease，无可用资源时返回 None。

        Returns:
            调用方负责释放的 Slot lease；没有空闲资源时返回 None。

        Raises:
            RuntimeError: 当前生命周期状态或操作顺序不允许此操作。
        """

        with self.condition:
            if self.closed:
                raise RuntimeError("closed")
            return ResourceLease(self, self.slots.popleft()) if self.slots else None

    def acquire(self, timeout=None):
        """等待可用的本地 Slot，并返回调用方负责释放的 lease。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。

        Returns:
            调用方负责释放的 Slot lease。

        Raises:
            TimeoutError: 等待未在指定时限内完成。
        """

        deadline = None if timeout is None else time.monotonic() + timeout
        with self.condition:
            while True:
                lease = self.try_acquire()
                if lease is not None:
                    return lease
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError()
                self.condition.wait(remaining)

    def subscribe_available(self, listener):
        """注册资源可用回调并返回取消订阅函数。

        Args:
            listener: 资源可用时调用的通知函数。

        Returns:
            用于取消当前可用通知订阅的函数。
        """

        self.listeners.append(listener)

        def detach():
            """解除当前句柄对应的注册关系。"""

            if listener in self.listeners:
                self.listeners.remove(listener)

        return detach

    def put(self, slot):
        """将使用结束的测试资源放回资源池。

        Args:
            slot: 当前逻辑执行链使用的本地执行槽。
        """

        with self.condition:
            self.slots.append(slot)
            self.condition.notify_all()
        for listener in tuple(self.listeners):
            listener()

    def close(self):
        """结束当前组件的生命周期并释放其拥有的资源。"""

        with self.condition:
            self.closed = True
            self.condition.notify_all()


def test_non_default_slot_provider_and_lease_preserve_cross_graph_resources():
    """验证第三方 SlotProvider 与 lease 保留跨图本地资源。"""

    pool = ResourcePool(1)
    assert isinstance(pool, SlotProvider) and not isinstance(pool, SlotPool)
    seen = []

    class Source(Node):
        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。
            """

            assert context.slot is not None
            context.slot["value"] = inputs["default"]
            seen.append(context.slot)
            context.emit("next")

    class Target(Node):
        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            assert context.slot is not None
            seen.append(context.slot)
            return Output(context.slot["value"])

    with Runtime() as runtime:
        runtime.register("source", Graph(entrypoint="node").add(node=Source()))
        runtime.register("target", Graph(entrypoint="node").add(node=Target()))
        runtime.on("root", graph="source", queue="roots", slots=pool)
        runtime.on("next", graph="target", queue="targets", slots=pool)
        runtime.emit("root", 42)
        runtime.wait_idle(2)
        assert seen[0] is seen[1]
        assert runtime.executions()[-1].result() == (Output(42),)
        assert pool.available == 1
    assert not pool.closed
    pool.close()
