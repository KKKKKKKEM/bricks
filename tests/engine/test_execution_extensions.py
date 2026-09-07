"""执行器、输出存储和通知实现共享的公共契约测试。"""

import asyncio
import pickle
import sqlite3
from collections.abc import Awaitable
from typing import Any, cast
from threading import Event as ThreadEvent

import pytest

from bricks import Context, Execution, ExecutionLimits, Graph, Node, Output, Runtime
from bricks.engine.errors import ExecutionCancelledError, ExecutionTimeoutError
from bricks.engine.execution_resources import LocalExecutionNotifier
from bricks.plugins import CAP_EXECUTION_FACTORY, PluginDescriptor
from bricks.runtime import LocalRuntimePlugin


class Produce(Node):
    def execute(self, inputs, context):
        """执行当前测试场景的节点行为，供外层契约断言检查。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            测试节点或替代执行器产生的返回值。
        """

        return tuple(Output(index) for index in range(inputs["default"]))


def graph():
    """构造当前契约测试使用的 Graph。

    Returns:
        供当前测试注册和执行的 Graph。
    """

    return Graph(entrypoint="produce").add(produce=Produce())


class SyncExecutor:
    """当前契约测试使用的 SyncExecutor 替代实现。

    Attributes:
        fail: 是否在当前测试组件中触发预设失败。
        gate: 协调测试线程或异步任务推进顺序的同步信号。
        closed: 测试组件是否已经关闭。
    """

    def __init__(self, *, fail=False, gate=None):
        """初始化实例及其依赖，建立当前对象独立维护的状态。

        Args:
            fail: 当前用例使用的 fail 夹具或参数化输入。
            gate: 当前用例使用的 gate 夹具或参数化输入。
        """

        self.fail = fail
        self.gate = gate
        self.closed = False

    def execute(
        self, name, graph, inputs, emit, plan=None, *, slot=None, execution
    ) -> Awaitable[None] | None:
        """执行当前测试 Graph，并通过 Execution 的公开接口交付输出。

        Args:
            name: 注册或查找使用的名称。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            inputs: 入口数据或按端口名称组织的输入映射。
            emit: 发布跨图事件的回调。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            slot: 当前逻辑执行链使用的本地执行槽。
            execution: 记录当前执行状态、控制限制及输出的句柄。

        Raises:
            ValueError: 参数值或字段组合不合法。
        """

        assert graph.frozen and len(graph.nodes) == 1
        assert graph.spec_for(graph.entrypoint).input_ports["default"] is object
        assert graph.outgoing_for(graph.entrypoint, "default") == ()
        if plan is not None:
            assert plan.graph is graph
        with execution.step(graph.entrypoint):
            outputs = graph.nodes[graph.entrypoint].execute(
                {"default": inputs},
                Context(emit, slot, checkpoint=execution.checkpoint),
            )
        for output in outputs:
            execution.publish_output(output)
            if self.gate is not None:
                while not self.gate.wait(0.005):
                    execution.checkpoint()
            if self.fail:
                raise ValueError("executor failed after output")
        return None

    def close(self):
        """结束当前组件的生命周期并释放其拥有的资源。"""

        self.closed = True


class AsyncExecutor(SyncExecutor):
    async def execute(
        self, name, graph, inputs, emit, plan=None, *, slot=None, execution
    ):
        """执行当前测试 Graph，并通过 Execution 的公开接口交付输出。

        Args:
            name: 注册或查找使用的名称。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            inputs: 入口数据或按端口名称组织的输入映射。
            emit: 发布跨图事件的回调。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            slot: 当前逻辑执行链使用的本地执行槽。
            execution: 记录当前执行状态、控制限制及输出的句柄。

        Raises:
            ValueError: 参数值或字段组合不合法。
        """

        assert graph.frozen
        with execution.step(graph.entrypoint):
            outputs = graph.nodes[graph.entrypoint].execute(
                {"default": inputs},
                Context(emit, slot, checkpoint=execution.checkpoint),
            )
        for output in outputs:
            await execution.apublish_output(output)
            if self.gate is not None:
                while not self.gate.is_set():
                    await asyncio.sleep(0.005)
            if self.fail:
                raise ValueError("executor failed after output")


@pytest.mark.parametrize("executor_type", [SyncExecutor, AsyncExecutor])
@pytest.mark.parametrize("fail", [False, True])
def test_alternate_executor_publishes_before_completion_and_preserves_failure(
    executor_type, fail
):
    """验证替代执行器可提前发布输出且保留最终失败。

    Args:
        executor_type: 当前用例使用的 executor_type 夹具或参数化输入。
        fail: 当前用例使用的 fail 夹具或参数化输入。
    """

    gate = ThreadEvent()
    executor = executor_type(fail=fail, gate=gate)
    with Runtime(plugins=(LocalRuntimePlugin(executor=executor),)) as runtime:
        runtime.register("work", graph())
        execution = runtime.start("work", 3, output_buffer=1)
        stream = iter(execution)
        try:
            assert next(stream) == Output(0)
            assert not execution.done
        finally:
            gate.set()
        if fail:
            with pytest.raises(ValueError, match="after output"):
                tuple(stream)
            with pytest.raises(ValueError, match="after output"):
                execution.result()
        else:
            assert tuple(stream) == (Output(1), Output(2))
            assert (
                execution.result()
                == tuple(execution)
                == tuple(Output(i) for i in range(3))
            )
    assert not executor.closed


@pytest.mark.parametrize("executor_type", [SyncExecutor, AsyncExecutor])
def test_custom_executor_async_stream_has_the_same_backpressure_contract(executor_type):
    """验证自定义执行器遵守相同的异步流背压契约。

    Args:
        executor_type: 当前用例使用的 executor_type 夹具或参数化输入。
    """

    with Runtime(plugins=(LocalRuntimePlugin(executor=executor_type()),)) as runtime:
        runtime.register("work", graph())

        async def consume():
            """为命名通道注册本地消费者与独立的执行并发限制。

            Returns:
                当前测试回调收集或构造的结果。
            """

            seen = []
            async for output in runtime.aiter("work", 30, output_buffer=1):
                seen.append(output)
                await asyncio.sleep(0)
            return tuple(seen)

        assert asyncio.run(consume()) == tuple(Output(i) for i in range(30))


@pytest.mark.parametrize("control", ["cancel", "timeout"])
def test_async_executor_cleanup_precedes_terminal_status(control):
    """验证异步执行器清理完成后才进入终态。

    Args:
        control: 当前用例使用的 control 夹具或参数化输入。
    """

    entered, cleaning, release = ThreadEvent(), ThreadEvent(), ThreadEvent()

    class Slow(AsyncExecutor):
        async def execute(
            self, name, graph, inputs, emit, plan=None, *, slot=None, execution
        ):
            """执行当前测试 Graph，并通过 Execution 的公开接口交付输出。

            Args:
                name: 注册或查找使用的名称。
                graph: 目标 Graph 定义或其注册名称，以类型声明为准。
                inputs: 入口数据或按端口名称组织的输入映射。
                emit: 发布跨图事件的回调。
                plan: 限定本次执行范围的计划，None 使用完整 Graph。
                slot: 当前逻辑执行链使用的本地执行槽。
                execution: 记录当前执行状态、控制限制及输出的句柄。
            """

            with execution.step(graph.entrypoint):
                entered.set()
                try:
                    await asyncio.sleep(60)
                finally:
                    cleaning.set()
                    while not release.is_set():
                        await asyncio.sleep(0.005)

    with Runtime(plugins=(LocalRuntimePlugin(executor=Slow()),)) as runtime:
        runtime.register("work", graph())
        execution = runtime.start(
            "work", 1, timeout=0.05 if control == "timeout" else None
        )
        try:
            assert entered.wait(1)
            if control == "cancel":
                execution.cancel()
            assert cleaning.wait(1)
            assert not execution.wait(0.05)
        finally:
            release.set()
        with pytest.raises(
            ExecutionCancelledError if control == "cancel" else ExecutionTimeoutError
        ):
            execution.result(2)


class DiskStore:
    """当前契约测试使用的 DiskStore 替代实现。

    Attributes:
        path: 测试持久存储使用的文件路径。
        reads: 测试存储记录的读取次数。
    """

    def __init__(self, path):
        """初始化实例及其依赖，建立当前对象独立维护的状态。

        Args:
            path: Cookie 路径或测试使用的文件路径。
        """

        self.path = path
        self.reads = 0
        with self.connect() as connection:
            connection.execute(
                "CREATE TABLE outputs (id INTEGER PRIMARY KEY, value BLOB)"
            )

    def connect(self):
        # 每次操作独立管理连接，保留 Execution 不需要持续占用连接。
        """为测试输出存储创建独立数据库连接。

        Returns:
            当前测试存储的独立 SQLite 连接。
        """

        from contextlib import contextmanager

        @contextmanager
        def connection():
            """在限定作用域内打开并关闭测试数据库连接。

            Yields:
                本次操作独立拥有的 SQLite 连接。
            """

            conn = sqlite3.connect(self.path)
            try:
                with conn:
                    yield conn
            finally:
                conn.close()

        return connection()

    def __len__(self):
        """返回当前容器中保存的条目数量。

        Returns:
            当前容器条目数量。
        """

        with self.connect() as connection:
            return connection.execute("SELECT count(*) FROM outputs").fetchone()[0]

    def __getitem__(self, index):
        """按索引或名称读取当前容器中的值。

        Args:
            index: 条目的索引或切片。

        Returns:
            指定位置或名称对应的值；切片行为由当前容器定义。

        Raises:
            IndexError: 指定索引超出当前容器范围。
        """

        self.reads += 1
        with self.connect() as connection:
            row = connection.execute(
                "SELECT value FROM outputs WHERE id = ?", (index + 1,)
            ).fetchone()
        if row is None:
            raise IndexError(index)
        return pickle.loads(row[0])

    def append(self, output):
        """按追加顺序保存一项输出。

        Args:
            output: 需要校验、保存或交付的一项 Output。
        """

        with self.connect() as connection:
            connection.execute(
                "INSERT INTO outputs(value) VALUES (?)", (pickle.dumps(output),)
            )


class Notifier:
    """当前契约测试使用的 Notifier 替代实现。

    Attributes:
        delegate: 实际执行工作的被包装实现。
        sync_waits: 测试通知器记录的同步等待调用次数。
        async_waits: 测试通知器记录的异步等待调用次数。
    """

    def __init__(self):
        """初始化实例及其依赖，建立当前对象独立维护的状态。"""

        self.delegate = LocalExecutionNotifier()
        self.sync_waits = self.async_waits = 0

    @property
    def version(self):
        """返回通知器当前的单调递增版本。

        Returns:
            当前通知版本，可用于识别是否发生了新的通知。
        """

        return self.delegate.version

    def notify(self):
        """推进通知版本并唤醒同步及异步等待方。"""

        self.delegate.notify()

    def wait(self, version, timeout=None):
        """等待通知版本变化或等待时限结束。

        Args:
            version: 等待开始前观察到的通知版本。
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
        """

        self.sync_waits += 1
        self.delegate.wait(version, timeout)

    async def wait_async(self, version):
        """异步等待通知版本变化，不占用同步等待线程。

        Args:
            version: 等待开始前观察到的通知版本。
        """

        self.async_waits += 1
        await self.delegate.wait_async(version)


def test_plugin_execution_factory_replaces_storage_for_direct_and_queued_work(tmp_path):
    """验证插件执行工厂能替换直接与排队工作的输出存储。

    Args:
        tmp_path: 当前用例使用的 tmp_path 夹具或参数化输入。
    """

    stores: list[DiskStore] = []

    def factory(name, *, limits, id=None, output_buffer=64):
        """按测试要求创建带替代存储或控制配置的执行句柄。

        Args:
            name: 注册或查找使用的名称。
            limits: 本次执行独立使用的步数和时长限制。
            id: 对象标识，允许缺省时由实现生成。
            output_buffer: 每个活跃输出流允许积压的输出条数。

        Returns:
            注入测试指定资源与限制的 Execution。
        """

        store = DiskStore(tmp_path / f"{len(stores)}.sqlite")
        stores.append(store)
        return Execution(
            name,
            limits=limits,
            id=id,
            output_buffer=output_buffer,
            output_store=store,
            notifier=Notifier(),
        )

    class Resources:
        """当前契约测试使用的 Resources 替代实现。

        Attributes:
            descriptor: 插件身份、依赖和能力声明。
        """

        descriptor = PluginDescriptor(
            "example/execution", "1", provides=(CAP_EXECUTION_FACTORY,)
        )

        def setup(self, context):
            """在插件装配阶段登记声明的能力或贡献。

            Args:
                context: 当前调用的执行或插件上下文。
            """

            context.provide(CAP_EXECUTION_FACTORY, factory)

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

            pass

    with Runtime(plugins=(Resources(),)) as runtime:
        runtime.register("work", graph())
        execution = runtime.start("work", 20)
        assert execution.wait(3)
        assert stores[0].reads == 0
        assert tuple(execution) == tuple(Output(i) for i in range(20))
        assert stores[0].reads == 20
        assert execution.result() == tuple(Output(i) for i in range(20))
        runtime.on("run", graph="work", queue="queue")
        runtime.emit("run", 5)
        runtime.wait_idle(3)
        assert len(stores) == 2 and stores[1].reads == 0
        queued = runtime.executions()[-1]
        assert queued.result() == tuple(Output(i) for i in range(5))


def test_notifier_wakes_async_result_and_stream_without_waiting_threads(monkeypatch):
    """验证通知器唤醒异步结果和输出流时不占用等待线程。

    Args:
        monkeypatch: 当前用例使用的 monkeypatch 夹具或参数化输入。
    """

    async def forbidden(*args, **kwargs):
        """拒绝进入不应被调用的实现路径。

        Args:
            *args: 调用协议传入的位置参数。
            **kwargs: 传给目标接口的关键字参数。

        Raises:
            AssertionError: 内部不变量或测试契约断言不成立。
        """

        raise AssertionError("async waits must not consume a worker thread")

    monkeypatch.setattr(asyncio, "to_thread", forbidden)
    notifier = Notifier()
    execution = Execution("work", notifier=notifier, output_buffer=1)
    execution.start(graph().freeze())

    async def scenario():
        """组织当前测试的异步调用顺序与结果断言。"""

        stream = execution.__aiter__()

        async def result_waiter():
            """等待执行最终结果，验证通知器的唤醒行为。

            Returns:
                当前测试回调收集或构造的结果。
            """

            return await execution

        result = asyncio.create_task(result_waiter())
        first = asyncio.ensure_future(stream.__anext__())
        await asyncio.sleep(0)
        await execution.apublish_output(Output(1))
        assert await first == Output(1)
        execution.succeed()
        with pytest.raises(StopAsyncIteration):
            await stream.__anext__()
        assert await result == (Output(1),)

    asyncio.run(scenario())
    assert notifier.async_waits >= 2


def test_notifier_does_not_lose_notification_before_wait():
    """验证等待开始前发生的通知不会丢失。"""

    notifier = LocalExecutionNotifier()
    old = notifier.version
    notifier.notify()
    notifier.wait(old, 0)
    asyncio.run(asyncio.wait_for(notifier.wait_async(old), 0.1))


def test_public_execution_control_rejects_invalid_lifecycle_and_outputs():
    """验证公开执行接口拒绝非法状态转换和输出。"""

    execution = Execution("work")
    with pytest.raises(RuntimeError, match="RUNNING"):
        execution.publish_output(Output(1))
    with pytest.raises(TypeError, match="frozen"):
        execution.start(graph())
    execution.start(graph().freeze())
    with pytest.raises(TypeError, match="only Output"):
        execution.publish_output(cast(Any, 1))  # 验证运行时拒绝非 Output 值。
    execution.publish_output(Output(2))
    execution.succeed()
    with pytest.raises(RuntimeError, match="RUNNING"):
        execution.publish_output(Output(3))
    assert execution.result() == (Output(2),)


def test_execution_factory_cannot_silently_change_limits():
    """验证替代执行工厂不能静默改变执行限制。"""

    def broken(graph, **kwargs):
        """抛出测试指定的异常以验证失败传播与清理。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            **kwargs: 传给目标接口的关键字参数。

        Returns:
            当前测试回调收集或构造的结果。
        """

        return Execution(graph, limits=ExecutionLimits(max_steps=999))

    with Runtime(plugins=(LocalRuntimePlugin(execution_factory=broken),)) as runtime:
        runtime.register("work", graph())
        with pytest.raises(ValueError, match="changed the execution contract"):
            runtime.start("work", 1, max_steps=1)


def test_executor_cannot_return_a_second_result_channel():
    """验证执行器不能通过返回值建立第二条输出通道。"""

    class Invalid(SyncExecutor):
        def execute(
            self, name, graph, inputs, emit, plan=None, *, slot=None, execution
        ):
            """执行当前测试 Graph，并通过 Execution 的公开接口交付输出。

            Args:
                name: 注册或查找使用的名称。
                graph: 目标 Graph 定义或其注册名称，以类型声明为准。
                inputs: 入口数据或按端口名称组织的输入映射。
                emit: 发布跨图事件的回调。
                plan: 限定本次执行范围的计划，None 使用完整 Graph。
                slot: 当前逻辑执行链使用的本地执行槽。
                execution: 记录当前执行状态、控制限制及输出的句柄。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            execution.publish_output(Output("accepted"))
            return cast(
                Any, (Output("not accepted"),)
            )  # 验证执行器拒绝第二条结果通道。

    with Runtime(plugins=(LocalRuntimePlugin(executor=Invalid()),)) as runtime:
        runtime.register("work", graph())
        stream = runtime.iter("work", 1)
        assert next(stream) == Output("accepted")
        with pytest.raises(TypeError, match="return None"):
            next(stream)


def test_host_rejects_foreign_plan_before_calling_alternate_executor():
    """验证宿主在调用替代执行器前拒绝其他 Graph 的计划。"""

    class NeverCalled(SyncExecutor):
        def execute(self, *args, **kwargs):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                *args: 调用协议传入的位置参数。
                **kwargs: 传给目标接口的关键字参数。

            Raises:
                AssertionError: 内部不变量或测试契约断言不成立。
            """

            raise AssertionError("foreign plan reached executor")

    foreign = graph().plan(include={"produce"})
    with Runtime(plugins=(LocalRuntimePlugin(executor=NeverCalled()),)) as runtime:
        runtime.register("work", graph())
        with pytest.raises(ValueError, match="different Graph"):
            runtime.run("work", 1, plan=foreign)


def test_output_store_failure_preserves_previously_accepted_outputs():
    """验证存储失败不抹除此前已接受的输出。"""

    class FailingStore:
        """当前契约测试使用的 FailingStore 替代实现。

        Attributes:
            items: 测试存储持有的输出条目。
        """

        def __init__(self):
            """初始化实例及其依赖，建立当前对象独立维护的状态。"""

            self.items = []

        def __len__(self):
            """返回当前容器中保存的条目数量。

            Returns:
                当前容器条目数量。
            """

            return len(self.items)

        def __getitem__(self, index):
            """按追加索引读取已接受的 Output。

            Args:
                index: 已追加输出的整数索引。

            Returns:
                指定位置或名称对应的值；切片行为由当前容器定义。
            """

            return self.items[index]

        def append(self, output):
            """按追加顺序保存一项输出。

            Args:
                output: 需要校验、保存或交付的一项 Output。
            """

            if self.items:
                raise OSError("storage unavailable")
            self.items.append(output)

    def factory(graph, **kwargs):
        """按测试要求创建带替代存储或控制配置的执行句柄。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            **kwargs: 传给目标接口的关键字参数。

        Returns:
            注入测试指定资源与限制的 Execution。
        """

        return Execution(graph, **kwargs, output_store=FailingStore())

    with Runtime(plugins=(LocalRuntimePlugin(execution_factory=factory),)) as runtime:
        runtime.register("work", graph())
        stream = runtime.iter("work", 2)
        assert next(stream) == Output(0)
        with pytest.raises(OSError, match="storage unavailable"):
            next(stream)


def test_cancelling_a_notifier_wait_does_not_cancel_other_waiters():
    """验证取消一个通知等待方不影响其他等待方。"""

    notifier = LocalExecutionNotifier()

    async def scenario():
        """组织当前测试的异步调用顺序与结果断言。"""

        first = asyncio.create_task(notifier.wait_async(notifier.version))
        second = asyncio.create_task(notifier.wait_async(notifier.version))
        await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert not second.done()
        notifier.notify()
        await asyncio.wait_for(second, 1)

    asyncio.run(scenario())
