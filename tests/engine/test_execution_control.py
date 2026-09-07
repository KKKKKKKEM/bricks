"""Execution 身份、步数、超时和协作式取消的契约测试。"""

from __future__ import annotations

import asyncio
import time
from threading import Event as ThreadEvent
from threading import Lock, Thread

import pytest

from bricks import (
    AsyncNode,
    Context,
    ExecutionLimits,
    ExecutionStatus,
    Graph,
    Node,
    Output,
    Ports,
    Runtime,
    SlotPool,
)
from bricks.engine.errors import (
    BricksRuntimeError,
    ExecutionCancelledError,
    ExecutionError,
    ExecutionTimeoutError,
    NodeTimeoutError,
    StepLimitExceededError,
)
from bricks.engine.hooks import NodeHook
from bricks.runtime import GraphWorker


@pytest.mark.parametrize("control", ["cancel", "timeout"])
def test_async_cleanup_finishes_before_execution_releases_slot(control) -> None:
    """验证异步清理完成后才释放执行链 Slot。

    Args:
        control: 当前用例使用的 control 夹具或参数化输入。
    """

    started, cleaning, release, finished, next_started = (
        ThreadEvent() for _ in range(5)
    )
    seen_slots = []

    class Slow(AsyncNode):
        async def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。
            """

            seen_slots.append(context.slot)
            started.set()
            try:
                await asyncio.sleep(60)
            finally:
                cleaning.set()
                while not release.is_set():
                    await asyncio.sleep(0.005)
                finished.set()

    class Next(Node):
        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。
            """

            assert finished.is_set()
            seen_slots.append(context.slot)
            next_started.set()

    slots = SlotPool(1)
    runtime = Runtime()
    try:
        runtime.register("slow", Graph(entrypoint="node").add(node=Slow()))
        runtime.register("next", Graph(entrypoint="node").add(node=Next()))
        runtime.on(
            "slow",
            graph="slow",
            queue="work",
            concurrency=2,
            slots=slots,
            timeout=0.1 if control == "timeout" else None,
        )
        runtime.on("next", graph="next", queue="work", concurrency=2, slots=slots)
        runtime.emit("slow")
        assert started.wait(2)
        execution = runtime.executions()[0]
        if control == "cancel":
            execution.cancel()
        assert cleaning.wait(2)
        runtime.emit("next")
        assert not execution.wait(0.1)
        assert slots.available == 0
        assert not next_started.is_set()
        release.set()
        assert next_started.wait(2)
        error = (
            ExecutionCancelledError if control == "cancel" else ExecutionTimeoutError
        )
        with pytest.raises(error):
            runtime.wait_idle(2)
        assert seen_slots[0] is seen_slots[1]
        assert slots.available == 1
    finally:
        release.set()
        try:
            runtime.close()
        except (ExecutionCancelledError, ExecutionTimeoutError):
            pass
        slots.close()


class Increment(Node):
    """当前契约测试使用的 Increment 替代实现。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
    """

    input_ports = Ports(value=int)
    output_ports = Ports(value=int)

    def execute(self, inputs, context: Context) -> Output:
        """执行当前测试场景的节点行为，供外层契约断言检查。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            测试节点或替代执行器产生的返回值。
        """

        del context
        return Output(inputs["value"] + 1, "value")


def looping_graph() -> Graph:
    """构造具有回边的计步测试 Graph。

    Returns:
        包含递增节点与回边的 Graph。
    """

    return (
        Graph(entrypoint="increment")
        .add(increment=Increment())
        .connect("increment", "increment", source_port="value", target_port="value")
    )


def test_execution_limits_default_to_unlimited() -> None:
    """验证默认执行限制不限制步数和时长。"""

    limits = ExecutionLimits()

    assert limits.max_steps == 0
    assert limits.timeout is None


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"max_steps": -1}, ValueError),
        ({"max_steps": 1.5}, TypeError),
        ({"timeout": 0}, ValueError),
        ({"timeout": float("nan")}, ValueError),
    ],
)
def test_execution_limits_reject_invalid_values(kwargs, error) -> None:
    """验证执行限制拒绝非法类型和值。

    Args:
        kwargs: 传给目标接口的关键字参数。
        error: 需要传播、记录或用于恢复的异常。
    """

    with pytest.raises(error):
        ExecutionLimits(**kwargs)


def test_max_steps_counts_node_firings_and_preserves_execution_state() -> None:
    """验证步数按节点触发计数且失败后保留执行状态。"""

    runtime = Runtime()
    runtime.register("loop.graph", looping_graph())

    with pytest.raises(StepLimitExceededError, match="max_steps=3"):
        runtime.run("loop.graph", 0, max_steps=3)

    execution = runtime.executions()[-1]
    assert execution.steps == 3
    assert execution.status is ExecutionStatus.STEP_LIMITED
    assert isinstance(execution.error, StepLimitExceededError)
    assert runtime.get_execution(execution.id) is execution
    runtime.close()


def test_max_steps_allows_exact_boundary() -> None:
    """验证恰好达到步数上限的执行能够成功。"""

    with Runtime() as runtime:
        runtime.register(
            "increment.graph",
            Graph(entrypoint="increment").add(increment=Increment()),
        )
        assert runtime.run("increment.graph", 1, max_steps=1) == (Output(2, "value"),)
        assert runtime.executions()[-1].steps == 1


def test_zero_max_steps_keeps_existing_unlimited_behavior() -> None:
    """验证零步数上限表示无限制。"""

    class StopAt(Node):
        """当前契约测试使用的 StopAt 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(again=int, done=int)

        def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            value = inputs["value"]
            return Output(value + 1, "again" if value < 20 else "done")

    graph = (
        Graph(entrypoint="counter")
        .add(counter=StopAt())
        .connect("counter", "counter", source_port="again", target_port="value")
    )
    with Runtime() as runtime:
        runtime.register("counter.graph", graph)
        assert runtime.run("counter.graph", 0, max_steps=0) == (Output(21, "done"),)
        assert runtime.executions()[-1].steps == 21


def test_start_returns_successful_queryable_execution() -> None:
    """验证启动接口返回可查询的成功执行句柄。"""

    with Runtime() as runtime:
        runtime.register(
            "increment.graph",
            Graph(entrypoint="increment").add(increment=Increment()),
        )
        execution = runtime.start("increment.graph", 2)

        assert execution.result(1) == (Output(3, "value"),)
        assert execution.status is ExecutionStatus.SUCCEEDED
        assert execution.started_at is not None
        assert execution.finished_at is not None
        assert execution.steps == 1
        assert runtime.get_execution(execution.id) is execution


def test_execution_is_awaitable() -> None:
    """验证可以直接 await Execution 取得结果。"""

    async def scenario() -> tuple[Output, ...]:
        """组织当前测试的异步调用顺序与结果断言。

        Returns:
            符合声明端口契约的 Output 集合。
        """

        with Runtime() as runtime:
            runtime.register(
                "increment.graph",
                Graph(entrypoint="increment").add(increment=Increment()),
            )
            return await runtime.start("increment.graph", 2)

    assert asyncio.run(scenario()) == (Output(3, "value"),)


def test_runtime_iter_streams_terminal_outputs_and_keeps_final_result() -> None:
    """验证同步输出流与最终完整结果共享输出记录。"""

    class Many(Node):
        """当前契约测试使用的 Many 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(count=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            return tuple(Output(value, "value") for value in range(inputs["count"]))

    with Runtime() as runtime:
        runtime.register("many.graph", Graph(entrypoint="many").add(many=Many()))
        execution = runtime.start("many.graph", 3)

        assert tuple(execution) == (
            Output(0, "value"),
            Output(1, "value"),
            Output(2, "value"),
        )
        assert execution.result() == tuple(execution)
        assert tuple(runtime.iter("many.graph", 2)) == (
            Output(0, "value"),
            Output(1, "value"),
        )


def test_async_output_iteration() -> None:
    """验证异步迭代按产生顺序交付输出。"""

    class Many(Node):
        """当前契约测试使用的 Many 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(count=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            return tuple(Output(value, "value") for value in range(inputs["count"]))

    async def scenario() -> list[Output]:
        """组织当前测试的异步调用顺序与结果断言。

        Returns:
            符合声明端口契约的 Output。
        """

        with Runtime() as runtime:
            runtime.register("many.graph", Graph(entrypoint="many").add(many=Many()))
            return [output async for output in runtime.aiter("many.graph", 3)]

    assert asyncio.run(scenario()) == [
        Output(0, "value"),
        Output(1, "value"),
        Output(2, "value"),
    ]


def test_stream_yields_committed_outputs_before_later_failure() -> None:
    """验证后续失败不撤回已经交付的输出。"""

    class EmitThenFail(Node):
        """当前契约测试使用的 EmitThenFail 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(result=int, next=int)

        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            return Output(inputs["value"], "result"), Output(inputs["value"], "next")

    class Fail(Node):
        """当前契约测试使用的 Fail 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Raises:
                ValueError: 参数值或字段组合不合法。
            """

            del inputs, context
            raise ValueError("after output")

    graph = (
        Graph(entrypoint="source")
        .add(source=EmitThenFail(), fail=Fail())
        .connect("source", "fail", source_port="next", target_port="value")
    )
    runtime = Runtime()
    runtime.register("partial.graph", graph)
    stream = runtime.iter("partial.graph", 7)

    assert next(stream) == Output(7, "result")
    with pytest.raises(ExecutionError, match="after output"):
        next(stream)
    runtime.close()


def test_active_stream_applies_bounded_backpressure() -> None:
    """验证活跃输出订阅对生产者施加有界背压。"""

    class Many(Node):
        """当前契约测试使用的 Many 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(count=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            return tuple(Output(value, "value") for value in range(inputs["count"]))

    with Runtime() as runtime:
        runtime.register("many.graph", Graph(entrypoint="many").add(many=Many()))
        execution = runtime.start("many.graph", 3, output_buffer=1)
        stream = iter(execution)

        assert next(stream) == Output(0, "value")
        time.sleep(0.05)
        assert not execution.done
        assert next(stream) == Output(1, "value")
        assert next(stream) == Output(2, "value")
        with pytest.raises(StopIteration):
            next(stream)
        assert execution.done


def test_unconsumed_stream_can_be_closed_without_blocking_execution() -> None:
    """验证关闭未消费输出流后执行不再受其阻塞。"""

    class Many(Node):
        """当前契约测试使用的 Many 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(count=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            return tuple(Output(value, "value") for value in range(inputs["count"]))

    runtime = Runtime()
    runtime.register("many.graph", Graph(entrypoint="many").add(many=Many()))
    stream = runtime.iter("many.graph", 3, output_buffer=1)
    close = getattr(stream, "close")
    assert callable(close)
    close()
    runtime.wait_idle(1)
    runtime.close()


def test_async_node_timeout_interrupts_awaitable() -> None:
    """验证异步节点超时会中断其等待对象。"""

    class Slow(AsyncNode):
        """当前契约测试使用的 Slow 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            timeout: 超时秒数，None 表示不限制。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)
        timeout = 0.02

        async def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            await asyncio.sleep(1)
            return Output(inputs["value"], "value")

    runtime = Runtime()
    runtime.register("slow.graph", Graph(entrypoint="slow").add(slow=Slow()))

    started = time.monotonic()
    with pytest.raises(NodeTimeoutError, match="timeout=0.02"):
        runtime.run("slow.graph", 1)
    assert time.monotonic() - started < 0.5
    assert runtime.executions()[-1].status is ExecutionStatus.TIMED_OUT
    runtime.close()


def test_none_timeouts_leave_slow_node_unlimited() -> None:
    """验证空超时不会终止较慢节点。"""

    class Slow(AsyncNode):
        """当前契约测试使用的 Slow 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        async def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            await asyncio.sleep(0.03)
            return Output(inputs["value"], "value")

    with Runtime() as runtime:
        runtime.register("slow.graph", Graph(entrypoint="slow").add(slow=Slow()))
        assert runtime.run(
            "slow.graph",
            1,
            timeout=None,
        ) == (Output(1, "value"),)


def test_async_node_business_timeout_is_not_misclassified_as_control_timeout() -> None:
    """验证业务 TimeoutError 不会被误判为引擎控制超时。"""

    class Failing(AsyncNode):
        """当前契约测试使用的 Failing 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        async def execute(self, inputs, context: Context) -> None:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Raises:
                TimeoutError: 等待未在指定时限内完成。
            """

            del inputs, context
            raise TimeoutError("upstream timed out")

    runtime = Runtime()
    runtime.register(
        "failing.graph",
        Graph(entrypoint="failing").add(failing=Failing()),
    )
    with pytest.raises(ExecutionError, match="upstream timed out") as raised:
        runtime.run("failing.graph", 1)
    assert not isinstance(raised.value, ExecutionTimeoutError)
    assert not isinstance(raised.value, NodeTimeoutError)
    runtime.close()


def test_graph_timeout_is_distinct_from_node_timeout() -> None:
    """验证 Graph 超时与节点超时采用不同异常。"""

    class SlowRelay(AsyncNode):
        """当前契约测试使用的 SlowRelay 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            timeout: 超时秒数，None 表示不限制。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)
        timeout = 0.2

        async def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            await asyncio.sleep(0.02)
            return Output(inputs["value"] + 1, "value")

    graph = (
        Graph(entrypoint="first")
        .add(first=SlowRelay(), second=SlowRelay())
        .connect("first", "second", source_port="value", target_port="value")
    )
    runtime = Runtime()
    runtime.register("relay.graph", graph)
    with pytest.raises(ExecutionTimeoutError, match="timeout=0.03"):
        runtime.run(
            "relay.graph",
            0,
            timeout=0.03,
        )
    runtime.close()


def test_each_node_uses_its_own_timeout() -> None:
    """验证各节点分别使用自身的超时限制。"""

    class SlowRelay(AsyncNode):
        """当前契约测试使用的 SlowRelay 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        async def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            await asyncio.sleep(0.02)
            return Output(inputs["value"] + 1, "value")

    first = SlowRelay()
    first.timeout = 0.1
    second = SlowRelay()
    second.timeout = 0.01
    graph = (
        Graph(entrypoint="first")
        .add(first=first, second=second)
        .connect("first", "second", source_port="value", target_port="value")
    )

    with Runtime() as runtime:
        runtime.register("per-node.graph", graph)
        with pytest.raises(NodeTimeoutError) as raised:
            runtime.run("per-node.graph", 0)

    assert raised.value.node == "second"


def test_graph_freeze_snapshots_node_timeout() -> None:
    """验证 Graph 冻结后使用节点超时快照。"""

    class Slow(AsyncNode):
        """当前契约测试使用的 Slow 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        async def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            await asyncio.sleep(0.03)
            return Output(inputs["value"], "value")

    slow = Slow()
    slow.timeout = 0.01
    runtime = Runtime()
    runtime.register("snapshot.graph", Graph(entrypoint="slow").add(slow=slow))
    slow.timeout = None

    with pytest.raises(NodeTimeoutError):
        runtime.run("snapshot.graph", 1)
    runtime.close()


def test_sync_node_can_observe_timeout_at_context_checkpoint() -> None:
    """验证同步节点可在上下文检查点观察超时。"""

    class Cooperative(Node):
        """当前契约测试使用的 Cooperative 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            timeout: 超时秒数，None 表示不限制。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)
        timeout = 0.02

        def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。
            """

            while True:
                time.sleep(0.005)
                context.checkpoint()

    runtime = Runtime()
    runtime.register(
        "cooperative.graph",
        Graph(entrypoint="cooperative").add(cooperative=Cooperative()),
    )
    with pytest.raises(NodeTimeoutError):
        runtime.run("cooperative.graph", 1)
    runtime.close()


def test_non_cooperative_sync_node_is_checked_when_it_returns() -> None:
    """验证不协作的同步节点返回后仍检查超时。"""

    class Blocking(Node):
        """当前契约测试使用的 Blocking 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            timeout: 超时秒数，None 表示不限制。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)
        timeout = 0.01

        def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            time.sleep(0.03)
            return Output(inputs["value"], "value")

    runtime = Runtime()
    runtime.register(
        "blocking.graph",
        Graph(entrypoint="blocking").add(blocking=Blocking()),
    )
    with pytest.raises(NodeTimeoutError):
        runtime.run("blocking.graph", 1)
    runtime.close()


def test_execution_cancel_interrupts_async_node() -> None:
    """验证执行取消能够中断异步节点。"""

    class Waiting(AsyncNode):
        """当前契约测试使用的 Waiting 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        async def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            await asyncio.sleep(10)
            return Output(inputs["value"], "value")

    runtime = Runtime()
    runtime.register(
        "waiting.graph",
        Graph(entrypoint="waiting").add(waiting=Waiting()),
    )
    execution = runtime.start("waiting.graph", 1)
    deadline = time.monotonic() + 1
    while execution.status is ExecutionStatus.PENDING:
        assert time.monotonic() < deadline
        time.sleep(0.001)

    assert execution.cancel()
    with pytest.raises(ExecutionCancelledError):
        execution.result(1)
    assert execution.status is ExecutionStatus.CANCELLED
    assert execution.cancel_requested
    assert not execution.cancel()
    runtime.close()


def test_control_timeout_bypasses_hook_business_error_recovery() -> None:
    """验证控制超时不能被业务错误 Hook 恢复。"""

    class Waiting(AsyncNode):
        """当前契约测试使用的 Waiting 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
            timeout: 超时秒数，None 表示不限制。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)
        timeout = 0.02

        async def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            await asyncio.sleep(1)
            return Output(inputs["value"], "value")

    class Recover(NodeHook):
        def error(self, call, error):
            """在错误阶段执行回调，未恢复的异常继续传播。

            Args:
                call: Hook 当前处理的节点调用记录。
                error: 需要传播、记录或用于恢复的异常。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            del call, error
            return (Output(99, "value"),)

    runtime = Runtime()
    runtime.register(
        "waiting.graph",
        Graph(entrypoint="waiting").add(waiting=Waiting()),
    )
    runtime.attach(Recover(), graph="waiting.graph")
    with pytest.raises(NodeTimeoutError):
        runtime.run("waiting.graph", 1)
    runtime.close()


def test_arun_cancellation_cancels_underlying_execution() -> None:
    """验证取消 arun 等待方会取消底层执行。"""

    class Waiting(AsyncNode):
        """当前契约测试使用的 Waiting 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        async def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            await asyncio.sleep(10)
            return Output(inputs["value"], "value")

    async def scenario() -> ExecutionStatus:
        """组织当前测试的异步调用顺序与结果断言。

        Returns:
            观测到的执行状态。
        """

        runtime = Runtime()
        runtime.register(
            "waiting.graph",
            Graph(entrypoint="waiting").add(waiting=Waiting()),
        )
        task = asyncio.create_task(runtime.arun("waiting.graph", 1))
        while not runtime.executions():
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        execution = runtime.executions()[-1]
        assert execution.wait(1)
        runtime.close()
        return execution.status

    assert asyncio.run(scenario()) is ExecutionStatus.CANCELLED


def test_queue_work_inherits_route_execution_limits() -> None:
    """验证排队工作使用路由配置的执行限制。"""

    runtime = Runtime()
    runtime.register("loop.graph", looping_graph())
    runtime.on(
        "loop.requested",
        graph="loop.graph",
        queue="loops",
        max_steps=4,
    )
    runtime.emit("loop.requested", 0)

    with pytest.raises(StepLimitExceededError):
        runtime.wait_idle()
    execution = runtime.executions()[-1]
    assert execution.steps == 4
    assert execution.status is ExecutionStatus.STEP_LIMITED
    runtime.close()


def test_wait_idle_tracks_synchronous_run_from_another_thread() -> None:
    """验证空闲等待包含其他线程提交的同步执行。"""

    entered = ThreadEvent()
    release = ThreadEvent()

    class Blocking(Node):
        """当前契约测试使用的 Blocking 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context: Context) -> Output:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            entered.set()
            release.wait(1)
            return Output(inputs["value"], "value")

    runtime = Runtime()
    runtime.register(
        "blocking.graph",
        Graph(entrypoint="blocking").add(blocking=Blocking()),
    )
    thread = Thread(target=runtime.run, args=("blocking.graph", 1))
    thread.start()
    assert entered.wait(1)
    try:
        with pytest.raises(TimeoutError):
            runtime.wait_idle(0)
    finally:
        release.set()
        thread.join(1)
    assert not thread.is_alive()
    runtime.wait_idle(0)
    runtime.close()


def test_completed_execution_burst_is_trimmed_to_history_limit() -> None:
    """验证集中完成的执行记录按历史上限清理。"""

    entered = 0
    entered_all = ThreadEvent()
    counter_lock = Lock()
    release = ThreadEvent()

    class Block(Node):
        """当前契约测试使用的 Block 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。
            """

            nonlocal entered
            del inputs, context
            with counter_lock:
                entered += 1
                if entered == 3:
                    entered_all.set()
            release.wait()

    with Runtime() as runtime:
        assert isinstance(runtime.worker, GraphWorker)
        runtime.worker._history_limit = 2
        runtime.register("block.graph", Graph(entrypoint="block").add(block=Block()))
        executions = [runtime.start("block.graph", value) for value in range(3)]

        try:
            assert entered_all.wait(1)
            assert len(runtime.executions()) == 3
        finally:
            release.set()
        runtime.wait_idle(1)

        assert all(execution.done for execution in executions)
        assert len(runtime.executions()) == 2


def test_queued_unknown_graph_is_recorded_as_failed_execution() -> None:
    """验证未知 Graph 的排队工作被记录为失败执行。"""

    runtime = Runtime()
    runtime.consume("missing")
    runtime.route("missing.requested", graph="missing.graph", queue="missing")
    runtime.emit("missing.requested", 1)

    with pytest.raises(BricksRuntimeError, match="unknown registered graph"):
        runtime.wait_idle()
    execution = runtime.executions()[-1]
    assert execution.graph == "missing.graph"
    assert execution.status is ExecutionStatus.FAILED
    assert isinstance(execution.error, BricksRuntimeError)
    runtime.close()
