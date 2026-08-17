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


class Increment(Node):
    input_ports = Ports(value=int)
    output_ports = Ports(value=int)

    def execute(self, inputs, context: Context) -> Output:
        del context
        return Output(inputs["value"] + 1, "value")


def looping_graph() -> Graph:
    return (
        Graph(entrypoint="increment")
        .add(increment=Increment())
        .connect("increment", "increment", source_port="value", target_port="value")
    )


def test_execution_limits_default_to_unlimited() -> None:
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
    with pytest.raises(error):
        ExecutionLimits(**kwargs)


def test_max_steps_counts_node_firings_and_preserves_execution_state() -> None:
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
    with Runtime() as runtime:
        runtime.register(
            "increment.graph",
            Graph(entrypoint="increment").add(increment=Increment()),
        )
        assert runtime.run("increment.graph", 1, max_steps=1) == (
            Output(2, "value"),
        )
        assert runtime.executions()[-1].steps == 1


def test_zero_max_steps_keeps_existing_unlimited_behavior() -> None:
    class StopAt(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(again=int, done=int)

        def execute(self, inputs, context: Context) -> Output:
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
        assert runtime.run("counter.graph", 0, max_steps=0) == (
            Output(21, "done"),
        )
        assert runtime.executions()[-1].steps == 21


def test_start_returns_successful_queryable_execution() -> None:
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
    async def scenario() -> tuple[Output, ...]:
        with Runtime() as runtime:
            runtime.register(
                "increment.graph",
                Graph(entrypoint="increment").add(increment=Increment()),
            )
            return await runtime.start("increment.graph", 2)

    assert asyncio.run(scenario()) == (Output(3, "value"),)


def test_runtime_iter_streams_terminal_outputs_and_keeps_final_result() -> None:
    class Many(Node):
        input_ports = Ports(count=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context):
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
    class Many(Node):
        input_ports = Ports(count=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context):
            del context
            return tuple(Output(value, "value") for value in range(inputs["count"]))

    async def scenario() -> list[Output]:
        with Runtime() as runtime:
            runtime.register("many.graph", Graph(entrypoint="many").add(many=Many()))
            return [output async for output in runtime.aiter("many.graph", 3)]

    assert asyncio.run(scenario()) == [
        Output(0, "value"),
        Output(1, "value"),
        Output(2, "value"),
    ]


def test_stream_yields_committed_outputs_before_later_failure() -> None:
    class EmitThenFail(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(result=int, next=int)

        def execute(self, inputs, context):
            del context
            return Output(inputs["value"], "result"), Output(inputs["value"], "next")

    class Fail(Node):
        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context):
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
    class Many(Node):
        input_ports = Ports(count=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context):
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
    class Many(Node):
        input_ports = Ports(count=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context):
            del context
            return tuple(Output(value, "value") for value in range(inputs["count"]))

    runtime = Runtime()
    runtime.register("many.graph", Graph(entrypoint="many").add(many=Many()))
    stream = runtime.iter("many.graph", 3, output_buffer=1)
    stream.close()
    runtime.wait_idle(1)
    runtime.close()


def test_async_node_timeout_interrupts_awaitable() -> None:
    class Slow(AsyncNode):
        input_ports = Ports(value=int)
        output_ports = Ports(value=int)
        timeout = 0.02

        async def execute(self, inputs, context: Context) -> Output:
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
    class Slow(AsyncNode):
        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        async def execute(self, inputs, context: Context) -> Output:
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
    class Failing(AsyncNode):
        input_ports = Ports(value=int)
        output_ports = Ports()

        async def execute(self, inputs, context: Context) -> None:
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
    class SlowRelay(AsyncNode):
        input_ports = Ports(value=int)
        output_ports = Ports(value=int)
        timeout = 0.2

        async def execute(self, inputs, context: Context) -> Output:
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
    class SlowRelay(AsyncNode):
        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        async def execute(self, inputs, context: Context) -> Output:
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
    class Slow(AsyncNode):
        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        async def execute(self, inputs, context: Context) -> Output:
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
    class Cooperative(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(value=int)
        timeout = 0.02

        def execute(self, inputs, context: Context) -> Output:
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
    class Blocking(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(value=int)
        timeout = 0.01

        def execute(self, inputs, context: Context) -> Output:
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
    class Waiting(AsyncNode):
        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        async def execute(self, inputs, context: Context) -> Output:
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
    class Waiting(AsyncNode):
        input_ports = Ports(value=int)
        output_ports = Ports(value=int)
        timeout = 0.02

        async def execute(self, inputs, context: Context) -> Output:
            del context
            await asyncio.sleep(1)
            return Output(inputs["value"], "value")

    class Recover(NodeHook):
        def error(self, call, error):
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
    class Waiting(AsyncNode):
        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        async def execute(self, inputs, context: Context) -> Output:
            del context
            await asyncio.sleep(10)
            return Output(inputs["value"], "value")

    async def scenario() -> ExecutionStatus:
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
    entered = ThreadEvent()
    release = ThreadEvent()

    class Blocking(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context: Context) -> Output:
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
    entered = 0
    entered_all = ThreadEvent()
    counter_lock = Lock()
    release = ThreadEvent()

    class Block(Node):
        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            nonlocal entered
            del inputs, context
            with counter_lock:
                entered += 1
                if entered == 3:
                    entered_all.set()
            release.wait()

    with Runtime() as runtime:
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
