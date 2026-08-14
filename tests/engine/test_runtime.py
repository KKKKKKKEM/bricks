"""Event 驱动 Runtime 的执行契约测试。"""

from __future__ import annotations

import asyncio
from threading import Lock, current_thread

import pytest

from bricks import (
    AsyncNode,
    Context,
    Event,
    Graph,
    InputPolicy,
    Node,
    Output,
    Ports,
    Runtime,
    Slot,
    SlotPool,
)
from bricks.adapters import memory
from bricks.engine.errors import (
    IncompleteInputsError,
    InvalidOutputError,
    PortValueTypeError,
)
from bricks.runtime import GraphWorker


class Split(Node):
    """产生 join 所需的两个图内端口。"""

    input_ports = Ports(value=int)
    output_ports = Ports(left=int, right=int)

    def execute(self, inputs, context: Context):
        """把整数复制到两个端口。

        参数：
            inputs: 当前整数输入。
            context: 当前执行上下文。

        返回：
            两个命名 Output。
        """

        del context
        value = inputs["value"]
        return (Output(value, "left"), Output(value + 1, "right"))


class Join(Node):
    """等待两个输入端口后求和。"""

    input_ports = Ports(left=int, right=int)
    output_ports = Ports(total=int)
    input_policy = InputPolicy.ALL

    def execute(self, inputs, context: Context) -> Output:
        """求和并返回终端 Output。

        参数：
            inputs: 同时包含 left 和 right。
            context: 当前执行上下文。

        返回：
            求和结果。
        """

        del context
        return Output(inputs["left"] + inputs["right"], "total")


def join_graph() -> Graph:
    """构建用于测试图内 Edge 和 InputPolicy 的 Graph。

    返回：
        已冻结的两节点 Graph。
    """

    return (
        Graph(entrypoint="split")
        .add("split", Split())
        .add("join", Join())
        .connect("split", "join", source_port="left", target_port="left")
        .connect("split", "join", source_port="right", target_port="right")
        .freeze()
    )


def test_runtime_executes_graph_internal_dataflow() -> None:
    """Runtime.run 返回未连接 Edge 的终端 Output。"""

    with Runtime() as runtime:
        runtime.register("join.graph", join_graph())
        outputs = runtime.run("join.graph", 2)

    assert outputs == (Output(5, "total"),)


def test_runtime_executes_self_loop_until_node_stops_emitting_feedback() -> None:
    """自环由 Node 停止产生反馈 Output 后自然结束。"""

    visits: list[int] = []

    class Counter(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(again=int, done=int)

        def execute(self, inputs, context: Context) -> Output:
            del context
            value = inputs["value"]
            visits.append(value)
            if value < 4:
                return Output(value + 1, "again")
            return Output(value, "done")

    graph = (
        Graph(entrypoint="counter")
        .add("counter", Counter())
        .connect("counter", "counter", source_port="again", target_port="value")
    )

    with Runtime() as runtime:
        runtime.register("loop.graph", graph)
        outputs = runtime.run("loop.graph", 0)

    assert visits == [0, 1, 2, 3, 4]
    assert outputs == (Output(4, "done"),)


def test_runtime_does_not_impose_cycle_step_limit() -> None:
    """核心不会按 Node 执行次数截断合法长循环。"""

    class Counter(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(again=int, done=int)

        def execute(self, inputs, context: Context) -> Output:
            del context
            value = inputs["value"]
            if value < 10_000:
                return Output(value + 1, "again")
            return Output(value, "done")

    graph = (
        Graph(entrypoint="counter")
        .add(counter=Counter())
        .connect("counter", "counter", source_port="again", target_port="value")
    )

    with Runtime() as runtime:
        runtime.register("long-cycle.graph", graph)
        outputs = runtime.run("long-cycle.graph", 0)

    assert outputs == (Output(10_000, "done"),)


def test_runtime_executes_cycle_across_multiple_nodes() -> None:
    """数据可以多次经过由多个 Node 组成的回路。"""

    calls: list[tuple[str, int]] = []

    class Check(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(continue_=int, done=int)

        def execute(self, inputs, context: Context) -> Output:
            del context
            value = inputs["value"]
            calls.append(("check", value))
            if value < 3:
                return Output(value, "continue_")
            return Output(value, "done")

    class Increment(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context: Context) -> Output:
            del context
            value = inputs["value"]
            calls.append(("increment", value))
            return Output(value + 1, "value")

    graph = (
        Graph(entrypoint="check")
        .add(check=Check(), increment=Increment())
        .connect(
            "check", "increment", source_port="continue_", target_port="value"
        )
        .connect("increment", "check", source_port="value", target_port="value")
    )

    with Runtime() as runtime:
        runtime.register("cycle.graph", graph)
        outputs = runtime.run("cycle.graph", 0)

    assert calls == [
        ("check", 0),
        ("increment", 0),
        ("check", 1),
        ("increment", 1),
        ("check", 2),
        ("increment", 2),
        ("check", 3),
    ]
    assert outputs == (Output(3, "done"),)


def test_runtime_executes_cycle_inside_execution_plan() -> None:
    """ExecutionPlan 保留所选节点之间的回边。"""

    class Counter(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(again=int, done=int)

        def execute(self, inputs, context: Context) -> Output:
            del context
            value = inputs["value"]
            if value < 2:
                return Output(value + 1, "again")
            return Output(value, "done")

    graph = (
        Graph(entrypoint="counter")
        .add(counter=Counter())
        .connect("counter", "counter", source_port="again", target_port="value")
    )
    plan = graph.plan(include={"counter"})

    with Runtime() as runtime:
        runtime.register("planned-cycle.graph", graph)
        outputs = runtime.run("planned-cycle.graph", 0, plan=plan)

    assert plan.edges == graph.edges
    assert outputs == (Output(2, "done"),)


def test_cycle_scheduler_does_not_starve_ready_branch() -> None:
    """自环每轮只执行一次，已经就绪的旁路会获得调度。"""

    calls: list[str] = []

    class Loop(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(again=int, observe=int, done=int)

        def execute(self, inputs, context: Context):
            del context
            value = inputs["value"]
            calls.append(f"loop:{value}")
            if value < 3:
                return (
                    Output(value + 1, "again"),
                    Output(value, "observe"),
                )
            return Output(value, "done")

    class Observe(Node):
        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            del context
            calls.append(f"observe:{inputs['value']}")

    graph = (
        Graph(entrypoint="loop")
        .add(loop=Loop(), observe=Observe())
        .connect("loop", "loop", source_port="again", target_port="value")
        .connect("loop", "observe", source_port="observe", target_port="value")
    )

    with Runtime() as runtime:
        runtime.register("fair.graph", graph)
        outputs = runtime.run("fair.graph", 0)

    assert calls == [
        "loop:0",
        "loop:1",
        "observe:0",
        "loop:2",
        "observe:1",
        "loop:3",
        "observe:2",
    ]
    assert outputs == (Output(3, "done"),)


def test_cycle_quiescence_reports_incomplete_inputs() -> None:
    """回路停止后，无法组成 ALL 输入的残留数据仍是执行错误。"""

    class Loop(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(again=int, partial=int, absent=int, done=int)

        def execute(self, inputs, context: Context):
            del context
            value = inputs["value"]
            if value < 2:
                return (
                    Output(value + 1, "again"),
                    Output(value, "partial"),
                )
            return Output(value, "done")

    class Join(Node):
        input_ports = Ports(left=int, right=int)
        output_ports = Ports()
        input_policy = InputPolicy.ALL

        def execute(self, inputs, context: Context) -> None:
            del inputs, context

    graph = (
        Graph(entrypoint="loop")
        .add(loop=Loop(), join=Join())
        .connect("loop", "loop", source_port="again", target_port="value")
        .connect("loop", "join", source_port="partial", target_port="left")
        .connect("loop", "join", source_port="absent", target_port="right")
    )

    with Runtime() as runtime:
        runtime.register("incomplete-cycle.graph", graph)
        with pytest.raises(IncompleteInputsError, match="join.left"):
            runtime.run("incomplete-cycle.graph", 0)


def test_runtime_executes_different_plans_from_one_graph() -> None:
    """同一冻结 Graph 可按每次调用选择不同分支，未选 Node 不执行。"""

    calls: list[str] = []

    class SourceNode(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(value=int)

        def execute(self, inputs, context: Context) -> Output:
            del context
            calls.append("source")
            return Output(inputs["value"], "value")

    class Branch(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(result=str)

        def __init__(self, name: str) -> None:
            self.name = name

        def execute(self, inputs, context: Context) -> Output:
            del context
            calls.append(self.name)
            return Output(f"{self.name}:{inputs['value']}", "result")

    graph = (
        Graph(entrypoint="source")
        .add("source", SourceNode())
        .add("fast", Branch("fast"))
        .add("full", Branch("full"))
        .connect("source", "fast", source_port="value", target_port="value")
        .connect("source", "full", source_port="value", target_port="value")
    )
    fast = graph.plan(include={"source", "fast"})
    full = graph.plan(include={"source", "full"})

    with Runtime() as runtime:
        runtime.register("work.graph", graph)
        assert runtime.run("work.graph", 3, plan=fast) == (
            Output("fast:3", "result"),
        )
        assert runtime.run("work.graph", 4, plan=full) == (
            Output("full:4", "result"),
        )

    assert calls == ["source", "fast", "source", "full"]


def test_runtime_rejects_plan_from_another_graph() -> None:
    """Plan 只能用于创建它的同一个 Graph 实例。"""

    class Start(Node):
        input_ports = Ports()
        output_ports = Ports()
        input_policy = InputPolicy.ON_START

        def execute(self, inputs, context: Context) -> None:
            del inputs, context

    first = Graph(entrypoint="source").add("source", Start()).freeze()
    second = Graph(entrypoint="source").add("source", Start()).freeze()
    plan = first.plan(include={"source"})

    with Runtime() as runtime:
        runtime.register("second", second)
        with pytest.raises(ValueError, match="different Graph"):
            runtime.run("second", plan=plan)


def test_single_port_receives_complete_mapping_payload() -> None:
    """单端口的 Mapping payload 不按多端口输入映射解释。"""

    class MappingConsumer(Node):
        input_ports = Ports(task=dict)
        output_ports = Ports(result=dict)

        def execute(self, inputs, context: Context) -> Output:
            del context
            return Output(inputs["task"], "result")

    graph = Graph(entrypoint="consume").add("consume", MappingConsumer())
    payload = {"task": 1}
    with Runtime() as runtime:
        runtime.register("mapping.graph", graph)
        outputs = runtime.run("mapping.graph", payload)

    assert outputs == (Output(payload, "result"),)


@pytest.mark.parametrize("node", ["sync", "async"])
def test_runtime_classifies_invalid_node_results(node: str) -> None:
    """同步和异步 Node 的非法返回值都使用公共输出协议错误。"""

    class InvalidSync(Node):
        input_ports = Ports()
        output_ports = Ports()
        input_policy = InputPolicy.ON_START

        def execute(self, inputs, context: Context):
            del inputs, context
            return 1

    class InvalidAsync(AsyncNode):
        input_ports = Ports()
        output_ports = Ports()
        input_policy = InputPolicy.ON_START

        async def execute(self, inputs, context: Context):
            del inputs, context
            return ["not-an-output"]

    invalid = InvalidSync() if node == "sync" else InvalidAsync()
    graph = Graph(entrypoint="invalid").add("invalid", invalid)
    with Runtime() as runtime:
        runtime.register("invalid.graph", graph)
        with pytest.raises(InvalidOutputError) as raised:
            runtime.run("invalid.graph")

    assert raised.value.graph == "invalid.graph"
    assert raised.value.node == "invalid"


def test_idle_runtime_supports_zero_timeout_polling() -> None:
    """零超时是即时空闲检查，空闲 Runtime 应立即成功。"""

    with Runtime() as runtime:
        runtime.wait_idle(0)


def test_context_emit_routes_to_another_graph() -> None:
    """context.emit 通过 route 启动另一张 Graph。"""

    received: list[str] = []

    class Producer(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            context.emit("value.created", inputs["value"])

    class Consumer(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            del context
            received.append(inputs["value"])

    producer = Graph(entrypoint="produce").add("produce", Producer())
    consumer = Graph(entrypoint="consume").add("consume", Consumer())

    with Runtime() as runtime:
        runtime.register("producer.graph", producer)
        runtime.register("consumer.graph", consumer)
        runtime.on(
            "value.created",
            graph="consumer.graph",
            queue="values",
            concurrency=2,
        )
        runtime.run("producer.graph", "hello")
        runtime.wait_idle()

    assert received == ["hello"]


def test_runtime_keeps_observe_separate_from_combined_on() -> None:
    """observe 只观察 Event，on 组合 route 与 consume。"""

    observed = []
    received: list[str] = []

    class Consumer(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            del context
            received.append(inputs["value"])

    graph = Graph(entrypoint="consume").add("consume", Consumer())
    with Runtime() as runtime:
        runtime.register("consumer.graph", graph)
        runtime.observe("value.observed", observed.append)
        runtime.on(
            "value.routed",
            graph="consumer.graph",
            queue="values",
        )
        runtime.emit("value.observed", "seen")
        runtime.emit("value.routed", "handled")
        runtime.wait_idle()

    assert [event.payload for event in observed] == ["seen"]
    assert received == ["handled"]


def test_runtime_on_forwards_subscription() -> None:
    """组合入口保留 route 的显式 subscription 身份。"""

    class Consumer(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            del inputs, context

    graph = Graph(entrypoint="consume").add(consume=Consumer())
    with Runtime() as runtime:
        runtime.register("consumer.graph", graph)
        runtime.on(
            "value.routed",
            graph="consumer.graph",
            queue="values",
            subscription="consumer-values",
        )


def test_runtime_on_does_not_leave_route_when_consumer_setup_fails() -> None:
    """消费配置失败时，同一 route 仍可在修正参数后正常注册。"""

    class Consumer(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            del inputs, context

    with Runtime() as runtime:
        runtime.register(
            "consumer.graph",
            Graph(entrypoint="consume").add(consume=Consumer()),
        )
        with pytest.raises(ValueError, match="at least 1"):
            runtime.on(
                "value.routed",
                graph="consumer.graph",
                queue="values",
                concurrency=0,
            )

        runtime.on(
            "value.routed",
            graph="consumer.graph",
            queue="values",
        )


def test_event_is_committed_even_if_source_node_later_fails() -> None:
    """emit 成功后的事件不因当前 Node 后续失败而撤回。"""

    class Broken(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            context.emit("value.committed", inputs["value"])
            raise ValueError("boom")

    graph = Graph(entrypoint="broken").add("broken", Broken())
    seen = []
    with Runtime() as runtime:
        runtime.register("broken.graph", graph)
        runtime.observe("value.committed", seen.append)
        with pytest.raises(Exception, match="boom"):
            runtime.run("broken.graph", "kept")

    assert [event.payload for event in seen] == ["kept"]


def test_runtime_dispatches_each_event_without_domain_deduplication() -> None:
    """Runtime 不判断两个领域 payload 是否代表相同工作。"""

    received: list[str] = []

    class Consumer(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            del context
            received.append(inputs["value"])

    graph = Graph(entrypoint="consume").add("consume", Consumer())
    with Runtime() as runtime:
        runtime.register("consumer.graph", graph)
        runtime.on("value", graph="consumer.graph", queue="values")
        runtime.emit("value", "Same")
        runtime.emit("value", "same")
        runtime.wait_idle()

    assert sorted(received) == ["Same", "same"]


def test_queue_concurrency_limits_graph_executions() -> None:
    """命名执行通道的 concurrency 限制同时运行的 Graph 数量。"""

    active = 0
    maximum = 0
    lock = Lock()

    class Slow(AsyncNode):
        input_ports = Ports(value=int)
        output_ports = Ports()

        async def execute(self, inputs, context: Context) -> None:
            nonlocal active, maximum
            del inputs, context
            with lock:
                active += 1
                maximum = max(maximum, active)
            try:
                await asyncio.sleep(0.02)
            finally:
                with lock:
                    active -= 1

    graph = Graph(entrypoint="slow").add("slow", Slow())
    with Runtime() as runtime:
        runtime.register("slow.graph", graph)
        runtime.on(
            "slow.requested",
            graph="slow.graph",
            queue="slow-tasks",
            concurrency=3,
        )
        for value in range(9):
            runtime.emit("slow.requested", value)
        runtime.wait_idle()

    assert maximum == 3


def test_event_can_drive_a_chain_of_graphs() -> None:
    """Event 输出可以继续驱动下一张 Graph。"""

    received: list[str] = []

    class Relay(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            context.emit("relayed", inputs["value"])

    class Sink(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            del context
            received.append(inputs["value"])

    graph = Graph(entrypoint="relay").add("relay", Relay())
    sink = Graph(entrypoint="sink").add("sink", Sink())
    with Runtime() as runtime:
        runtime.register("relay.graph", graph)
        runtime.register("sink.graph", sink)
        runtime.on("root", graph="relay.graph", queue="relay-tasks")
        runtime.on("relayed", graph="sink.graph", queue="sink-tasks")
        runtime.emit("root", "value")
        runtime.wait_idle()

    assert received == ["value"]


def test_slot_follows_work_across_consumers() -> None:
    """下游 Worker 沿用上游 Work 的 Slot，而不是从自己的池重新获取。"""

    seen: list[tuple[str, str, str]] = []

    class Producer(Node):
        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            assert context.slot is not None
            context.slot["value"] = inputs["value"]
            seen.append(("source", context.slot.id, current_thread().name))
            context.emit("slot.forwarded", inputs["value"])

    class Consumer(Node):
        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            assert context.slot is not None
            assert context.slot["value"] == inputs["value"]
            seen.append(("sink", context.slot.id, current_thread().name))

    shared = SlotPool(slots=[Slot(id="shared")])
    with Runtime() as runtime:
        runtime.register("source.graph", Graph(entrypoint="source").add(source=Producer()))
        runtime.register("sink.graph", Graph(entrypoint="sink").add(sink=Consumer()))
        runtime.on(
            "slot.started",
            graph="source.graph",
            queue="sources",
            concurrency=2,
            slots=shared,
        )
        runtime.on(
            "slot.forwarded",
            graph="sink.graph",
            queue="sinks",
            concurrency=4,
            slots=shared,
        )
        runtime.emit("slot.started", 7)
        runtime.wait_idle()

    assert [(stage, slot_id) for stage, slot_id, _ in seen] == [
        ("source", "shared"),
        ("sink", "shared"),
    ]
    assert seen[0][2].startswith("bricks-sources")
    assert seen[1][2].startswith("bricks-sinks")
    assert shared.available == 1


def test_default_slot_pool_matches_consumer_concurrency() -> None:
    """不传 slots 时自动池限制逻辑执行链数量，并在 Work 后复用 Slot。"""

    slot_ids: list[str] = []
    active = 0
    maximum = 0
    lock = Lock()

    class Capture(AsyncNode):
        input_ports = Ports(value=int)
        output_ports = Ports()

        async def execute(self, inputs, context: Context) -> None:
            nonlocal active, maximum
            del inputs
            assert context.slot is not None
            with lock:
                slot_ids.append(context.slot.id)
                active += 1
                maximum = max(maximum, active)
            try:
                await asyncio.sleep(0.01)
            finally:
                with lock:
                    active -= 1

    with Runtime() as runtime:
        runtime.register("capture.graph", Graph(entrypoint="capture").add(capture=Capture()))
        runtime.on(
            "capture.requested",
            graph="capture.graph",
            queue="captures",
            concurrency=2,
        )
        for value in range(6):
            runtime.emit("capture.requested", value)
        runtime.wait_idle()

    assert maximum == 2
    assert len(set(slot_ids)) == 2


def test_slot_lease_waits_for_all_event_branches() -> None:
    """一个 Work 发出的多个分支共享 Slot，所有分支结束后才归还。"""

    seen: list[tuple[str, str]] = []

    class Fork(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            assert context.slot is not None
            context.emit("branch.left", inputs["value"])
            context.emit("branch.right", inputs["value"])

    class Branch(Node):
        input_ports = Ports(value=str)
        output_ports = Ports()

        def __init__(self, name: str) -> None:
            self.name = name

        def execute(self, inputs, context: Context) -> None:
            del inputs
            assert context.slot is not None
            seen.append((self.name, context.slot.id))

    shared = SlotPool(slots=[Slot(id="branch-slot")])
    with Runtime() as runtime:
        runtime.register("fork.graph", Graph(entrypoint="fork").add(fork=Fork()))
        runtime.register(
            "left.graph", Graph(entrypoint="left").add(left=Branch("left"))
        )
        runtime.register(
            "right.graph", Graph(entrypoint="right").add(right=Branch("right"))
        )
        runtime.on("fork", graph="fork.graph", queue="forks", slots=shared)
        runtime.on("branch.left", graph="left.graph", queue="left", slots=shared)
        runtime.on("branch.right", graph="right.graph", queue="right", slots=shared)
        runtime.emit("fork", "value")
        runtime.wait_idle()

    assert sorted(seen) == [("left", "branch-slot"), ("right", "branch-slot")]
    assert shared.available == 1


def test_failed_work_releases_its_slot() -> None:
    """Graph 异常不能泄漏 Slot，后续根 Work 仍可获得它。"""

    class Broken(Node):
        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            del inputs, context
            raise ValueError("broken slot work")

    shared = SlotPool(1)
    runtime = Runtime()
    runtime.register("broken.graph", Graph(entrypoint="broken").add(broken=Broken()))
    runtime.on("broken", graph="broken.graph", queue="broken", slots=shared)
    runtime.emit("broken", 1)

    with pytest.raises(Exception, match="broken slot work"):
        runtime.wait_idle()
    assert shared.available == 1
    runtime.close()


def test_rejected_emitted_event_releases_its_retained_slot() -> None:
    """Emitter 拒绝接管 Event 时回滚为它保留的 Slot 引用。"""

    def reject(event) -> None:
        del event
        raise ValueError("rejected event")

    slots = SlotPool(1)
    lease = slots._acquire()
    worker = GraphWorker(
        consumer=memory.TaskBackend(),
        emit=reject,
        close_injected=True,
    )

    with pytest.raises(ValueError, match="rejected event"):
        worker._emit_with_lease(lease, Event("rejected"))

    lease.release()
    assert slots.available == 1
    worker.close()


def test_failed_event_dispatch_releases_forwarded_slot_once() -> None:
    """EventBus 失败后 Router 与 Worker 交接 lease 时不会重复释放。"""

    class Relay(Node):
        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            context.emit("rejected", inputs["value"])

    def reject(event) -> None:
        del event
        raise ValueError("rejected event")

    slots = SlotPool(1)
    runtime = Runtime()
    runtime.register("relay.graph", Graph(entrypoint="relay").add(relay=Relay()))
    runtime.observe("rejected", reject)
    runtime.on("relay", graph="relay.graph", queue="relay", slots=slots)
    runtime.emit("relay", 1)

    with pytest.raises(Exception, match="rejected event"):
        runtime.wait_idle()
    assert slots.available == 1
    runtime.close()


def test_waiting_roots_do_not_starve_slot_continuations() -> None:
    """根 Work 等 Slot 时不占 Worker，携带 lease 的下游 Work 可继续执行。"""

    received: list[int] = []

    class Relay(Node):
        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            context.emit("continued", inputs["value"])

    class Sink(Node):
        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            del context
            received.append(inputs["value"])

    shared = SlotPool(1)
    with Runtime() as runtime:
        runtime.register("relay.graph", Graph(entrypoint="relay").add(relay=Relay()))
        runtime.register("sink.graph", Graph(entrypoint="sink").add(sink=Sink()))
        runtime.on(
            "root",
            graph="relay.graph",
            queue="roots",
            concurrency=4,
            slots=shared,
        )
        runtime.on(
            "continued",
            graph="sink.graph",
            queue="continuations",
            concurrency=2,
            slots=shared,
        )
        for value in range(8):
            runtime.emit("root", value)
        runtime.wait_idle(2)

    assert sorted(received) == list(range(8))
    assert shared.available == 1


def test_shared_pool_works_when_consumer_and_slot_sizes_differ() -> None:
    """Consumer 并发与共享池大小相互独立，实际链路并发取二者约束的结果。"""

    active = 0
    maximum = 0
    lock = Lock()

    class Slow(AsyncNode):
        input_ports = Ports(value=int)
        output_ports = Ports()

        async def execute(self, inputs, context: Context) -> None:
            nonlocal active, maximum
            del inputs
            assert context.slot is not None
            with lock:
                active += 1
                maximum = max(maximum, active)
            try:
                await asyncio.sleep(0.01)
            finally:
                with lock:
                    active -= 1

    slots = SlotPool(2)
    with Runtime() as runtime:
        runtime.register("slow.graph", Graph(entrypoint="slow").add(slow=Slow()))
        runtime.on(
            "slow.slot",
            graph="slow.graph",
            queue="slow-slots",
            concurrency=5,
            slots=slots,
        )
        for value in range(8):
            runtime.emit("slow.slot", value)
        runtime.wait_idle()

    assert maximum == 2
    assert slots.available == 2


def test_multiple_routes_retain_one_slot_until_every_work_finishes() -> None:
    """同一 Event 的多个 route 各自接管 lease，最后一个 Work 后才归还。"""

    seen: list[tuple[str, str]] = []

    class FanOut(Node):
        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            context.emit("fanout", inputs["value"])

    class Sink(Node):
        input_ports = Ports(value=int)
        output_ports = Ports()

        def __init__(self, name: str) -> None:
            self.name = name

        def execute(self, inputs, context: Context) -> None:
            del inputs
            assert context.slot is not None
            seen.append((self.name, context.slot.id))

    slots = SlotPool(slots=[Slot(id="fanout-slot")])
    with Runtime() as runtime:
        runtime.register("fanout.graph", Graph(entrypoint="fanout").add(fanout=FanOut()))
        runtime.register("sink-a.graph", Graph(entrypoint="sink").add(sink=Sink("a")))
        runtime.register("sink-b.graph", Graph(entrypoint="sink").add(sink=Sink("b")))
        runtime.on("root.fanout", graph="fanout.graph", queue="fanout", slots=slots)
        runtime.consume("sink-a", slots=slots)
        runtime.consume("sink-b", slots=slots)
        runtime.route("fanout", graph="sink-a.graph", queue="sink-a")
        runtime.route("fanout", graph="sink-b.graph", queue="sink-b")
        runtime.emit("root.fanout", 1)
        runtime.wait_idle()

    assert sorted(seen) == [("a", "fanout-slot"), ("b", "fanout-slot")]
    assert slots.available == 1


def test_queue_rejects_a_different_slot_pool_when_already_bound() -> None:
    """同一 Worker 的同一 queue 不能在重复配置时偷偷切换 SlotPool。"""

    with Runtime() as runtime:
        runtime.consume("queue", concurrency=2, slots=SlotPool(1))
        with pytest.raises(Exception, match="different SlotPool"):
            runtime.consume("queue", concurrency=2, slots=SlotPool(1))


def test_slot_pool_rejects_duplicate_ids() -> None:
    """池内 Slot ID 唯一，便于日志、观测和插件定位逻辑槽。"""

    with pytest.raises(ValueError, match="duplicate Slot ids"):
        SlotPool(slots=[Slot(id="same"), Slot(id="same")])


def test_returned_slot_keeps_state_for_later_root_work() -> None:
    """Slot 归还池时不清空状态，后续 Work 可继续复用其中资源。"""

    counts: list[int] = []

    class Reuse(Node):
        input_ports = Ports(value=int)
        output_ports = Ports()

        def execute(self, inputs, context: Context) -> None:
            del inputs
            assert context.slot is not None
            context.slot["uses"] = context.slot.get("uses", 0) + 1
            counts.append(context.slot["uses"])

    slots = SlotPool(1)
    with Runtime() as runtime:
        runtime.register("reuse.graph", Graph(entrypoint="reuse").add(reuse=Reuse()))
        runtime.on("reuse", graph="reuse.graph", queue="reuse", slots=slots)
        runtime.emit("reuse", 1)
        runtime.wait_idle()
        runtime.emit("reuse", 2)
        runtime.wait_idle()

    assert counts == [1, 2]
    assert slots.available == 1


def test_runtime_validates_entry_value_type() -> None:
    """入口 payload 必须满足 Ports 声明。"""

    with Runtime() as runtime:
        runtime.register("join.graph", join_graph())
        with pytest.raises(PortValueTypeError):
            runtime.run("join.graph", "not-an-int")


def test_runtime_reports_incomplete_join() -> None:
    """图停止时残留的半组输入必须显式失败。"""

    class LeftOnly(Split):
        def execute(self, inputs, context: Context):
            del context
            return Output(inputs["value"], "left")

    graph = (
        Graph(entrypoint="split")
        .add("split", LeftOnly())
        .add("join", Join())
        .connect("split", "join", source_port="left", target_port="left")
        .connect("split", "join", source_port="right", target_port="right")
    )
    # 两个必需端口都有 Edge，但本次 Node firing 只产生 left。
    graph.freeze()
    with Runtime() as runtime:
        runtime.register("incomplete.graph", graph)
        with pytest.raises(IncompleteInputsError):
            runtime.run("incomplete.graph", 1)
