"""动态 Node Hook、控制信号和异步执行桥接的契约测试。"""

from __future__ import annotations

import asyncio
from threading import Event as ThreadEvent
from threading import get_ident

import pytest

from bricks import AsyncNode, Graph, Node, Output, Ports, Runtime
from bricks.engine import (
    HookExecutionError,
    InvalidOutputError,
    NodeCall,
    NodeHook,
    ShortCircuit,
    StopGraph,
)


class AddOne(Node):
    input_ports = Ports(value=int)
    output_ports = Ports(result=int)

    def __init__(self) -> None:
        self.calls = 0

    def execute(self, inputs, context) -> Output:
        del context
        self.calls += 1
        return Output(inputs["value"] + 1, "result")


def single_node_graph(node: Node) -> Graph:
    return Graph(entrypoint="work").add("work", node)


def test_object_hook_transforms_inputs_and_outputs() -> None:
    node = AddOne()
    order: list[str] = []

    class Transform(NodeHook):
        def enter(self, call: NodeCall) -> NodeCall:
            order.append("enter")
            return call.with_inputs({"value": call.inputs["value"] * 2})

        def exit(self, call, outputs):
            del call
            order.append("exit")
            return (Output(outputs[0].value + 3, "result"),)

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(node))
        runtime.attach(Transform(), graph="work.graph", node="work")
        outputs = runtime.run("work.graph", 4)

    assert outputs == (Output(12, "result"),)
    assert node.calls == 1
    assert order == ["enter", "exit"]


def test_short_circuit_skips_node_and_continues_through_edges() -> None:
    request = AddOne()

    class Double(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(result=int)

        def execute(self, inputs, context):
            del context
            return Output(inputs["value"] * 2, "result")

    graph = (
        Graph(entrypoint="request")
        .add("request", request)
        .add("parse", Double())
        .connect(
            "request",
            "parse",
            source_port="result",
            target_port="value",
        )
    )

    class Cached(NodeHook):
        def enter(self, call):
            del call
            raise ShortCircuit(Output(10, "result"))

    with Runtime() as runtime:
        runtime.register("request.graph", graph)
        runtime.attach(Cached(), graph="request.graph", node="request")
        outputs = runtime.run("request.graph", 1)

    assert request.calls == 0
    assert outputs == (Output(20, "result"),)


def test_short_circuit_runs_exit_for_entered_hooks_in_reverse_order() -> None:
    node = AddOne()
    calls: list[str] = []

    class Outer(NodeHook):
        def enter(self, call):
            calls.append("outer.enter")
            return call

        def exit(self, call, outputs):
            del call
            calls.append("outer.exit")
            return outputs

    class Inner(NodeHook):
        def enter(self, call):
            del call
            calls.append("inner.enter")
            raise ShortCircuit(Output(5, "result"))

        def exit(self, call, outputs):
            del call
            calls.append("inner.exit")
            return outputs

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(node))
        runtime.attach(Outer(), graph="work.graph")
        runtime.attach(Inner(), graph="work.graph")
        outputs = runtime.run("work.graph", 1)

    assert outputs == (Output(5, "result"),)
    assert calls == [
        "outer.enter",
        "inner.enter",
        "inner.exit",
        "outer.exit",
    ]


def test_stop_graph_ends_without_running_remaining_nodes() -> None:
    first = AddOne()
    second = AddOne()
    graph = (
        Graph(entrypoint="first")
        .add("first", first)
        .add("second", second)
        .connect("first", "second", source_port="result", target_port="value")
    )

    class Stop(NodeHook):
        def exit(self, call, outputs):
            del call, outputs
            raise StopGraph(Output(99, "stopped"))

    with Runtime() as runtime:
        runtime.register("work.graph", graph)
        runtime.attach(Stop(), graph="work.graph", node="first")
        outputs = runtime.run("work.graph", 1)

    assert outputs == (Output(99, "stopped"),)
    assert first.calls == 1
    assert second.calls == 0


def test_error_hook_recovers_and_then_runs_outer_exit() -> None:
    calls: list[str] = []

    class Broken(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(result=int)

        def execute(self, inputs, context):
            del inputs, context
            raise ValueError("temporary")

    class Outer(NodeHook):
        def enter(self, call):
            calls.append("outer.enter")
            return call

        def exit(self, call, outputs):
            del call
            calls.append("outer.exit")
            return outputs

        def error(self, call, error):
            del call
            calls.append(f"outer.error:{error}")
            raise error

    class Recover(NodeHook):
        def enter(self, call):
            calls.append("recover.enter")
            return call

        def error(self, call, error):
            del call
            calls.append(f"recover.error:{error}")
            return (Output(7, "result"),)

        def exit(self, call, outputs):
            del call
            calls.append("recover.exit")
            return outputs

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(Broken()))
        runtime.attach(Outer(), graph="work.graph")
        runtime.attach(Recover(), graph="work.graph")
        outputs = runtime.run("work.graph", 1)

    assert outputs == (Output(7, "result"),)
    assert calls == [
        "outer.enter",
        "recover.enter",
        "recover.error:temporary",
        "recover.exit",
        "outer.exit",
    ]


def test_single_function_hooks_support_all_phases() -> None:
    node = AddOne()

    def enter(call: NodeCall) -> NodeCall:
        return call.with_inputs({"value": call.inputs["value"] + 1})

    async def exit(call: NodeCall, outputs):
        del call
        await asyncio.sleep(0)
        return (Output(outputs[0].value * 2, "result"),)

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(node))
        runtime.attach(enter, phase="enter", graph="work.graph")
        runtime.attach(exit, phase="exit", graph="work.graph")
        outputs = runtime.run("work.graph", 2)

    assert outputs == (Output(8, "result"),)


def test_async_node_and_async_hooks_share_background_loop() -> None:
    loop_threads: list[int] = []
    caller_thread = get_ident()

    class AsyncWork(AsyncNode):
        input_ports = Ports(value=int)
        output_ports = Ports(result=int)

        async def execute(self, inputs, context):
            del context
            await asyncio.sleep(0)
            loop_threads.append(get_ident())
            return Output(inputs["value"] + 1, "result")

    class AsyncHook(NodeHook):
        async def enter(self, call):
            await asyncio.sleep(0)
            loop_threads.append(get_ident())
            return call

        async def exit(self, call, outputs):
            del call
            await asyncio.sleep(0)
            loop_threads.append(get_ident())
            return outputs

    with Runtime() as runtime:
        runtime.register("async.graph", single_node_graph(AsyncWork()))
        runtime.attach(AsyncHook(), graph="async.graph")
        outputs = runtime.run("async.graph", 1)

    assert outputs == (Output(2, "result"),)
    assert len(set(loop_threads)) == 1
    assert loop_threads[0] != caller_thread


def test_hook_detach_is_hot_and_idempotent() -> None:
    node = AddOne()

    def double(call, outputs):
        del call
        return (Output(outputs[0].value * 2, "result"),)

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(node))
        handle = runtime.attach(double, phase="exit", graph="work.graph")
        assert runtime.run("work.graph", 1) == (Output(4, "result"),)
        handle.detach()
        handle.detach()
        assert runtime.run("work.graph", 1) == (Output(2, "result"),)


def test_execution_uses_hook_snapshot_until_graph_finishes() -> None:
    entered = ThreadEvent()
    release = ThreadEvent()

    class First(AddOne):
        def execute(self, inputs, context):
            entered.set()
            release.wait(1)
            return super().execute(inputs, context)

    graph = (
        Graph(entrypoint="first")
        .add("first", First())
        .add("second", AddOne())
        .connect("first", "second", source_port="result", target_port="value")
    )
    seen: list[str] = []

    def record(call):
        seen.append(call.node_id)
        return call

    with Runtime() as runtime:
        runtime.register("work.graph", graph)
        from threading import Thread

        result: list[tuple[Output, ...]] = []
        thread = Thread(target=lambda: result.append(runtime.run("work.graph", 1)))
        thread.start()
        assert entered.wait(1)
        runtime.attach(record, graph="work.graph")
        release.set()
        thread.join(1)
        assert not thread.is_alive()

        assert seen == []
        assert result == [(Output(3, "result"),)]
        runtime.run("work.graph", 1)
        assert seen == ["first", "second"]


def test_hook_outputs_still_obey_node_contract() -> None:
    node = AddOne()

    class Invalid(NodeHook):
        def enter(self, call):
            del call
            raise ShortCircuit(Output("wrong", "result"))

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(node))
        runtime.attach(Invalid(), graph="work.graph")
        with pytest.raises(Exception, match="expected int"):
            runtime.run("work.graph", 1)


def test_short_circuit_is_rejected_outside_enter() -> None:
    class Invalid(NodeHook):
        def exit(self, call, outputs):
            del call, outputs
            raise ShortCircuit(Output(1, "result"))

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(AddOne()))
        runtime.attach(Invalid(), graph="work.graph")
        with pytest.raises(HookExecutionError, match="only valid during hook enter"):
            runtime.run("work.graph", 1)


def test_control_signal_is_rejected_when_raised_by_node() -> None:
    class Invalid(Node):
        input_ports = Ports(value=int)
        output_ports = Ports(result=int)

        def execute(self, inputs, context):
            del inputs, context
            raise StopGraph(Output(1, "result"))

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(Invalid()))
        with pytest.raises(HookExecutionError, match="only be raised by a hook"):
            runtime.run("work.graph", 1)


def test_async_hook_can_stop_graph() -> None:
    class Stop(NodeHook):
        async def enter(self, call):
            del call
            await asyncio.sleep(0)
            raise StopGraph(Output(11, "stopped"))

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(AddOne()))
        runtime.attach(Stop(), graph="work.graph")
        outputs = runtime.run("work.graph", 1)

    assert outputs == (Output(11, "stopped"),)


def test_hook_scope_is_validated() -> None:
    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(AddOne()))
        with pytest.raises(ValueError, match="requires graph"):
            runtime.attach(lambda call: call, node="work")
        with pytest.raises(ValueError, match="has no node"):
            runtime.attach(
                lambda call: call,
                graph="work.graph",
                node="missing",
            )


def test_invalid_function_hook_result_is_classified() -> None:
    def invalid(call, outputs):
        del call, outputs
        return "wrong"

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(AddOne()))
        runtime.attach(invalid, phase="exit", graph="work.graph")
        with pytest.raises(InvalidOutputError):
            runtime.run("work.graph", 1)


def test_async_enter_can_short_circuit() -> None:
    node = AddOne()

    class AsyncCache(NodeHook):
        async def enter(self, call):
            del call
            await asyncio.sleep(0)
            raise ShortCircuit(Output(8, "result"))

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(node))
        runtime.attach(AsyncCache(), graph="work.graph")
        outputs = runtime.run("work.graph", 1)

    assert outputs == (Output(8, "result"),)
    assert node.calls == 0


def test_function_error_hook_can_recover_async_node_error() -> None:
    class Broken(AsyncNode):
        input_ports = Ports(value=int)
        output_ports = Ports(result=int)

        async def execute(self, inputs, context):
            del inputs, context
            await asyncio.sleep(0)
            raise LookupError("missing")

    async def recover(call, error):
        del call
        await asyncio.sleep(0)
        assert isinstance(error, LookupError)
        return (Output(3, "result"),)

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(Broken()))
        runtime.attach(recover, phase="error", graph="work.graph")
        outputs = runtime.run("work.graph", 1)

    assert outputs == (Output(3, "result"),)
