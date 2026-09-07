"""动态 Node Hook、控制信号和异步执行桥接的契约测试。"""

from __future__ import annotations

import asyncio
from threading import Event as ThreadEvent
from threading import get_ident

import pytest

from bricks import AsyncNode, Graph, Node, Output, Ports, Runtime
from bricks.engine.errors import HookExecutionError, InvalidOutputError
from bricks.engine.hooks import (
    NodeCall,
    NodeHook,
    ShortCircuit,
    StopGraph,
)


class AddOne(Node):
    """当前契约测试使用的 AddOne 替代实现。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
        calls: 按顺序记录的测试调用信息。
    """

    input_ports = Ports(value=int)
    output_ports = Ports(result=int)

    def __init__(self) -> None:
        """初始化实例及其依赖，建立当前对象独立维护的状态。"""

        self.calls = 0

    def execute(self, inputs, context) -> Output:
        """执行当前测试场景的节点行为，供外层契约断言检查。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            测试节点或替代执行器产生的返回值。
        """

        del context
        self.calls += 1
        return Output(inputs["value"] + 1, "result")


def single_node_graph(node: Node) -> Graph:
    """构造只有一个节点的 Hook 契约测试 Graph。

    Args:
        node: 节点实例或作用域中的节点 ID，以接口类型为准。

    Returns:
        包含指定测试节点的 Graph。
    """

    return Graph(entrypoint="work").add("work", node)


def test_object_hook_transforms_inputs_and_outputs() -> None:
    """验证对象 Hook 可以转换节点输入和输出。"""

    node = AddOne()
    order: list[str] = []

    class Transform(NodeHook):
        def enter(self, call: NodeCall) -> NodeCall:
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            order.append("enter")
            return call.with_inputs({"value": call.inputs["value"] * 2})

        def exit(self, call, outputs):
            """在节点完成后调用出口 Hook 并取得输出。

            Args:
                call: Hook 当前处理的节点调用记录。
                outputs: 按交付顺序组织的节点输出。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

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
    """验证短路跳过节点并沿原边继续传递输出。"""

    request = AddOne()

    class Double(Node):
        """当前契约测试使用的 Double 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(result=int)

        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

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
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。
            """

            del call
            raise ShortCircuit(Output(10, "result"))

    with Runtime() as runtime:
        runtime.register("request.graph", graph)
        runtime.attach(Cached(), graph="request.graph", node="request")
        outputs = runtime.run("request.graph", 1)

    assert request.calls == 0
    assert outputs == (Output(20, "result"),)


def test_short_circuit_runs_exit_for_entered_hooks_in_reverse_order() -> None:
    """验证短路后按逆序执行已进入 Hook 的出口。"""

    node = AddOne()
    calls: list[str] = []

    class Outer(NodeHook):
        def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            calls.append("outer.enter")
            return call

        def exit(self, call, outputs):
            """在节点完成后调用出口 Hook 并取得输出。

            Args:
                call: Hook 当前处理的节点调用记录。
                outputs: 按交付顺序组织的节点输出。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            del call
            calls.append("outer.exit")
            return outputs

    class Inner(NodeHook):
        def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。
            """

            del call
            calls.append("inner.enter")
            raise ShortCircuit(Output(5, "result"))

        def exit(self, call, outputs):
            """在节点完成后调用出口 Hook 并取得输出。

            Args:
                call: Hook 当前处理的节点调用记录。
                outputs: 按交付顺序组织的节点输出。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

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
    """验证停止信号阻止剩余节点执行。"""

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
            """在节点完成后调用出口 Hook 并取得输出。

            Args:
                call: Hook 当前处理的节点调用记录。
                outputs: 按交付顺序组织的节点输出。
            """

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
    """验证错误 Hook 恢复后继续执行外层出口 Hook。"""

    calls: list[str] = []

    class Broken(Node):
        """当前契约测试使用的 Broken 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(result=int)

        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Raises:
                ValueError: 参数值或字段组合不合法。
            """

            del inputs, context
            raise ValueError("temporary")

    class Outer(NodeHook):
        def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            calls.append("outer.enter")
            return call

        def exit(self, call, outputs):
            """在节点完成后调用出口 Hook 并取得输出。

            Args:
                call: Hook 当前处理的节点调用记录。
                outputs: 按交付顺序组织的节点输出。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            del call
            calls.append("outer.exit")
            return outputs

        def error(self, call, error):
            """在错误阶段执行回调，未恢复的异常继续传播。

            Args:
                call: Hook 当前处理的节点调用记录。
                error: 需要传播、记录或用于恢复的异常。
            """

            del call
            calls.append(f"outer.error:{error}")
            raise error

    class Recover(NodeHook):
        def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            calls.append("recover.enter")
            return call

        def error(self, call, error):
            """在错误阶段执行回调，未恢复的异常继续传播。

            Args:
                call: Hook 当前处理的节点调用记录。
                error: 需要传播、记录或用于恢复的异常。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            del call
            calls.append(f"recover.error:{error}")
            return (Output(7, "result"),)

        def exit(self, call, outputs):
            """在节点完成后调用出口 Hook 并取得输出。

            Args:
                call: Hook 当前处理的节点调用记录。
                outputs: 按交付顺序组织的节点输出。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

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
    """验证单函数 Hook 支持入口、出口和错误阶段。"""

    node = AddOne()

    def enter(call: NodeCall) -> NodeCall:
        """在节点执行前调用入口 Hook 并取得调用参数。

        Args:
            call: Hook 当前处理的节点调用记录。

        Returns:
            同步调用或异步等待完成后的处理结果。
        """

        return call.with_inputs({"value": call.inputs["value"] + 1})

    async def exit(call: NodeCall, outputs):
        """在节点完成后调用出口 Hook 并取得输出。

        Args:
            call: Hook 当前处理的节点调用记录。
            outputs: 按交付顺序组织的节点输出。

        Returns:
            同步调用或异步等待完成后的处理结果。
        """

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
    """验证异步节点与异步 Hook 使用同一后台循环。"""

    loop_threads: list[int] = []
    caller_thread = get_ident()

    class AsyncWork(AsyncNode):
        """当前契约测试使用的 AsyncWork 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(result=int)

        async def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            del context
            await asyncio.sleep(0)
            loop_threads.append(get_ident())
            return Output(inputs["value"] + 1, "result")

    class AsyncHook(NodeHook):
        async def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

            await asyncio.sleep(0)
            loop_threads.append(get_ident())
            return call

        async def exit(self, call, outputs):
            """在节点完成后调用出口 Hook 并取得输出。

            Args:
                call: Hook 当前处理的节点调用记录。
                outputs: 按交付顺序组织的节点输出。

            Returns:
                同步调用或异步等待完成后的处理结果。
            """

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
    """验证 Hook 可以动态且幂等地卸载。"""

    node = AddOne()

    def double(call, outputs):
        """将测试输入翻倍，验证函数 Hook 能转换输入。

        Args:
            call: Hook 当前处理的节点调用记录。
            outputs: 按交付顺序组织的节点输出。

        Returns:
            输入经过倍增处理的 NodeCall。
        """

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
    """验证 Graph 整次执行使用固定的 Hook 快照。"""

    entered = ThreadEvent()
    release = ThreadEvent()

    class First(AddOne):
        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

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
        """记录当前回调观察到的数据供测试断言。

        Args:
            call: Hook 当前处理的节点调用记录。

        Returns:
            当前测试回调收集或构造的结果。
        """

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
    """验证 Hook 输出仍受节点端口契约约束。"""

    node = AddOne()

    class Invalid(NodeHook):
        def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。
            """

            del call
            raise ShortCircuit(Output("wrong", "result"))

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(node))
        runtime.attach(Invalid(), graph="work.graph")
        with pytest.raises(Exception, match="expected int"):
            runtime.run("work.graph", 1)


def test_short_circuit_is_rejected_outside_enter() -> None:
    """验证入口以外阶段拒绝短路信号。"""

    class Invalid(NodeHook):
        def exit(self, call, outputs):
            """在节点完成后调用出口 Hook 并取得输出。

            Args:
                call: Hook 当前处理的节点调用记录。
                outputs: 按交付顺序组织的节点输出。
            """

            del call, outputs
            raise ShortCircuit(Output(1, "result"))

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(AddOne()))
        runtime.attach(Invalid(), graph="work.graph")
        with pytest.raises(HookExecutionError, match="only valid during hook enter"):
            runtime.run("work.graph", 1)


def test_control_signal_is_rejected_when_raised_by_node() -> None:
    """验证节点自身不能伪造 Hook 流程控制信号。"""

    class Invalid(Node):
        """当前契约测试使用的 Invalid 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(result=int)

        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。
            """

            del inputs, context
            raise StopGraph(Output(1, "result"))

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(Invalid()))
        with pytest.raises(HookExecutionError, match="only be raised by a hook"):
            runtime.run("work.graph", 1)


def test_async_hook_can_stop_graph() -> None:
    """验证异步 Hook 能终止 Graph。"""

    class Stop(NodeHook):
        async def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。
            """

            del call
            await asyncio.sleep(0)
            raise StopGraph(Output(11, "stopped"))

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(AddOne()))
        runtime.attach(Stop(), graph="work.graph")
        outputs = runtime.run("work.graph", 1)

    assert outputs == (Output(11, "stopped"),)


@pytest.mark.parametrize(
    "stop_outputs", [(), (Output("stopped"),), (Output(1), Output(2))]
)
@pytest.mark.parametrize("mode", ["sync", "async", "engine"])
def test_stop_graph_preserves_published_outputs_in_result_and_stream(
    stop_outputs, mode
) -> None:
    """验证停止 Graph 后结果与输出流保留已发布输出。

    Args:
        stop_outputs: 当前用例使用的 stop_outputs 夹具或参数化输入。
        mode: 当前用例使用的 mode 夹具或参数化输入。
    """

    class Fan(Node):
        """当前契约测试使用的 Fan 替代实现。

        Attributes:
            output_ports: 节点声明的输出端口及其类型。
        """

        output_ports = Ports(default=object, next=object)

        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Returns:
                测试节点或替代执行器产生的返回值。
            """

            return (Output("before"), Output("next", "next"))

    class End(Node):
        def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Raises:
                AssertionError: 内部不变量或测试契约断言不成立。
            """

            raise AssertionError("stopped node must not execute")

    class Stop(NodeHook):
        def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。
            """

            raise StopGraph(*stop_outputs)

    graph = (
        Graph(entrypoint="first")
        .add(first=Fan(), last=End())
        .connect("first", "last", source_port="next")
    )
    expected = (Output("before"), *stop_outputs)
    if mode == "engine":
        from bricks import Execution
        from bricks.engine.executor import Engine

        engine = Engine()
        try:
            engine.attach(Stop(), graph="work", node="last")
            execution = Execution("work")
            execution.start(graph.freeze())
            engine.execute("work", graph, None, lambda e: None, execution=execution)
            execution.succeed()
            assert execution.result() == tuple(execution) == expected
        finally:
            engine.close()
        return

    with Runtime() as runtime:
        runtime.register("work", graph)
        runtime.attach(Stop(), graph="work", node="last")
        if mode == "sync":
            assert tuple(runtime.iter("work", output_buffer=1)) == expected
        else:

            async def collect():
                """收集当前回调观察到的输出，供测试断言。

                Returns:
                    当前测试回调收集或构造的结果。
                """

                return tuple(
                    [output async for output in runtime.aiter("work", output_buffer=1)]
                )

            assert asyncio.run(collect()) == expected
        assert runtime.executions()[-1].result() == expected


def test_hook_scope_is_validated() -> None:
    """验证 Hook 作用域参数的合法性。"""

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
    """验证非法函数 Hook 返回值使用明确的错误类型。"""

    def invalid(call, outputs):
        """产生非法返回值以验证契约校验。

        Args:
            call: Hook 当前处理的节点调用记录。
            outputs: 按交付顺序组织的节点输出。

        Returns:
            故意不满足被测接口契约的返回值。
        """

        del call, outputs
        return "wrong"

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(AddOne()))
        runtime.attach(invalid, phase="exit", graph="work.graph")
        with pytest.raises(InvalidOutputError):
            runtime.run("work.graph", 1)


def test_async_enter_can_short_circuit() -> None:
    """验证异步入口 Hook 能短路节点。"""

    node = AddOne()

    class AsyncCache(NodeHook):
        async def enter(self, call):
            """在节点执行前调用入口 Hook 并取得调用参数。

            Args:
                call: Hook 当前处理的节点调用记录。
            """

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
    """验证函数错误 Hook 能恢复异步节点的业务异常。"""

    class Broken(AsyncNode):
        """当前契约测试使用的 Broken 替代实现。

        Attributes:
            input_ports: 节点声明的输入端口及其类型。
            output_ports: 节点声明的输出端口及其类型。
        """

        input_ports = Ports(value=int)
        output_ports = Ports(result=int)

        async def execute(self, inputs, context):
            """执行当前测试场景的节点行为，供外层契约断言检查。

            Args:
                inputs: 入口数据或按端口名称组织的输入映射。
                context: 当前调用的执行或插件上下文。

            Raises:
                LookupError: 指定编码不存在或不是文本编码。
            """

            del inputs, context
            await asyncio.sleep(0)
            raise LookupError("missing")

    async def recover(call, error):
        """用测试指定的输出恢复业务异常。

        Args:
            call: Hook 当前处理的节点调用记录。
            error: 需要传播、记录或用于恢复的异常。

        Returns:
            用于恢复业务异常的测试输出。
        """

        del call
        await asyncio.sleep(0)
        assert isinstance(error, LookupError)
        return (Output(3, "result"),)

    with Runtime() as runtime:
        runtime.register("work.graph", single_node_graph(Broken()))
        runtime.attach(recover, phase="error", graph="work.graph")
        outputs = runtime.run("work.graph", 1)

    assert outputs == (Output(3, "result"),)
