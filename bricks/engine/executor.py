"""只负责执行单张 Graph 的内核。"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping
from types import MappingProxyType
from typing import Any

from .core import (
    InputPolicy,
    Node,
    Output,
    require_non_empty_string,
)
from .errors import (
    ExecutionError,
    HookExecutionError,
    IncompleteInputsError,
    InvalidOutputError,
    PortValueTypeError,
)
from .events import Context, Event
from .graph import ExecutionPlan, Graph
from .hooks import (
    HookHandle,
    HookPhase,
    HookRegistry,
    NodeCall,
    NodeHook,
    Outputs,
    ShortCircuit,
    StopGraph,
)
from .runner import LocalRunner
from .slots import Slot

Emit = Callable[[Event], None]


class Engine:
    """执行 Graph 内的 Node、InputPolicy、Output 和 Edge。"""

    def __init__(
        self,
        *,
        hooks: HookRegistry | None = None,
        runner: LocalRunner | None = None,
    ) -> None:
        """组装动态 Hook 注册表和同步/异步调用 Runner。"""

        self.hooks = HookRegistry() if hooks is None else hooks
        self._runner = LocalRunner() if runner is None else runner

    def execute(
        self,
        name: str,
        graph: Graph,
        inputs: Any,
        emit: Emit,
        plan: ExecutionPlan | None = None,
        *,
        slot: Slot | None = None,
    ) -> tuple[Output, ...]:
        """执行一张 Graph 并返回终端 Output。"""

        name = require_non_empty_string(name, "graph name")
        if not isinstance(graph, Graph):
            raise TypeError("graph must be a Graph")
        if not callable(emit):
            raise TypeError("emit must be callable")
        if slot is not None and not isinstance(slot, Slot):
            raise TypeError("slot must be a Slot or None")
        if not graph.frozen:
            raise RuntimeError("Engine requires a frozen Graph")
        if plan is not None:
            if not isinstance(plan, ExecutionPlan):
                raise TypeError("plan must be an ExecutionPlan")
            if plan._graph is not graph:
                raise ValueError("execution plan belongs to a different Graph")

        prepared = self._coerce_inputs(graph, inputs)
        snapshot = self.hooks.snapshot(name)
        try:
            return self._run(name, graph, prepared, emit, snapshot, plan, slot)
        except StopGraph as signal:
            return self._coerce_outputs(signal.outputs, name, None, "StopGraph")

    def close(self) -> None:
        """关闭 Hook 注册表和后台异步 Runner。"""

        self.hooks.close()
        self._runner.close()

    def attach(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle:
        """声明并实现 GraphExecutor 的可选动态 Hook 能力。"""

        return self.hooks.attach(hook, phase=phase, graph=graph, node=node)

    def _run(
        self,
        graph_name: str,
        graph: Graph,
        initial_inputs: Mapping[str, Any],
        emit: Emit,
        hook_snapshot: tuple[Any, ...],
        plan: ExecutionPlan | None,
        slot: Slot | None,
    ) -> tuple[Output, ...]:
        nodes = graph.nodes
        active_nodes = set(nodes) if plan is None else plan.nodes
        specs = {
            node_id: graph._spec_for(node_id)
            for node_id in nodes
            if node_id in active_nodes
        }
        input_ports = {
            node_id: tuple(spec.input_ports) for node_id, spec in specs.items()
        }
        hooks_by_node = {
            node_id: self.hooks.for_node(hook_snapshot, node_id)
            for node_id in active_nodes
        }
        queues: dict[str, dict[str, deque[Any]]] = {
            node_id: {port: deque() for port in specs[node_id].input_ports}
            for node_id in nodes
            if node_id in active_nodes
        }
        outgoing_for = graph._outgoing_for if plan is None else plan._outgoing_for
        for port, value in initial_inputs.items():
            queues[graph.entrypoint][port].append(value)
        started: set[str] = set()
        terminal: list[Output] = []
        ready: deque[str] = deque((graph.entrypoint,))
        scheduled = {graph.entrypoint}

        def schedule_if_ready(node_id: str) -> None:
            if node_id in scheduled:
                return
            spec = specs[node_id]
            if spec.input_policy is InputPolicy.ON_START:
                runnable = node_id == graph.entrypoint and node_id not in started
            else:
                runnable = (
                    spec.input_policy._select(input_ports[node_id], queues[node_id])
                    is not None
                )
            if runnable:
                ready.append(node_id)
                scheduled.add(node_id)

        while ready:
            node_id = ready.popleft()
            scheduled.remove(node_id)
            node = nodes[node_id]
            spec = specs[node_id]
            policy = spec.input_policy
            if policy is InputPolicy.ON_START:
                started.add(node_id)
                consumed: dict[str, Any] = {}
            else:
                selection = policy._select(input_ports[node_id], queues[node_id])
                if selection is None:
                    continue
                consumed = {
                    port: queues[node_id][port].popleft() for port in selection
                }

            try:
                outputs = self._call_node(
                    graph_name,
                    node_id,
                    node,
                    MappingProxyType(consumed),
                    Context(emit, slot),
                    hooks_by_node[node_id],
                    spec.input_ports,
                )
            except ShortCircuit as exc:
                raise HookExecutionError(
                    "ShortCircuit is only valid during hook enter",
                    graph=graph_name,
                    node=node_id,
                ) from exc
            for output in outputs:
                declared = spec.output_ports
                if output.port not in declared:
                    raise InvalidOutputError(
                        f"node {node_id!r} produced unknown port {output.port!r}",
                        graph=graph_name,
                        node=node_id,
                    )
                expected = declared[output.port]
                if not isinstance(output.value, expected):
                    raise PortValueTypeError(
                        f"node {node_id!r} output {output.port!r} expected "
                        f"{expected.__name__}, got {type(output.value).__name__}",
                        graph=graph_name,
                        node=node_id,
                    )
                edges = outgoing_for(node_id, output.port)
                if not edges:
                    terminal.append(output)
                    continue
                for edge in edges:
                    target_type = specs[edge.target].input_ports[edge.target_port]
                    if not isinstance(output.value, target_type):
                        raise PortValueTypeError(
                            f"edge target {edge.target}.{edge.target_port} "
                            f"expected {target_type.__name__}",
                            graph=graph_name,
                            node=node_id,
                        )
                    queues[edge.target][edge.target_port].append(output.value)
                    schedule_if_ready(edge.target)

            # Consume one input group per turn so a hot cycle cannot starve peers.
            schedule_if_ready(node_id)

        leftovers = {
            f"{node_id}.{port}": len(values)
            for node_id, ports in queues.items()
            for port, values in ports.items()
            if values
        }
        if leftovers:
            raise IncompleteInputsError(
                f"graph stopped with incomplete inputs: {leftovers!r}",
                graph=graph_name,
            )
        return tuple(terminal)

    def _call_node(
        self,
        graph_name: str,
        node_id: str,
        node: Node,
        inputs: Mapping[str, Any],
        context: Context,
        hooks: tuple[NodeHook, ...],
        input_ports: Mapping[str, type[Any]],
    ) -> tuple[Output, ...]:
        """在 Hook 生命周期内调用 Node，并保留原始异常供 error 处理。"""

        original = NodeCall(graph_name, node_id, node, inputs, context)

        def invoke(index: int, call: NodeCall) -> Outputs:
            if index == len(hooks):
                try:
                    result = self._runner.resolve(
                        node.execute(call.inputs, context)
                    )
                except (ShortCircuit, StopGraph) as signal:
                    raise HookExecutionError(
                        f"{type(signal).__name__} can only be raised by a hook",
                        graph=graph_name,
                        node=node_id,
                    ) from signal
                return self._coerce_outputs(result, graph_name, node_id, "Node")

            hook = hooks[index]
            try:
                entered = self._runner.resolve(hook.enter(call))
            except ShortCircuit as signal:
                outputs = self._coerce_outputs(
                    signal.outputs, graph_name, node_id, "ShortCircuit"
                )
                return self._resolve_hook_outputs(
                    hook.exit(call, outputs), graph_name, node_id
                )

            entered = self._validate_hook_call(
                original, entered, input_ports, graph_name, node_id
            )
            try:
                outputs = invoke(index + 1, entered)
            except Exception as error:
                outputs = self._resolve_hook_outputs(
                    hook.error(entered, error), graph_name, node_id
                )
            return self._resolve_hook_outputs(
                hook.exit(entered, outputs), graph_name, node_id
            )

        try:
            return invoke(0, original)
        except ExecutionError:
            raise
        except (ShortCircuit, StopGraph):
            raise
        except Exception as exc:
            raise ExecutionError(
                f"node {node_id!r} failed: {exc}",
                graph=graph_name,
                node=node_id,
            ) from exc

    def _resolve_hook_outputs(
        self,
        value: object,
        graph_name: str,
        node_id: str,
    ) -> Outputs:
        resolved = self._runner.resolve(value)
        return self._coerce_outputs(resolved, graph_name, node_id, "Hook")

    @staticmethod
    def _coerce_outputs(
        result: object,
        graph_name: str,
        node_id: str | None,
        source: str,
    ) -> Outputs:
        try:
            return tuple(Engine._iter_outputs(result))
        except ExecutionError:
            raise
        except TypeError as exc:
            raise InvalidOutputError(
                f"{source} returned invalid output: {exc}",
                graph=graph_name,
                node=node_id,
            ) from exc
        except Exception as exc:
            raise ExecutionError(
                f"{source} failed while producing output: {exc}",
                graph=graph_name,
                node=node_id,
            ) from exc

    @staticmethod
    def _validate_hook_call(
        original: NodeCall,
        modified: object,
        input_ports: Mapping[str, type[Any]],
        graph_name: str,
        node_id: str,
    ) -> NodeCall:
        if not isinstance(modified, NodeCall):
            raise HookExecutionError(
                "hook enter must return NodeCall",
                graph=graph_name,
                node=node_id,
            )
        if (
            modified.graph != original.graph
            or modified.node_id != original.node_id
            or modified.node is not original.node
            or modified.context is not original.context
        ):
            raise HookExecutionError(
                "hook cannot replace graph, node, or context",
                graph=graph_name,
                node=node_id,
            )
        if set(modified.inputs) != set(original.inputs):
            raise HookExecutionError(
                "hook cannot add or remove node input ports",
                graph=graph_name,
                node=node_id,
            )
        for port, value in modified.inputs.items():
            expected = input_ports[port]
            if not isinstance(value, expected):
                raise PortValueTypeError(
                    f"hook input {port!r} expected {expected.__name__}, "
                    f"got {type(value).__name__}",
                    graph=graph_name,
                    node=node_id,
                )
        return modified

    @staticmethod
    def _iter_outputs(result: object) -> Iterator[Output]:
        """把 Node 返回值规范化为按顺序校验的 Output 迭代器。"""

        if result is None:
            return iter(())
        if isinstance(result, Output):
            return iter((result,))
        if not isinstance(result, Iterable) or isinstance(
            result, (str, bytes, Mapping)
        ):
            raise TypeError("Node must return Output, Iterable[Output], or None")

        def validated() -> Iterator[Output]:
            for output in result:
                if not isinstance(output, Output):
                    raise TypeError("Node output iterable must contain only Output")
                yield output

        return validated()

    @staticmethod
    def _coerce_inputs(graph: Graph, value: Any) -> Mapping[str, Any]:
        ports = graph._spec_for(graph.entrypoint).input_ports
        names = tuple(ports)
        if not names:
            if value not in (None, {}):
                raise PortValueTypeError("graph entrypoint accepts no input")
            return MappingProxyType({})
        if len(names) == 1:
            prepared = {names[0]: value}
        elif isinstance(value, Mapping) and set(value) == set(names):
            prepared = dict(value)
        else:
            raise PortValueTypeError(f"graph entrypoint requires input ports {names!r}")
        for port, item in prepared.items():
            expected = ports[port]
            if not isinstance(item, expected):
                raise PortValueTypeError(
                    f"entry input {port!r} expected {expected.__name__}, "
                    f"got {type(item).__name__}"
                )
        return MappingProxyType(prepared)
