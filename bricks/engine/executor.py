"""只负责执行单张 Graph 的内核。"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping
from types import MappingProxyType
from typing import Any, cast

from .core import (
    AsyncNode,
    InputPolicy,
    Node,
    Output,
    require_non_empty_string,
)
from .errors import (
    ExecutionError,
    IncompleteInputsError,
    InvalidOutputError,
    PortValueTypeError,
)
from .events import Context, Event
from .graph import Graph

Emit = Callable[[Event], None]


class Engine:
    """执行 Graph 内的 Node、InputPolicy、Output 和 Edge。"""

    def execute(
        self,
        name: str,
        graph: Graph,
        inputs: Any,
        emit: Emit,
    ) -> tuple[Output, ...]:
        """执行一张 Graph 并返回终端 Output。"""

        name = require_non_empty_string(name, "graph name")
        if not isinstance(graph, Graph):
            raise TypeError("graph must be a Graph")
        if not callable(emit):
            raise TypeError("emit must be callable")
        if not graph.frozen:
            raise RuntimeError("Engine requires a frozen Graph")

        prepared = self._coerce_inputs(graph, inputs)
        return self._run(name, graph, prepared, emit)

    def close(self) -> None:
        """本地无状态执行器没有需要释放的资源。"""

    def _run(
        self,
        graph_name: str,
        graph: Graph,
        initial_inputs: Mapping[str, Any],
        emit: Emit,
    ) -> tuple[Output, ...]:
        specs = {
            node_id: graph._spec_for(node_id)
            for node_id in graph.nodes
        }
        queues: dict[str, dict[str, deque[Any]]] = {
            node_id: {port: deque() for port in specs[node_id].input_ports}
            for node_id in graph.nodes
        }
        for port, value in initial_inputs.items():
            queues[graph.entrypoint][port].append(value)
        started: set[str] = set()
        terminal: list[Output] = []

        while True:
            progressed = False
            for node_id, node in graph.nodes.items():
                spec = specs[node_id]
                policy = spec.input_policy
                if policy is InputPolicy.ON_START:
                    if node_id != graph.entrypoint or node_id in started:
                        continue
                    started.add(node_id)
                    consumed: dict[str, Any] = {}
                else:
                    selection = policy._select(
                        tuple(spec.input_ports), queues[node_id]
                    )
                    if selection is None:
                        continue
                    consumed = {
                        port: queues[node_id][port].popleft()
                        for port in selection
                    }
                progressed = True
                outputs = self._call_node(
                    graph_name,
                    node_id,
                    node,
                    MappingProxyType(consumed),
                    Context(emit),
                )
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
                    edges = graph._outgoing_for(node_id, output.port)
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
            if not progressed:
                break

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

    @staticmethod
    def _call_node(
        graph_name: str,
        node_id: str,
        node: Node,
        inputs: Mapping[str, Any],
        context: Context,
    ) -> tuple[Output, ...]:
        try:
            result = node.execute(inputs, context)
            if isinstance(node, AsyncNode):
                result = asyncio.run(Engine._await_node_result(result))
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                f"node {node_id!r} failed: {exc}",
                graph=graph_name,
                node=node_id,
            ) from exc
        try:
            return tuple(Engine._iter_outputs(result))
        except ExecutionError:
            raise
        except TypeError as exc:
            raise InvalidOutputError(
                f"node {node_id!r} returned invalid output: {exc}",
                graph=graph_name,
                node=node_id,
            ) from exc
        except Exception as exc:
            raise ExecutionError(
                f"node {node_id!r} failed while producing output: {exc}",
                graph=graph_name,
                node=node_id,
            ) from exc

    @staticmethod
    async def _await_node_result(result: object) -> object:
        return await cast(Awaitable[object], result)

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
