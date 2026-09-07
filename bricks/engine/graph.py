"""可冻结的 typed Graph 定义。"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from inspect import iscoroutinefunction
from types import MappingProxyType
from typing import overload

from .core import (
    AsyncNode,
    InputPolicy,
    Node,
    Ports,
    require_non_empty_string,
)
from .errors import GraphError, GraphFrozenError, GraphValidationError
from .policies import BoundPolicy, PolicyRef, PolicyRegistry


@dataclass(frozen=True, slots=True)
class Edge:
    """连接源 Node output port 和目标 Node input port。"""

    source: str
    target: str
    source_port: str = "default"
    target_port: str = "default"

    def __post_init__(self) -> None:
        """校验节点 ID 和端口名称。"""

        require_non_empty_string(self.source, "edge source")
        require_non_empty_string(self.target, "edge target")
        require_non_empty_string(self.source_port, "edge source_port")
        require_non_empty_string(self.target_port, "edge target_port")


@dataclass(frozen=True, slots=True)
class NodeSpec:
    """Graph 冻结时保存的单个 Node 执行元数据。"""

    input_ports: Ports
    output_ports: Ports
    input_policy: BoundPolicy
    timeout: float | None


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    """一张冻结 Graph 的单次执行子图。"""

    graph: Graph
    entrypoint: str
    nodes: frozenset[str]
    edges: tuple[Edge, ...]
    _outgoing: Mapping[tuple[str, str], tuple[Edge, ...]]

    def outgoing_for(self, node_id: str, port: str) -> tuple[Edge, ...]:
        """返回计划内指定 output port 的有序下游连接。"""

        return self._outgoing.get((node_id, port), ())


class Graph:
    """描述 Node 通过 typed Edge 传递数据的静态有向图。"""

    def __init__(self, *, entrypoint: str | None = None) -> None:
        """创建处于构建状态的空 Graph。

        参数：
            entrypoint: 可选的入口 Node ID，也可稍后用 entry() 设置。
        """

        self._entrypoint = (
            None
            if entrypoint is None
            else require_non_empty_string(entrypoint, "graph entrypoint")
        )
        self._nodes: dict[str, Node] = {}
        self._edges: list[Edge] = []
        self._edge_set: set[Edge] = set()
        self._outgoing: dict[tuple[str, str], tuple[Edge, ...]] = {}
        self._node_specs: dict[str, NodeSpec] = {}
        self._frozen = False

    @property
    def entrypoint(self) -> str:
        """返回入口 Node ID。

        异常：
            GraphError: Graph 尚未设置入口。
        """

        if self._entrypoint is None:
            raise GraphError("graph has no entrypoint")
        return self._entrypoint

    @property
    def frozen(self) -> bool:
        """返回 Graph 是否已经冻结。"""

        return self._frozen

    @property
    def nodes(self) -> Mapping[str, Node]:
        """返回只读的 Node binding。"""

        return MappingProxyType(self._nodes)

    @property
    def edges(self) -> tuple[Edge, ...]:
        """返回按定义顺序排列的 Edge。"""

        return tuple(self._edges)

    def entry(self, node_id: str) -> Graph:
        """设置当前 Graph 的唯一入口。

        参数：
            node_id: 作为入口的 Node ID。

        返回：
            当前 Graph，便于链式构建。
        """

        self._ensure_mutable()
        self._entrypoint = require_non_empty_string(node_id, "graph entrypoint")
        return self

    @overload
    def add(self, node_id: str, node: Node, /) -> Graph: ...

    @overload
    def add(self, **nodes: Node) -> Graph: ...

    def add(self, *args: object, **nodes: Node) -> Graph:
        """把一个或多个 Node 行为绑定到 Graph 中的位置。

        参数：
            args: 单个 Node 的 ID 和可复用 Node 行为。
            nodes: 以关键字名称作为 Node ID 的一组 Node 行为。

        返回：
            当前 Graph，便于链式构建。
        """

        self._ensure_mutable()
        if args and nodes:
            raise TypeError("add accepts either (node_id, node) or keyword nodes")
        bindings: tuple[tuple[object, object], ...]
        if args:
            if len(args) != 2:
                raise TypeError("add expects a node_id and node")
            bindings = ((args[0], args[1]),)
        else:
            if not nodes:
                raise TypeError("add requires at least one node")
            bindings = tuple(nodes.items())

        validated: list[tuple[str, Node]] = []
        for node_id, node in bindings:
            node_id = require_non_empty_string(node_id, "node_id")
            if not isinstance(node, Node):
                raise TypeError(f"node {node_id!r} must be a Node")
            if node_id in self._nodes:
                raise GraphError(f"duplicate node {node_id!r}")
            validated.append((node_id, node))
        self._nodes.update(validated)
        return self

    def connect(
        self,
        source: str,
        target: str,
        *,
        source_port: str = "default",
        target_port: str = "default",
    ) -> Graph:
        """增加一条端口到端口的有向连接。

        参数：
            source: 源 Node ID。
            target: 目标 Node ID。
            source_port: 源 output port。
            target_port: 目标 input port。

        返回：
            当前 Graph，便于链式构建。
        """

        self._ensure_mutable()
        edge = Edge(source, target, source_port, target_port)
        if edge in self._edge_set:
            raise GraphError(f"duplicate edge {edge!r}")
        self._edges.append(edge)
        self._edge_set.add(edge)
        return self

    def plan(
        self,
        *,
        include: Iterable[str],
        policies: PolicyRegistry | None = None,
    ) -> ExecutionPlan:
        """选择由原有节点和边组成的严格执行子图。

        未选节点不会执行，也不会自动连接其前后节点。首次创建计划时会
        冻结 Graph，保证计划所引用的定义之后不再变化。
        """

        if isinstance(include, (str, bytes)):
            raise TypeError("plan include must be an iterable of node IDs")
        try:
            selected = frozenset(include)
        except TypeError as exc:
            raise TypeError("plan include must be an iterable of node IDs") from exc
        if not selected:
            raise GraphValidationError("execution plan must contain at least one node")
        if any(not isinstance(node_id, str) or not node_id for node_id in selected):
            raise TypeError("plan node IDs must be non-empty strings")
        if not self._frozen:
            self.freeze(policies)

        unknown = selected - set(self._nodes)
        if unknown:
            raise GraphValidationError(
                f"execution plan references unknown nodes: {sorted(unknown)!r}"
            )
        if self.entrypoint not in selected:
            raise GraphValidationError(
                f"execution plan must contain graph entrypoint {self.entrypoint!r}"
            )

        edges = tuple(
            edge
            for edge in self._edges
            if edge.source in selected and edge.target in selected
        )
        adjacency: dict[str, set[str]] = defaultdict(set)
        incoming_ports: dict[str, set[str]] = defaultdict(set)
        outgoing: dict[tuple[str, str], list[Edge]] = defaultdict(list)
        for edge in edges:
            adjacency[edge.source].add(edge.target)
            incoming_ports[edge.target].add(edge.target_port)
            outgoing[(edge.source, edge.source_port)].append(edge)

        reachable = self._reachable(adjacency)
        unreachable = selected - reachable
        if unreachable:
            raise GraphValidationError(
                "execution plan nodes are unreachable from entrypoint using original "
                f"edges: {sorted(unreachable)!r}"
            )
        for node_id in selected:
            spec = self._node_specs[node_id]
            if (
                node_id == self.entrypoint
                or spec.input_policy.ref.name != "bricks.core/all"
            ):
                continue
            missing = set(spec.input_ports) - incoming_ports[node_id]
            if missing:
                raise GraphValidationError(
                    f"execution plan leaves ALL node {node_id!r} without inputs: "
                    f"{sorted(missing)!r}"
                )

        return ExecutionPlan(
            self,
            self.entrypoint,
            selected,
            edges,
            MappingProxyType({key: tuple(value) for key, value in outgoing.items()}),
        )

    def freeze(self, policies: PolicyRegistry | None = None) -> Graph:
        """校验并冻结 Graph。

        返回：
            已冻结的当前 Graph。

        异常：
            GraphValidationError: 节点、端口、策略、连接或可达性不合法。
        """

        if self._frozen:
            return self
        if policies is None:
            policies = PolicyRegistry()
        if not isinstance(policies, PolicyRegistry):
            raise TypeError("policies must be a PolicyRegistry or None")
        self._validate_structure(policies)
        outgoing: dict[tuple[str, str], list[Edge]] = defaultdict(list)
        for edge in self._edges:
            outgoing[(edge.source, edge.source_port)].append(edge)
        self._outgoing = {key: tuple(edges) for key, edges in outgoing.items()}
        self._frozen = True
        return self

    def spec_for(self, node_id: str) -> NodeSpec:
        """返回冻结后的 Node 执行元数据。"""

        self._ensure_frozen()
        return self._node_specs[node_id]

    def _execution_timeouts(self) -> Mapping[str, float | None]:
        """返回冻结后的 Node timeout 快照。"""

        self._ensure_frozen()
        return MappingProxyType(
            {node_id: spec.timeout for node_id, spec in self._node_specs.items()}
        )

    def outgoing_for(self, node_id: str, port: str) -> tuple[Edge, ...]:
        """返回指定 output port 的有序下游连接。

        参数：
            node_id: 源 Node ID。
            port: 源 output port。

        返回：
            按定义顺序排列的 Edge。
        """

        self._ensure_frozen()
        return self._outgoing.get((node_id, port), ())

    def _validate_structure(self, policies: PolicyRegistry) -> None:
        """执行冻结前的完整静态校验。"""

        if not self._nodes:
            raise GraphValidationError("graph must contain at least one node")
        if self._entrypoint not in self._nodes:
            raise GraphValidationError("graph entrypoint must reference a node")

        specs: dict[str, NodeSpec] = {}
        for node_id, node in self._nodes.items():
            inputs = node.input_ports
            outputs = node.output_ports
            policy = node.input_policy
            timeout = node.timeout
            if not isinstance(inputs, Ports) or not isinstance(outputs, Ports):
                raise GraphValidationError(f"node {node_id!r} ports must be Ports")
            if not isinstance(policy, (InputPolicy, PolicyRef)):
                raise GraphValidationError(
                    f"node {node_id!r} input_policy must be InputPolicy or PolicyRef"
                )
            if timeout is not None:
                if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
                    raise GraphValidationError(
                        f"node {node_id!r} timeout must be a number or None"
                    )
                if not math.isfinite(timeout) or timeout <= 0:
                    raise GraphValidationError(
                        f"node {node_id!r} timeout must be finite and greater than zero"
                    )
            self._validate_execute_style(node_id, node)
            try:
                bound_policy = policies.bind(policy)
            except (TypeError, ValueError) as exc:
                raise GraphValidationError(
                    f"node {node_id!r} has invalid input policy: {exc}"
                ) from exc
            self._validate_policy(node_id, inputs, bound_policy)
            specs[node_id] = NodeSpec(inputs, outputs, bound_policy, timeout)

        adjacency: dict[str, set[str]] = defaultdict(set)
        incoming_ports: dict[str, set[str]] = defaultdict(set)
        for edge in self._edges:
            if edge.source not in self._nodes or edge.target not in self._nodes:
                raise GraphValidationError(f"edge references unknown node: {edge!r}")
            source_ports = specs[edge.source].output_ports
            target_ports = specs[edge.target].input_ports
            if edge.source_port not in source_ports:
                raise GraphValidationError(
                    f"node {edge.source!r} has no output port {edge.source_port!r}"
                )
            if edge.target_port not in target_ports:
                raise GraphValidationError(
                    f"node {edge.target!r} has no input port {edge.target_port!r}"
                )
            if not Ports.is_type_compatible(
                source_ports[edge.source_port], target_ports[edge.target_port]
            ):
                raise GraphValidationError(
                    f"incompatible edge {edge.source}.{edge.source_port} -> "
                    f"{edge.target}.{edge.target_port}"
                )
            adjacency[edge.source].add(edge.target)
            incoming_ports[edge.target].add(edge.target_port)

        reachable = self._reachable(adjacency)
        unreachable = set(self._nodes) - reachable
        if unreachable:
            raise GraphValidationError(
                f"nodes are unreachable from entrypoint: {sorted(unreachable)!r}"
            )
        for node_id, spec in specs.items():
            if (
                node_id == self.entrypoint
                or spec.input_policy.ref.name != "bricks.core/all"
            ):
                continue
            missing = set(spec.input_ports) - incoming_ports[node_id]
            if missing:
                raise GraphValidationError(
                    f"ALL node {node_id!r} has no incoming edge for inputs: "
                    f"{sorted(missing)!r}"
                )
        self._node_specs = specs

    @staticmethod
    def _validate_execute_style(node_id: str, node: Node) -> None:
        """保证 Node 类型与 execute 的同步风格一致。

        参数：
            node_id: 用于错误定位的 Node ID。
            node: 等待校验的 Node。
        """

        execute = type(node).execute
        asynchronous = iscoroutinefunction(execute)
        if isinstance(node, AsyncNode) != asynchronous:
            expected = "async" if isinstance(node, AsyncNode) else "sync"
            raise GraphValidationError(
                f"node {node_id!r} must implement {expected} execute()"
            )

    @staticmethod
    def _validate_policy(
        node_id: str,
        ports: Ports,
        policy: BoundPolicy,
    ) -> None:
        """校验策略和声明端口是否匹配。

        参数：
            node_id: 用于错误定位的 Node ID。
            ports: Node 的 input ports。
            policy: Node 的输入策略。
        """

        declared = tuple(ports)
        if policy.on_start:
            if declared:
                raise GraphValidationError(
                    f"node {node_id!r} on_start policy requires zero inputs"
                )
            return
        if not declared:
            raise GraphValidationError(
                f"zero-input node {node_id!r} must use InputPolicy.ON_START"
            )

    def _reachable(self, adjacency: Mapping[str, set[str]]) -> set[str]:
        """计算从入口可达的 Node。

        参数：
            adjacency: Node ID 到直接下游的邻接表。

        返回：
            包含入口的可达 Node ID 集合。
        """

        reached: set[str] = set()
        pending = [self.entrypoint]
        while pending:
            node_id = pending.pop()
            if node_id in reached:
                continue
            reached.add(node_id)
            pending.extend(adjacency.get(node_id, set()))
        return reached

    def _ensure_mutable(self) -> None:
        """拒绝冻结后的修改。"""

        if self._frozen:
            raise GraphFrozenError("graph is frozen")

    def _ensure_frozen(self) -> None:
        """拒绝在冻结前读取执行快照。"""

        if not self._frozen:
            raise GraphError("graph is not frozen")
