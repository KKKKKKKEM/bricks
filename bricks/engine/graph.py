"""静态 Graph 拓扑和 Flow 定义。"""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from ._validation import require_non_empty_string
from .errors import (
    GraphDefinitionError,
    GraphFrozenError,
    GraphValidationError,
    UnknownFlowError,
    UnknownNodeError,
)
from .inputs import InputPolicy
from .node import Node
from .ports import Ports, is_type_compatible


@dataclass(frozen=True, slots=True)
class Edge:
    """从源 Node 端口到目标 Node 的静态连接。"""

    source: str
    target: str
    source_port: str = "default"
    target_port: str = "default"

    def __post_init__(self) -> None:
        """校验 Edge 的节点 ID 和端口。

        异常：
            TypeError: 任一字段不是字符串。
            ValueError: 任一字段为空。
        """

        require_non_empty_string(self.source, "edge source")
        require_non_empty_string(self.target, "edge target")
        require_non_empty_string(self.source_port, "edge source_port")
        require_non_empty_string(self.target_port, "edge target_port")


@dataclass(frozen=True, slots=True)
class Endpoint:
    """Flow 用于返回结果的 Node 端口。"""

    node_id: str
    port: str = "default"

    def __post_init__(self) -> None:
        """校验 Endpoint 的 Node ID 和端口。

        异常：
            TypeError: 任一字段不是字符串。
            ValueError: 任一字段为空。
        """

        require_non_empty_string(self.node_id, "endpoint node_id")
        require_non_empty_string(self.port, "endpoint port")


@dataclass(frozen=True, slots=True)
class Flow:
    """Graph 对外提供的一项执行能力。"""

    name: str
    entrypoint: str
    endpoints: frozenset[Endpoint]

    def __post_init__(self) -> None:
        """校验 Flow 身份、入口和终止端点。

        异常：
            TypeError: 名称或入口不是字符串，或 endpoints 包含非法类型。
            ValueError: 名称、入口或 endpoints 为空。
        """

        require_non_empty_string(self.name, "flow name")
        require_non_empty_string(self.entrypoint, "flow entrypoint")

        endpoints = frozenset(self.endpoints)
        if not endpoints:
            raise ValueError("flow endpoints must not be empty")
        if not all(isinstance(endpoint, Endpoint) for endpoint in endpoints):
            raise TypeError("flow endpoints must contain only Endpoint instances")
        object.__setattr__(self, "endpoints", endpoints)


class Graph:
    """构建完成后可以冻结的 Graph 定义。"""

    __slots__ = (
        "_edge_set",
        "_edges",
        "_flows",
        "_frozen",
        "_nodes",
        "_outgoing",
        "name",
    )

    def __init__(self, name: str) -> None:
        """创建一个处于构建状态的空 Graph。

        参数：
            name: Graph 的稳定名称。

        异常：
            TypeError: name 不是字符串。
            ValueError: name 为空。
        """

        self.name = require_non_empty_string(name, "graph name")
        self._nodes: dict[str, Node] = {}
        self._edges: list[Edge] = []
        self._edge_set: set[Edge] = set()
        self._flows: dict[str, Flow] = {}
        self._outgoing: dict[tuple[str, str], list[Edge]] = defaultdict(list)
        self._frozen = False

    @property
    def frozen(self) -> bool:
        """返回 Graph 是否已经冻结。"""

        return self._frozen

    @property
    def nodes(self) -> Mapping[str, Node]:
        """返回只读的 Node ID 到 Node 映射。"""

        return MappingProxyType(self._nodes)

    @property
    def edges(self) -> tuple[Edge, ...]:
        """返回按定义顺序排列的全部 Edge。"""

        return tuple(self._edges)

    @property
    def flows(self) -> Mapping[str, Flow]:
        """返回只读的 Flow 名称到 Flow 映射。"""

        return MappingProxyType(self._flows)

    def add_node(self, node_id: str, node: Node) -> Graph:
        """把 Node 行为绑定到 Graph 中的一个位置。

        参数：
            node_id: Node 在当前 Graph 中的唯一 ID。
            node: 需要绑定的 Node 行为对象。

        返回：
            当前 Graph，便于链式构建。

        异常：
            GraphFrozenError: Graph 已经冻结。
            GraphDefinitionError: node_id 已经存在。
            TypeError: node_id 或 node 类型不合法。
            ValueError: node_id 为空。
        """

        self._ensure_mutable()
        node_id = require_non_empty_string(node_id, "node_id")
        if not isinstance(node, Node):
            raise TypeError("node must be a Node instance")
        if node_id in self._nodes:
            raise GraphDefinitionError(f"duplicate node {node_id!r}")
        self._nodes[node_id] = node
        return self

    def connect(
        self,
        source: str,
        target: str,
        *,
        source_port: str = "default",
        target_port: str = "default",
    ) -> Graph:
        """连接一个源 Node 端口和目标 Node。

        参数：
            source: 源 Node ID。
            target: 目标 Node ID。
            source_port: Output 离开源 Node 时使用的端口。
            target_port: 目标 Node 接收 value 的 input port。

        返回：
            当前 Graph，便于链式构建。

        异常：
            GraphFrozenError: Graph 已经冻结。
            GraphDefinitionError: 相同 Edge 已经存在。
            TypeError: 任一参数类型不合法。
            ValueError: 任一字符串参数为空。

        说明：
            source 和 target 是否存在统一在 freeze() 时检查，因此可以先连边再加节点。
        """

        self._ensure_mutable()
        edge = Edge(
            source=source,
            target=target,
            source_port=source_port,
            target_port=target_port,
        )
        if edge in self._edge_set:
            raise GraphDefinitionError(
                "duplicate edge "
                f"({edge.source!r}, {edge.source_port!r}, "
                f"{edge.target!r}, {edge.target_port!r})"
            )

        self._edges.append(edge)
        self._edge_set.add(edge)
        self._outgoing[(edge.source, edge.source_port)].append(edge)
        return self

    def add_flow(self, flow: Flow) -> Graph:
        """向 Graph 注册一项公开 Flow。

        参数：
            flow: 包含名称、入口和终止端点的 Flow。

        返回：
            当前 Graph，便于链式构建。

        异常：
            GraphFrozenError: Graph 已经冻结。
            GraphDefinitionError: 同名 Flow 已经存在。
            TypeError: flow 不是 Flow 实例。
        """

        self._ensure_mutable()
        if not isinstance(flow, Flow):
            raise TypeError("flow must be a Flow instance")
        if flow.name in self._flows:
            raise GraphDefinitionError(f"duplicate flow {flow.name!r}")
        self._flows[flow.name] = flow
        return self

    def freeze(self) -> Graph:
        """校验并冻结 Graph。

        返回：
            已冻结的当前 Graph；重复调用仍返回自身。

        异常：
            GraphValidationError: Graph 引用、入口或可达性不合法。
        """

        if self._frozen:
            return self

        issues = self._validation_issues()
        if issues:
            raise GraphValidationError(issues)

        self._frozen = True
        return self

    def node(self, node_id: str) -> Node:
        """根据 Node ID 查询行为对象。

        参数：
            node_id: 需要查询的 Node ID。

        返回：
            绑定在该位置的 Node。

        异常：
            UnknownNodeError: Graph 中不存在 node_id。
        """

        try:
            return self._nodes[node_id]
        except KeyError:
            raise UnknownNodeError(node_id) from None

    def flow(self, flow_name: str) -> Flow:
        """根据名称查询 Flow。

        参数：
            flow_name: 需要查询的 Flow 名称。

        返回：
            对应的 Flow。

        异常：
            UnknownFlowError: Graph 中不存在 flow_name。
        """

        try:
            return self._flows[flow_name]
        except KeyError:
            raise UnknownFlowError(flow_name) from None

    def outgoing(
        self,
        node_id: str,
        source_port: str = "default",
    ) -> tuple[Edge, ...]:
        """查询一个 Node 端口连接的全部 Edge。

        参数：
            node_id: 源 Node ID。
            source_port: 需要查询的输出端口。

        返回：
            按定义顺序排列的匹配 Edge。

        异常：
            UnknownNodeError: Graph 中不存在 node_id。
            TypeError: source_port 不是字符串。
            ValueError: source_port 为空。
        """

        if node_id not in self._nodes:
            raise UnknownNodeError(node_id)
        require_non_empty_string(source_port, "output port")
        return tuple(self._outgoing.get((node_id, source_port), ()))

    def is_endpoint(
        self,
        flow_name: str,
        node_id: str,
        port: str = "default",
    ) -> bool:
        """判断一个 Node 端口是否是指定 Flow 的终止端点。

        参数：
            flow_name: 需要判断的 Flow 名称。
            node_id: 当前 Node ID。
            port: 当前 Output 使用的端口。

        返回：
            匹配 Flow Endpoint 时返回 True，否则返回 False。

        异常：
            UnknownFlowError: Graph 中不存在 flow_name。
            TypeError: Endpoint 参数类型不合法。
            ValueError: Endpoint 参数为空。
        """

        return Endpoint(node_id=node_id, port=port) in self.flow(flow_name).endpoints

    def _ensure_mutable(self) -> None:
        """确保 Graph 仍处于可构建状态。

        异常：
            GraphFrozenError: Graph 已经冻结。
        """

        if self._frozen:
            raise GraphFrozenError(self.name)

    def _validation_issues(self) -> list[str]:
        """收集当前 Graph 的全部静态校验问题。

        返回：
            可直接展示给调用方的问题描述列表。
        """

        issues: list[str] = []

        if not self._nodes:
            issues.append("graph must contain at least one node")
        if not self._flows:
            issues.append("graph must contain at least one flow")

        for edge in self._edges:
            if edge.source not in self._nodes:
                issues.append(
                    f"edge source {edge.source!r} is not a registered node"
                )
            if edge.target not in self._nodes:
                issues.append(
                    f"edge target {edge.target!r} is not a registered node"
                )

            source = self._nodes.get(edge.source)
            target = self._nodes.get(edge.target)
            source_ports = self._ports_for(source, "output", edge.source, issues)
            target_ports = self._ports_for(target, "input", edge.target, issues)
            if source_ports is not None and edge.source_port not in source_ports:
                issues.append(
                    f"edge source port {edge.source}.{edge.source_port} "
                    "is not declared"
                )
            if target_ports is not None and edge.target_port not in target_ports:
                issues.append(
                    f"edge target port {edge.target}.{edge.target_port} "
                    "is not declared"
                )
            if (
                source_ports is not None
                and target_ports is not None
                and edge.source_port in source_ports
                and edge.target_port in target_ports
                and not is_type_compatible(
                    source_ports[edge.source_port],
                    target_ports[edge.target_port],
                )
            ):
                issues.append(
                    f"edge {edge.source}.{edge.source_port} produces "
                    f"{source_ports[edge.source_port].__name__}, but "
                    f"{edge.target}.{edge.target_port} requires "
                    f"{target_ports[edge.target_port].__name__}"
                )

        for node_id, node in self._nodes.items():
            input_ports = self._ports_for(node, "input", node_id, issues)
            self._ports_for(node, "output", node_id, issues)
            policy = getattr(node, "input_policy", None)
            if not isinstance(policy, InputPolicy):
                issues.append(
                    f"node {node_id!r} input_policy must be InputPolicy"
                )
            elif input_ports is not None:
                try:
                    policy.groups_for(input_ports)
                except ValueError as error:
                    issues.append(
                        f"node {node_id!r} has invalid input policy: {error}"
                    )

        for flow in self._flows.values():
            if flow.entrypoint not in self._nodes:
                issues.append(
                    f"flow {flow.name!r} entrypoint "
                    f"{flow.entrypoint!r} is not a registered node"
                )

            for endpoint in flow.endpoints:
                if endpoint.node_id not in self._nodes:
                    issues.append(
                        f"flow {flow.name!r} endpoint node "
                        f"{endpoint.node_id!r} is not registered"
                    )
                else:
                    output_ports = self._ports_for(
                        self._nodes[endpoint.node_id],
                        "output",
                        endpoint.node_id,
                        issues,
                    )
                    if (
                        output_ports is not None
                        and endpoint.port not in output_ports
                    ):
                        issues.append(
                            f"flow {flow.name!r} endpoint port "
                            f"{endpoint.node_id}.{endpoint.port} "
                            "is not declared"
                        )

            if flow.entrypoint in self._nodes:
                reachable = self._reachable_nodes(flow.entrypoint)
                if not any(
                    endpoint.node_id in reachable
                    for endpoint in flow.endpoints
                ):
                    issues.append(
                        f"flow {flow.name!r} cannot reach any endpoint "
                        f"from {flow.entrypoint!r}"
                    )

        return list(dict.fromkeys(issues))

    @staticmethod
    def _ports_for(
        node: Node | None,
        direction: str,
        node_id: str,
        issues: list[str],
    ) -> Ports | None:
        """读取并校验 Node 的端口声明。

        参数：
            node: 需要读取声明的 Node；未知 Node 时为 None。
            direction: 需要读取的 input 或 output 方向。
            node_id: Node 在 Graph 中的 ID。
            issues: 用于追加问题描述的校验结果列表。

        返回：
            声明合法时返回 Ports，否则返回 None。
        """

        if node is None:
            return None
        ports = getattr(node, f"{direction}_ports", None)
        if not isinstance(ports, Ports):
            issues.append(
                f"node {node_id!r} {direction}_ports must be Ports"
            )
            return None
        return ports

    def _reachable_nodes(self, entrypoint: str) -> set[str]:
        """计算从入口出发可以到达的 Node ID。

        参数：
            entrypoint: 广度优先遍历使用的起始 Node ID。

        返回：
            包含 entrypoint 自身的可达 Node ID 集合。
        """

        reachable: set[str] = set()
        pending = deque([entrypoint])
        adjacency: dict[str, list[str]] = defaultdict(list)
        for edge in self._edges:
            if edge.source in self._nodes and edge.target in self._nodes:
                adjacency[edge.source].append(edge.target)

        while pending:
            node_id = pending.popleft()
            if node_id in reachable:
                continue
            reachable.add(node_id)
            pending.extend(adjacency[node_id])

        return reachable
