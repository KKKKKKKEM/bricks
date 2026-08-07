"""流式图构建 API。"""

from __future__ import annotations

from typing import Any, Mapping, Optional

from ..errors import DuplicateNodeError, DuplicateTransitionError
from ..types import Action, EventName, Guard, NodeId
from .graph import Graph
from .nodes import ActionNode, BaseNode, SubGraphNode, TerminalNode, WaitNode
from .transitions import Transition
from .validate import assert_valid


class GraphBuilder:
    """在冻结并校验为 Graph 之前构建图。"""

    def __init__(
        self,
        graph_id: str,
        *,
        initial: NodeId,
        version: str = "1",
    ) -> None:
        self.graph_id = graph_id
        self.initial = initial
        self.version = version
        self._nodes: dict[NodeId, BaseNode] = {}
        self._transitions: list[Transition] = []
        self._transition_ids: set[str] = set()
        self._order = 0

    def add_node(self, node: BaseNode) -> "GraphBuilder":
        if not node.id:
            raise ValueError("节点 id 不能为空")
        if node.id in self._nodes:
            raise DuplicateNodeError(f"node already exists: {node.id!r}")
        self._nodes[node.id] = node
        return self

    def include(
        self, graph: Graph, *, prefix: Optional[str] = None
    ) -> str:
        """将一张已校验的 Graph 作为命名空间片段加入当前 Builder。

        节点和迁移会获得 ``<prefix>.`` 前缀，Action、Guard 和元数据保持复用。
        返回值是被包含图的入口节点 ID，调用方可以用它连接外部迁移。可复用片段的
        边界节点应使用非终止节点，由外部图决定何时离开片段。
        """
        prefix = prefix or graph.id
        if not prefix:
            raise ValueError("subgraph prefix 不能为空")

        node_ids = {
            node_id: f"{prefix}.{node_id}" for node_id in graph.nodes
        }
        if any(node_id in self._nodes for node_id in node_ids.values()):
            duplicate = next(
                node_id for node_id in node_ids.values() if node_id in self._nodes
            )
            raise DuplicateNodeError(f"node already exists: {duplicate!r}")
        transition_ids = {f"{prefix}.{transition.id}" for transition in graph.transitions}
        duplicate_transitions = transition_ids & self._transition_ids
        if duplicate_transitions:
            duplicate = next(iter(duplicate_transitions))
            raise DuplicateTransitionError(
                f"transition already exists: {duplicate!r}"
            )
        for node_id, node in graph.nodes.items():
            self.add_node(node.with_id(node_ids[node_id]))

        for transition in graph.transitions:
            target = (
                None
                if transition.target is None
                else node_ids[transition.target]
            )
            transition_id = f"{prefix}.{transition.id}"
            self._transition_ids.add(transition_id)
            self._transitions.append(
                Transition(
                    id=transition_id,
                    source=node_ids[transition.source],
                    event=transition.event,
                    target=target,
                    guard=transition.guard,
                    action=transition.action,
                    priority=transition.priority,
                    metadata=transition.metadata,
                    order=self._order,
                )
            )
            self._order += 1
        return node_ids[graph.initial]

    def node(self, node: BaseNode | str, **options: Any) -> "GraphBuilder":
        if isinstance(node, str):
            node = BaseNode(id=node, **options)
        elif options:
            raise TypeError("options cannot be used with a BaseNode instance")
        return self.add_node(node)

    def action(
        self,
        node_id: NodeId,
        action: Optional[Action] = None,
        *,
        on_exit: Optional[Action] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> "GraphBuilder":
        return self.add_node(
            ActionNode(
                id=node_id,
                action=action,
                on_exit=on_exit,
                metadata=metadata or {},
            )
        )

    def wait(
        self,
        node_id: NodeId,
        *,
        delay: Optional[float] = None,
        resume_event: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> "GraphBuilder":
        return self.add_node(
            WaitNode(
                id=node_id,
                delay=delay,
                resume_event=resume_event,
                metadata=metadata or {},
            )
        )

    def terminal(
        self,
        node_id: NodeId,
        action: Optional[Action] = None,
        *,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> "GraphBuilder":
        return self.add_node(
            TerminalNode(
                id=node_id,
                action=action,
                metadata=metadata or {},
            )
        )

    def subgraph(
        self,
        node_id: NodeId,
        graph: Graph,
        *,
        entry_event: Optional[str] = None,
        return_event: Optional[str] = None,
        data: Optional[Mapping[str, Any]] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> "GraphBuilder":
        """添加一个进入时启动独立 Graph 的子图节点。"""
        return self.add_node(
            SubGraphNode(
                id=node_id,
                graph=graph,
                entry_event=entry_event,
                return_event=return_event,
                data=data or {},
                metadata=metadata or {},
            )
        )

    def transition(
        self,
        source: NodeId,
        event: EventName,
        target: Optional[NodeId],
        *,
        transition_id: Optional[str] = None,
        guard: Optional[Guard] = None,
        action: Optional[Action] = None,
        priority: int = 0,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> "GraphBuilder":
        if not source or not event:
            raise ValueError("迁移 source 和 event 不能为空")
        transition_id = transition_id or f"{source}:{event}:{target}:{self._order}"
        if transition_id in self._transition_ids:
            raise DuplicateTransitionError(
                f"transition already exists: {transition_id!r}"
            )
        self._transition_ids.add(transition_id)
        self._transitions.append(
            Transition(
                id=transition_id,
                source=source,
                event=event,
                target=target,
                guard=guard,
                action=action,
                priority=priority,
                metadata=metadata or {},
                order=self._order,
            )
        )
        self._order += 1
        return self

    def build(self) -> Graph:
        graph = Graph(
            id=self.graph_id,
            initial=self.initial,
            nodes=self._nodes,
            transitions=tuple(self._transitions),
            version=self.version,
        )
        return assert_valid(graph)
