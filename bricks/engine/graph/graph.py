"""不可变图定义。"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Callable, Iterable, Mapping

from ..errors import GraphValidationError
from ..types import EventName, NodeId, thaw_value
from .nodes import BaseNode, SubGraphNode, WaitNode
from .transitions import Transition


GRAPH_DESCRIPTION_VERSION = 1


@dataclass(frozen=True, slots=True)
class Graph:
    """经过校验、可复用的图定义。

    Builder 是可变的，生成的 Graph 不可变，可以安全地被多个独立 Machine 运行实例共享。
    """

    id: str
    initial: NodeId
    nodes: Mapping[NodeId, BaseNode]
    transitions: tuple[Transition, ...]
    version: str = "1"
    _transition_index: Mapping[
        tuple[NodeId, EventName], tuple[Transition, ...]
    ] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("graph id cannot be empty")
        if not self.version:
            raise ValueError("graph version cannot be empty")
        object.__setattr__(self, "version", str(self.version))
        object.__setattr__(self, "nodes", MappingProxyType(dict(self.nodes)))
        object.__setattr__(self, "transitions", tuple(self.transitions))
        self._validate_invariants()
        index: dict[tuple[NodeId, EventName], list[Transition]] = {}
        for transition in self.transitions:
            index.setdefault((transition.source, transition.event), []).append(
                transition
            )
        object.__setattr__(
            self,
            "_transition_index",
            MappingProxyType(
                {
                    key: tuple(
                        sorted(items, key=lambda item: (item.priority, item.order))
                    )
                    for key, items in index.items()
                }
            ),
        )

    def _validate_invariants(self) -> None:
        if self.initial not in self.nodes:
            raise GraphValidationError(
                f"initial node {self.initial!r} is not defined"
            )
        for node_id, node in self.nodes.items():
            if not isinstance(node, BaseNode):
                raise GraphValidationError(f"node {node_id!r} must be a BaseNode")
            if node.id != node_id:
                raise GraphValidationError(
                    f"node mapping key {node_id!r} does not match node id {node.id!r}"
                )
        transition_ids: set[str] = set()
        for transition in self.transitions:
            if not isinstance(transition, Transition):
                raise GraphValidationError(
                    "graph transitions must be Transition instances"
                )
            if not transition.id or transition.id in transition_ids:
                raise GraphValidationError(
                    f"transition id {transition.id!r} is empty or duplicated"
                )
            transition_ids.add(transition.id)
            if not transition.event:
                raise GraphValidationError(
                    f"transition {transition.id!r} has an empty event"
                )
            if transition.source not in self.nodes:
                raise GraphValidationError(
                    f"transition source {transition.source!r} is not defined"
                )
            if transition.target is not None and transition.target not in self.nodes:
                raise GraphValidationError(
                    f"transition target {transition.target!r} is not defined"
                )

    def node(self, node_id: NodeId) -> BaseNode:
        try:
            return self.nodes[node_id]
        except KeyError as exc:
            raise GraphValidationError(f"unknown node: {node_id!r}") from exc

    def transitions_from(
        self, source: NodeId, event: EventName
    ) -> Iterable[Transition]:
        return self._transition_index.get((source, event), ())

    @property
    def event_names(self) -> frozenset[EventName]:
        """返回图声明过的事件名，供响应式运行时建立订阅。"""
        return frozenset(transition.event for transition in self.transitions)

    def describe(self) -> dict[str, Any]:
        """返回不包含可执行对象的结构描述，供检查器和可视化工具使用。"""
        return {
            "schema": "bricks.graph.description",
            "schema_version": GRAPH_DESCRIPTION_VERSION,
            "id": self.id,
            "version": self.version,
            "graph_version": self.version,
            "initial": self.initial,
            "nodes": [_describe_node(node) for node in self.nodes.values()],
            "transitions": [
                {
                    "id": transition.id,
                    "source": transition.source,
                    "event": transition.event,
                    "target": transition.target,
                    "priority": transition.priority,
                    "has_guard": transition.guard is not None,
                    "has_action": transition.action is not None,
                    "metadata": thaw_value(transition.metadata),
                }
                for transition in self.transitions
            ],
        }

    def to_dict(
        self,
        *,
        action_serializer: Callable[[Any], Any] | None = None,
        guard_serializer: Callable[[Any], Any] | None = None,
        node_serializer: Callable[[BaseNode], Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """输出可恢复的结构定义；可执行对象必须由调用方显式编码。"""
        from .serialization import graph_to_dict

        return graph_to_dict(
            self,
            action_serializer=action_serializer,
            guard_serializer=guard_serializer,
            node_serializer=node_serializer,
        )

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
        *,
        action_resolver: Callable[[Any], Any] | None = None,
        guard_resolver: Callable[[Any], Any] | None = None,
        node_resolver: Callable[[Mapping[str, Any]], BaseNode] | None = None,
        graph_resolver: Callable[[str], "Graph"] | None = None,
    ) -> "Graph":
        """从结构定义恢复 Graph，并校验节点、迁移和版本。"""
        from .serialization import graph_from_dict

        return graph_from_dict(
            value,
            action_resolver=action_resolver,
            guard_resolver=guard_resolver,
            node_resolver=node_resolver,
            graph_resolver=graph_resolver,
        )

    def to_mermaid(self) -> str:
        """生成不依赖第三方库的 Mermaid 流程图文本。"""
        references = {
            node_id: f"node_{index}"
            for index, node_id in enumerate(self.nodes)
        }
        lines = ["graph TD"]
        for node_id, node in self.nodes.items():
            label = str(node_id).replace('"', "'")
            reference = references[node_id]
            if node.terminal:
                lines.append(f'    {reference}(({label}))')
            else:
                lines.append(f'    {reference}[{label}]')
        lines.append(f"    start((start)) --> {references[self.initial]}")
        for transition in self.transitions:
            target = transition.target or transition.source
            event = str(transition.event).replace('"', "'")
            lines.append(
                f'    {references[transition.source]} -->|"{event}"| '
                f"{references[target]}"
            )
        return "\n".join(lines)

    def to_dot(self) -> str:
        """生成不依赖 Graphviz 库的 DOT 图文本。"""
        lines = [f'digraph "{_dot_escape(self.id)}" {{', "    rankdir=LR;"]
        for node_id, node in self.nodes.items():
            shape = "doublecircle" if node.terminal else "box"
            lines.append(
                f'    "{_dot_escape(node_id)}" '
                f'[label="{_dot_escape(node_id)}", shape={shape}];'
            )
        for transition in self.transitions:
            target = transition.target or transition.source
            label = str(transition.event)
            if transition.guard is not None:
                label += " [guard]"
            lines.append(
                f'    "{_dot_escape(transition.source)}" -> '
                f'"{_dot_escape(target)}" '
                f'[label="{_dot_escape(label)}"];'
            )
        lines.append("}")
        return "\n".join(lines)


def _describe_node(node: BaseNode) -> dict[str, Any]:
    """将内置节点的非可执行配置放入结构描述。"""
    config: dict[str, Any] = {}
    if isinstance(node, WaitNode):
        config.update(delay=node.delay, resume_event=node.resume_event)
    elif isinstance(node, SubGraphNode):
        config.update(
            graph_id=node.graph.id,
            entry_event=node.entry_event,
            return_event=node.return_event,
            data=thaw_value(node.data),
        )
    return {
        "id": node.id,
        "kind": node.kind,
        "terminal": node.terminal,
        "has_action": node.action is not None,
        "has_on_exit": node.on_exit is not None,
        "metadata": thaw_value(node.metadata),
        "config": config,
    }


def _dot_escape(value: str) -> str:
    """转义 DOT 字符串中的反斜杠、引号和换行。"""
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )
