"""Graph 定义的结构化编码协议。

Graph 不负责猜测 Python 可调用对象的导入路径。Action、Guard、自定义节点和
子图都通过调用方提供的显式编码器或解析器接入，这样同一份图定义可以服务于
不同的进程、版本和部署环境。
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from ..errors import GraphSerializationError
from ..types import thaw_value
from .graph import Graph
from .nodes import (
    ActionNode,
    BaseNode,
    SubGraphNode,
    TerminalNode,
    WaitNode,
)
from .transitions import Transition
from .validate import assert_valid

GRAPH_SCHEMA_VERSION = 1

ReferenceEncoder = Callable[[Any], Any]
ReferenceResolver = Callable[[Any], Any]
NodeEncoder = Callable[[BaseNode], Mapping[str, Any]]
NodeResolver = Callable[[Mapping[str, Any]], BaseNode]
GraphResolver = Callable[[str], Graph]


def graph_to_dict(
    graph: Graph,
    *,
    action_serializer: ReferenceEncoder | None = None,
    guard_serializer: ReferenceEncoder | None = None,
    node_serializer: NodeEncoder | None = None,
) -> dict[str, Any]:
    """将 Graph 编码为不包含可执行对象的普通字典。

    编码器返回的值会原样写入结果，通常应该是字符串注册名或其它 JSON 兼容的
    引用。没有编码器时，包含 Action、Guard 或未知节点类型的图会明确失败。
    """

    return {
        "version": GRAPH_SCHEMA_VERSION,
        "id": graph.id,
        "graph_version": graph.version,
        "initial": graph.initial,
        "nodes": [
            _node_to_dict(node, action_serializer, node_serializer)
            for node in graph.nodes.values()
        ],
        "transitions": [
            {
                "id": transition.id,
                "source": transition.source,
                "event": transition.event,
                "target": transition.target,
                "guard": _encode_reference(
                    transition.guard,
                    guard_serializer,
                    "Guard",
                ),
                "action": _encode_reference(
                    transition.action,
                    action_serializer,
                    "Action",
                ),
                "priority": transition.priority,
                "metadata": thaw_value(transition.metadata),
                "order": transition.order,
            }
            for transition in graph.transitions
        ],
    }


def graph_from_dict(
    value: Mapping[str, Any],
    *,
    action_resolver: ReferenceResolver | None = None,
    guard_resolver: ReferenceResolver | None = None,
    node_resolver: NodeResolver | None = None,
    graph_resolver: GraphResolver | None = None,
) -> Graph:
    """从结构化字典恢复 Graph，并重新执行完整图校验。"""

    if not isinstance(value, Mapping):
        raise GraphSerializationError("graph definition must be a mapping")
    version = value.get("version")
    if version != GRAPH_SCHEMA_VERSION:
        raise GraphSerializationError(
            f"unsupported graph schema version: {version!r}; "
            f"expected {GRAPH_SCHEMA_VERSION}"
        )

    try:
        graph_id = value["id"]
        initial = value["initial"]
        node_values = value["nodes"]
        transition_values = value["transitions"]
    except (KeyError, TypeError) as exc:
        raise GraphSerializationError("graph definition is missing required fields") from exc

    if not isinstance(node_values, list) or not isinstance(transition_values, list):
        raise GraphSerializationError("nodes and transitions must be lists")

    try:
        nodes = {}
        for item in node_values:
            node = _node_from_dict(
                item,
                action_resolver=action_resolver,
                node_resolver=node_resolver,
                graph_resolver=graph_resolver,
            )
            if node.id in nodes:
                raise GraphSerializationError(
                    f"duplicate node id: {node.id!r}"
                )
            nodes[node.id] = node
        transitions = tuple(
            _transition_from_dict(
                item,
                action_resolver=action_resolver,
                guard_resolver=guard_resolver,
            )
            for item in transition_values
        )
    except GraphSerializationError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise GraphSerializationError("invalid graph definition") from exc

    return assert_valid(
        Graph(
            id=graph_id,
            initial=initial,
            nodes=nodes,
            transitions=transitions,
            version=str(value.get("graph_version", "1")),
        )
    )


def _node_to_dict(
    node: BaseNode,
    action_serializer: ReferenceEncoder | None,
    node_serializer: NodeEncoder | None,
) -> dict[str, Any]:
    # BaseNode 本身是已知类型；只有未知 kind 的继承类需要自定义编码器。
    known = type(node) in {BaseNode, ActionNode, WaitNode, TerminalNode, SubGraphNode}
    if not known:
        if node_serializer is None:
            raise GraphSerializationError(
                f"node {node.id!r} has custom type {node.kind!r}; "
                "provide node_serializer"
            )
        try:
            config = node_serializer(node)
        except Exception as exc:
            raise GraphSerializationError(
                f"failed to serialize custom node {node.id!r}"
            ) from exc
        if not isinstance(config, Mapping):
            raise GraphSerializationError("node_serializer must return a mapping")
        return {
            "id": node.id,
            "kind": node.kind,
            "custom": True,
            "config": dict(config),
        }

    result: dict[str, Any] = {
        "id": node.id,
        "kind": node.kind,
        "terminal": node.terminal,
        "metadata": thaw_value(node.metadata),
        "action": _encode_reference(node.action, action_serializer, "Action"),
        "on_exit": _encode_reference(
            node.on_exit,
            action_serializer,
            "Action",
        ),
    }
    if isinstance(node, WaitNode):
        result.update({"delay": node.delay, "resume_event": node.resume_event})
    elif isinstance(node, SubGraphNode):
        result.update(
            {
                "graph_id": node.graph.id,
                "entry_event": node.entry_event,
                "return_event": node.return_event,
                "data": thaw_value(node.data),
            }
        )
    return result


def _node_from_dict(
    value: Mapping[str, Any],
    *,
    action_resolver: ReferenceResolver | None,
    node_resolver: NodeResolver | None,
    graph_resolver: GraphResolver | None,
) -> BaseNode:
    if not isinstance(value, Mapping):
        raise GraphSerializationError("node definition must be a mapping")
    node_id = value.get("id")
    kind = value.get("kind")
    if not node_id or not kind:
        raise GraphSerializationError("node id and kind are required")

    if value.get("custom"):
        if node_resolver is None:
            raise GraphSerializationError(
                f"node {node_id!r} is custom; provide node_resolver"
            )
        try:
            node = node_resolver(value)
        except Exception as exc:
            raise GraphSerializationError(
                f"failed to resolve custom node {node_id!r}"
            ) from exc
        if not isinstance(node, BaseNode) or node.id != node_id:
            raise GraphSerializationError(
                f"node_resolver must return BaseNode {node_id!r}"
            )
        return node

    action = _resolve_reference(value.get("action"), action_resolver, "Action")
    on_exit = _resolve_reference(value.get("on_exit"), action_resolver, "Action")
    common = {
        "id": node_id,
        "action": action,
        "on_exit": on_exit,
        "terminal": bool(value.get("terminal", False)),
        "metadata": dict(value.get("metadata") or {}),
    }
    if kind == "node":
        return BaseNode(**common)
    if kind == "action":
        return ActionNode(**common)
    if kind == "terminal":
        return TerminalNode(
            id=node_id,
            action=action,
            on_exit=on_exit,
            metadata=common["metadata"],
        )
    if kind == "wait":
        return WaitNode(
            **common,
            delay=value.get("delay"),
            resume_event=value.get("resume_event"),
        )
    if kind == "subgraph":
        graph_id = value.get("graph_id")
        if not graph_id:
            raise GraphSerializationError(
                f"subgraph node {node_id!r} is missing graph_id"
            )
        if graph_resolver is None:
            raise GraphSerializationError(
                f"subgraph node {node_id!r} requires graph_resolver"
            )
        try:
            graph = graph_resolver(graph_id)
        except Exception as exc:
            raise GraphSerializationError(
                f"failed to resolve subgraph {graph_id!r}"
            ) from exc
        return SubGraphNode(
            **common,
            graph=graph,
            entry_event=value.get("entry_event"),
            return_event=value.get("return_event"),
            data=dict(value.get("data") or {}),
        )
    raise GraphSerializationError(
        f"unknown node kind {kind!r}; mark it custom and provide node_resolver"
    )


def _transition_from_dict(
    value: Mapping[str, Any],
    *,
    action_resolver: ReferenceResolver | None,
    guard_resolver: ReferenceResolver | None,
) -> Transition:
    if not isinstance(value, Mapping):
        raise GraphSerializationError("transition definition must be a mapping")
    try:
        return Transition(
            id=value["id"],
            source=value["source"],
            event=value["event"],
            target=value.get("target"),
            guard=_resolve_reference(value.get("guard"), guard_resolver, "Guard"),
            action=_resolve_reference(value.get("action"), action_resolver, "Action"),
            priority=int(value.get("priority", 0)),
            metadata=dict(value.get("metadata") or {}),
            order=int(value.get("order", 0)),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise GraphSerializationError("invalid transition definition") from exc


def _encode_reference(
    value: Any,
    serializer: ReferenceEncoder | None,
    label: str,
) -> Any:
    if value is None:
        return None
    if serializer is None:
        raise GraphSerializationError(
            f"{label} is executable; provide {label.lower()}_serializer"
        )
    try:
        reference = serializer(value)
    except Exception as exc:
        raise GraphSerializationError(f"failed to serialize {label}") from exc
    if reference is None:
        raise GraphSerializationError(f"{label} serializer returned None")
    return reference


def _resolve_reference(
    reference: Any,
    resolver: ReferenceResolver | None,
    label: str,
) -> Any:
    if reference is None:
        return None
    if resolver is None:
        raise GraphSerializationError(
            f"{label} reference requires {label.lower()}_resolver"
        )
    try:
        return resolver(reference)
    except Exception as exc:
        raise GraphSerializationError(f"failed to resolve {label} reference") from exc
