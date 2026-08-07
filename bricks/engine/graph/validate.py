"""独立于图构建过程的图校验。"""

from __future__ import annotations

from dataclasses import dataclass
from collections import deque
from typing import List

from ..errors import GraphValidationError
from .graph import Graph


@dataclass(frozen=True, slots=True)
class GraphIssue:
    code: str
    message: str
    subject: str | None = None


def validate_graph(graph: Graph) -> tuple[GraphIssue, ...]:
    issues: List[GraphIssue] = []
    if not graph.id:
        issues.append(GraphIssue("empty_graph_id", "graph id is required"))
    if graph.initial not in graph.nodes:
        issues.append(
            GraphIssue(
                "missing_initial",
                f"initial node {graph.initial!r} is not defined",
                graph.initial,
            )
        )
    transition_ids = set()
    for transition in graph.transitions:
        if not transition.id:
            issues.append(
                GraphIssue("empty_transition_id", "迁移 id 不能为空", transition.id)
            )
        if not transition.event:
            issues.append(
                GraphIssue("empty_event", "迁移 event 不能为空", transition.id)
            )
        if transition.id in transition_ids:
            issues.append(
                GraphIssue(
                    "duplicate_transition",
                    f"transition id {transition.id!r} is duplicated",
                    transition.id,
                )
            )
        transition_ids.add(transition.id)
        if transition.source not in graph.nodes:
            issues.append(
                GraphIssue(
                    "missing_source",
                    f"transition source {transition.source!r} is not defined",
                    transition.id,
                )
            )
        if transition.target is not None and transition.target not in graph.nodes:
            issues.append(
                GraphIssue(
                    "missing_target",
                    f"transition target {transition.target!r} is not defined",
                    transition.id,
                )
            )
    return tuple(issues)


def assert_valid(graph: Graph) -> Graph:
    issues = validate_graph(graph)
    if issues:
        details = "; ".join(issue.message for issue in issues)
        raise GraphValidationError(details)
    return graph


def reachable_nodes(graph: Graph) -> frozenset[str]:
    """返回从初始节点沿迁移可以到达的节点。"""
    if graph.initial not in graph.nodes:
        return frozenset()
    outgoing: dict[str, set[str]] = {node_id: set() for node_id in graph.nodes}
    for transition in graph.transitions:
        if transition.target is not None:
            outgoing[transition.source].add(transition.target)

    visited: set[str] = set()
    queue = deque([graph.initial])
    while queue:
        node_id = queue.popleft()
        if node_id in visited:
            continue
        visited.add(node_id)
        queue.extend(outgoing[node_id] - visited)
    return frozenset(visited)


def unreachable_nodes(graph: Graph) -> frozenset[str]:
    """返回没有从初始节点到达路径的节点，供静态检查使用。"""
    return frozenset(graph.nodes) - reachable_nodes(graph)


def terminal_nodes(graph: Graph) -> frozenset[str]:
    """返回声明为终止节点的节点 ID。"""
    return frozenset(node_id for node_id, node in graph.nodes.items() if node.terminal)


def dead_end_nodes(graph: Graph) -> frozenset[str]:
    """返回可达但既非终止、又没有任何迁移的节点。

    死端不一定是错误：它可以代表等待外部扩展的边界，但应在发布流程前显式确认。
    """
    reachable = reachable_nodes(graph)
    outgoing = {transition.source for transition in graph.transitions}
    return frozenset(
        node_id
        for node_id in reachable
        if not graph.node(node_id).terminal and node_id not in outgoing
    )


def cycle_nodes(graph: Graph) -> frozenset[str]:
    """返回参与有向环的节点；结果包含不可达节点，便于发布前检查。"""
    outgoing: dict[str, set[str]] = {node_id: set() for node_id in graph.nodes}
    for transition in graph.transitions:
        if transition.source not in graph.nodes:
            continue
        target = transition.source if transition.target is None else transition.target
        if target in graph.nodes:
            outgoing[transition.source].add(target)

    # 用 Tarjan 算法找强连通分量；一个自环也算一个环。
    index = 0
    indices: dict[str, int] = {}
    lowlinks: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    cyclic: set[str] = set()

    def visit(node_id: str) -> None:
        nonlocal index
        indices[node_id] = index
        lowlinks[node_id] = index
        index += 1
        stack.append(node_id)
        on_stack.add(node_id)

        for target in outgoing[node_id]:
            if target not in indices:
                visit(target)
                lowlinks[node_id] = min(lowlinks[node_id], lowlinks[target])
            elif target in on_stack:
                lowlinks[node_id] = min(lowlinks[node_id], indices[target])

        if lowlinks[node_id] != indices[node_id]:
            return

        component: list[str] = []
        while True:
            member = stack.pop()
            on_stack.remove(member)
            component.append(member)
            if member == node_id:
                break
        if len(component) > 1 or node_id in outgoing[node_id]:
            cyclic.update(component)

    for node_id in graph.nodes:
        if node_id not in indices:
            visit(node_id)
    return frozenset(cyclic)


def non_terminating_nodes(graph: Graph) -> frozenset[str]:
    """返回可达且没有路径到任何终止节点的节点。"""
    reachable = reachable_nodes(graph)
    terminal = terminal_nodes(graph) & reachable
    reverse: dict[str, set[str]] = {node_id: set() for node_id in graph.nodes}
    for transition in graph.transitions:
        if (
            transition.target in graph.nodes
            and transition.source in reachable
            and transition.target in reachable
        ):
            reverse[transition.target].add(transition.source)

    can_terminate = set(terminal)
    queue = deque(terminal)
    while queue:
        node_id = queue.popleft()
        for source in reverse[node_id] - can_terminate:
            can_terminate.add(source)
            queue.append(source)
    return frozenset(reachable - can_terminate)


def transition_conflicts(graph: Graph) -> tuple[tuple[str, ...], ...]:
    """返回同一来源、事件和优先级下的迁移组，供人工检查分支冲突。"""
    groups: dict[tuple[str, str, int], list[str]] = {}
    for transition in graph.transitions:
        key = (transition.source, transition.event, transition.priority)
        groups.setdefault(key, []).append(transition.id)
    return tuple(tuple(ids) for ids in groups.values() if len(ids) > 1)
