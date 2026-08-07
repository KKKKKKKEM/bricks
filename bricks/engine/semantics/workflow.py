"""建立在 Graph/Machine 之上的工作流语义。"""

from __future__ import annotations

from collections import deque
from typing import Any, Iterable, Mapping, Optional

from ..errors import GraphValidationError
from ..events.messages import Event
from ..graph.graph import Graph
from ..runtime.context import Context
from ..runtime.machine import Machine


class Workflow:
    """以工作流视角运行一张图的轻量外观。

    Workflow 不复制 Graph，也不实现另一套状态机；它只负责创建运行实例、
    初始化业务数据，以及提供 DAG 校验和批量执行入口。
    """

    def __init__(self, graph: Graph) -> None:
        self.graph = graph

    def machine(
        self,
        *,
        context: Optional[Context] = None,
        data: Optional[Mapping[str, Any]] = None,
        **options: Any,
    ) -> Machine:
        """创建一个尚未启动的独立运行实例。"""
        if context is not None and data is not None:
            raise ValueError("context 和 data 不能同时提供")
        if context is None:
            context = Context(
                graph_id=self.graph.id,
                graph_version=self.graph.version,
                data=dict(data or {}),
            )
        return Machine(self.graph, context=context, **options)

    def start(
        self,
        *,
        context: Optional[Context] = None,
        data: Optional[Mapping[str, Any]] = None,
        **options: Any,
    ) -> Machine:
        """创建并启动一个工作流运行实例。"""
        machine = self.machine(context=context, data=data, **options)
        machine.start()
        return machine

    async def start_async(
        self,
        *,
        context: Optional[Context] = None,
        data: Optional[Mapping[str, Any]] = None,
        **options: Any,
    ) -> Machine:
        """异步创建并启动一个工作流运行实例。"""
        machine = self.machine(context=context, data=data, **options)
        await machine.start_async()
        return machine

    def run(
        self,
        events: Iterable[Event | str | tuple[str, Any]] = (),
        *,
        context: Optional[Context] = None,
        data: Optional[Mapping[str, Any]] = None,
        **options: Any,
    ) -> Machine:
        """启动工作流并按顺序消费一组事件。"""
        machine = self.machine(context=context, data=data, **options)
        machine.invoke(events)
        return machine

    async def run_async(
        self,
        events: Iterable[Event | str | tuple[str, Any]] = (),
        *,
        context: Optional[Context] = None,
        data: Optional[Mapping[str, Any]] = None,
        **options: Any,
    ) -> Machine:
        """异步启动工作流并按顺序消费一组事件。"""
        machine = self.machine(context=context, data=data, **options)
        await machine.ainvoke(events)
        return machine

    def topological_order(self) -> tuple[str, ...]:
        """返回 DAG 拓扑序；存在环时抛出 GraphValidationError。"""
        indegree = {node_id: 0 for node_id in self.graph.nodes}
        outgoing: dict[str, list[str]] = {node_id: [] for node_id in self.graph.nodes}
        for transition in self.graph.transitions:
            target = transition.source if transition.target is None else transition.target
            if target in outgoing[transition.source]:
                continue
            outgoing[transition.source].append(target)
            indegree[target] += 1

        queue = deque(node_id for node_id, degree in indegree.items() if degree == 0)
        order = []
        while queue:
            node_id = queue.popleft()
            order.append(node_id)
            for target in outgoing[node_id]:
                indegree[target] -= 1
                if indegree[target] == 0:
                    queue.append(target)
        if len(order) != len(indegree):
            raise GraphValidationError("workflow graph contains a cycle")
        return tuple(order)
