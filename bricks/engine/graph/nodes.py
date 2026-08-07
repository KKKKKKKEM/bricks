"""图节点定义。

BaseNode 只保存所有节点共有的声明数据，具体节点通过继承扩展自己的语义。
Action 的执行、重试和资源管理由运行时负责。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any, ClassVar, Optional

from ..types import Action, FrozenDict, NodeId, freeze_value


@dataclass(frozen=True)
class BaseNode:
    """所有节点的最小基类。"""

    id: NodeId
    action: Optional[Action] = field(default=None, kw_only=True)
    on_exit: Optional[Action] = field(default=None, kw_only=True)
    terminal: bool = field(default=False, kw_only=True)
    metadata: Mapping[str, Any] = field(default_factory=dict, kw_only=True)

    kind: ClassVar[str] = "node"

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", FrozenDict(self.metadata))

    def with_id(self, node_id: NodeId) -> "BaseNode":
        """Clone this declaration with a new ID and rerun subclass invariants."""
        return replace(self, id=node_id)

    def enter(self, context: Any, event: Any, executor: Any) -> Any:
        """执行节点进入行为；自定义节点可以覆盖这个方法。"""
        if self.action is None:
            return None
        return executor.execute(self.action, context, event)

    async def enter_async(self, context: Any, event: Any, executor: Any) -> Any:
        """异步执行节点进入行为。"""
        if self.action is None:
            return None
        return await executor.execute_async(self.action, context, event)

    def exit(self, context: Any, event: Any, executor: Any) -> Any:
        """执行节点退出行为；自定义节点可以覆盖这个方法。"""
        if self.on_exit is None:
            return None
        return executor.execute(self.on_exit, context, event)

    async def exit_async(self, context: Any, event: Any, executor: Any) -> Any:
        """异步执行节点退出行为。"""
        if self.on_exit is None:
            return None
        return await executor.execute_async(self.on_exit, context, event)


@dataclass(frozen=True)
class ActionNode(BaseNode):
    """进入时执行 Action 的节点。"""

    kind: ClassVar[str] = "action"


@dataclass(frozen=True)
class WaitNode(BaseNode):
    """进入后等待事件或计时器的节点。"""

    delay: Optional[float] = None
    resume_event: Optional[str] = None
    kind: ClassVar[str] = "wait"

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.delay is not None and self.delay < 0:
            raise ValueError("WaitNode.delay cannot be negative")

    def enter(self, context: Any, event: Any, executor: Any) -> Any:
        outcome = super().enter(context, event, executor)
        if outcome is not None:
            return outcome
        from ..runtime.outcomes import Wait

        return Wait(self.delay, self.resume_event)

    async def enter_async(self, context: Any, event: Any, executor: Any) -> Any:
        outcome = await super().enter_async(context, event, executor)
        if outcome is not None:
            return outcome
        from ..runtime.outcomes import Wait

        return Wait(self.delay, self.resume_event)


@dataclass(frozen=True)
class TerminalNode(BaseNode):
    """进入后结束运行的节点。"""

    terminal: bool = True
    kind: ClassVar[str] = "terminal"


@dataclass(frozen=True)
class SubGraphNode(BaseNode):
    """进入时启动独立 Graph 的节点；运行时通过 Fork/Join 返回父图。"""

    graph: Any = None
    entry_event: Optional[str] = None
    return_event: Optional[str] = None
    data: Mapping[str, Any] = field(default_factory=dict)
    terminal: bool = True
    kind: ClassVar[str] = "subgraph"

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.graph is None:
            raise ValueError("SubGraphNode.graph 不能为空")
        object.__setattr__(
            self,
            "data",
            freeze_value(self.data),
        )

    def enter(self, context: Any, event: Any, executor: Any) -> Any:
        outcome = super().enter(context, event, executor)
        if outcome is not None:
            return outcome
        from ..runtime.outcomes import Outcome

        return Outcome.subgraph(
            self.graph,
            entry_event=self.entry_event,
            return_event=self.return_event,
            data=self.data,
        )

    async def enter_async(self, context: Any, event: Any, executor: Any) -> Any:
        outcome = await super().enter_async(context, event, executor)
        if outcome is not None:
            return outcome
        from ..runtime.outcomes import Outcome

        return Outcome.subgraph(
            self.graph,
            entry_event=self.entry_event,
            return_event=self.return_event,
            data=self.data,
        )
