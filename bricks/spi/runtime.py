"""Runtime 的可替换能力协议和跨后端任务模型。"""

from __future__ import annotations

import enum
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable
from uuid import uuid4

from ..engine.core import Output, require_non_empty_string
from ..engine.events import Event
from ..engine.execution import Execution, ExecutionLimits
from ..engine.graph import ExecutionPlan, Graph
from ..engine.hooks import HookHandle, HookPhase, NodeHook
from ..engine.slots import Slot, SlotLease, SlotPool

EventHandler = Callable[[Event], None]


@dataclass(frozen=True, slots=True)
class Work:
    """TaskBackend 搬运的可序列化执行请求。"""

    graph: str
    inputs: Any = None
    trigger: Event | None = field(default=None, compare=False, repr=False)
    id: str = field(default_factory=lambda: str(uuid4()), kw_only=True)
    limits: ExecutionLimits = field(default_factory=ExecutionLimits, kw_only=True)

    def __post_init__(self) -> None:
        require_non_empty_string(self.graph, "work graph")
        require_non_empty_string(self.id, "work id")
        if not isinstance(self.limits, ExecutionLimits):
            raise TypeError("work limits must be ExecutionLimits")


@dataclass(frozen=True, slots=True)
class Delivery:
    """一次本地可确认投递；attempt 从 1 开始并随重投递递增。"""

    work: Work
    attempt: int = 1
    slot_lease: SlotLease | None = field(
        default=None,
        compare=False,
        repr=False,
        kw_only=True,
    )

    def __post_init__(self) -> None:
        if not isinstance(self.work, Work):
            raise TypeError("delivery work must be Work")
        if type(self.attempt) is not int or self.attempt < 1:
            raise ValueError("delivery attempt must be an integer greater than zero")
        if self.slot_lease is not None and not isinstance(self.slot_lease, SlotLease):
            raise TypeError("delivery slot_lease must implement SlotLease or be None")


class DeliveryOutcome(str, enum.Enum):
    ACK = "ack"
    RETRY = "retry"
    REJECT = "reject"


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    """由 handler 返回给 backend 的唯一交付决定。"""

    outcome: DeliveryOutcome
    error: BaseException | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.outcome, DeliveryOutcome):
            raise TypeError("delivery outcome must be DeliveryOutcome")

    @classmethod
    def ack(cls) -> DeliveryResult:
        return cls(DeliveryOutcome.ACK)

    @classmethod
    def retry(cls, error: BaseException | None = None) -> DeliveryResult:
        return cls(DeliveryOutcome.RETRY, error)

    @classmethod
    def reject(cls, error: BaseException | None = None) -> DeliveryResult:
        return cls(DeliveryOutcome.REJECT, error)


class EventBus(Protocol):
    """发布和订阅 Event 的替换协议。"""

    @property
    def idle(self) -> bool:
        """返回当前是否没有尚未投递完成的 Event。"""

    def subscribe(
        self,
        event_type: str,
        handler: EventHandler,
        *,
        subscription: str | None = None,
    ) -> None:
        """订阅 Event；同名 subscription 的实例竞争消费。"""

    def publish(self, event: Event) -> None:
        """发布一个 Event。"""

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待已接受 Event 投递完成。"""

    def close(self) -> None:
        """关闭传输。"""


WorkHandler = Callable[[Delivery], DeliveryResult]


class TaskPublisher(Protocol):
    """向命名任务通道投递 Work。"""

    def submit(self, queue: str, work: Work) -> None:
        """向命名通道提交 Work；正常返回表示后端已接受。"""

    def close(self) -> None:
        """关闭发布端持有的资源。"""


class LocalTaskPublisher(TaskPublisher, Protocol):
    """可在当前进程延续 Slot 链的可选任务发布能力。"""

    def submit_local(self, queue: str, work: Work, lease: SlotLease) -> None:
        """正常返回时接管一个 lease 引用；抛错时引用仍由调用方持有。"""


class TaskConsumer(Protocol):
    """从命名任务通道消费 Work。"""

    @property
    def idle(self) -> bool:
        """返回当前实例是否没有尚未完成的 Work。"""

    def bind(
        self,
        queue: str,
        handler: WorkHandler,
        *,
        concurrency: int,
        slots: SlotPool | None = None,
    ) -> None:
        """绑定通道，并为不携带 Slot 的根 Work 分配执行槽。"""

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待当前实例已接受的 Work 完成。"""

    def close(self) -> None:
        """关闭消费端持有的资源。"""


class TaskBackend(TaskPublisher, TaskConsumer, Protocol):
    """兼具任务发布和消费能力的便捷组合协议。"""


Emit = Callable[[Event], None]


class GraphExecutor(Protocol):
    """执行单张 Graph 的替换协议。"""

    def execute(
        self,
        name: str,
        graph: Graph,
        inputs: Any,
        emit: Emit,
        plan: ExecutionPlan | None = None,
        *,
        slot: Slot | None = None,
        execution: Execution | None = None,
    ) -> tuple[Output, ...]:
        """执行 Graph 并返回终端 Output。"""

    def close(self) -> None:
        """关闭执行器持有的资源。"""


@runtime_checkable
class HookableGraphExecutor(GraphExecutor, Protocol):
    """额外支持动态 Node Hook 的 GraphExecutor 可选能力。"""

    def attach(
        self,
        hook: NodeHook | Callable[..., object],
        *,
        phase: HookPhase | str | None = None,
        graph: str | None = None,
        node: str | None = None,
    ) -> HookHandle:
        """挂载 Hook，并返回可用于卸载的句柄。"""
