"""Runtime 的可替换能力协议和跨后端任务模型。"""

from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable
from uuid import uuid4

from ..engine.core import require_non_empty_string
from ..engine.events import Event
from ..engine.execution import Execution, ExecutionLimits
from ..engine.graph import ExecutionPlan, Graph
from ..engine.hooks import HookHandle, HookPhase, NodeHook
from ..engine.slots import Slot, SlotLease, SlotProvider

EventHandler = Callable[[Event], None]


@dataclass(frozen=True, slots=True)
class Work:
    """TaskBackend 搬运的可序列化执行请求。

    Attributes:
        graph: 关联的 Graph 定义或注册名称。
        inputs: 节点调用或工作请求的输入数据。
        trigger: 触发当前 Work 的领域事件。
        id: 当前对象的唯一标识。
        limits: 单次执行的步数与超时限制。
    """

    graph: str
    inputs: Any = None
    trigger: Event | None = field(default=None, compare=False, repr=False)
    id: str = field(default_factory=lambda: str(uuid4()), kw_only=True)
    limits: ExecutionLimits = field(default_factory=ExecutionLimits, kw_only=True)

    def __post_init__(self) -> None:
        """校验构造字段并固定需要保持不变的数据。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        require_non_empty_string(self.graph, "work graph")
        require_non_empty_string(self.id, "work id")
        if not isinstance(self.limits, ExecutionLimits):
            raise TypeError("work limits must be ExecutionLimits")


@dataclass(frozen=True, slots=True)
class Delivery:
    """一次本地可确认投递；attempt 从 1 开始并随重投递递增。

    Attributes:
        work: 本次投递携带的工作请求。
        attempt: 当前投递次数，从 1 开始。
        slot_lease: 当前进程内随投递传递的 Slot 引用，不跨进程序列化。
    """

    work: Work
    attempt: int = 1
    slot_lease: SlotLease | None = field(
        default=None,
        compare=False,
        repr=False,
        kw_only=True,
    )

    def __post_init__(self) -> None:
        """校验构造字段并固定需要保持不变的数据。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
            ValueError: 参数值或字段组合不合法。
        """

        if not isinstance(self.work, Work):
            raise TypeError("delivery work must be Work")
        if type(self.attempt) is not int or self.attempt < 1:
            raise ValueError("delivery attempt must be an integer greater than zero")
        if self.slot_lease is not None and not isinstance(self.slot_lease, SlotLease):
            raise TypeError("delivery slot_lease must implement SlotLease or be None")


class DeliveryOutcome(str, enum.Enum):
    """后端处理单次交付结果的固定决定。

    Attributes:
        ACK: 接受本次交付，不要求重投递。
        RETRY: 要求任务后端重投递。
        REJECT: 拒绝本次交付。
    """

    ACK = "ack"
    RETRY = "retry"
    REJECT = "reject"


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    """由 handler 返回给 backend 的唯一交付决定。

    Attributes:
        outcome: 后端应执行的接受、重试或拒绝决定。
        error: 当前结果携带的原始异常，正常结果为 None。
    """

    outcome: DeliveryOutcome
    error: BaseException | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        """校验构造字段并固定需要保持不变的数据。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if not isinstance(self.outcome, DeliveryOutcome):
            raise TypeError("delivery outcome must be DeliveryOutcome")

    @classmethod
    def ack(cls) -> DeliveryResult:
        """创建接受本次投递的交付决定。

        Returns:
            包含交付决定和可选异常的 DeliveryResult。
        """

        return cls(DeliveryOutcome.ACK)

    @classmethod
    def retry(cls, error: BaseException | None = None) -> DeliveryResult:
        """创建请求后端重新投递的交付决定。

        Args:
            error: 需要传播、记录或用于恢复的异常。

        Returns:
            包含交付决定和可选异常的 DeliveryResult。
        """

        return cls(DeliveryOutcome.RETRY, error)

    @classmethod
    def reject(cls, error: BaseException | None = None) -> DeliveryResult:
        """创建拒绝本次投递的交付决定。

        Args:
            error: 需要传播、记录或用于恢复的异常。

        Returns:
            包含交付决定和可选异常的 DeliveryResult。
        """

        return cls(DeliveryOutcome.REJECT, error)


class EventBus(Protocol):
    """发布和订阅 Event 的替换协议。"""

    @property
    def idle(self) -> bool:
        """返回当前是否没有尚未投递完成的 Event。

        Returns:
            没有未完成工作时为 True，否则为 False。
        """
        ...

    def subscribe(
        self,
        event_type: str,
        handler: EventHandler,
        *,
        subscription: str | None = None,
    ) -> None:
        """订阅 Event；同名 subscription 的实例竞争消费。

        Args:
            event_type: 用于订阅或路由匹配的事件类型。
            handler: 接收事件或投递的处理函数。
            subscription: 竞争消费组名称，None 创建独立订阅。
        """

    def publish(self, event: Event) -> None:
        """发布一个 Event。

        Args:
            event: 需要发布、观察或处理的事件。
        """

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待已接受 Event 投递完成。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
        """

    def close(self) -> None:
        """关闭传输。"""


WorkHandler = Callable[[Delivery], DeliveryResult]


class TaskPublisher(Protocol):
    """向命名任务通道投递 Work。"""

    def submit(self, queue: str, work: Work) -> None:
        """向命名通道提交 Work；正常返回表示后端已接受。

        Args:
            queue: 命名消费通道。
            work: 需要投递或执行的工作请求。
        """

    def close(self) -> None:
        """关闭发布端持有的资源。"""


class LocalTaskPublisher(TaskPublisher, Protocol):
    """可在当前进程延续 Slot 链的可选任务发布能力。"""

    def submit_local(self, queue: str, work: Work, lease: SlotLease) -> None:
        """正常返回时接管一个 lease 引用；抛错时引用仍由调用方持有。

        Args:
            queue: 命名消费通道。
            work: 需要投递或执行的工作请求。
            lease: 当前进程内执行槽的引用与串行执行能力。
        """


class TaskConsumer(Protocol):
    """从命名任务通道消费 Work。"""

    @property
    def idle(self) -> bool:
        """返回当前实例是否没有尚未完成的 Work。

        Returns:
            没有未完成工作时为 True，否则为 False。
        """
        ...

    def bind(
        self,
        queue: str,
        handler: WorkHandler,
        *,
        concurrency: int,
        slots: SlotProvider | None = None,
    ) -> None:
        """绑定通道，并为不携带 Slot 的根 Work 分配执行槽。

        Args:
            queue: 命名消费通道。
            handler: 接收事件或投递的处理函数。
            concurrency: 当前消费者允许并行执行的完整 Graph 数量。
            slots: 提供本地执行槽的资源池能力。
        """

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待当前实例已接受的 Work 完成。

        Args:
            timeout: 等待或执行时限，单位秒；None 表示不设置时限。
        """

    def close(self) -> None:
        """关闭消费端持有的资源。"""


class TaskBackend(TaskPublisher, TaskConsumer, Protocol):
    """兼具任务发布和消费能力的便捷组合协议。"""


Emit = Callable[[Event], None]


@runtime_checkable
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
        execution: Execution,
    ) -> None | Awaitable[None]:
        """执行冻结 Graph，通过 execution.publish_output/apublish_output 交付结果。

        Args:
            name: 注册或查找使用的名称。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            inputs: 入口数据或按端口名称组织的输入映射。
            emit: 发布跨图事件的回调。
            plan: 限定本次执行范围的计划，None 使用完整 Graph。
            slot: 当前逻辑执行链使用的本地执行槽。
            execution: 记录当前执行状态、控制限制及输出的句柄。

        Returns:
            同步实现返回 None；异步实现返回最终结果为 None 的等待对象。
        """

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
        """挂载 Hook，并返回可用于卸载的句柄。

        Args:
            hook: 节点 Hook 对象或单阶段回调。
            phase: 函数 Hook 对应的执行阶段。
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            node: 节点实例或作用域中的节点 ID，以接口类型为准。

        Returns:
            用于卸载本次注册的句柄。
        """
        ...


class ExecutionFactory(Protocol):
    def __call__(
        self,
        graph: str,
        *,
        limits: ExecutionLimits,
        id: str | None = None,
        output_buffer: int = 64,
    ) -> Execution:
        """创建全新的 Execution，可为其注入独立存储和通知实现。

        Args:
            graph: 目标 Graph 定义或其注册名称，以类型声明为准。
            limits: 本次执行独立使用的步数和时长限制。
            id: 对象标识，允许缺省时由实现生成。
            output_buffer: 每个活跃输出流允许积压的输出条数。

        Returns:
            本次操作得到的 Execution 实例。
        """
        ...
