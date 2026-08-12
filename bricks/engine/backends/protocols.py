"""Runtime 的可替换能力协议和跨后端任务模型。"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from ..core import Output, require_non_empty_string
from ..events import Event
from ..graph import Graph

EventHandler = Callable[[Event], None]


@dataclass(frozen=True, slots=True)
class Work:
    """TaskBackend 在进程或消息系统之间搬运的最小执行请求。"""

    graph: str
    inputs: Any = None
    trigger: Event | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        require_non_empty_string(self.graph, "work graph")


class EventBus(Protocol):
    """发布和订阅 Event 的替换协议。"""

    @property
    def idle(self) -> bool:
        """返回当前是否没有尚未投递完成的 Event。"""

    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        """订阅精确 Event type；``*`` 表示全部类型。"""

    def publish(self, event: Event) -> None:
        """发布一个 Event。"""

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待已接受 Event 投递完成。"""

    def close(self) -> None:
        """关闭传输。"""


WorkHandler = Callable[[Work], None]


class TaskBackend(Protocol):
    """配置命名执行通道并投递 Work 的替换协议。"""

    @property
    def idle(self) -> bool:
        """返回当前是否没有尚未完成的 Work。"""

    def bind(
        self,
        queue: str,
        handler: WorkHandler,
        *,
        concurrency: int,
    ) -> None:
        """绑定一个命名通道及其消费并发。"""

    def submit(self, queue: str, work: Work) -> None:
        """向命名通道提交 Work。"""

    def wait_idle(self, timeout: float | None = None) -> None:
        """等待已接受 Work 完成。"""

    def close(self) -> None:
        """关闭全部执行通道。"""


Emit = Callable[[Event], None]


class GraphExecutor(Protocol):
    """执行单张 Graph 的替换协议。"""

    def execute(
        self,
        name: str,
        graph: Graph,
        inputs: Any,
        emit: Emit,
    ) -> tuple[Output, ...]:
        """执行 Graph 并返回终端 Output。"""

    def close(self) -> None:
        """关闭执行器持有的资源。"""
