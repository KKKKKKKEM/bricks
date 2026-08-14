"""Runtime 可独立替换和组合的窄角色协议。"""

from .runtime import (
    Delivery,
    DeliveryOutcome,
    DeliveryResult,
    Emit,
    EventBus,
    EventHandler,
    GraphExecutor,
    HookableGraphExecutor,
    TaskBackend,
    TaskConsumer,
    TaskPublisher,
    Work,
    WorkHandler,
)

__all__ = [
    "Delivery",
    "DeliveryOutcome",
    "DeliveryResult",
    "Emit",
    "EventBus",
    "EventHandler",
    "GraphExecutor",
    "HookableGraphExecutor",
    "TaskBackend",
    "TaskConsumer",
    "TaskPublisher",
    "Work",
    "WorkHandler",
]
