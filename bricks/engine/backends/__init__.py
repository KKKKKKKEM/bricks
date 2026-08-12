"""Runtime 的可替换 backend：协议、任务模型和默认内存实现。"""

from .memory import MemoryEventBus, MemoryTaskBackend
from .protocols import (
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
    "Emit",
    "EventBus",
    "EventHandler",
    "GraphExecutor",
    "HookableGraphExecutor",
    "MemoryEventBus",
    "MemoryTaskBackend",
    "TaskBackend",
    "TaskConsumer",
    "TaskPublisher",
    "Work",
    "WorkHandler",
]
