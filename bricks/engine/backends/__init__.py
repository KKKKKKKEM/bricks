"""Runtime 的可替换 backend：协议、任务模型和默认内存实现。"""

from .memory import MemoryEventBus, MemoryTaskBackend
from .protocols import (
    Emit,
    EventBus,
    EventHandler,
    GraphExecutor,
    TaskBackend,
    Work,
    WorkHandler,
)

__all__ = [
    "Emit",
    "EventBus",
    "EventHandler",
    "GraphExecutor",
    "MemoryEventBus",
    "MemoryTaskBackend",
    "TaskBackend",
    "Work",
    "WorkHandler",
]
