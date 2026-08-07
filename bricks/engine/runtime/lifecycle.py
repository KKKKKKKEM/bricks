"""运行实例生命周期词汇。"""

from __future__ import annotations

from enum import Enum


class Status(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    WAITING = "waiting"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    STOPPED = "stopped"


class LifecycleEvent(str, Enum):
    BEFORE_START = "machine.before_start"
    AFTER_START = "machine.after_start"
    BEFORE_DISPATCH = "machine.before_dispatch"
    AFTER_DISPATCH = "machine.after_dispatch"
    BEFORE_TRANSITION = "transition.before"
    AFTER_TRANSITION = "transition.after"
    TRANSITION_ERROR = "transition.error"
    NODE_ENTER = "node.enter"
    NODE_EXIT = "node.exit"
    EVENT_UNHANDLED = "event.unhandled"
    EVENT_EMITTED = "event.emitted"
    RUN_PAUSED = "machine.paused"
    RUN_RESUMED = "machine.resumed"
    AFTER_RESUME = "machine.after_resume"
    AFTER_JOIN = "machine.after_join"
    CONTEXT_UPDATED = "context.updated"
