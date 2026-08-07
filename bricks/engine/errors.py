"""引擎定义层和运行时抛出的异常。"""

from __future__ import annotations


class EngineError(Exception):
    """所有 Bricks 引擎异常的基类。"""


class GraphError(EngineError):
    """图定义异常的基类。"""


class GraphValidationError(GraphError):
    """图无法安全执行时抛出。"""


class DuplicateNodeError(GraphError):
    """图中出现重复节点标识时抛出。"""


class DuplicateTransitionError(GraphError):
    """迁移标识被重复使用时抛出。"""


class GraphSerializationError(GraphError):
    """图定义无法编码或恢复时抛出。"""


class RuntimeErrorBase(EngineError):
    """运行时异常的基类。"""


class MachineNotStarted(RuntimeErrorBase):
    """运行实例尚未启动就收到事件时抛出。"""


class MachineNotRunnable(RuntimeErrorBase):
    """运行实例处于暂停、等待、完成或失败状态时抛出。"""


class AmbiguousEventRoute(RuntimeErrorBase):
    """未定向事件同时匹配多个运行实例时抛出。"""


class InternalStepLimitExceeded(RuntimeErrorBase):
    """连续的内部 Next 事件超过安全步数上限时抛出。"""


class OutcomeConflictError(RuntimeErrorBase):
    """Legacy compatibility error retained for integrations importing it."""


class NoTransition(RuntimeErrorBase):
    """当前节点没有符合条件的事件边时抛出。"""

    def __init__(self, node_id: str, event_name: str):
        self.node_id = node_id
        self.event_name = event_name
        super().__init__(
            f"no eligible transition for event {event_name!r} from node {node_id!r}"
        )


class AsyncActionRequired(RuntimeErrorBase):
    """同步执行器收到异步 Action 或 Hook 时抛出。"""


class AsyncGuardRequired(RuntimeErrorBase):
    """同步图入口收到异步 Guard 时抛出。"""


class CancellationError(RuntimeErrorBase):
    """运行实例在执行前发现已经被取消。"""


class ActionTimeout(RuntimeErrorBase):
    """Action 超过执行策略允许的时间。"""


class DuplicateEvent(RuntimeErrorBase):
    """同一个幂等键已经被当前运行实例消费。"""

    def __init__(self, key: str):
        self.key = key
        super().__init__(f"event has already been consumed: {key!r}")


class PersistenceError(EngineError):
    """快照或事件日志存储失败时抛出。"""


class SnapshotConflictError(PersistenceError):
    """保存快照时发现存储中的版本已经领先。"""


class PolicyError(EngineError):
    """执行策略配置或执行失败时抛出。"""
