"""Bricks 精简内核的公共错误。"""

from __future__ import annotations


class BricksError(Exception):
    """所有 Bricks 公共错误的基类。"""


class GraphError(BricksError):
    """Graph 定义、冻结或引用不合法。"""


class GraphFrozenError(GraphError):
    """调用方尝试修改已经冻结的 Graph。"""


class GraphValidationError(GraphError):
    """Graph 在冻结时未满足静态约束。"""


class BricksRuntimeError(BricksError):
    """Runtime 注册、路由或生命周期操作失败。"""


class UnknownGraphError(BricksRuntimeError):
    """Runtime 中不存在指定的 Graph 注册名。"""


class ExecutionError(BricksRuntimeError):
    """一次 Graph 或 Node 执行失败。"""

    def __init__(
        self,
        message: str,
        *,
        graph: str | None = None,
        node: str | None = None,
        event: object | None = None,
    ) -> None:
        """保存失败位置和可选触发事件。

        参数：
            message: 面向调用方的错误说明。
            graph: 失败的 Graph 注册名或定义名。
            node: 失败的 Node ID。
            event: 触发跨图执行的领域事件。
        """

        super().__init__(message)
        self.graph = graph
        self.node = node
        self.event = event


class ExecutionControlError(ExecutionError):
    """执行因调用方配置的控制条件而终止。"""


class ExecutionCancelledError(ExecutionControlError):
    """执行收到协作式取消请求。"""


class ExecutionTimeoutError(ExecutionControlError, TimeoutError):
    """整张 Graph 超过允许的总执行时长。"""


class NodeTimeoutError(ExecutionControlError, TimeoutError):
    """一次 Node firing 超过允许的执行时长。"""


class StepLimitExceededError(ExecutionControlError):
    """Graph 尝试执行超过允许步数的 Node firing。"""


class HookExecutionError(ExecutionError):
    """Hook 返回非法数据、修改调用身份或在非法阶段发出信号。"""


class IncompleteInputsError(ExecutionError):
    """Graph 静止时仍存在无法满足输入策略的数据。"""


class InvalidOutputError(ExecutionError, TypeError):
    """Node 返回了不符合输出协议的值。"""


class PortValueTypeError(ExecutionError, TypeError):
    """实际端口值不满足 Ports 声明的类型。"""


class EventDispatchError(BricksRuntimeError):
    """事件订阅者或目标 Graph 执行失败。"""

    def __init__(self, event: object, cause: BaseException) -> None:
        """保存投递失败的事件与原始异常。

        参数：
            event: 投递失败的领域事件。
            cause: handler 或目标 Graph 抛出的原始异常。
        """

        super().__init__(f"failed to dispatch event: {cause}")
        self.event = event
        self.cause = cause


class RuntimeClosedError(BricksRuntimeError):
    """调用方在 Runtime 关闭后继续提交工作。"""
