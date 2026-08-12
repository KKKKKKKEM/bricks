"""Bricks 的精简 typed Graph、领域事件与 Runtime API。"""

from .core import AsyncNode, InputPolicy, Node, Output, Ports
from .errors import (
    BricksError,
    BricksRuntimeError,
    EventDispatchError,
    ExecutionError,
    GraphError,
    GraphFrozenError,
    GraphValidationError,
    IncompleteInputsError,
    InvalidOutputError,
    PortValueTypeError,
    RuntimeClosedError,
    UnknownGraphError,
)
from .events import Context, Event
from .graph import Edge, Graph
from .runtime import Runtime

# 兼容旧版本的直接导入；不再通过 __all__ 推荐这个易混淆名称。
RuntimeError = BricksRuntimeError

__all__ = [
    "AsyncNode",
    "BricksError",
    "BricksRuntimeError",
    "Context",
    "Edge",
    "Event",
    "EventDispatchError",
    "ExecutionError",
    "Graph",
    "GraphError",
    "GraphFrozenError",
    "GraphValidationError",
    "IncompleteInputsError",
    "InputPolicy",
    "InvalidOutputError",
    "Node",
    "Output",
    "PortValueTypeError",
    "Ports",
    "Runtime",
    "RuntimeClosedError",
    "UnknownGraphError",
]
