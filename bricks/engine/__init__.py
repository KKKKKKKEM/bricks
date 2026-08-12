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
    HookExecutionError,
    IncompleteInputsError,
    InvalidOutputError,
    PortValueTypeError,
    RuntimeClosedError,
    UnknownGraphError,
)
from .events import Context, Event
from .graph import Edge, ExecutionPlan, Graph
from .hooks import (
    HookHandle,
    HookPhase,
    HookRegistry,
    HookSignal,
    NodeCall,
    NodeHook,
    ShortCircuit,
    StopGraph,
)
from .runtime import EventRouter, GraphWorker, Runtime
from .slots import Slot, SlotPool

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
    "EventRouter",
    "ExecutionError",
    "ExecutionPlan",
    "Graph",
    "GraphError",
    "GraphFrozenError",
    "GraphValidationError",
    "GraphWorker",
    "HookExecutionError",
    "HookHandle",
    "HookPhase",
    "HookRegistry",
    "HookSignal",
    "IncompleteInputsError",
    "InputPolicy",
    "InvalidOutputError",
    "Node",
    "NodeCall",
    "NodeHook",
    "Output",
    "PortValueTypeError",
    "Ports",
    "Runtime",
    "RuntimeClosedError",
    "ShortCircuit",
    "Slot",
    "SlotPool",
    "StopGraph",
    "UnknownGraphError",
]
