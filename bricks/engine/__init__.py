"""Bricks 的精简 typed Graph、领域事件与 Runtime API。"""

from .core import AsyncNode, InputPolicy, Node, Output, Ports
from .errors import (
    BricksError,
    BricksRuntimeError,
    EventDispatchError,
    ExecutionCancelledError,
    ExecutionControlError,
    ExecutionError,
    ExecutionTimeoutError,
    GraphError,
    GraphFrozenError,
    GraphValidationError,
    HookExecutionError,
    IncompleteInputsError,
    InvalidOutputError,
    NodeTimeoutError,
    PortValueTypeError,
    RuntimeClosedError,
    StepLimitExceededError,
    UnknownGraphError,
)
from .events import Context, Event
from .execution import Execution, ExecutionLimits, ExecutionStatus
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
from .observation import (
    CompositeObserverHandle,
    ObservationHub,
    ObserverHandle,
    RuntimeEvent,
    RuntimeEventKind,
    RuntimeObserver,
)
from .policies import InputSelector, PolicyRef, PolicyRegistry
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
    "Execution",
    "ExecutionCancelledError",
    "ExecutionControlError",
    "ExecutionError",
    "ExecutionLimits",
    "ExecutionPlan",
    "ExecutionStatus",
    "ExecutionTimeoutError",
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
    "NodeTimeoutError",
    "ObservationHub",
    "ObserverHandle",
    "CompositeObserverHandle",
    "Output",
    "PortValueTypeError",
    "Ports",
    "PolicyRef",
    "PolicyRegistry",
    "InputSelector",
    "Runtime",
    "RuntimeClosedError",
    "RuntimeEvent",
    "RuntimeEventKind",
    "RuntimeObserver",
    "ShortCircuit",
    "Slot",
    "SlotPool",
    "StepLimitExceededError",
    "StopGraph",
    "UnknownGraphError",
]
