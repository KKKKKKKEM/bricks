"""领域无关的图执行基础抽象。"""

from .context import ExecutionContext
from .errors import (
    BricksError,
    GraphDefinitionError,
    GraphError,
    GraphFrozenError,
    GraphValidationError,
    InvalidOutputError,
    UnknownFlowError,
    UnknownNodeError,
)
from .graph import Edge, Endpoint, Flow, Graph
from .inputs import (
    InputAvailability,
    InputGroup,
    InputPolicy,
    InputSelection,
    InputToken,
    NodeInputs,
)
from .node import Node, NodeResult, Output
from .ports import Ports, is_type_compatible

__all__ = [
    "BricksError",
    "Edge",
    "Endpoint",
    "ExecutionContext",
    "Flow",
    "Graph",
    "GraphDefinitionError",
    "GraphError",
    "GraphFrozenError",
    "GraphValidationError",
    "InputAvailability",
    "InputGroup",
    "InputPolicy",
    "InputSelection",
    "InputToken",
    "InvalidOutputError",
    "Node",
    "NodeInputs",
    "NodeResult",
    "Output",
    "Ports",
    "UnknownFlowError",
    "UnknownNodeError",
    "is_type_compatible",
]
