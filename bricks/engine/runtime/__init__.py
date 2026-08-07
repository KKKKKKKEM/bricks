"""执行图定义的运行时对象。"""

from .context import Context
from .executor import ActionExecutor, InlineExecutor
from .effects import StagedEffect
from .fork import ForkRuntime, ForkRuntimeFactory
from .lifecycle import LifecycleEvent, Status
from .interpreter import (
    LegacyOutcomeHandler,
    OutcomeDirective,
    OutcomeHandler,
    OutcomeInterpreter,
    OutcomeRegistry,
    OutcomeRule,
    default_outcome_interpreter,
)
from .outcome_runtime import ContextView, OutcomeRuntime
from .machine import ForkGroup, Machine, TransitionResult
from .outcomes import (
    Emit,
    Fail,
    Fork,
    ForkBranch,
    Next,
    Outcome,
    Retry,
    Stop,
    Update,
    Wait,
)
from .selector import DefaultTransitionSelector, TransitionSelector

__all__ = [
    "ActionExecutor",
    "Emit",
    "Fail",
    "Fork",
    "ForkBranch",
    "ForkGroup",
    "ForkRuntime",
    "ForkRuntimeFactory",
    "InlineExecutor",
    "LifecycleEvent",
    "LegacyOutcomeHandler",
    "Machine",
    "Next",
    "Outcome",
    "OutcomeDirective",
    "OutcomeHandler",
    "OutcomeInterpreter",
    "OutcomeRegistry",
    "OutcomeRule",
    "OutcomeRuntime",
    "Retry",
    "Context",
    "ContextView",
    "DefaultTransitionSelector",
    "Status",
    "StagedEffect",
    "Stop",
    "TransitionResult",
    "TransitionSelector",
    "Update",
    "Wait",
    "default_outcome_interpreter",
]
