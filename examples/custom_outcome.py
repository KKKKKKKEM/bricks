"""Extend the runtime with an Agent-style effect without modifying Machine."""

from dataclasses import dataclass

from bricks import GraphBuilder, Machine, Outcome, Status
from bricks.engine.runtime import OutcomeDirective, default_outcome_interpreter


@dataclass(frozen=True, slots=True)
class RecordThought(Outcome):
    text: str


def record_thought(runtime, outcome):
    history = list(runtime.context.get("thoughts", ()))
    history.append(outcome.text)
    runtime.update({"thoughts": history})


interpreter = default_outcome_interpreter().with_handler(
    RecordThought,
    record_thought,
    # Effects continue to the target node; controls use the default STOP.
    directive=OutcomeDirective.CONTINUE,
)

builder = GraphBuilder("minimal-agent", initial="plan")
builder.action("plan")
builder.terminal("answer", lambda context, event: {"answer": "done"})
builder.transition(
    "plan",
    "execute",
    "answer",
    action=lambda context, event: RecordThought("inspect the available tools"),
)

machine = Machine(builder.build(), outcome_interpreter=interpreter)
machine.start()
machine.dispatch("execute")

assert machine.status is Status.COMPLETED
assert machine.context.data == {
    "thoughts": ["inspect the available tools"],
    "answer": "done",
}
