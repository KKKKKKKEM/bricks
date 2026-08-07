"""A minimal Spider control plane built without adding Spider logic to engine."""

from dataclasses import dataclass

from bricks import Event, GraphBuilder, Machine, Outcome, Status
from bricks.engine.persistence import (
    AtomicPersistenceBinding,
    InMemoryAtomicCommitStore,
)
from bricks.engine.runtime import OutcomeDirective, default_outcome_interpreter


@dataclass(frozen=True, slots=True)
class Request:
    id: str
    url: str


@dataclass(frozen=True, slots=True)
class ScheduleRequest(Outcome):
    request: Request


def schedule(runtime, outcome):
    request = outcome.request
    runtime.stage_effect(
        "spider.request",
        {"url": request.url},
        effect_id=request.id,
    )


interpreter = default_outcome_interpreter().with_handler(
    ScheduleRequest,
    schedule,
    directive=OutcomeDirective.CONTINUE,
)

builder = GraphBuilder("spider-job", initial="ready")
builder.action("ready")
builder.wait("downloading", resume_event="response.received")
builder.terminal(
    "parsed",
    lambda context, event: {"title": event.payload["title"]},
)
builder.transition(
    "ready",
    "crawl",
    "downloading",
    action=lambda context, event: ScheduleRequest(
        Request("request-1", "https://example.test")
    ),
)
builder.transition("downloading", "response.received", "parsed")

store = InMemoryAtomicCommitStore()
machine = Machine(builder.build(), outcome_interpreter=interpreter)
AtomicPersistenceBinding(machine, store).attach()
machine.start()
machine.dispatch("crawl")

# A queue adapter publishes pending outbox effects and acknowledges delivery.
request = store.pending_effects(topic="spider.request")[0]
assert request.payload == {"url": "https://example.test"}
store.mark_effect_sent(request.id)

# A downloader adapter turns its result back into a normal engine Event.
machine.resume(Event("response.received", {"title": "Bricks"}))
assert machine.status is Status.COMPLETED
assert machine.context.get("title") == "Bricks"
