"""演示一次 Outcome 同时更新 Context 并沿图继续路由。"""

from bricks import GraphBuilder, Machine, Outcome, Status


def prepare(context, event):
    return Outcome.next("classify", update={"kind": "manual"})


def review(context, event):
    return Outcome.next("approve", update={"reviewed": True})


builder = GraphBuilder("dynamic-route", initial="start")
builder.action("start", prepare)
builder.action("review", review)
builder.terminal("done")
builder.transition(
    "start",
    "classify",
    "review",
    guard=lambda context, event: context.get("kind") == "manual",
)
builder.transition("review", "approve", "done")

machine = Machine(builder.build())
machine.start()

assert machine.status is Status.COMPLETED
assert machine.context.data == {"kind": "manual", "reviewed": True}
print(machine.context.data)
