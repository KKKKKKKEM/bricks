"""演示 Fork 在部分分支失败时继续汇聚结果。"""

from bricks import GraphBuilder, Machine, Outcome, Status


def record_children(context, event):
    context.set(
        "child_statuses",
        [child["status"] for child in event.payload["children"]],
    )


builder = GraphBuilder("fork-failure", initial="start")
builder.action("start")
builder.action(
    "forking",
    lambda context, event: Outcome.fork(
        {"event": "fail"},
        {"event": "finish"},
        join_event="joined",
        failure_policy="continue",
    ),
)
builder.action("failing", lambda context, event: Outcome.fail("temporary error"))
builder.terminal("child_done")
builder.action("joined", record_children)
builder.terminal("done")
builder.transition("start", "fork", "forking")
builder.transition("start", "fail", "failing")
builder.transition("start", "finish", "child_done")
builder.transition("forking", "joined", "joined")
builder.transition("joined", "finish", "done")

machine = Machine(builder.build())
machine.start()
machine.dispatch("fork")
machine.join()
machine.dispatch("finish")

assert machine.status is Status.COMPLETED
assert machine.context.get("child_statuses") == [
    Status.FAILED.value,
    Status.COMPLETED.value,
]
print(machine.context.get("child_statuses"))
