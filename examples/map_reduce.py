"""演示根据 Context 动态创建分支，并在 Join 处汇聚结果。"""

from bricks import GraphBuilder, Machine, Outcome, Status


def fan_out(context, event):
    return Outcome.fork(
        *(
            {"event": "process", "data": {"item": item}}
            for item in context.get("items", [])
        ),
        join_event="joined",
    )


def process(context, event):
    context.set("value", context.get("item") * 2)
    return Outcome.next("finish")


def reduce_results(context, event):
    context.set(
        "results",
        [child["data"]["value"] for child in event.payload["children"]],
    )


builder = GraphBuilder("map-reduce", initial="start")
builder.action("start", lambda context, event: context.set("items", [1, 2, 3]))
builder.action("fan_out", fan_out)
builder.action("process", process)
builder.terminal("branch_done")
builder.action("joined", reduce_results)
builder.terminal("done")
builder.transition("start", "fork", "fan_out")
builder.transition("start", "process", "process")
builder.transition("process", "finish", "branch_done")
builder.transition("fan_out", "joined", "joined")
builder.transition("joined", "finish", "done")

machine = Machine(builder.build())
machine.start()
machine.dispatch("fork")
machine.join()
machine.dispatch("finish")

assert machine.status is Status.COMPLETED
assert machine.context.get("results") == [2, 4, 6]
print(machine.context.get("results"))
