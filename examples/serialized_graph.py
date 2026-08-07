"""演示 Graph 定义的版本化编码和显式 Action/Guard 解析。"""

import json

from bricks import Context, Graph, GraphBuilder, Machine, Status


def mark_started(context, event):
    context.set("started", True)


def mark_finished(context, event):
    context.set("finished", True)


def allow(context, event):
    return True


actions = {
    "mark_started": mark_started,
    "mark_finished": mark_finished,
}
guards = {"allow": allow}

builder = GraphBuilder("serialized", initial="start")
builder.action("start", mark_started)
builder.terminal("done")
builder.transition(
    "start",
    "finish",
    "done",
    guard=allow,
    action=mark_finished,
)
graph = builder.build()

definition = graph.to_dict(
    action_serializer=lambda action: next(
        name for name, value in actions.items() if value is action
    ),
    guard_serializer=lambda guard: next(
        name for name, value in guards.items() if value is guard
    ),
)
print(json.dumps(definition, ensure_ascii=False, indent=2))

restored = Graph.from_dict(
    definition,
    action_resolver=actions.__getitem__,
    guard_resolver=guards.__getitem__,
)
machine = Machine(restored, context=Context(graph_id=restored.id))
machine.invoke(["finish"])

assert machine.status is Status.COMPLETED
assert machine.context.data == {"started": True, "finished": True}
