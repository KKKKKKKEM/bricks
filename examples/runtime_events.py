"""演示在保持 stream() 简洁的同时观察完整运行事件。"""

from bricks import GraphBuilder, Machine, Status


builder = GraphBuilder("runtime-events", initial="ready")
builder.action("ready")
builder.terminal("done")
builder.transition("ready", "finish", "done")

machine = Machine(builder.build())
observations = list(machine.stream_events(["finish"]))
transitions = list(
    Machine(builder.build()).stream_events(
        ["finish"],
        match=lambda item: item.name == "transition.after",
    )
)

assert observations[0].name == "machine.before_start"
assert any(item.name == "transition.after" for item in observations)
assert transitions
assert all(item.name == "transition.after" for item in transitions)
assert observations[-1].run_id == machine.context.run_id
assert machine.status is Status.COMPLETED

for item in observations:
    print(item.sequence, item.name, item.node_id, item.status)
