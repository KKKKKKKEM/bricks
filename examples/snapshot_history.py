"""演示可选的快照历史查询能力。"""

from bricks import GraphBuilder, Machine
from bricks.engine.persistence import InMemorySnapshotStore, PersistenceBinding


builder = GraphBuilder("snapshot-history", initial="ready")
builder.action("ready", lambda context, event: context.set("step", "ready"))
builder.terminal("done")
builder.transition("ready", "finish", "done")

store = InMemorySnapshotStore()
machine = Machine(builder.build())
binding = PersistenceBinding(machine, store).attach()
machine.start()
machine.dispatch("finish")

history = binding.history()
assert [snapshot.revision for snapshot in history] == [1, 2]
historical = Machine.from_snapshot(machine.graph, history[0])
assert historical.node_id == "ready"
print([snapshot.context["status"] for snapshot in history])
