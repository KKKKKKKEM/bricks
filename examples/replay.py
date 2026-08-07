"""演示 EventLog 的显式重放边界。"""

from bricks import GraphBuilder, Machine, Status
from bricks.engine.persistence import (
    InMemoryEventLog,
    InMemorySnapshotStore,
    PersistenceBinding,
    replay_events,
)


builder = GraphBuilder("replay", initial="ready")
builder.action("ready")
builder.terminal("done")
builder.transition("ready", "finish", "done")
graph = builder.build()

log = InMemoryEventLog()
machine = Machine(graph)
PersistenceBinding(machine, InMemorySnapshotStore(), log).attach()
machine.start()
machine.dispatch("finish")

replayed = replay_events(graph, log.read(machine.context.run_id))
assert machine.status is Status.COMPLETED
assert replayed.status is Status.COMPLETED
print(replayed.context.last_event.name)
