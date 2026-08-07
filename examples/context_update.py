"""演示控制面更新 Context，以及事件驱动的人工审批迁移。"""

from bricks import GraphBuilder, Machine, Status


builder = GraphBuilder("context-update", initial="waiting_for_approval")
builder.wait("waiting_for_approval", resume_event="approved")
builder.terminal("done")
builder.transition("waiting_for_approval", "approved", "done")
graph = builder.build()

machine = Machine(graph)
machine.start()
assert machine.status is Status.WAITING

# 外部控制面可以先写入业务数据，但不会让图自动迁移。
machine.update_context(approved=True, reviewer="alice")
assert machine.status is Status.WAITING
assert machine.node_id == "waiting_for_approval"

# 真正的迁移仍然由事件触发。
machine.resume("approved")
assert machine.status is Status.COMPLETED
assert machine.context.data == {"approved": True, "reviewer": "alice"}

print(machine.context.data)
