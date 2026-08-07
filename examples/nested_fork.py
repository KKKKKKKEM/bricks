"""演示事件路由到嵌套 Fork，以及按层级 Join。"""

from bricks import GraphBuilder, Machine, Outcome, Status


child_builder = GraphBuilder("nested-child", initial="start")
child_builder.action("start")
child_builder.action(
    "forking",
    lambda context, event: Outcome.fork({"event": "wait"}, join_event="joined"),
)
child_builder.wait("waiting", resume_event="wake")
child_builder.terminal("grandchild_done")
child_builder.terminal("child_done")
child_builder.transition("start", "fork", "forking")
child_builder.transition("start", "wait", "waiting")
child_builder.transition("waiting", "wake", "grandchild_done")
child_builder.transition("forking", "joined", "child_done")
child_graph = child_builder.build()

parent_builder = GraphBuilder("nested-parent", initial="start")
parent_builder.action("start")
parent_builder.subgraph(
    "child_flow",
    child_graph,
    entry_event="fork",
    return_event="returned",
)
parent_builder.terminal("done")
parent_builder.transition("start", "enter", "child_flow")
parent_builder.transition("child_flow", "returned", "done")
machine = Machine(parent_builder.build())

machine.start()
machine.dispatch("enter")
child = machine.fork_group.children[0]
grandchild = child.fork_group.children[0]

machine.route(grandchild.context.run_id, "wake")
machine.join(child.context.run_id)
machine.join()

assert machine.status is Status.COMPLETED
print(machine.status.value)
