"""演示独立子图运行、返回父图以及 Context 数据合并。"""

from bricks import GraphBuilder, Machine, Status


child_builder = GraphBuilder("review", initial="start")
child_builder.action("start", lambda context, event: context.set("reviewed", True))
child_builder.terminal("done")
child_builder.transition("start", "finish", "done")
child_graph = child_builder.build()

parent_builder = GraphBuilder("order", initial="start")
parent_builder.action("start")
parent_builder.subgraph(
    "review_flow",
    child_graph,
    entry_event="finish",
    return_event="review_finished",
)
parent_builder.terminal("done")
parent_builder.transition("start", "review", "review_flow")
parent_builder.transition("review_flow", "review_finished", "done")
parent_graph = parent_builder.build()

machine = Machine(parent_graph)
machine.start()
machine.dispatch("review")
assert machine.status is Status.WAITING
machine.join()

assert machine.status is Status.COMPLETED
assert machine.context.get("reviewed") is True
print(machine.status.value)
