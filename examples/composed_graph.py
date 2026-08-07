"""演示把一张已校验的 Graph 作为命名空间片段组合进另一张图。"""

from bricks import GraphBuilder, Machine, Status


review = GraphBuilder("review", initial="draft")
review.action("draft")
review.action("approved")
review.transition("draft", "approve", "approved")

order = GraphBuilder("order", initial="start")
order.action("start")
review_entry = order.include(review.build(), prefix="review_flow")
order.transition("start", "review", review_entry)

graph = order.build()
machine = Machine(graph)
machine.start()
machine.dispatch("review")
machine.dispatch("approve")

assert machine.node_id == "review_flow.approved"
assert machine.status is Status.RUNNING
description = graph.describe()
assert description["initial"] == "start"
assert description["schema"] == "bricks.graph.description"
assert description["schema_version"] == 1

print(machine.node_id)
