# 图组合和结构检查

## 复用图片段

`GraphBuilder.include()` 将已经校验的 Graph 作为命名空间片段加入另一张图。它只做
静态定义组合，不创建第二个 Machine：

```python
from bricks import GraphBuilder

review = GraphBuilder("review", initial="draft")
review.action("draft")
review.action("approved")
review.transition("draft", "approve", "approved")

order = GraphBuilder("order", initial="start")
order.action("start")
entry = order.include(review.build(), prefix="review_flow")
order.transition("start", "review", entry)
graph = order.build()
```

被包含图中的节点和迁移会变成：

```text
review_flow.draft
review_flow.approved
review_flow.draft --approve--> review_flow.approved
```

`include()` 返回被包含图的入口节点 ID，外部图可以用它创建入口迁移。可复用片段
的边界应使用非终止节点；终止节点会直接结束整个 Machine。Action、Guard 和元数据
会被复用，但节点和迁移 ID 会隔离在前缀下。

## 结构描述

```python
description = graph.describe()
```

`describe()` 返回节点、迁移、优先级、Guard/Action 是否存在等结构信息，不暴露可执行
对象本身。它适合：

- 静态检查和测试图结构。
- 转换为 Mermaid、DOT 或其它可视化格式。
- 在编辑器或调试工具中展示图。

这个描述不是可直接恢复执行的序列化格式。如果需要持久化执行定义，使用带版本的
`graph.to_dict()`，并显式提供 Action、Guard 和自定义节点的编码/解析协议。引擎不会
根据函数名猜测导入路径。

需要快速查看流程时，可以直接生成 Mermaid 文本：

```python
print(graph.to_mermaid())
```

如果下游工具链使用 Graphviz，则直接生成 DOT 文本：

```python
print(graph.to_dot())
```

`reachable_nodes(graph)` 和 `unreachable_nodes(graph)` 可以检查是否存在从初始节点
不可达的定义节点。

发布前还可以检查循环、终止覆盖和同优先级分支：

```python
from bricks.engine.graph import (
    cycle_nodes,
    non_terminating_nodes,
    transition_conflicts,
)

print(cycle_nodes(graph))
print(non_terminating_nodes(graph))
print(transition_conflicts(graph))
```

这些检查只返回报告，不改变图的运行语义。事件驱动流程可以有意包含循环或没有终止
节点的等待边界；是否允许它们由应用自己的发布规则决定。

## 条件分支

Bricks 不额外创建 `ConditionalEdge` 类型。对同一源节点和事件声明多条带 Guard 的
`Transition` 即可表达条件分支：先按优先级选择，再按 Guard 过滤。

```python
builder.transition(
    "review",
    "submit",
    "approved",
    guard=lambda context, event: context.get("valid", False),
)
builder.transition("review", "submit", "rejected")
```

## 运行时子图

如果子图需要独立的 `Context` 和生命周期，可以使用 `GraphBuilder.subgraph()`：

```python
builder.subgraph(
    "review_flow",
    review_graph,
    entry_event="start_review",
    return_event="review_finished",
)
builder.transition("start", "review", "review_flow")
builder.transition("review_flow", "review_finished", "done")
```

进入 `review_flow` 后，父 Machine 会进入 Fork 等待，子 Machine 使用自己的 Graph 和
Context 运行；子图完成后调用 `join()`，父 Machine 通过 `review_finished` 回到父图。
成功子图的 `Context.data` 会合并回父 Context。

如果子图内部又创建了 Fork，可以按运行 ID 将事件路由到任意后代，再按层级 Join：

```python
machine.route(grandchild.context.run_id, "wake")
machine.join(child.context.run_id)
machine.join()
```

`route()` 只负责找到并消费目标运行的事件；它不会绕过目标 Graph 的迁移和 Guard。
`join(child_run_id)` 先完成指定子运行自己的 Fork，最后无参数的 `join()` 才完成当前
父运行的 Fork。

运行时子图是单分支 Fork/Join 的专用语义，不是第二套执行引擎。恢复包含不同 Graph
的子图快照时，需要给 `Machine.from_snapshot()` 提供：

```python
graph_resolver=lambda graph_id: graph_registry[graph_id]
```

Graph 定义本身也可以使用同一个 `graph_resolver` 恢复 `SubGraphNode`：

```python
restored = Graph.from_dict(
    encoded,
    graph_resolver=lambda graph_id: graph_registry[graph_id],
)
```
