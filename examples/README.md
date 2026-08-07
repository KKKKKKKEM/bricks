# Bricks 示例

示例只使用稳定的最小 API，用来说明引擎的核心模型，不承载具体领域逻辑。

```text
basic_graph.py   GraphBuilder、Graph、Machine 和 Event
custom_node.py   BaseNode 继承扩展
custom_executor.py ActionExecutor 边界和 Fork 外部执行
async_guard.py    异步 Guard 和异步事件迁移
composed_graph.py Graph.include() 和 Graph.describe()
subgraph.py     独立子图、Join 和 Context 合并
external_events.py 外部消息驱动、持久化和恢复
external_driver.py 按 run_id 恢复并投递外部消息
custom_store.py   只实现 save/load/delete 的外部快照存储
graph_visualization.py Graph 的 JSON、Mermaid 和 DOT 输出
exception_retry.py 节点异常的显式自动重试
snapshot_history.py 可选快照历史查询
context_update.py  控制面更新 Context 和人工审批事件
dynamic_route.py  Context 更新和内部条件路由
map_reduce.py     动态 Fan-out、分支处理和 Join 汇聚
```

运行示例：

```bash
uv run python examples/basic_graph.py
uv run python examples/custom_node.py
```
