# 编排能力对照

Bricks 借鉴通用图编排中的稳定概念，但不复制 LangGraph 的 `State`、`Command` 或
特定 Runnable 层次。Bricks 使用 `Context` 表达运行数据，使用 `Outcome` 表达运行
意图。

| 编排能力 | Bricks 对应 | 当前边界 |
| --- | --- | --- |
| 图定义 | `GraphBuilder` / `Graph` | 已支持不可变图和结构校验 |
| 图检查/可视化 | `Graph.describe()` / `to_mermaid()` / `to_dot()` / 静态分析函数 | 已支持可达性、死端、环、终止覆盖和迁移冲突检查 |
| 节点 | `BaseNode` / `ActionNode` | 可通过继承扩展 |
| 普通边 | `Transition` | 事件驱动 |
| 条件边 | 多条 `Transition` + `Guard` | 按优先级和 Guard 选择；支持异步 Guard |
| 状态/共享数据 | `Context` | 不单独引入 `State` |
| 节点数据更新 | Action 返回映射 / `Outcome.update()` / `Context.update()` | 更新与控制流可以通过同一 Action 组合 |
| 动态路由 | `Outcome.next(event, update=...)` + Guard | 先更新 Context，再经过图上的迁移，不直接跳转节点 |
| 起点 | `Graph.initial` + `Machine.start()` | 已支持 |
| 终点 | `TerminalNode` + `Status.COMPLETED` | 已支持 |
| 子图复用 | `GraphBuilder.include()` / `GraphBuilder.subgraph()` | 静态组合和单分支运行时子图 |
| 动态 Fan-out | `Outcome.fork()` / `ForkBranch` | 可从 Context 动态生成分支；子运行独立 Context，Join payload 提供分支快照 |
| Fan-out 汇聚 | `join()` + Join 事件 | `all/any` 成功策略和 `failure_policy`；聚合规则由父图决定 |
| 中断/人工等待 | `Outcome.wait()` / `Outcome.interrupt()` + `resume()` | WakeupScheduler 外部定时端口 |
| 检查点 | `ContextSnapshot` + `PersistenceBinding` | 存储由协议提供 |
| 检查点历史 | 可选 `read_history()` + `PersistenceBinding.history()` | 按提交顺序查询快照，不改变运行 |
| 事件历史 | `EventLog` + `replay_events()` | 记录事实，重放必须显式调用 |
| 重试 | `RetryPolicy` + `Outcome.retry()`，可选 `retry_on` | 默认显式重试；匹配的节点异常可选择自动进入 Retry |
| 错误结果 | `Outcome.fail()` / Hook / 原始异常 | 不隐式把所有异常转换成图迁移 |
| 事件驱动 | `EventBus` + `ReactiveRuntime` | 已支持同步/异步路由 |
| 生命周期监听 | `HookRegistry` | 已支持同步/异步 Hook |
| 并行分支 | `Fork` / `Join` | 异步分支并发启动并支持 `max_concurrency`；同步分支顺序执行；可配置失败处理 |
| 补偿流程 | `SagaRuntime` | 已支持逆序补偿 |
| 运行流 | `Machine.stream()` / `Machine.astream()` | 每个外部事件产出一个结果 |
| 运行事件 | `stream_events()` / `astream_events()` | 生命周期、迁移、Emit、错误和父子运行关系；异步支持增量输出 |
| 远程执行 | `ActionExecutor` 协议 | 具体执行器不在核心 |
| 运行身份 | `Context.run_id` / `parent_run_id` | 支持嵌套路由、快照和事件追踪 |

## 有意保持的差异

- `Context` 是可变的运行上下文，避免同时维护 State、Config、Command 等重复模型。
- Action 可以直接更新 `Context.data`，控制流通过 `Outcome` 和 Graph 迁移表达。
- Fork 分支结果不会自动写入父 Context；父图通过 Join 事件 payload 明确决定如何合并，
  避免提前引入隐式 reducer 和领域数据规则。
- 快照和事件日志是组合能力，不自动把运行变成事件溯源系统。
- 调度器、队列、远程执行和领域对象保持在引擎之外。

## 尚未进入核心的能力

- 分布式调度、队列投递和跨进程锁。
- 外部执行器的具体线程、进程、RPC 实现。
- 自动 reducer、隐式状态合并和事件溯源事务。
- 领域级错误恢复、工具调用、LLM、Spider 或 Agent 规划。

这些不是 Graph 编排模型的缺失，而是应在引擎外部由调度器、持久化实现或领域适配器
按场景提供的能力。
