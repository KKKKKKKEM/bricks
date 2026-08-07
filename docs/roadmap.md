# Bricks 后续路线

当前目标是先把通用图编排内核做稳，而不是马上扩展领域能力。当前已经形成一条可
运行、可测试、可恢复的最小链路，后续继续围绕协议边界推进。

## 基线：已完成

- 固定 `Graph / Context / Event / Machine / Outcome` 的关系。
- 固定 `BaseNode` 的继承扩展方式和 Graph 的不可变边界。
- 完善 EventBus、Hook、响应式路由和生命周期契约。
- 接入取消、超时、重试和幂等策略。
- 明确快照版本、Graph 身份校验和事件日志边界。
- 稳定 Workflow、Reactive、Parallel、Saga 的薄组合语义。
- 建立 guides、reference、decisions 文档和行为测试。
- 冻结 0.3 的同步/异步、生命周期、策略、Hook 和恢复行为契约。

## 已完成：0.3 行为契约

这一轮已经完成：

- 通过 `py.typed` 发布包类型信息。
- 在 `docs/reference/behavior.md` 固定同步、异步、生命周期和恢复行为。
- 在 `CHANGELOG.md` 记录 0.3.0 的核心范围和明确不保证的能力。
- 用契约测试保护顶层 API、生命周期异常和 Hook 错误边界。

后续接口变更必须同时更新行为矩阵、变更记录和测试。

## 进行中：增强图建模能力

本轮已经完成第一批基础能力：

- `GraphBuilder.include()` 静态命名空间组合。
- `Graph.describe()`、`Graph.to_mermaid()` 和 `Graph.to_dot()` 结构输出。
- `reachable_nodes()` / `unreachable_nodes()` 可达性分析。
- `invoke()`、`stream()` 和 `Outcome.interrupt()` 运行编排入口。
- `SubGraphNode` 的独立 Graph/Context、返回事件和快照恢复。
- `Graph.to_dict()` / `Graph.from_dict()` 的版本化结构定义，以及 Action、Guard、自定义
  Node 和子图的显式解析协议。
- `stream_events()` / `astream_events()` 的统一运行事件流和父子运行关联字段。
- `terminal_nodes()` / `dead_end_nodes()` 的轻量静态分析。
- `cycle_nodes()`、`non_terminating_nodes()` 和 `transition_conflicts()` 的发布前分析。
- Action 返回映射更新 `Context.data` 的 `Update` 语义。
- 异步 Task 取消时的停止语义、快照 revision/CAS 和显式 `replay_events()`。
- 事件流 `match` 筛选、嵌套运行路由和父级持久化绑定传播。
- Retry 恢复完成 Hook、Retry 事实重放和 Context 嵌套数据快照隔离。
- `RetryPolicy.retry_on` 显式接入节点异常重试；默认仍保持异常直接失败的行为。
- 外部消息转换、进程重启后的响应式恢复，以及动态 Fan-out / Join 汇聚示例。
- `Machine.update_context()` / `update_context_async()` 控制面更新、`context.updated` Hook、
  `context_update` 事件事实和持久化绑定。
- `ActionExecutor` 的同步/异步调用边界、节点进入/退出/迁移动作覆盖、外部异常、取消、
  超时和 Fork 子运行契约测试与示例。
- 异步 Guard 及 `AllOf`、`AnyOf`、`Not`、`Predicate` 的异步组合。
- 修正 Fork 创建阶段后代事件日志归属，使子运行日志可以按 `run_id` 独立重放。
- 修复 Guard 完整 Event 传递、等待恢复失败回滚、迁移前异步取消和终止节点 Next 链的
  运行一致性边界。
- 修复无返回事件 Join 的可重放事实、EventLog 读取隔离、重复节点 ID、Workflow 隐式自环、
  SubGraphNode 静态数据冻结和 Context 快照 Graph 版本继承。

本轮补充了 `Graph.to_dot()`；`Graph.describe()`、Mermaid 和 DOT 共同作为图编辑器与
检查器的无第三方依赖结构出口。

后续只在真实用例出现后添加更复杂的条件分支、循环、Join 和 Builder 组合 API，
不把领域知识带入 `Machine`。

这些能力仍然只属于图定义层，不把领域知识带进 `Machine`。

## 下一步：执行与并发边界

当前 Fork/Join 已支持进程内同步/异步语义，并已通过 `ForkRuntime` / factory 固定外部
协调边界，但不等同于已经提供分布式调度服务：

- `Fork` 分支的并发执行和资源限制（当前已支持进程内异步 `max_concurrency`）。
- 线程、协程、进程或远程执行器的生命周期。
- 取消和超时传播的更多外部执行器映射；Join 已支持默认失败、继续汇聚和快速失败，
  其中异步快速失败可以取消已运行的兄弟分支。
- WakeupScheduler 已固定定时请求协议；队列、远程 Fork 实现和指标仍作为引擎外部基础设施。

执行器边界已经通过真实示例和契约测试固定：执行器只负责一次调用，不在核心里引入调度器、
队列或全能运行时管理器。后续只在出现新的外部执行模型时补充独立适配器。

不为这些能力提前创建全能的 `RuntimeManager` 或 `SchedulerManager`。

## 下一步：可观测性与可靠恢复增强

- 已将 RuntimeEvent 的稳定追踪字段接入 Hook 和 EventLog；后续补充外部追踪适配。
- `SnapshotStore` 已收敛为 `save/load/delete` 最小协议；revision/CAS 作为可选结构能力，
  并由 `examples/custom_store.py` 和协议契约测试覆盖。
- 快照存储增加可选 `read_history()`，通过 `PersistenceBinding.history()` 提供按提交顺序
  的运行历史查询，并由 `examples/snapshot_history.py` 覆盖。
- 已提供显式事件重放工具；后续验证更多等待、子图和副作用场景。
- 继续验证 Fork/Join、控制面 `Context` 更新、快照历史之间的一致性。
- 明确 `context_update`、Retry、Fork/Join 事实在 `EventLog` 中的回放边界；控制面记录当前
  只审计、不自动重放。
- AtomicCommitStore 与 StagedEffect Outbox 已固定可靠副作用提交边界；后续增加真实 SQL
  适配器和 Worker 租约实现。
- 快照历史可以直接交给 `Machine.from_snapshot()` 创建独立检查运行；后续只在真实需求
  出现后增加更高层的时间旅行入口。

## 下一步：图工具和流模式

- 已提供环、分支冲突、终止覆盖等静态分析函数，以及 JSON、Mermaid、DOT 三种结构出口。
- 已在不破坏 `stream()` 简洁契约的前提下提供按 RuntimeEvent 筛选的流模式。
- 后续只在实际编辑器需求出现时增加字段，并通过 schema 版本管理格式变化。

## 接下来按顺序推进

1. 完成执行器边界、取消/超时和持久化组合的契约测试。
2. 完成控制面更新与 Fork/Join、快照历史、EventLog 回放之间的行为矩阵。
3. 收敛核心公开接口和文档；只有真实用例需要时，才评估 Context 合并/reducer 等新抽象。
4. 核心契约稳定后，再以独立适配器开始 Spider、Agent、ETL 或 Browser 的实验性实现。

## 最后才做领域适配器

当引擎契约稳定、并且出现明确场景时，再在引擎之外增加：

```text
adapters/
├── spider/
├── agent/
├── etl/
├── browser/
└── rpc/
```

Agent、Spider、LLM、工具、记忆和规划器都不进入 `bricks/engine`。它们只通过 Graph、
Context、Action、Event、Hook、执行器和持久化协议接入。
