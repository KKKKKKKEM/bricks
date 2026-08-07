# Bricks

Bricks 正在重新构建为一个通用的图执行引擎。

它不以爬虫、Agent 或某一种业务为核心，而是提供一套可以组合的基础能力：

```text
图定义 → 运行上下文 → 事件迁移 → 可替换执行器 → 快照与历史
```

未来的工作流、自动化任务、爬虫、Agent、ETL 和 RPC 编排，都可以作为适配器建立在这套引擎之上。

## 当前状态

项目目前处于重构早期阶段，当前分支只保留通用引擎的新架构，不再维护旧版 Spider 实现。

核心目录：

```text
bricks/engine/
├── graph/        # 不可变图定义、节点、迁移和校验
├── runtime/      # 一次运行、上下文、Outcome 和生命周期
├── events/       # EventBus、消息和生命周期 Hook
├── persistence/  # 快照与事件日志协议
├── scheduling/   # 外部定时唤醒协议
├── policies/     # 重试、超时、取消和幂等策略
└── semantics/    # 工作流、响应式、并行和补偿语义
```

当前暂不创建领域适配器目录。核心稳定后，再按需新增 Spider、Agent、ETL 等适配器。

## 最小示例

```python
from bricks.engine import GraphBuilder, Machine


builder = GraphBuilder("approval", initial="draft")
builder.action("draft")
builder.terminal("approved")
builder.transition("draft", "approve", "approved")

machine = Machine(builder.build())
machine.start()
machine.dispatch("approve")

assert machine.status.value == "completed"
```

更多设计说明见 [`docs/README.md`](docs/README.md)，核心运行说明见
[`docs/graph_engine.md`](docs/graph_engine.md)，各模块职责见
[`docs/module_map.md`](docs/module_map.md)，总体开发原则见
[`docs/design_principles.md`](docs/design_principles.md)，0.3 行为契约见
[`docs/reference/behavior.md`](docs/reference/behavior.md)，变更记录见
[`CHANGELOG.md`](CHANGELOG.md)。

需要从设计思路一路读到生产接入和源码实现时，使用
[`docs/handbook/README.md`](docs/handbook/README.md)。手册覆盖迁移流水线、状态提交边界、
全部核心组件、持久化与 Outbox、Wakeup、Fork/Join，以及完整使用和扩展示例。

当前引擎已经包含：

- `Machine` 的同步/异步事件迁移、等待、重试、内部 `Next` 和 `Fork/Join`，以及分支失败
  的继续汇聚和快速失败策略。
- `Machine.invoke()`、`stream()`、`Outcome.interrupt()` 以及命名空间图组合。
- `Machine.stream_events()` / `astream_events()` 运行事件流，以及 Action 返回映射更新
  `Context.data` 的能力。
- 不可变 `OutcomeRegistry` 将 Effect/Control 解释与 Machine 执行循环分离，领域框架可以
  增加 Tool、Memory、HumanInput 等 Outcome，而不修改核心分支。
- 支持异步 Guard；同步入口会明确拒绝异步条件，异步入口会等待并继续执行迁移。
- `EventBus`、生命周期 Hook，以及 `ReactiveRuntime` 事件路由。
- `ContextSnapshot`、可选 revision/CAS 的 `SnapshotStore`、带追踪字段的 `EventLog`，并支持通过
  `PersistenceBinding` 保存、恢复和显式重放。
- 内存快照存储还提供可选历史查询，外部存储只需实现最小 `save/load/delete` 协议即可接入。
- 可选 AtomicCommit/Outbox 将快照、事件事实和领域副作用意图作为一个存储事务提交；
  WakeupScheduler 为 Wait/Retry 提供不依赖后台线程的外部定时端口。
- `Workflow`、`ParallelPlan`、`SagaRuntime` 等建立在核心运行时之上的组合语义。

这些语义都不依赖具体领域、HTTP、数据库或消息队列；后续领域能力应作为独立适配器，
通过 Graph、ActionExecutor、EventBus 和持久化协议接入。

## 开发

```bash
uv run --with pytest python -m pytest -q
```

项目使用 MIT License。
