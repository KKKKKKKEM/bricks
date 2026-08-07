# 变更记录

## Unreleased

- 增加不可变 `OutcomeRegistry` 和最小 `OutcomeInterpreter` 协议；Machine 只消费
  `CONTINUE/STOP` 指令，领域 Effect/Control 不再要求修改核心类型分支。旧的
  `outcome_handlers` 参数保留为停止型 Control 的兼容入口。
- 迁移阶段改为顺序提交：`Update/Emit` 等 Effect 立即生效，Control 立即停止后续阶段；
  同步和异步路径共享同一个 Transition frame 状态模型。
- 新 Outcome handler 改用窄 `OutcomeRuntime` 能力端口；Retry 解释移出 Machine，旧
  `outcome_handlers` 保持 Machine handler 兼容。
- 增加 WakeupScheduler 同步/异步端口、恢复协调和稳定唤醒请求，不在核心创建定时线程。
- 增加 AtomicCommitStore、AtomicPersistenceBinding 和 StagedEffect Outbox，使快照、执行
  事实和领域副作用意图可以在一个外部存储事务中提交；Fork 注册前的子运行事实会同批保存
  子快照，异步提交按绑定器串行化。
- ForkController 抽象为可注入 `ForkRuntime`，factory 自动传递给子运行；增加 Spider
  控制面组合示例。
- Wakeup 投递校验图版本和完整等待身份；内存调度器使用非破坏性、至少一次到期轮询。
- Guard 求值和迁移选择从 Graph 移到可替换的 `TransitionSelector`，Graph 不再依赖运行时
  Event 或同步/异步执行细节。
- Graph 公开构造、Context 恢复和 ContextSnapshot 增加强不变量校验；Graph metadata、
  SubGraph data、Event 和 RuntimeEvent payload 改为递归只读结构。
- ReactiveRuntime 增加按 `run_id` 定向的 `route()`；未定向事件匹配多个运行时明确拒绝，
  防止多实例串任务。
- 增加 `AsyncSnapshotStore`、`AsyncEventLog` 和 `AsyncPersistenceBinding`，避免异步 Machine
  在事件循环内执行同步持久化 I/O。
- BaseNode 默认配置改为 keyword-only 字段，自定义 dataclass 节点可以声明必填字段；
  修复 PEP 561 静态类型错误。
- 增加命名空间 Graph 组合、结构描述、Mermaid 输出和可达性分析。
- 增加 `Machine.invoke()`、`stream()`、`Outcome.interrupt()` 和运行时子图。
- 子图支持独立 Context、Join 返回事件和带 `graph_resolver` 的快照恢复。
- 异步 Fork 分支并发启动，分支失败时清理未完成的兄弟任务。
- 增加带版本的 Graph 结构化序列化，以及 Action、Guard、自定义 Node 和子图的显式解析协议。
- 增加 Fork 异步并发上限、父级取消传播、成功优先的 `any` Join，以及运行事件流。
- Action 支持返回映射更新 `Context.data`；EventLog 增加追踪字段、显式重放和快照 revision/CAS。
- 增加嵌套 Fork/Join 的运行 ID 路由、嵌套快照恢复和父级持久化传播。
- 增加 RuntimeEvent 的 `match` 筛选、图环/终止覆盖/迁移冲突分析，以及外部执行器边界文档和示例。
- 增加 Fork 的 `failure_policy`：默认失败、继续汇聚和快速失败三种分支失败语义。
- `Outcome.next()` 支持携带 Context 增量更新；增加动态 Fan-out / Join 示例。
- 明确成功迁移后的 Hook 异常不会释放已消费事件的幂等键。
- Event 和 RuntimeEvent 的 payload 边界增加深复制隔离。
- PersistenceBinding 在用户观察 Hook 之前保存提交快照，并覆盖无返回事件的 Fork Join。
- 收敛 `SnapshotStore` 为 `save/load/delete` 最小协议，revision/CAS 改为可选能力。
- 增加无 CAS 外部存储示例，以及 SnapshotStore、EventLog 和 IdempotencyStore 契约测试。
- 增加无第三方依赖的 `Graph.to_dot()` 图结构输出和可运行可视化示例。
- `RetryPolicy` 增加可选 `retry_on`，支持节点异常进入已有 Retry/恢复生命周期。
- 内存快照存储增加可选历史查询，并提供 `PersistenceBinding.history()` 和对应示例。
- 增加 `Machine.update_context()` / `update_context_async()`，以 `Context` 承载受控的控制面
  更新；更新触发 `context.updated` Hook，并由 `PersistenceBinding` 记录为
  `context_update` 事实，但不会自动推动图迁移。
- Graph 支持异步 Guard；同步入口明确拒绝异步 Guard，异步入口会等待普通 Guard 组合器。
- 修正父级持久化绑定在 Fork 创建阶段的后代事件归属，子运行启动和初始事件现在可以按
  `run_id` 独立查询和重放。
- 修正异步 Machine 对只覆盖同步 `BaseNode.enter()` / `exit()` 的自定义节点扩展支持。
- 修复边界运行契约：Guard 接收完整 Event，等待恢复解析失败保持 `WAITING`，迁移前异步
  Task 取消进入 `STOPPED`，节点退出 Outcome 不被空迁移动作覆盖，终止节点的 `Next` 会继续
  执行。
- 修复图与持久化一致性：拒绝重复节点 ID，Workflow 将隐式目标视为自环，无返回事件 Join
  写入可重放事实，EventLog 读取返回隔离副本，SubGraphNode 数据冻结，并让 Context 快照
  继承 Graph definition version。

## 0.3.0

当前分支版本，仍处于 Alpha 阶段。

### 核心

- Bricks 重构为领域无关的图执行引擎。
- 固定 `GraphBuilder`、`Graph`、`Machine`、`Context`、`Event` 和 `Outcome` 的核心关系。
- 使用 `Context` 承载运行位置、生命周期、业务数据和恢复信息，不再保留旧版 `State` API。
- 节点通过 `BaseNode`、`enter()` 和 `exit()` 继承扩展。

### 通用能力

- 增加 EventBus、生命周期 Hook 和响应式事件路由。
- 增加 Wait、Retry、Fork/Join、Stop、Fail 和 Emit Outcome。
- 增加取消、超时、重试和幂等策略。
- 增加带版本和 Graph 身份校验的快照，以及追加式 EventLog。
- 增加 Workflow、Reactive、Parallel 和 Saga 组合语义。

### 边界

- 旧版 Spider、下载器、队列、RPC 和兼容 API 不属于当前分支实现。
- 当前 Fork/Join 是进程内组合语义，不提供分布式调度。
- EventLog 不自动重放事件，也不替外部副作用提供事务。
- 领域适配器、Agent、LLM、工具和记忆系统暂不进入 `bricks/engine`。
