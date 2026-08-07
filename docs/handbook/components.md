# 核心组件与源码导读

本章按源码目录解释组件职责、公开扩展点和内部实现。应用代码通常不需要直接使用所有
对象，但理解它们有助于判断功能应该放在哪一层。

## 1. 顶层公开入口

`bricks.engine` 只导出运行一张图所需的最小对象：

```python
from bricks.engine import Context, Event, Graph, GraphBuilder, Machine, Outcome, Status
```

扩展能力从对应包导入，避免顶层命名空间失控：

```python
from bricks.engine.events import EventBus, HookRegistry
from bricks.engine.persistence import PersistenceBinding
from bricks.engine.policies import RetryPolicy
from bricks.engine.scheduling import WakeupBinding
from bricks.engine.semantics import ReactiveRuntime, Workflow
```

## 2. `graph`：静态定义层

### 2.1 GraphBuilder

`GraphBuilder` 保存可变的节点字典、迁移列表、迁移 ID 集合和递增声明顺序。

| 方法 | 创建内容 |
| --- | --- |
| `node()` / `add_node()` | BaseNode 或自定义节点 |
| `action()` | ActionNode |
| `wait()` | WaitNode |
| `terminal()` | TerminalNode |
| `subgraph()` | SubGraphNode |
| `transition()` | Transition |
| `include()` | 将另一张 Graph 静态复制进命名空间 |
| `build()` | Graph 并执行 `assert_valid()` |

`include(graph, prefix=...)` 是静态组合：节点和迁移 ID 增加 `<prefix>.`，Action、Guard 和
metadata 复用。它和运行时 SubGraph 不同；include 后仍只有一台 Machine 和一个 Context。

### 2.2 Graph

Graph 构造时执行基础不变量校验并建立迁移索引。`transitions_from(source, event)` 不扫描
全图，而是从只读索引返回按优先级和声明顺序排序的候选元组。

Graph 还提供三类只读输出：

- `describe()`：不包含可执行对象的结构摘要；
- `to_mermaid()`：生成 Mermaid 文本；
- `to_dot()`：生成 Graphviz DOT 文本。

### 2.3 节点类型

所有节点都是 frozen dataclass，并通过统一 `enter/exit` 协议执行：

| 类型 | enter 行为 | terminal |
| --- | --- | --- |
| `BaseNode` | 执行可选 action | 可配置 |
| `ActionNode` | 与 BaseNode 相同，提供语义名称 | False |
| `WaitNode` | Action 没有返回值时自动返回 Wait | False |
| `TerminalNode` | 执行 Action 后由 Machine 完成 | True |
| `SubGraphNode` | Action 没有返回值时自动返回单分支 SubGraph Fork | True |

节点的 `on_exit` 在离开源节点时执行。自定义节点应覆盖 `enter()` 和 `enter_async()`，并让
同步/异步行为保持一致。

### 2.4 Transition 和 Guard

Transition 自身只保存声明。选择逻辑位于 `DefaultTransitionSelector`：

1. 获取当前 `(node_id, event.name)` 的候选边；
2. 按 priority 从小到大、order 从小到大处理；
3. 无 Guard 的第一条边直接命中；
4. 有 Guard 时选择第一条返回真值的边；
5. 全部不匹配则返回 None，由 Machine 抛 `NoTransition`。

Guard 可以是普通 callable，也可以使用 `Predicate`、`AllOf`、`AnyOf`、`Not` 组合。组合器
支持同步和异步短路求值。

### 2.5 静态校验

`validate.py` 提供：

- `validate_graph()` / `assert_valid()`；
- `reachable_nodes()` / `unreachable_nodes()`；
- `terminal_nodes()` / `dead_end_nodes()`；
- `cycle_nodes()`，内部使用 Tarjan 强连通分量算法；
- `non_terminating_nodes()`；
- `transition_conflicts()`。

构建错误与诊断提示分开：不存在的节点等不变量会阻止 build；有意等待外部事件的死端
可以作为分析结果保留，不必强制当作错误。

### 2.6 Graph 序列化

`Graph.to_dict()` 输出 schema version 1。Python callable 不会被自动写成导入路径；必须
显式传入 `action_serializer`、`guard_serializer`，恢复时提供对应 resolver。自定义节点
使用 `node_serializer/node_resolver`，SubGraph 使用 `graph_resolver`。

这种设计避免反序列化任意模块路径，也让注册名、版本迁移和部署环境由应用控制。

## 3. `runtime`：动态执行层

### 3.1 Context

Context 是可变 dataclass。`snapshot()` 深复制业务数据、metadata、waiting 和 Event；
`from_snapshot()` 重建运行状态。Graph 和 callable 从不进入 Context。

直接修改 `context.data` 可以工作，但不会触发持久化 Hook。控制面或人工输入应该调用
`machine.update_context()`，使更新获得 `__context_update__` Event 和 `context.updated` Hook。

### 3.2 Machine

Machine 组合以下依赖：

| 构造参数 | 默认值 | 作用 |
| --- | --- | --- |
| `executor` | InlineExecutor | 调用 Action |
| `events` | EventBus | 发布 Emit |
| `hooks` | HookRegistry | 生命周期扩展 |
| `retry_policy` | RetryPolicy | Retry 次数和退避 |
| `cancellation` | None | 协作式取消 |
| `timeout` | None | Action 超时策略 |
| `idempotency` | None | Event ID claim/release |
| `graph_resolver` | None | 恢复异构子图 |
| `selector` | DefaultTransitionSelector | 选择迁移 |
| `outcome_interpreter` | 默认 Registry | 解释 Outcome |
| `fork_runtime_factory` | ForkController | Fork 实现 |
| `clock` | UTC now | Wait/Retry 时间来源 |
| `max_internal_steps` | 100 | Next 安全上限 |

公开入口按用途分组：

- 生命周期：`start`、`pause`、`resume_run`；
- 事件：`dispatch`、`resume`、`resume_retry`；
- 并行：`route`、`join`；
- 批量：`invoke`、`stream`；
- 观测：`stream_events`；
- 恢复：`snapshot`、`from_snapshot`；
- 每个需要等待 I/O 的入口都有异步对应方法。

### 3.3 TransitionResult 和内部 frame

`TransitionResult` 是一次迁移的不可变摘要，包含 Event、source、target、Transition、最后
一个 Outcome、提交时 Status 和可能的 children。

内部 `_TransitionFrame` 跟踪声明目标、实际目标、最后 Outcome 和是否被 STOP 指令截断。
这让 exit、transition action、enter 三个阶段共用一套状态推进逻辑。

### 3.4 OutcomeInterpreter

`OutcomeRegistry` 是不可变的类型到 OutcomeRule 映射。解析时沿 Outcome 类型的 MRO 查找
第一条规则，所以可以为基类提供通用处理，也可以为具体子类覆盖。

`with_handler()` 返回新 Registry，不修改共享默认实例。Outcome handler 只得到
`OutcomeRuntime` 窄端口，而不是完整 Machine。

### 3.5 OutcomeRuntime 和 StagedEffect

OutcomeRuntime 暴露：

```text
只读 context / run_id
update / publish / stage_effect
wait / retry / stop / fail
start_fork
```

它故意不暴露 dispatch、join、snapshot 和 Hook 注册，避免领域 handler 重入 Machine 或
绕过 Graph。

`ContextView` 为 data、metadata、waiting 返回只读映射。`StagedEffect` 是冻结的 Outbox
意图，包含 effect ID、run ID、topic、payload 和 created_at。

### 3.6 ActionExecutor 和策略包装

`InlineExecutor` 同步调用 callable；如果同步入口得到 awaitable，会关闭 coroutine 并抛
`AsyncActionRequired`。异步入口既兼容同步返回值，也会 await 异步值。

`PolicyExecutor` 包装基础 executor，在 Action 前后检查 CancellationToken，并应用
TimeoutPolicy。自定义远程执行器仍需实现相同 `execute/execute_async` 协议。

## 4. `events`：领域事件和生命周期

### 4.1 EventBus

EventBus 是线程安全的进程内有序总线：

- 每个监听器有 priority、注册 order、once 和 match；
- 精确事件和 `*` 监听器合并后统一排序；
- `publish()` 会先预检已知异步 callable，防止同步监听器先产生部分副作用；
- `publish_async()` 兼容同步和异步监听器；
- Subscription token 用于幂等解除订阅。

EventBus 本身不知道 Machine。ReactiveRuntime 使用内部路由登记表解决多个运行共享总线
时的歧义。

### 4.2 HookRegistry

HookRegistry 与 EventBus 分离。HookContext 包含 machine、context、event、transition、
result、error 和对应 RuntimeEvent。

Hook priority 越小越先执行。持久化使用 -1000，Wakeup 使用 -900，默认业务 Hook 使用 0，
所以一次状态变化通常先落存储、再同步调度器、最后通知普通观察者。

### 4.3 RuntimeEvent

RuntimeEvent 是冻结后的观察事实，不携带可执行对象。Machine 为每个生命周期点递增
sequence，并把 sequence 写回 metadata，使快照恢复后继续递增。

完整 LifecycleEvent 名称如下：

| 阶段 | Hook 名称 |
| --- | --- |
| 启动 | `machine.before_start`、`machine.after_start` |
| 派发 | `machine.before_dispatch`、`machine.after_dispatch` |
| 迁移 | `transition.before`、`transition.after`、`transition.error` |
| 节点 | `node.enter`、`node.exit` |
| 事件 | `event.unhandled`、`event.emitted` |
| 暂停恢复 | `machine.paused`、`machine.resumed`、`machine.after_resume` |
| 并行 | `machine.after_join` |
| 控制面更新 | `context.updated` |

## 5. `policies`：执行策略

### 5.1 RetryPolicy

配置项包括 `max_attempts`、基础 `backoff`、`exponential` 和可重试异常类型。

- `allows(attempt)` 判断是否还能增加一次尝试；
- `delay_for(attempt)` 计算线性常量或指数退避；
- `matches(error)` 判断节点异常是否应转换为 Retry。

节点 Action 抛出匹配异常时，Machine 会把异常类型和消息写入 Retry reason，而不是直接
进入 FAILED。

### 5.2 CancellationToken

Token 同时提供同步检查和 `wait_async()`。取消是协作式的：Machine、PolicyExecutor 和异步
Fork 在明确检查点响应；它不是任意 Python 代码的强制终止器。

### 5.3 TimeoutPolicy

异步实现可通过 asyncio 超时取消等待；同步实现测量耗时并在 Action 返回后检查，不能安全
抢占正在运行的线程。

### 5.4 IdempotencyStore

Machine 使用 `<run_id>:<event_id>` 作为 claim key。提交前失败会 release；迁移提交后即使
after Hook 失败也保留 claim。生产实现应提供跨进程原子 claim，而不是进程内 set。

## 6. `semantics`：组合外观

这些类不创建第二套执行引擎：

- `Workflow` 负责创建 Machine、初始化 data、批量运行和 DAG 拓扑序；
- `ReactiveRuntime` 把 EventBus 消息映射到 dispatch/resume；
- `ParallelPlan` 只是生成 Fork Outcome 的声明式包装；
- `SagaRuntime` 从成功的 `transition.after` metadata 收集补偿 Action；
- `CompensationPlan` 逆序执行补偿，单步失败不会阻止其它补偿。

## 7. 异常体系

| 异常 | 常见原因 |
| --- | --- |
| `GraphValidationError` | 图结构不合法 |
| `GraphSerializationError` | 缺少 callable resolver 或 schema 不匹配 |
| `MachineNotStarted` | start 前 dispatch |
| `MachineNotRunnable` | 状态或恢复入口不正确 |
| `NoTransition` | 当前节点没有符合 Guard 的事件边 |
| `AsyncActionRequired` | 同步入口遇到异步 Action/Hook/listener |
| `AsyncGuardRequired` | 同步入口遇到异步 Guard |
| `DuplicateEvent` | event ID 已被当前运行消费 |
| `InternalStepLimitExceeded` | Next 连续步数超限 |
| `SnapshotConflictError` | snapshot revision CAS 失败 |
| `PersistenceError` | 存储、日志或 AtomicCommit 协议失败 |

应用应针对可恢复异常制定外部重试策略，但不能把 DuplicateEvent 或 CAS 冲突简单当作“再跑
一次业务 Action”。

## 8. 源文件责任索引

| 文件 | 主要责任 |
| --- | --- |
| `graph/builder.py` | 构图 DSL |
| `graph/graph.py` | 冻结图、索引、描述和可视化 |
| `graph/nodes.py` | 节点协议和内建节点 |
| `graph/guards.py` | Guard 组合器 |
| `graph/serialization.py` | 安全的显式 callable 引用协议 |
| `graph/validate.py` | 静态分析 |
| `runtime/machine.py` | 状态机、迁移流水线、恢复入口和观测 |
| `runtime/interpreter.py` | Outcome 类型分派 |
| `runtime/outcome_runtime.py` | Outcome 窄能力实现 |
| `runtime/fork.py` | 子 Machine、Join、嵌套路由和恢复 |
| `runtime/executor.py` | Action 调用边界 |
| `events/bus.py` | 领域消息分发 |
| `events/hooks.py` | 生命周期扩展 |
| `persistence/binding.py` | 普通快照和日志 Hook 绑定 |
| `persistence/atomic.py` | 原子快照、事实和 Outbox |
| `scheduling/wakeup.py` | 持久化唤醒请求 |
| `semantics/reactive.py` | EventBus 到 Machine 的路由适配 |
