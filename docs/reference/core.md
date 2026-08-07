# 核心 API

这份文档只描述当前稳定的核心入口。扩展模块有自己的文档，不会全部重新导出到
`bricks` 顶层。

同步、异步、生命周期和恢复的精确行为见 [behavior.md](behavior.md)。

## 导入

```python
from bricks import Context, Event, Graph, GraphBuilder, Machine, Outcome, Status
```

核心对象的关系是：

```text
GraphBuilder -> Graph -> Machine -> Context
                              ^
                           Event / Outcome
```

## `GraphBuilder`

`GraphBuilder` 是可变的图构建器。它负责添加节点和迁移，`build()` 时执行校验并生成
可复用的 `Graph`。可以通过 `version` 标识流程定义版本，用于快照恢复时的兼容校验。

```python
builder = GraphBuilder("approval", initial="draft")
builder.action("draft")
builder.terminal("approved")
builder.transition("draft", "approve", "approved")
graph = builder.build()
```

常用方法：

- `action(node_id, action=None, on_exit=None)`：添加进入时可执行 Action 的节点。
- `wait(node_id, delay=None, resume_event=None)`：添加等待节点。
- `terminal(node_id, action=None)`：添加终止节点。
- `transition(source, event, target, guard=None, action=None, priority=0)`：添加事件迁移。
- `add_node(node)`：加入 `BaseNode` 或其子类实例。

## `Graph`

`Graph` 是经过校验的不可变定义，可以被多个 `Machine` 共享。它保存节点、迁移和
初始节点，不保存某次运行的业务数据或生命周期。

同一源节点和事件有多条迁移时，先按 `priority` 升序选择，再按声明顺序选择；带有
Guard 的迁移只有在 Guard 返回真时才可用。

## `Machine`

`Machine` 表示一张图的一次运行：

```python
machine = Machine(graph)
machine.start()
machine.dispatch("approve")
assert machine.status is Status.COMPLETED
```

公开生命周期入口：

- `start()` / `start_async()`：从 `Graph.initial` 开始运行。
- `dispatch(event, payload=None)` / `dispatch_async(...)`：消费一个事件并执行迁移。
- `invoke(events)` / `ainvoke(events)`：启动并按顺序消费一组事件，返回最终 Context。
- `stream(events)` / `astream(events)`：启动并按顺序产出每个外部事件的 `TransitionResult`。
- `stream_events(events, match=None)` / `astream_events(events, match=None)`：产出生命周期和
  迁移级 `RuntimeEvent`，用于追踪和调试；可用一个接收 `RuntimeEvent` 的谓词筛选输出，
  不改变 `stream()` 的简单结果契约。
- `resume(event, payload=None)` / `resume_async(...)`：恢复 `Wait` 等待的事件。
- `resume_retry(payload=None, *, event=None)` / `resume_retry_async(...)`：恢复 `Retry`
  等待；默认使用 `Outcome.retry(event=...)` 声明的事件，外部调度器可传入带稳定 ID 的 Event。
- `join(run_id=None)` / `join_async(...)`：完成当前 Fork；传入后代运行 ID 时先完成
  指定子运行自己的 Fork。
- `route(run_id, event, payload=None)` / `route_async(...)`：按运行 ID 将事件交给当前
  运行或嵌套子运行，仍然经过目标 Graph 的等待、Guard 和迁移规则。
- `pause()` / `resume_run()`：暂停或恢复同步运行；异步 Hook 或异步持久化绑定使用
  `pause_async()` / `resume_run_async()`。
- `update_context(values=None, **kwargs)` / `update_context_async(...)`：通过控制面合并
  `Context.data`，触发 `context.updated` Hook，但不触发 Graph 迁移。
- `snapshot()` / `from_snapshot(...)`：导出或恢复运行快照。

`Machine` 默认使用进程内 `InlineExecutor`。可以通过 `executor`、`events`、`hooks`、
`retry_policy`、`graph_resolver`、`selector`、`outcome_interpreter`、
`fork_runtime_factory` 和策略对象接入替换
能力，但 Machine 不直接依赖数据库、队列或远程服务。`graph_resolver` 只在恢复包含其它
Graph 的运行时子图时需要。

领域框架可以声明自己的 `Outcome` 子类，并通过不可变的 `OutcomeRegistry.with_handler()`
注册解释器。Fork 创建的子运行会继承同一个解释器。旧的 `outcome_handlers` 参数继续兼容，
其规则默认作为停止型 Control。`Update` 和 `Emit` 在各自产生阶段
立即生效并继续执行；`Fail`、`Stop`、`Wait`、`Retry`、`Fork` 和 `Next` 是控制结果，
会立即停止本次迁移的后续阶段。自定义 Outcome 由 Registry 明确选择 `CONTINUE/STOP`，
默认是停止型 Control。

常见异常：

- `MachineNotStarted`：在 `start()` 前派发事件。
- `MachineNotRunnable`：运行处于等待、暂停、完成、失败或停止状态时使用了错误入口。
- `NoTransition`：当前节点没有符合事件和 Guard 的迁移。
- `AsyncGuardRequired`：同步入口遇到异步 Guard，应使用异步运行入口。
- `DuplicateEvent`：配置幂等存储后重复消费同一运行实例的 Event。
- `AsyncActionRequired`：同步入口收到异步 Action 或 Hook。
- `CancellationError` / `ActionTimeout`：策略在执行边界拒绝当前 Action。

Action 或 Hook 自身抛出的异常不会被包装成通用异常；调用方可以按原异常类型处理。

## `Context`

`Context` 是一次运行的唯一动态数据容器，不再另设一个并列的 `State` 模型。

- `node_id`：当前图节点。
- `status`：运行生命周期。
- `data`：业务数据，可通过 `get()` / `set()` / `update()` 访问。
- `metadata`：运行元数据和策略记录。
- `last_event`：最近一次推动迁移的 Event。
- `waiting`：等待事件、重试或 Fork 的恢复信息。
- `attempt`：当前重试尝试次数。
- `graph_version`：Context 所属 Graph 定义版本，恢复时必须与 Graph 匹配。

`Status` 只描述运行生命周期，不表示业务领域状态。

`Machine.update_context()` 是外部人工输入、恢复驱动或其它控制面更新的受控入口。它只
合并业务数据，不改变 `node_id` 或 `status`，也不会因为某个字段值满足 Guard 就自动
迁移；需要推进图时仍然调用 `dispatch()` 或等待场景下的 `resume()`。该入口会触发
`context.updated` Hook，因此持久化绑定也能保存这次更新。直接修改 `context.data` 仍然
可行，但不会触发 Hook 或自动持久化。

`resume()` 会先解析等待事件对应的迁移；Guard 不通过或迁移缺失时会抛出
`NoTransition`，同时保留 `WAITING` 和原等待信息。Guard 的第二个参数始终是完整的
`Event` 对象，而不是事件名字符串。

## `Event`

`Event` 是不可变输入消息，包含 `name`、`payload`、`source`、全局唯一的 `event_id`
和创建时间。创建时会递归冻结常见映射和序列，避免外部消息或监听器修改运行中的输入；
跨 JSON/消息系统边界时使用 `event.to_dict()` 得到普通容器。传入
字符串时，Machine 会自动创建 Event：

```python
machine.dispatch("payment.completed", {"amount": 100})
```

需要幂等时，应复用带有稳定 `event_id` 的 Event，并在 Machine 上配置
`InMemoryIdempotencyStore` 或其它 `IdempotencyStore`。

## `Outcome`

Action 返回 `Outcome` 表达控制意图，而不是直接修改 Machine 的图位置：

| Outcome | 作用 |
| --- | --- |
| `Next` | 可更新 `Context.data`，再产生内部事件继续经过图上的迁移 |
| `Emit` | 向 EventBus 发布外部事件 |
| `Wait` | 等待时间或指定恢复事件 |
| `Outcome.interrupt()` | `Wait` 的明确中断写法，等待外部输入恢复 |
| `Retry` | 保存重试信息，稍后重新执行当前节点 |
| `Fork` | 创建独立子运行并等待 Join；支持 `failure_policy=fail/continue/fail_fast` |
| `Update` | 将 Action 返回的映射合并到 `Context.data` |
| `Stop` | 正常停止 |
| `Fail` | 以失败状态结束 |

## 不保证的内容

核心 API 当前不保证：

- 自动事件重放或完整事件溯源。
- 强制终止正在运行的同步线程。
- 分布式调度、队列、锁和事务。
- 领域对象、LLM、工具调用、Spider 或其它适配器。

这些能力应在引擎边界之外通过协议、绑定对象或领域适配器提供。
