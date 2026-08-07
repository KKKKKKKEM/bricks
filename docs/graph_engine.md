# Bricks 图执行引擎

Bricks 的核心不是某一个领域框架，而是一个可以承载多种执行模型的图执行引擎。

## 架构分层

```text
engine/graph         不可变图定义
engine/runtime       一次运行、上下文、结果和生命周期
engine/semantics     工作流、响应式、并行、补偿等组合语义
engine/events        领域事件与生命周期 Hook
engine/persistence   快照与执行历史
engine/policies      重试、超时、取消、幂等策略
future adapters/     领域适配器（当前未实现）
```

核心层不应该依赖 Spider、HTTP、数据库或 Agent。它只处理节点、迁移、事件、运行上下文和执行协议。

## Graph 与 GraphBuilder

`GraphBuilder` 只负责构建图，`build()` 之后得到的 `Graph` 会经过校验并作为不可变定义使用。一个 Graph 可以被多个 Machine 运行实例共享。

```python
from bricks.engine import GraphBuilder, Machine


builder = GraphBuilder("approval", initial="draft")
builder.action("draft")
builder.terminal("approved")
builder.terminal("rejected")
builder.transition(
    "draft",
    "review",
    "approved",
    guard=lambda context, event: context.get("valid", False),
)
builder.transition("draft", "review", "rejected")

graph = builder.build()
machine = Machine(graph)
machine.start()
machine.context.set("valid", True)
machine.dispatch("review")
```

## 运行时

每一次运行都有独立的 `Context`：

```text
GraphDefinition  # 静态定义，可共享
      ↓
Machine          # 一次运行
      ↓
Context       # 当前节点、业务数据、状态和等待信息
```

Action 通过 `ActionExecutor` 执行，因此线程、协程、进程、远程执行器不需要改变图定义。
Action 也可以返回字典或 `Outcome.update(...)`，引擎会把增量数据合并到当前
`Context.data`，不需要引入额外的 State 对象。

## 事件与 Hook

`EventBus` 和 `HookRegistry` 是两个不同概念：

- `EventBus` 处理工作流中的领域事件，例如 `order.created`。
- `HookRegistry` 监听引擎生命周期，例如 `transition.before`、`node.enter` 和 `transition.after`。

两者都支持优先级、一次性订阅和条件匹配。

如果希望让图由外部事件自动驱动，可以使用 `ReactiveRuntime`：

```python
from bricks.engine import Machine
from bricks.engine.events import EventBus
from bricks.engine.semantics import ReactiveRuntime


events = EventBus()
machine = Machine(graph, events=events)
binding = ReactiveRuntime(events).attach(machine)
events.publish("review", {"valid": True})
binding.close()
```

响应式运行时会根据当前状态选择 `dispatch()` 或 `resume()`；重试等待不会被普通
事件误消费。

## 持久化边界

Machine 不直接依赖数据库，也不在构造函数中接收存储对象。持久化通过
`PersistenceBinding` 接入，它依赖两个小协议：

- `SnapshotStore` 保存当前运行状态，适合 Redis、SQLite 或远程 KV 实现。
- `EventLog` 追加输入事件和内部事件，适合审计、追踪或事件溯源实现。

存储如果需要支持调试或时间旅行，可以额外实现 `read_history(run_id)`；这不是最小
`SnapshotStore` 协议的必需方法。

```python
from bricks.engine import Machine
from bricks.engine.persistence import InMemoryEventLog, InMemorySnapshotStore
from bricks.engine.persistence import PersistenceBinding


store = InMemorySnapshotStore()
log = InMemoryEventLog()
machine = Machine(graph)
persistence = PersistenceBinding(machine, store, log).attach()
machine.start()
restored = PersistenceBinding.restore(graph, machine.context.run_id, store, event_log=log)
```

快照是恢复运行实例的最小状态；事件日志不是 Machine 的隐式重放器，重放策略应该
由更上层的 Workflow 或领域适配器决定。

## 运行结果

Action 可以返回运行时结果：

```python
from bricks.engine import Outcome


def wait_for_callback(context, event):
    return Outcome.wait(delay=5, resume_event="callback.received")
```

当前核心提供 `Next`、`Emit`、`Wait`、`Retry`、`Fork`、`Stop` 和 `Fail` 这些结果类型。

- `Next` 可以先更新 `Context.data`，再生成内部事件并继续消费后续迁移；连续内部步骤
  有安全上限。
- `Retry` 会保存等待信息和尝试次数，由 `resume_retry()` 或异步版本重新执行当前节点。
- `Fork` 会创建共享 Graph、独立 `Context` 的子运行；父运行通过 `join()` 消费 `all` 或
  `any` Join，并可用 `failure_policy` 控制失败分支是让父级失败、继续汇聚还是快速失败。
- 结果不直接绕过图修改状态，真正的状态变化仍然应该通过事件、迁移和 Guard 完成。

## 组合语义

`semantics/` 不重新实现 Machine，而是提供不同建模方式的入口：

- `Machine`：默认的事件迁移运行器，既可以表达状态机，也可以承载工作流语义。
- `Workflow`：批量运行事件和 DAG 拓扑校验。
- `ReactiveRuntime`：把 EventBus 事件路由到运行实例。
- `ParallelPlan`：声明 `all` / `any` Fork/Join 分支和失败处理策略。
- `SagaRuntime`：从迁移元数据收集补偿 Action，并按逆序执行。

领域能力统一放入 `adapters/`，不反向污染引擎核心。
