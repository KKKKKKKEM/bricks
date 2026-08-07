# Bricks 基础架构设计

Bricks 的核心是一个领域无关的图执行引擎。图不是某种业务流程的代码，而是
可以被不同运行语义、执行器和基础设施重复使用的静态定义。

## 分层关系

```text
Spider / Agent / ETL / Workflow / Saga / Reactive
                         |
                         v
Graph + Machine + Context + OutcomeInterpreter
                         |
                  explicit ports
                         v
ActionExecutor / EventBus / Persistence / WakeupScheduler / ForkRuntime
                         |
                         v
HTTP / Queue / SQL / Redis / Remote Workers
```

领域组合依赖稳定核心，核心只通过显式端口接触基础设施，不反向依赖某个领域实现：

- `graph` 只描述节点、边、条件和 Action 引用，不保存运行状态。
- `runtime` 只负责一次运行实例的生命周期和事件迁移。
- `events` 同时提供领域事件总线和引擎生命周期 Hook，但两者保持分离。
- `persistence` 定义快照、追加日志和可选 AtomicCommit/Outbox 协议，存储介质由外部提供。
- `scheduling` 只定义稳定 Wakeup，不在引擎内运行定时线程。
- `policies` 提供可插拔策略值对象，不把调度器、队列和数据库写死在核心。
- `semantics` 是组合外观，可以选择工作流、响应式、并行或 Saga 的视角；基础 Machine
  已经覆盖单活动节点的事件迁移。

## 一次运行

```text
start / dispatch / resume
          |
          v
selector.select(graph, source, event, context) / select_async(...)
          |
          v
before hooks -> exit -> transition action -> enter
          |
          v
OutcomeInterpreter：Effect 继续 / Control 停止
          |
          v
after hooks -> snapshot -> event log
```

迁移解析由 runtime 的 `TransitionSelector` 完成，默认实现按优先级、声明顺序和 Guard
选择边。Graph 只提供静态候选边；需要领域特定路由规则时可以注入其它 selector，而不修改
Graph 或 Machine。

Guard 可以是同步或异步函数；同步入口不会偷偷创建事件循环，异步 Guard 必须通过
`dispatch_async()` 等异步入口求值。`Next` 只产生内部事件，仍然必须经过图上的迁移；`Wait` 和 `Retry` 让运行实例
进入可恢复上下文；`Fork` 创建共享 Graph、独立 Context 的子运行。这样 Action
可以表达控制意图，但不能绕过 Graph 直接修改运行节点。

Machine 不按具体 Outcome 类型扩展执行循环。默认的 `OutcomeRegistry` 注册内建语义，
领域框架通过 `with_handler()` 返回一个新 Registry；规则只声明 handler 和
`CONTINUE/STOP`，因此 Agent 的 Tool、Memory、HumanInput 等语义不需要进入核心分支。

## 外部驱动

手动驱动适合测试和简单任务：

```python
machine.start()
machine.dispatch("next", payload)
```

事件驱动适合长时间运行任务：

```python
runtime = ReactiveRuntime(events)
runtime.attach(machine)
events.publish("next", payload)
```

`ReactiveRuntime` 是适配层，不让 `EventBus` 了解 Machine，也不让 Machine 依赖
具体消息队列。以后接入 Redis、Kafka 或 RPC 时，只需要把外部消息转换成
`Event`，再交给同一套路由规则。

## 持久化策略

核心默认是内存运行；通过 `PersistenceBinding` 绑定 `SnapshotStore` 和
`EventLog` 后，运行实例在公开操作结束时保存快照，并记录启动、外部事件和内部
事件。恢复流程是：

```text
SnapshotStore.load(run_id)
        |
        v
PersistenceBinding.restore(graph, run_id, store)
        |
        v
继续 dispatch / resume / join
```

事件日志只负责记录事实，不自动重放。是否重放、如何去重、如何处理外部副作用，
属于上层适配器和幂等策略的职责。

## 扩展约束

新增领域适配器时应遵守以下边界：

1. 未来的领域对象放在独立的 adapter 包中，不要反向导入旧 Spider 模块。
2. 外部执行能力实现 `ActionExecutor`，不要把线程、协程或远程调用写进 BaseNode。
3. 外部消息转换成 `Event`，不要让 EventBus 感知 Redis、HTTP 或 RPC 类型。
4. 需要可靠恢复时实现 `SnapshotStore` / `EventLog`，不要修改 Graph 的不可变定义。
5. 未来的 Agent、Spider 和 ETL 都应当是引擎之上的组合，不应成为引擎的特殊分支。
