# Bricks 实现与使用手册

这套手册面向两类读者：需要使用 Bricks 构建流程的人，以及需要阅读、修改或扩展
引擎实现的人。内容以当前 `bricks/engine` 代码为准，既解释公开接口，也解释接口背后的
提交边界、状态变化和基础设施协议。

## 建议阅读顺序

1. [设计与运行模型](design-and-runtime.md)：先建立 Graph、Machine、Context、Event、
   Transition 和 Outcome 的完整心智模型。
2. [核心组件与源码导读](components.md)：逐目录了解每个组件负责什么、内部怎样协作。
3. [可靠性与生产运行](reliability.md)：理解快照、CAS、事件日志、幂等、AtomicCommit、
   Outbox、Wakeup 和 Fork 恢复。
4. [完整使用指南](usage.md)：从最小图开始，逐步使用异步 Action、等待、重试、并行、
   持久化和自定义扩展。

已有文档仍然保留各自用途：

- `docs/guides/` 是按任务拆分的短教程。
- `docs/reference/` 是公开 API 速查。
- `docs/decisions/` 记录长期架构决策及取舍。
- 本手册负责把这些能力与源码实现串成一个整体。

## 一句话理解 Bricks

Bricks 是一个单活动节点、显式事件驱动、可恢复的图执行内核：

```text
不可变 Graph
    +
每次运行独立的 Context
    +
Event 选择 Transition
    +
Action 返回 Outcome
    =
Machine 的一次确定性状态推进
```

它不自带 HTTP 客户端、消息队列、数据库、定时线程或远程 Worker。它为这些能力定义窄
协议，让应用按需要接入。这样 Spider、Agent、ETL、审批流和 Saga 可以共享执行内核，
而不需要让内核知道具体领域。

## 代码导航

```text
bricks/engine/
├── graph/        静态图、节点、迁移、Guard、校验和序列化
├── runtime/      Machine、Context、Outcome、执行器和 Fork/Join
├── events/       领域 EventBus、生命周期 Hook 和可观察 RuntimeEvent
├── persistence/  快照、事件日志、重放和 AtomicCommit/Outbox
├── scheduling/   可持久化 Wakeup 与外部调度器绑定
├── policies/     重试、取消、超时和事件幂等
├── semantics/    Workflow、Reactive、Parallel 和 Saga 外观
├── errors.py     公开异常体系
└── types.py      Action/Guard 协议及递归冻结工具
```

最值得按顺序阅读的源码是：

```text
graph/builder.py
    -> graph/graph.py
    -> runtime/machine.py
    -> runtime/interpreter.py
    -> runtime/outcome_runtime.py
    -> runtime/fork.py
    -> persistence/binding.py
    -> persistence/atomic.py
```

## 公共和内部边界

应用优先使用文档中的公开类和方法。以下名称虽然是理解实现的关键，但以下划线开头，
不是兼容性承诺：

- `Machine._dispatch_event_once()`、`_transition()` 和 `_drain_result()`；
- `MachineOutcomeRuntime`；
- `ForkController` 的 `_prepare_join_dispatch()` 等辅助方法；
- `EventBus._reactive_candidates()`；
- 持久化 Binding 的 Hook 回调。

扩展框架时应实现公开 Protocol 或注入公开策略，而不是直接调用这些内部方法。
