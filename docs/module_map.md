# Bricks 模块地图

这份文档用于回答一个问题：Bricks 里的每个目录和文件分别负责什么。

Bricks 的核心运行链路只有一条：

```text
GraphBuilder
    ↓ build()
Graph
    ↓ 创建一次运行
Machine
    ↓ 持有
Context
    ↓ 接收
Event
    ↓ 查找
Transition
    ↓ 执行 Action
Outcome
    ↓ OutcomeInterpreter
继续迁移 / 等待 / 重试 / Fork / 结束
```

其余目录都是围绕这条链路增加能力，不需要在第一次阅读时全部掌握。

## 先记住八个核心概念

| 概念 | 作用 | 所在位置 |
| --- | --- | --- |
| `GraphBuilder` | 用代码构建流程图 | `engine/graph/builder.py` |
| `Graph` | 校验后的不可变流程定义 | `engine/graph/graph.py` |
| `Machine` | 执行一张图的一次运行 | `engine/runtime/machine.py` |
| `Context` | 保存一次运行的当前节点、数据和生命周期信息 | `engine/runtime/context.py` |
| `Event` | 推动图发生迁移的输入 | `engine/events/messages.py` |
| `Outcome` | Action 返回给运行时的控制意图 | `engine/runtime/outcomes.py` |
| `OutcomeInterpreter` | 把领域结果解释成继续或停止指令 | `engine/runtime/interpreter.py` |
| `Status` | 表示一次运行的生命周期 | `engine/runtime/lifecycle.py` |

`Status` 表示运行生命周期，例如 `RUNNING`、`WAITING`、`COMPLETED`，不是单独的
业务状态模型。流程当前位于哪个节点由 `Context.node_id` 表示。

最小入口只有：

```python
from bricks import Context, Event, Graph, GraphBuilder, Machine, Outcome, Status
```

事件总线、Hook、策略、持久化和高级语义应该从对应的子模块导入，不从核心入口
混入。

## 目录总览

```text
bricks/
├── engine/          # 通用图执行内核
│   ├── graph/       # 流程图的静态定义
│   ├── runtime/     # 一次运行的动态执行
│   ├── events/      # 事件总线和生命周期 Hook
│   ├── persistence/ # 快照和事件历史
│   ├── scheduling/  # 外部定时唤醒请求和绑定
│   ├── policies/    # 重试、超时、取消、幂等规则
│   └── semantics/   # Workflow、Reactive、Parallel、Saga 等组合语义
└── __init__.py      # 简化后的公开入口
```

依赖方向应该保持为：

```text
future adapters
    ↓
semantics / events / persistence / policies
    ↓
runtime
    ↓
graph
```

图定义不应该反向依赖 Spider、HTTP、数据库、消息队列或 Agent。

## `engine/graph`：定义流程图

这一层只回答：

> 流程图长什么样？节点之间如何迁移？

### `builder.py`

提供流式构建 API：

- `action()` 添加带 Action 的节点
- `wait()` 添加等待节点
- `terminal()` 添加终止节点
- `transition()` 添加事件迁移
- `include()` 将已校验 Graph 作为前缀命名空间片段加入
- `build()` 校验并生成 `Graph`

`GraphBuilder` 可以修改，生成的 `Graph` 不可变。通常业务代码只需要直接使用
`GraphBuilder`，不需要手动组装 `Graph` 的内部字段。

### `graph.py`

保存一张已经构建完成的图：

- 图 ID
- 图定义版本
- 初始节点
- 节点集合
- 迁移集合
- 根据当前节点和事件解析迁移
- 返回结构描述供静态检查和可视化使用

同一个 `Graph` 可以被多台 `Machine` 复用。

### `nodes.py`

定义最小基类 `BaseNode`，以及几个内置扩展：

- `ActionNode`：进入时执行 Action
- `WaitNode`：进入后等待事件或计时器
- `TerminalNode`：进入后结束运行
- `SubGraphNode`：进入时启动独立 Graph，并通过 Fork/Join 返回

所有节点共享 ID、Action、退出 Action 和元数据；具体节点通过继承增加自己的
字段和运行语义。`Machine` 只调用节点统一的 `enter()` / `exit()` 协议，不为每个
自定义节点增加判断分支。业务代码通常使用 `GraphBuilder.action()`、`wait()` 和
`terminal()`，只有需要自定义节点时才直接继承 `BaseNode`。

### `transitions.py`

定义 `Transition`，表达：

```text
当前节点 + 事件 → 目标节点
```

迁移还可以带有 Guard、Action、优先级和元数据。

### `guards.py`

提供迁移条件判断的类型和辅助定义。Guard 接收 `Context` 和 `Event`，决定某条
迁移是否可以被选中；异步 Guard 会由异步运行入口等待，`AllOf`、`AnyOf`、`Not` 和
`Predicate` 支持组合异步 Guard。

### `validate.py`

在 `GraphBuilder.build()` 时执行静态校验，例如：

- 初始节点是否存在
- 迁移源节点和目标节点是否存在
- 节点和迁移 ID 是否重复
- 图定义是否满足运行时的基本要求
- 从初始节点的可达性分析

`Graph.describe()`、`Graph.to_mermaid()` 和 `Graph.to_dot()` 位于 `graph.py`，用于结构
检查和可视化。
`serialization.py` 提供带版本的 `Graph.to_dict()` / `Graph.from_dict()` 协议；Action、
Guard、自定义节点和子图都必须通过调用方提供的显式解析器恢复。

`validate.py` 还提供可达节点、终止节点和非终止死端分析；死端是检查提示，不会被默认
当作构建错误，因为它可能是等待外部事件的有意边界。

## `engine/runtime`：执行一次流程

这一层只回答：

> 这张图现在运行到哪里？接收到事件后应该做什么？

### `machine.py`

`Machine` 是运行时的核心。它负责：

- 启动运行
- 接收外部事件
- 解析并执行迁移
- 进入和退出节点
- 执行 Action
- 把 `Outcome` 交给解释器并根据指令推进阶段
- 提供 `invoke()`、`stream()` 及异步对应入口
- 提供 `stream_events()` / `astream_events()` 运行事件流
- 调用 Hook

一张 `Graph` 是定义，一台 `Machine` 是这张定义的一次运行。

### `fork.py`

由 Machine 委托的 Fork/Join 实现，负责创建子运行、等待分支结束和恢复并行快照。
它不是基础图执行的必需部分，后续可以替换为不同的并行调度实现。

### `context.py`

`Context` 是一次运行的动态上下文，保存：

- `node_id`：当前所在节点
- `status`：运行生命周期
- `data`：业务数据
- `metadata`：运行元数据
- `last_event`：最近接收的事件
- `attempt`：当前尝试次数
- `waiting`：等待或重试信息

它就是引擎内部的运行记忆，不等同于某个业务领域的 Context。领域适配器可以
在 `Context.data` 中放入自己的数据，或者在上层包装它。

### `outcomes.py`

Action 不直接修改 Machine 的控制流程，而是返回 `Outcome`：

| Outcome | 含义 |
| --- | --- |
| `Next` | 可更新 Context 后产生内部事件，继续经过图上的迁移 |
| `Emit` | 发布一个事件给外部监听者 |
| `Wait` | 暂停运行，等待时间或事件 |
| `Retry` | 保存重试信息，稍后重新执行 |
| `Fork` | 创建多个独立子运行 |
| `Stop` | 主动停止运行 |
| `Fail` | 让运行进入失败状态 |

### `interpreter.py`

定义最小 `OutcomeInterpreter` 协议和不可变 `OutcomeRegistry`。内建 Outcome 与领域
Outcome 使用同一种规则注册；每条规则只包含处理函数以及 `CONTINUE/STOP` 指令。
Machine 因此不需要为 Tool、Memory、HumanInput 等领域结果增加类型分支。

### `outcome_runtime.py` 与 `effects.py`

`OutcomeRuntime` 是 handler 可使用的窄能力端口，不暴露 dispatch、join、snapshot 等
Machine 控制能力。需要与运行提交绑定的领域副作用先保存为 `StagedEffect`，由原子
持久化端口写入 Outbox。

### `executor.py`

定义 Action 执行协议。目前的 `InlineExecutor` 直接在当前进程执行。以后可以
实现协程、线程、进程或远程执行器，而不改变 Graph 定义。`PolicyExecutor` 只负责
把取消和超时策略组合到基础执行器外层。执行器只负责一次 Action 调用；何时运行
Machine 由外部调度器决定，跨进程消息由外部 Queue/Transport 转换成 `Event`。具体
边界见 `decisions/005-external-execution-boundary.md`。

### `lifecycle.py`

定义运行生命周期的 `Status` 和生命周期事件名称。它服务于 Machine 和 Hook，
不负责业务迁移。

## `engine/events`：事件和 Hook

这一层包含两个不同的机制：

```text
EventBus      处理业务事件和外部消息
HookRegistry  监听引擎生命周期
```

### `messages.py`

核心只定义 `Event` 消息。Command/Result 属于具体 RPC 或领域适配器，不提前放进
图引擎。

### `bus.py`

提供发布和订阅能力，例如：

```python
events.publish("payment.completed", {"amount": 100})
```

它不应该知道 Redis、Kafka 或 HTTP 的具体实现；外部消息应该先转换成引擎的
`Event`。

### `hooks.py`

监听 `Machine` 的内部生命周期，例如：

- 启动前后
- 迁移前后
- 节点进入和退出
- 事件未处理
- 运行失败

适合用于日志、审计、指标、调试和统一拦截。

### `filters.py`

提供订阅过滤、通配匹配、优先级、一次性订阅和作用域匹配等能力。

## `engine/persistence`：保存和恢复

这一层只回答：

> 进程重启后，这次运行能不能从上次位置继续？

### `snapshot.py`

将 `Context` 转成可保存的快照。快照只保存恢复运行所需的数据，不保存函数、
执行器或临时资源。

### `store.py`

定义只包含 `save/load/delete` 的 `SnapshotStore` 协议，并提供内存实现。需要并发控制的
外部实现可以额外提供 `save_if_current()`；以后可以接入 Redis、SQLite、PostgreSQL 或
远程 KV 存储。

### `event_log.py`

定义事件追加日志，用于审计、调试、追踪和历史查询。它记录发生过什么，但不
自动决定如何重放事件。

### `binding.py`

提供 `PersistenceBinding`，通过 Hook 监听 Machine 并保存快照、追加事件。Machine
本身不接收存储对象，也不直接调用存储协议。

## `engine/policies`：通用运行规则

策略不是图，也不是运行器，而是可插拔的规则对象：

| 文件 | 负责什么 |
| --- | --- |
| `retry.py` | 最大尝试次数、退避间隔和可重试条件 |
| `timeout.py` | Action 的同步耗时检查和异步等待超时 |
| `cancellation.py` | 主动取消运行 |
| `idempotency.py` | 避免事件或 Action 被重复处理 |

这部分先不看也不影响理解基本的 `start()` 和 `dispatch()`。

## `engine/semantics`：不同的编排视角

这里不是几套新的执行引擎，而是基于 `Machine` 的组合入口：

| 文件 | 适合场景 |
| --- | --- |
| `workflow.py` | 批量步骤、DAG 和工作流式编排 |
| `reactive.py` | 由 `EventBus` 自动驱动 Machine |
| `parallel.py` | Fork/Join、等待全部或任意分支 |
| `compensation.py` | Saga 和失败后的逆序补偿 |

如果只是理解基础图执行，可以最后再看这个目录。

## 领域适配器：暂不实现

当前仓库只搭建通用图引擎，尚未创建领域适配器目录。未来的 Spider、Agent、Browser、
ETL 和 RPC 应作为独立适配器新增，不能反向污染引擎核心。

未来的适配器可以使用：

- `Graph` 定义流程
- `Machine` 运行流程
- `Context` 保存领域数据
- `EventBus` 接收外部消息
- `ActionExecutor` 执行领域动作
- `SnapshotStore` 和 `EventLog` 做持久化

但 `engine` 不应该为了支持某个适配器而加入 Spider、LLM、HTTP 或数据库的特殊
分支。

## 推荐阅读顺序

### 只想跑通一个流程

1. `docs/graph_engine.md`
2. `engine/graph/builder.py`
3. `engine/graph/graph.py`
4. `engine/graph/nodes.py`
5. `engine/graph/transitions.py`
6. `engine/runtime/context.py`
7. `engine/runtime/machine.py`
8. `engine/runtime/outcomes.py`
9. `engine/runtime/interpreter.py`

### 想理解事件监听

在主干之后阅读：

1. `engine/events/messages.py`
2. `engine/events/bus.py`
3. `engine/events/hooks.py`
4. `engine/events/filters.py`
5. `engine/semantics/reactive.py`

### 想理解可靠运行

在主干之后阅读：

1. `engine/persistence/snapshot.py`
2. `engine/persistence/store.py`
3. `engine/persistence/event_log.py`
4. `engine/policies/retry.py`
5. `engine/policies/idempotency.py`

## 新功能应该放在哪里

可以先按下面的规则判断：

| 新需求 | 放置位置 |
| --- | --- |
| 新的节点、迁移或 Guard | `engine/graph` |
| 新的领域运行结果 | 领域适配器中的 Outcome + `OutcomeRegistry.with_handler()` |
| 新的生命周期监听 | `engine/events/hooks.py` |
| 新的业务消息路由 | `engine/events` |
| 新的存储介质 | `engine/persistence` 的协议实现或适配器 |
| 新的重试、超时、取消规则 | `engine/policies` |
| 新的编排视角 | `engine/semantics` |
| Spider、Agent、ETL 等领域能力 | 未来新增的适配器 |

判断标准只有一个：

> 这个能力是否仍然适用于所有领域？

如果不是，就不要放进 `engine`，而应该放进未来对应的 `adapter`。

## 当前可以暂时忽略的内容

第一次阅读时可以先忽略：

- `persistence/`：不需要恢复运行时
- `policies/`：不需要复杂重试和取消时
- `semantics/`：不需要 Workflow、Reactive、Fork 或 Saga 外观时
- 未来的 `adapters/`：还没有具体领域扩展时
- `filters.py`：只做简单事件订阅时

先掌握下面这条最小链路即可：

```text
GraphBuilder → Graph → Machine → Context
                              ↑
                         Event / Outcome
```
