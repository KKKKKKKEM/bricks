# 设计与运行模型

## 1. 设计目标

Bricks 把流程定义、一次运行和外部基础设施分开，核心目标是：

- 同一张 Graph 可以安全复用于许多独立运行；
- 所有控制流都经过具名 Event 和静态 Transition，Action 不能偷偷跳节点；
- 同步和异步入口语义一致，但同步入口不会暗中创建事件循环；
- 等待、重试、并行和恢复都能序列化为明确状态；
- 数据库、队列、调度器和远程执行器通过协议接入，不进入核心分支；
- 领域可以增加新的 Outcome，而不修改 Machine 的执行循环。

对应的依赖方向是：

```text
Application / Spider / Agent / ETL / Workflow
                    |
                    v
semantics / persistence / scheduling / events
                    |
                    v
                 runtime
                    |
                    v
                  graph
```

`graph` 不反向依赖运行时；运行时不依赖具体数据库或消息中间件。

## 2. 静态定义和动态运行

### 2.1 Graph 是不可变定义

`GraphBuilder` 是构建阶段的可变对象。`build()` 会创建 `Graph`、执行校验并冻结：

- `nodes` 被包装为只读映射；
- `transitions` 固定为元组；
- 节点和迁移的 metadata 递归冻结；
- `(source, event)` 对应的迁移按 `(priority, order)` 建立索引。

因此一张 Graph 可以在不同线程或请求中被多台 Machine 共享。Graph 不保存当前节点、
业务数据、重试次数或运行 ID。

### 2.2 Machine 是一次运行

`Machine(graph)` 为 Graph 创建一次执行。动态状态全部位于 `Context`：

| 字段 | 含义 |
| --- | --- |
| `graph_id` / `graph_version` | 绑定的图定义身份 |
| `run_id` | 本次运行的唯一身份 |
| `node_id` | 当前唯一活动节点 |
| `status` | 生命周期状态 |
| `data` | 业务数据 |
| `metadata` | 引擎和适配器元数据 |
| `last_event` | 最近参与迁移的 Event |
| `attempt` | 当前 Retry 尝试次数 |
| `waiting` | Wait、Retry 或 Fork 的可恢复等待声明 |

Bricks 是单活动节点模型。一台 Machine 始终最多只有一个 `node_id`；并行通过多个子
Machine 表达，而不是把父 Context 变成 token 集合。

### 2.3 Event、Transition 和 Outcome 各自负责什么

```text
Event       说明“发生了什么”
Transition 说明“在当前节点收到该事件时允许走哪条边”
Action      执行业务计算
Outcome     说明“业务计算希望运行时做什么”
```

Event 包含 `name`、冻结后的 `payload`、`source`、`event_id`、`created_at` 和可选的
`target_run_id`。`event_id` 是幂等边界，不等同于事件名。

Transition 包含源节点、事件名、目标节点、Guard、迁移 Action、优先级和声明顺序。
`target=None` 表示仍在源节点。

## 3. 生命周期状态

`Status` 是运行生命周期，不是业务状态：

```text
CREATED
   |
 start()
   v
RUNNING <------ resume / resume_retry / resume_run / join
   |  \
   |   \ terminal node
   |    v
   |  COMPLETED
   |
   +---- Wait / Retry / Fork ----> WAITING
   +---- pause() ----------------> PAUSED
   +---- Outcome.stop() ---------> STOPPED
   +---- exception / Fail -------> FAILED
```

等待状态还要看 `context.waiting`：

| waiting 类型 | 识别方式 | 正确恢复入口 |
| --- | --- | --- |
| 普通事件或定时 Wait | 没有 `kind` 或普通等待字段 | `resume()` |
| Retry | `kind == "retry"` | `resume_retry()` |
| Fork | `kind == "fork"` | `join()`，或先 `route()` 驱动子运行 |
| 手工暂停 | 状态为 `PAUSED`，没有 waiting | `resume_run()` |

使用错误入口会抛出 `MachineNotRunnable`，不会猜测调用者意图。

## 4. 启动过程

`start()` 的实际顺序是：

```text
校验状态必须为 CREATED
  -> 检查取消令牌
  -> 创建 __start__ Event
  -> machine.before_start Hook
  -> status = RUNNING，node_id = graph.initial
  -> node.enter Hook
  -> 执行初始节点 Action
  -> 解释 Outcome
  -> 排空连续 Next
  -> 如果当前节点 terminal 且仍为 RUNNING，则 COMPLETED
  -> 投递本次操作暂存的 Emit
  -> machine.after_start Hook
```

初始节点可以立即返回 Wait、Retry、Fork、Fail 或 Next，所以 `start()` 返回时不保证状态
一定是 RUNNING。

如果进入节点或解释 Outcome 失败，未提交的 StagedEffect 和 Emit 会被丢弃，状态进入
FAILED。`machine.after_start` 属于提交后观察阶段；它失败会向调用方传播，但不会把已经
完成的启动伪装成失败。

## 5. 一次 dispatch 的完整流水线

`dispatch(event)` 首先要求 Machine 为 RUNNING，然后进入以下流水线：

```text
1. claim event_id（配置 IdempotencyStore 时）
2. selector.select(source, event)
   - 按 priority、声明 order 遍历候选边
   - 执行 Guard
   - 没有边时触发 event.unhandled 并抛 NoTransition
3. machine.before_dispatch
4. transition.before
5. source node.exit
6. transition.action
7. context.node_id = target
8. target node.enter
9. terminal 检查
10. 标记结构迁移已经提交
11. 投递迁移期间暂存的 Emit
12. transition.after
13. machine.after_dispatch
14. 如果 Outcome 是 Next，继续处理内部事件
```

第 5、6、8 阶段都可以返回 Outcome。每个阶段处理完 Outcome 后，Machine 根据
`OutcomeDirective` 决定是否继续后续阶段。

### 5.1 提交点为什么位于 after Hook 之前

节点位置、Context 和控制 Outcome 完成后，迁移已经成为运行事实。`transition.after` 和
`machine.after_dispatch` 是观察、持久化和集成边界，不是事务回滚脚本。

因此 after Hook 抛错时：

- 异常会传播给调用方；
- 已经更新的节点和状态保留；
- 原 Event 的幂等声明保留；
- 调用方不能用同一 event ID 重做业务 Action。

这避免“业务已经执行，但审计 Hook 失败后又执行一遍”的重复副作用。

### 5.2 迁移体失败

source exit、transition action 或 target enter 抛错时：

- 丢弃尚未提交的 StagedEffect；
- 丢弃尚未发布的 Emit；
- 默认把状态设为 FAILED；
- 触发 `transition.error`；
- 在提交前释放 Event 幂等声明。

这里的“未提交”不是任意 Python 对象的内存事务。Action 在抛错前直接写入
`Context.data/metadata` 的内容可能保留，已经调用的外部系统也无法自动撤销。引擎只明确
回收自己管理的 StagedEffect、Emit 和 Event claim。需要回滚的业务应使用补偿流程，可靠
外部动作应使用 Outbox。

Fork Join 使用额外的 `on_abort` 回调：Join 迁移没有提交时，会先恢复原 Fork 和父 Context，
再触发错误 Hook，使持久化看到的仍是可重试 Join 状态。

## 6. Action 返回值和 Outcome

Action 签名统一为：

```python
def action(context, event):
    ...
```

返回值转换规则：

| 返回值 | 运行时解释 |
| --- | --- |
| `Outcome` 实例 | 交给 OutcomeInterpreter |
| Mapping | 自动转换为 `Update`，合并进 `Context.data` |
| `None` 或其它值 | 没有控制含义，作为普通 Action 结果忽略 |

内建 Outcome 的阶段指令如下：

| Outcome | 作用 | 指令 |
| --- | --- | --- |
| `Update` | 合并业务数据、清零 attempt | CONTINUE |
| `Emit` | 暂存领域事件，提交后发布 | CONTINUE |
| `Next` | 更新数据并产生内部 Event | STOP |
| `Wait` | 写入 waiting，状态变 WAITING | STOP |
| `Retry` | 计算退避和 due_at，状态变 WAITING | STOP |
| `Fork` | 创建子运行，父状态变 WAITING | STOP |
| `Stop` | 状态变 STOPPED | STOP |
| `Fail` | 状态变 FAILED | STOP |

STOP 表示停止当前迁移剩余阶段。例如迁移 Action 返回 Wait 时，不会先进入目标节点。
Next 会在当前迁移返回后作为内部 Event 再次经过 Selector，绝不是直接跳转。

连续 Next 受 `max_internal_steps` 限制，默认 100；超出后状态变 FAILED 并抛出
`InternalStepLimitExceeded`。

## 7. Emit 的提交后投递

`Outcome.emit()` 不会在 Action 正执行到一半时立即调用 EventBus。Machine 先把 Event 放入
内部队列，结构迁移提交后再按顺序投递。

这条规则很重要：如果 Machine 和 ReactiveRuntime 共用同一个 EventBus，Emit 可能再次
路由回同一台 Machine。提交后投递保证嵌套 dispatch 看到的是新节点，不会让外层迁移随后
覆盖嵌套迁移结果。

需要数据库级可靠投递时不要依赖 Emit；应使用自定义 Outcome 调用
`OutcomeRuntime.stage_effect()`，再通过 AtomicCommit/Outbox 提交。

## 8. 同步和异步是两套入口、同一套语义

同步入口包括 `start()`、`dispatch()`、`resume()`、`join()`；异步入口带 `_async` 或 `a`
前缀。

规则是：

- 同步 Action、Guard、Hook 可以在同步和异步入口使用；
- 异步 Action 必须走异步 Machine 入口，否则抛 `AsyncActionRequired`；
- 异步 Guard 必须走异步入口，否则抛 `AsyncGuardRequired`；
- EventBus 同步发布会在任何已知监听器执行前预检异步 handler；
- HookRegistry 同步发射同样拒绝已知异步 Hook；
- 引擎不会用 `asyncio.run()` 隐式接管应用事件循环。

异步和同步执行最终使用相同的阶段顺序、OutcomeRegistry 和状态语义。

## 9. Hook、Event 和 RuntimeEvent 的区别

| 机制 | 用途 | 是否推动图迁移 |
| --- | --- | --- |
| `Event` / `EventBus` | 领域输入、跨组件消息 | ReactiveRuntime 绑定后可以 |
| `HookRegistry` | 引擎生命周期扩展 | 否 |
| `RuntimeEvent` | 只读观测、流式调试、追踪 | 否 |

RuntimeEvent 由每次 `_hook()` 调用产生，带递增 sequence、run_id、parent_run_id、节点、
状态、Event 和 Transition 摘要。`stream_events()` 临时安装 observer 并按发生顺序产出。
observer 异常会被隔离，不改变图语义；Hook 异常会传播，应只在确实需要影响调用结果时使用。

## 10. 设计上的明确限制

- 核心不是 Petri Net，不支持单 Machine 多活动位置。
- EventLog 是事实日志，不是自动事件溯源数据库。
- 普通 PersistenceBinding 不保证快照和日志原子写入。
- TimeoutPolicy 的同步实现只能在 Action 返回后检查耗时，不能抢占线程。
- InMemory 实现用于参考行为和测试，不是多进程生产存储。
- Replay 会重新执行 Action，外部副作用必须自行幂等或通过 Outbox 隔离。
