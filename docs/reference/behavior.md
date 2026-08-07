# 0.3 行为契约

本页是当前版本的行为基线。实现、测试和文档发生冲突时，应先修正其中一项，不能
默默改变运行语义。

## 同步与异步

| 操作 | 同步入口 | 异步入口 | 结果 |
| --- | --- | --- | --- |
| 启动同步 Action | `start()` | `start_async()` 也支持 | 正常执行 |
| 启动异步 Action | 抛出 `AsyncActionRequired` | `start_async()` 等待 | 同步运行失败时为 `FAILED` |
| 派发同步事件 | `dispatch()` | `dispatch_async()` 也支持 | 执行 Guard、迁移和 Action |
| 同步入口遇到异步 Action/Hook | 抛出 `AsyncActionRequired` | 不适用 | 当前调用中止 |
| 同步入口遇到异步 Guard | 抛出 `AsyncGuardRequired` | 不适用 | 当前节点和生命周期不变 |
| 异步入口遇到同步 Action/Hook | 支持 | 支持 | 返回普通值 |
| 异步入口遇到异步 Guard | 不适用 | 等待 | 再按 Guard 结果选择迁移 |
| 异步入口使用同步自定义节点扩展 | 不适用 | 支持 | 保留 `enter()` / `exit()` 覆盖逻辑 |
| EventBus 异步 Handler | `publish()` 拒绝 | `publish_async()` 等待 | Handler 按顺序执行 |
| 批量编排 | `invoke()` / `stream()` | `ainvoke()` / `astream()` | 按输入顺序消费 |

同步入口不会偷偷创建事件循环；同一个调用内的异步 Handler 和 Hook 按注册顺序等待。
并发调用同一个 Machine 的行为不属于当前 0.3 契约，应由上层串行化。

## 生命周期

| 场景 | 状态变化 | 备注 |
| --- | --- | --- |
| 新建运行 | `CREATED -> RUNNING` | `start()` 前不能派发事件 |
| 进入 Wait | `RUNNING -> WAITING` | 使用 `resume()` 恢复 |
| 进入 Retry | `RUNNING -> WAITING` | 使用 `resume_retry()` 恢复 |
| 人工暂停 | `RUNNING -> PAUSED` | 使用 `resume_run()` 恢复 |
| 进入终止节点 | `RUNNING -> COMPLETED` | 终止节点 Action 可返回其它 Outcome |
| `Stop` | `RUNNING -> STOPPED` | 保存 `stop_reason` |
| 外部取消异步 Task | `RUNNING -> STOPPED` | 保存 `stop_reason="task_cancelled"` 并继续抛出 `CancelledError` |
| `Fail` 或运行异常 | `RUNNING -> FAILED` | 保存失败信息或触发 `transition.error` |

`WAITING` 的 Fork 必须使用 `join()`；事件等待和重试等待不能互相替代。

事件等待的 `resume()` / `resume_async()` 会先按当前节点和完整 `Event` 解析迁移，解析
失败时保留原有 `WAITING` 状态和等待信息；只有迁移确认后才提交恢复。Guard 接收到的
始终是完整 `Event`，可以读取 `name`、`payload`、`source` 和 `event_id`。

`Machine.update_context()` 不属于事件消费或图迁移：它只合并 `Context.data`，保持当前
`node_id` 和 `status` 不变，并触发一次 `context.updated` Hook。异步 Hook 必须通过
`update_context_async()` 等待；控制面更新不会因为 Guard 条件满足而自动推进图。

## 事件消费

- 同一源节点和事件按 `priority` 升序、声明顺序选择迁移。
- Guard 全部不通过时抛出 `NoTransition`，运行位置不改变。
- 配置 `IdempotencyStore` 后，同一运行实例的同一 `event_id` 只能成功消费一次。
- 迁移失败会释放本次幂等键；成功消费的键保留。
- `Next` 生成新的内部 Event，仍然必须经过 Graph 上的迁移。
- 终止节点 Action 返回 `Next` 时，先继续消费内部事件；只有 Next 链结束后才完成运行。
- `Emit` 只发布到 EventBus，不直接改变 Machine 的节点。
- `RuntimeEvent` 会记录 `Emit` 发布的 `event.emitted` 事实；EventBus 仍负责实际订阅调用。
- `machine.after_resume` 在 Wait、Retry 或人工暂停恢复操作完成后触发；Retry 恢复会以
  `kind="retry"` 写入 EventLog，显式 Replay 会重新调用 `resume_retry()`。
- `machine.after_join` 在一个没有返回事件的 Fork Join 完成后触发；持久化绑定使用它保存
  父运行从 `WAITING` 回到可运行状态的快照，并记录一个 `kind="join"` 的可重放事实。

## 策略

- 取消在运行入口和 Action 执行前后协作式检查；不会强制终止同步线程。
- 异步超时通过取消等待中的协程实现；同步超时在 Action 返回后检查。
- `RetryPolicy` 控制 `Outcome.retry()` 的等待次数和退避；只有显式配置
  `retry_on=(...)` 时，节点进入 Action 抛出的匹配异常才会自动转成 Retry。
- `ActionExecutor` 只负责一次 Action 调用；节点进入、退出、迁移动作和 Fork 子运行都
  通过同一执行器边界，执行器异常不会被 Machine 改写成另一种异常。
- Fork 子运行继承父运行的取消、超时、重试和幂等策略对象。
- 异步 Fork 的 `max_concurrency` 限制同时启动的分支；`any` 等待任一分支成功，全部
  分支结束且没有成功时才失败。
- Fork 默认 `failure_policy="fail"`；`continue` 会把失败子运行快照交给 Join 事件，
  `fail_fast` 会在失败后停止尚未结束的兄弟分支。同步 Action 无法被强制中断，快速
  失败只会阻止尚未开始的同步分支，并在 Join 时停止等待分支。
- `route(run_id, event)` 会递归查找嵌套子运行，但不会绕过目标运行自己的等待和迁移
  校验；嵌套 Fork 应先 `join(child_run_id)`，再由父级 `join()`。

## Hook 错误

Hook 异常会向当前调用方传播。Hook 不是事务边界：如果异常发生在状态已经改变的
生命周期之后，已经发生的状态改变不会自动回滚；对应事件也视为已经成功消费，幂等键
不会因为观察 Hook 失败而释放。只有迁移、Action 或其它核心执行步骤失败时，当前事件
的幂等键才会释放，允许上层重试。

持久化绑定以较高优先级监听提交边界，因此用户的 `transition.after`、`after_start` 或
`after_resume` Hook 即使失败，已经提交的快照仍会先写入存储。

## 快照与日志

- `ContextSnapshot` 带有版本、`graph_id`、`graph_version` 和 `run_id`。
- `Context` 也保存 `graph_version`；通过 `ContextSnapshot.from_context()` 创建快照时会
  继承该版本。独立构造版本化 Graph 的 Context 时，应传入相同的 `graph_version`。
- 恢复必须使用相同 `graph_id` 和 Graph definition version 的 Graph。
- 快照保存恢复所需的 Context 和 Fork 运行时信息，不保存 Action、执行器或连接。
- `SnapshotStore.read_history()` 是可选能力；提供时，`PersistenceBinding.history()` 返回按
  提交顺序排列且与存储隔离的快照副本。
- 父级 `PersistenceBinding` 会保存嵌套 Fork/子图的最新子运行快照；子运行事件日志使用
  自己的 `run_id`，父子运行可以分别恢复和查询。
- EventLog 在迁移提交后记录可重放的 `event`；执行异常记录为仅审计的 `failed`，
  `replay_events()` 不会重新执行失败尝试。它不替外部副作用提供事务。
- EventLog 的读取结果是隔离副本，修改读取到的嵌套 payload 不会改写审计事实。
- `context_update` 事实记录控制面数据增量；`replay_events()` 当前会跳过这类记录，不会
  自动将增量写回新的 Context。
- Context 快照会递归冻结业务数据、元数据、等待信息和最近事件 payload；构造输入、运行中
  的嵌套对象及 `to_dict()` 的返回值都不会反向污染已经保存的快照。
