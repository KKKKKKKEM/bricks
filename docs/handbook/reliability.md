# 可靠性与生产运行

Bricks 的核心是内存状态机。可靠运行不是由一个“开启持久化”的布尔参数完成，而是由快照、
事件事实、事件幂等、Outbox、Wakeup 和外部驱动共同组成。本章说明每层解决什么问题，以及
仍需应用承担什么责任。

## 1. 三种持久化数据

| 数据 | 对象 | 回答的问题 |
| --- | --- | --- |
| 当前状态 | `ContextSnapshot` | 现在从哪里继续？ |
| 执行事实 | `EventRecord` | 过去处理过什么？ |
| 待发送副作用 | `StagedEffect` | 哪些外部动作已随状态提交但尚未发送？ |

它们不能互相替代。只存 EventRecord 需要重放 Action；只存 Snapshot 没有完整审计；直接在
Action 中调用外部系统则无法和状态提交保持一致。

## 2. ContextSnapshot

ContextSnapshot 包含：

- snapshot schema `version`；
- Graph 的 `graph_id` 和 `graph_version`；
- `run_id`；
- 独立 Context 视图；
- 可选完整 runtime snapshot，包括 Fork 子树；
- CAS 使用的 `revision`。

`ContextSnapshot.from_machine()` 会调用 `machine.snapshot()`，所以 pending Fork 及嵌套子
运行会被递归保存。恢复时 `to_machine_snapshot()` 把 revision 写回
`context.metadata["snapshot_revision"]`。

Graph 本身不进入快照。恢复方必须提供与 `graph_id/graph_version` 匹配的 Graph；异构
SubGraph 还要提供 `graph_resolver`。

## 3. 普通 PersistenceBinding

最小 SnapshotStore 协议只有：

```python
class SnapshotStore(Protocol):
    def save(self, snapshot): ...
    def load(self, run_id): ...
    def delete(self, run_id): ...
```

如果 Store 额外提供 `save_if_current()`，Binding 会自动使用 revision CAS。如果额外提供
`read_history()`，可以通过 `binding.history()` 查询历史。

PersistenceBinding 订阅 start、transition、pause/resume、after_resume、after_join 和
context.updated Hook。它还会识别父运行的子 Machine，记录子运行事实，并在 ForkGroup 已
登记时保存父运行完整快照。

普通 Binding 的限制是：SnapshotStore 和 EventLog 是两个调用，不能保证数据库事务原子性。
它适合测试、单进程工具和可以容忍审计日志短暂不一致的场景。

## 4. EventRecord 和重放

EventRecord 记录 Event 身份、source、target_run_id、graph version、运行 sequence、节点、
状态、迁移 ID 和父运行 ID。`kind` 常见值：

| kind | 含义 |
| --- | --- |
| `start` | 启动运行 |
| `event` | 外部事件迁移 |
| `internal` | source 是当前 run_id 的 Next 内部事件 |
| `retry` | Retry 恢复 |
| `join` | Fork Join |
| `failed` | transition.error |
| `context_update` | 控制面数据更新 |

`replay_events()` 会重新创建 Machine，并按日志处理 start、外部事件、Retry 和 Join；内部
Next 会由 Action 再次产生，所以日志中的 internal 记录被跳过。

重放不是数据库回滚，也不是 exactly-once：

- Action 会重新执行；
- `context_update` 当前只作为事实，不自动重放业务数据修改；
- 外部副作用必须有领域幂等键，或改用 Outbox；
- Graph 版本必须由调用方正确选择。

## 5. Snapshot revision 和并发控制

InMemorySnapshotStore 的 `save_if_current()` 体现参考 CAS 语义：

```text
提交 snapshot.revision == 当前 revision
    -> 保存 revision + 1
否则
    -> SnapshotConflictError
```

生产数据库应在一条条件更新或事务内完成比较和递增。出现冲突意味着另一个执行者已经推进
同一个 run，不能盲目覆盖；调用方应重新 load，判断当前状态是否已经包含本次事件。

## 6. AtomicCommit 和 Outbox

需要可靠外部副作用时，自定义 Outcome handler 调用 `runtime.stage_effect()`，不要直接发
HTTP、消息或任务：

```text
Action
  -> StagedEffect（仅 Machine 内暂存）
  -> transition.after
  -> AtomicCommit(snapshot, records, effects, commit_id)
  -> 数据库事务提交
  -> Worker 查询 pending effects
  -> 对外发送
  -> mark sent
```

AtomicCommitStore 必须在同一事务中校验 snapshot revision，并写入 snapshot、EventRecord、
Effect 和 commit identity。

### 6.1 commit_id 幂等契约

每个 AtomicCommit 创建时获得稳定 `commit_id`。Store 必须遵守：

- 第一次 ID：校验并提交整个 batch；
- 相同 ID、相同 batch：返回第一次提交结果，不再次检查旧 revision；
- 相同 ID、不同 batch：抛出冲突；
- commit_id 记录必须与业务 batch 在同一事务持久化。

这解决“数据库已经提交，但响应在网络中丢失”的窗口。Binding 捕获异常后保留完全相同的
batch，`flush()` 重试时 Store 可以识别前一次成功，而不会因 snapshot 已加一永久冲突。

### 6.2 Binding pending 状态

AtomicPersistenceBinding 一旦有 pending batch，会拒绝新的 Hook commit，要求调用方先
`flush()`。Store 明确返回成功后，Binding 才会：

- 把新 revision 写回正确的父或子 Machine；
- 从 Machine 中 ack 本批 StagedEffect；
- 清除 pending。

异步 Binding 额外使用 asyncio Lock 串行化 commit/flush。

### 6.3 Outbox 投递语义

内置参考 Store 的 Worker 接口是 `pending_effects()` 和 `mark_effect_sent()`。实际系统通常是：

1. 查询未发送行并加租约或行锁；
2. 使用 `effect.id` 作为下游幂等键；
3. 发送成功后标记 sent；
4. Worker 崩溃时允许再次发送。

这是至少一次投递。若下游不支持幂等，就无法仅靠本地数据库声称 exactly-once。

## 7. Event 幂等

配置 IdempotencyStore 后，Machine 在执行前 claim：

```text
key = run_id + ":" + event_id
```

边界规则：

- Guard 无匹配、before Hook 失败或迁移体失败：提交前 release；
- 节点位置和状态已经提交：保留 claim；
- transition.after、machine.after_dispatch 或 after_resume 失败：仍保留 claim；
- Wakeup 使用稳定 wakeup ID 作为 Event ID。

生产 Store 的 `claim()` 必须是原子操作，并根据业务保留期清理旧 key。进程内参考实现不支持
多 Worker 竞争。

## 8. Wait、Retry 和 WakeupScheduler

Wait/Retry 只在 Context 写入 `delay`、`due_at`、`resume_event`、kind 和 attempt；Machine 不
创建后台 timer。

WakeupBinding 在相关 Hook 后把 waiting 转换成 Wakeup：

```text
run_id + kind + node_id + attempt + event -> stable wakeup.id
```

Wakeup 包含 run、graph/version、due_at、event、kind、node 和 attempt。调度器到期后调用
`dispatch_wakeup()`；该函数先根据 Machine 当前 waiting 重新计算期望 Wakeup，所有身份字段
一致才允许恢复。旧定时任务、错误版本和被篡改请求会被拒绝。

持久化 Hook priority 为 -1000，WakeupBinding 为 -900，因此状态先提交，再安排唤醒。两步间
仍可能进程崩溃；恢复后调用 `binding.sync()` 重新协调即可关闭窗口。

## 9. Fork/Join 的恢复语义

Fork 为每个 branch 创建独立 Machine：

- 初始 data 是父 data 的深复制，再覆盖 branch data；
- metadata 写入直接 parent_run_id 和根 run_id；
- 子 Machine 共享 Graph 依赖、executor、EventBus、Hook、策略和 resolver；
- 每个子运行拥有独立 run_id、Context、attempt 和 waiting。

同步 Fork 顺序启动分支；异步 Fork 使用 task 和 semaphore 实现 `max_concurrency`。

### 9.1 Join 策略

| policy | finished 条件 | successful 条件 |
| --- | --- | --- |
| `all` | 所有子运行终止 | 默认所有子运行 COMPLETED |
| `any` | 任一成功，或所有分支终止 | 至少一个 COMPLETED |

failure_policy 修改失败解释：

- `fail`：失败分支使 Join 不成功；
- `continue`：全部达到 finished 即视为可继续，结果由 join payload 汇总；
- `fail_fast`：首个失败即可 finished，并停止尚未结束的兄弟。

`any` 成功或 `fail_fast` 结束时，剩余未终止子运行被标为 STOPPED，并记录 stop reason。

### 9.2 Join 提交边界

有 join_event 时，父运行先临时准备为 RUNNING，并 dispatch join_event；只有迁移体完成才清理
旧 Fork。无可用边、Guard 不通过、before Hook 或 Action 失败时恢复原 Context 和 Fork，调用方
可以修正条件后再次 join。

清理在 after Hook 之前完成，因此 persistence 看到已经提交的 Join。若 after Hook 失败，
旧 Fork 不会复活并导致业务重复。

### 9.3 子图

SubGraph 是 kind 为 `subgraph` 的单分支 Fork。成功 Join 前，第一子运行 data 会合并回父
Context。静态 `include()` 不创建子运行；运行时 SubGraph 有独立 run_id 和可恢复边界。

## 10. ReactiveRuntime 和消息路由

ReactiveRuntime 默认订阅 Graph 声明过的全部 event name：

- RUNNING -> dispatch；
- 普通 WAITING 且 event 符合 resume_event -> resume；
- Retry 等待忽略普通 EventBus 消息，必须 resume_retry；
- 多个运行都声明同一未定向事件时抛 AmbiguousEventRoute；
- `route(run_id, event)` 写入 target_run_id，实现明确路由。

多个 ReactiveRuntime 共享 EventBus 时仍共享路由登记表，不会各自误判为唯一候选。

Outcome.emit 在迁移提交后才发布，避免同一 Machine 的响应式重入破坏外层迁移。

## 11. 推荐生产拓扑

```text
API / Consumer
    -> load Graph by graph_id + version
    -> load Machine snapshot by run_id
    -> claim external message/event ID
    -> dispatch / resume / join
    -> AtomicCommit transaction

Timer worker
    -> query due Wakeup
    -> restore Machine
    -> dispatch_wakeup

Outbox worker
    -> query pending StagedEffect
    -> send with effect ID
    -> mark sent
```

还应具备：

- Graph 注册表和版本迁移策略；
- run_id 级串行消费或数据库 CAS；
- Event ID 与 Effect ID 的保留策略；
- pending AtomicCommit 的报警和 flush 重试；
- Wakeup 租约和重复投递；
- RuntimeEvent/Hook 的 tracing、metrics 与错误隔离；
- 对 FAILED、STOPPED、长期 WAITING run 的运维查询。

## 12. 故障边界速查

| 故障位置 | 状态是否可能已提交 | 正确处理 |
| --- | --- | --- |
| Guard / before_dispatch | 否 | 可用同一事件重试 |
| exit/action/enter | 否，默认 FAILED | 根据 RetryPolicy 或业务修复 |
| after_transition / after_dispatch | 是 | 不要重做同一 event ID |
| Store 明确未提交 | 否 | `flush()` 原 batch |
| Store 响应丢失 | 未知 | 用相同 commit_id `flush()` |
| Outbox 发送后 ack 丢失 | Effect 已外发 | 用 effect ID 幂等重发 |
| Wakeup 重复到达 | 可能已恢复 | stale 校验或 Event 幂等拒绝 |
| Join 迁移提交前失败 | Fork 保留 | 修正条件后再次 join |
| Join after Hook 失败 | Join 已提交 | 不得恢复旧 Fork |
