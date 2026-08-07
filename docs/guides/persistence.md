# 快照和事件日志

## 绑定持久化

```python
from bricks.engine.persistence import (
    InMemoryEventLog,
    InMemorySnapshotStore,
    PersistenceBinding,
)

store = InMemorySnapshotStore()
log = InMemoryEventLog()
machine = Machine(graph)
binding = PersistenceBinding(machine, store, log).attach()

machine.start()
machine.dispatch("next")
restored = PersistenceBinding.restore(
    graph,
    machine.context.run_id,
    store,
    event_log=log,
)
```

绑定会在启动、事件完成、迁移错误和暂停/恢复等生命周期边界保存快照，并把启动、
外部事件和内部 `Next` 事件追加到日志。日志记录还会带上 `graph_id`、运行序号、节点、
状态、迁移 ID 和父运行 ID，方便跨子图追踪。

通过 `machine.update_context(...)` 进行的控制面更新也会保存快照，并追加一条
`kind="context_update"` 的 `EventRecord`。这条记录的事件名固定为
`__context_update__`，payload 是本次 Context 增量：

```python
machine.update_context(approved=True, reviewer="alice")
record = log.read(machine.context.run_id)[-1]
assert record.kind == "context_update"
assert record.payload == {"approved": True, "reviewer": "alice"}
```

控制面更新不会触发图迁移；它只是被持久化的运行事实。直接修改 `context.data` 不会被
`PersistenceBinding` 观察，也不会自动保存。

如果父运行包含 Fork 或运行时子图，子运行会共享父级的 HookRegistry。父级
`PersistenceBinding` 会在后代运行发生变化时重新保存完整的父子快照；子运行产生的日志
则使用自己的 `run_id`，可以独立查询：

```python
machine.route(child_run_id, "callback")
saved = store.load(machine.context.run_id)
child_records = log.read(child_run_id)
```

因此进程在父级 Join 之前退出时，恢复的仍是子运行最新的等待或完成位置，而不是父级
开始 Fork 时的旧副本。

没有 `join_event` 的 Fork Join 也会写入一条 `kind="join"` 的内部事实记录。该记录不
推动新的图迁移，但 `replay_events()` 会消费它并完成同一个 Join，因此重放不会停在
原本的 Fork `WAITING` 边界。

事件日志按 `run_id` 隔离。父级日志记录父级收到的 Fork/Join 事件，子级日志记录子级
自己的启动和外部事件；如果需要独立重放一个已经被外部路由的子运行，应读取该子运行
的日志并传入它对应的 Graph：

```python
child_records = log.read(child_run_id)
child_replayed = replay_events(child_graph, child_records)
```

父级 `replay_events()` 可以重新执行父级 Action 产生新的 Fork，但不会凭空读取所有后代
运行日志。对于外部路由到子运行的消息，快照恢复或按子运行 ID 分别重放是更明确的边界。

## 快照边界

`ContextSnapshot` 包含：

- 快照版本。
- `graph_id` 和 `run_id`。
- Graph definition `graph_version`；`ContextSnapshot.from_context()` 从 Context 继承它。
- 当前节点、生命周期、业务数据、等待信息和元数据。
- 最近一次迁移事件的可恢复字段。
- Fork 运行时存在时的父子运行快照。

快照不包含 Graph 对象、Action、执行器、Hook、EventBus 连接或临时资源。恢复时必须
传入当前 Graph，并且快照的 `graph_id` 和 `graph_version` 必须匹配；图定义升级应通过
版本迁移或新图 ID 明确处理。

`InMemorySnapshotStore` 的 `save_if_current()` 会使用快照 `revision` 做原子比较更新；
自定义存储只需要实现 `save(snapshot)`、`load(run_id)` 和 `delete(run_id)` 就能接入：

```python
class SnapshotStore:
    def save(self, snapshot): ...
    def load(self, run_id): ...
    def delete(self, run_id): ...
```

如果还实现 `save_if_current(snapshot)`，`PersistenceBinding` 会自动启用 revision/CAS
并发更新保护；没有这个可选方法时，保存仍然有效，但并发覆盖需要由外部驱动串行化。
完整的最小外部存储示例见 `examples/custom_store.py`。

幂等键不写入快照。需要跨进程重启继续去重时，应恢复 Machine 时传入同一个持久化的
`IdempotencyStore`，不能依赖内存实现自动恢复。

## 快照历史

`SnapshotStore` 的最小协议仍然只有 `save/load/delete`。如果存储额外实现
`read_history(run_id)`，可以通过绑定读取按提交顺序排列的快照历史：

```python
history = binding.history()
for snapshot in history:
    print(snapshot.revision, snapshot.context["status"])
```

这是可选的检查和时间旅行基础能力，不会改变当前运行实例，也不会让最小外部存储承担
历史保存职责。内置 `InMemorySnapshotStore` 提供该能力；外部存储可以按自己的保留策略
实现。选中的历史快照可以直接交给 `Machine.from_snapshot(graph, snapshot)` 创建独立的
检查运行实例；完整示例见 `examples/snapshot_history.py`。

## 事件日志边界

`EventLog` 是追加事实记录，适合审计、调试和追踪。它不是自动重放器：

- 不会自动重新执行 Action。
- 不会替外部副作用提供事务。
- 不会自动决定哪些事件可以重放。
- `InMemoryEventLog.read()` 返回隔离副本，调用方修改 payload 不会污染已写入记录。

需要重放时，上层必须根据事件类型、幂等策略和外部副作用规则显式实现。Bricks 提供
一个明确的辅助入口，但仍不会自动调用它：

```python
from bricks.engine.persistence import replay_events

replayed = replay_events(graph, log.read(run_id))
```

重放会重新执行 Action，跳过日志中的内部 `Next` 事实和 `context_update` 控制面记录，
并处理普通等待事件、Retry 恢复和 Fork Join 记录。当前版本不会自动把控制面更新重新
应用到新运行；如果业务需要重建人工输入或外部系统写入的数据，应由调用方在重放前后
显式调用 `update_context()`，或从快照恢复。外部副作用必须由调用方保证幂等；重放不是
事务回滚。

## 原子提交和 Outbox

需要可靠发送队列任务、Spider Request 或领域命令时，使用 `OutcomeRuntime.stage_effect()`
暂存意图，并选择 `AtomicPersistenceBinding`。它通过一个存储事务提交：

```text
ContextSnapshot + EventRecord + StagedEffect
```

`AtomicCommitStore.commit()` 是数据库适配器需要实现的原子边界。适配器必须持久化
`AtomicCommit.commit_id`，并让相同 ID、相同内容的重试返回原提交结果；相同 ID 对应不同
内容时必须报冲突。提交成功后 Worker 从 Outbox 读取 Effect、按 Effect ID 幂等发送，再标记
为 sent；提交失败时绑定保留原 batch，存储恢复后调用 `binding.flush()`。内置
`InMemoryAtomicCommitStore` 是行为参考，不是跨进程生产存储。对可能重试或重复投递的领域动作，应显式传入由领域主键构造的确定性
`effect_id`，不要依赖默认随机 ID。完整领域组合见 `examples/spider_adapter.py`。
