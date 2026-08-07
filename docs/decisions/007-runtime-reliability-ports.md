# 007：定时、可靠副作用和远程 Fork 使用显式运行时端口

## 决策

Machine 保持单活动位置的图运行模型，不内置定时线程、消息代理或分布式 Worker。需要这些
能力时，通过三个独立端口接入：

```text
WakeupScheduler     保存到期唤醒请求
AtomicCommitStore   原子提交快照、事件事实和 Outbox Effect
ForkRuntime         替换本地 Fork/Join 协调实现
```

领域 Outcome handler 只接收 `OutcomeRuntime`，不能调用 Machine 的 dispatch、join、
snapshot 或 Hook。需要可靠执行的外部动作使用 `stage_effect()`，而不是在 handler 中直接
发送不可回滚的消息。

## 可靠副作用

`AtomicPersistenceBinding` 把当前快照、EventRecord 和 StagedEffect 作为一个
`AtomicCommit` 写入。每个 batch 都携带稳定的 `commit_id`；存储必须把相同 ID、相同内容的
重复提交识别为前一次成功，并拒绝相同 ID 的不同内容。提交失败时保留完全相同的 batch 和
Machine 暂存 Effect，调用方可在存储恢复后调用 `flush()`。即使数据库已提交但响应丢失，
重试也不会因旧 snapshot revision 永久冲突。外部 Worker 只消费已经提交的 Effect，并以
Effect ID 幂等发送。
Fork 子运行在父 ForkGroup 完成登记前产生事实或 Effect 时，同一批次保存该子运行快照，
避免出现“事实已提交但运行不可恢复”的窗口。需要跨重试去重的领域动作应提供稳定、确定的
Effect ID；发送成功后再标记 sent，因此投递语义是至少一次而不是恰好一次。

普通 `PersistenceBinding` 仍适合不需要 Outbox 的轻量场景；同一 Machine 不应同时绑定普通
和原子持久化实现。

## 定时唤醒

`Wait(delay)` 和 `Retry(delay)` 会由 `WakeupBinding` 转换成稳定、可序列化的 Wakeup。
Scheduler 只保存请求；真正到期后，外部驱动恢复 Machine 并调用 `dispatch_wakeup()`。
绑定器的 `sync()` 用于进程恢复后重新协调等待状态，避免把定时线程放进核心。
`due()` 是非破坏性轮询：只有成功恢复后绑定器才会取消 Wakeup；不使用绑定器的调用方必须
自行 ack/cancel。重复投递由稳定 Wakeup ID 和当前等待状态校验拒绝。

## Fork 边界

默认 `ForkController` 是进程内实现。`fork_runtime_factory` 可以注入其它 `ForkRuntime`，
并自动传递给子运行。远程实现仍必须维护 ForkGroup、路由、Join、快照和恢复契约；网络、
租约和 Worker 生命周期不进入 Machine。

## 单活动位置

一台 Machine 有且只有一个 `Context.node_id`。并行由 Fork 子运行表达，而不是把 Context
改成任意 Token 集合。这让恢复、幂等和事件路由保持确定。如果领域需要 Petri Net 或
大规模流处理，应提供独立 semantics/runtime，而不是扩大基础 Machine 的状态模型。
