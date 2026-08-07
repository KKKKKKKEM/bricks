# 外部驱动

`Machine` 是一次运行实例，不需要长期驻留在进程内。外部队列、定时器或 RPC 服务可以
在收到消息时按 `run_id` 加载快照，恢复 Machine，投递一个 `Event`，再保存结果。

最小组合关系是：

```text
Transport message
       ↓ 转换
Event(name, payload, event_id)
       ↓
SnapshotStore.load(run_id)
       ↓
PersistenceBinding.restore(...)
       ↓
Machine.route(run_id, event)
       ↓
SnapshotStore.save(...)
```

外部驱动只负责三件事：

1. 将外部消息转换为稳定的 `Event.event_id`。
2. 根据 `run_id` 恢复正确的 Graph 和 Machine 运行实例。
3. 调用 `route()`、`resume_retry()` 或 `join()`，再把运行结果交给持久化绑定保存。

它不解释节点、Guard 或 Outcome，也不把队列、线程池和 RPC 实现放进 `bricks.engine`。
`SnapshotStore` 的最小接入面只有 `save/load/delete`；如果部署环境需要并发覆盖检测，
可以再按约定提供可选的 `save_if_current()`。`EventLog` 和 `IdempotencyStore` 的具体
实现同样可以由部署环境替换。

事件等待可以直接使用 `route()`：

```python
restored = PersistenceBinding.restore(
    graph,
    run_id,
    store,
    event_log=log,
    idempotency=idempotency_store,
)
restored.route(run_id, Event("approved", {"by": "alice"}, event_id="message-2"))
```

`Retry` 等待必须调用 `resume_retry()`，`Fork` 等待必须调用 `join()`；外部驱动不应
绕过运行实例自己的生命周期校验。

完整示例见 `examples/external_driver.py`。它用内存实现模拟两次进程启动，真实部署时只
需要替换存储、传输和调度部分。

只实现三个快照方法的外部存储示例见 `examples/custom_store.py`。
