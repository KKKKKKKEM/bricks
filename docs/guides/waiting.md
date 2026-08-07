# 等待、重试和生命周期

## Wait

Action 可以返回：

```python
from bricks import Outcome


def wait_for_callback(context, event):
    return Outcome.wait(delay=30, resume_event="callback.received")
```

运行进入 `Status.WAITING`，恢复时必须调用：

```python
machine.resume("callback.received", {"ok": True})
```

`resume_event=None` 表示不限定事件名称。等待的计时信息会保存在 `Context.waiting`。
核心不创建调度线程，但提供显式外部端口：

```python
from bricks.engine.scheduling import (
    InMemoryWakeupScheduler,
    WakeupBinding,
    dispatch_wakeup,
)

scheduler = InMemoryWakeupScheduler()
binding = WakeupBinding(machine, scheduler).attach()
machine.start()

for wakeup in scheduler.due():
    dispatch_wakeup(machine, wakeup)
```

生产环境可将 Scheduler 协议实现为 SQL、Redis 或云定时服务。恢复等待中的 Machine 后
调用 `binding.sync()`，可以重新协调尚未登记的唤醒请求。
`due()` 不会取走请求，而是支持至少一次轮询；`dispatch_wakeup()` 成功后，绑定器会根据
最新运行状态取消它。不使用 `WakeupBinding` 时，调度适配器必须在成功恢复后自行取消或
确认 Wakeup，失败时保留它以便重试。

## Retry

```python
from bricks.engine.policies import RetryPolicy

machine = Machine(graph, retry_policy=RetryPolicy(max_attempts=3, backoff=1))
```

Action 返回 `Outcome.retry()` 后，运行进入重试等待，使用
`resume_retry()` 或 `resume_retry_async()` 重新执行当前节点。重试次数保存在
`Context.attempt`。超过策略上限后进入 `FAILED`。

如果希望把节点 Action 抛出的特定异常也转成同一套等待语义，可以显式配置
`retry_on`；默认值为空，不会改变异常传播行为：

```python
machine = Machine(
    graph,
    retry_policy=RetryPolicy(
        max_attempts=3,
        backoff=1,
        retry_on=(TimeoutError, ConnectionError),
    ),
)
```

只有节点进入 Action 抛出匹配异常时才会自动进入 Retry；迁移 Action、Hook、取消信号和
未匹配的异常仍按普通错误处理。异常会以类型和消息摘要保存到
`Context.waiting["reason"]`，避免把不可序列化的异常对象写入快照。

## 暂停

`pause()` 是人工暂停，`resume_run()` 恢复到可运行状态；它与 Action 产生的
`Wait` / `Retry` 等待不同。Fork 等待必须使用 `join()` 或 `join_async()`。

异步调用方直接取消正在运行的 Task 时，Machine 会把未完成运行标记为 `STOPPED`，并
保留 `stop_reason="task_cancelled"`；取消异常仍会交给调用方处理。

## 取消、超时和幂等

```python
from bricks.engine.policies import CancellationToken, TimeoutPolicy

token = CancellationToken()
machine = Machine(graph, cancellation=token, timeout=TimeoutPolicy(10))
token.cancel()
```

取消是协作式的：引擎不会强制终止同步线程，只在运行入口和 Action 执行前后检查。
异步超时会取消等待中的协程；同步 Action 在返回后检查耗时。

为 Machine 配置 `IdempotencyStore` 后，重复的 `Event.event_id` 会被拒绝；事件
处理失败会释放幂等键，允许上层安全重试。
