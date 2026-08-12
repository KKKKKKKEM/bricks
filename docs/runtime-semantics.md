# 运行语义

本页记录的是当前代码的行为契约，尤其是同步边界、并发和失败传播。

## 注册与直接执行

```python
with Runtime() as runtime:
    runtime.register("parse.graph", graph)
    outputs = runtime.run("parse.graph", "payload")
```

注册名在同一个 Runtime 内唯一。Graph 会在注册时冻结。入口有一个端口时，`run()` 的第二个参数整体作为该端口
的值；即使它是 Mapping 也不会被拆开。入口有多个端口时，必须传入端口名完全匹配的 Mapping。零输入入口只
接受 `None` 或 `{}`。

`Runtime.arun()` 会把 `run()` 放到工作线程，避免阻塞异步调用方；它不改变 Graph、Output 或 Event 的语义。

## 事件路由与并发

通常用 `on()` 组合注册事件路由并启动本地消费者：

```python
runtime.register("crawl.graph", crawl_graph)
runtime.on(
    "crawl.task.created",
    graph="crawl.graph",
    queue="crawl",
    concurrency=20,
)
runtime.emit("crawl.task.created", {"url": "https://example.com"})
runtime.wait_idle()
```

`on()` 等价于依次调用 `route()` 与 `consume()`；需要把 Router 和 Worker 分开部署时可以直接使用后二者。同一
事件类型可以有多个观察者和路由。`observe()` 独立注册同步观察者，`"*"` 观察所有类型，不承担 Graph 路由。
每条 route 都有稳定 subscription 身份：同名 subscription 的 Router 实例竞争消费，不同 subscription 各自收到
事件。

默认 `MemoryEventBus` 在调用 `emit()` 的线程同步调用观察者并提交路由任务；目标 Graph 不在源 Node 的调用
栈执行。默认 `MemoryTaskBackend` 使用线程池。`concurrency` 限制当前 Runtime/Worker 实例同时执行的完整
Graph 数量，而不是集群全局并发，也不是其中某一个 Node 的并发数。

## Slot 与逻辑并发

不传 `slots` 时，`consume()` 自动创建一个大小等于 `concurrency` 的池；显式传入同一个池即可让多个 Consumer
共享逻辑执行槽：

```python
from bricks import SlotPool

slots = SlotPool(size=10)
runtime.consume("requests", concurrency=20, slots=slots)
runtime.consume("responses", concurrency=6, slots=slots)
```

`concurrency` 是某个 Consumer 最多同时执行的 Graph 数，`slots.size` 是共享池最多同时承载的独立逻辑执行链
数，两者不要求相等。没有 Slot 的根 Work 会等待，不占用线程池 Worker；携带 Slot 的下游 Work 优先继续执行。
一个 Work 发出多个事件时，各分支共享同一个 Slot，并在全部结束后自动归还。

Slot 与 Work 链绑定而不是与线程绑定。它可由一个 Consumer 的 Worker 交给另一个 Consumer 的任意 Worker，
`Context.slot` 中的状态保持不变。显式 SlotPool 的生命周期由创建者管理；自动池由 GraphWorker 关闭。

`wait_idle(timeout)` 会交替等待事件总线和任务后端，直到由事件继续产生的工作也已完成。传 `0` 可以进行即时
空闲检查。

## 失败传播

| 发生位置 | 调用方看到的结果 |
| --- | --- |
| Graph 定义错误 | `GraphValidationError`（在 `freeze()` / `register()` 时） |
| Node 抛异常 | `ExecutionError`，带有 `graph` 和 `node` 信息 |
| 输入或输出类型不匹配 | `PortValueTypeError` |
| Node 返回非法值或端口 | `InvalidOutputError` |
| 半组输入遗留 | `IncompleteInputsError` |
| 同步观察者或事件传输失败 | 发布点抛出 `EventDispatchError` |
| 队列中异步 Graph 失败 | 下一次 `wait_idle()` 抛出该失败 |

默认事件总线即使一个观察者失败，也会继续调用同一事件的其余观察者，然后报告第一个失败。异步任务没有自动
重试；一次 `wait_idle()` 已报告的队列失败会被消费，不会在下一次等待时重复报告。

## 生命周期

推荐使用上下文管理器。`close()` 会先等待已接受的事件与任务完成，再依次关闭任务后端、事件总线和执行器。
关闭后，注册、运行或发布会抛出 `RuntimeClosedError`。

默认实现仅在进程内有效：没有持久化、事务、消息 ack、跨进程恢复、定时调度、死信队列、背压或 exactly-once
保证。不要把 `wait_idle()` 理解为跨进程消息确认。
