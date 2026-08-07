# 005：执行器与调度基础设施保持在引擎边界之外

## 决策

`bricks.engine` 只定义 `ActionExecutor` 协议和进程内 `InlineExecutor`。它不内置线程池、
进程池、远程 RPC、消息队列或调度器。

各对象的职责保持如下：

```text
ActionExecutor  执行一次 Action
Scheduler       决定何时推进 Machine
Queue/Transport 传递跨线程、跨进程或跨服务的 Event
SnapshotStore   保存和恢复 Context
EventLog        追加运行事实
```

## 原因

`Machine` 的职责是按照 Graph 消费事件并应用 Outcome；它不应该知道 Action 是在当前
进程、线程池还是远程服务中执行。把调度和传输塞进 Machine 会让同步、异步、恢复和
分布式场景相互耦合，也会迫使所有使用者接受同一套基础设施。

因此，外部执行器只需要实现：

```python
class ActionExecutor(Protocol):
    def execute(self, action, context, event): ...
    async def execute_async(self, action, context, event): ...
```

两条方法的最小契约是：`execute()` 返回普通结果，`execute_async()` 返回普通结果或
可等待结果；同步入口不会替外部执行器创建事件循环。执行器收到的 Action 可能来自
节点进入、节点退出或迁移动作，但它不需要区分这些来源，也不应该直接改变
`Context.node_id`。

`Machine(executor=...)` 仍然使用同一套 Graph 和 Context。上层调度器可以在执行前后
调用 `start()`、`dispatch()`、`resume()`、`join()` 或对应的异步入口，而不需要修改
图定义。

## 边界

- 执行器负责一次 Action 的调用结果，不负责推进下一条迁移。
- 调度器负责运行时机，不负责解释 Graph 的节点和 Guard。
- 队列或传输层负责把外部消息转换成 `Event`，不让 `EventBus` 感知具体中间件。
- 快照和日志只通过协议接入，不改变图的静态定义。
- 线程、进程和远程执行器的取消、重试、超时语义必须明确映射到引擎已有策略，
  不能假定跨进程取消等同于本地协作式取消。
- Fork 子运行复用父运行的执行器对象；如果执行器包含连接池或进程客户端，应由执行器
  自己定义其生命周期，不把资源管理塞进 Machine。

## 暂不实现

当前不创建 `RuntimeManager`、`SchedulerManager` 或“万能远程节点”。外部驱动只需要
组合 `SnapshotStore`、`PersistenceBinding`、`Event` 和 `Machine` 的公开入口；具体
示例见 `examples/external_driver.py`。等真实场景出现后，再在引擎外部按具体执行模型
提供独立实现和测试。
