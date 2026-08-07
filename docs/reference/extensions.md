# 扩展 API

扩展 API 通过独立模块导入，核心只保留最小运行入口。

## 事件和 Hook

```python
from bricks.engine.events import EventBus
from bricks.engine.events.hooks import HookRegistry
```

`EventBus` 是业务事件的发布订阅机制；`HookRegistry` 是引擎生命周期监听机制。
两者都支持优先级、`once` 和 `match`。优先级数字越小越先执行，同优先级保持订阅
顺序。

同步入口收到异步处理器时抛出 `AsyncActionRequired`；异步入口会等待处理器完成。

## 持久化

```python
from bricks.engine.persistence import (
    InMemoryEventLog,
    InMemorySnapshotStore,
    PersistenceBinding,
)
```

`SnapshotStore` 只保存当前运行快照，`EventLog` 只追加执行事实。通过
`PersistenceBinding(machine, store, event_log).attach()` 绑定。`ContextSnapshot` 带有
版本号、`graph_id` 和 `graph_version`，恢复时必须匹配当前 Graph。

`SnapshotStore` 的最小协议只有三个方法：

```python
class SnapshotStore(Protocol):
    def save(self, snapshot: ContextSnapshot) -> None: ...
    def load(self, run_id: str) -> ContextSnapshot | None: ...
    def delete(self, run_id: str) -> None: ...
```

因此外部 KV、数据库或文件实现不需要为了接入 Bricks 额外继承基类。一个只实现这三个
方法的完整示例见 `examples/custom_store.py`。

实现可以额外提供 `read_history(run_id) -> list[ContextSnapshot]`，然后通过
`PersistenceBinding.history()` 查询提交历史。这个方法是可选扩展，不影响最小存储协议。

异步数据库或网络存储应实现 `AsyncSnapshotStore` / `AsyncEventLog`，并使用
`AsyncPersistenceBinding`。异步绑定注册的是可等待 Hook，只能通过 `start_async()`、
`dispatch_async()` 等异步 Machine 入口推进，不会在事件循环中调用同步存储方法：

```python
binding = AsyncPersistenceBinding(machine, async_store, async_event_log).attach()
await machine.start_async()
```

`EventRecord` 还可以包含运行序号、节点、状态、迁移 ID 和父运行 ID。需要重放时显式
调用 `replay_events(graph, records)`；事件日志不会自动重放，也不会替外部系统保证
事务或幂等。通过 `Machine.update_context()` 产生的记录使用
`kind="context_update"`，用于审计和恢复判断；当前 `replay_events()` 会跳过它，调用方
需要自行决定是否重新应用这类控制面输入。

## 执行器边界

`ActionExecutor` 只负责调用一次 Action。它接收可调用对象、当前 `Context` 和输入
`Event`，返回 Action 的原始结果；它不解析 Graph、不选择迁移、不调用 `dispatch()`，
也不负责保存快照：

```python
from bricks.engine.runtime import ActionExecutor


class MyExecutor:
    def execute(self, action, context, event):
        return action(context, event)

    async def execute_async(self, action, context, event):
        result = action(context, event)
        if hasattr(result, "__await__"):
            return await result
        return result
```

通过 `Machine(graph, executor=MyExecutor())` 注入。同步入口只调用
`execute()`；异步入口只调用 `execute_async()`。执行器抛出的业务异常会原样交给
Machine，Machine 再按自己的生命周期和 `RetryPolicy` 处理；执行器不应该把异常偷偷
转换成迁移。

Machine 会在执行器外层组合 `CancellationToken` 和 `TimeoutPolicy`。因此一个外部执行器
无需重新实现这些策略，但必须理解同步超时只能在 Action 返回后检查，异步超时可以取消
等待中的协程。Fork 创建的子运行会复用父运行的执行器实例，保证同一张图在分支中保持
相同的执行边界。完整示例见 `examples/custom_executor.py`。

## Outcome 解释边界

`Machine` 只根据解释器返回的 `OutcomeDirective.CONTINUE` 或 `STOP` 推进迁移，不识别
领域 Outcome。扩展应从默认 Registry 派生新值，而不是修改共享对象：

```python
from bricks.engine.runtime import OutcomeDirective, default_outcome_interpreter

interpreter = default_outcome_interpreter().with_handler(
    CallTool,
    call_tool,
    directive=OutcomeDirective.CONTINUE,
)
machine = Machine(graph, outcome_interpreter=interpreter)
```

Effect 使用 `CONTINUE`，执行后仍会进入下一阶段；HumanInput、委派、终止等 Control 保持
默认 `STOP`。新 handler 接收窄 `OutcomeRuntime`，可以读取 Context、发布内部事件或暂存
可靠 Effect，但不能直接 dispatch、join 或替换 Graph。旧 `outcome_handlers` 参数仍按
Machine handler 兼容。handler 可以是同步或异步函数，异步 handler 必须通过异步 Machine 入口执行。
完整示例见 `examples/custom_outcome.py`。

需要把消息、Spider Request 或远程任务与运行提交绑定时，handler 使用
`runtime.stage_effect(topic, payload, effect_id=...)`，再配合
`AtomicPersistenceBinding`；不要先直接发送外部消息再保存快照。

## 迁移选择边界

默认 `DefaultTransitionSelector` 按迁移优先级、声明顺序和 Guard 选择第一条符合条件的边。
需要领域特定路由策略时实现 `TransitionSelector.select()` / `select_async()`，再通过
`Machine(graph, selector=...)` 注入。Selector 只读取 Graph、Context 和 Event，不执行
Action，也不修改运行位置；Fork 子运行会继承父运行的同一个 selector 实例，因此自定义
Selector 应保持无状态，或自行保证并发访问安全。

调度器决定什么时候推进 Machine，队列或 Transport 只负责把外部消息转换成 `Event`；
这些基础设施不属于 `bricks.engine`。

需要并发更新控制时，存储实现可以额外提供结构一致的
`save_if_current(snapshot) -> ContextSnapshot`。`PersistenceBinding` 会自动探测并使用
这个可选能力；内置内存实现使用 revision 做 compare-and-swap。没有这个方法的存储仍然
完全符合最小协议，但不提供并发覆盖检测。

## 策略

```python
from bricks.engine.policies import (
    CancellationToken,
    InMemoryIdempotencyStore,
    RetryPolicy,
    TimeoutPolicy,
)
```

- `CancellationToken`：协作式取消，Action 执行前后检查。
- `TimeoutPolicy`：异步 Action 使用可取消等待；同步 Action 只能在返回后检查耗时。
- `RetryPolicy`：控制 `Retry` 的最大等待次数和退避时间；可用 `retry_on` 显式声明节点
  Action 的可重试异常类型。
- `IdempotencyStore`：按运行实例和 Event ID 记录已消费事件，失败时释放占用的键。

## 组合语义

```python
from bricks.engine.semantics import (
    Parallel,
    ReactiveRuntime,
    SagaRuntime,
    Workflow,
)
```

这些对象是 Machine 的薄组合层：

- `Workflow` 提供批量运行和 DAG 拓扑校验。
- `ReactiveRuntime` 把 EventBus 事件路由到 `dispatch()` 或 `resume()`。
- `Parallel` 声明 Fork/Join 计划。
- `SagaRuntime` 根据迁移元数据记录并执行逆序补偿。

它们不复制 Graph 或 Machine 的核心执行逻辑。

## 定时和 Fork 运行时端口

`bricks.engine.scheduling` 提供 `WakeupScheduler` / `AsyncWakeupScheduler`、绑定器和
`dispatch_wakeup()`。它只定义外部定时服务的请求边界，不创建后台线程；SQL、Redis 或
云调度器实现只需保存和取消 `Wakeup`。

默认 Fork 使用进程内 `ForkController`。需要外部协调时实现 `ForkRuntime`，通过
`Machine(..., fork_runtime_factory=...)` 注入；同一个 factory 会传递给所有子运行。
