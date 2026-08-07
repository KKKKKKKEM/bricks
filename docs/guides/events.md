# 事件和 Hook

Bricks 有两个相似但边界不同的机制：

```text
EventBus      业务事件和外部消息
HookRegistry  Machine 生命周期
Reactive      把业务事件路由到 Machine
```

## EventBus

```python
from bricks.engine.events import EventBus

events = EventBus()
events.on("payment.completed", lambda event: print(event.payload))
events.publish("payment.completed", {"amount": 100})
```

订阅规则：

- 优先级数字越小越先执行。
- 同优先级按照订阅顺序执行。
- `once=True` 只处理一次。
- `match` 返回假时跳过当前事件。
- `"*"` 可以订阅所有事件。

同步 `publish()` 不会偷偷运行协程；遇到异步处理器会抛出
`AsyncActionRequired`。异步处理器使用 `publish_async()`。

`Event` 会在创建时复制 payload，`RuntimeEvent` 也会与原始消息和观察者返回的字典
隔离；监听器可以读取消息，但不应依赖修改 payload 来改变图运行数据。

## Hook

```python
machine.hooks.on(
    "transition.after",
    lambda hook: print(hook.transition.id, hook.result.status),
)
```

当前生命周期名称包括：

- `machine.before_start` / `machine.after_start`
- `machine.before_dispatch` / `machine.after_dispatch`
- `transition.before` / `transition.after` / `transition.error`
- `node.enter` / `node.exit`
- `event.unhandled`
- `machine.paused` / `machine.resumed` / `machine.after_resume` / `machine.after_join`

`machine.resumed` 表示运行已经从等待或暂停状态恢复，`machine.after_resume` 表示恢复
操作（包括 Retry 节点重新执行）已经完成。持久化绑定使用后者保存最终快照。

Hook 异常会中断当前调用并向调用方传播。Hook 是观察和拦截边界，不应直接修改
Graph；如果 Hook 需要改变业务流程，应发布 Event 或让 Action 返回 Outcome。

## ReactiveRuntime

```python
from bricks.engine.events import EventBus
from bricks.engine.semantics import ReactiveRuntime

events = EventBus()
machine = Machine(graph, events=events)
binding = ReactiveRuntime(events).attach(machine)
events.publish("next", {"source": "webhook"})
binding.close()
```

Machine 处于 `RUNNING` 时，响应式运行时调用 `dispatch()`；处于事件等待时调用
`resume()`；重试等待不会被普通事件误消费。异步 Machine 使用
`await ReactiveRuntime(events).attach_async(machine)` 和 `publish_async()`。

共享同一个 EventBus 的运行（即使属于不同 ReactiveRuntime）声明相同事件时，未定向事件
会抛出 `AmbiguousEventRoute`，不会广播给所有运行。使用运行 ID 明确定向；异步绑定使用
`await runtime.route_async(...)`：

```python
runtime = ReactiveRuntime(events)
runtime.attach(first)
runtime.attach(second)
runtime.route(first.context.run_id, "next", {"source": "webhook"})
```

需要广播时应由上层显式枚举目标运行并分别调用 `route()`，这样路由行为可以审计和持久化。

## 外部消息和恢复

外部系统只需要负责把自己的消息转换成 `Event`，不需要让 `EventBus` 认识 HTTP、队列
或 RPC。长运行实例可以先绑定 `PersistenceBinding`，进程重启后恢复 Machine，再绑定
一个新的 `ReactiveRuntime`：

```python
restored = PersistenceBinding.restore(
    graph,
    run_id,
    store,
    event_log=log,
)
ReactiveRuntime(EventBus()).attach(restored, auto_start=False)
```

恢复后的 Machine 会根据 `Context.waiting` 继续判断外部事件应该进入 `resume()` 还是
`dispatch()`。事件的唯一 ID 由外部适配器传入，若需要跨进程去重，还应在恢复时传入同
一个持久化的 `IdempotencyStore`。

完整的消息转换、保存、模拟进程重启和继续审批示例见
`examples/external_events.py`。
