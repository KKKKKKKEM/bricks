# 动态路由

Bricks 的条件路由由多条同名迁移和 `Guard` 表达。节点不直接修改 Machine 的节点位置，
而是返回 `Outcome.next()` 产生一个内部事件；内部事件仍然必须经过 Graph 的迁移和
Guard 校验。

## 更新并路由

`Outcome.next()` 可以同时携带一次 `Context.data` 增量：

```python
def classify(context, event):
    return Outcome.next(
        "classified",
        update={"category": "priority"},
    )
```

对应的迁移仍然由 Graph 声明：

```python
builder.transition(
    "start",
    "classified",
    "priority_review",
    guard=lambda context, event: context.get("category") == "priority",
)
builder.transition(
    "start",
    "classified",
    "normal_review",
    guard=lambda context, event: context.get("category") == "normal",
)
```

这提供了类似“更新数据并选择下一步”的编排能力，但不增加一个与 `Outcome`、`Context`
并列的 `Command` 对象。所有路由仍然能被静态检查、Hook 观察和 EventLog 记录。

## 选择顺序

同一来源节点和事件的迁移按 `priority` 升序选择；相同优先级按声明顺序选择。Guard
全部不通过时，运行抛出 `NoTransition`，当前位置不会被偷偷跳过。

完整示例见 `examples/dynamic_route.py`。

## 控制面更新

如果数据来自人工审批、外部系统或恢复控制面，而不是当前节点 Action，可以使用
`Machine.update_context()`：

```python
machine.update_context(approved=True, reviewer="alice")
```

这次更新使用当前运行的 `Context`，会触发 `context.updated` Hook，但不会改变当前节点、
生命周期或等待条件。要继续等待中的图，仍需显式发送事件：

```python
machine.resume("approved")
```

这种分离很重要：Context 负责承载运行数据，Event 才负责推动图迁移。异步 Hook 场景使用
`await machine.update_context_async(...)`。直接写入 `machine.context.data` 不会触发 Hook
或 `PersistenceBinding` 的自动保存。

完整示例见 `examples/context_update.py`。

## 异步 Guard

Guard 默认可以是同步函数；如果 Guard 需要访问异步资源，可以定义为异步函数，并使用
`start_async()` / `dispatch_async()`：

```python
async def allowed(context, event):
    result = await permission_service.check(context.get("user_id"))
    return result.ok

await machine.start_async()
await machine.dispatch_async("approve")
```

同步入口不会偷偷运行异步 Guard。误用 `dispatch()` 时会抛出
`AsyncGuardRequired`，当前位置保持不变。`AllOf`、`AnyOf`、`Not` 和 `Predicate` 也会在
异步运行入口中等待内部 Guard。完整示例见 `examples/async_guard.py`。
