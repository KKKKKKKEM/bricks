# 完整使用指南

本章从最小用法开始，逐步加入真实项目常用能力。示例默认使用同步 API；异步用法在专门
章节说明。

## 1. 安装与导入

项目要求 Python 3.10 至 3.13。仓库开发环境可以直接执行：

```bash
uv sync
uv run python examples/basic_graph.py
```

最小入口：

```python
from bricks.engine import Event, GraphBuilder, Machine, Outcome, Status
```

## 2. 创建并运行第一张图

```python
from bricks.engine import GraphBuilder, Machine, Status


builder = GraphBuilder("approval", initial="draft", version="1")
builder.action("draft")
builder.action("review")
builder.terminal("approved")
builder.transition("draft", "submit", "review")
builder.transition("review", "approve", "approved")

graph = builder.build()
machine = Machine(graph)

machine.start()
machine.dispatch("submit", {"document_id": "doc-1"})
machine.dispatch("approve")

assert machine.status is Status.COMPLETED
assert machine.node_id == "approved"
```

Graph 可以复用：

```python
first = Machine(graph)
second = Machine(graph)

first.start()
second.start()
assert first.context.run_id != second.context.run_id
```

## 3. Action、Context 和 Event

Action 接收当前 Context 和触发本阶段的 Event：

```python
def prepare(context, event):
    context.set("document_id", event.payload["document_id"])
    context.set("prepared_by", event.source)
```

Action 返回 Mapping 时会自动合并到 data：

```python
def classify(context, event):
    return {
        "category": event.payload["category"],
        "received_event": event.name,
    }
```

如果希望控制面更新被持久化和 Hook 观察，使用：

```python
machine.update_context({"reviewer": "alice"}, priority="high")
```

不要把连接、锁、协程、Graph 或 callable 放进 data；快照数据应保持可序列化。

## 4. 条件路由

同一节点和事件可以有多条迁移，priority 较小者先检查：

```python
builder.transition(
    "review",
    "decide",
    "approved",
    guard=lambda context, event: context.get("score", 0) >= 80,
    priority=0,
)
builder.transition(
    "review",
    "decide",
    "rejected",
    guard=lambda context, event: context.get("score", 0) < 80,
    priority=10,
)
```

组合 Guard：

```python
from bricks.engine.graph import AllOf, Not, Predicate


is_adult = Predicate(lambda context, event: context.get("age", 0) >= 18)
is_blocked = Predicate(lambda context, event: context.get("blocked", False))

builder.transition(
    "review",
    "accept",
    "approved",
    guard=AllOf(is_adult, Not(is_blocked)),
)
```

没有 Guard 通过时，dispatch 抛 `NoTransition`，节点不会移动。

## 5. 使用 Outcome 表达控制意图

### 5.1 Update 和 Next

```python
def enrich(context, event):
    return Outcome.update(enriched=True)


def choose_next(context, event):
    return Outcome.next(
        "continue",
        payload={"source": "internal"},
        update={"step": context.get("step", 0) + 1},
    )
```

必须为 `continue` 在当前节点声明 Transition。Next 不会绕过 Guard。

### 5.2 Wait 和人工中断

```python
builder = GraphBuilder("manual-approval", initial="request_approval")
builder.action(
    "request_approval",
    lambda context, event: Outcome.interrupt(resume_event="approved"),
)
builder.terminal("done")
builder.transition("request_approval", "approved", "done")

machine = Machine(builder.build())
machine.start()

assert machine.status is Status.WAITING
machine.resume("approved", {"reviewer": "alice"})
```

也可以直接声明 WaitNode：

```python
timer_builder = GraphBuilder("timer", initial="cooldown")
timer_builder.wait("cooldown", delay=30, resume_event="wake")
timer_builder.terminal("done")
timer_builder.transition("cooldown", "wake", "done")
```

### 5.3 Retry

```python
from bricks.engine.policies import RetryPolicy


def call_service(context, event):
    if context.attempt < 2:
        return Outcome.retry(
            event="try_again",
            reason="service unavailable",
        )
    return {"result": "ok"}


retry_builder = GraphBuilder("service-call", initial="call")
retry_builder.action("call", call_service)

machine = Machine(
    retry_builder.build(),
    retry_policy=RetryPolicy(
        max_attempts=3,
        backoff=0.5,
        exponential=True,
    ),
)
machine.start()
machine.resume_retry()  # Action 收到 event.name == "try_again"
```

节点抛出的特定异常也可以自动转换为 Retry：

```python
policy = RetryPolicy(
    max_attempts=3,
    backoff=1,
    retry_on=(ConnectionError, TimeoutError),
)
```

### 5.4 Emit

```python
from bricks.engine.events import EventBus


events = EventBus()
events.on("audit.created", lambda event: print(event.payload))

emit_builder = GraphBuilder("emit-example", initial="work")
emit_builder.action(
    "work",
    lambda context, event: Outcome.emit(
        "audit.created",
        {"run_id": context.run_id},
    ),
)
machine = Machine(emit_builder.build(), events=events)
machine.start()
```

Emit 在当前状态变化提交后投递，适合进程内通知。可靠跨进程消息使用 Outbox。

### 5.5 Stop 和 Fail

```python
return Outcome.stop("cancelled by operator")
return Outcome.fail({"code": "invalid_document"})
```

原因分别写入 `metadata["stop_reason"]` 和 `metadata["failure"]`。

## 6. 异步运行

```python
import asyncio

from bricks.engine import GraphBuilder, Machine


async def fetch(context, event):
    await asyncio.sleep(0)
    return {"fetched": True}


async def allowed(context, event):
    await asyncio.sleep(0)
    return context.get("enabled", True)


builder = GraphBuilder("async-job", initial="ready")
builder.action("ready")
builder.terminal("done", fetch)
builder.transition("ready", "run", "done", guard=allowed)
machine = Machine(builder.build())


async def main():
    await machine.start_async()
    await machine.dispatch_async("run")


asyncio.run(main())
```

一旦 Graph、Hook、EventBus listener、存储或调度器中有异步实现，应从最外层一直使用异步
入口，不要在同步入口外包 `asyncio.run()` 逐段混用。

## 7. 批量调用和流式结果

```python
context = Machine(graph).invoke(
    [
        ("submit", {"id": 1}),
        "approve",
    ]
)
```

逐个取得外部迁移结果：

```python
machine = Machine(graph)
for result in machine.stream(["submit", "approve"]):
    print(result.source, result.event.name, result.target, result.status)
```

观察所有生命周期事实：

```python
machine = Machine(graph)
for runtime_event in machine.stream_events(["submit", "approve"]):
    print(runtime_event.sequence, runtime_event.name, runtime_event.node_id)
```

异步版本分别是 `ainvoke()`、`astream()` 和 `astream_events()`。

## 8. EventBus、Hook 和 ReactiveRuntime

### 8.1 EventBus

```python
events = EventBus()

subscription = events.on(
    "document.approved",
    lambda event: print(event.payload),
    priority=10,
    once=False,
    match=lambda event: event.source == "api",
)

events.publish("document.approved", {"id": "doc-1"})
events.unsubscribe(subscription)
```

### 8.2 生命周期 Hook

```python
def audit(hook):
    print(
        hook.name,
        hook.context.run_id,
        hook.event.name if hook.event else None,
    )


machine.hooks.on("transition.after", audit)
```

Hook 会影响调用结果；纯 tracing 更适合 RuntimeEvent observer/stream。

### 8.3 响应式驱动

```python
from bricks.engine.events import EventBus
from bricks.engine.semantics import ReactiveRuntime


events = EventBus()
machine = Machine(graph, events=events)
runtime = ReactiveRuntime(events)
binding = runtime.attach(machine)

events.publish("submit", {"id": "doc-1"})
runtime.route(machine.context.run_id, "approve")

binding.close()
```

共享总线有多个 Machine 时，应始终使用 `route(run_id, ...)`，避免未定向事件歧义。

## 9. 快照、日志和恢复

```python
from bricks.engine.persistence import (
    InMemoryEventLog,
    InMemorySnapshotStore,
    PersistenceBinding,
)


store = InMemorySnapshotStore()
event_log = InMemoryEventLog()
machine = Machine(graph)
binding = PersistenceBinding(machine, store, event_log).attach()

machine.start()
machine.dispatch("submit")
run_id = machine.context.run_id

restored = PersistenceBinding.restore(
    graph,
    run_id,
    store,
    event_log=event_log,
)
restored.dispatch("approve")
```

自定义 Store 只需实现 save/load/delete；生产场景建议额外实现 revision CAS。

事件重放：

```python
from bricks.engine.persistence import replay_events


records = event_log.read(run_id)
replayed = replay_events(graph, records)
```

只有 Action 可安全重做时才使用 replay。

## 10. 定时 Wait 和 Retry

```python
from datetime import datetime, timezone

from bricks.engine.scheduling import (
    InMemoryWakeupScheduler,
    WakeupBinding,
    dispatch_wakeup,
)


scheduler = InMemoryWakeupScheduler()
binding = WakeupBinding(machine, scheduler).attach()
machine.start()

for wakeup in scheduler.due(datetime.now(timezone.utc)):
    restored = machine  # 生产环境通常按 wakeup.run_id 从 Store 恢复
    dispatch_wakeup(restored, wakeup)
```

进程重启后先恢复 Machine，再执行 `binding.sync()`，重新建立当前 waiting 对应的 Wakeup。

## 11. Fork、路由和 Join

```python
def fan_out(context, event):
    return Outcome.fork(
        {"event": "process", "data": {"item": "a"}},
        {"event": "process", "data": {"item": "b"}},
        join_event="joined",
        policy="all",
        failure_policy="continue",
        max_concurrency=4,
    )


fork_builder = GraphBuilder("fan-out", initial="start")
fork_builder.action("start")
fork_builder.action("forking", fan_out)
fork_builder.terminal("child_done")
fork_builder.terminal("done")
fork_builder.transition("start", "fork", "forking")
fork_builder.transition("start", "process", "child_done")
fork_builder.transition("forking", "joined", "done")

machine = Machine(fork_builder.build())
machine.start()
machine.dispatch("fork")

group = machine.fork_group
assert group is not None
for child in group.children:
    print(child.context.run_id, child.status, child.context.data)

machine.join()
```

等待中的子运行可以定向驱动：

```python
child_run_id = group.children[0].context.run_id
machine.route(child_run_id, "wake", {"result": 42})
machine.join()
```

嵌套 Fork 同样通过根 Machine 的 `route(run_id, ...)` 递归查找。

## 12. 子图和静态组合

运行时独立子图：

```python
parent_builder = GraphBuilder("order", initial="payment")
parent_builder.subgraph(
    "payment",
    payment_graph,
    entry_event="charge",
    return_event="paid",
    data={"currency": "CNY"},
)
parent_builder.transition("payment", "paid", "done")
```

静态 include：

```python
entry = parent_builder.include(validation_graph, prefix="validation")
parent_builder.transition("start", "validate", entry)
```

需要独立生命周期、run_id 和恢复边界时选 SubGraph；只想复用一段图结构时选 include。

## 13. AtomicCommit 和可靠副作用

先定义领域 Outcome：

```python
from dataclasses import dataclass

from bricks.engine.runtime import (
    Outcome,
    OutcomeDirective,
    default_outcome_interpreter,
)


@dataclass(frozen=True)
class SendEmail(Outcome):
    message_id: str
    recipient: str


def stage_email(runtime, outcome):
    runtime.stage_effect(
        "email.send",
        {"recipient": outcome.recipient},
        effect_id=f"email:{outcome.message_id}",
    )


interpreter = default_outcome_interpreter().with_handler(
    SendEmail,
    stage_email,
    directive=OutcomeDirective.CONTINUE,
)
```

绑定原子 Store：

```python
from bricks.engine.persistence import (
    AtomicPersistenceBinding,
    InMemoryAtomicCommitStore,
)


store = InMemoryAtomicCommitStore()
email_builder = GraphBuilder("email-job", initial="ready")
email_builder.action(
    "ready",
    lambda context, event: SendEmail("message-1", "user@example.test"),
)
machine = Machine(
    email_builder.build(),
    outcome_interpreter=interpreter,
)
binding = AtomicPersistenceBinding(machine, store).attach()

machine.start()

for effect in store.pending_effects(topic="email.send"):
    send_email(effect.payload)
    store.mark_effect_sent(effect.id)
```

如果 commit 抛错，修复存储后调用 `binding.flush()`；不要重新 dispatch 原业务事件。

## 14. Graph 序列化和版本

```python
actions = {
    "prepare": prepare,
    "classify": classify,
}

definition = graph.to_dict(
    action_serializer=lambda action: next(
        name for name, value in actions.items() if value is action
    )
)

from bricks.engine import Graph


restored_graph = Graph.from_dict(
    definition,
    action_resolver=actions.__getitem__,
)
```

生产中应使用稳定注册名，并将 Graph definition schema version 与业务 graph_version 分开：

- schema version 决定 JSON 结构如何解析；
- graph_version 决定运行快照应该匹配哪一版业务流程。

## 15. 自定义扩展

### 15.1 自定义节点

继承 BaseNode，并同时实现同步和异步 enter；使用 `builder.add_node()` 注册。需要序列化时提供
node serializer/resolver。

### 15.2 自定义 Outcome

优先使用 `OutcomeRegistry.with_handler()` 和 OutcomeRuntime 窄端口。只有兼容旧集成时才使用
`outcome_handlers` 直接接收 Machine。

### 15.3 自定义 TransitionSelector

实现 `select()` 和 `select_async()`，通过 `Machine(selector=...)` 注入。Selector 应只选择边，
不要执行节点或直接修改 Context。

### 15.4 自定义 ActionExecutor

实现 `execute/execute_async`。远程执行器需要自行定义参数序列化、租约、取消和结果幂等；
Machine 仍把它视为一次 Action 调用。

### 15.5 自定义基础设施

按需要实现：

- SnapshotStore / AsyncSnapshotStore；
- EventLog / AsyncEventLog；
- AtomicCommitStore / AsyncAtomicCommitStore；
- WakeupScheduler / AsyncWakeupScheduler；
- IdempotencyStore；
- ForkRuntimeFactory。

## 16. Workflow、Parallel 和 Saga 外观

```python
from bricks.engine.semantics import Workflow


workflow = Workflow(graph)
machine = workflow.run(["submit", "approve"], data={"owner": "alice"})
```

ParallelPlan 生成 Fork Outcome；SagaRuntime 从 Transition metadata 的 `compensate` callable
自动收集补偿步骤，并按 LIFO 执行：

```python
from bricks.engine.semantics import CompensationPlan, SagaRuntime


plan = CompensationPlan()
saga = SagaRuntime(machine, plan)

# 正向迁移完成后，失败边界由应用显式决定何时补偿。
results = saga.compensate()
saga.close()
```

## 17. 测试建议

至少覆盖：

- 每个 Event 在每个可能 source 的路由；
- Guard 真、假和异步路径；
- Action 在 exit、transition、enter 阶段返回控制 Outcome；
- Wait/Retry/Fork 快照恢复；
- 重复 event ID；
- after Hook 失败时业务不重做；
- Store 提交前失败和提交后响应丢失；
- Wakeup 重复投递和旧 graph version；
- Fork 的 all/any、fail/continue/fail_fast；
- 同步入口误用异步 Action/Guard/Hook。

仓库验证命令：

```bash
uv run --with pytest pytest -q
uv run --with mypy mypy bricks
```

## 18. 常见错误

| 现象 | 原因 | 处理 |
| --- | --- | --- |
| `MachineNotStarted` | start 前 dispatch | 先 start 或使用 invoke |
| `MachineNotRunnable` | 用 dispatch 驱动 WAITING | 根据 waiting 使用 resume/retry/join |
| `NoTransition` | 当前节点没有符合 Guard 的边 | 检查 node_id、event.name 和 Context |
| `AsyncActionRequired` | 同步入口遇到 async callable | 改用异步入口 |
| `DuplicateEvent` | event ID 已提交 | 查询当前快照，不要换 ID 重做 |
| `SnapshotConflictError` | 多 Worker 同时推进 run | reload 并做 run 级串行化 |
| Atomic binding pending | 上次 commit 结果未知 | 用同一 binding.flush() |
| `AmbiguousEventRoute` | 多运行共享未定向事件名 | 使用 ReactiveRuntime.route |
| Wakeup stale | run 已恢复或等待声明改变 | ack/drop 旧 Wakeup |
