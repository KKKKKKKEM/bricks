# 批量调用和运行流

## `invoke`

当上层已经有一组按顺序到达的事件时，可以直接使用 `Machine.invoke()`：

```python
machine = Machine(graph)
context = machine.invoke([
    ("submitted", {"source": "form"}),
    "approved",
])
```

Machine 处于 `CREATED` 时会自动启动；处于事件等待时，普通事件会自动走
`resume()`。Retry 等待仍然必须显式调用 `resume_retry()`，Fork 等待必须调用
`join()`。

`ainvoke()` 是对应的异步入口，输入格式相同。

## `stream`

`stream()` 为每个外部输入事件产出一个 `TransitionResult`：

```python
for result in Machine(graph).stream(["submitted", "approved"]):
    print(result.source, "->", result.target, result.status.value)
```

节点 Action 产生的内部 `Next` 会在外部结果返回前排空，不会重复产出一条额外的
结果。需要观察节点进入、迁移前后或外部 Emit 时，使用 HookRegistry 和 EventBus。

`astream()` 是异步生成器：

```python
async for result in machine.astream(["submitted", "approved"]):
    print(result.target)
```

同一个 Machine 的并发调用不在当前契约内；上层应串行消费输入。

## 运行事件流

如果需要调试、追踪或驱动编排界面，可以使用 `stream_events()`：

```python
for item in machine.stream_events(["submitted", "approved"]):
    print(item.name, item.run_id, item.sequence, item.node_id)
```

它产出 `RuntimeEvent`，包含生命周期名称、运行 ID、Graph ID、序号、当前节点、迁移
ID、输入事件、`Outcome.emit()` 发布的事件和错误信息。`astream_events()` 是异步版本。子图运行会复用父运行的观察器，
因此可以通过 `parent_run_id` 区分嵌套执行。

如果只需要观察某一类事件，可以传入一个简单的 `match` 谓词。谓词只影响当前生成器
返回的内容，不会阻止运行时执行，也不会影响已经注册的观察器：

```python
transition_events = machine.stream_events(
    ["submitted", "approved"],
    match=lambda item: item.name == "transition.after",
)
for item in transition_events:
    print(item.transition_id, item.status)
```

`match` 接收完整的 `RuntimeEvent`，所以也可以按 `graph_id`、`run_id`、`node_id`、
`event_name` 或 `parent_run_id` 筛选子图和嵌套运行事件。异步入口使用相同参数：
`astream_events(events, match=...)`。

运行事件流是观察接口，不携带 `Context` 或可执行对象；观察器异常不会改变图执行结果。
需要修改流程时仍应使用 Action、Outcome、Event 或 Hook。
