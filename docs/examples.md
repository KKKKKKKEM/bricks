# 常见编排方式

本目录只保留与具体业务无关、可直接运行的编排示例。每个文件都可以单独运行：

```bash
uv run python examples/linear.py
uv run python examples/fan_in.py
uv run python examples/event_routing.py
uv run python examples/async_node.py
```

## 1. 线性处理

[`examples/linear.py`](../examples/linear.py) 展示最常见的数据处理链：一个 Node 的 Output 经 Edge 交给下一
个 Node。只有没有下游 Edge 的 Output 才会由 `Runtime.run()` 返回。

```python
graph = (
    Graph(entrypoint="strip")
    .add("strip", Strip())
    .add("upper", Upper())
    .connect("strip", "upper", source_port="text", target_port="text")
)

with Runtime() as runtime:
    runtime.register("linear", graph)
    outputs = runtime.run("linear", "  hello  ")
```

当两个 Node 都使用默认端口名 `default` 时，`.connect("strip", "upper")` 即可。端口名不同则显式传入
`source_port` 和 `target_port`。

## 2. 分支与汇聚

[`examples/fan_in.py`](../examples/fan_in.py) 中 `Split` 一次产生 `left` 与 `right` 两项 Output；`Add` 使用
`InputPolicy.ALL`，只有两个输入各到达一个值后才执行。

```python
graph = (
    Graph(entrypoint="split")
    .add("split", Split())
    .add("add", Add())
    .connect("split", "add", source_port="left", target_port="left")
    .connect("split", "add", source_port="right", target_port="right")
)
```

若 `ALL` Node 只收到部分输入，Graph 停止时会抛出 `IncompleteInputsError`，而不是悄悄丢弃数据。

`InputPolicy.ANY` 适合“任一输入到达即可处理”的消费者；它按 Ports 声明顺序从第一个就绪端口取一个值，因此
Node 必须能根据实际存在的 key 区分输入来源。

## 3. 用事件连接 Graph

[`examples/event_routing.py`](../examples/event_routing.py) 将同步的发布 Graph 与队列中的消费 Graph 分开：

```python
runtime.register("producer", producer)
runtime.register("consumer", consumer)
runtime.route(
    "message.created",
    graph="consumer",
    queue="messages",
    concurrency=2,
)

runtime.run("producer", "hello")
runtime.wait_idle()
```

发布 Node 使用 `context.emit("message.created", value)`。这不是 Graph 内 Output 的替代物：事件用于表达已发生
的事实，并可能启动多张 Graph；`wait_idle()` 确保已接受的级联任务完成后再读取结果。

## 4. 异步 Node

[`examples/async_node.py`](../examples/async_node.py) 展示 `AsyncNode`。它与 `Node` 有相同的 Ports、Output、
Edge 和 InputPolicy，只是 `execute()` 是 `async def`：

```python
class DelayedUpper(AsyncNode):
    input_ports = Ports(text=str)
    output_ports = Ports(result=str)

    async def execute(self, inputs, context):
        await asyncio.sleep(0.01)
        return Output(inputs["text"].upper(), "result")
```

队列路由中的 `concurrency` 限制整张 Graph 的同时执行数。对于单个需要等待 I/O 的 Node，继承 `AsyncNode`；
对于多份彼此独立的工作，使用 Event 路由和命名队列。
