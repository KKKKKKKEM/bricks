# Bricks

Bricks 是一个 Python typed graph runtime：Graph 内以 `Output` 和 `Edge` 传递局部数据，Graph 间以
`Event` 和 `Runtime` 连接领域工作流。

```text
Node -- Output / Edge --> Node
Graph -- Event / Runtime --> Graph
```

顶层 API 还提供 `Slot` 与 `SlotPool`：队列 Work 可以跨 Consumer 传递并复用代理、Cookie、连接等执行状态，
而不依赖具体线程。

执行默认不限步数和时长；`run()`、`start()` 和事件 route 可按需设置 `max_steps` 与 Graph `timeout`，单次
Node firing 的时限由该 Node 的 `timeout` 属性声明。`start()` 返回可查询和协作式取消的 `Execution`。
`Execution` 可调用 `result()`、直接 `await`，也可同步或异步迭代 terminal Output；`Runtime.iter()` 和
`Runtime.aiter()` 提供对应便利入口。

## 最小示例

```python
from bricks import Graph, Node, Output, Ports, Runtime


class Upper(Node):
    input_ports = Ports(text=str)
    output_ports = Ports(result=str)
    timeout = 5

    def execute(self, inputs, context):
        del context
        return Output(inputs["text"].upper(), "result")


graph = Graph(entrypoint="upper").add(upper=Upper())

with Runtime() as runtime:
    runtime.register("upper.graph", graph)
    print(runtime.run("upper.graph", "bricks"))
```

更多内容从[文档首页](docs/README.md)开始：

- [快速开始](docs/getting-started.md)
- [核心概念](docs/core-concepts.md)
- [运行语义](docs/runtime-semantics.md)
- [Runtime 扩展](docs/extending-runtime.md)
- [常见编排方式](docs/examples.md)

## 当前范围

默认实现提供内存事件分发、线程池队列并发、Graph 冻结与类型校验，以及可替换的 EventBus、TaskPublisher、
TaskConsumer、TaskBackend 和 GraphExecutor 协议。它不提供持久化、ack、进程恢复、定时器、死信队列或
exactly-once 语义。

```bash
uv run python examples/linear.py
uv run python examples/fan_in.py
uv run python examples/cycle.py
uv run python examples/event_routing.py
uv run python examples/async_node.py
uv run python examples/output_stream.py
uv run --with pytest pytest -q
```
