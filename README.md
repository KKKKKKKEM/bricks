# Bricks

Bricks 是一个 Python typed graph runtime：Graph 内以 `Output` 和 `Edge` 传递局部数据，Graph 间以
`Event` 和 `Runtime` 连接领域工作流。

```text
Node -- Output / Edge --> Node
Graph -- Event / Runtime --> Graph
```

顶层 API 只有十个概念：`Ports`、`Node`、`AsyncNode`、`InputPolicy`、`Output`、`Edge`、`Graph`、
`Event`、`Context` 与 `Runtime`。

## 最小示例

```python
from bricks import Graph, Node, Output, Ports, Runtime


class Upper(Node):
    input_ports = Ports(text=str)
    output_ports = Ports(result=str)

    def execute(self, inputs, context):
        del context
        return Output(inputs["text"].upper(), "result")


graph = Graph(entrypoint="upper").add("upper", Upper())

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

默认实现提供内存事件分发、线程池队列并发、Graph 冻结与类型校验，以及可替换的 EventBus、TaskBackend、
GraphExecutor 协议。它不提供持久化、ack、进程恢复、定时器、死信队列或 exactly-once 语义。

```bash
uv run python examples/linear.py
uv run python examples/fan_in.py
uv run python examples/event_routing.py
uv run python examples/async_node.py
uv run --with pytest pytest -q
```
