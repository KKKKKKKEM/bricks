# 快速开始

下面的例子把一个字符串转换为大写。它展示 Bricks 的最小闭环：声明 Node、构建并注册 Graph、再执行它。

```python
from bricks import Graph, Node, Output, Ports, Runtime


class Upper(Node):
    input_ports = Ports(text=str)
    output_ports = Ports(result=str)

    def execute(self, inputs, context):
        del context
        return Output(inputs["text"].upper(), port="result")


graph = Graph(entrypoint="upper").add("upper", Upper())

with Runtime() as runtime:
    runtime.register("upper.graph", graph)  # register() 会冻结并校验 Graph
    outputs = runtime.run("upper.graph", "bricks")

assert outputs == (Output("BRICKS", port="result"),)
```

单节点没有下游 Edge，因此它产生的 `Output` 会作为 `Runtime.run()` 的返回值。若把 output port 连接给另一个
Node，它只在同一张 Graph 内传播。

## 运行仓库中的示例

项目没有运行时第三方依赖；使用 uv 可直接执行示例：

```bash
uv run python examples/linear.py
uv run python examples/fan_in.py
uv run python examples/event_routing.py
uv run python examples/async_node.py
```

测试依赖尚未写入项目依赖组；在本地可临时安装并运行：

```bash
uv run --with pytest pytest -q
```

## 下一步

- Graph 内如何连线、输入怎样触发，见[核心概念](core-concepts.md)。
- 要通过事件启动另一张 Graph，见[运行语义：事件路由](runtime-semantics.md#事件路由与并发)。
- 想直接比较四种常见编排，见[常见编排方式](examples.md)。
