# 第二章：第一张 Graph

上一章建立了 Output、Event 和插件边界的整体模型。本章只做一件事：定义并运行一张最小 Graph。

下面的例子把一个字符串转换为大写。它展示 Bricks 的最小闭环：声明 Node、构建并注册 Graph、再执行它。

```python
from bricks import Graph, Node, Output, Ports, Runtime


class Upper(Node):
    input_ports = Ports(text=str)
    output_ports = Ports(result=str)
    timeout = 5  # None 表示该 Node 的单次执行不限时

    def execute(self, inputs, context):
        del context
        return Output(inputs["text"].upper(), port="result")


graph = Graph(entrypoint="upper").add(upper=Upper())

with Runtime() as runtime:
    runtime.register("upper.graph", graph)  # register() 会冻结并校验 Graph
    outputs = runtime.run("upper.graph", "bricks")

assert outputs == (Output("BRICKS", port="result"),)
```

这段程序经历了一个完整但很短的生命周期：

```mermaid
flowchart LR
    NodeDef[定义 Upper Node] --> GraphDef[绑定到 Graph]
    GraphDef --> Register[Runtime.register]
    Register --> Freeze[冻结并校验]
    Freeze --> Run[Runtime.run]
    Run --> Execute[Upper.execute]
    Execute --> Output[terminal Output]
```

单节点没有下游 Edge，因此它产生的 `Output` 会作为 `Runtime.run()` 的返回值。若把 output port 连接给另一个
Node，它只在同一张 Graph 内传播。

默认不限制执行步数和时长。需要约束循环或外部调用时，可以显式配置：

```python
outputs = runtime.run(
    "upper.graph",
    "bricks",
    max_steps=10,       # 0 表示无限步
    timeout=30,         # None 表示 Graph 总时长不限
)
```

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

[上一章：设计哲学与心智模型](01-design-philosophy.md) · [下一章：Graph 数据流](03-graph-dataflow.md)
