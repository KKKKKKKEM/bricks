# Bricks

Bricks 正在重新构建为一个领域无关的 Python 图执行引擎。

它以少量、稳定的执行原语为核心，让 Agent、ETL、Spider 等领域框架通过继承、组合和插件建立在
Engine 之上，而不把任何领域策略写入内核。

```text
RunRequest
    ↓
Flow(entrypoint, endpoints)
    ↓
Graph(typed Node ports, Edge)
    ↓
InputPolicy → NodeInputs → NodeResult → Output
```

## 当前状态

项目处于 1.0 重构早期阶段。当前已经完成 typed dataflow 抽象：

- `Ports`：不可变的 `port -> Python type` 声明。
- `Node`：声明 typed input/output ports、输入策略和执行行为。
- `NodeInputs`：一次执行实际消费的只读输入。
- `InputPolicy`：用组内 AND、组间 OR 统一表达 all、any 和 required inputs。
- `Output`：带 port 的图内输出。
- `NodeResult`：零到多个、支持异步渐进产生的 Output。
- `Edge`：连接上游 output port 和下游 input port。
- `Flow`：同一 Graph 上的一项执行能力，声明入口和终止 Endpoint。
- `Graph`：构建后冻结的拓扑，以及严格的引用和可达性校验。
- `ExecutionContext`：传给 Node 的只读运行身份和元数据。

`InputBuffer`、`Run`、`Task` 和 `Engine` 执行循环尚未实现。当前代码用于先稳定抽象契约，不提供旧版
`GraphBuilder` 或 `Machine` API。

## 抽象示例

```python
from bricks.engine import (
    Endpoint,
    Flow,
    Graph,
    Node,
    NodeInputs,
    NodeResult,
    Ports,
)


class TransformNode(Node):
    input_ports = Ports(source=str)
    output_ports = Ports(result=str)

    async def execute(self, inputs: NodeInputs, context):
        """原样返回输入，演示最小 Node 实现。

        参数：
            inputs: 本次执行消费的 source 输入。
            context: 当前执行的只读上下文。

        返回：
            从 result 端口产生的节点结果。
        """

        return NodeResult.one(inputs["source"], port="result")


transform = TransformNode()

graph = Graph("etl")
graph.add_node("transform", transform)
graph.add_flow(
    Flow(
        name="transform",
        entrypoint="transform",
        endpoints=frozenset({Endpoint("transform", "result")}),
    )
)
graph.freeze()
```

同一 Graph 可以提供不同 Flow。不同 Flow 可以从不同节点进入、共享部分路径，并在不同
`Endpoint(node_id, port)` 终止。

## 设计文档

- [核心概念入门](docs/core-concepts.md)
- [Engine 设计](docs/engine-design.md)
- [Engine 宪法](docs/constitution.md)

## 开发

```bash
uv run --with pytest python -m pytest -q
```

项目使用 MIT License。
