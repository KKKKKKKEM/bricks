# 核心概念入门

Bricks 当前实现的是一套类型安全的图定义：Node 可以有多个 input/output port，每个 port 声明
Python 类型，输入策略决定哪些 port 就绪时可以执行。

当前还没有 `Engine.run()`。这篇文档只介绍已经实现并通过测试的抽象。

## 1. 一张图就是一组带插口的处理器

可以把 Node 想象成一台带输入插口和输出插口的机器：

```text
Users ──> users  ┌───────────┐ merged  ──> Save
Orders ─> orders │ MergeNode │ rejected ─> Report
                 └───────────┘
```

- `Ports` 声明每个插口的名称和数据类型。
- `Edge` 连接上游 output port 和下游 input port。
- `InputPolicy` 决定哪些输入组合就绪时可以执行。
- `NodeInputs` 保存本次执行实际消费的输入。
- `NodeResult` 产生零到多个 `Output`。
- `Flow` 决定从哪个 Node 开始，以及在哪个 output port 返回结果。

## 2. Ports：声明端口名称和类型

```python
from bricks.engine import Ports


class UserBatch:
    pass


class OrderBatch:
    pass


input_ports = Ports(
    users=UserBatch,
    orders=OrderBatch,
)
```

`Ports` 是只读 Mapping：

```python
assert input_ports["users"] is UserBatch
assert tuple(input_ports) == ("users", "orders")
```

端口类型必须是普通 Python class。当前不接受 `list[str]` 等 typing 泛型，建议使用有明确领域含义
的包装类型，例如 `UserBatch`、`DatasetRef` 或 `Message`。

类型兼容是有方向的：

```python
class Dataset:
    pass


class TableDataset(Dataset):
    pass
```

```text
TableDataset output → Dataset input       允许
Dataset output      → TableDataset input  拒绝
```

Engine 不做隐式转换。`bytes -> str` 或 `dict -> User` 必须通过显式 Node 完成。

## 3. Node：声明端口、策略和执行行为

```python
from bricks.engine import InputPolicy, Node, NodeInputs, NodeResult, Ports


class MergedBatch:
    pass


class MergeNode(Node):
    input_ports = Ports(
        users=UserBatch,
        orders=OrderBatch,
    )
    output_ports = Ports(
        merged=MergedBatch,
    )
    input_policy = InputPolicy.all()

    async def execute(
        self,
        inputs: NodeInputs,
        context,
    ) -> NodeResult:
        """合并用户和订单数据。

        参数：
            inputs: 本次执行消费的 users 和 orders。
            context: 当前执行的只读上下文。

        返回：
            从 merged 端口产生的合并结果。
        """

        result = merge(
            inputs["users"],
            inputs["orders"],
        )
        return NodeResult.one(result, port="merged")
```

Node 不保存 `node_id`。同一个无状态 Node 行为可以放到 Graph 的多个位置。

普通单输入、单输出 Node 可以继续使用默认的 `default: object` 端口，但领域 Node 应尽量显式声明
类型，让 Graph 可以在运行前发现错误。

## 4. NodeInputs：一次执行实际拿到的输入

```python
from bricks.engine import NodeInputs

inputs = NodeInputs({
    "users": users,
    "orders": orders,
})

assert inputs["users"] is users
```

`NodeInputs` 会复制外部 Mapping，并只读暴露数据。普通单输入场景可以简写：

```python
inputs = NodeInputs.from_value("hello")
assert inputs.single() == "hello"
```

`single()` 只有在恰好存在一个输入时才成功，不会静默忽略其他 port。

## 5. InputPolicy：什么时候可以执行

所有策略统一成一个模型：

```text
输入组内部使用 AND
多个输入组之间使用 OR
```

### 5.1 等待全部输入

```python
input_policy = InputPolicy.all()
```

```text
users ready AND orders ready → execute(users, orders)
```

### 5.2 任意输入都可以触发

```python
input_policy = InputPolicy.any()
```

```text
message ready → execute(message)
cancel ready  → execute(cancel)
timeout ready → execute(timeout)
```

如果多个 port 同时有 token，策略选择全局最早到达的 token，避免按照端口名称选择造成饥饿。

### 5.3 指定一组必需输入

```python
input_policy = InputPolicy.require(
    "users",
    "orders",
)
```

它等价于一个 `{users, orders}` 输入组。

Node 声明的每个 input port 必须被策略覆盖，否则该 port 的 token 可能永远无法消费，
`Graph.freeze()` 会拒绝这种定义。

### 5.4 多种触发组合

```python
input_policy = InputPolicy.groups(
    ("users", "orders"),
    ("cancel",),
)
```

对应：

```text
(users AND orders) OR cancel
```

本次策略只消费被选中组的 port。Node 可以通过成员判断分辨触发来源：

```python
if "cancel" in inputs:
    return NodeResult.one(cancelled, port="cancelled")
```

如果多个输入组都完整就绪，选择最早完整就绪的一组；时间相同时使用声明顺序。

### 5.5 无输入 Source Node

```python
class SourceNode(Node):
    input_ports = Ports()
    output_ports = Ports(items=ItemBatch)
    input_policy = InputPolicy.on_start()
```

`on_start` 只适用于零 input port Node。未来的 Engine 必须保证它只在 Flow 启动时触发一次。

## 6. InputAvailability 和 InputSelection

这两个对象供未来的输入缓冲区和调度器使用，领域 Node 通常不会直接接触。

```python
from bricks.engine import InputAvailability, InputToken

availability = InputAvailability({
    "users": [InputToken(sequence=1, value=users)],
    "orders": [InputToken(sequence=2, value=orders)],
})

selection = InputPolicy.all().select(availability)
assert selection.ports == ("users", "orders")
```

- `InputToken.sequence` 表示全局到达顺序。
- `InputAvailability` 只暴露数量和顺序，不向策略暴露领域判断能力。
- `InputSelection` 明确本次从哪些 port 各消费一个 token。

业务条件仍然属于 `Node.execute()`，不属于 InputPolicy。

## 7. Edge：连接两侧端口

Edge 现在同时声明源输出端口和目标输入端口：

```python
graph.connect(
    "load_users",
    "merge",
    source_port="result",
    target_port="users",
)
```

表示：

```text
load_users.result → merge.users
```

同一个 output port 可以连接多个目标，表示广播。同一个 input port 也可以接收多条 Edge，未来的
InputBuffer 会按 token 到达顺序放入 FIFO 队列。

## 8. NodeResult 和 Output

单个输出：

```python
return NodeResult.one(value, port="result")
```

多个输出：

```python
return NodeResult.many(
    Output(item, port="item")
    for item in items
)
```

渐进式输出：

```python
async def generate():
    """渐进产生 Item。"""

    async for item in source:
        yield Output(item, port="item")


return NodeResult.stream(generate())
```

未来 Engine 会检查实际 `Output.value` 是否满足 Node 对应 output port 声明的类型。

## 9. Flow：从哪里开始、在哪里返回

```python
from bricks.engine import Endpoint, Flow

flow = Flow(
    name="full",
    entrypoint="load_users",
    endpoints=frozenset({
        Endpoint("merge", "merged"),
    }),
)
```

同一个 output port 可以在一个 Flow 中作为结果返回，在另一个 Flow 中继续沿 Edge 传播。Flow 只
定义边界，不重复保存中间路线。

## 10. 完整定义示例

```python
from bricks.engine import (
    Endpoint,
    Flow,
    Graph,
    InputPolicy,
    Node,
    NodeInputs,
    NodeResult,
    Ports,
)


class UserBatch:
    pass


class OrderBatch:
    pass


class MergedBatch:
    pass


class StartNode(Node):
    input_ports = Ports(config=dict)
    output_ports = Ports(config=dict)

    async def execute(self, inputs: NodeInputs, context) -> NodeResult:
        """把启动配置广播给两个加载节点。"""

        return NodeResult.one(inputs["config"], port="config")


class LoadUsersNode(Node):
    input_ports = Ports(config=dict)
    output_ports = Ports(result=UserBatch)

    async def execute(self, inputs: NodeInputs, context) -> NodeResult:
        """加载用户数据。"""

        return NodeResult.one(UserBatch(), port="result")


class LoadOrdersNode(Node):
    input_ports = Ports(config=dict)
    output_ports = Ports(result=OrderBatch)

    async def execute(self, inputs: NodeInputs, context) -> NodeResult:
        """加载订单数据。"""

        return NodeResult.one(OrderBatch(), port="result")


class MergeNode(Node):
    input_ports = Ports(users=UserBatch, orders=OrderBatch)
    output_ports = Ports(result=MergedBatch)
    input_policy = InputPolicy.all()

    async def execute(self, inputs: NodeInputs, context) -> NodeResult:
        """合并用户和订单数据。"""

        return NodeResult.one(MergedBatch(), port="result")


graph = Graph("etl")
graph.add_node("start", StartNode())
graph.add_node("load_users", LoadUsersNode())
graph.add_node("load_orders", LoadOrdersNode())
graph.add_node("merge", MergeNode())

graph.connect(
    "start",
    "load_users",
    source_port="config",
    target_port="config",
)
graph.connect(
    "start",
    "load_orders",
    source_port="config",
    target_port="config",
)
graph.connect(
    "load_users",
    "merge",
    source_port="result",
    target_port="users",
)
graph.connect(
    "load_orders",
    "merge",
    source_port="result",
    target_port="orders",
)

graph.add_flow(
    Flow(
        name="merge",
        entrypoint="start",
        endpoints=frozenset({Endpoint("merge", "result")}),
    )
)

graph.freeze()
```

## 11. Graph.freeze() 当前会检查什么

- Node、Edge 和 Flow 是否重复。
- Edge 两端 Node 是否存在。
- `source_port` 是否由源 Node 声明。
- `target_port` 是否由目标 Node 声明。
- output 类型是否可以安全赋值给 input 类型。
- InputPolicy 是否引用未知 port。
- InputPolicy 是否覆盖全部声明的 input port。
- Flow 入口和 Endpoint Node 是否存在。
- Flow Endpoint port 是否由对应 Node 声明。
- Flow 入口是否至少可以到达一个 Endpoint Node。
- Graph 冻结后是否发生修改。

## 12. 当前尚未实现

- InputBuffer 和 token 消费。
- RunRequest、Run、Task 和 Engine。
- 多上游入口如何由一个 Flow 启动。
- 实际 Output.value 的运行时类型检查。
- 未完成输入组的 deadlock 检测。
- 并发、背压、取消、重试和持久化。

这些内容会在对应实现落地时同步更新文档。
