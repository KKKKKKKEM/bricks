# Bricks 图执行引擎设计

状态：Draft  
目标版本：1.0  
适用范围：`bricks.engine`

## 1. 定位

Bricks 是一个领域无关的 Python typed dataflow engine。Agent、ETL、Spider 等领域通过定义 Node
和组合 Graph 使用 Engine，Engine 本身不理解 Prompt、Dataset、Request 等领域对象。

核心模型借鉴节点编排系统的 typed ports，但不把 UI widget、颜色、位置等编辑器概念放入内核。

## 2. 设计目标

- Node 可以声明多个 typed input/output ports。
- Edge 连接上游 output port 和下游 input port。
- Node 通过统一 InputPolicy 声明什么时候具备执行条件。
- InputPolicy 使用“组内 AND、组间 OR”表达 all、any 和 required inputs。
- Task 只在策略选中的输入全部就绪后创建。
- NodeResult 支持零个、一个、多个和异步渐进式 Output。
- 一个 Graph 可以通过多个 Flow 暴露不同入口和终止点，并共享中间路线。
- Graph 在运行前完成引用、端口、类型、策略和可达性校验。
- 领域能力通过 Node、组合和小型插件接口扩展。

## 3. 非目标

当前阶段不提供：

- 完整 Engine、Run、Task 和 InputBuffer。
- typing 泛型解析和隐式类型转换。
- optional、latest、collect-all 等高级 input port 模式。
- 分布式调度、持久化、重试、补偿和 exactly-once。
- Agent、ETL、Spider 的具体领域节点。
- 可视化编辑器和 UI Schema。

## 4. 核心原则

### 4.1 机制与策略分离

Engine 负责：

- 校验 typed Graph。
- 接收和路由 Output。
- 缓冲目标 input port 的 token。
- 请求 InputPolicy 生成 InputSelection。
- 消费选中输入并创建 Task。
- 执行 Node、处理 Flow Endpoint 和判断 Run 生命周期。

领域层负责：

- 定义端口的领域类型。
- 实现 Node.execute()。
- 在 Node 内判断业务条件并选择 output port。
- 提供类型转换、聚合、重试等领域或插件节点。

### 4.2 定义与实例分离

```text
静态定义                         动态实例

Graph                            Run
├── Node                         ├── InputToken
│   ├── input_ports              ├── InputBuffer
│   ├── output_ports             └── Task
│   └── input_policy
├── Edge
└── Flow
```

Node 是行为定义，Task 是 Node 在全部所需输入就绪后的一次调用。

### 4.3 Graph 运行时不可变

Graph 可以构建，但交给 Engine 前必须 freeze。动态发现的数据表达为新的 Output 和 InputToken，
不是运行时新增 Graph Node。

### 4.4 显式优于隐式

- Node 显式声明 port 和 Python 类型。
- Edge 显式声明 source_port 和 target_port。
- 类型不兼容时拒绝连接，不做隐式转换。
- 未路由 Output 是错误，不静默丢弃。
- 业务过滤使用 NodeResult.empty()。

## 5. 术语

| 概念 | 含义 |
|---|---|
| Ports | 不可变的 `port name -> Python class` 映射 |
| Node | 声明端口、输入策略和执行行为的可复用对象 |
| NodeInputs | 一次 Task 实际消费的只读输入 |
| InputToken | input port FIFO 队列中的一个值和到达序号 |
| InputAvailability | 提供给策略的只读端口数量和顺序快照 |
| InputPolicy | 判断能否执行并选择本次消费端口的策略 |
| InputGroup | 组内必须全部就绪的一组 input port |
| InputSelection | 策略选中的本次消费端口 |
| Output | Node 从某个 output port 产生的一项值 |
| NodeResult | 一次 Node 调用产生的 Output 流 |
| Edge | output port 到 input port 的静态连接 |
| Flow | Graph 对外暴露的入口和终止 Endpoint |
| Endpoint | Flow 返回结果的 `node_id + output port` |
| Run | 一次 Flow 执行实例 |
| Task | 输入就绪后创建的一次 Node 调用实例 |

## 6. 总体数据流

```text
Output(source_port, value)
          │
          ▼
Edge(source_port → target_port)
          │
          ▼
InputToken(sequence, value)
          │
          ▼
InputBuffer[target_node][target_port]
          │
          ▼
InputPolicy.select(InputAvailability)
          │
          ├── None ─────────────> 继续等待
          │
          └── InputSelection
                    │
                    ▼
            NodeInputs + Task
                    │
                    ▼
           Node.execute(inputs, context)
                    │
                    ▼
                NodeResult
```

## 7. 当前已实现的抽象

### 7.1 Ports

```python
class MergeNode(Node):
    input_ports = Ports(
        users=UserBatch,
        orders=OrderBatch,
    )
    output_ports = Ports(
        result=MergedBatch,
    )
```

类型兼容方向为：

```python
issubclass(source_type, target_type)
```

派生类型输出可以连接基础类型输入，反向不允许。`object` input 可以接收任意具体类型，但
`object` output 不能安全连接到具体子类 input。

当前只接受普通 Python class，不解析 `list[T]`。跨类型转换必须使用显式 Node。

### 7.2 NodeInputs

```python
inputs = NodeInputs({
    "users": users,
    "orders": orders,
})
```

NodeInputs 是只读 Mapping。普通单输入节点可以使用：

```python
inputs = NodeInputs.from_value(value)
value = inputs.single()
```

### 7.3 InputPolicy

策略统一为输入组：组内 AND，组间 OR。

```python
InputPolicy.all()
InputPolicy.any()
InputPolicy.require("users", "orders")
InputPolicy.groups(
    ("users", "orders"),
    ("cancel",),
)
InputPolicy.on_start()
```

等价关系：

```text
all      → 一个包含全部 port 的 InputGroup
any      → 每个 port 各自形成一个 InputGroup
require  → 一个包含指定 port 的 InputGroup
groups   → 多个 AND group 之间 OR
on_start → 零输入 Flow 入口触发一次
```

所有声明的 input port 必须被至少一个 InputGroup 覆盖，避免 token 永远积压。

### 7.4 策略选择顺序

每个 InputToken 带单调递增 sequence。一个 group 的 ready sequence 是其所需队首 token 中最大的
sequence，也就是该组最后一个必需输入到达的时间。

多个 group 同时可用时：

1. 选择 ready sequence 最小的 group。
2. 相同 sequence 按 group 声明顺序。
3. InputSelection 中的 port 按 Node input_ports 声明顺序排列。

策略只看到数量和 sequence，不读取领域 value。业务判断仍属于 Node.execute()。

### 7.5 Node

```python
class Node(ABC):
    input_ports = Ports(default=object)
    output_ports = Ports(default=object)
    input_policy = InputPolicy.all()

    @abstractmethod
    async def execute(
        self,
        inputs: NodeInputs,
        context: ExecutionContext,
    ) -> NodeResult:
        ...
```

Node 不持有 node_id，不保存单次 Run 的可变状态，也不直接指定下游 Node。

### 7.6 Output 与 NodeResult

```python
@dataclass(frozen=True)
class Output:
    value: Any = None
    port: str = "default"
```

NodeResult 统一普通和渐进式输出：

```python
NodeResult.empty()
NodeResult.one(value, port="result")
NodeResult.many(outputs)
NodeResult.stream(async_outputs)
```

Output 推动 Graph；日志、进度和生命周期事实属于未来 Event 系统。

### 7.7 Edge

```python
@dataclass(frozen=True)
class Edge:
    source: str
    target: str
    source_port: str = "default"
    target_port: str = "default"
```

连接示例：

```python
graph.connect(
    "load_users",
    "merge",
    source_port="result",
    target_port="users",
)
```

Edge 只描述连接，不执行 condition 或类型转换。

### 7.8 Flow 与 Endpoint

```python
Flow(
    name="full",
    entrypoint="start",
    endpoints=frozenset({Endpoint("merge", "result")}),
)
```

Flow 不保存完整路线。同一个 output port 可以在 Flow A 中返回，在 Flow B 中继续传播。

### 7.9 ExecutionContext

ExecutionContext 当前只保存只读的 run_id、task_id、flow 和 metadata，不作为共享可变 state 或万能
service locator。

## 8. Graph.freeze() 契约

当前实现检查：

- Node ID、Edge 和 Flow 不重复。
- Edge 两端 Node 存在。
- Node 的 input_ports/output_ports 是 Ports。
- Edge source_port 在源 Node output_ports 中。
- Edge target_port 在目标 Node input_ports 中。
- output 类型可以安全赋值给 input 类型。
- Node input_policy 是 InputPolicy。
- 策略不引用未知 port，并覆盖全部 input port。
- Flow 入口和 Endpoint Node 存在。
- Endpoint port 由对应 Node output_ports 声明。
- Flow 入口至少可以到达一个 Endpoint Node。
- frozen Graph 不再允许修改。

当前静态检查尚不能证明每个 Flow 中的每个 input port 最终一定收到 token；这需要结合未来运行时的
InputBuffer、分支结果和 deadlock 检测。

## 9. 未来运行时语义

以下是设计目标，尚未实现。

### 9.1 路由 Output

1. 校验 Output.port 由源 Node 声明。
2. 校验 Output.value 满足 output port 类型。
3. 如果命中当前 Flow Endpoint，形成 GraphOutput。
4. 否则沿 Edge 将 value 包装成 InputToken。
5. 再次校验 value 满足目标 input port 类型。

### 9.2 输入缓冲和 firing

每个 `(run_id, node_id, input_port)` 对应一个 FIFO 队列。Output 到达后，Engine 构建
InputAvailability 并调用 InputPolicy.select()。

策略返回 InputSelection 后：

1. 每个选中 port 消费一个队首 InputToken。
2. 构造 NodeInputs。
3. 创建 Task。
4. 调用 Node.execute()。
5. 重复选择，直到没有完整就绪的输入组。

### 9.3 渐进式提交

NodeResult.stream() 产生的 Output 一经 Engine 接收并路由就视为提交。流随后失败不会撤回之前的
Output。默认语义接近 at-least-once，不承诺 exactly-once。

### 9.4 不完整输入

当 Run 已没有运行中或可创建的 Task，但 InputBuffer 仍保存无法组成完整 InputGroup 的 token，
Engine 应报告 IncompleteInputsError，而不是永久保持 running。

等待外部事件属于未来显式 waiting 协议，不能与图内 deadlock 混淆。

## 10. Flow 与路径复用

```text
full:      Start → Extract → Transform → Validate → Load
transform:                   Transform → Validate
```

不同 Flow 可以从不同 Node 进入，共享 Transform 和 Validate，并使用不同 Endpoint。路线仍由 Edge
和 Output.port 决定。

由数据决定分支时，Node 输出不同 port；由当前 Flow 决定分支时，使用明确 RouterNode 或不同 Graph
位置，不在 Edge 中加入 Flow 特例。

## 11. 扩展边界

核心不建立万能 Plugin 基类。未来执行、存储和事件能力通过小接口接入：

```text
TaskExecutor
RunStore
EventSink
```

InputPolicy 可以扩展，但自定义策略只能读取 token 数量和顺序，并返回 InputSelection，不能读取领域
value 或执行领域业务。

## 12. 建议包结构

```text
bricks/engine/
├── _validation.py  # 内部共享校验
├── ports.py        # Ports 和类型兼容
├── inputs.py       # NodeInputs、token 和 InputPolicy
├── node.py         # Node、Output、NodeResult
├── graph.py        # Edge、Endpoint、Flow、Graph
├── context.py      # ExecutionContext
└── errors.py       # 公共异常
```

保持扁平，只有模块职责明显过大时再拆包。

## 13. 下一阶段

1. 设计 InputBuffer，并验证 FIFO 消费和 InputSelection。
2. 定义 RunRequest、Run 和 Task。
3. 实现单进程 Engine 执行循环。
4. 增加 Output 实际值类型检查、Flow Endpoint 返回和 deadlock 检测。
5. 使用 Agent、ETL、Spider 三个最小示例验证抽象。

暂不实现 optional/latest/collect-all input 模式。出现至少两个真实领域用例后，再判断它们属于核心
策略还是插件 Node。

