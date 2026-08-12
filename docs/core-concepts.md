# 核心概念

Bricks 只区分两种流动：Graph 内的值流动，以及 Graph 间的事件流动。不要用其中一种去模拟另一种。

| 范围 | 使用的对象 | 用途 |
| --- | --- | --- |
| 单张 Graph | `Output`、`Edge` | 把一个 Node 的值交给下游 Node |
| 多张 Graph | `Event`、`Context.emit()`、`Runtime.route()` | 发布领域事实并异步启动工作 |
| 动态执行扩展 | `Runtime.attach()`、`NodeHook` | 不修改冻结 Graph，转换 Node 输入、结果和流程 |

## Ports 与 Node

`Ports` 是一个不可变的端口名到 Python class 的映射。端口类型必须是普通 Python class；连接时，源类型必须
是目标类型的子类。例如 `str` 可以连接到 `object` 输入，反过来不行。

```python
class Parse(Node):
    input_ports = Ports(raw=str)
    output_ports = Ports(length=int)

    def execute(self, inputs, context):
        del context
        return Output(len(inputs["raw"]), port="length")
```

`execute(inputs, context)` 的 `inputs` 是只读 Mapping。返回值只能是：

- 单个 `Output(value, port="...")`；
- 一个只包含 `Output` 的 iterable；
- `None`。

输出的端口必须声明在 `output_ports` 中，值也必须满足声明类型。未连接到 Edge 的合法输出会成为本次
`Runtime.run()` 的终端输出。

同步工作继承 `Node`；需要 await 的工作继承 `AsyncNode` 并把 `execute` 写为 `async def`。冻结时会校验继承
类别与方法风格匹配。

## Graph 与 Edge

Graph 在构建阶段可变，`freeze()` 后成为可执行的静态定义：

```python
graph = (
    Graph(entrypoint="parse")
    .add("parse", Parse())
    .add("store", Store())
    .connect("parse", "store", source_port="length", target_port="length")
    .freeze()
)
```

冻结会拒绝以下定义：空图或未知入口、重复节点/边、未知端口、端口类型不兼容、Graph 内环、不可从入口到达
的节点，以及不合法的输入策略。`Runtime.register()` 会自动冻结尚未冻结的 Graph。

Graph 是有向无环图。重复工作、轮询、爬虫的继续发现等，都应发布 Event 再触发另一张 Graph，而不是在图中
添加回边。

### ExecutionPlan：从完整 Graph 选择子路径

同一张完整 Graph 可以为不同需求创建不同的严格执行计划：

```python
fast = graph.plan(include={"load", "parse", "save"})
full = graph.plan(include={"load", "parse", "enrich", "save"})

runtime.run("document.graph", payload, plan=fast)
```

`plan()` 会在需要时先冻结 Graph。计划只保留两端都被选择的原有 Edge，不会跨过未选择 Node 自动补边；入口不在
计划内、所选 Node 不可达，或 `ALL` Node 缺少输入端口时，创建计划会立即失败。计划绑定创建它的 Graph 实例，
创建后不可变，可以安全复用，也可以在并发 execution 中使用不同计划。没有传 `plan` 时仍执行完整 Graph。

## 输入触发策略

每个 Node 固定使用一种 `InputPolicy`：

| 策略 | 何时执行 | 本次 `inputs` |
| --- | --- | --- |
| `ALL`（默认） | 每个声明端口各有一个值 | 包含所有端口 |
| `ANY` | 按端口声明顺序找到第一个有值的端口 | 只包含被消费的一个端口 |
| `ON_START` | Graph 执行开始时 | 空 Mapping |

`ON_START` 只能用于零输入 Node。执行器为每个端口维护 FIFO 队列；一次执行每个被选中的端口只消费一个值。
如果 Graph 静止后还剩无法组合的值，例如 `ALL` Node 只收到一半输入，会抛出 `IncompleteInputsError`。

## Event 与 Context

`Event(type, payload)` 是最小的领域消息。内核不自动添加 ID、时间、来源、幂等键或 tracing 信息；这些数据
应该是领域 payload 的一部分，或交给外部观测系统。

Node 通过 Context 发布跨图事件：

```python
class CreateTask(Node):
    input_ports = Ports(url=str)
    output_ports = Ports()

    def execute(self, inputs, context):
        context.emit("crawl.task.created", {"url": inputs["url"]})
```

`Context` 不提供数据库、HTTP 客户端、Runtime 或队列。把这些依赖通过 Node 构造器显式注入。Node 本身应保持
可重入；单次执行状态放在输入、事件 payload 或领域 Store 中。

事件一旦被 Runtime 接受就是渐进提交：源 Node 随后失败不会撤销已发布事件。核心不会比较 payload、自动去重
或自动重试；这些是领域或后端的责任。
