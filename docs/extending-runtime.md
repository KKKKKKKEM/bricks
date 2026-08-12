# 扩展 Runtime

Runtime 有三个彼此独立的注入点，定义在 `bricks.engine.backends`：

```python
runtime = Runtime(
    events=my_event_bus,
    tasks=my_task_backend,
    executor=my_graph_executor,
)
```

它们是高级扩展接口，不属于顶层 `bricks` 公共 API。应用侧的 Graph、Node、Event 和 `Runtime.route()` 用法不
需要因此改变。

| 协议 | 负责什么 | 默认实现 |
| --- | --- | --- |
| `EventBus` | Event 订阅、发布、投递空闲与关闭 | `MemoryEventBus` |
| `TaskBackend` | 命名队列、Work 提交、并发、空闲与关闭 | `MemoryTaskBackend` |
| `GraphExecutor` | 执行已冻结 Graph 并返回终端 Output | `Engine` |

## 适配器的最低要求

所有协议都必须实现 `idle`、`wait_idle(timeout)` 和 `close()` 所表达的生命周期语义。Runtime 依赖这些方法来
正确完成 `wait_idle()` 和 `close()`；不能只实现消息的发送或接收。

- EventBus 的 `subscribe(event_type, handler)` 需要支持精确类型和 `"*"` 通配订阅。
- TaskBackend 的 `bind(queue, handler, concurrency=...)` 需要对同名 queue 的并发配置保持一致，并把提交的
  `Work` 交给 handler。
- GraphExecutor 接收注册名、冻结 Graph、入口输入和 Event emitter；若替换执行器，就必须保留 Graph 的
  Ports、InputPolicy、Output、Edge 和 Event 语义。

可参考 [test_backends.py](../tests/engine/test_backends.py) 中的同步替身：它验证三个能力可独立替换，也验证
Runtime 不依赖默认内存实现的私有字段。

## 语义必须由适配器声明

协议刻意没有规定序列化、持久化、确认或重试策略。因此 Redis、RabbitMQ、数据库任务表等适配器必须在自己的
文档中明确：

- 事件和 Work 的投递语义（至多一次、至少一次等）；
- 哪一方确认消息、何时重试、失败放到哪里；
- `wait_idle()` 在分布式场景下具体表示什么；
- `close()` 是停止接收、排空本地任务，还是等待远程 broker 完成；
- payload 的序列化限制，以及幂等性由谁保证。

不要仅因后端名为 Redis 或 MQ 就暗示这些能力已经存在。领域 ID、去重和外部副作用的幂等性仍应由领域模型
显式实现。

## 动态 Node Hook

Graph 注册后保持冻结，但默认 `Engine` 允许使用者给后续 Graph execution 动态挂载 Hook：

```python
from bricks import Output
from bricks.engine import NodeCall, NodeHook, ShortCircuit


class RequestCache(NodeHook):
    async def enter(self, call: NodeCall) -> NodeCall:
        response = await cache.get(call.inputs["request"])
        if response is not None:
            raise ShortCircuit(Output(response, "response"))
        return call

    async def exit(self, call, outputs):
        await cache.put(call.inputs["request"], outputs[0].value)
        return outputs


handle = runtime.attach(
    RequestCache(),
    graph="crawl.graph",
    node="request",
)
handle.detach()
```

`enter()` 转换 Node 输入，`exit()` 转换结果，`error()` 可以返回结果来恢复普通异常。三者都可以是同步或异步方法；
Engine 把 awaitable 提交给常驻后台 event loop，并在执行 Graph 的 worker 中等待结果。Hook 无需、也不能手动推进
Node 执行：`enter()` 正常返回后 Engine 默认执行 Node。

单阶段转换不需要定义 class：

```python
def normalize(call, outputs):
    return tuple(Output(clean(item.value), item.port) for item in outputs)


handle = runtime.attach(
    normalize,
    phase="exit",
    graph="crawl.graph",
    node="parse",
)
```

`phase` 可以是 `"enter"`、`"exit"` 或 `"error"`，省略时函数作为 `enter` Hook。注册范围可以是全部 Graph、
指定 Graph，或者指定 Graph 内的 Node；Node 范围必须同时给出 Graph 名。

Hook 通过两个信号显式改变流程：

- `ShortCircuit(*outputs)` 只能从 `enter()` 发出。它跳过当前 Node，把 outputs 当作该 Node 的结果，经过
  当前 Node 的 output port/type 校验后继续走既有 Edge。
- `StopGraph(*outputs)` 可以从任意 Hook 阶段发出。它立即停止整个 Graph，并直接返回携带的终端 outputs；
  当前 Graph 没有 graph-level output schema，因此只校验它们是 `Output`。

Hook 按注册顺序进入、逆序退出。短路后，已经进入的 Hook 仍会执行 `exit()`；终止 Graph 则不会继续执行剩余
生命周期。每次 Graph execution 在开始时固定 Hook 快照，所以 `attach()`/`detach()` 不会改变已经在途的执行，
但会作用于下一次执行。`detach()` 是幂等的，也不会等待旧快照结束。

Hook 最终产生的结果仍受 Graph 契约约束：除 `StopGraph` 的整图终端结果外，Hook 不能产生未声明的 output port
或错误类型，也不能替换 Graph、Node、Context，或增删 Node input port。
