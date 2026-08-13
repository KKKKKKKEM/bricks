# 扩展 Runtime

扩展部署先组装职责明确的 `EventRouter` 与 `GraphWorker`，再按需组合成 Runtime：

```python
from bricks import Runtime
from bricks.engine import EventRouter, GraphWorker

router = EventRouter(
    events=my_event_bus,
    publisher=my_task_publisher,
)
worker = GraphWorker(
    consumer=my_task_consumer,
    executor=my_graph_executor,
    emit=router.publish,
)
runtime = Runtime(router=router, worker=worker)
```

`EventRouter`、`GraphWorker` 和底层协议是高级扩展接口，不属于顶层 `bricks` 公共 API。应用侧的 Graph、Node、
Event 和 Runtime 门面用法不需要因此改变。`Runtime()` 仍会创建完整的默认内存组合。

| 协议 | 负责什么 | 默认实现 |
| --- | --- | --- |
| `EventBus` | Event 订阅、发布、投递空闲与关闭 | `MemoryEventBus` |
| `TaskPublisher` | 向命名队列提交 Work | `MemoryTaskBackend` |
| `TaskConsumer` | 调度带 Slot 的 Work，并控制当前实例的本地并发 | `MemoryTaskBackend` |
| `TaskBackend` | 同时实现发布与消费的组合协议 | `MemoryTaskBackend` |
| `GraphExecutor` | 执行已冻结 Graph 并返回终端 Output | `Engine` |
| `HookableGraphExecutor` | GraphExecutor 的可选动态 Hook 能力 | `Engine` |

## 适配器的最低要求

EventBus 和 TaskConsumer 必须实现 `idle`、`wait_idle(timeout)` 和 `close()` 所表达的本实例生命周期语义；
TaskPublisher 的 `submit()` 正常返回即表示后端已接受 Work，同时提供 `close()` 释放发布端资源。GraphExecutor
提供 `execute()` 和 `close()`。角色依赖这些方法完成自身的等待与关闭。

- EventBus 的 `subscribe(event_type, handler, subscription=...)` 需要支持精确类型和 `"*"` 通配订阅。同名
  subscription 的多个实例竞争消费，不同 subscription 各自收到一份；省略 subscription 的观察者相互独立。
  Event 携带内部 Slot lease 时，EventBus 从 `publish()` 调用开始接管该引用，并在所有 handler 投递结束或发布
  失败时释放；默认 `MemoryEventBus` 已实现该约束。
- TaskPublisher 把 Work 提交到命名 queue，`submit()` 正常返回表示后端已经接受；TaskConsumer 的
  `bind(queue, handler, concurrency=..., slots=...)` 在调度根 Work 前从 SlotPool 获取 lease，延续 Work 则保留
  自身 lease。等待 Slot 的根 Work 不能占用 concurrency，Work 完成或失败后必须释放它持有的 lease。
- GraphExecutor 接收注册名、冻结 Graph、入口输入、Event emitter、可选 ExecutionPlan，以及关键字参数
  `slot=` 和 `execution=`。GraphWorker 会把冻结后的 Node timeout 快照绑定到 Execution；替换执行器必须为每次
  Node firing 使用 `with execution.step(node_id): ...` 包住完整调用，并在调度边界调用
  `execution.checkpoint()`，从而保留步数、取消和 timeout 语义。自定义执行器若还实现
  `HookableGraphExecutor` 的 `attach()`，`Runtime.attach()` 会按结构化能力委托给它。

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
- `Work.limits` 与 execution ID 如何跨进程传递，取消请求如何送达执行进程。

不要仅因后端名为 Redis 或 MQ 就暗示这些能力已经存在。领域 ID、去重和外部副作用的幂等性仍应由领域模型
显式实现。

`Slot` 保存进程内对象，默认不能随 Work 跨进程序列化。远程 TaskBackend 若要保留相同语义，必须让同一逻辑
执行链路由到持有该 Slot 的执行进程，或自行实现可序列化的状态引用及其租约协议；否则应明确声明不支持 Slot。

## 分离 Router 与 Worker

`route()` 只负责把 Event 转成 Work 并投递到 queue，`consume()` 只负责消费 queue 并执行 Work 指定的 Graph：

```python
# Router 不需要注册目标 Graph。
router = EventRouter(events=events, publisher=tasks)
router.route("order.created", graph="order.process", queue="orders")

# Worker 不需要订阅源 Event；同一 queue 可以启动多个 Worker 竞争消费。
worker = GraphWorker(consumer=tasks, emit=router.publish)
worker.register("order.process", order_graph)
worker.consume("orders", concurrency=8)
```

`route()` 默认生成稳定的 `route:{event_type}:{graph}:{queue}` subscription，也可通过 `subscription=` 显式指定。
多个 Router 注册同一条 route 时属于同一逻辑订阅，只应由其中一个实例投递 Work。

`on(event_type, graph=..., queue=..., concurrency=...)` 是 Runtime 上组合 `consume()` 与 `route()` 的常用入口。
它先准备本地消费者，再暴露 Event route，避免消费端配置失败后留下仍会投递 Work 的 route。
`observe(event_type, handler)` 独立用于观察 Event。只有需要独立部署 Router 和 Worker，或多条 route 共享一次
queue 消费配置时，才需要显式调用 `route()` 与 `consume()`。

## 组件生命周期

EventRouter 和 GraphWorker 只自动关闭自己创建的默认组件。注入的 EventBus、TaskPublisher、TaskConsumer 和
GraphExecutor 默认由调用方管理，这使多个角色可以安全共享客户端或连接池。若注入组件明确由某个角色独占，
可对该角色传 `close_injected=True`。同一共享组件不要同时交给两个角色管理。

Runtime 显式拥有传入的角色，`Runtime.close()` 会依次关闭 Worker 和 Router。`Runtime()` 创建的默认底层组件则
由 Runtime 统一关闭，避免共享 TaskBackend 被重复管理。

TaskConsumer 的 `idle` 和 `wait_idle()` 只描述当前消费实例能够跟踪的工作，不是分布式系统的全局完成屏障；
TaskPublisher 不等待远端消费者执行完成。

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
