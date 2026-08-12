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
