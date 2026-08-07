# 组合语义

Bricks 只有一套 Graph 和 Machine 核心。下面的对象提供不同的编排视角，但不复制
Machine 的事件迁移逻辑。

## Workflow

`Workflow` 适合批量创建运行实例、按顺序消费事件，并提供 DAG 拓扑校验。它不改变
Graph 的事件语义；循环图仍可用于一般 Machine，只在调用 `topological_order()` 时
被 DAG 语义拒绝。

## Reactive

`ReactiveRuntime` 只负责订阅 EventBus 和选择 `dispatch()` / `resume()` 入口。它不
修改节点模型，也不负责外部消息队列。

## Parallel

`Parallel.plan(...)` 生成 `Fork` Outcome 的声明。Fork 子运行共享 Graph，却拥有独立
Context；父运行通过 `join()` 等待 `all` 或 `any` 策略。异步 Machine 会并发启动各个
分支；`max_concurrency` 可以限制异步分支数。使用 `fail_fast` 时，失败分支会取消
尚未完成的兄弟任务。
`any` 表示任一分支成功，单个失败分支不会提前结束竞争；同步 Machine 仍然顺序执行。
调度器和真正的远程并行执行不属于当前核心。

分支以 `Outcome.fail()` 结束时，可以通过一个可选的 `failure_policy` 控制汇聚行为：

```python
plan = Parallel.plan(
    {"event": "fetch-a"},
    {"event": "fetch-b"},
    join_event="joined",
    failure_policy="continue",
)
```

- `fail`（默认）：等待策略满足后，任一失败分支都会让父运行进入 `FAILED`。
- `continue`：等待分支结束，并通过 `join_event` 继续；子运行快照会放在 Join 事件
  的 `payload["children"]` 中，由父图决定如何处理部分失败。
- `fail_fast`：任一分支进入 `FAILED` 后，立即停止尚未结束的兄弟分支，父运行进入
  `FAILED`。

### 动态 Fan-out

`Outcome.fork()` 的分支可以由当前 `Context` 在运行时生成。每个分支接收独立的
Context，`data` 会覆盖父 Context 中同名字段；Join 事件的 payload 再由父图决定如何
汇聚：

```python
def fan_out(context, event):
    return Outcome.fork(
        *(
            {"event": "process", "data": {"item": item}}
            for item in context.get("items", [])
        ),
        join_event="joined",
    )
```

这覆盖了常见的 map-reduce 编排方式：动态分支相当于 map，父图在 `joined` 迁移或其
Action 中实现 reduce。完整示例见 `examples/map_reduce.py`。

这只处理分支以 `Outcome.fail()` 结束的情况；Action 直接抛出的异常默认仍按普通运行
异常传播。若配置 `RetryPolicy(retry_on=(...))`，节点进入 Action 的匹配异常会进入
统一的 Retry 等待。

## Saga

`SagaRuntime` 监听迁移完成 Hook，从迁移元数据中收集补偿 Action，并按后进先出顺序
执行。补偿失败会记录在 `CompensationResult`，不会掩盖其它补偿步骤的执行。

这些语义都可以单独使用，也可以通过 EventBus、PersistenceBinding 和策略对象组合。
