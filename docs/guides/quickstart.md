# 快速开始

## 创建一张图

```python
from bricks import GraphBuilder, Machine, Status


builder = GraphBuilder("approval", initial="draft")
builder.action("draft")
builder.terminal("approved")
builder.transition("draft", "approve", "approved")

machine = Machine(builder.build())
machine.start()
machine.dispatch("approve")

assert machine.status is Status.COMPLETED
```

## 运行数据

把业务数据放入 `Context.data`，流程位置和生命周期由引擎维护：

```python
builder = GraphBuilder("counter", initial="start")
builder.action("start", lambda context, event: context.set("count", 1))
builder.terminal("done")
builder.transition("start", "finish", "done")

machine = Machine(builder.build())
machine.context.set("owner", "alice")
machine.start()
machine.dispatch("finish")
```

不要把 Graph、Machine 或执行器实例放进需要持久化的业务数据里。

## 下一步

- 需要外部消息驱动时阅读 [events.md](events.md)。
- 需要等待、重试或暂停时阅读 [waiting.md](waiting.md)。
- 需要恢复长运行实例时阅读 [persistence.md](persistence.md)。
