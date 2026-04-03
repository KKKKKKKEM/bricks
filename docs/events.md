# 事件系统

Bricks 的事件系统是框架的核心扩展点。它允许你在爬虫生命周期的任意阶段注入自定义逻辑，而无需修改爬虫主流程代码。

---

## 生命周期事件

所有事件名称定义在 `bricks.state.const` 中，建议通过常量引用而非硬编码字符串。

| 常量 | 触发时机 |
|---|---|
| `BEFORE_START` | 爬虫整体启动之前 |
| `BEFORE_WORKER_START` | 每个 Worker 线程启动之前 |
| `BEFORE_CLOSE` | 爬虫整体关闭之前 |
| `BEFORE_WORKER_CLOSE` | 每个 Worker 线程关闭之前 |
| `BEFORE_GET_SEEDS` | 从队列取种子之前 |
| `ON_SEEDS` | 取到种子之后，处理之前 |
| `AFTER_GET_SEEDS` | 取种子流程完成之后 |
| `BEFORE_PUT_SEEDS` | 将种子放入队列之前 |
| `AFTER_PUT_SEEDS` | 种子放入队列之后 |
| `BEFORE_MAKE_REQUEST` | 构建 Request 对象之前 |
| `ON_MAKE_REQUEST` | 构建 Request 对象时 |
| `AFTER_MAKE_REQUEST` | 构建 Request 对象之后 |
| `BEFORE_REQUEST` | 发送 HTTP 请求之前 |
| `ON_REQUEST` | 发送 HTTP 请求时（下载器层） |
| `AFTER_REQUEST` | 收到响应之后（最常用的钩子）|
| `ON_PARSE` | 解析响应时 |
| `ON_INIT` | 初始化种子时 |
| `BEFORE_PIPELINE` | 进入数据管道之前 |
| `ON_PIPELINE` | 执行数据管道时 |
| `AFTER_PIPELINE` | 数据管道执行完毕之后 |
| `BEFORE_RETRY` | 触发重试之前 |
| `AFTER_RETRY` | 重试流程执行之后 |
| `ON_CONSUME` | 消费任务时 |
| `ERROR_OCCURRED` | 发生未捕获异常时 |

---

## 注册事件的方式

### 方式一：`@events.on` 装饰器（推荐）

这是最常见的用法。装饰器将函数注册为**全局事件监听器**，对所有爬虫实例生效。

```python
from bricks import const
from bricks.core import events, signals
from bricks.spider.air import Context


@events.on(const.AFTER_REQUEST)
def check_response(context: Context):
    """检查响应是否正常，否则重试"""
    if context.response.get("code") != 0:
        raise signals.Retry
```

如果要将事件绑定到**某个爬虫类**（而非全局），把装饰器写在类方法上，并配合 `@staticmethod`：

```python
from bricks.spider import air
from bricks.spider.air import Context
from bricks import const
from bricks.core import events, signals


class MySpider(air.Spider):

    @staticmethod
    @events.on(const.AFTER_REQUEST)  # @on 放在最内层，紧贴函数定义
    def check_response(context: Context):
        if context.response.get("code") != 0:
            raise signals.Retry
```

> **注意**：`@on` 必须放在 `@staticmethod` 的内层（即紧贴函数定义）。

---

### 方式二：`EventManager.register()` 手动注册

适用于需要动态注册或在运行时创建事件的场景。

```python
from bricks.core.events import EventManager, Task
from bricks.core.context import Context
from bricks import const


def my_handler(context: Context):
    print("请求前处理:", context.seeds)


# 注册为持久事件（全局，无目标）
register = EventManager.register(
    Context(form=const.BEFORE_REQUEST, target=None),
    Task(func=my_handler)
)

# 之后可以通过返回的 Register 对象取消注册
register[0].unregister()
```

---

## `@events.on` 参数详解

```python
@events.on(
    form,           # 必填：事件类型，来自 bricks.state.const
    index=None,     # 执行顺序，数字越小越先执行，默认按注册顺序递增
    disposable=False,  # 是否为一次性事件（触发一次后自动移除）
    args=None,      # 传递给处理函数的额外位置参数
    kwargs=None,    # 传递给处理函数的额外关键字参数
    match=None,     # 匹配条件：函数或表达式字符串，返回 False 时跳过该事件
)
```

### `match` 条件过滤

`match` 可以是一个函数或字符串表达式，用于按条件决定是否执行该事件：

```python
# 只在特定页码时执行
@events.on(
    const.BEFORE_REQUEST,
    match=lambda context: context.seeds.get("page", 1) > 1
)
def skip_first_page(context: Context):
    ...

# 字符串表达式（context 变量可直接使用）
@events.on(
    const.AFTER_REQUEST,
    match="context.seeds.get('type') == 'detail'"
)
def handle_detail_response(context: Context):
    ...
```

### `index` 执行顺序

```python
# 先执行（index 小）
@events.on(const.AFTER_REQUEST, index=10)
def check_status(context: Context):
    ...

# 后执行（index 大）
@events.on(const.AFTER_REQUEST, index=20)
def log_response(context: Context):
    ...
```

### `disposable` 一次性事件

```python
# 只在第一次请求后执行，之后自动移除
@events.on(const.AFTER_REQUEST, disposable=True)
def first_request_handler(context: Context):
    print("这是第一次请求！")
```

---

## Register 对象

`EventManager.register()` 返回 `List[Register]`，每个 `Register` 提供以下操作：

```python
register = EventManager.register(ctx, task)[0]

# 取消注册
register.unregister()

# 修改执行顺序
register.reindex(5)

# 移动到最前
register.move2top()

# 移动到最后
register.move2tail()
```

---

## 错误处理模式

通过 `EventManager.invoke()` 触发事件时，可指定错误处理模式：

| 模式 | 行为 |
|---|---|
| `"raise"`（默认）| 抛出异常，中断流程 |
| `"ignore"` | 静默忽略异常，继续下一个事件 |
| `"output"` | 打印异常日志，继续下一个事件 |

框架内部的事件触发默认使用 `"raise"` 模式，因此在事件处理函数中主动 `raise` 信号（如 `raise signals.Retry`）可以精确控制爬虫行为。

---

## 常用模式

### 响应校验 + 重试

```python
from bricks import const
from bricks.core import events, signals
from bricks.spider.air import Context


@events.on(const.AFTER_REQUEST)
def validate_response(context: Context):
    """通用响应校验"""
    resp = context.response
    if not resp.ok:
        raise signals.Retry
    if resp.is_json() and resp.get("code") != 0:
        raise signals.Retry
```

### 统一设置请求头

```python
@events.on(const.BEFORE_REQUEST)
def set_headers(context: Context):
    context.request.headers["X-Custom-Token"] = "my-token"
```

### 请求耗时统计

```python
import time
from bricks import const
from bricks.core import events
from bricks.spider.air import Context

@events.on(const.BEFORE_REQUEST, index=0)
def start_timer(context: Context):
    context["_start_time"] = time.time()

@events.on(const.AFTER_REQUEST, index=999)
def end_timer(context: Context):
    cost = time.time() - context.get("_start_time", time.time())
    print(f"请求耗时: {cost:.2f}s  URL: {context.request.url}")
```

### 错误日志

```python
@events.on(const.ERROR_OCCURRED)
def on_error(context: Context):
    print(f"发生错误: {context.error}  种子: {context.seeds}")
```
