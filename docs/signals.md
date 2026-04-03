# 信号机制

信号（Signal）是 Bricks 流程控制的语言。通过在事件处理函数或爬虫方法中 `raise` 一个信号，你可以精确地控制爬虫的下一步行为——重试、跳过、退出、等待，或切换流程。

所有信号都继承自 `BaseException`（而非 `Exception`），因此只会被框架的内部机制捕获，不会被普通的 `except Exception` 误捕获。

---

## 信号概览

```python
from bricks.core import signals
```

| 信号类 | 触发方式 | 作用 |
|---|---|---|
| `Retry` | `raise signals.Retry` | 重新将当前种子放回队列，等待重试 |
| `Success` | `raise signals.Success` | 标记种子处理成功并从临时队列移除 |
| `Failure` | `raise signals.Failure` | 标记种子处理失败，移入失败队列 |
| `Exit` | `raise signals.Exit` | 退出当前 Worker 任务处理，不影响其他 Worker |
| `Break` | `raise signals.Break` | 中断当前 flow 流程，不再处理后续步骤 |
| `Switch` | `raise signals.Switch()` | 切换到另一个 flow 步骤 |
| `Skip` | `raise signals.Skip` | 跳过当前及后续 flow 步骤 |
| `Wait` | `raise signals.Wait(duration=1)` | 暂停指定秒数后继续 |
| `Empty` | `raise signals.Empty` | 标记队列为空，触发空队列处理逻辑 |
| `Pass` | `raise signals.Pass` | 空操作，透传，不影响流程 |

---

## Retry — 重试

将当前种子重新放回任务队列，等待再次被消费。适用于网络错误、响应异常等临时失败场景。

```python
from bricks.core import signals
from bricks.spider.air import Context


@events.on(const.AFTER_REQUEST)
def check_response(context: Context):
    if context.response.get("code") != 0:
        raise signals.Retry
```

### 设置最大重试次数

在种子中通过 `$futureMaxRetry` 字段控制最大重试次数：

```python
def make_seeds(self, context: Context, **kwargs):
    return [
        {
            "page": 1,
            "$futureMaxRetry": 3,   # 最多重试 3 次
        }
    ]
```

### 立即重试（不入队）

`$futureRetry=True` 时，种子不经过队列，由当前 Worker 立即重试：

```python
yield {
    "url": "https://example.com",
    "$futureRetry": True,   # 立即重试，不经队列
}
```

---

## Success — 标记成功

主动标记当前种子处理成功，从临时队列（temp）中移除，不再重试。通常在 `item_pipeline` 中调用 `context.success()` 来触发，这是对 `raise signals.Success` 的封装。

```python
def item_pipeline(self, context: Context):
    # 存储数据...
    context.success()   # 等价于 raise signals.Success
```

也可以在 `form.Pipeline` 节点中通过 `success=True` 自动触发：

```python
form.Pipeline(
    func=my_save_func,
    success=True,   # 执行完成后自动标记成功
)
```

---

## Failure — 标记失败

将当前种子标记为失败，移入失败队列（failure），不再自动重试。

```python
def item_pipeline(self, context: Context):
    try:
        save_to_db(context.items)
    except Exception:
        raise signals.Failure  # 存储失败，标记为永久失败
```

---

## Exit — 退出 Worker

退出当前 Worker 的任务处理循环，但不影响其他 Worker 继续工作。通常用于检测到某种终止条件时。

```python
@events.on(const.AFTER_REQUEST)
def check_end(context: Context):
    if context.response.get("has_more") is False:
        raise signals.Exit  # 没有更多数据，退出当前 Worker
```

---

## Break — 中断当前 flow

中断当前 flow 的执行链，后续步骤不再运行，但不标记种子的成功/失败状态。

```python
from bricks.core import signals

@events.on(const.AFTER_REQUEST)
def skip_empty(context: Context):
    data = context.response.get("data.list")
    if not data:
        raise signals.Break  # 没有数据，中断后续解析和存储
```

---

## Skip — 跳过后续步骤

与 `Break` 类似，跳过当前 flow 中后续的所有步骤。

```python
from bricks.core import signals

def make_request(self, context: Context):
    seeds = context.seeds
    if seeds.get("skip"):
        raise signals.Skip  # 不发请求，直接跳过
    return Request(url="...")
```

---

## Wait — 暂停等待

当前 Worker 暂停指定时间（秒），然后继续处理。适用于限速场景。

```python
from bricks.core import signals

@events.on(const.AFTER_REQUEST)
def rate_limit(context: Context):
    if context.response.status_code == 429:  # Too Many Requests
        raise signals.Wait(duration=5)   # 等待 5 秒后重试
```

`Wait` 信号通常与 `Retry` 配合使用——先等待再重试。也可以直接在种子中设置等待时间：

```python
yield {
    "page": 2,
    "$futureRetry": True,   # 立即重试
    # Wait 会让 Worker 等待一段时间
}
```

---

## Empty — 队列为空

标记队列当前为空，触发框架的空队列逻辑（如等待新种子、或关闭 Worker）。通常由框架自动触发，不需要手动使用。

---

## Pass — 空操作

透传信号，不影响任何流程。可以用于占位或条件分支中的无操作分支。

```python
from bricks.core import signals

@events.on(const.AFTER_REQUEST)
def maybe_retry(context: Context):
    if should_retry(context):
        raise signals.Retry
    else:
        raise signals.Pass  # 什么都不做，继续正常流程
```

---

## 信号与 Context 方法对照

爬虫的 `Context` 对象提供了几个方便方法，是对信号的封装：

| Context 方法 | 等价信号 |
|---|---|
| `context.success()` | `raise signals.Success` |
| `context.failure()` | `raise signals.Failure` |
| `context.retry()` | `raise signals.Retry` |

---

## 信号在 form.Spider 中

`form.Spider` 的节点配置也可以通过返回值间接触发信号效果：

```python
form.Pipeline(
    func=save_data,
    success=True,   # func 执行后自动触发 Success 信号
)

form.Download(
    url="https://example.com",
    # 下载失败时可通过 AFTER_REQUEST 事件触发 Retry
)
```

---

## 完整示例

```python
from bricks import const, Request
from bricks.core import events, signals
from bricks.spider import air
from bricks.spider.air import Context


class RobustSpider(air.Spider):

    def make_seeds(self, context: Context, **kwargs):
        return [
            {"page": i, "$futureMaxRetry": 5}
            for i in range(1, 11)
        ]

    def make_request(self, context: Context) -> Request:
        return Request(url="https://api.example.com/list",
                       params={"page": context.seeds["page"]})

    def parse(self, context: Context):
        return context.response.extract(
            engine="json",
            rules={"data": {"id": "id", "name": "name"}},
        )

    def item_pipeline(self, context: Context):
        if not context.items:
            raise signals.Break  # 无数据，跳过存储
        save_to_db(context.items)
        context.success()

    @staticmethod
    @events.on(const.AFTER_REQUEST)
    def validate(context: Context):
        resp = context.response
        if not resp.ok:
            raise signals.Wait(duration=2)  # 服务器错误，等 2 秒
        if resp.get("code") == 429:
            raise signals.Wait(duration=10)  # 限速，等 10 秒
        if resp.get("code") != 0:
            raise signals.Retry  # 业务错误，直接重试
```
