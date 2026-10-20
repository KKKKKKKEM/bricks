# 第十三章：配置式爬虫

`bricks.spider.form.Spider` 用一组步骤声明流程，复用 Air 的任务队列、重试、插件、代理与 Slot 会话。
旧版 form 与 template 合并为这一种写法：template 按 `$config` 轮换多套配置的场景，改写为多个步骤段。

## 写法

```python
from bricks import hooks
from bricks.parsers import Rows
from bricks.plugins import make_seeds, scripts, storage
from bricks.spider import form


def save(context):
    storage.to_sqlite("products", "products.sqlite3", row_keys=["id"])


def add_client(context):
    context.request.headers["X-Client"] = "bricks"


class ProductSpider(form.Spider):
    config = form.Config(
        init=[form.Init({"page": 1})],
        spider=[
            form.Download(url="https://example.com/api/list", params={"page": "{page}"}),
            form.Task(scripts.is_success, kwargs={"match": "context.response.get('code') == 0"}),
            form.Parse("json", Rows("data.list", {"id": "id"})),
            form.Task(scripts.turn_page, kwargs={"match": "context.response.get('data.hasNext')"}),
            form.Download(url="https://example.com/api/item/{id}", each=True),
            form.Parse(
                "json",
                {"id": "data.id", "title": "data.title"},
                layout=form.Layout(default={"page": "{page}"}),
            ),
            form.Pipeline(save),
        ],
        events={hooks.BEFORE_REQUEST: [form.Task(add_client)]},
    )


ProductSpider(concurrency=4).run()
```

`config` 可以是类属性，也可以是读取实例配置的属性；它在打开时编译一次。构造参数与 Air 完全相同。

## 步骤与段

每个 `Download` 开启一段，段内依次是：下载后的 Task、至多一个 Parse、解析后的 Task、按顺序执行的
Pipeline 与 Task。打开时校验：首步必须是 Download，首段不能使用 `each=True`，每段至多一个 Parse 且位于
该段 Pipeline 之前。

| 步骤 | 执行位置 | 说明 |
| --- | --- | --- |
| `Download(url, ...)` | make_request | 字段同 Request；字符串中的 `{name}` 读取同名种子字段，整串恰为占位符时保留原类型；`max_retry`、`ok` 设置本段重试上限与成功判断 |
| 下载后的 `Task` | AFTER_REQUEST 末尾 | 可抛出 `signals.Retry` 重新下载本段，排在 bypass 等内置检查之后 |
| `Parse("json", rules)` | parse | 引擎为 json、jsonpath、css、xpath、regex，规则语法同 `bricks.parsers.match` |
| `Parse(func)` | parse | 函数返回 Items 或 list[dict]；本段没有 Parse 时 Items 为空 |
| 解析后的 `Task` | parse 之后 | 可读取 `context.items`，常用于翻页 |
| `Pipeline(func)` | item_pipeline | 返回 None 表示继续；layout 只作用于本步骤的 Items 副本 |

Task 即 `bricks.hooks.Task`，函数可以显式首参接收 Context，也可以通过 `Context.get_context()` 读取，
`match` 条件同样生效；index 在步骤中不使用。Parse 与 Pipeline 的函数遵循同一 Context 传入规则。
`events` 中的声明在打开时经 `Spider.use` 注册到各阶段插槽，与 Air 插件一致。

`Layout` 依次执行 `default`（缺失字段补默认值，字符串占位符按种子渲染）、`show`（False 删除字段）、
`factory`（单参数函数接收当前值并写回）和 `rename`。

## 段间推进与派发

每个 Work 只执行一段，段号保存在 Work 的 `crawler.form.stage` 执行配置中，业务种子不额外增加 `$` 字段。
本段 AFTER_PIPELINE 插槽开始时提交下一段：

- 下一段 `Download(each=False)`：沿用当前种子（含本段 Task 对 `context.seeds` 的修改），例如先取令牌再请求接口；
- 下一段 `Download(each=True)`：对本段每条 Items 提交一个任务，种子为当前种子合并该记录，例如列表页派发详情页；
- `Download(affinity="any")` 让进入该段的任务只入队等待任意 Worker 领取，缺省 `current` 持久化后优先在当前 Slot
  继续，保持同一会话。

`context.submit(...)` 默认留在当前段，`scripts.turn_page` 因此在列表段内翻页；显式传入
`options={"crawler.form.stage": n}` 可提交到指定段。`context.stage` 读取当前段号。
`raise signals.Success()` 表示当前种子到此结束，不再进入后续段；BEFORE_PIPELINE 中的 Skip 只跳过本段存储，仍会推进。

## 断点与恢复

下一段任务在当前交付确认之前经任务存储原子持久化，因此：

- 进程在段中途中断时，当前段未确认，租约到期后由任意 Worker 从本段 Download 重新执行；
- 已持久化的下一段任务不会丢失，恢复时重新提交同一内容按稳定 Work ID 去重，不重复下载已完成的段；
- 同一段内的步骤会随重试重放，Pipeline 写入应按业务键幂等，例如 `row_keys`。

初始种子由 `Init` 提供：静态种子或种子列表每轮深复制；函数以 `source(*args, record=record, **kwargs)` 调用，
可直接使用 `make_seeds.by_csv`、`by_range` 等插件，初始化断点随提交保存。持久性与交付语义取决于任务存储，
跨进程或重启后恢复需使用 SqliteTaskQueue（单机）或 RedisTaskQueue（多机），见[任务队列](07-task-queues.md)。

## 从旧 form/template 迁移

| 旧写法 | 当前写法 |
| --- | --- |
| `form.Init(func=lambda: {"page": 1})` | `form.Init({"page": 1})`；函数来源以 `record=` 关键字接收恢复记录 |
| `form.Parse(func="json", kwargs={"rules": {...}})` | `form.Parse("json", rules)`，规则改用 `bricks.parsers` 语法 |
| `form.Pipeline(..., success=True)` | 删除 success；段完成即确认，提前结束改用 `raise signals.Success()` |
| `Download(archive=True)`、`$signpost`、`$bookmark` | 每段自动持久化，无需标记 |
| `Download(ok=..., max_retry=...)` | 保留同名字段，按段生效；执行配置 `crawler.response.ok`、`crawler.retry.max` 优先 |
| template 的 `download/parse/pipeline` 轮换与 `context.next_step()` | 按顺序写成多段，下一段用 `each=True` 派发记录 |
| `template.Task(match="context.signpost == 0")` | 把 Task 写在对应段内，或在条件中读取 `context.stage` |
| `Layout(factory={"time": lambda: time.time()})` | factory 函数接收当前值：`lambda _: time.time()` |

[返回目录](README.md)
