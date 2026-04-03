# 爬虫基类

Bricks 提供三种爬虫基类，对应不同的开发范式。从灵活到结构化，选择最适合自己需求的方式。

---

## 三种基类对比

| 基类 | 模块 | 适用场景 | 代码量 | 灵活性 |
|---|---|---|---|---|
| `air.Spider` | `bricks.spider.air` | 复杂逻辑、多分支处理 | 多 | 最高 |
| `form.Spider` | `bricks.spider.form` | 多步骤请求、流程清晰 | 中 | 高 |
| `template.Spider` | `bricks.spider.template` | 标准化采集、快速配置 | 少 | 中 |

---

## air.Spider — 纯代码式

`air.Spider` 是最底层的基类，所有逻辑通过重写方法实现，适合需要精细控制流程的场景。

### 必须实现的方法

#### `make_seeds(context, **kwargs)`

生产初始种子，支持三种形式：

```python
# 1. 返回列表
def make_seeds(self, context, **kwargs):
    return [{"page": 1}, {"page": 2}, {"page": 3}]

# 2. 返回单个字典
def make_seeds(self, context, **kwargs):
    return {"page": 1}

# 3. 生成器（适合大批量）
def make_seeds(self, context, **kwargs):
    for page in range(1, 100):
        yield {"page": page}
```

`**kwargs` 中包含 `record`，是上一次运行保存的记录，用于断点续传：

```python
def make_seeds(self, context, **kwargs):
    record = kwargs.get("record", {})
    start_page = record.get("last_page", 1)
    for page in range(start_page, 100):
        yield {"page": page}
```

#### `make_request(context) -> Request`

将种子转换为 `Request` 对象：

```python
def make_request(self, context) -> Request:
    seeds = context.seeds
    return Request(
        url="https://api.example.com/data",
        params={"page": seeds["page"], "size": 20},
        method="GET",
        headers={
            "User-Agent": "@chrome",     # 自动替换为随机 Chrome UA
            "Content-Type": "application/json",
        },
        # body={"key": "value"},         # POST body
        # cookies={"token": "abc"},
        # proxies="http://127.0.0.1:7890",  # 直接指定代理
        # ok="response.status_code == 200", # 自定义成功判断
        max_retry=3,                      # 最大重试次数
    )
```

#### `parse(context) -> list | Items`

解析响应数据：

```python
def parse(self, context):
    response = context.response
    
    # JSON 路径提取
    return response.extract(
        engine="json",
        rules={"data.list": {"id": "id", "name": "name"}},
    )
    
    # 或者直接操作
    data = response.json()
    return [{"id": item["id"]} for item in data["data"]["list"]]
```

#### `item_pipeline(context)`

处理数据并标记完成：

```python
def item_pipeline(self, context):
    items = context.items
    # 写入数据库、文件等
    for item in items:
        db.insert(item)
    context.success()  # 标记种子处理完毕，从队列移除
```

### 可选方法

#### `on_retry(context)`

重试前的钩子（框架默认实现了重试逻辑，通常不需要重写）。

### 完整示例

```python
from bricks import Request, const
from bricks.core import events, signals
from bricks.spider import air
from bricks.spider.air import Context


class NewsSpider(air.Spider):

    def make_seeds(self, context: Context, **kwargs):
        return [{"page": i} for i in range(1, 11)]

    def make_request(self, context: Context) -> Request:
        return Request(
            url="https://news.example.com/api/list",
            params={"page": context.seeds["page"], "pageSize": 20},
            headers={"User-Agent": "@chrome"},
        )

    def parse(self, context: Context):
        return context.response.extract(
            engine="json",
            rules={
                "data.articles": {
                    "id": "id",
                    "title": "title",
                    "author": "author.name",
                    "publishedAt": "publishedAt",
                }
            },
        )

    def item_pipeline(self, context: Context):
        print(f"获取到 {len(context.items)} 条数据")
        context.success()

    @staticmethod
    @events.on(const.AFTER_REQUEST)
    def check_success(context: Context):
        if context.response.get("code") != 200:
            raise signals.Retry


if __name__ == "__main__":
    NewsSpider(concurrency=3).run()
```

---

## form.Spider — 自定义流程配置式

`form.Spider` 通过 `Config` 描述爬取流程，每一步（下载、解析、管道）作为配置节点，按顺序执行。

### Config 结构

```python
from bricks.spider import form

form.Config(
    init=[...],    # 初始化种子的方法列表
    spider=[...],  # 爬取流程节点列表（按顺序执行）
    events={...},  # 事件钩子
)
```

### 流程节点类型

#### `form.Download` — 发送请求

```python
form.Download(
    url="https://api.example.com/list",
    params={"page": "{page}"},   # {page} 会从 seeds 中取值
    method="GET",
    headers={"User-Agent": "@chrome"},
    body=None,
    ok=...,          # 成功判断，... 表示默认
    max_retry=5,     # 最大重试次数
    archive=False,   # 是否归档（断点续传）
)
```

URL 参数中的 `{key}` 语法会从当前 `seeds` 字典中取值，支持类型转换：

```python
params={
    "page": "{page}",        # 直接取值
    "size": "{size:int}",    # 转换为 int
    "tag": "{tag:lower}",    # 转换为小写
}
```

#### `form.Parse` — 解析响应

```python
form.Parse(
    func="json",       # "json" / "xpath" / "jsonpath" / "regex" / 可调用对象
    kwargs={
        "rules": {
            "data.list": {
                "id": "id",
                "name": "name",
            }
        }
    },
    layout=form.Layout(
        rename={"userId": "user_id"},  # 字段重命名
        default={"status": 1},         # 默认值
        factory={"ts": lambda: time.time()},  # 工厂函数
        show={"id": True, "name": True},      # 字段白名单
    ),
)
```

#### `form.Pipeline` — 数据处理

```python
form.Pipeline(
    func=lambda context: save_to_db(context.items),
    success=True,   # 执行后自动调用 context.success()
    layout=form.Layout(rename={"userId": "user_id"}),
)
```

#### `form.Task` — 自定义任务

在流程中执行任意逻辑（如校验响应、翻页等）：

```python
from bricks.plugins import scripts

form.Task(
    func=scripts.is_success,
    kwargs={"match": ["context.response.get('code') == 0"]},
),
form.Task(
    func=scripts.turn_page,
    kwargs={
        "match": ["context.response.get('data.hasNextPage') == 1"],
        "key": "page",
    },
),
```

#### `form.Init` — 初始化种子

```python
form.Init(
    func=lambda: {"page": 1},     # 直接返回种子
    # 或从数据库查询
    func="mymodule.get_seeds",    # 字符串形式，延迟加载
    layout=form.Layout(
        rename={"pageNum": "page"},
    ),
)
```

### 完整示例

```python
from bricks.plugins import scripts
from bricks.spider import form


class ListSpider(form.Spider):

    @property
    def config(self) -> form.Config:
        return form.Config(
            init=[
                form.Init(func=lambda: {"page": 1}),
            ],
            spider=[
                # Step 1: 下载列表页
                form.Download(
                    url="https://api.example.com/list",
                    params={"page": "{page}", "size": 20},
                    headers={"User-Agent": "@chrome"},
                ),
                # Step 2: 校验响应
                form.Task(
                    func=scripts.is_success,
                    kwargs={"match": ["context.response.get('code') == 0"]},
                ),
                # Step 3: 解析数据
                form.Parse(
                    func="json",
                    kwargs={"rules": {"data.list": {"id": "id", "title": "title"}}},
                    layout=form.Layout(default={"source": "example"}),
                ),
                # Step 4: 翻页
                form.Task(
                    func=scripts.turn_page,
                    kwargs={
                        "match": ["context.response.get('data.hasNextPage') == 1"],
                        "key": "page",
                    },
                ),
                # Step 5: 存储
                form.Pipeline(
                    func=lambda context: print(context.items),
                    success=True,
                ),
            ],
        )


if __name__ == "__main__":
    ListSpider(concurrency=3).run()
```

---

## template.Spider — 固定流程配置式

`template.Spider` 将下载、解析、管道分开声明，用 `$config`（signpost）在多个配置间轮换，适合规律性较强的多阶段爬取。

### Config 结构

```python
from bricks.spider import template

template.Config(
    init=[...],      # 种子初始化
    events={...},    # 事件钩子
    download=[...],  # 下载配置列表（按 signpost 轮换）
    parse=[...],     # 解析配置列表（按 signpost 轮换）
    pipeline=[...],  # 管道配置列表
)
```

`signpost` 是当前配置的索引（从 0 开始），通过 `context.next_step()` 切换到下一个配置。

### 多阶段爬取示例

以"先爬列表页，再爬详情页"为例：

```python
import time
from bricks import const
from bricks.plugins import scripts
from bricks.spider import template


class MultiStepSpider(template.Spider):

    @property
    def config(self) -> template.Config:
        return template.Config(
            init=[
                template.Init(func=lambda: {"page": 1}),
            ],
            events={
                const.AFTER_REQUEST: [
                    # signpost=0（列表页）时校验响应
                    template.Task(
                        match="context.signpost == 0",
                        func=scripts.is_success,
                        kwargs={"match": ["context.response.get('code') == 0"]},
                    ),
                ],
                const.BEFORE_PIPELINE: [
                    # 列表页处理完毕 → 切换到详情页配置
                    template.Task(
                        match="context.signpost == 0",
                        func=lambda context: context.next_step(
                            *[{"detail_url": item["url"]} for item in context.items]
                        ),
                    ),
                    # 列表页翻页
                    template.Task(
                        match="context.signpost == 0",
                        func=scripts.turn_page,
                        kwargs={"match": ["context.response.get('data.hasNext') == 1"]},
                    ),
                ],
            },
            download=[
                # 配置 0：列表页
                template.Download(
                    url="https://api.example.com/list",
                    params={"page": "{page}"},
                ),
                # 配置 1：详情页
                template.Download(
                    url="{detail_url}",
                ),
            ],
            parse=[
                # 解析 0：列表页
                template.Parse(
                    func="json",
                    kwargs={"rules": {"data.list": {"id": "id", "url": "url"}}},
                ),
                # 解析 1：详情页
                template.Parse(
                    func="json",
                    kwargs={"rules": {"data": {"id": "id", "content": "content"}}},
                ),
            ],
            pipeline=[
                template.Pipeline(
                    func=lambda context: print(context.items),
                    success=True,
                    match="context.signpost == 1",  # 只在详情页存储
                ),
            ],
        )


if __name__ == "__main__":
    MultiStepSpider(concurrency=5).run()
```

---

## 特殊种子字段

框架保留了若干以 `$` 开头的种子字段：

| 字段 | 说明 |
|---|---|
| `$config` | `template.Spider` 中的配置索引（signpost） |
| `$bookmark` | `form.Spider` 中的当前节点位置 |
| `$signpost` | `form.Spider` 中的下一个节点位置 |
| `$division` | 分裂种子的唯一标识 |
| `$futureMaxRetry` | 下次拉起时的最大重试次数 |
| `$futureRetry` | 下次拉起时的当前重试次数 |

---

## survey — 调试工具

`spider.survey` 方法允许你快速测试单个种子，而无需运行完整的爬虫：

```python
results = MySpider.survey(
    {"page": 1},
    {"page": 2},
    extract=["seeds", "response", "items"],  # 只提取指定字段
)
for r in results:
    print(r["items"])
```

---

## 下一步

- [事件系统详解](events.md)
- [Request / Response API](parsers.md)
