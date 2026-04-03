# 快速入门

本文档带你在 5 分钟内了解 Bricks 的核心概念，并运行第一个爬虫。

---

## 安装

```bash
pip install -U bricks-py
```

**Python 版本要求**：>= 3.9

---

## 核心概念

Bricks 围绕以下几个概念构建：

### 种子（Seed）

**种子**是爬虫的最小工作单元，本质上是一个字典（`dict`），描述了"要爬什么"。例如：

```python
{"page": 1, "category": "news"}
```

种子由 `make_seeds` 方法生产，投放到任务队列后被消费。

### 上下文（Context）

**上下文**贯穿整个请求生命周期，携带当前任务的所有状态：

```python
context.seeds    # 当前种子
context.request  # 当前请求对象
context.response # 当前响应对象
context.items    # 解析出的数据条目
```

### 流程（Flow）

一次完整的爬取流程按照以下顺序执行：

```
make_seeds → put_seeds → get_seeds → make_request → on_request → parse → item_pipeline
```

每个阶段前后都有事件钩子可以注册，无需修改核心代码即可扩展行为。

### 信号（Signal）

信号是控制流程走向的机制，通过 `raise` 触发：

| 信号 | 作用 |
|---|---|
| `signals.Retry` | 触发重试 |
| `signals.Success` | 标记成功并从队列移除种子 |
| `signals.Failure` | 标记失败并将种子移至失败队列 |
| `signals.Exit` | 退出当前任务 |
| `signals.Break` | 中断当前流程 |

---

## 第一个爬虫

### 步骤一：继承爬虫基类

Bricks 提供三种爬虫基类，最基础的是 `air.Spider`：

```python
from bricks.spider import air
from bricks.spider.air import Context
from bricks import Request


class MySpider(air.Spider):
    pass
```

### 步骤二：实现 make_seeds

告诉爬虫要爬取哪些页面：

```python
def make_seeds(self, context: Context, **kwargs):
    return [{"page": 1}, {"page": 2}, {"page": 3}]
```

`make_seeds` 支持三种返回方式：
- 返回单个字典
- 返回字典列表
- 使用生成器 `yield`（适合大量种子）

### 步骤三：实现 make_request

将种子转换为 HTTP 请求：

```python
def make_request(self, context: Context) -> Request:
    seeds = context.seeds
    return Request(
        url="https://api.example.com/list",
        params={"page": seeds["page"]},
        headers={"User-Agent": "@chrome"},  # @chrome 会自动替换为真实 UA
    )
```

### 步骤四：实现 parse

从响应中提取数据：

```python
def parse(self, context: Context):
    return context.response.extract(
        engine="json",
        rules={"data.list": {"id": "id", "title": "title"}},
    )
```

### 步骤五：实现 item_pipeline

处理提取到的数据并标记完成：

```python
def item_pipeline(self, context: Context):
    for item in context.items:
        print(item)
    context.success()  # 必须调用，否则种子不会从队列中移除
```

### 完整代码

```python
from bricks import Request
from bricks.spider import air
from bricks.spider.air import Context


class MySpider(air.Spider):

    def make_seeds(self, context: Context, **kwargs):
        return [{"page": 1}, {"page": 2}, {"page": 3}]

    def make_request(self, context: Context) -> Request:
        seeds = context.seeds
        return Request(
            url="https://api.example.com/list",
            params={"page": seeds["page"]},
        )

    def parse(self, context: Context):
        return context.response.extract(
            engine="json",
            rules={"data.list": {"id": "id", "title": "title"}},
        )

    def item_pipeline(self, context: Context):
        for item in context.items:
            print(item)
        context.success()


if __name__ == "__main__":
    spider = MySpider(concurrency=5)  # 5 并发
    spider.run()
```

---

## Spider 初始化参数

```python
spider = MySpider(
    concurrency=5,           # 并发数，默认 1
    downloader=cffi.Downloader(),  # 下载器，默认 curl-cffi
    task_queue=LocalQueue(), # 任务队列，默认本地队列
    proxy=None,              # 全局代理
    forever=False,           # 是否持续运行（队列空也不退出）
)
```

---

## 下一步

- [三种爬虫基类详解](spiders.md)
- [事件系统与生命周期钩子](events.md)
- [数据解析规则](parsers.md)
