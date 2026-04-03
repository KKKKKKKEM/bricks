# 任务队列

任务队列（TaskQueue）是 Bricks 调度系统的核心，负责管理爬虫的种子（任务单元）。框架提供了单机和分布式两种队列实现，接口完全统一，切换时只需替换队列对象。

---

## 队列类型概览

| 类 | 模块 | 特点 | 适用场景 |
|---|---|---|---|
| `LocalQueue` | `bricks.lib.queues.local` | 内存队列，无外部依赖 | 单机爬虫 |
| `RedisQueue` | `bricks.lib.queues.redis_` | 持久化，支持多进程/多机器 | 分布式爬虫 |
| `SqliteQueue` | `bricks.lib.queues.sqlite_` | 文件持久化，无需部署 Redis | 单机持久化 |
| `SmartQueue` | `bricks.lib.queues.smart` | 带优先级的内存队列 | LocalQueue 的底层实现 |
| `CacheQueue` | `bricks.lib.queues.cache` | 内存缓存队列 | 临时缓存 |
| `RocksDBQueue` | `bricks.lib.queues.rocksdb_` | RocksDB 持久化 | 超大规模本地持久化 |

---

## 基本概念

每个队列名（`name`）下包含三个内部分区：

| 分区 | `qtypes` | 说明 |
|---|---|---|
| 当前队列 | `"current"` | 等待处理的种子 |
| 临时队列 | `"temp"` | 正在处理中的种子（防丢失备份）|
| 失败队列 | `"failure"` | 处理失败的种子 |

框架从 `current` 取种子时，会同时将其复制到 `temp`（防止 Worker 崩溃丢失）。处理成功后从 `temp` 删除，失败后移入 `failure`，等待重试。

---

## LocalQueue — 单机内存队列

默认队列，无需任何依赖，开箱即用。

```python
from bricks.lib.queues.local import LocalQueue

queue = LocalQueue()
```

### 在爬虫中使用

```python
from bricks.spider import air
from bricks.lib.queues.local import LocalQueue


class MySpider(air.Spider):
    queue = LocalQueue()
```

不显式指定时，爬虫默认使用 `LocalQueue`。

---

## RedisQueue — 分布式队列

需要部署 Redis，支持多机器、多进程协同爬取。

```python
from bricks.lib.queues.redis_ import RedisQueue

queue = RedisQueue(
    host="127.0.0.1",
    port=6379,
    password=None,
    database=0,
    genre="set",    # 底层存储类型：set（去重）/ list（顺序）/ zset（有序）
)
```

### 在爬虫中使用

```python
from bricks.spider import air
from bricks.lib.queues.redis_ import RedisQueue


class MySpider(air.Spider):
    queue = RedisQueue(
        host="redis-server",
        port=6379,
        password="your-password",
        database=1,
    )
```

### `genre` 参数

| 值 | 特性 |
|---|---|
| `"set"`（默认）| 自动去重，防止重复爬取 |
| `"list"` | 先进先出，保留顺序，允许重复 |
| `"zset"` | 有序集合，按 score 排序，支持优先级 |

---

## SqliteQueue — SQLite 持久化队列

不需要 Redis，数据持久化到本地文件，适合中小规模爬虫。

```python
from bricks.lib.queues.sqlite_ import SqliteQueue

queue = SqliteQueue(path="./spider_queue.db")
```

---

## 分布式爬虫

使用 `RedisQueue` 后，只需在多台机器上运行相同的爬虫代码，它们会共享同一个任务队列，自动实现分布式协作。

### 完整示例

```python
# 在多台机器上部署相同的代码
from bricks.spider import air
from bricks.lib.queues.redis_ import RedisQueue
from bricks import Request
from bricks.spider.air import Context


class DistributedSpider(air.Spider):
    # 所有机器使用同一个 Redis 队列
    queue = RedisQueue(
        host="shared-redis-host",
        port=6379,
        password="secret",
        database=0,
        genre="set",    # 使用 set 避免重复爬取
    )

    def make_seeds(self, context: Context, **kwargs):
        # 只有第一台机器（或第一次运行）会执行此方法生成种子
        # 其他机器直接从队列消费
        return [{"page": i} for i in range(1, 101)]

    def make_request(self, context: Context) -> Request:
        return Request(
            url="https://api.example.com/list",
            params={"page": context.seeds["page"]},
        )

    def parse(self, context: Context):
        return context.response.extract(
            engine="json",
            rules={"data.list": {"id": "id", "name": "name"}},
        )

    def item_pipeline(self, context: Context):
        print(f"[机器 {context.seeds['page']}]", context.items)
        context.success()


if __name__ == "__main__":
    DistributedSpider(concurrency=10).run()
```

---

## 断点续爬

Bricks 内置了断点续爬机制。当 `make_seeds` 方法使用生成器模式并配合 `$bookmark` 记录进度时，爬虫重启后会从上次中断的位置继续：

```python
def make_seeds(self, context: Context, **kwargs):
    bookmark = context.seeds.get("$bookmark", 1)  # 读取书签（默认从第1页）

    for page in range(bookmark, 10001):
        yield {
            "page": page,
            "$bookmark": page,   # 每次更新书签
        }
```

---

## 队列 API

所有队列类型均实现了相同的接口：

```python
# 放入种子
queue.put("spider_name", {"page": 1}, {"page": 2})

# 取出种子（取出后自动移入 temp 分区）
item = queue.get("spider_name", count=1)

# 查询队列大小
total = queue.size("spider_name")
current = queue.size("spider_name", qtypes=("current",))

# 清空队列
queue.clear("spider_name")

# 将失败/临时队列的种子重新放回当前队列
queue.reverse("spider_name")

# 智能反转（自动判断是否需要反转）
queue.smart_reverse("spider_name")

# 删除指定种子
queue.remove("spider_name", {"page": 1})

# 替换种子
queue.replace("spider_name", (old_seed, new_seed))
```

---

## 优先级队列

通过 `priority` 参数让种子插队到队列头部：

```python
queue.put("spider_name", {"page": 1, "urgent": True}, priority=True)
```

或在 `make_seeds` 中通过特殊字段 `$signpost`：

```python
# 在 form.Spider 中，优先处理某类型种子
yield {"type": "vip", "$signpost": 0}   # 低索引优先处理
yield {"type": "normal", "$signpost": 1}
```
