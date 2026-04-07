# 存储插件

Bricks 在 `bricks.plugins.storage` 模块中提供了四个开箱即用的存储函数，分别对应 SQLite、MongoDB、CSV 和 Redis。它们都接受相同的 `items` 参数（`List[dict]` 或 `Items` 对象），可以直接在 `item_pipeline` 中调用，也可以作为 `form.Spider` 的 `Pipeline` 节点使用。

---

## to_sqlite — 存入 SQLite

```python
from bricks.plugins.storage import to_sqlite
from bricks.db.sqlite import Sqlite

conn = Sqlite("./data.db")

to_sqlite(
    path="articles",          # 表名
    conn=conn,                # Sqlite 实例
    items=context.items,      # 数据列表
    row_keys=["id"],          # 主键字段（用于 upsert），可选
    with_lock=True,           # 是否使用锁（多线程写入时建议开启）
)
```

### 参数说明

| 参数 | 类型 | 说明 |
|---|---|---|
| `path` | `str` | 表名 |
| `conn` | `Sqlite` | SQLite 连接实例 |
| `items` | `List[dict] \| Items` | 要存储的数据 |
| `row_keys` | `List[str]` | 主键字段列表，用于 upsert（冲突时更新）|
| `with_lock` | `bool` | 是否加锁，多线程写入时应设为 `True`（默认）|

### 在 `item_pipeline` 中使用

```python
from bricks.plugins.storage import to_sqlite
from bricks.db.sqlite import Sqlite
from bricks.spider.air import Context


conn = Sqlite("./spider_data.db")


def item_pipeline(self, context: Context):
    to_sqlite(
        path="products",
        conn=conn,
        items=context.items,
        row_keys=["product_id"],
    )
    context.success()
```

### 在 `form.Spider` 中使用

```python
from bricks.spider import form
from bricks.plugins.storage import to_sqlite
from bricks.db.sqlite import Sqlite

conn = Sqlite("./data.db")

form.Pipeline(
    func=to_sqlite,
    kwargs={
        "path": "articles",
        "conn": conn,
        "row_keys": ["id"],
    },
    success=True,
)
```

---

## to_mongo — 存入 MongoDB

```python
from bricks.plugins.storage import to_mongo
from bricks.db.mongo import Mongo

conn = Mongo(host="127.0.0.1", port=27017)

to_mongo(
    path="articles",          # 集合名
    conn=conn,                # Mongo 实例
    items=context.items,      # 数据列表
    row_keys=["id"],          # 用于 upsert 的字段（相当于查询条件）
    database="spider_db",     # 数据库名，可选（默认使用 conn 的默认库）
)
```

### 参数说明

| 参数 | 类型 | 说明 |
|---|---|---|
| `path` | `str` | MongoDB 集合名 |
| `conn` | `Mongo` | MongoDB 连接实例 |
| `items` | `List[dict] \| Items` | 要存储的数据 |
| `row_keys` | `List[str]` | upsert 的查询字段，传入后以该字段为 key 做更新/插入 |
| `database` | `str` | 数据库名（可选）|
| `**kwargs` | | 透传给 `conn.write()` 的额外参数 |

### 完整示例

```python
from bricks.spider import air
from bricks.plugins.storage import to_mongo
from bricks.db.mongo import Mongo
from bricks.spider.air import Context


mongo = Mongo(host="localhost", port=27017)


class MySpider(air.Spider):

    def item_pipeline(self, context: Context):
        to_mongo(
            path="products",
            conn=mongo,
            items=context.items,
            row_keys=["sku_id"],        # 以 sku_id 做去重 upsert
            database="e_commerce",
        )
        context.success()
```

---

## to_csv — 存入 CSV 文件

```python
from bricks.plugins.storage import to_csv

to_csv(
    path="./output/articles.csv",   # CSV 文件路径
    items=context.items,            # 数据列表
    encoding="utf-8-sig",           # 编码（utf-8-sig 在 Excel 中不乱码）
)
```

### 参数说明

| 参数 | 类型 | 说明 |
|---|---|---|
| `path` | `str` | CSV 文件路径（不存在时自动创建）|
| `items` | `List[dict] \| Items` | 要存储的数据 |
| `conn` | `Writer` | 可复用的 CSV 写入器，可选（不传则自动创建）|
| `encoding` | `str` | 文件编码，默认 `"utf-8-sig"`（推荐，Excel 可直接打开）|
| `**kwargs` | | 透传给 `csv.DictWriter` 的额外参数 |

### 高性能写入（复用 Writer）

对于大量数据，建议创建一个 `Writer` 实例复用，避免每次都重新打开文件：

```python
from bricks.utils.csv_ import Writer
from bricks.plugins.storage import to_csv

# 在爬虫初始化时创建 Writer
csv_writer = Writer.create_safe_writer(
    path="./output/data.csv",
    encoding="utf-8-sig",
)

def item_pipeline(self, context: Context):
    to_csv(
        path="./output/data.csv",
        items=context.items,
        conn=csv_writer,   # 复用 Writer 实例
    )
    context.success()
```

---

## to_redis — 存入 Redis

支持三种 Redis 数据结构：`set`（有序集合去重）、`list`（列表）、`string`（键值对）。

```python
from bricks.plugins.storage import to_redis
from bricks.db.redis_ import Redis

conn = Redis(host="127.0.0.1", port=6379)

# 存为 set（自动去重）
to_redis(
    path="crawled:articles",    # Redis key 名称
    conn=conn,
    items=context.items,
    key_type="set",             # set / list / string
    ttl=86400,                  # key 的过期时间（秒），0 表示不过期
)
```

### 参数说明

| 参数 | 类型 | 说明 |
|---|---|---|
| `path` | `str` | Redis key 名称（key_type 为 string 时无效）|
| `conn` | `Redis` | Redis 连接实例 |
| `items` | `List[dict] \| Items` | 要存储的数据 |
| `key_type` | `str` | 存储类型：`"set"` / `"list"` / `"string"` |
| `row_keys` | `list` | 当 key_type 为 `"string"` 时必填，用于生成每条记录的 key |
| `splice` | `str` | key_type 为 `"string"` 时，多字段拼接分隔符，默认 `"\|"` |
| `ttl` | `int` | key 过期时间（秒），0 表示永不过期 |

### 三种存储模式

```python
# 1. set 模式：数据序列化为 JSON 字符串后存入 Redis set（自动去重）
to_redis(path="articles:set", conn=conn, items=items, key_type="set")

# 2. list 模式：数据序列化为 JSON 字符串后追加到 Redis list（保留顺序）
to_redis(path="articles:list", conn=conn, items=items, key_type="list")

# 3. string 模式：每条记录以 row_keys 字段拼接作为 key，记录本身作为 value
to_redis(
    path="",   # string 模式下 path 无效
    conn=conn,
    items=items,
    key_type="string",
    row_keys=["article_id"],   # 必填：用于生成 key 的字段
    splice="|",                # 多字段拼接时的分隔符
)
# 最终存储形如：MSET article_id_value '{"title": ..., "content": ...}'
```

---

## 在 `item_pipeline` 中组合多个存储目标

数据可以同时写入多个存储：

```python
from bricks.plugins.storage import to_sqlite, to_csv
from bricks.db.sqlite import Sqlite
from bricks.spider.air import Context


conn = Sqlite("./data.db")


def item_pipeline(self, context: Context):
    items = context.items

    # 同时写入 SQLite 和 CSV
    to_sqlite(path="products", conn=conn, items=items, row_keys=["id"])
    to_csv(path="./output/products.csv", items=items)

    context.success()
```

---

## 在 `form.Spider` 中使用内置脚本

`bricks.plugins.scripts` 也提供了几个在 `form.Spider` 配置中常用的辅助函数：

### `scripts.is_success()` — 响应成功检查

```python
from bricks.plugins import scripts
from bricks.spider import form

form.Pipeline(
    func=scripts.is_success,
    kwargs={
        "match": [
            "response.get('code') == 0",  # code 字段必须等于 0
        ]
    },
)
```

### `scripts.turn_page()` — 自动翻页

```python
from bricks.plugins import scripts
from bricks.spider import form

form.Pipeline(
    func=scripts.turn_page,
    kwargs={
        "match": [
            "response.get('data.hasNextPage') == 1",  # 有下一页时翻页
        ],
        "key": "page",    # 种子里的翻页字段名，默认 "page"
        "action": "+1",   # 翻页操作，默认 "+1"
    },
)
```

### `scripts.inject()` — 注入代码片段

在 pipeline 中运行任意代码片段（如修改 items、触发信号等）：

```python
from bricks.plugins import scripts
from bricks.spider import form
from bricks.utils import codes

form.Pipeline(
    func=scripts.inject,
    kwargs={
        "flows": [
            (codes.Type.code, [
                "import time",
                "for item in items: item['crawl_time'] = time.strftime('%Y-%m-%d %H:%M:%S')",
            ]),
        ]
    },
)
```
