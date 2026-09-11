# 第九章：领域插件

插件使用当前的 `bricks.plugins` 包。种子源是普通函数；请求、分页和存储插件显式接收当前
Context，使用 `Spider.use` 或在业务方法中直接调用。不会反射函数参数、查询全局 Context 或执行字符串脚本。

## 种子读取

| 函数 | 输入与边界 |
| --- | --- |
| `make_seeds.by_range` | Python range 语义，支持步长、有限反向、无界递增和批次恢复 |
| `make_seeds.by_csv` | 有表头的 CSV 路径/glob，文件按路径排序；可对临时 SQLite `data` 表执行 SQL |
| `make_seeds.by_sqlite` | 显式原生连接、完整 SQL 和参数；不改写 LIMIT/OFFSET |
| `make_seeds.by_mongo` | 显式同步 Collection；默认排除 `_id`，支持查询、投影、排序和转换 |
| `make_seeds.by_redis` | 显式同步客户端；无损读取 set/list 或 SCAN 匹配的 string 键，内容必须是 JSON 对象 |

```python
from bricks.plugins import make_seeds
from bricks.spider.air import Spider


class CatalogSpider(Spider):
    """使用插件生成第一页种子。"""

    def make_seeds(self, context, **kwargs):
        """从初始化记录继续生产页码。

        Args:
            context: 初始化上下文。
            kwargs: 含框架提供的原始 record。

        Returns:
            种子批次迭代器。
        """
        return make_seeds.by_range(
            1, 101, key="page", batch_size=10, record=kwargs["record"]
        )
```

`record` 必须传入原对象，包括最初的空字典。`skip=None` 默认使用记录，传入整数则显式覆盖起点。
插件在 yield 前更新已生成的位置；Air 完成整批入队后才保存记录。中途失败时以旧持久位置和稳定任务 ID 重放。
直接遍历插件只更新内存字典，不自动把它持久化。

位置恢复要求源数据与排序稳定。Mongo 应提供包含唯一键的排序；不同查询使用不同 `checkpoint`。
Redis SCAN 可能产生重复，不承诺顺序和断点恢复；列表读取期间也应保持源稳定。
源中 ObjectId、日期、二进制等非 JSON 值必须用同步 `transform` 转换，不自动转字符串。

```python
batches = make_seeds.by_mongo(
    collection,
    projection={},
    sort=[("_id", 1)],
    checkpoint="articles-v1",
    record=record,
    transform=lambda row: {**row, "_id": str(row["_id"])},
)
```

CSV SQL 查询把每个文件装入本函数拥有的临时数据库，字段类型为 TEXT，表名为 `data`。
需要数字比较时显式 `CAST`。普通 CSV 路径逐行读取；SQL 查询需要额外内存。
调用方传给 `by_sqlite` 的连接归调用方管理。图节点可能串行跨线程推进源，应显式创建
`sqlite3.connect(..., check_same_thread=False)`，并确保该连接没有其他并发使用者。

## 存储与交付确认

| 函数 | 装配参数 | 写入行为 |
| --- | --- | --- |
| `storage.to_sqlite` | `path` 数据库路径、`table`、可选 `row_keys` | 每批独立连接和事务；不存在时建表，提供主键时 upsert |
| `storage.to_mongo` | 原生 `conn=collection`、可选 `row_keys` | insert_many 或逐条 upsert；必须使用有确认的写入 |
| `storage.to_csv` | `path`、可选 `fieldnames`、共享 `lock` | 检查表头后追加；并发写同一文件必须共享锁 |
| `storage.to_redis` | `path`、原生 `conn`、`key_type`、`row_keys`、`ttl` | 规范 JSON，set/list/string；string 需要业务键 |

```python
from bricks import hooks
from bricks.plugins import storage

spider.use(
    hooks.AFTER_PIPELINE,
    {
        "func": storage.to_sqlite,
        "kwargs": {
            "path": "articles.sqlite3",
            "table": "articles",
            "row_keys": ["id"],
        },
    },
)
```

也可以在 `item_pipeline` 中直接调用 `storage.to_sqlite(context, ...)`。所有存储插件返回 None，
不调用 ACK、不修改 Items。pipeline 正常返回后仍执行 AFTER_PIPELINE；存储插件报错时 Graph 失败，不提前确认。

SQLite 主键必须完整且非空，每批字段必须一致。既有表结构需与输入匹配，不自动迁移字段；
嵌套值应在业务层显式编码。Mongo 的业务唯一索引由调用方创建，多文档操作不保证整批原子。
CSV 追加无法回滚，也不自动按业务键去重。Redis MULTI 不保证命令错误回滚；string 模式使用
`path + ':' + sha256(规范 JSON 业务键数组)` 生成键，避免字段拼接冲突；不向业务记录写入 `$key`。

Redis、Mongo 客户端由调用方管理，Spider 停止后再关闭。安装可选客户端：

```bash
uv sync --locked --extra redis --extra mongo
```

## 请求、分页与条件动作

- `Before.set_proxy`：由 Air 默认注册，选择固定代理或当前 Slot 代理；请求显式值优先。
- `Before.fake_ua(context, factory=...)`：显式 UA 工厂，默认保留已有值，重试不重新生成。
- `After.show_response`：保留 `handle`、`fmt`；默认记录响应摘要。
- `After.bypass`：默认 2xx 接受，-1/429/5xx 有限重试；支持同步业务条件。
- `After.conditional_scripts(context, rules=[(条件, 动作)], first=False)`：按序执行命中动作。
- `scripts.is_success(context, match=..., pre=(), post=(), flow=None)`：组合条件和前后动作，默认成功抛出 Pass，失败抛出 Retry。
- `scripts.turn_page(context, match=..., key='page', step=1)`：复制当前种子并提交下一页；游标分页用 `action(context)` 返回新种子。
- `scripts.inject(context, flows=[动作, ...])`：按序执行同步动作，失败立即传播。

```python
from bricks.plugins import scripts

spider.use(
    hooks.AFTER_PIPELINE,
    {
        "func": scripts.turn_page,
        "kwargs": {"match": lambda context: context.seeds["page"] < 10},
        "index": 10,
    },
)
```

条件必须返回 bool，动作返回 None；这些组合助手只接受同步回调。
Air 的普通 Hook 仍支持 async def，可直接编写异步业务 Hook。
分页产生独立 Work，不延续当前 Slot 或自动继承当前执行选项。
`success=True` 会抛出 Success 提前结束当前任务，跳过后续插槽；默认 False 继续正常流程。
校验通过时需要绕过插槽剩余默认判断可抛出 Pass；重试只能发生在规定的下载前后阶段。
完整规则见[Hook 节点与 signals](12-hooks-and-signals.md)。

## 验证

```bash
BRICKS_TEST_REDIS_URL=redis://127.0.0.1:6379/0 \
BRICKS_TEST_MONGO_URL=mongodb://127.0.0.1:27017 \
uv run --locked pytest -q tests/crawler/test_plugins.py tests/crawler/test_proxies.py tests/crawler/test_air.py
```

测试只清理随机前缀和随机数据库。只使用专用测试实例，不指向业务服务。

[返回目录](README.md) · [下一章：代理管理](10-proxies.md)
