# 第九章：领域插件

插件使用当前的 `bricks.plugins` 包。内置请求、分页、条件和存储插件通过 `Context.get_context()`
取得当前消费状态，可以像 dev 一样直接交给 `Spider.use`，无需手工传递 Context。自定义插件仍可选择
显式首参 Context；`@hooks.current` 用于声明带可选业务参数但不接收 Context 的插件。

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
    "articles",
    collection,
    projection={},
    sort=[("_id", 1)],
    checkpoint="articles-v1",
    record=record,
    transform=lambda row: {**row, "_id": str(row["_id"])},
)
```

CSV SQL 查询把每个文件装入本函数拥有的临时数据库，字段类型为 TEXT；`<TABLE>` 会替换为 `data`，
也可以直接使用 `data` 表名。需要数字比较时显式 `CAST`。普通 CSV 路径逐行读取；SQL 查询需要额外内存。
`by_csv` 若路径已存在则按字面路径读取，否则展开 glob；`by_range` 直接计算恢复起点。
若向 `by_sqlite`、`by_mongo` 或 `to_sqlite` 传入原生连接，应按 Slot 分别取得，避免跨线程共享连接。
多个 Slot 共用 SQLite Connection 时，`to_sqlite` 必须传入共享互斥锁 `lock`。

## 存储与交付确认

| 函数 | 装配参数 | 写入行为 |
| --- | --- | --- |
| `storage.to_sqlite` | `path` 表名、`conn` 数据库路径或 Connection、可选 `items/row_keys/with_lock/lock` | 事务建表并 insert/upsert |
| `storage.to_mongo` | `path` Collection 名、原生 `conn`、可选 `items/row_keys/database` | insert_many 或 ordered bulk upsert；必须使用有确认的写入 |
| `storage.to_csv` | `path`、可选 `items/conn/encoding/header/lock` | 检查表头后追加；并发写同一文件必须共享锁 |
| `storage.to_redis` | `path`、原生 `conn`、`key_type`、`row_keys`、`ttl` | 规范 JSON，set/list/string；string 需要业务键 |

```python
from bricks import hooks
from bricks.plugins import storage

spider.use(
    hooks.AFTER_PIPELINE,
    {
        "func": storage.to_sqlite,
        "kwargs": {
            "path": "articles",
            "conn": "articles.sqlite3",
            "row_keys": ["id"],
        },
    },
)
```

也可以独立调用 `storage.to_sqlite("articles", "articles.sqlite3", items, ...)`；在业务方法或 Hook 中
省略 items 时自动读取当前 `context.items`。所有存储插件返回 None，
不调用 ACK、不修改 Items。pipeline 正常返回后仍执行 AFTER_PIPELINE；存储插件报错时 Graph 失败，不提前确认。

SQLite 主键必须完整且非空，每批字段必须一致。既有表结构需与输入匹配，不自动迁移字段；
业务唯一索引由调用方创建，多文档操作不保证整批原子；upsert 的非 key 字段不能包含点号或以 `$` 开头。CSV 追加无法回滚，也不自动按业务键去重。Redis MULTI 不保证
命令错误回滚；string 模式使用 `path + ':' + sha256(规范 JSON 业务键数组)` 生成键，避免字段拼接冲突；不向业务记录写入 `$key`。

Redis、Mongo 客户端由调用方管理，Spider 停止后再关闭。安装可选客户端：

```bash
uv sync --locked --extra redis --extra mongo
```

## 请求、分页与条件动作

- `Before.set_proxy(timeout=30)`：由 Air 默认注册，选择固定代理或当前 Slot 代理；请求显式值优先。
- `Before.fake_ua()`：展开请求头中的 `@chrome/@edge/@mobile/@pc/@random`，也可传同步 factory。
- `After.show_response` 保留 `handle`、`fmt`；默认记录响应摘要。`fmt=1` 仅列出请求头名称及请求体是否存在，
  不输出任何请求头值或请求体内容；代理认证、请求 URL 中的认证信息、查询参数值及片段均脱敏。
  方法、状态、字节数和脱敏后的 URL/代理仍可用于诊断。
- `After.bypass(ok=...)`：默认及映射未命中均采用 2xx 接受、-1/429/5xx 有限重试、其他状态 Failure；字符串、映射或 callable 可覆盖，省略时读取 `$ok`。
- `After.conditional_scripts(rules=None)`：按序执行全部命中动作；省略 rules 时读取 execution options 的 `$scripts`。
- 动态脚本仅暴露 `context/ctx/request/response/items/seeds/signals/logger/time` 与调用方显式传入对象，不读取插件模块全局命名空间。
- `scripts.is_success(match, pre=None, post=None, flow=None)`：参数顺序贴近 dev，失败缺省抛出 Retry。
- `scripts.turn_page(..., success=False)`：只有匹配且确实提交下一页后，`success=True` 才提前结束当前任务；否则继续解析和 pipeline。
- `scripts.inject(flows)`：执行 `Type.define/choice/condition/code` 组合。

```python
from bricks.plugins import scripts

spider.use(
    hooks.AFTER_PIPELINE,
    {
        "func": scripts.turn_page,
        "kwargs": {"match": ["context.seeds['page'] < 10"], "action": "+1"},
        "index": 10,
    },
)
```

match、pre、post、flow 和 action 可以混合可信 Python 字符串与同步 callable。标准命名空间包含
`context/ctx/request/response/items/seeds/signals/logger/time`，也可通过 namespace 追加对象。
脚本使用有界编译缓存，但执行的是任意 Python，不是安全沙箱，只能加载可信业务配置。
callable 动作必须返回 None；普通脚本异常、signals 与取消原样传播。
Air 的普通 Hook 仍支持 async def，可直接编写异步业务 Hook。
`call_later=False` 是默认值：映射 `context.submit(..., affinity="current")`，下一页先持久化并优先携带当前
SlotLease，适合复用代理、Cookie、连接池或 Slot Session。`call_later=True` 映射 affinity="any"，
只放入待领取队列，可由其他 Worker 消费。两种模式都有独立 Work 和 ACK；异常恢复后都不保证原 Slot。
`success=True` 在提交下一页后抛出 Success；默认 False 正常完成当前任务。
重试仍只能发生在规定的下载前后阶段。
完整规则见[Hook 节点与 signals](12-hooks-and-signals.md)。

## 验证

```bash
BRICKS_TEST_REDIS_URL=redis://127.0.0.1:6379/0 \
BRICKS_TEST_MONGO_URL=mongodb://127.0.0.1:27017 \
uv run --locked pytest -q tests/crawler/test_plugins.py tests/crawler/test_proxies.py tests/crawler/test_air.py
```

测试只清理随机前缀和随机数据库。只使用专用测试实例，不指向业务服务。

[返回目录](README.md) · [下一章：代理管理](10-proxies.md)
