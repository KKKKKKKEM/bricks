"""迁移领域插件的真实存储、恢复位置和 Air 交付边界。"""

import copy
import csv
import json
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from types import SimpleNamespace
from uuid import uuid4

import pytest

from bricks import Items, Request, Response, hooks
from bricks.plugins import make_seeds, on_request, scripts, storage
from bricks.spider.air import Spider
from interlace.engine.errors import ExecutionError


@pytest.fixture
def redis_client():
    """提供专用 Redis 客户端，具体测试只清理自己的随机键。

    Yields:
        同步 Redis 客户端。
    """
    url = os.environ.get("BRICKS_TEST_REDIS_URL")
    if not url:
        pytest.skip("set BRICKS_TEST_REDIS_URL for Redis plugin contracts")
    from redis import Redis

    with Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2) as client:
        client.ping()
        yield client


@pytest.fixture
def mongo_collection():
    """提供专用随机 Mongo 数据库中的集合。

    Yields:
        测试独占集合；退出只删除当前随机数据库。
    """
    url = os.environ.get("BRICKS_TEST_MONGO_URL")
    if not url:
        pytest.skip("set BRICKS_TEST_MONGO_URL for Mongo plugin contracts")
    from pymongo import MongoClient

    name = "bricks_plugins_" + uuid4().hex
    with MongoClient(url, serverSelectionTimeoutMS=2000) as client:
        client.admin.command("ping")
        try:
            yield client[name]["items"]
        finally:
            client.drop_database(name)


def test_range_checkpoint_keeps_empty_record_and_resumes_exactly():
    """空记录原对象得到进度，恢复不重复已投放前缀，反向遵循 range。"""
    record = {}
    source = make_seeds.by_range(0, 7, 2, batch_size=2, record=record)
    assert next(source) == [{"id": 0}, {"id": 2}]
    source.close()
    assert list(record.values()) == [2]
    assert list(make_seeds.by_range(0, 7, 2, batch_size=2, record=record)) == [
        [{"id": 4}, {"id": 6}]
    ]
    assert list(make_seeds.by_range(-3, 4, 2, reverse=True)) == [
        [{"id": 3}, {"id": 1}, {"id": -1}, {"id": -3}]
    ]
    with pytest.raises(ValueError):
        list(make_seeds.by_range(step=0))


def test_csv_query_multiple_files_and_sqlite_query_boundaries(tmp_path):
    """CSV 查询按文件稳定恢复，SQLite 原有 LIMIT/OFFSET 不被改写。

    Args:
        tmp_path: 测试独占文件目录。
    """
    for index in (0, 1):
        (tmp_path / f"{index}.csv").write_text(
            "id,name\n1,one\n2,two\n3,three\n", encoding="utf-8"
        )
    record = {}
    options = {
        "query": "SELECT * FROM data WHERE id >= ? ORDER BY id",
        "parameters": ["2"],
        "record": record,
        "batch_size": 1,
    }
    source = make_seeds.by_csv(tmp_path / "*.csv", **options)
    assert next(source) == [{"id": "2", "name": "two"}]
    source.close()
    assert [
        row[0]["id"] for row in make_seeds.by_csv(tmp_path / "*.csv", **options)
    ] == ["3", "2", "3"]
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute("CREATE TABLE data (id INTEGER)")
        connection.executemany("INSERT INTO data VALUES (?)", [(i,) for i in range(6)])
        assert list(
            make_seeds.by_sqlite(
                "SELECT id FROM data ORDER BY id LIMIT 3 OFFSET 1", connection, skip=1
            )
        ) == [[{"id": 2}, {"id": 3}]]
        assert connection.execute("SELECT COUNT(*) FROM data").fetchone() == (6,)
    finally:
        connection.close()


def test_source_errors_do_not_advance_checkpoint(tmp_path):
    """整批校验失败不发布新断点，坏 CSV 不静默丢失列。

    Args:
        tmp_path: 独占临时目录。
    """
    path = tmp_path / "invalid.csv"
    path.write_text("id\n1,extra\n", encoding="utf-8")
    record = {}
    with pytest.raises(ValueError, match="width"):
        list(make_seeds.by_csv(path, record=record))
    assert record == {}


def test_csv_query_generator_can_move_between_serial_workers(tmp_path):
    """Air 的串行节点跨线程推进及关闭源时，临时 SQLite 仍可正常释放。

    Args:
        tmp_path: 独占 CSV 目录。
    """
    path = tmp_path / "rows.csv"
    path.write_text("id\n1\n2\n", encoding="utf-8")
    source = make_seeds.by_csv(
        path, query="SELECT * FROM data ORDER BY id", batch_size=1
    )
    with (
        ThreadPoolExecutor(max_workers=1) as first,
        ThreadPoolExecutor(max_workers=1) as second,
    ):
        assert first.submit(next, source).result() == [{"id": "1"}]
        assert second.submit(next, source).result() == [{"id": "2"}]
    source.close()


def test_sqlite_upsert_and_batch_rollback_preserve_input(tmp_path):
    """幂等写入不修改 Items，后续行失败会回滚整批而不影响旧数据。

    Args:
        tmp_path: 独占数据库目录。
    """
    path = tmp_path / "items.sqlite3"
    context = SimpleNamespace(items=Items([{"id": 1, "name": "first"}]))
    original = copy.deepcopy(list(context.items))
    storage.to_sqlite(context, path=path, table='items"quoted', row_keys=["id"])
    assert list(context.items) == original
    context.items = Items([{"id": 1, "name": "updated"}])
    storage.to_sqlite(context, path=path, table='items"quoted', row_keys=["id"])
    connection = sqlite3.connect(path)
    try:
        assert connection.execute('SELECT * FROM "items""quoted"').fetchall() == [
            (1, "updated")
        ]
        connection.execute(
            "CREATE TABLE checked (id INTEGER PRIMARY KEY, value INTEGER CHECK(value > 0))"
        )
        connection.commit()
        context.items = Items([{"id": 1, "value": 1}, {"id": 2, "value": -1}])
        with pytest.raises(sqlite3.IntegrityError):
            storage.to_sqlite(context, path=path, table="checked", row_keys=["id"])
        assert connection.execute("SELECT * FROM checked").fetchall() == []
    finally:
        connection.close()


def test_csv_shared_lock_and_header_validation(tmp_path):
    """共同持锁的并发写入只产生一个表头，字段变更明确失败。

    Args:
        tmp_path: 独占 CSV 目录。
    """
    path, lock = tmp_path / "items.csv", Lock()

    def write(index):
        """写入一条独立记录。

        Args:
            index: 当前记录编号。
        """
        storage.to_csv(SimpleNamespace(items=[{"id": index}]), path=path, lock=lock)

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(write, range(20)))
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert sorted(int(row["id"]) for row in rows) == list(range(20))
    before = path.read_bytes()
    with pytest.raises(ValueError, match="header"):
        storage.to_csv(SimpleNamespace(items=[{"name": "different"}]), path=path)
    assert path.read_bytes() == before


@pytest.mark.parametrize("key_type", ["set", "list", "string"])
def test_real_redis_storage_and_seed_roundtrip(redis_client, key_type):
    """真实 Redis 写入后可无损读回，保留大整数、空列表且不修改输入。

    Args:
        redis_client: 专用测试客户端。
        key_type: 被验证的 Redis 类型。
    """
    prefix = "bricks-plugin:" + uuid4().hex
    rows = [{"id": 2**60 + 1, "empty": []}, {"id": 2, "empty": []}]
    original = copy.deepcopy(rows)
    try:
        storage.to_redis(
            SimpleNamespace(items=rows),
            path=prefix,
            conn=redis_client,
            key_type=key_type,
            row_keys=["id"],
            ttl=60,
        )
        path = prefix + ":*" if key_type == "string" else prefix
        loaded = [
            row
            for batch in make_seeds.by_redis(
                path, redis_client, key_type=key_type, batch_size=1
            )
            for row in batch
        ]
        assert sorted(loaded, key=lambda row: row["id"]) == sorted(
            rows, key=lambda row: row["id"]
        )
        assert rows == original
        assert all(
            redis_client.ttl(key) > 0
            for key in redis_client.scan_iter(match=prefix + "*")
        )
        if key_type == "string":
            storage.to_redis(
                SimpleNamespace(items=rows),
                path=prefix,
                conn=redis_client,
                key_type="string",
                row_keys=["id"],
            )
            assert len(list(redis_client.scan_iter(match=path))) == 2
    finally:
        keys = list(redis_client.scan_iter(match=prefix + "*"))
        if keys:
            redis_client.delete(*keys)


def test_real_mongo_upsert_projection_and_explicit_conversion(mongo_collection):
    """原生 Mongo upsert 保留输入，种子源显式处理 ObjectId。

    Args:
        mongo_collection: 独占测试集合。
    """
    mongo_collection.create_index("id", unique=True)
    rows = [{"id": 1, "name": "first"}, {"id": 2, "name": "second"}]
    storage.to_mongo(
        SimpleNamespace(items=rows), conn=mongo_collection, row_keys=["id"]
    )
    storage.to_mongo(
        SimpleNamespace(items=[{"id": 1, "name": "updated"}]),
        conn=mongo_collection,
        row_keys=["id"],
    )
    assert all("_id" not in row for row in rows)
    record = {}
    source = make_seeds.by_mongo(
        mongo_collection, sort=[("id", 1)], batch_size=1, record=record
    )
    assert next(source) == [{"id": 1, "name": "updated"}]
    source.close()
    assert list(
        make_seeds.by_mongo(mongo_collection, sort=[("id", 1)], record=record)
    ) == [[{"id": 2, "name": "second"}]]
    with pytest.raises(TypeError):
        list(make_seeds.by_mongo(mongo_collection, projection={}))
    converted = list(
        make_seeds.by_mongo(
            mongo_collection,
            projection={},
            transform=lambda row: {**row, "_id": str(row["_id"])},
        )
    )
    assert all(type(row["_id"]) is str for row in converted[0])
    from pymongo.write_concern import WriteConcern

    unacknowledged = mongo_collection.with_options(write_concern=WriteConcern(w=0))
    with pytest.raises(ValueError, match="acknowledged"):
        storage.to_mongo(SimpleNamespace(items=[{"id": 3}]), conn=unacknowledged)
    assert mongo_collection.count_documents({}) == 2


def test_plugins_are_composable_and_reject_async_or_nonboolean_conditions():
    """条件动作按序执行，错误条件和异步回调不被解释为业务成功。"""
    context = SimpleNamespace(request=Request("https://example.com"))
    observed = []
    on_request.After.conditional_scripts(
        context,
        rules=[
            (lambda ctx: True, lambda ctx: observed.append(1)),
            (lambda ctx: True, lambda ctx: observed.append(2)),
        ],
        first=True,
    )
    assert observed == [1]
    with pytest.raises(TypeError, match="bool"):
        scripts.is_success(context, match=lambda ctx: 1)

    async def invalid(ctx):
        """提供被同步插件明确拒绝的异步条件。

        Args:
            ctx: 当前上下文。

        Returns:
            异步布尔结果。
        """
        return True

    with pytest.raises(TypeError, match="synchronous"):
        scripts.is_success(context, match=invalid)
    on_request.Before.fake_ua(context, factory=lambda: "test-UA")
    on_request.Before.fake_ua(context, factory=lambda: "different-UA")
    assert context.request.headers["User-Agent"] == "test-UA"
    scripts.inject(context, [lambda ctx: observed.append(3)])
    assert observed == [1, 3]


def test_air_plugins_page_store_then_ack(tmp_path):
    """种子插件、分页动作和 SQLite 插件组成完整 Air，存储提交后才 ACK。

    Args:
        tmp_path: 独占数据库目录。
    """
    path = tmp_path / "air.sqlite3"
    checked = []

    class LocalDownloader:
        """返回当前页的确定性响应。"""

        def fetch(self, request):
            """构造一页数据。

            Args:
                request: 当前独立请求。

            Returns:
                JSON 响应。
            """
            return Response(
                json.dumps({"id": int(request.params["page"])}).encode(),
                status_code=200,
                request=request,
            )

    class PluginSpider(Spider):
        """使用迁移插件完成分页存储。"""

        def make_seeds(self, context, **kwargs):
            """使用范围插件提供第一页。

            Args:
                context: 初始化上下文。
                kwargs: 含原始恢复记录。

            Returns:
                种子批次迭代器。
            """
            return make_seeds.by_range(1, 2, key="page", record=kwargs["record"])

        def make_request(self, context):
            """按当前种子构造请求。

            Args:
                context: 消费上下文。

            Returns:
                带页码的请求。
            """
            return Request("https://example.com", params=context.seeds)

        def parse(self, context):
            """返回当前响应记录。

            Args:
                context: 当前响应上下文。

            Returns:
                一条记录的列表。
            """
            return [context.response.json()]

        def install(self):
            """装配存储及分页插件，按顺序在 pipeline 后执行。"""
            super().install()
            self.use(
                hooks.AFTER_PIPELINE,
                {
                    "func": storage.to_sqlite,
                    "kwargs": {"path": path, "table": "items", "row_keys": ["id"]},
                    "index": 0,
                },
                {"func": self.check_commit, "index": 1},
                {
                    "func": scripts.turn_page,
                    "kwargs": {"match": lambda ctx: ctx.seeds["page"] < 3},
                    "index": 2,
                },
            )

        def check_commit(self, context):
            """交付确认前验证数据库事务已提交。

            Args:
                context: 存储后的上下文。
            """
            connection = sqlite3.connect(path)
            try:
                assert connection.execute(
                    "SELECT COUNT(*) FROM items WHERE id = ?", (context.seeds["page"],)
                ).fetchone() == (1,)
            finally:
                connection.close()
            assert context.task_queue.counts()["leased"] == 1
            checked.append(context.seeds["page"])

    spider = PluginSpider(downloader=LocalDownloader())
    assert spider.run().counts["acked"] == 3
    assert checked == [1, 2, 3]


def test_storage_hook_failure_after_pipeline_does_not_ack(tmp_path, server):
    """pipeline 正常返回后，存储 Hook 失败仍拒绝交付。

    Args:
        tmp_path: 故意作为数据库文件传入的目录。
        server: 本地 HTTP 站点。
    """
    base, _, _ = server

    class FailureSpider(Spider):
        """让存储插件报告真实 SQLite 打开错误。"""

        def make_seeds(self, context, **kwargs):
            """提供一项本地种子。

            Args:
                context: 初始化上下文。
                kwargs: 恢复记录。

            Returns:
                唯一种子。
            """
            return {"url": base + "/echo"}

        def parse(self, context):
            """返回可写入的标量记录。

            Args:
                context: 当前响应。

            Returns:
                一条业务记录。
            """
            return [{"id": 1}]

    spider = FailureSpider()
    spider.use(
        hooks.AFTER_PIPELINE,
        {"func": storage.to_sqlite, "kwargs": {"path": tmp_path, "table": "items"}},
    )
    with pytest.raises(ExecutionError):
        spider.run()
    assert spider.report().counts == {"pending": 0, "leased": 0, "acked": 0, "dead": 1}
