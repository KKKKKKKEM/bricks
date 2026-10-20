"""迁移领域插件的真实存储、恢复位置和 Air 交付边界。"""

import copy
import csv
import json
import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from uuid import uuid4

import pytest
from interlace.engine.errors import ExecutionError

from bricks import Items, Request, Response, hooks
from bricks.plugins import make_seeds, on_request, scripts, storage
from bricks.spider.air import Spider


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
    assert record == {next(iter(record)): 4}
    resumed = {"position": 2}
    unbounded = make_seeds.by_range(
        0, None, 3, batch_size=2, record=resumed, checkpoint="position"
    )
    assert next(unbounded) == [{"id": 6}, {"id": 9}]
    unbounded.close()
    assert resumed == {"position": 4}
    reverse = {}
    descending = make_seeds.by_range(
        0, 6, 1, batch_size=2, reverse=True, record=reverse
    )
    assert next(descending) == [{"id": 5}, {"id": 4}]
    descending.close()
    assert list(
        make_seeds.by_range(0, 6, 1, batch_size=2, reverse=True, record=reverse)
    ) == [
        [{"id": 3}, {"id": 2}],
        [{"id": 1}, {"id": 0}],
    ]
    assert list(reverse.values()) == [6]
    assert list(make_seeds.by_range(-3, 4, 2, reverse=True)) == [
        [{"id": 3}, {"id": 1}, {"id": -1}, {"id": -3}]
    ]
    assert list(make_seeds.by_range(0, 4, 1, 2, 1, {}, "page", False)) == [
        [{"page": 1}, {"page": 2}],
        [{"page": 3}],
    ]
    with pytest.raises(ValueError):
        make_seeds.by_range(step=0)


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
        "query": "SELECT * FROM <TABLE> WHERE id >= ? ORDER BY id",
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


def test_csv_existing_path_with_glob_metacharacters_is_literal(tmp_path):
    """现存 CSV 路径含 glob 元字符时仍按字面文件名读取。"""
    path = tmp_path / "data[1].csv"
    path.write_text("id\n1\n", encoding="utf-8")
    assert list(make_seeds.by_csv(path)) == [[{"id": "1"}]]


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
    items = Items([{"id": 1, "name": "first"}])
    original = copy.deepcopy(list(items))
    storage.to_sqlite('items"quoted', path, items, row_keys=["id"])
    assert list(items) == original
    items = Items([{"id": 1, "name": "updated"}])
    storage.to_sqlite('items"quoted', path, items, row_keys=["id"])
    connection = sqlite3.connect(path)
    try:
        assert connection.execute('SELECT * FROM "items""quoted"').fetchall() == [
            (1, "updated")
        ]
        connection.execute(
            "CREATE TABLE checked (id INTEGER PRIMARY KEY, value INTEGER CHECK(value > 0))"
        )
        connection.commit()
        items = Items([{"id": 1, "value": 1}, {"id": 2, "value": -1}])
        with pytest.raises(sqlite3.IntegrityError):
            storage.to_sqlite("checked", path, items, row_keys=["id"])
        assert connection.execute("SELECT * FROM checked").fetchall() == []
    finally:
        connection.close()


def test_sqlite_shared_connection_lock_serializes_slot_writes():
    """多个 Slot 共享 SQLite Connection 时显式 lock 串行写入。"""
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    lock = Lock()
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            list(
                executor.map(
                    lambda value: storage.to_sqlite(
                        "locked", connection, [{"id": value}], lock=lock
                    ),
                    range(20),
                )
            )
        assert connection.execute("SELECT COUNT(*) FROM locked").fetchone() == (20,)
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
        storage.to_csv(path, [{"id": index}], lock=lock)

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(write, range(20)))
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert sorted(int(row["id"]) for row in rows) == list(range(20))
    before = path.read_bytes()
    with pytest.raises(ValueError, match="header"):
        storage.to_csv(path, [{"name": "different"}])
    assert path.read_bytes() == before


def test_csv_shared_writer_connection_writes_under_lock():
    """调用方共享的 DictWriter 与 lock 同时传入时，写入在锁内执行。"""
    lock, held = Lock(), []

    class Writer:
        """记录写入时锁状态的 DictWriter 替身。"""

        def writerows(self, rows):
            """登记当前是否持锁。

            Args:
                rows: 待写入记录。
            """
            held.append(lock.locked())

    storage.to_csv("shared.csv", [{"id": 1}], Writer(), lock=lock)
    assert held == [True]
    assert not lock.locked()


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
            prefix,
            redis_client,
            rows,
            key_type=key_type,
            row_keys=["id"],
            ttl=60,
        )
        path = prefix + "*" if key_type == "string" else prefix
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
                prefix,
                redis_client,
                rows,
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
    storage.to_mongo("items", mongo_collection, rows, row_keys=["id"])
    storage.to_mongo(
        "items",
        mongo_collection,
        [{"id": 1, "name": "updated"}],
        row_keys=["id"],
    )
    assert all("_id" not in row for row in rows)
    record = {}
    source = make_seeds.by_mongo(
        "items", mongo_collection, sort=[("id", 1)], batch_size=1, record=record
    )
    assert next(source) == [{"id": 1, "name": "updated"}]
    source.close()
    assert list(
        make_seeds.by_mongo("items", mongo_collection, sort=[("id", 1)], record=record)
    ) == [[{"id": 2, "name": "second"}]]
    with pytest.raises(TypeError):
        list(make_seeds.by_mongo("items", mongo_collection, projection={}))
    converted = list(
        make_seeds.by_mongo(
            "items",
            mongo_collection,
            projection={},
            transform=lambda row: {**row, "_id": str(row["_id"])},
        )
    )
    assert all(type(row["_id"]) is str for row in converted[0])
    from pymongo.write_concern import WriteConcern

    unacknowledged = mongo_collection.with_options(write_concern=WriteConcern(w=0))
    with pytest.raises(ValueError, match="acknowledged"):
        storage.to_mongo("items", unacknowledged, [{"id": 3}])
    assert mongo_collection.count_documents({}) == 2


def test_real_mongo_row_key_operator_value_is_matched_literally(mongo_collection):
    """row_keys 中形似查询操作符的值仍按业务数据精确匹配。

    Args:
        mongo_collection: 独占测试集合。
    """
    mongo_collection.insert_one({"id": 1, "name": "existing"})
    storage.to_mongo(
        "items",
        mongo_collection,
        [{"id": {"$ne": None}, "name": "operator-shaped value"}],
        row_keys=["id"],
    )

    assert mongo_collection.count_documents({}) == 2
    assert (
        mongo_collection.find_one({"id": {"$eq": {"$ne": None}}})["name"]
        == "operator-shaped value"
    )
    assert mongo_collection.find_one({"id": 1})["name"] == "existing"
    with pytest.raises(ValueError, match="plain field names"):
        storage.to_mongo(
            "items", mongo_collection, [{"id.value": 3}], row_keys=["id.value"]
        )


def test_show_response_redacts_credentials_and_request_body_by_default():
    """响应诊断保留状态信息但不暴露认证、查询值或请求体。"""
    messages = []

    class LoggingSpider(Spider):
        """使用带敏感字段的请求验证消费者实际收到的日志。"""

        def __init__(self):
            """装配返回固定响应的下载器。"""
            super().__init__(
                downloader=type(
                    "Downloader",
                    (),
                    {
                        "fetch": lambda _, request: Response(
                            b"{}", status_code=200, request=request
                        )
                    },
                )()
            )

        def make_seeds(self, context, **kwargs):
            """提供单条请求种子。

            Args:
                context: 初始化上下文。
                kwargs: 初始化记录。

            Returns:
                含敏感 URL 的种子。
            """
            return {
                "url": "https://user:known-url-credential@example.test/path?token=url-secret"
            }

        def make_request(self, context):
            """构造包含认证信息的非 GET 请求。

            Args:
                context: 消费上下文。

            Returns:
                待下载请求。
            """
            return Request(
                context.seeds["url"],
                method="POST",
                headers={
                    "Authorization": "header-secret",
                    "Cookie": "cookie-secret",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "X-Auth-Token": "token-header-secret",
                    "Referer": "https://referrer.test/page?token=referer-secret",
                    "X-Password": "password-header-secret",
                },
                body="field=known-body-credential",
                proxy="http://proxy-user:known-proxy-credential@proxy.test:8080",
            )

        def parse(self, context):
            """返回空解析结果。

            Args:
                context: 当前响应上下文。

            Returns:
                空记录列表。
            """
            return []

        def install(self):
            """收集两种格式的消费者可见诊断文本。"""
            self.use(
                hooks.AFTER_REQUEST,
                {
                    "func": on_request.After.show_response,
                    "kwargs": {"handle": messages.append},
                },
                {
                    "func": on_request.After.show_response,
                    "kwargs": {"handle": messages.append, "fmt": 1},
                    "index": 1,
                },
            )

    spider = LoggingSpider()
    assert spider.run().counts["acked"] == 1
    assert len(messages) == 2
    output = "\n".join(messages)
    for secret in (
        "user",
        "known-url-credential",
        "url-secret",
        "proxy-user",
        "known-proxy-credential",
        "header-secret",
        "cookie-secret",
        "token-header-secret",
        "referer-secret",
        "password-header-secret",
        "application/x-www-form-urlencoded",
        "known-body-credential",
    ):
        assert secret not in output
    assert "status=200" in output
    assert "POST" in output
    assert "body: [REDACTED]" in output
    assert "Authorization" in output and "Cookie" in output
    assert "Referer" in output and "X-Password" in output
    assert "example.test" in output and "proxy.test:8080" in output


def test_dynamic_plugins_keep_dev_parameters_and_callable_flexibility():
    """直接 use 的动态脚本保留旧参数，并可与 callable 和当前 Context 混用。"""

    class DynamicSpider(Spider):
        """记录默认和显式动态插件产生的动作。"""

        def __init__(self):
            """装配单次成功响应和动作记录。"""
            super().__init__(
                downloader=type(
                    "Downloader",
                    (),
                    {
                        "fetch": lambda _, request: Response(
                            b"{}", status_code=200, request=request
                        )
                    },
                )(),
                options={
                    "crawler.response.scripts": {
                        "response.status_code == 200": (
                            "context.target.observed.append('options')"
                        )
                    }
                },
            )
            self.observed = []

        def make_seeds(self, context, **kwargs):
            """提供一条动态插件测试种子。

            Args:
                context: 初始化 Context。
                kwargs: 初始化记录。

            Returns:
                单条种子。
            """
            return {"url": "https://example.com", "page": 1}

        def make_request(self, context):
            """构造含旧式 UA 预设的请求。

            Args:
                context: 消费 Context。

            Returns:
                当前请求。
            """
            return Request(context.seeds["url"], headers={"User-Agent": "@chrome"})

        def parse(self, context):
            """确认默认 UA 插件已经展开预设。

            Args:
                context: 当前响应 Context。

            Returns:
                空记录集合。
            """
            assert not context.request.headers["User-Agent"].startswith("@")
            return []

        def inspect_namespace(self, context):
            """断言脚本范围仅含公开名称和调用方追加对象。

            Args:
                context: 当前响应上下文。
            """
            namespace = scripts.build_namespace(context, {"owner": self})
            assert set(namespace) == {
                "context",
                "ctx",
                "request",
                "response",
                "items",
                "seeds",
                "signals",
                "logger",
                "time",
                "owner",
            }
            assert namespace["owner"] is self
            self.observed.append("namespace")

        def install(self):
            """通过 use 组合旧式字符串参数和 callable。"""
            super().install()
            self.use(
                hooks.AFTER_REQUEST,
                hooks.Task(self.inspect_namespace, index=0),
                {
                    "func": on_request.After.conditional_scripts,
                    "kwargs": {
                        "rules": {
                            lambda: True: lambda: self.observed.append("callable")
                        }
                    },
                    "index": 1,
                },
                {
                    "func": scripts.is_success,
                    "kwargs": {
                        "match": ["response.status_code == expected"],
                        "pre": "context.target.observed.append('pre')",
                        "post": "context.target.observed.append('post')",
                        "namespace": {"expected": 200},
                    },
                    "index": 2,
                },
            )
            self.use(
                hooks.AFTER_PIPELINE,
                {
                    "func": scripts.inject,
                    "kwargs": {
                        "flows": [
                            (
                                scripts.Type.condition,
                                {
                                    "context.seeds['page'] == 1": (
                                        "context.target.observed.append('inject')"
                                    )
                                },
                            )
                        ]
                    },
                },
            )

    spider = DynamicSpider()
    assert spider.run().counts["acked"] == 1
    assert spider.observed == [
        "options",
        "namespace",
        "callable",
        "pre",
        "post",
        "inject",
    ]


@pytest.mark.parametrize("call_later", [False, True])
def test_air_plugins_page_store_then_ack(tmp_path, call_later):
    """本地翻页共用交付，持久翻页逐页建 Work，存储均在 ACK 前提交。

    Args:
        tmp_path: 独占数据库目录。
        call_later: 是否通过持久队列投递下一页。
    """
    path = tmp_path / "air.sqlite3"
    checked = []
    after_turn = []

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
                    "kwargs": {"path": "items", "conn": path, "row_keys": ["id"]},
                    "index": 0,
                },
                {"func": self.check_commit, "index": 1},
                {
                    "func": scripts.turn_page,
                    "kwargs": {
                        "match": ["context.seeds['page'] < 3"],
                        "action": "+1",
                        "call_later": call_later,
                        "success": True,
                    },
                    "index": 2,
                },
                {"func": self.after_turn_page, "index": 3},
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
            checked.append(context.seeds["page"])

        def after_turn_page(self, context):
            """记录翻页判断未命中时仍执行的后置 Hook。

            Args:
                context: 当前 pipeline 结果。
            """
            if context.seeds["page"] == 3:
                after_turn.append(context.seeds["page"])

    spider = PluginSpider(downloader=LocalDownloader())
    assert spider.run().counts["acked"] == 3
    assert checked == [1, 2, 3]
    assert after_turn == [3]


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
        {"func": storage.to_sqlite, "kwargs": {"path": "items", "conn": tmp_path}},
    )
    with pytest.raises(ExecutionError):
        spider.run()
    assert spider.report().counts == {"pending": 0, "leased": 0, "acked": 0, "dead": 1}


def test_mongo_accepts_collection_database_and_client(mongo_collection):
    """Mongo 三种原生句柄均正确选择 Collection，并保留批量 ordered 参数。"""
    database = mongo_collection.database
    client = database.client
    rows = [{"id": 1, "value": "one"}]
    storage.to_mongo("items", mongo_collection, rows, row_keys=["id"], ordered=False)
    storage.to_mongo("items", database, [{"id": 2, "value": "two"}], row_keys=["id"])
    storage.to_mongo(
        "items",
        client,
        [{"id": 3, "value": "three"}],
        row_keys=["id"],
        database=database.name,
    )
    assert mongo_collection.count_documents({}) == 3
    assert [
        row["id"]
        for batch in make_seeds.by_mongo("items", database, sort=[("id", 1)])
        for row in batch
    ] == [1, 2, 3]
    assert [
        row["id"]
        for batch in make_seeds.by_mongo(
            "items", client, database=database.name, sort=[("id", 1)]
        )
        for row in batch
    ] == [1, 2, 3]
    with pytest.raises(ValueError, match="database is required"):
        list(make_seeds.by_mongo("items", client))
    with pytest.raises(ValueError, match="database is required"):
        storage.to_mongo("items", client, rows, row_keys=["id"])
    for field in ("nested.value", "$operator"):
        with pytest.raises(ValueError, match="Mongo upsert fields"):
            storage.to_mongo(
                "items",
                mongo_collection,
                [{"id": 4, field: "invalid"}],
                row_keys=["id"],
            )


@pytest.mark.parametrize(
    "status,attempts,accepted", [(200, 1, True), (404, 1, False), (429, 3, False)]
)
def test_bypass_unmatched_mapping_uses_status_defaults(status, attempts, accepted):
    """bypass 映射未命中时沿用默认接受、重试和失败规则。"""
    calls = []

    class Downloader:
        """按参数状态码返回固定响应。"""

        def fetch(self, request):
            """记录调用并构造响应。

            Args:
                request: 当前请求。

            Returns:
                指定状态码的响应。
            """
            calls.append(request)
            return Response(b"{}", status_code=status, request=request)

    class BypassSpider(Spider):
        """只装配无命中映射的 bypass 插件。"""

        def install(self):
            """注册用于验证默认回退的响应插件。"""
            self.use(
                hooks.AFTER_REQUEST,
                {
                    "func": on_request.After.bypass,
                    "kwargs": {"ok": {"response.status_code == -999": "pass"}},
                },
            )

        def parse(self, context):
            """返回空记录。

            Args:
                context: 当前响应上下文。

            Returns:
                空记录列表。
            """
            return []

    spider = BypassSpider(downloader=Downloader())
    spider.submit({"url": "https://example.test"})
    with spider:
        if accepted:
            assert spider.run_spider(timeout=3).counts["acked"] == 1
        else:
            with pytest.raises(ExecutionError):
                spider.run_spider(timeout=3)
    assert len(calls) == attempts


def test_safe_url_redacts_bare_proxy_credentials_and_bare_query():
    """无 scheme 代理认证和无等号查询值均不会出现在诊断 URL。"""
    from bricks.plugins.on_request import _safe_url

    assert "secret" not in _safe_url("user:secret@proxy.test:8080")
    assert "secret" not in _safe_url("https://example.test/path?secret")
    # 解析失败时返回固定占位，既不抛错也不回显含凭据的原值。
    assert _safe_url("http://user:p\u2100secret@proxy.test:80") == "[INVALID URL]"
    assert _safe_url("http://[bad/x?token=secret") == "[INVALID URL]"


@pytest.mark.parametrize(
    "module",
    [
        "bricks.plugins.on_request",
        "bricks.plugins.scripts",
        "bricks.spider.context",
        "bricks.spider.air",
    ],
)
def test_plugin_and_spider_modules_import_in_fresh_interpreter(module):
    """插件和 Spider 模块可在全新解释器中独立导入。"""
    result = subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
