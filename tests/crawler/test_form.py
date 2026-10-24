"""配置式爬虫的段推进、派发、断点接管与配置校验。"""

from __future__ import annotations

import json
import threading
from collections import Counter
from urllib.parse import parse_qs, urlsplit

import pytest

from bricks import Response, signals
from bricks.models.options import FORM_STAGE as STAGE_OPTION
from bricks.parsers import Rows
from bricks.queues import MemoryTaskQueue
from bricks.spider import form

BASE = "http://example.test"


class SiteDownloader:
    """按路径返回令牌、分页列表和详情 JSON 的同步下载器。

    Attributes:
        hits: 各 URL 的下载次数。
        headers: 各 URL 最近一次请求携带的 X-Token。
        failures: 详情地址到剩余业务失败次数，失败时返回 code=1。
        lock: 保护并发 Slot 写入记录。
    """

    def __init__(self, failures=None):
        """初始化当前测试拥有的记录。

        Args:
            failures: 详情地址到返回 code=1 的次数。
        """
        self.hits = Counter()
        self.headers = {}
        self.failures = dict(failures or {})
        self.lock = threading.Lock()

    def fetch(self, request):
        """记录请求并返回对应 JSON。

        Args:
            request: 节点复制后的请求。

        Returns:
            200 响应。
        """
        url = request.real_url
        parts = urlsplit(url)
        with self.lock:
            self.hits[url] += 1
            self.headers[parts.path] = request.headers.get("X-Token")
            failing = self.failures.get(parts.path, 0)
            if failing:
                self.failures[parts.path] = failing - 1
        if parts.path == "/token":
            data = {"token": "t1"}
        elif parts.path == "/list":
            page = int(parse_qs(parts.query)["page"][0])
            data = {"next": page < 2, "list": [{"id": page * 10 + i} for i in (0, 1)]}
        else:
            data = {"code": 1 if failing else 0, "title": parts.path.rsplit("/", 1)[1]}
        return Response(
            json.dumps(data).encode(), url=url, status_code=200, request=request
        )


def _keep_token(context):
    """把令牌写入种子，供后续段渲染。

    Args:
        context: 当前上下文。
    """
    context.seeds["token"] = context.response.get("token")


def _next_page(context):
    """存在下一页时提交同段任务。

    Args:
        context: 当前上下文。
    """
    if context.response.get("next"):
        context.submit({**context.seeds, "page": context.seeds["page"] + 1})


def _require_success(context):
    """业务码非 0 时重新下载当前段。

    Args:
        context: 当前上下文。

    Raises:
        signals.Retry: 业务码失败。
    """
    if context.response.get("code") != 0:
        raise signals.Retry


def _store(context):
    """把当前 Items 追加到 Spider 的收集器。

    Args:
        context: 当前上下文。
    """
    context.target.stored.extend(context.items)


class Catalog(form.Spider):
    """令牌、分页列表、详情三段流程。

    Attributes:
        stored: 本测试拥有的存储结果。
    """

    def __init__(self, **kwargs):
        """准备存储收集器。

        Args:
            kwargs: Spider 配置。
        """
        super().__init__(**kwargs)
        self.stored = []

    @property
    def config(self):
        """声明三段流程，详情段对列表 Items 逐条派发。

        Returns:
            流程配置。
        """
        return form.Config(
            init=[form.Init(lambda record: [{"page": 1}])],
            spider=[
                form.Download(url=BASE + "/token"),
                form.Task(_keep_token),
                form.Download(
                    url=BASE + "/list", params={"page": "{page}", "token": "{token}"}
                ),
                form.Parse("json", Rows("list", {"id": "id"})),
                form.Task(_next_page),
                form.Download(
                    url=BASE + "/detail/{id}", headers={"X-Token": "{token}"}, each=True
                ),
                form.Task(_require_success),
                form.Parse(
                    "json",
                    {"title": "title"},
                    layout=form.Layout(
                        default={"id": "{id}"}, rename={"title": "name"}
                    ),
                ),
                form.Pipeline(_store),
            ],
        )


def test_stages_carry_seeds_fan_out_details_and_keep_pages_in_stage():
    """令牌沿用种子进入列表段，翻页留在列表段，详情逐条派发且每个地址只下载一次。"""
    downloader = SiteDownloader(failures={"/detail/11": 2})
    queue = MemoryTaskQueue("catalog")
    spider = Catalog(
        downloader=downloader, task_queue=queue, queue_name="catalog", concurrency=2
    )
    report = spider.run(timeout=5)
    assert report.counts == {"pending": 0, "leased": 0, "acked": 7, "dead": 0}
    assert sorted(spider.stored, key=lambda row: row["id"]) == [
        {"name": str(item), "id": item} for item in (10, 11, 20, 21)
    ]
    assert downloader.hits[BASE + "/list?page=2&token=t1"] == 1
    assert downloader.hits[BASE + "/detail/11"] == 3
    assert all(
        count == 1
        for url, count in downloader.hits.items()
        if url != BASE + "/detail/11"
    )
    assert downloader.headers["/detail/20"] == "t1"


def test_interrupted_stage_resumes_without_losing_persisted_next_stage():
    """已领取未确认的段与已持久化的下一段在租约过期后接管完成，重放按 ID 去重。"""
    downloader = SiteDownloader()
    queue = MemoryTaskQueue("catalog")
    spider = Catalog(
        downloader=downloader,
        task_queue=queue,
        queue_name="catalog",
        options={"spider.queue.lease": 1},
    )
    listing = spider._make_work({"page": 2, "token": "t1"}, options={STAGE_OPTION: 1})
    detail = spider._make_work(
        {"page": 2, "token": "t1", "id": 20}, options={STAGE_OPTION: 2}
    )
    queue.put(listing)
    assert queue.claim("dead-worker", lease_seconds=0.2) is not None
    assert queue.put_claim(detail, "dead-worker", lease_seconds=0.2) is not None
    spider.run("spider", timeout=5)
    assert queue.counts() == {"pending": 0, "leased": 0, "acked": 3, "dead": 0}
    assert sorted(row["id"] for row in spider.stored) == [20, 21]
    assert downloader.hits[BASE + "/detail/20"] == 1
    assert downloader.hits[BASE + "/detail/21"] == 1


def test_request_task_retry_redownloads_and_parses_once():
    """下载后的 Task 抛出 Retry 时重新下载当前段，解析与存储只执行一次。"""
    downloader = SiteDownloader(failures={"/detail/7": 2})
    parsed = []

    class Detail(form.Spider):
        """只有详情段的流程。"""

        config = form.Config(
            spider=[
                form.Download(url=BASE + "/detail/{id}"),
                form.Task(_require_success),
                form.Parse(lambda context: parsed.append(1) or [{"ok": True}]),
            ]
        )

    Detail(downloader=downloader).survey({"id": 7})
    assert downloader.hits[BASE + "/detail/7"] == 3
    assert parsed == [1]


@pytest.mark.parametrize(
    "steps",
    [
        [form.Parse("json", {})],
        [form.Download(url=BASE, each=True)],
        [form.Download(url=BASE), form.Pipeline(print), form.Parse("json", {})],
        [form.Download(url=BASE), form.Parse("json", {}), form.Parse("json", {})],
    ],
)
def test_invalid_step_order_fails_when_opening(steps):
    """首步必须是 Download、首段不能派发、每段至多一个位于 Pipeline 前的 Parse。

    Args:
        steps: 非法步骤序列。
    """

    class Broken(form.Spider):
        """使用非法步骤的流程。"""

        config = form.Config(spider=steps)

    with pytest.raises(ValueError):
        Broken().open()


@pytest.mark.parametrize(
    "arguments",
    [("JSON", {}), ("json",), (print, {})],
)
def test_parse_requires_engine_rules_or_function_without_rules(arguments):
    """引擎名大小写敏感且必须带规则，函数形式不接受规则。

    Args:
        arguments: Parse 构造参数。
    """
    with pytest.raises(ValueError):
        form.Parse(*arguments)


def test_download_ok_and_max_retry_override_spider_defaults():
    """段级 ok 拒绝业务失败并触发重试，段级 max_retry 覆盖 Spider 默认的两次。"""
    downloader = SiteDownloader(failures={"/detail/7": 3})

    class Detail(form.Spider):
        """详情段自带成功规则与重试预算。"""

        config = form.Config(
            spider=[
                form.Download(
                    url=BASE + "/detail/{id}",
                    ok=lambda context: context.response.get("code") == 0,
                    max_retry=3,
                ),
                form.Parse("json", {"title": "title"}),
            ]
        )

    rows = Detail(downloader=downloader).survey({"id": 7}, extract=["items"])
    assert rows[0]["items"].to_list() == [{"title": "7"}]
    assert downloader.hits[BASE + "/detail/7"] == 4
