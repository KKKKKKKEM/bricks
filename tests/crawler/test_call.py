"""同步调用图、GraphService 服务入口与爬虫指标插件的契约测试。"""

from __future__ import annotations

import pytest
from interlace.metrics import Metrics
from interlace.service import GraphService
from starlette.testclient import TestClient

from bricks import Items, Request
from bricks.downloaders.curl_cffi import CurlCffiDownloader
from bricks.plugins.metrics import crawler_metrics
from bricks.spider import Spider


class Echo(Spider):
    """按种子路径请求本地服务，可在解析时派生子种子。

    Attributes:
        base: 本地服务地址。
        stored: item_pipeline 收到的记录，同步调用不应写入。
    """

    def __init__(self, base: str, **kwargs):
        """保存服务地址并使用不读取环境代理的会话下载器。

        Args:
            base: 本地服务地址。
            kwargs: Spider 配置。
        """
        kwargs.setdefault("downloader", CurlCffiDownloader(trust_env=False))
        super().__init__(**kwargs)
        self.base = base
        self.stored: list[dict] = []

    def make_seeds(self, context, **kwargs):
        """同步调用测试不生产初始种子。

        Args:
            context: 初始化上下文。
            kwargs: 恢复记录。

        Returns:
            空列表。
        """
        return []

    def make_request(self, context):
        """请求种子指定的路径。

        Args:
            context: 消费上下文。

        Returns:
            当前请求。
        """
        return Request(self.base + context.seeds["path"])

    def parse(self, context):
        """按种子要求派生子种子，并返回服务回显。

        Args:
            context: 已下载的上下文。

        Returns:
            回显记录。
        """
        affinity = context.seeds.get("submit")
        if affinity is not None:
            context.submit({"path": "/echo"}, affinity=affinity)
        echo = context.response.json()
        return Items([{"path": echo["path"], "cookie": echo["cookie"]}])

    def item_pipeline(self, context):
        """记录存储调用。

        Args:
            context: 含 Items 的上下文。
        """
        self.stored.extend(context.items)


def test_call_reuses_slot_session_and_rejects_current_submissions(server):
    """同步调用在 Slot 会话内执行、跳过默认存储，current 派生被拒绝而 any 持久化。

    Args:
        server: 本地 HTTP 服务。
    """
    base, _, _ = server
    with Echo(base) as spider:
        spider.call({"path": "/login/abc"})
        result = spider.call({"path": "/echo"})
        assert result["seeds"] == {"path": "/echo"}
        assert result["response"]["status_code"] == 200
        assert result["items"] == [{"path": "/echo", "cookie": "account=abc"}]
        assert (result["attempts"], result["retries"]) == (1, 0)
        assert spider.stored == []

        spider.call({"path": "/echo", "submit": "any"})
        assert spider.task_queue.counts()["pending"] == 1
        with pytest.raises(Exception, match="affinity='current' is unavailable"):
            spider.call({"path": "/echo", "submit": "current"})
        assert spider.task_queue.counts()["pending"] == 1

        with pytest.raises(Exception, match="retries exhausted"):
            spider.call({"path": "/error"}, options={"crawler.retry.max": 0})


def test_serve_allows_only_listed_options_and_exposes_metrics(server, monkeypatch):
    """服务入口经调用图返回结果，拒绝未列入白名单的可执行配置，并以令牌保护 /metrics。

    Args:
        server: 本地 HTTP 服务。
        monkeypatch: 用测试客户端替代阻塞的 uvicorn 入口。
    """
    base, _, _ = server
    responses: dict[str, object] = {}

    def exercise(self, *, host, port, token, allowed_hosts, metrics):
        """在服务关闭前以 ASGI 客户端发出请求。

        Args:
            self: 被服务入口创建的 GraphService。
            host: 监听地址。
            port: 监听端口。
            token: Bearer 令牌。
            allowed_hosts: 允许的 Host 头。
            metrics: 指标聚合器。
        """
        app = self.create_app(
            token=token, allowed_hosts=["testserver"], metrics=metrics
        )
        auth = {"Authorization": f"Bearer {token}"}
        with TestClient(app) as client:
            body = {"graph": "echo.call", "inputs": {"seeds": {"path": "/echo"}}}
            responses["call"] = client.post("/api/call", json=body, headers=auth)
            responses["unsafe"] = client.post(
                "/api/call",
                json={**body, "options": {"crawler.response.ok": "True"}},
                headers=auth,
            )
            responses["anonymous"] = client.get("/metrics")
            responses["metrics"] = client.get("/metrics", headers=auth)

    monkeypatch.setattr(GraphService, "serve", exercise)
    metrics = Metrics()
    Echo(base, queue_name="echo", plugins=[metrics.plugin, crawler_metrics]).serve(
        token="secret", metrics=metrics
    )
    call = responses["call"]
    assert call.status_code == 200
    assert call.json()["outputs"][0]["value"]["items"] == [
        {"path": "/echo", "cookie": ""}
    ]
    assert responses["unsafe"].status_code == 422
    assert responses["unsafe"].json()["error_type"] == "InvalidRequestError"
    assert responses["anonymous"].status_code == 401
    assert 'crawler_requests_total{status="2xx"} 1.0' in responses["metrics"].text


def test_crawler_metrics_plugin_counts_requests_retries_and_submissions(server):
    """指标插件按节点边界统计请求状态段、重试、拒绝、Items 与派生种子。

    Args:
        server: 本地 HTTP 服务。
    """
    base, _, _ = server
    metrics = Metrics()
    spider = Echo(
        base, max_retry=1, strict=False, plugins=[metrics.plugin, crawler_metrics]
    )
    spider.submit({"path": "/echo", "submit": "any"})
    spider.submit({"path": "/error"})
    report = spider.run("spider", kwargs={"timeout": 10})
    assert report.counts["acked"] == 2 and report.counts["dead"] == 1

    def value(name, **labels):
        """读取一条计数器序列的值。"""
        series = metrics.snapshot()[name]["series"]
        return next(item["value"] for item in series if item["labels"] == labels)

    assert value("crawler.requests", status="2xx") == 2
    assert value("crawler.requests", status="5xx") == 2
    assert value("crawler.retries") == 1
    assert value("crawler.seeds.rejected", stage="retry") == 1
    assert value("crawler.seeds.submitted", source="any") == 1
    assert value("crawler.items") == 2
    assert value("crawler.seeds.completed") == 2
