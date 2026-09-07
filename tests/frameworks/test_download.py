import asyncio
import gzip
import json
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, Thread

import httpx
import pytest

from bricks import Context, Graph, Runtime
from bricks.engine.errors import ExecutionCancelledError, NodeTimeoutError
from bricks.frameworks.crawler import (
    AsyncDownloadNode,
    DownloadNode,
    Request,
    Response,
)
from bricks.frameworks.crawler.downloaders.httpx import (
    AsyncHttpxDownloader,
    HttpxDownloader,
)


@pytest.fixture
def server():
    """启动本地 HTTP 服务，并在用例结束后关闭服务和线程。

    Yields:
        服务基础 URL、收到的请求记录和慢请求启动事件。
    """

    seen = []
    slow_started = Event()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            """处理本地测试 HTTP 请求并生成当前场景的响应。"""

            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            seen.append((self.command, self.path, self.headers, body))
            headers = []
            status = 200
            data = json.dumps(
                {
                    "path": self.path,
                    "cookie": self.headers.get("Cookie", ""),
                    "body": body.decode(),
                }
            ).encode()
            if self.path == "/redirect":
                status = 302
                headers = [
                    ("Location", "/delete"),
                    ("Set-Cookie", "session=abc; Path=/"),
                ]
            elif self.path == "/delete":
                status = 302
                headers = [
                    ("Location", "/echo"),
                    ("Set-Cookie", "session=; Max-Age=0; Path=/"),
                ]
            elif self.path == "/loop":
                status = 302
                headers = [("Location", "/loop")]
            elif self.path == "/post-redirect":
                status = 303
                headers = [("Location", "/echo")]
            elif self.path == "/cross":
                status = 302
                headers = [
                    ("Location", f"http://localhost:{self.server.server_port}/echo")
                ]
            elif self.path == "/gzip":
                data = gzip.compress(b"hello" * 100)
                headers = [("Content-Encoding", "gzip")]
            elif self.path == "/slow":
                slow_started.set()
                time.sleep(0.2)
            elif self.path == "/error":
                status = 500
            elif self.path == "/cookies":
                headers = [("Set-Cookie", "a=1; Path=/"), ("Set-Cookie", "b=2; Path=/")]
            self.send_response(status)
            for name, value in headers:
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass

        do_POST = do_GET

        def log_message(self, *args):
            """屏蔽本地测试 HTTP 服务的常规访问日志。

            Args:
                *args: 调用协议传入的位置参数。
            """

            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}", seen, slow_started
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join()


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def fetch(request):
    """取得当前场景的下载响应或参数化下载函数。

    Args:
        request: 当前 HTTP 请求或 pytest 提供的参数化夹具对象。

    Returns:
        当前场景构造的响应；夹具入口返回参数化下载函数。
    """

    if request.param:
        downloader = AsyncHttpxDownloader(max_redirects=2)
        return lambda value: asyncio.run(downloader.fetch(value))
    return HttpxDownloader(max_redirects=2).fetch


def test_http_round_trip_and_actual_sent_snapshot(server, fetch):
    """验证真实 HTTP 收发与实际发送请求快照。

    Args:
        server: 当前用例使用的 server 夹具或参数化输入。
        fetch: 当前用例使用的 fetch 夹具或参数化输入。
    """

    base, seen, _ = server
    request = Request(
        base + "/echo?old=1#fragment",
        method="POST",
        params={"tag": ["a", "b"]},
        headers=[("X-Test", "one"), ("X-Test", "two")],
        cookies={"token": "value"},
        body={"page": 1},
    )
    response = fetch(request)
    assert response.ok
    assert response.json()["path"] == "/echo?old=1&tag=a&tag=b"
    assert response.json()["body"] == '{"page":1}'
    assert response.json()["cookie"] == "token=value"
    assert seen[0][2].get_all("X-Test") == ["one", "two"]
    assert response.request.body == seen[0][3]
    assert response.request.headers["Content-Length"] == str(len(seen[0][3]))
    assert response.request.headers["Cookie"] == "token=value"
    assert response.request.params.raw == ()
    request.body = b"changed"
    assert response.request.body == b'{"page":1}'
    assert response.cost > 0


def test_redirect_cookie_updates_history_and_no_cross_fetch_session(server, fetch):
    """验证重定向 Cookie 更新、删除与跨请求会话隔离。

    Args:
        server: 当前用例使用的 server 夹具或参数化输入。
        fetch: 当前用例使用的 fetch 夹具或参数化输入。
    """

    base, seen, _ = server
    response = fetch(Request(base + "/redirect"))
    assert response.url == base + "/echo"
    assert [item.status_code for item in response.history] == [302, 302]
    assert seen[1][2]["Cookie"] == "session=abc"
    assert response.json()["cookie"] == ""
    cookies = fetch(Request(base + "/cookies"))
    assert len(cookies.headers.get_all("Set-Cookie")) == 2
    assert cookies.cookies.get("a") == "1"
    assert fetch(Request(base + "/echo")).json()["cookie"] == ""


def test_redirect_method_and_disabled_redirects(server, fetch):
    """验证重定向的方法转换和禁止跟随选项。

    Args:
        server: 当前用例使用的 server 夹具或参数化输入。
        fetch: 当前用例使用的 fetch 夹具或参数化输入。
    """

    base, _, _ = server
    response = fetch(Request(base + "/post-redirect", method="POST", body=b"data"))
    assert response.request.method == "GET"
    assert response.request.body == b""
    assert response.history[0].request.method == "POST"
    assert response.history[0].request.body == b"data"
    response = fetch(Request(base + "/redirect", allow_redirects=False))
    assert response.status_code == 302
    assert response.history == ()


def test_http_errors_are_not_retried_and_content_is_decoded(server, fetch):
    """验证 HTTP 错误不重试且响应内容正确解压。

    Args:
        server: 当前用例使用的 server 夹具或参数化输入。
        fetch: 当前用例使用的 fetch 夹具或参数化输入。
    """

    base, seen, _ = server
    response = fetch(Request(base + "/error"))
    assert response.status_code == 500
    assert response.error is None
    assert len(seen) == 1
    response = fetch(Request(base + "/gzip"))
    assert response.content == b"hello" * 100
    assert response.size() == 500
    assert int(response.headers["Content-Length"]) != response.size()


def test_transport_timeout_and_redirect_limit_are_failure_responses(server, fetch):
    """验证网络超时和重定向超限转换为失败响应。

    Args:
        server: 当前用例使用的 server 夹具或参数化输入。
        fetch: 当前用例使用的 fetch 夹具或参数化输入。
    """

    base, seen, _ = server
    response = fetch(Request(base + "/slow", timeout=0.03))
    assert response.status_code == -1
    assert isinstance(response.error, httpx.TimeoutException)
    assert response.request.url == base + "/slow"
    assert len(seen) == 1
    response = fetch(Request(base + "/loop"))
    assert response.status_code == -1
    assert isinstance(response.error, httpx.TooManyRedirects)


def test_explicit_proxy_and_no_timeout(server, fetch):
    """验证显式代理以及不限时请求。

    Args:
        server: 当前用例使用的 server 夹具或参数化输入。
        fetch: 当前用例使用的 fetch 夹具或参数化输入。
    """

    base, seen, _ = server
    response = fetch(
        Request("http://unresolvable.invalid/echo", proxy=base, timeout=None)
    )
    assert response.ok
    assert seen[0][1] == "http://unresolvable.invalid/echo"


def test_cross_origin_redirect_does_not_forward_credentials(server, fetch):
    """验证跨来源重定向不转发原认证信息。

    Args:
        server: 当前用例使用的 server 夹具或参数化输入。
        fetch: 当前用例使用的 fetch 夹具或参数化输入。
    """

    base, seen, _ = server
    response = fetch(
        Request(
            base + "/cross",
            headers={"Authorization": "secret"},
            cookies={"session": "secret"},
        )
    )
    assert response.ok
    assert seen[0][2]["Authorization"] == "secret"
    assert seen[1][2].get("Authorization") is None
    assert response.json()["cookie"] == ""


@pytest.mark.parametrize("asynchronous", [False, True])
def test_selection_and_downloader_return_contracts(asynchronous):
    """验证下载器选择函数及下载结果的返回契约。

    Args:
        asynchronous: 是否使用异步接口执行当前场景。
    """

    node_type = AsyncDownloadNode if asynchronous else DownloadNode
    downloader_type = FakeAsyncDownloader if asynchronous else FakeDownloader
    bindings = {"default": downloader_type("default")}

    async def async_select(request):
        """提供异步选择函数，以验证下载节点拒绝异步选择逻辑。

        Args:
            request: 当前 HTTP 请求或 pytest 提供的参数化夹具对象。

        Returns:
            测试使用的默认下载器名称。
        """

        return "default"

    for select in (1, async_select):
        with pytest.raises(TypeError, match="select"):
            node_type(bindings, select=select)
    for selected in (1, [], {}):
        node = node_type(bindings, select=lambda request: selected)
        with pytest.raises(TypeError, match="select"):
            result = node.execute(
                {"request": Request("https://example.com")}, Context(lambda *args: None)
            )
            if asynchronous:
                asyncio.run(result)

    class Invalid:
        def fetch(self, request):
            """取得当前场景的下载响应或参数化下载函数。

            Args:
                request: 当前 HTTP 请求或 pytest 提供的参数化夹具对象。
            """

            return None

    class AsyncInvalid:
        async def fetch(self, request):
            """取得当前场景的下载响应或参数化下载函数。

            Args:
                request: 当前 HTTP 请求或 pytest 提供的参数化夹具对象。
            """

            return None

    node = node_type({"default": AsyncInvalid() if asynchronous else Invalid()})
    with pytest.raises(TypeError, match="Response"):
        result = node.execute(
            {"request": Request("https://example.com")}, Context(lambda *args: None)
        )
        if asynchronous:
            asyncio.run(result)


def test_async_downloader_can_be_shared_without_cookie_state(server):
    """验证并发共享异步下载器时 Cookie 状态隔离。

    Args:
        server: 当前用例使用的 server 夹具或参数化输入。
    """

    base, _, _ = server

    async def run():
        """运行当前测试的异步或并发场景并完成断言。"""

        downloader = AsyncHttpxDownloader()
        responses = await asyncio.gather(
            *(
                downloader.fetch(Request(base + "/echo", cookies={"session": str(i)}))
                for i in range(4)
            )
        )
        assert [response.json()["cookie"] for response in responses] == [
            f"session={i}" for i in range(4)
        ]

    asyncio.run(run())


def test_download_example(server):
    """验证下载示例能与本地 HTTP 服务完成收发。

    Args:
        server: 当前用例使用的 server 夹具或参数化输入。
    """

    from examples.crawler_download import run

    base, _, _ = server
    assert run(base + "/echo").ok


class FakeDownloader:
    """当前契约测试使用的 FakeDownloader 替代实现。

    Attributes:
        value: 当前记录携带的数据值。
    """

    def __init__(self, value):
        """初始化实例及其依赖，建立当前对象独立维护的状态。

        Args:
            value: 当前操作处理的输入值。
        """

        self.value = value

    def fetch(self, request):
        """取得当前场景的下载响应或参数化下载函数。

        Args:
            request: 当前 HTTP 请求或 pytest 提供的参数化夹具对象。

        Returns:
            当前场景构造的响应；夹具入口返回参数化下载函数。
        """

        request.headers["X-Downloader"] = self.value
        return Response(self.value.encode(), request=request)


class FakeAsyncDownloader:
    """当前契约测试使用的 FakeAsyncDownloader 替代实现。

    Attributes:
        value: 当前记录携带的数据值。
    """

    def __init__(self, value):
        """初始化实例及其依赖，建立当前对象独立维护的状态。

        Args:
            value: 当前操作处理的输入值。
        """

        self.value = value

    async def fetch(self, request):
        """取得当前场景的下载响应或参数化下载函数。

        Args:
            request: 当前 HTTP 请求或 pytest 提供的参数化夹具对象。

        Returns:
            当前场景构造的响应；夹具入口返回参数化下载函数。
        """

        await asyncio.sleep(0)
        return FakeDownloader(self.value).fetch(request)


@pytest.mark.parametrize("asynchronous", [False, True])
def test_graph_dynamic_selection_and_concurrent_request_isolation(asynchronous):
    """验证 Graph 动态选择下载器且并发请求互不污染。

    Args:
        asynchronous: 是否使用异步接口执行当前场景。
    """

    node_type = AsyncDownloadNode if asynchronous else DownloadNode
    downloader_type = FakeAsyncDownloader if asynchronous else FakeDownloader
    bindings = {
        "default": downloader_type("default"),
        "special": downloader_type("special"),
    }
    node = node_type(
        bindings,
        select=lambda request: "special" if request.url.endswith("/special") else None,
    )
    bindings.clear()
    graph = Graph(entrypoint="download").add(download=node)
    requests = [
        Request("https://example.com/" + name) for name in ("special", "normal") * 4
    ]
    with Runtime() as runtime:
        runtime.register("download", graph)
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(
                executor.map(
                    lambda value: runtime.run("download", value)[0].value, requests
                )
            )
    assert [response.text for response in results] == ["special", "default"] * 4
    assert all("X-Downloader" not in request.headers for request in requests)


@pytest.mark.parametrize(
    "node_type,downloader_type",
    [
        (DownloadNode, FakeDownloader),
        (AsyncDownloadNode, FakeAsyncDownloader),
    ],
)
def test_registry_validation_and_unknown_name(node_type, downloader_type):
    """验证下载器注册参数和未知名称错误。

    Args:
        node_type: 当前用例使用的 node_type 夹具或参数化输入。
        downloader_type: 当前用例使用的 downloader_type 夹具或参数化输入。
    """

    with pytest.raises(ValueError, match="default"):
        node_type({})
    with pytest.raises(ValueError, match="names"):
        node_type({"": downloader_type("x")})
    with pytest.raises(TypeError, match="fetch"):
        node_type({"default": object()})
    node = node_type(
        {"other": downloader_type("other")},
        default="other",
        select=lambda request: "missing",
    )
    context = Context(lambda *args: None)
    with pytest.raises(ValueError, match="missing"):
        result = node.execute({"request": Request("https://example.com")}, context)
        if isinstance(node, AsyncDownloadNode):
            asyncio.run(result)


def test_sync_async_mismatch_fails_during_assembly():
    """验证同步异步下载器类别不匹配时装配失败。"""

    with pytest.raises(TypeError, match="sync"):
        DownloadNode({"default": FakeAsyncDownloader("x")})
    with pytest.raises(TypeError, match="async"):
        AsyncDownloadNode({"default": FakeDownloader("x")})


@pytest.mark.parametrize(
    "error",
    [ValueError("bug"), ExecutionCancelledError("cancel"), NodeTimeoutError("timeout")],
)
@pytest.mark.parametrize("asynchronous", [False, True])
def test_node_does_not_swallow_programming_or_control_errors(error, asynchronous):
    """验证下载节点不吞掉程序错误与执行控制异常。

    Args:
        error: 需要传播、记录或用于恢复的异常。
        asynchronous: 是否使用异步接口执行当前场景。
    """

    class Broken:
        def fetch(self, request):
            """取得当前场景的下载响应或参数化下载函数。

            Args:
                request: 当前 HTTP 请求或 pytest 提供的参数化夹具对象。
            """

            raise error

    class AsyncBroken:
        async def fetch(self, request):
            """取得当前场景的下载响应或参数化下载函数。

            Args:
                request: 当前 HTTP 请求或 pytest 提供的参数化夹具对象。
            """

            raise error

    node = (
        AsyncDownloadNode({"default": AsyncBroken()})
        if asynchronous
        else DownloadNode({"default": Broken()})
    )
    with pytest.raises(type(error)) as caught:
        result = node.execute(
            {"request": Request("https://example.com")}, Context(lambda *args: None)
        )
        if asynchronous:
            asyncio.run(result)
    assert caught.value is error


def test_real_async_download_cancellation_propagates(server):
    """验证真实异步下载的任务取消继续传播。

    Args:
        server: 当前用例使用的 server 夹具或参数化输入。
    """

    base, _, started = server

    async def run():
        """运行当前测试的异步或并发场景并完成断言。"""

        task = asyncio.create_task(
            AsyncHttpxDownloader().fetch(Request(base + "/slow"))
        )
        assert await asyncio.to_thread(started.wait, 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())


@pytest.mark.parametrize("asynchronous", [False, True])
def test_real_http_download_through_graph(server, asynchronous):
    """验证同步和异步下载节点在 Graph 中完成真实 HTTP 请求。

    Args:
        server: 当前用例使用的 server 夹具或参数化输入。
        asynchronous: 是否使用异步接口执行当前场景。
    """

    base, _, _ = server
    node = (
        AsyncDownloadNode({"default": AsyncHttpxDownloader()})
        if asynchronous
        else DownloadNode({"default": HttpxDownloader()})
    )
    with Runtime() as runtime:
        runtime.register("download", Graph(entrypoint="download").add(download=node))
        response = runtime.run("download", Request(base + "/echo"))[0].value
    assert response.ok
    assert response.json()["path"] == "/echo"
