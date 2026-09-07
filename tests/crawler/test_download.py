import asyncio
from http.cookies import SimpleCookie
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from threading import Barrier

import httpx
import pytest
from interlace import AsyncNode, Context, Graph, Node, Ports, Runtime, Slot, SlotPool
from interlace.engine.errors import ExecutionCancelledError, NodeTimeoutError

from bricks import (
    AsyncDownloadNode,
    DownloadNode,
    Request,
    Response,
)
from bricks.downloaders.httpx import (
    AsyncHttpxDownloader,
    AsyncHttpxSessionDownloader,
    HttpxDownloader,
    HttpxSessionDownloader,
)
from bricks.downloaders import AsyncSessionDownloader, SessionDownloader


@pytest.mark.parametrize("asynchronous", [False, True])
def test_context_session_policy_preserves_existing_session(server, asynchronous):
    """验证临时请求隔离 Cookie 和连接，且不破坏已有会话。

    Args:
        server: 本地 HTTP 服务。
        asynchronous: 是否验证异步下载节点。
    """

    base, _, _ = server

    async def run():
        """在同一事件循环内创建、使用和关闭会话。"""

        downloader = AsyncHttpxDownloader() if asynchronous else HttpxDownloader()
        session = (
            await downloader.prepare_session()
            if asynchronous
            else downloader.prepare_session()
        )
        slot = Slot({"crawler.downloaders": {"default": session}})
        node_type = AsyncDownloadNode if asynchronous else DownloadNode
        node = node_type(
            lambda current: current["crawler.downloaders"],
            isolated_downloaders={"default": downloader},
        )

        async def fetch(path, *, isolated=False, cookies=None):
            """按本次调用策略执行下载。

            Args:
                path: 本地请求路径。
                isolated: 是否禁用会话复用。
                cookies: 本次请求显式提供的 Cookie。

            Returns:
                服务端回显的请求信息。
            """

            context = Context(
                lambda event: None,
                None if isolated else slot,
                options={"crawler.session.reuse": False} if isolated else {},
            )
            result = node.execute(
                {"request": Request(base + path, cookies=cookies)}, context
            )
            output = await result if asynchronous else result
            return output.value.json()

        try:
            login = await fetch("/login/original")
            before = await fetch("/echo")
            isolated = await fetch("/login/temporary", isolated=True, cookies={"explicit": "yes"})
            fresh = await fetch("/echo", isolated=True)
            after = await fetch("/echo")
            assert before["cookie"] == after["cookie"] == "account=original"
            assert isolated["cookie"] == "explicit=yes"
            assert fresh["cookie"] == ""
            assert login["peer_port"] == before["peer_port"] == after["peer_port"]
            assert isolated["peer_port"] != before["peer_port"]
            assert fresh["peer_port"] != before["peer_port"]
        finally:
            if asynchronous:
                await downloader.close_session(session)
            else:
                downloader.close_session(session)

    asyncio.run(run())


def test_runtime_session_options_reach_download_node(server):
    """验证 Runtime 配置通过 Context 影响真实 HTTP 下载。

    Args:
        server: 本地 HTTP 服务。
    """

    base, _, _ = server
    downloader = HttpxDownloader()
    session = downloader.prepare_session()
    try:
        graph = Graph(entrypoint="download").add(
            download=DownloadNode(
                {"default": session}, isolated_downloaders={"default": downloader}
            )
        )
        with Runtime() as runtime:
            runtime.register("crawl", graph)
            runtime.run("crawl", Request(base + "/login/original"))
            response = runtime.run(
                "crawl", Request(base + "/echo"), options={"crawler.session.reuse": False}
            )[0].value
            assert response.json()["cookie"] == ""
            response = runtime.run("crawl", Request(base + "/echo"))[0].value
            assert response.json()["cookie"] == "account=original"
    finally:
        downloader.close_session(session)


@pytest.mark.parametrize("asynchronous", [False, True])
def test_invalid_session_policy_fails_before_download(asynchronous):
    """验证错误策略或缺失临时下载器在网络调用之前失败。

    Args:
        asynchronous: 是否验证异步节点。
    """

    downloader = AsyncHttpxDownloader() if asynchronous else HttpxDownloader()
    node_type = AsyncDownloadNode if asynchronous else DownloadNode
    node = node_type({"default": downloader})

    def invoke(value):
        """执行非法配置用例。

        Args:
            value: 本次会话复用选项。
        """

        result = node.execute(
            {"request": Request("https://example.com")},
            Context(lambda event: None, options={"crawler.session.reuse": value}),
        )
        if asynchronous:
            asyncio.run(result)

    for value in (None, "false", 0, 1):
        with pytest.raises(TypeError, match="boolean"):
            invoke(value)
    with pytest.raises(ValueError, match="isolated_downloaders"):
        invoke(False)




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


def test_slot_sessions_follow_events_and_isolate_concurrent_chains(server):
    """验证真实事件链跨图复用会话且不同 Slot 并发隔离。

    Args:
        server: 本地 HTTP 服务夹具。
    """

    base, _, _ = server
    barrier = Barrier(2)
    results = []

    def resolve(slot):
        """读取槽内资源并让两个根执行链同时进入下载。

        Args:
            slot: 当前执行槽。

        Returns:
            当前槽专用下载器映射。
        """

        if not slot.get("started"):
            slot["started"] = True
            barrier.wait(timeout=5)
        return slot["downloaders"]

    class Continue(Node):
        """登录后发出携带同一 Slot 的后续请求。"""

        input_ports = Ports(response=Response)  # 登录响应端口。

        def execute(self, inputs, context):
            """保存账号标记并显式发出后续事件。

            Args:
                inputs: 登录响应。
                context: 当前槽及事件发布接口。
            """

            context.slot["account"] = inputs["response"].url.rsplit("/", 1)[1]
            context.emit("follow", Request(base + "/echo"))

    class Collect(Node):
        """收集跨图延续的响应及会话归属。"""

        input_ports = Ports(response=Response)  # 后续请求响应端口。

        def execute(self, inputs, context):
            """记录响应 Cookie 与 Slot 身份。

            Args:
                inputs: 后续响应。
                context: 当前执行槽。
            """

            results.append(
                (
                    context.slot.id,
                    context.slot["account"],
                    inputs["response"].json()["cookie"],
                )
            )

    with ExitStack() as resources:
        clients = [
            resources.enter_context(httpx.Client(trust_env=False)) for _ in range(2)
        ]
        slots = [
            Slot({"downloaders": {"default": HttpxSessionDownloader(c)}})
            for c in clients
        ]
        pool = SlotPool(slots=slots)
        resources.callback(pool.close)
        node = DownloadNode(resolve)
        with Runtime() as runtime:
            runtime.register(
                "login",
                Graph(entrypoint="download")
                .add(download=node, next=Continue())
                .connect(
                    "download", "next", source_port="response", target_port="response"
                ),
            )
            runtime.register(
                "follow",
                Graph(entrypoint="download")
                .add(download=node, collect=Collect())
                .connect(
                    "download",
                    "collect",
                    source_port="response",
                    target_port="response",
                ),
            )
            runtime.on("login", graph="login", queue="login", concurrency=2, slots=pool)
            runtime.on(
                "follow", graph="follow", queue="follow", concurrency=2, slots=pool
            )
            runtime.emit("login", Request(base + "/login/A"))
            runtime.emit("login", Request(base + "/login/B"))
            runtime.wait_idle(timeout=10)
        assert pool.available == 2
        assert all(not client.is_closed for client in clients)
        assert len({slot_id for slot_id, _, _ in results}) == 2
        assert sorted((account, cookie) for _, account, cookie in results) == [
            ("A", "account=A"),
            ("B", "account=B"),
        ]
    assert all(client.is_closed for client in clients)


def test_async_slot_sessions_and_cancellation(server):
    """验证异步同槽复用、不同槽隔离和取消后的外部所有权。

    Args:
        server: 本地 HTTP 服务夹具。
    """

    base, _, slow_started = server

    async def run():
        """在同一事件循环内使用并关闭所有异步会话。"""

        async with (
            httpx.AsyncClient(trust_env=False) as a,
            httpx.AsyncClient(trust_env=False) as b,
        ):
            node = AsyncDownloadNode(lambda slot: slot["downloaders"])
            contexts = [
                Context(
                    lambda event: None,
                    Slot(
                        {
                            "downloaders": {
                                "default": AsyncHttpxSessionDownloader(client)
                            }
                        }
                    ),
                )
                for client in (a, b)
            ]

            async def chain(context, account):
                """在一个槽内先登录再读取 Cookie。

                Args:
                    context: 当前槽上下文。
                    account: 本链独立账号。

                Returns:
                    后续请求实际携带的 Cookie。
                """

                await node.execute(
                    {"request": Request(base + "/login/" + account)}, context
                )
                output = await node.execute(
                    {"request": Request(base + "/echo")}, context
                )
                return output.value.json()["cookie"]

            assert await asyncio.gather(
                chain(contexts[0], "A"), chain(contexts[1], "B")
            ) == ["account=A", "account=B"]
            task = asyncio.create_task(
                node.execute({"request": Request(base + "/slow")}, contexts[0])
            )
            assert await asyncio.to_thread(slow_started.wait, 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not a.is_closed
            assert await chain(contexts[0], "C") == "account=C"
        assert a.is_closed and b.is_closed

    asyncio.run(run())


@pytest.mark.parametrize("asynchronous", [False, True])
def test_slot_resolver_contracts(asynchronous):
    """验证无槽、非法解析结果和同步异步类别错误明确失败。

    Args:
        asynchronous: 是否使用异步下载节点。
    """

    node_type = AsyncDownloadNode if asynchronous else DownloadNode

    def invoke(resolver, context):
        """执行当前类别节点。

        Args:
            resolver: 待校验资源解析函数。
            context: 有槽或无槽上下文。
        """

        result = node_type(resolver).execute(
            {"request": Request("https://example.com")}, context
        )
        if asynchronous:
            asyncio.run(result)

    with pytest.raises(RuntimeError, match="Slot"):
        invoke(lambda slot: {}, Context(lambda event: None))
    context = Context(lambda event: None, Slot())
    with pytest.raises(ValueError, match="not registered"):
        invoke(lambda slot: {}, context)
    with pytest.raises(TypeError, match="mapping"):
        invoke(lambda slot: None, context)
    wrong = HttpxDownloader() if asynchronous else AsyncHttpxDownloader()
    with pytest.raises(TypeError, match="fetch"):
        invoke(lambda slot: {"default": wrong}, context)
    with pytest.raises(KeyError, match="missing"):
        invoke(lambda slot: slot["missing"], context)


@pytest.mark.parametrize("asynchronous", [False, True])
def test_session_request_options_and_errors(server, asynchronous):
    """验证会话模式保持请求策略、复制、传输错误与配置错误语义。

    Args:
        server: 本地 HTTP 服务夹具。
        asynchronous: 是否测试异步会话下载器。
    """

    base, _, _ = server

    async def check(fetch):
        """对同步和异步下载器运行相同请求契约。

        Args:
            fetch: 当前下载方法。
        """

        async def send(request):
            """统一等待当前方法。

            Args:
                request: 待发送请求。

            Returns:
                下载结果。
            """

            result = fetch(request)
            return await result if asynchronous else result

        request = Request(base + "/redirect", allow_redirects=False)
        assert (await send(request)).status_code == 302
        assert "Cookie" not in request.headers
        assert (await send(Request(base + "/echo"))).json()["cookie"] == "session=abc"
        assert (await send(Request(base + "/delete"))).json()["cookie"] == ""
        assert (await send(Request(base + "/error"))).status_code == 500
        assert (await send(Request(base + "/slow", timeout=0.01))).status_code == -1
        with pytest.raises(ValueError, match="proxy"):
            await send(Request(base + "/echo", proxy="http://127.0.0.1:1"))

    async def run():
        """在所有者作用域内运行会话契约。"""

        if asynchronous:
            async with httpx.AsyncClient(trust_env=False) as client:
                await check(AsyncHttpxSessionDownloader(client).fetch)
        else:
            with httpx.Client(trust_env=False) as client:
                await check(HttpxSessionDownloader(client).fetch)

    asyncio.run(run())


def test_session_example(server):
    """验证示例使用真实 SlotPool 完成下载并释放资源。

    Args:
        server: 本地 HTTP 服务夹具。
    """

    from examples.crawler_session import run

    base, _, _ = server
    responses = run([base + "/cookies", base + "/echo"], concurrency=1)
    assert len(responses) == 2
    cookies = SimpleCookie(responses[1].json()["cookie"])
    assert {name: morsel.value for name, morsel in cookies.items()} == {"a": "1", "b": "2"}


@pytest.mark.parametrize("asynchronous", [False, True])
def test_optional_session_lifecycle(server, asynchronous):
    """验证可选会话协议的独立准备、配置继承和重复关闭。

    Args:
        server: 本地 HTTP 服务夹具。
        asynchronous: 是否使用异步会话能力。
    """

    base, _, _ = server

    async def run():
        """对两种会话能力执行相同生命周期契约。"""

        downloader = (
            AsyncHttpxDownloader(max_redirects=0)
            if asynchronous
            else HttpxDownloader(max_redirects=0)
        )
        protocol = AsyncSessionDownloader if asynchronous else SessionDownloader
        assert isinstance(downloader, protocol)
        assert not isinstance(FakeDownloader("plain"), protocol)
        sessions = []
        try:
            for _ in range(2):
                session = (
                    await downloader.prepare_session()
                    if asynchronous
                    else downloader.prepare_session()
                )
                sessions.append(session)
            assert sessions[0].client is not sessions[1].client
            ports = []
            for _ in range(2):
                response = (
                    await sessions[0].fetch(Request(base + "/echo"))
                    if asynchronous
                    else sessions[0].fetch(Request(base + "/echo"))
                )
                ports.append(response.json()["peer_port"])
            assert ports[0] == ports[1]
            for session in sessions:
                response = (
                    await session.fetch(Request(base + "/redirect"))
                    if asynchronous
                    else session.fetch(Request(base + "/redirect"))
                )
                assert response.status_code == -1
                assert isinstance(response.error, httpx.TooManyRedirects)
            sessions[0].client.cookies.set("private", "A")
            assert "private" not in sessions[1].client.cookies
        finally:
            for session in sessions:
                for _ in range(2):
                    if asynchronous:
                        await downloader.close_session(session)
                    else:
                        downloader.close_session(session)
                assert session.client.is_closed
        with pytest.raises(RuntimeError, match="closed"):
            if asynchronous:
                await sessions[0].fetch(Request(base + "/echo"))
            else:
                sessions[0].fetch(Request(base + "/echo"))

    asyncio.run(run())


def test_async_runtime_session_lifecycle(server):
    """验证准备、下载和关闭通过公开 Node API 在同一执行循环完成。

    Args:
        server: 本地 HTTP 服务夹具。
    """

    from examples.crawler_session import Collect

    base, _, _ = server
    downloader = AsyncHttpxDownloader()
    slot = Slot()
    pool = SlotPool(slots=[slot])
    sessions = []
    from queue import SimpleQueue

    results = SimpleQueue()

    class Prepare(AsyncNode):
        """在执行器事件循环准备会话。"""

        async def execute(self, inputs, context):
            """创建并登记当前槽会话。

            Args:
                inputs: 本节点无输入端口。
                context: 执行器提供的上下文。
            """

            session = await downloader.prepare_session()
            sessions.append(session)
            slot["downloaders"] = {"default": session}

    class Close(AsyncNode):
        """在执行器停止前关闭其循环中的会话。"""

        async def execute(self, inputs, context):
            """调用可选协议关闭已停止下载的会话。

            Args:
                inputs: 本节点无输入端口。
                context: 执行器提供的上下文。
            """

            for session in sessions:
                await downloader.close_session(session)

    try:
        with Runtime() as runtime:
            runtime.register(
                "prepare", Graph(entrypoint="prepare").add(prepare=Prepare())
            )
            runtime.register("close", Graph(entrypoint="close").add(close=Close()))
            runtime.register(
                "download",
                Graph(entrypoint="download")
                .add(
                    download=AsyncDownloadNode(lambda current: current["downloaders"]),
                    collect=Collect(results),
                )
                .connect(
                    "download",
                    "collect",
                    source_port="response",
                    target_port="response",
                ),
            )
            try:
                runtime.run("prepare")
                runtime.on("request", graph="download", queue="download", slots=pool)
                runtime.emit("request", Request(base + "/cookies"))
                runtime.emit("request", Request(base + "/echo"))
                runtime.wait_idle(timeout=10)
            finally:
                runtime.run("close")
        assert len(sessions) == 1 and sessions[0].client.is_closed
        assert results.qsize() == 2
        first, second = results.get_nowait(), results.get_nowait()
        assert second.json()["cookie"] == "a=1; b=2"
        assert first.json()["peer_port"] == second.json()["peer_port"]
    finally:
        pool.close()


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
