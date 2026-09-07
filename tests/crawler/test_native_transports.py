"""wreq 和 primp 的网络、会话和指纹行为测试。"""

import asyncio
import inspect
import sys

import pytest
from interlace import Context, Graph, Runtime, Slot

from bricks import AsyncDownloadNode, DownloadNode, Request, UploadFile
from bricks.downloaders.primp import AsyncPrimpDownloader, PrimpDownloader

_TRANSPORTS = [PrimpDownloader, AsyncPrimpDownloader]  # 当前解释器可验证的下载器集合。
if sys.version_info >= (3, 11):
    from bricks.downloaders.wreq import AsyncWreqDownloader, WreqDownloader

    _TRANSPORTS.extend([WreqDownloader, AsyncWreqDownloader])


async def resolve(value):
    """等待异步值或返回同步值。

    Args:
        value: 当前调用结果。

    Returns:
        已完成的结果。
    """

    return await value if inspect.isawaitable(value) else value


@pytest.fixture(params=_TRANSPORTS)
def transport(request):
    """准备当前测试的无状态传输实例。

    Args:
        request: pytest 参数化输入。

    Returns:
        当前下载器及是否使用 wreq。
    """

    factory = request.param
    return factory(max_redirects=2), "Wreq" in factory.__name__


def test_buffered_http_model_and_redirects(server, transport):
    """验证 JSON、上传、解压、HTTP 状态、请求复制和重定向。

    Args:
        server: 本地 HTTP 服务。
        transport: 当前下载器及其类别。
    """

    downloader, is_wreq = transport
    base, seen, _ = server

    async def run():
        """验证当前下载器的完整请求响应映射。"""

        request = Request(
            base + "/echo",
            method="POST",
            params={"q": "中文"},
            body={"a": 1},
            cookies={"explicit": "yes"},
        )
        original = request.copy()
        response = await resolve(downloader.fetch(request))
        assert response.status_code == 200
        assert response.json()["body"] == '{"a":1}'
        assert "explicit=yes" in response.json()["cookie"]
        assert "q=" in seen[-1][1]
        assert request.body == original.body
        assert request.headers.raw == original.headers.raw
        assert request.cookies == original.cookies
        assert response.request is not request
        response = await resolve(downloader.fetch(Request(base + "/cookies")))
        assert response.cookies.get("a") == "1"
        assert response.cookies.get("b") == "2"
        if is_wreq:
            assert len(response.headers.get_all("set-cookie")) == 2
        upload = Request(
            base + "/echo",
            method="POST",
            body={"file": UploadFile("test.txt", b"upload")},
            body_type="multipart",
        )
        assert (await resolve(downloader.fetch(upload))).status_code == 200
        assert b"upload" in seen[-1][3]
        assert (
            await resolve(downloader.fetch(Request(base + "/gzip")))
        ).content == b"hello" * 100
        assert (
            await resolve(downloader.fetch(Request(base + "/error")))
        ).status_code == 500
        response = await resolve(downloader.fetch(Request(base + "/redirect")))
        assert response.url == base + "/echo"
        assert response.json()["cookie"] == ""
        if is_wreq:
            assert [item.status_code for item in response.history] == [302, 302]
            assert response.history[0].url == base + "/redirect"
        else:
            assert response.history == ()
        assert (
            await resolve(
                downloader.fetch(Request(base + "/redirect", allow_redirects=False))
            )
        ).status_code == 302
        assert (await resolve(downloader.fetch(Request(base + "/echo")))).json()[
            "cookie"
        ] == ""

    asyncio.run(run())


def test_network_failures_and_proxy(server, transport):
    """验证网络失败转换与显式代理。

    Args:
        server: 本地 HTTP 服务及代理响应端。
        transport: 当前传输实现。
    """

    downloader, _ = transport
    base, seen, _ = server

    async def run():
        """执行超时、重定向超限及代理请求。"""

        for request in (Request(base + "/slow", timeout=0.01), Request(base + "/loop")):
            response = await resolve(downloader.fetch(request))
            assert response.status_code == -1
            assert response.error is not None
        response = await resolve(
            downloader.fetch(Request("http://proxy-target.invalid/echo", proxy=base))
        )
        assert response.status_code == 200
        assert seen[-1][1] == "http://proxy-target.invalid/echo"

    asyncio.run(run())


def test_sessions_and_context_fingerprints(server, transport):
    """验证登录会话隔离、Context 指纹策略和成对关闭。

    Args:
        server: 本地 HTTP 服务。
        transport: 当前下载器及类别。
    """

    downloader, is_wreq = transport
    first = "Chrome149" if is_wreq else "chrome_146"
    second = "Firefox151" if is_wreq else "firefox_146"
    downloader = type(downloader)(impersonate=first)
    asynchronous = inspect.iscoroutinefunction(downloader.fetch)

    async def run():
        """在同一个事件循环内创建和关闭会话。"""

        sessions = [await resolve(downloader.prepare_session()) for _ in range(2)]
        slot = Slot({"downloaders": {"default": sessions[0]}})
        node_type = AsyncDownloadNode if asynchronous else DownloadNode
        node = node_type(
            lambda slot: slot["downloaders"],
            isolated_downloaders={"default": downloader},
        )
        base = server[0]

        async def download(path, options):
            """使用 Context 配置执行当前 Slot 下载。

            Args:
                path: 本地路径。
                options: 当前执行选项。

            Returns:
                当前节点输出响应。
            """

            output = await resolve(
                node.execute(
                    {"request": Request(base + path)},
                    Context(lambda event: None, slot, options=options),
                )
            )
            return output.value

        try:
            await download("/login/original", {})
            before = await download("/echo", {})
            assert before.json()["cookie"] == "account=original"
            repeated = await download("/echo", {})
            assert repeated.json()["peer_port"] == before.json()["peer_port"]
            profiled = await download("/echo", {"crawler.fingerprint.impersonate": first})
            assert profiled.json()["cookie"] == "account=original"
            assert (await resolve(sessions[1].fetch(Request(base + "/echo")))).json()[
                "cookie"
            ] == ""
            if is_wreq:
                changed = await download(
                    "/echo", {"crawler.fingerprint.impersonate": second}
                )
                assert changed.json()["cookie"] == "account=original"
            else:
                with pytest.raises(ValueError, match="fingerprint is fixed"):
                    await download("/echo", {"crawler.fingerprint.impersonate": second})
            isolated = await download(
                "/login/temporary",
                {
                    "crawler.fingerprint.impersonate": second,
                    "crawler.session.reuse": False,
                },
            )
            assert isolated.json()["cookie"] == ""
            after = await download("/echo", {})
            assert after.json()["cookie"] == "account=original"
            with pytest.raises(ValueError, match="proxy"):
                await resolve(sessions[0].fetch(Request(base, proxy=base)))
        finally:
            for session in sessions:
                await resolve(downloader.close_session(session))
                await resolve(downloader.close_session(session))
        with pytest.raises(RuntimeError, match="closed"):
            await resolve(sessions[0].fetch(Request(base + "/echo")))

    asyncio.run(run())


def test_context_runtime_integration(server, transport):
    """验证公开 Runtime 入口可选择新下载器和指纹。

    Args:
        server: 本地 HTTP 服务。
        transport: 当前下载器及类别。
    """

    downloader, is_wreq = transport
    node_type = (
        AsyncDownloadNode
        if inspect.iscoroutinefunction(downloader.fetch)
        else DownloadNode
    )
    graph = Graph(entrypoint="download").add(
        download=node_type({"default": downloader})
    )
    with Runtime() as runtime:
        runtime.register("crawl", graph)
        response = runtime.run(
            "crawl",
            Request(server[0] + "/echo"),
            options={
                "crawler.fingerprint.impersonate": "Chrome149"
                if is_wreq
                else "chrome_146"
            },
        )[0].value
        assert response.status_code == 200


def test_configuration_errors_and_duplicate_headers(server, transport):
    """验证无效指纹不随机降级，头能力差异如实表达。

    Args:
        server: 本地 HTTP 服务。
        transport: 当前下载器及类别。
    """

    downloader, is_wreq = transport
    with pytest.raises(ValueError, match="fingerprint"):
        type(downloader)(impersonate="invalid")
    assert server[1] == []

    async def run():
        """发送或拒绝重复请求头。"""

        request = Request(server[0], headers=[("X-Test", "a"), ("X-Test", "b")])
        if is_wreq:
            assert (await resolve(downloader.fetch(request))).status_code == 200
            assert server[1][-1][2].get_all("X-Test") == ["a", "b"]
        else:
            with pytest.raises(ValueError, match="duplicate"):
                await resolve(downloader.fetch(request))

    asyncio.run(run())


@pytest.mark.parametrize(
    "factory", [item for item in _TRANSPORTS if inspect.iscoroutinefunction(item.fetch)]
)
def test_async_cancellation_and_session_recovery(server, factory):
    """验证取消继续传播，复用会话随后仍可下载。

    Args:
        server: 本地 HTTP 服务和开始事件。
        factory: 原生异步下载器类型。
    """

    async def run():
        """等服务端接收请求后取消，并验证后续调用。"""

        downloader = factory()
        session = await downloader.prepare_session()
        task = asyncio.create_task(session.fetch(Request(server[0] + "/slow")))
        try:
            for _ in range(1000):
                if server[2].is_set():
                    break
                await asyncio.sleep(0.001)
            assert server[2].is_set()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert (
                await session.fetch(Request(server[0] + "/echo"))
            ).status_code == 200
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await downloader.close_session(session)

    asyncio.run(run())
