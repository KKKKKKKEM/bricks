"""默认 curl_cffi 与可选 requests 下载器的真实 HTTP 契约测试。"""

import asyncio
import inspect
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import pytest
from curl_cffi.requests.exceptions import ImpersonateError, SessionClosed
from interlace import Context, Graph, Runtime, Slot

from bricks import AsyncDownloadNode, DownloadNode, Request, UploadFile
from bricks.downloaders.curl_cffi import AsyncCurlCffiDownloader, CurlCffiDownloader
from bricks.downloaders.requests import RequestsDownloader


@pytest.fixture(
    params=[CurlCffiDownloader, AsyncCurlCffiDownloader, RequestsDownloader]
)
def transport(request):
    """为共享契约提供每种新增传输实现。

    Args:
        request: pytest 参数化请求。

    Returns:
        无共享执行状态的下载器实例。
    """

    return request.param(max_redirects=2)


def fetch(transport, request):
    """统一执行临时同步或异步下载。

    Args:
        transport: 当前测试下载器。
        request: 待发送请求。

    Returns:
        下载结果。
    """

    result = transport.fetch(request)
    return asyncio.run(result) if inspect.isawaitable(result) else result


def test_buffered_request_and_response(server, transport):
    """验证编码请求体、重复响应头、请求复制和 HTTP 错误状态。

    Args:
        server: 本地服务。
        transport: 当前下载器。
    """

    base, seen, _ = server
    request = Request(
        base + "/echo",
        method="POST",
        params={"q": "中文"},
        body={"a": 1},
        cookies={"explicit": "yes"},
    )
    original = request.copy()
    response = fetch(transport, request)
    assert response.status_code == 200
    assert response.json()["body"] == '{"a":1}'
    assert "explicit=yes" in response.json()["cookie"]
    assert request.body == original.body
    assert request.headers.raw == original.headers.raw
    assert request.cookies == original.cookies
    assert response.request is not request
    assert "q=" in seen[-1][1]
    response = fetch(transport, Request(base + "/cookies"))
    assert len(response.headers.get_all("Set-Cookie")) == 2
    assert fetch(transport, Request(base + "/gzip")).content == b"hello" * 100
    assert fetch(transport, Request(base + "/error")).status_code == 500
    assert fetch(transport, Request(base + "/echo")).json()["cookie"] == ""


def test_redirect_and_transport_failure(server, transport):
    """验证重定向控制、历史信息和网络超时。

    Args:
        server: 本地服务。
        transport: 当前下载器。
    """

    base, _, _ = server
    response = fetch(transport, Request(base + "/redirect"))
    assert response.status_code == 200
    assert [item.status_code for item in response.history] == [302, 302]
    assert response.url == base + "/echo"
    assert "session=abc" not in response.json()["cookie"]
    assert (
        fetch(transport, Request(base + "/redirect", allow_redirects=False)).status_code
        == 302
    )
    for request in (Request(base + "/loop"), Request(base + "/slow", timeout=0.01)):
        response = fetch(transport, request)
        assert response.status_code == -1
        assert response.error is not None


def test_memory_upload_and_explicit_proxy(server, transport):
    """验证已编码 multipart 上传与逐请求代理。

    Args:
        server: 接收上传或代理请求的本地 HTTP 服务。
        transport: 当前下载器。
    """

    base, seen, _ = server
    request = Request(
        base + "/echo",
        method="POST",
        body={"file": UploadFile("sample.txt", b"upload-body")},
        body_type="multipart",
    )
    assert fetch(transport, request).status_code == 200
    assert b"upload-body" in seen[-1][3]
    assert "multipart/form-data" in seen[-1][2]["Content-Type"]
    response = fetch(transport, Request("http://proxy-target.invalid/echo", proxy=base))
    assert response.status_code == 200
    assert seen[-1][1] == "http://proxy-target.invalid/echo"


@pytest.mark.parametrize(
    "downloader_type", [CurlCffiDownloader, AsyncCurlCffiDownloader]
)
def test_curl_preserves_duplicate_request_headers(server, downloader_type):
    """验证默认传输不会合并重复请求头。

    Args:
        server: 本地 HTTP 服务。
        downloader_type: 同步或异步 curl_cffi 实现。
    """

    response = fetch(
        downloader_type(),
        Request(server[0], headers=[("X-Test", "a"), ("X-Test", "b")]),
    )
    assert response.status_code == 200
    assert server[1][-1][2].get_all("X-Test") == ["a", "b"]


def test_session_isolation_and_context_override(server, transport):
    """验证会话登录态、不同 Slot 隔离和临时请求覆盖。

    Args:
        server: 本地服务。
        transport: 当前下载器。
    """

    base, _, _ = server
    asynchronous = inspect.iscoroutinefunction(transport.fetch)

    async def resolve(value):
        """按下载器类别等待结果。

        Args:
            value: 同步值或异步结果。

        Returns:
            已完成的结果。
        """

        return await value if inspect.isawaitable(value) else value

    async def run():
        """在同一事件循环内验证会话完整生命周期。"""

        sessions = [await resolve(transport.prepare_session()) for _ in range(2)]
        slots = [Slot({"downloaders": {"default": session}}) for session in sessions]
        node_type = AsyncDownloadNode if asynchronous else DownloadNode
        node = node_type(
            lambda slot: slot["downloaders"],
            isolated_downloaders={"default": transport},
        )

        async def download(path, slot, reuse=True):
            """执行指定 Slot 的单次下载。

            Args:
                path: 请求路径。
                slot: 当前会话的执行槽。
                reuse: 是否复用当前槽会话。

            Returns:
                服务端回显信息。
            """

            output = await resolve(
                node.execute(
                    {"request": Request(base + path)},
                    Context(
                        lambda event: None,
                        slot,
                        options={"crawler.session.reuse": reuse},
                    ),
                )
            )
            return output.value.json()

        try:
            await download("/login/one", slots[0])
            before = await download("/echo", slots[0])
            assert before["cookie"] == "account=one"
            assert (await download("/echo", slots[1]))["cookie"] == ""
            assert (await download("/login/temporary", None, False))["cookie"] == ""
            after = await download("/echo", slots[0])
            assert after["cookie"] == "account=one"
            assert after["peer_port"] == before["peer_port"]
            with pytest.raises(ValueError, match="proxy"):
                await resolve(sessions[0].fetch(Request(base, proxy=base)))
        finally:
            for session in sessions:
                await resolve(transport.close_session(session))
                await resolve(transport.close_session(session))

    asyncio.run(run())


def test_default_nodes_use_curl_cffi(server):
    """验证无需装配下载器即可同步或异步下载。

    Args:
        server: 本地服务。
    """

    base, _, _ = server
    with Runtime() as runtime:
        for name, node in (("sync", DownloadNode()), ("async", AsyncDownloadNode())):
            runtime.register(name, Graph(entrypoint="download").add(download=node))
            response = runtime.run(
                name, Request(base + "/echo"), options={"crawler.session.reuse": False}
            )[0].value
            assert response.status_code == 200


def test_curl_session_preserves_connection_across_threads(server):
    """验证串行跨线程调用仍使用同一 Slot 会话的连接。

    Args:
        server: 本地服务。
    """

    base, _, _ = server
    downloader = CurlCffiDownloader()
    session = downloader.prepare_session()
    try:
        with (
            ThreadPoolExecutor(max_workers=1) as first,
            ThreadPoolExecutor(max_workers=1) as second,
        ):
            before = (
                first.submit(session.fetch, Request(base + "/login/account"))
                .result()
                .json()
            )
            after = (
                second.submit(session.fetch, Request(base + "/echo")).result().json()
            )
        assert before["peer_port"] == after["peer_port"]
        assert after["cookie"] == "account=account"
    finally:
        downloader.close_session(session)


def test_environment_proxy_is_disabled_by_default(server, transport, monkeypatch):
    """验证环境代理不会覆盖默认直连配置。

    Args:
        server: 本地服务。
        transport: 当前下载器。
        monkeypatch: 环境修改夹具。
    """

    for key in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        monkeypatch.setenv(key, "http://127.0.0.1:1")
    monkeypatch.setenv("NO_PROXY", "")
    monkeypatch.setenv("no_proxy", "")
    assert fetch(transport, Request(server[0] + "/echo")).status_code == 200


def test_curl_configuration_errors_propagate(server):
    """验证非法浏览器指纹不会被包装成网络失败。

    Args:
        server: 本地服务。
    """

    for downloader in (
        CurlCffiDownloader(impersonate="invalid"),
        AsyncCurlCffiDownloader(impersonate="invalid"),
    ):
        with pytest.raises(ImpersonateError):
            fetch(downloader, Request(server[0]))


def test_requests_rejects_duplicate_headers(server):
    """验证 requests 不静默丢弃重复请求头。

    Args:
        server: 本地服务。
    """

    with pytest.raises(ValueError, match="duplicate"):
        RequestsDownloader().fetch(
            Request(server[0], headers=[("X-Test", "a"), ("x-test", "b")])
        )


def test_async_curl_cancellation_closes_temporary_session(server, monkeypatch):
    """验证取消传播且临时 Session 得到关闭。

    Args:
        server: 本地服务。
        monkeypatch: 临时替换会话工厂的夹具。
    """

    base, _, started = server
    sessions = []
    prepare = AsyncCurlCffiDownloader.prepare_session

    async def tracked(self, **kwargs):
        """记录本次临时会话。

        Args:
            self: 当前下载器。
            **kwargs: 原会话构造参数。

        Returns:
            新准备的会话下载器。
        """

        session = await prepare(self, **kwargs)
        sessions.append(session)
        return session

    async def run():
        """等到 HTTP 请求进入服务端后取消。"""

        task = asyncio.create_task(
            AsyncCurlCffiDownloader().fetch(Request(base + "/slow"))
        )
        try:
            for _ in range(1000):
                if started.is_set():
                    break
                await asyncio.sleep(0.001)
            assert started.is_set()
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        with pytest.raises(SessionClosed):
            await sessions[0].fetch(Request(base + "/echo"))

    monkeypatch.setattr(AsyncCurlCffiDownloader, "prepare_session", tracked)
    asyncio.run(run())


def test_default_import_does_not_load_optional_transports():
    """验证默认节点不会导入 HTTPX 或 requests。"""

    code = "from bricks import DownloadNode, AsyncDownloadNode; import sys; DownloadNode(); AsyncDownloadNode(); assert 'httpx' not in sys.modules; assert 'requests' not in sys.modules"
    subprocess.run([sys.executable, "-c", code], check=True)
