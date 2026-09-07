"""真实 Chromium 与 Camoufox 的双模式、Tab 复用及代理隔离测试。"""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest
from playwright.sync_api import Error as SyncError
from interlace import Context, Graph, Runtime, Slot

from bricks import AsyncDownloadNode, DownloadNode, Request, UploadFile
from bricks.downloaders.playwright import (
    AsyncPlaywrightDownloader,
    PlaywrightDownloader,
)
from bricks.downloaders.camoufox import AsyncCamoufoxDownloader, CamoufoxDownloader

_SYNC = [PlaywrightDownloader, CamoufoxDownloader]  # 两个真实同步浏览器后端。
_ASYNC = [AsyncPlaywrightDownloader, AsyncCamoufoxDownloader]  # 同样的原生异步后端。


@pytest.fixture
def proxies():
    """提供两个可区分的本地代理，支持 HTTP 和明文 CONNECT 隧道。

    Yields:
        代理 URL 列表；目标使用测试域名，不依赖外网。
    """

    servers = []
    threads = []

    def start(label):
        """启动一个返回自身标签的测试代理。

        Args:
            label: 当前代理的唯一标签。

        Returns:
            本地代理 URL。
        """

        class Handler(BaseHTTPRequestHandler):
            """在代理端模拟目标 HTTP 服务，防止测试访问真实外网。"""

            protocol_version = "HTTP/1.1"  # 保留连接以验证池复用。

            def do_CONNECT(self):
                """接受 HTTP 客户端的测试隧道；后续 HTTP 请求由同一处理器响应。"""

                self.send_response(200)
                self.end_headers()
                self.close_connection = False

            def do_GET(self):
                """返回当前代理标记和原请求信息。"""

                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                data = json.dumps(
                    {"proxy": label, "method": self.command, "body": body.decode()}
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_POST = do_GET

            def log_message(self, *args):
                """屏蔽代理访问日志。

                Args:
                    *args: 原生日志参数。
                """

                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        servers.append(server)
        threads.append(thread)
        thread.start()
        return f"http://127.0.0.1:{server.server_port}"

    try:
        yield [start("first"), start("second")]
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join()


@pytest.mark.parametrize("factory", _SYNC)
def test_sync_modes_share_cookies_and_reuse_tab(server, factory):
    """验证 API 登录后页面可见 Cookie，并且多次导航不创建额外 Tab。

    Args:
        server: 本地目标服务。
        factory: 当前同步下载器类型。
    """

    base = server[0]
    downloader = factory()
    session = downloader.prepare_session()
    browser = session.context.browser
    try:
        session.fetch_api(Request(base + "/login/original"))
        assert session.context.pages == []
        rendered = session.fetch_page(Request(base + "/browser-page"))
        page = session.context.pages[0]
        assert b">rendered</h1>" in rendered.content
        assert "account=original" in rendered.text
        assert rendered.headers.get("Content-Length") is None
        page.evaluate("window.name = 'retained-tab'")
        slot = Slot({"downloaders": {"default": session}})
        node = DownloadNode(lambda current: current["downloaders"])
        node.execute(
            {"request": Request(base + "/browser-page")},
            Context(lambda event: None, slot),
        )
        assert session.context.pages == [page]
        assert page.evaluate("window.name") == "retained-tab"
        request = Request(base + "/echo", method="PUT", body={"x": 1})
        original = request.copy()
        response = session.fetch_api(request)
        assert response.json()["body"] == '{"x":1}'
        assert "account=original" in response.json()["cookie"]
        assert request.body == original.body
        assert request.headers.raw == original.headers.raw
        assert session.context.pages == [page]
        response = session.fetch_api(Request(base + "/cookies"))
        assert len(response.headers.get_all("Set-Cookie")) == 2
        assert (
            session.fetch_api(
                Request(base + "/redirect", allow_redirects=False)
            ).status_code
            == 302
        )
        assert session.fetch_api(Request(base + "/error")).status_code == 500
        upload = Request(
            base + "/echo",
            method="POST",
            body={"file": UploadFile("test.txt", b"browser-upload")},
            body_type="multipart",
        )
        assert "browser-upload" in session.fetch_api(upload).json()["body"]
        with pytest.raises(ValueError, match="proxy is fixed"):
            session.fetch(Request(base, proxy="http://127.0.0.1:1"))
    finally:
        downloader.close_session(session)
        downloader.close_session(session)
    assert not browser.is_connected()
    with pytest.raises(RuntimeError, match="closed"):
        session.fetch(Request(base))


@pytest.mark.parametrize("factory", _ASYNC)
def test_async_modes_share_cookies_and_reuse_tab(server, factory):
    """验证原生异步模式同样复用 Tab，并允许 API 与页面共享 Cookie。

    Args:
        server: 本地服务。
        factory: 当前异步后端。
    """

    async def run():
        """在同一个事件循环内完成会话生命周期。"""

        async def callback(page):
            """在真实页面上执行异步自定义操作。

            Args:
                page: 当前复用的 Tab。
            """

            await page.evaluate("document.body.dataset.hook = 'executed'")

        downloader = factory(content_mode="response", after_load=callback)
        session = await downloader.prepare_session()
        browser = session.context.browser
        base = server[0]
        try:
            await session.fetch_api(Request(base + "/login/original"))
            response = await session.fetch_page(Request(base + "/browser-page"))
            assert b">initial</h1>" in response.content
            page = session.context.pages[0]
            assert await page.locator("#result").inner_text() == "rendered"
            assert await page.locator("body").get_attribute("data-hook") == "executed"
            assert (
                await page.locator("body").get_attribute("data-cookie")
                == "account=original"
            )
            await session.fetch_page(Request(base + "/echo"))
            assert session.context.pages == [page]
            result = await session.fetch_api(
                Request(
                    base + "/echo", method="POST", body=b"api-body", body_type="raw"
                )
            )
            assert result.json()["body"] == "api-body"
            assert result.json()["cookie"] == "account=original"
        finally:
            await downloader.close_session(session)
            await downloader.close_session(session)
        assert not browser.is_connected()

    asyncio.run(run())


@pytest.mark.parametrize("factory", _SYNC)
def test_sync_single_browser_tabs_have_independent_proxies(proxies, factory):
    """验证同一浏览器中不同 Context 的 Tab 和 API 分别使用独立代理。

    Args:
        proxies: 两个本地代理地址。
        factory: 当前同步后端。
    """

    owner = factory()
    root = owner.prepare_session()
    browser = root.context.browser
    downloader = factory(browser=browser, content_mode="response")
    sessions = []
    try:
        sessions = [downloader.prepare_session(proxy=proxy) for proxy in proxies]
        for session, label in zip(sessions, ("first", "second")):
            assert (
                session.fetch_page(Request("http://target.invalid/page")).json()[
                    "proxy"
                ]
                == label
            )
            page = session.context.pages[0]
            assert (
                session.fetch_api(
                    Request(
                        "http://target.invalid/api",
                        method="POST",
                        body=b"through-proxy",
                        body_type="raw",
                    )
                ).json()["proxy"]
                == label
            )
            assert (
                session.fetch_page(Request("http://target.invalid/again")).json()[
                    "proxy"
                ]
                == label
            )
            assert session.context.pages == [page]
        assert sessions[0].context != sessions[1].context
        for session in sessions:
            downloader.close_session(session)
        assert browser.is_connected()
    finally:
        for session in sessions:
            downloader.close_session(session)
        owner.close_session(root)


@pytest.mark.parametrize("factory", _ASYNC)
def test_async_single_browser_tabs_have_independent_proxies(proxies, factory):
    """验证异步模式的两个 Context 在同一浏览器中隔离代理。

    Args:
        proxies: 两个本地测试代理。
        factory: 当前异步后端。
    """

    async def run():
        """并列装配两份会话，每份保留自己的 Tab。"""

        owner = factory()
        root = await owner.prepare_session()
        browser = root.context.browser
        downloader = factory(browser=browser, content_mode="response")
        sessions = []
        try:
            for proxy in proxies:
                sessions.append(await downloader.prepare_session(proxy=proxy))
            results = await asyncio.gather(
                *(
                    session.fetch_page(Request("http://target.invalid/page"))
                    for session in sessions
                )
            )
            assert [response.json()["proxy"] for response in results] == [
                "first",
                "second",
            ]
            results = await asyncio.gather(
                *(
                    session.fetch_api(Request("http://target.invalid/api"))
                    for session in sessions
                )
            )
            assert [response.json()["proxy"] for response in results] == [
                "first",
                "second",
            ]
            for session in sessions:
                await downloader.close_session(session)
            assert browser.is_connected()
        finally:
            for session in sessions:
                await downloader.close_session(session)
            await owner.close_session(root)

    asyncio.run(run())


@pytest.mark.parametrize("factory", _SYNC)
def test_browser_callback_errors_propagate_and_close_resources(server, factory):
    """验证用户回调异常原样传播并清理 Tab、Context 和自建浏览器。

    Args:
        server: 本地页面服务。
        factory: 同步后端。
    """

    failure = ValueError("callback failure")
    captured = []

    def callback(page):
        """记录资源并触发测试异常。

        Args:
            page: 已完成导航的 Tab。

        Raises:
            ValueError: 固定的用户回调失败。
        """

        captured.append(page)
        raise failure

    with pytest.raises(ValueError) as caught:
        factory(after_load=callback).fetch(Request(server[0] + "/browser-page"))
    assert caught.value is failure
    assert captured[0].is_closed()
    assert not captured[0].context.browser.is_connected()


@pytest.mark.parametrize("factory", _ASYNC)
@pytest.mark.parametrize("mode", ["page", "api"])
def test_async_cancel_closes_session_and_rejects_overlap(server, factory, mode):
    """验证取消后关闭会话，防止残留请求继续更新 Cookie 或页面。

    Args:
        server: 本地慢服务。
        factory: 异步浏览器后端。
        mode: 页面或 API 模式。
    """

    async def run():
        """等到服务器收到请求后检查重叠调用并取消。"""

        downloader = factory(mode=mode)
        session = await downloader.prepare_session()
        browser = session.context.browser
        task = asyncio.create_task(session.fetch(Request(server[0] + "/long-slow")))
        try:
            for _ in range(2000):
                if server[2].is_set():
                    break
                await asyncio.sleep(0.001)
            assert server[2].is_set()
            with pytest.raises(RuntimeError, match="already executing"):
                await session.fetch(Request(server[0]))
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not browser.is_connected()
            with pytest.raises(RuntimeError, match="closed"):
                await session.fetch(Request(server[0]))
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await downloader.close_session(session)

    asyncio.run(run())


def test_sync_thread_affinity_and_page_validation(server):
    """验证同步会话拒绝跨线程，页面模式不静默丢弃 POST 或重定向设置。

    Args:
        server: 本地服务。
    """

    downloader = PlaywrightDownloader()
    session = downloader.prepare_session()
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            with pytest.raises(RuntimeError, match="creating thread"):
                executor.submit(session.fetch, Request(server[0])).result()
        with pytest.raises(ValueError, match="api mode"):
            session.fetch_page(Request(server[0], method="POST", body="x"))
        with pytest.raises(ValueError, match="redirects"):
            session.fetch_page(Request(server[0], allow_redirects=False))
        assert session.fetch_api(Request(server[0])).status_code == 200
    finally:
        downloader.close_session(session)


def test_node_runtime_supports_both_browser_adapters(server):
    """验证现有 DownloadNode 和 AsyncDownloadNode 可直接使用新下载器。

    Args:
        server: 本地页面服务。
    """

    with Runtime() as runtime:
        runtime.register(
            "page",
            Graph(entrypoint="download").add(
                download=DownloadNode({"default": PlaywrightDownloader()})
            ),
        )
        runtime.register(
            "api",
            Graph(entrypoint="download").add(
                download=AsyncDownloadNode(
                    {"default": AsyncCamoufoxDownloader(mode="api")}
                )
            ),
        )
        response = runtime.run("page", Request(server[0] + "/browser-page"))[0].value
        assert b">rendered</h1>" in response.content
        response = runtime.run(
            "api", Request(server[0] + "/echo", method="POST", body={"runtime": True})
        )[0].value
        assert response.json()["body"] == '{"runtime":true}'


@pytest.mark.parametrize("factory", _SYNC)
def test_https_certificate_verification_is_enabled(https_server, factory):
    """验证两种模式默认拒绝不受信任的 HTTPS 证书。

    Args:
        https_server: 自签名证书服务。
        factory: 同步浏览器后端。
    """

    for mode in ("page", "api"):
        with pytest.raises(SyncError):
            factory(mode=mode).fetch(Request(https_server[0]))
        assert (
            factory(mode=mode, verify=False).fetch(Request(https_server[0])).status_code
            == 200
        )


@pytest.mark.parametrize("factory", _ASYNC)
def test_cancel_during_borrowed_context_creation_does_not_leak(factory, monkeypatch):
    """验证 Context 创建期间取消不会泄漏到调用方拥有的 Browser。

    Args:
        factory: 异步浏览器后端。
        monkeypatch: 延迟创建结果交付的夹具。
    """

    async def run():
        """保持原生创建完成但尚未返回的窗口，再请求取消。"""

        owner = factory()
        root = await owner.prepare_session()
        browser = root.context.browser
        original = browser.new_context
        created = asyncio.Event()
        release = asyncio.Event()

        async def delayed(**kwargs):
            """延迟向装配器交付已创建的 Context。

            Args:
                **kwargs: 原生 Context 参数。

            Returns:
                已创建的 BrowserContext。
            """

            context = await original(**kwargs)
            created.set()
            await release.wait()
            return context

        monkeypatch.setattr(browser, "new_context", delayed)
        task = asyncio.create_task(factory(browser=browser).prepare_session())
        try:
            await asyncio.wait_for(created.wait(), 5)
            task.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert browser.contexts == [root.context]
            assert browser.is_connected()
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            await owner.close_session(root)

    asyncio.run(run())


@pytest.mark.parametrize("factory", _SYNC)
def test_navigation_timeout_closes_unfinished_tab(server, factory):
    """验证导航超时返回失败并关闭仍可能运行脚本的会话。

    Args:
        server: 本地慢服务。
        factory: 同步浏览器后端。
    """

    downloader = factory()
    session = downloader.prepare_session()
    browser = session.context.browser
    try:
        response = session.fetch_page(Request(server[0] + "/long-slow", timeout=0.02))
        assert response.status_code == -1
        assert not browser.is_connected()
        with pytest.raises(RuntimeError, match="closed"):
            session.fetch_api(Request(server[0]))
    finally:
        downloader.close_session(session)
