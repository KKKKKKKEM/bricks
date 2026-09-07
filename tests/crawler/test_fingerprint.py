"""Context 指纹覆盖的传递、隔离与错误契约。"""

import asyncio
import inspect

import pytest
from curl_cffi import requests as curl_requests
from curl_cffi.requests.exceptions import ImpersonateError
from interlace import Context, Graph, Runtime, Slot

from bricks import AsyncDownloadNode, DownloadNode, Request
from bricks.downloaders import AsyncFingerprintDownloader, FingerprintDownloader
from bricks.downloaders.curl_cffi import AsyncCurlCffiDownloader, CurlCffiDownloader
from bricks.downloaders.httpx import AsyncHttpxDownloader, HttpxDownloader


def invoke(node, request, options, slot=None):
    """直接调用节点以检查 Context 覆盖行为。

    Args:
        node: 同步或异步下载节点。
        request: 本次请求。
        options: 本次 Context 配置。
        slot: 可选会话资源槽。

    Returns:
        当前调用的领域响应。
    """

    result = node.execute(
        {"request": request}, Context(lambda event: None, slot, options=options)
    )
    output = asyncio.run(result) if inspect.isawaitable(result) else result
    return output.value


@pytest.mark.parametrize("asynchronous", [False, True])
def test_runtime_fingerprints_are_isolated_and_keep_other_settings(
    server, monkeypatch, asynchronous
):
    """验证并发执行各用独立指纹配置，并保留其他传输设置。

    Args:
        server: 本地 HTTP 服务。
        monkeypatch: 记录实际传输会话的夹具。
        asynchronous: 是否验证异步实现。
    """

    calls = []
    client_type = curl_requests.AsyncSession if asynchronous else curl_requests.Session
    original = client_type.request

    def record(client, kwargs):
        """记录传给真实传输库的会话参数。

        Args:
            client: 本次请求的 curl_cffi 会话。
            kwargs: 实际 HTTP 请求参数。
        """

        calls.append(
            (
                kwargs["url"],
                kwargs["impersonate"],
                client.impersonate,
                client.verify,
                client.max_redirects,
            )
        )

    def sync_request(self, **kwargs):
        """记录同步请求并执行真实 HTTP 传输。

        Args:
            self: 当前会话。
            **kwargs: 原请求参数。

        Returns:
            真实传输响应。
        """

        record(self, kwargs)
        return original(self, **kwargs)

    async def async_request(self, **kwargs):
        """记录异步请求并执行真实 HTTP 传输。

        Args:
            self: 当前会话。
            **kwargs: 原请求参数。

        Returns:
            真实传输响应。
        """

        record(self, kwargs)
        return await original(self, **kwargs)

    monkeypatch.setattr(
        client_type, "request", async_request if asynchronous else sync_request
    )
    downloader_type = AsyncCurlCffiDownloader if asynchronous else CurlCffiDownloader
    downloader = downloader_type(impersonate="chrome", verify=False, max_redirects=3)
    assert isinstance(
        downloader,
        AsyncFingerprintDownloader if asynchronous else FingerprintDownloader,
    )
    node_type = AsyncDownloadNode if asynchronous else DownloadNode
    node = node_type(
        {"default": downloader}, isolated_downloaders={"default": downloader}
    )
    base = server[0]
    with Runtime() as runtime:
        runtime.register("crawl", Graph(entrypoint="download").add(download=node))
        executions = [
            runtime.start(
                "crawl",
                Request(base + path),
                options={
                    "crawler.fingerprint.impersonate": fingerprint,
                },
            )
            for path, fingerprint in (
                ("/chrome", "chrome"),
                ("/safari", "safari"),
                ("/inherit", None),
            )
        ]
        for execution in executions:
            assert execution.result()[0].value.status_code == 200
        assert (
            runtime.run("crawl", Request(base + "/default"))[0].value.status_code == 200
        )
    assert sorted(calls) == sorted(
        [
            (base + "/chrome", "chrome", "chrome", False, 3),
            (base + "/safari", "safari", "chrome", False, 3),
            (base + "/inherit", None, "chrome", False, 3),
            (base + "/default", None, "chrome", False, 3),
        ]
    )
    assert downloader.impersonate == "chrome"


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("reuse", [None, True, False])
def test_fingerprint_override_keeps_slot_session(server, asynchronous, reuse):
    """验证指纹不会改变显式或缺省的会话复用策略。

    Args:
        server: 本地 HTTP 服务。
        asynchronous: 是否验证异步实现。
        reuse: None 省略配置，True 复用会话，False 使用临时会话。
    """

    async def resolve(value):
        """取得同步或异步调用的结果。

        Args:
            value: 当前调用结果。

        Returns:
            已完成的结果。
        """

        return await value if inspect.isawaitable(value) else value

    async def run():
        """在同一事件循环内验证会话保留与临时请求。"""

        downloader_type = (
            AsyncCurlCffiDownloader if asynchronous else CurlCffiDownloader
        )
        downloader = downloader_type(impersonate="chrome")
        session = await resolve(downloader.prepare_session())
        slot = Slot({"downloaders": {"default": session}})
        node_type = AsyncDownloadNode if asynchronous else DownloadNode
        node = node_type(
            lambda slot: slot["downloaders"],
            isolated_downloaders={"default": downloader},
        )
        base = server[0]
        try:
            await resolve(session.fetch(Request(base + "/login/original")))
            request = Request(base + "/login/temporary")
            options = {"crawler.fingerprint.impersonate": "safari"}
            if reuse is not None:
                options["crawler.session.reuse"] = reuse
            output = await resolve(
                node.execute(
                    {"request": request},
                    Context(
                        lambda event: None,
                        slot,
                        options=options,
                    ),
                )
            )
            assert output.value.json()["cookie"] == (
                "" if reuse is False else "account=original"
            )
            assert request.cookies == {}
            after = await resolve(session.fetch(Request(base + "/echo")))
            assert after.json()["cookie"] == (
                "account=original" if reuse is False else "account=temporary"
            )
            assert session.client.impersonate == "chrome"
        finally:
            await resolve(downloader.close_session(session))

    asyncio.run(run())


@pytest.mark.parametrize("node_type", [DownloadNode, AsyncDownloadNode])
@pytest.mark.parametrize("fingerprint", [True, 1, {}, "", " chrome", "chrome "])
def test_invalid_fingerprint_fails_before_http(server, node_type, fingerprint):
    """验证非法指纹值不发出网络请求。

    Args:
        server: 本地 HTTP 服务。
        node_type: 同步或异步节点。
        fingerprint: 非法指纹值。
    """

    with pytest.raises((TypeError, ValueError)):
        invoke(
            node_type(),
            Request(server[0]),
            {
                "crawler.session.reuse": False,
                "crawler.fingerprint.impersonate": fingerprint,
            },
        )
    assert server[1] == []


@pytest.mark.parametrize("node_type", [DownloadNode, AsyncDownloadNode])
def test_fingerprint_accepts_explicit_reuse(server, node_type):
    """验证显式复用与请求指纹可同时设置。

    Args:
        server: 本地 HTTP 服务。
        node_type: 同步或异步节点。
    """

    for options in (
        {"crawler.fingerprint.impersonate": "chrome", "crawler.session.reuse": True},
        {"crawler.fingerprint.impersonate": None, "crawler.session.reuse": True},
    ):
        assert invoke(node_type(), Request(server[0]), options).status_code == 200


@pytest.mark.parametrize(
    "node_type,downloader_type",
    [(DownloadNode, HttpxDownloader), (AsyncDownloadNode, AsyncHttpxDownloader)],
)
def test_unsupported_selected_downloader_is_rejected(
    server, node_type, downloader_type
):
    """验证指纹能力在选择后检查，不静默忽略 HTTPX 的不支持行为。

    Args:
        server: 本地 HTTP 服务。
        node_type: 同步或异步节点。
        downloader_type: 不支持指纹的下载器类型。
    """

    node = node_type(isolated_downloaders={"default": downloader_type()})
    with pytest.raises(TypeError, match="does not support .*fingerprint"):
        invoke(
            node,
            Request(server[0]),
            {
                "crawler.session.reuse": False,
                "crawler.fingerprint.impersonate": "chrome",
            },
        )
    assert server[1] == []


@pytest.mark.parametrize("node_type", [DownloadNode, AsyncDownloadNode])
def test_unknown_fingerprint_remains_configuration_error(server, node_type):
    """验证传输库拒绝未知指纹时异常继续传播。

    Args:
        server: 本地 HTTP 服务。
        node_type: 同步或异步节点。
    """

    with pytest.raises(ImpersonateError):
        invoke(
            node_type(),
            Request(server[0]),
            {
                "crawler.session.reuse": False,
                "crawler.fingerprint.impersonate": "invalid",
            },
        )
    assert server[1] == []
