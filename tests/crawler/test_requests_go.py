"""requests-go 的真实 HTTP/HTTPS、原生会话所有权和指纹测试。"""

import importlib
import json
from concurrent.futures import ThreadPoolExecutor

import pytest
import requests_go
from requests_go.tls_client.exceptions import TLSClientExeption
from interlace import Context, Graph, Runtime, Slot

from bricks import DownloadNode, Request, UploadFile
from bricks.downloaders.requests_go import RequestsGoDownloader


@pytest.fixture(params=["server", "https_server"])
def endpoint(request):
    """分别提供 HTTP 和真正经过 Go 后端的 HTTPS 服务。

    Args:
        request: pytest 夹具请求。

    Returns:
        当前协议的服务信息。
    """

    return request.getfixturevalue(request.param)


def test_buffered_http_and_https(endpoint):
    """验证请求复制、编码上传、重复响应头、解压和重定向。

    Args:
        endpoint: 当前协议的本地服务。
    """

    base, seen, _ = endpoint
    downloader = RequestsGoDownloader(verify=False)
    request = Request(
        base + "/echo",
        method="POST",
        params={"q": "中文"},
        body={"a": 1},
        cookies={"explicit": "yes"},
    )
    original = request.copy()
    response = downloader.fetch(request)
    assert response.status_code == 200
    assert response.json()["body"] == '{"a":1}'
    assert response.json()["cookie"] == "explicit=yes"
    assert "q=" in seen[-1][1]
    assert request.headers.raw == original.headers.raw
    assert request.body == original.body
    assert request.cookies == original.cookies
    assert response.request is not request
    response = downloader.fetch(Request(base + "/cookies"))
    assert len(response.headers.get_all("Set-Cookie")) == 2
    assert response.cookies.get("a") == "1"
    assert response.cookies.get("b") == "2"
    upload = Request(
        base + "/echo",
        method="POST",
        body={"file": UploadFile("sample.txt", b"sample-content")},
        body_type="multipart",
    )
    assert downloader.fetch(upload).status_code == 200
    assert b"sample-content" in seen[-1][3]
    assert downloader.fetch(Request(base + "/gzip")).content == b"hello" * 100
    assert downloader.fetch(Request(base + "/error")).status_code == 500
    response = downloader.fetch(Request(base + "/redirect"))
    assert response.status_code == 200
    assert response.url == base + "/echo"
    assert response.json()["cookie"] == ""
    assert [item.status_code for item in response.history] == [302, 302]
    assert (
        downloader.fetch(Request(base + "/redirect", allow_redirects=False)).status_code
        == 302
    )
    assert downloader.fetch(Request(base + "/echo")).json()["cookie"] == ""


def test_go_sessions_and_context_fingerprints(https_server, monkeypatch):
    """验证 Go 调用的会话 ID 独立、指纹覆盖和完整资源关闭。

    Args:
        https_server: 本地 TLS 服务。
        monkeypatch: 记录 Go 调用参数和关闭动作。
    """

    native = importlib.import_module("requests_go.tls_client.request")
    native_request = native.request
    native_free = requests_go.sessions.freeSession
    calls = []
    freed = []

    def capture(payload):
        """记录发给真实 Go 后端的参数。

        Args:
            payload: 原生 JSON 请求字节。

        Returns:
            原生响应字节。
        """

        calls.append(json.loads(payload))
        return native_request(payload)

    def release(identifier):
        """记录并执行真正的原生会话释放。

        Args:
            identifier: ctypes 会话 ID 指针。

        Returns:
            原生释放函数的结果。
        """

        freed.append(identifier.value.decode())
        return native_free(identifier)

    monkeypatch.setattr(native, "request", capture)
    monkeypatch.setattr(requests_go.sessions, "freeSession", release)
    global_id = requests_go.tls_config.TLS_FIREFOX_LATEST.id
    downloader = RequestsGoDownloader(verify=False)
    first, second = downloader.prepare_session(), downloader.prepare_session()
    first_id, second_id = first.client.tls_config.id, second.client.tls_config.id
    slot = Slot({"downloaders": {"default": first}})
    node = DownloadNode(
        lambda current: current["downloaders"],
        isolated_downloaders={"default": downloader},
    )
    base = https_server[0]

    def download(path, options=None):
        """按当前 Context 配置发送下载请求。

        Args:
            path: 本地请求路径。
            options: 可选执行配置。

        Returns:
            节点输出的 Response。
        """

        return node.execute(
            {"request": Request(base + path)},
            Context(lambda event: None, slot, options=options),
        ).value

    try:
        download("/login/original")
        before = download("/echo")
        assert before.json()["cookie"] == "account=original"
        assert download("/echo").json()["peer_port"] == before.json()["peer_port"]
        assert second.fetch(Request(base + "/echo")).json()["cookie"] == ""
        override = download(
            "/echo", {"crawler.fingerprint.impersonate": "TLS_FIREFOX_LATEST"}
        )
        assert override.json()["cookie"] == "account=original"
        override_call = calls[-1]
        assert override_call["Id"] == first_id
        assert (
            override_call["Ja3"].split(",")[1]
            == str(requests_go.tls_config.TLS_FIREFOX_LATEST.ja3).split(",")[1]
        )
        assert first.client.tls_config.name == "TLS_CHROME_LATEST"
        isolated = download("/login/temporary", {"crawler.session.reuse": False})
        assert isolated.json()["cookie"] == ""
        assert download("/echo").json()["cookie"] == "account=original"
        assert first_id != second_id
        assert {first_id, second_id}.issubset({item["Id"] for item in calls})
        assert requests_go.tls_config.TLS_FIREFOX_LATEST.id == global_id
    finally:
        downloader.close_session(first)
        downloader.close_session(first)
        downloader.close_session(second)
    assert freed.count(first_id) == 1
    assert freed.count(second_id) == 1
    with pytest.raises(RuntimeError, match="closed"):
        first.fetch(Request(base + "/echo"))


def test_https_verification_and_native_errors(https_server):
    """拒绝原生库无法满足的证书校验要求，并保留不透明 Go 错误。

    Args:
        https_server: 使用自签名证书的 HTTPS 服务。
    """

    base, seen, _ = https_server
    with pytest.raises(ValueError, match="certificate verification"):
        RequestsGoDownloader().fetch(Request(base + "/echo"))
    assert seen == []
    with pytest.raises(requests_go.ConnectionError) as failure:
        RequestsGoDownloader(verify=False).fetch(
            Request(base + "/long-slow", timeout=1)
        )
    assert isinstance(failure.value.args[0], TLSClientExeption)


def test_configuration_errors_fail_before_network(https_server):
    """验证非法预设、重复头和无法表达的 HTTPS 超时会明确报错。

    Args:
        https_server: 本地 TLS 服务。
    """

    downloader = RequestsGoDownloader(verify=False)
    base = https_server[0]
    with pytest.raises(ValueError, match="fingerprint"):
        RequestsGoDownloader(impersonate="invalid")
    for timeout in (None, 0.1, 1.5):
        with pytest.raises(ValueError, match="whole number"):
            downloader.fetch(Request(base, timeout=timeout))
    with pytest.raises(ValueError, match="duplicate"):
        downloader.fetch(Request(base, headers=[("X-Test", "a"), ("x-test", "b")]))
    with pytest.raises(ValueError, match="Content-Length"):
        downloader.fetch(Request(base, headers={"Content-Length": "0"}))
    assert https_server[1] == []


def test_http_timeout_and_proxy(server):
    """验证 HTTP 路径保留标准 requests 的超时和代理行为。

    Args:
        server: 本地 HTTP 服务和代理响应端。
    """

    downloader = RequestsGoDownloader()
    response = downloader.fetch(Request(server[0] + "/slow", timeout=0.01))
    assert response.status_code == -1
    assert isinstance(response.error, requests_go.Timeout)
    response = downloader.fetch(
        Request("http://proxy-target.invalid/echo", proxy=server[0])
    )
    assert response.status_code == 200
    assert server[1][-1][1] == "http://proxy-target.invalid/echo"


def test_go_session_can_move_between_threads(https_server):
    """验证 Slot 串行跨线程调用保留原生会话和 Cookie。

    Args:
        https_server: 本地 TLS 服务。
    """

    downloader = RequestsGoDownloader(verify=False)
    session = downloader.prepare_session()
    base = https_server[0]
    try:
        with (
            ThreadPoolExecutor(max_workers=1) as first,
            ThreadPoolExecutor(max_workers=1) as second,
        ):
            before = first.submit(
                session.fetch, Request(base + "/login/original")
            ).result()
            after = second.submit(session.fetch, Request(base + "/echo")).result()
        assert after.json()["cookie"] == "account=original"
        assert after.json()["peer_port"] == before.json()["peer_port"]
    finally:
        downloader.close_session(session)


def test_runtime_fingerprint_entrypoint(https_server):
    """验证 Runtime 配置可以抵达真实 Go HTTPS 请求。

    Args:
        https_server: 本地 TLS 服务。
    """

    node = DownloadNode({"default": RequestsGoDownloader(verify=False)})
    with Runtime() as runtime:
        runtime.register("crawl", Graph(entrypoint="download").add(download=node))
        response = runtime.run(
            "crawl",
            Request(https_server[0]),
            options={"crawler.fingerprint.impersonate": "TLS_FIREFOX_LATEST"},
        )[0].value
        assert response.status_code == 200
