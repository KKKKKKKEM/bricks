import asyncio
import codecs
from http.cookiejar import CookieJar

import pytest
from interlace.engine.errors import ExecutionCancelledError, NodeTimeoutError

from bricks import Cookies, Request, Response


@pytest.mark.parametrize(
    "content,size",
    [
        (b"", 0),
        (b"abc", 3),
        ("\u4e2d\u6587".encode("utf-8"), 6),
        ("\U0001f600".encode("utf-8"), 4),
        (codecs.BOM_UTF16_LE + "hello".encode("utf-16-le"), 12),
        (b"\xff", 1),
    ],
)
def test_response_body_size(content, size):
    """验证响应大小按实际内容字节数计算。

    Args:
        content: 响应或上传文件的内容字节。
        size: 资源池容量或本次断言使用的预期字节数。
    """

    response = Response(content, headers={"Content-Length": "999"})
    assert response.size() == size


def test_response_body_size_follows_content_changes():
    """验证修改内容后大小更新且不受解码设置影响。"""

    response = Response("\u4e2d\u6587".encode("utf-8"))
    assert response.size() == 6
    response.encoding = "latin-1"
    assert response.size() == 6
    response.content = b"x" * 2048
    assert response.size() == 2048


def test_internal_failure_retains_exception_and_cause():
    """验证内部失败保留原异常身份和异常链。"""

    cause = OSError("connection refused")
    error = ConnectionError("proxy unavailable")
    error.__cause__ = cause
    response = Response(status_code=-1, error=error)
    assert response.reason == "proxy unavailable"
    assert response.error is error
    copied_error = response.copy().error
    assert copied_error is not None
    assert copied_error.__cause__ is cause
    assert not response.ok
    assert Response(status_code=-1, error=TimeoutError()).reason == "TimeoutError"
    assert Response(status_code=-1, error=error, reason="custom").reason == "custom"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"status_code": -1},
        {"status_code": None},
        {"status_code": 0},
        {"status_code": -2},
        {"status_code": True},
        {"status_code": 1000},
        {"status_code": 200, "error": OSError()},
        {"status_code": -1, "error": "text"},
    ],
)
def test_invalid_failure_combinations(kwargs):
    """验证响应状态与异常的非法组合被拒绝。

    Args:
        kwargs: 传给目标接口的关键字参数。
    """

    with pytest.raises((ValueError, TypeError)):
        Response(**kwargs)


@pytest.mark.parametrize(
    "error",
    [
        ExecutionCancelledError("stop"),
        NodeTimeoutError("limit"),
        asyncio.CancelledError(),
    ],
)
def test_control_errors_propagate(error):
    """验证引擎控制异常与取消信号原样传播。

    Args:
        error: 需要传播、记录或用于恢复的异常。
    """

    with pytest.raises(type(error)) as caught:
        Response(status_code=-1, error=error)
    assert caught.value is error


def test_encoding_and_content_hooks_always_refresh_decoding():
    """验证修改编码或内容后解码结果即时更新。"""

    response = Response("\u4e2d\u6587".encode("gb18030"))
    response.encoding = "gb18030"
    assert response.text == "\u4e2d\u6587"
    response.content = b'{"page":1}'
    assert response.json() == {"page": 1}
    response.content = b'{"page":2}'
    assert response.json() == {"page": 2}
    response.encoding = None
    response.content = codecs.BOM_UTF16_LE + "hello".encode("utf-16-le")
    assert response.text == "hello"
    with pytest.raises(LookupError):
        response.encoding = "not-an-encoding"
    assert response.encoding is None
    with pytest.raises(TypeError):
        response.content = "text"  # 验证运行时拒绝非字节内容。
    with pytest.raises(AttributeError):
        response.status_code = -1


def test_response_copies_request_and_history():
    """验证响应复制隔离关联请求与重定向历史。"""

    request = Request("https://example.com", headers={"X-Test": "original"})
    previous = Response(b"before", status_code=302, request=request)
    response = Response(b"after", request=request, history=[previous])
    request.headers["X-Test"] = "changed"
    previous.content = b"changed"
    assert response.url == "https://example.com"
    assert response.request is not None
    assert response.request.headers["X-Test"] == "original"
    assert response.history[0].content == b"before"
    copied = response.copy(content=b"copy")
    assert copied.request is not None
    copied.request.headers["X-Test"] = "copy"
    assert response.request.headers["X-Test"] == "original"
    assert response.content == b"after"
    assert copied.content == b"copy"


def test_cookie_strings_preserve_duplicate_names_and_attributes():
    """验证 Cookie 保留同名不同域路径的记录及属性。"""

    response = Response(
        url="https://example.com/account/login",
        headers=[
            ("Set-Cookie", "session=root; Path=/; HttpOnly; SameSite=Lax"),
            ("Set-Cookie", "session=private; Path=/account; Secure"),
            ("Set-Cookie", "shared=yes; Domain=example.com; Path=/"),
            ("Set-Cookie", "foreign=no; Domain=other.com; Path=/"),
        ],
    )
    cookies = response.cookies
    assert str(cookies) == cookies.to_string()
    assert len(cookies) == 3
    assert "session=root" in str(cookies)
    assert "session=private" in str(cookies)
    with pytest.raises(ValueError):
        cookies.get("session")
    assert cookies.get("session", path="/account") == "private"
    assert cookies.get("missing", "fallback") == "fallback"
    assert cookies.to_string(url="https://other.com/") == ""
    assert cookies.to_string(url="https://sub.example.com/account") == "shared=yes"
    # CookieJar 不保证相同长度路径之间的 Cookie 顺序。
    assert sorted(cookies.to_string(url="http://example.com/account").split("; ")) == [
        "session=root",
        "shared=yes",
    ]
    assert "session=private" not in cookies.to_string(
        url="https://example.com/accounting"
    )
    assert cookies.to_string(url="https://example.com/account").startswith(
        "session=private"
    )
    record = next(
        cookie for cookie in cookies if cookie.path == "/" and cookie.name == "session"
    )
    assert record.has_nonstandard_attr("HttpOnly")
    assert record.get_nonstandard_attr("SameSite") == "Lax"
    record.value = "modified"
    assert cookies.get("session", path="/") == "root"
    assert len(response.headers.get_all("Set-Cookie")) == 4


def test_cookie_public_suffix_domain_is_not_accepted_or_sent():
    """Domain 为公共后缀的 Cookie 不被接收，已有记录也不随其他站点请求发送。"""

    from http.cookiejar import Cookie

    response = Response(
        url="https://a.co.uk/",
        headers=[
            ("Set-Cookie", "sid=x; Domain=.co.uk; Path=/"),
            ("Set-Cookie", "ok=1; Domain=a.co.uk; Path=/"),
        ],
    )
    assert [cookie.name for cookie in response.cookies] == ["ok"]
    assert response.cookies.to_string(url="https://b.co.uk/") == ""
    assert response.cookies.to_string(url="https://www.a.co.uk/") == "ok=1"
    forged = Cookie(
        version=0,
        name="sid",
        value="x",
        port=None,
        port_specified=False,
        domain=".co.uk",
        domain_specified=True,
        domain_initial_dot=True,
        path="/",
        path_specified=True,
        secure=False,
        expires=None,
        discard=True,
        comment=None,
        comment_url=None,
        rest={},
    )
    assert Cookies([forged]).to_string(url="https://b.co.uk/") == ""


def test_cookiejar_expiry_copy_and_mapping_convenience():
    """验证 CookieJar 有效期筛选、复制和简写映射。"""

    source = Cookies({"token": "a"}, url="https://example.com")
    assert str(source) == "token=a"
    assert source.to_string(url="https://example.com") == "token=a"
    jar = CookieJar()
    record = next(iter(source))
    record.expires = 1
    jar.set_cookie(record)
    cookies = Cookies(jar)
    record.value = "changed"
    assert str(cookies) == "token=a"
    assert cookies.to_string(url="https://example.com") == ""
    assert Cookies({"token": "a"}).to_string(url="https://example.com") == ""
    assert str(Response(cookies={"token": "a"}).cookies) == "token=a"


def test_cookie_header_parsing_requires_origin_and_copy_reparses_headers():
    """验证解析 Cookie 要求来源 URL 且覆盖响应头时重新解析。"""

    with pytest.raises(ValueError):
        Response(headers={"Set-Cookie": "a=b"})
    response = Response(url="https://example.com", headers={"Set-Cookie": "a=b"})
    assert str(response.copy(headers={"Set-Cookie": "c=d"}).cookies) == "c=d"
    assert str(response.cookies) == "a=b"


@pytest.mark.parametrize(
    "status,ok", [(100, False), (200, True), (302, True), (404, False), (500, False)]
)
def test_http_statuses_are_responses(status, ok):
    """验证 HTTP 状态保留为正常响应且 ok 仅判断状态范围。

    Args:
        status: 需要记录或验证的状态值。
        ok: 当前用例使用的 ok 夹具或参数化输入。
    """

    response = Response(status_code=status)
    assert response.error is None
    assert response.ok is ok


def test_peer_response_values_are_preserved_and_copyable():
    """验证异常状态、响应头字节及 Cookie 名不会因对端数据拒绝响应。"""

    import copy
    import pickle

    response = Response(
        status_code=999,
        url="http://example.test/",
        headers=[("X-Peer", "a\x01\x7f"), ("Set-Cookie", "a[b]=1")],
    )
    assert response.headers["x-peer"] == "a\x01\x7f"
    assert response.cookies.get("a[b]") == "1"
    assert copy.copy(response).status_code == 999
    assert pickle.loads(pickle.dumps(response)).headers.raw == response.headers.raw
    assert copy.deepcopy(response).headers.raw == response.headers.raw


@pytest.mark.parametrize(
    "url", ["http://localhost/", "http://intranet:8080/", "http://[::1]:8000/"]
)
def test_cookie_mapping_domain_supports_local_hosts(url):
    """验证简写 Cookie 可按 localhost、内网和 IPv6 主机匹配。"""

    cookies = Cookies({"a": "b"}, url=url)
    assert cookies.to_string(url=url) == "a=b"


def test_get_queries_json_and_returns_default_for_missing_or_null():
    """验证 get 对 JSON 执行 JMESPath，缺失与 null 都回退默认值，假值原样返回。"""

    response = Response(b'{"code": 0, "data": {"list": [{"id": 1}]}, "none": null}')
    assert response.get("code") == 0
    assert response.get("data.list[0].id") == 1
    assert response.get("missing", "d") == "d"
    assert response.get("none", "d") == "d"


def test_extract_selects_source_by_engine():
    """验证 JSON 引擎读取解码值，标记与正则引擎读取按响应编码解码的文本。"""

    from bricks.parsers import Rows

    data = Response(b'{"list": [{"id": 1}, {"id": 2}]}')
    assert data.extract("json", Rows("list", {"uid": "id"})) == [{"uid": 1}, {"uid": 2}]
    assert data.extract("jsonpath", {"ids": "$.list[*].id"}) == [{"ids": [1, 2]}]
    page = Response(
        "<ul><li>甲</li><li>乙</li></ul>".encode("gbk"),
        headers={"Content-Type": "text/html; charset=gbk"},
    )
    assert page.extract("xpath", Rows("//li", {"t": "string(.)"})) == [
        {"t": "甲"},
        {"t": "乙"},
    ]
    assert page.extract("regex", {"items": "<li>(.*?)</li>"}) == [
        {"items": ["<li>甲</li>", "<li>乙</li>"]}
    ]
    with pytest.raises(ValueError, match="unsupported extract engine"):
        data.extract("JSON", {})


def test_single_expression_shortcuts_and_first_defaults():
    """验证 css/xpath/jsonpath/re 返回全部结果，*_first 取首项且空结果回退 default。"""

    page = Response(
        b'<ul><li><a href="/1">one</a></li><li><a href="/2">two</a></li></ul>',
        headers={"Content-Type": "text/html"},
    )
    assert page.css("a", attribute="href") == ["/1", "/2"]
    assert page.css_first("a", output="text") == "one"
    assert page.css_first("p", "none", output="text") == "none"
    assert page.xpath("//a/text()") == ["one", "two"]
    assert page.xpath("count(//li)") == 2.0
    assert page.xpath_first("//a/@href") == "/1"
    with pytest.raises(TypeError):
        page.xpath_first("count(//li)")
    assert page.re(r"/(\d)", group=1) == ["1", "2"]
    assert page.re_first(r"/(\d)") == "/1"
    assert page.re_first("missing", "d") == "d"
    data = Response(b'{"list": [{"id": 1}, {"id": 2}]}')
    assert data.jsonpath("$.list[*].id") == [1, 2]
    assert data.jsonpath_first("$.missing", 0) == 0
