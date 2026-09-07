import asyncio
import codecs
from http.cookiejar import CookieJar

import pytest

from bricks.engine.errors import ExecutionCancelledError, NodeTimeoutError
from bricks.frameworks.crawler import Cookies, Request, Response


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
    response = Response(content, headers={"Content-Length": "999"})
    assert response.size() == size


def test_response_body_size_follows_content_changes():
    response = Response("\u4e2d\u6587".encode("utf-8"))
    assert response.size() == 6
    response.encoding = "latin-1"
    assert response.size() == 6
    response.content = b"x" * 2048
    assert response.size() == 2048


def test_internal_failure_retains_exception_and_cause():
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
        {"status_code": 600},
        {"status_code": 200, "error": OSError()},
        {"status_code": -1, "error": "text"},
    ],
)
def test_invalid_failure_combinations(kwargs):
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
    with pytest.raises(type(error)) as caught:
        Response(status_code=-1, error=error)
    assert caught.value is error


def test_encoding_and_content_hooks_always_refresh_decoding():
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
        setattr(response, "content", "text")  # 验证运行时拒绝非字节内容。
    with pytest.raises(AttributeError):
        response.status_code = -1


def test_response_copies_request_and_history():
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
    # CookieJar does not guarantee ordering between equal-length paths.
    assert sorted(
        cookies.to_string(url="http://example.com/account").split("; ")
    ) == ["session=root", "shared=yes"]
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


def test_cookiejar_expiry_copy_and_mapping_convenience():
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
    with pytest.raises(ValueError):
        Response(headers={"Set-Cookie": "a=b"})
    response = Response(url="https://example.com", headers={"Set-Cookie": "a=b"})
    assert str(response.copy(headers={"Set-Cookie": "c=d"}).cookies) == "c=d"
    assert str(response.cookies) == "a=b"


@pytest.mark.parametrize(
    "status,ok", [(100, False), (200, True), (302, True), (404, False), (500, False)]
)
def test_http_statuses_are_responses(status, ok):
    response = Response(status_code=status)
    assert response.error is None
    assert response.ok is ok
