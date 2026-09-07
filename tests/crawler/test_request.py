from email import policy
from email.parser import BytesParser
from typing import Any, cast

import pytest

from bricks import Request, Response, UploadFile


def test_before_request_hook_can_edit_request():
    """验证请求前 Hook 可以编辑请求数据。"""

    request = Request("https://example.com/path?existing=1#section")
    request.headers["Authorization"] = "token"
    request.headers["authorization"] = "new-token"
    request.params["page"] = 2
    request.params.add("tag", "a")
    request.params.add("tag", "b")
    request.cookies["session"] = "abc"
    request.body = "payload"
    request.method = "POST"
    request.timeout = None

    assert request.headers.get_all("AUTHORIZATION") == ("new-token",)
    assert request.params.get_all("tag") == ("a", "b")
    assert request.real_url == "https://example.com/path?existing=1&page=2&tag=a&tag=b"
    assert request.body == b"payload"
    assert request.method == "POST"
    assert request.cookies["session"] == "abc"
    assert request.timeout is None


def test_copy_isolates_mutable_containers_and_preserves_repeated_fields():
    """验证复制隔离可变容器且保留重复字段。"""

    request = Request(
        "https://example.com",
        params=[("tag", "a"), ("tag", "b")],
        headers=[("X-Test", "a"), ("x-test", "b")],
        cookies={"session": "original"},
    )
    copied = request.copy(method="post")
    assert copied.params.raw == request.params.raw
    assert copied.headers.raw == request.headers.raw
    copied.params["tag"] = ["c", "d"]
    copied.headers["x-test"] = "c"
    copied.cookies["session"] = "changed"
    assert request.params.get_all("tag") == ("a", "b")
    assert request.headers.get_all("x-test") == ("a", "b")
    assert request.cookies == {"session": "original"}
    assert copied.method == "POST"


def test_container_editing_and_whole_field_assignment():
    """验证容器原地编辑与整体字段赋值。"""

    request = Request("https://example.com")
    setattr(request, "params", {"tag": ["a", "b"]})
    setattr(request, "headers", {"X-Test": "one"})
    request.headers.add("x-test", "two")
    assert request.headers.get_all("X-Test") == ("one", "two")
    request.params.update({"page": "2"})
    del request.params["tag"]
    del request.headers["X-TEST"]
    assert request.params.raw == (("page", "2"),)
    assert not request.headers
    with pytest.raises(KeyError):
        del request.params["missing"]
    with pytest.raises(KeyError):
        del request.headers["missing"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("url", "relative"),
        ("method", "bad method"),
        ("method", "UNKNOWN"),
        ("timeout", -1),
        ("body", object()),
        ("allow_redirects", 1),
        ("proxy", ""),
    ],
)
def test_invalid_assignment_preserves_previous_value(field, value):
    """验证非法字段赋值保留原值。

    Args:
        field: 当前用例使用的 field 夹具或参数化输入。
        value: 当前操作处理的输入值。
    """

    request = Request("https://example.com")
    previous = getattr(request, field)
    with pytest.raises((TypeError, ValueError)):
        setattr(request, field, value)
    assert getattr(request, field) == previous


def test_invalid_container_edits_are_atomic():
    """验证非法容器修改不会部分生效。"""

    request = Request(
        "https://example.com", headers={"X-Test": "valid"}, params={"p": 1}
    )
    with pytest.raises(ValueError):
        request.headers["x-test"] = "bad\r\nvalue"
    with pytest.raises(TypeError):
        request.params["p"] = cast(Any, object())  # 故意绕过静态类型，验证运行时校验。
    assert request.headers["x-test"] == "valid"
    assert request.params["p"] == "1"


def test_body_copy_overrides_replace_content_headers():
    """验证覆盖请求体时更新关联请求头。"""

    request = Request("https://example.com", headers={"Content-Length": "100"})
    encoded = request.copy(body={"value": 1}, body_type="json")
    form = request.copy(body=[("tag", "a"), ("tag", "b")], body_type="form")
    assert encoded.body == b'{"value":1}'
    assert encoded.headers["content-type"] == "application/json"
    assert "content-length" not in encoded.headers
    assert form.body == b"tag=a&tag=b"
    assert request.body is None
    assert request.headers["content-length"] == "100"


@pytest.mark.parametrize("changes", [{"body": b"abcdef"}, {"body_type": "json"}])
def test_copy_clears_length_from_explicit_headers(changes):
    """验证同时覆盖请求头和请求体时清理旧长度。

    Args:
        changes: 复制对象时需要覆盖的字段。
    """

    request = Request(
        "https://example.com", body="abc", headers={"Content-Length": "3"}
    )
    copied = request.copy(headers=request.headers, **changes)
    assert "Content-Length" not in copied.headers
    assert request.headers["Content-Length"] == "3"


def test_copy_tracks_new_generated_content_type():
    """验证复制后继续跟踪新生成的内容类型。"""

    request = Request(
        "https://example.com",
        body={"a": 1},
        headers={"Content-Type": "application/custom"},
    )
    copied = request.copy(headers={})
    assert copied.headers["Content-Type"] == "application/json"
    copied.body = b"raw"
    assert "Content-Type" not in copied.headers
    assert request.headers["Content-Type"] == "application/custom"


def test_copy_keeps_explicit_content_type_ownership():
    """验证复制不会将显式内容类型误当成自动生成值。"""

    request = Request("https://example.com", body={"a": 1})
    copied = request.copy(headers={"Content-Type": "application/json"})
    copied.body = b"raw"
    assert copied.headers["Content-Type"] == "application/json"


@pytest.mark.parametrize(
    "value",
    [
        "abc; admin=yes",
        "a,b",
        'a"b',
        "a\\b",
        "a b",
        "\t",
        "\r\n",
        "\x00",
        "\x7f",
        "\u4e2d",
    ],
)
def test_request_cookie_values_reject_unsafe_serialization(value):
    """验证请求 Cookie 拒绝会改变序列化结构的字符。

    Args:
        value: 当前操作处理的输入值。
    """

    with pytest.raises(ValueError):
        Request("https://example.com", cookies={"session": value})
    request = Request("https://example.com", cookies={"session": "valid"})
    with pytest.raises(ValueError):
        request.cookies["session"] = value
    assert request.cookies == {"session": "valid"}


def test_request_cookie_mutations_are_validated():
    """验证请求 Cookie 的可变操作执行校验。"""

    request = Request("https://example.com", cookies={"session": "abc=="})
    with pytest.raises(ValueError):
        request.cookies["bad name"] = "value"
    with pytest.raises(TypeError):
        request.cookies["session"] = cast(Any, 1)
    with pytest.raises(ValueError):
        request.cookies.update({"session": "bad; other=yes"})
    request.cookies.setdefault("empty", "")
    assert request.cookies == {"session": "abc==", "empty": ""}
    restored = Request.from_curl(request.to_curl())
    assert restored.cookies == request.cookies
    del request.cookies["empty"]
    assert request.cookies == {"session": "abc=="}


def test_response_headers_remain_read_only_and_detached():
    """验证响应头只读且与来源容器隔离。"""

    request = Request("https://example.com", headers={"X-Test": "original"})
    response = Response(headers=request.headers)
    request.headers["X-Test"] = "changed"
    assert response.headers["x-test"] == "original"
    with pytest.raises(TypeError):
        cast(Any, response.headers)["x-test"] = "other"  # 验证只读容器拒绝写入。


@pytest.mark.parametrize(
    "body,expected",
    [
        ({"page": 1}, b'{"page":1}'),
        ({}, b"{}"),
        ("\u4e2d\u6587", "\u4e2d\u6587".encode("utf-8")),
        (b"raw", b"raw"),
        (None, None),
    ],
)
def test_body_constructor_and_assignment(body, expected):
    """验证构造与后续赋值使用一致的请求体编码。

    Args:
        body: 待编码的请求体数据。
        expected: 当前用例使用的 expected 夹具或参数化输入。
    """

    request = Request("https://example.com", body=body)
    assert request.body == expected
    assert request.copy().body == expected
    request.body = body
    assert request.body == expected
    if isinstance(body, dict):
        assert request.headers["content-type"] == "application/json"


def test_json_body_is_encoded_independently_and_preserves_explicit_content_type():
    """验证 JSON 编码隔离输入且保留显式内容类型。"""

    data = {"nested": [1]}
    request = Request(
        "https://example.com",
        body=data,
        headers={"Content-Type": "application/vnd.example+json"},
    )
    data["nested"].append(2)
    assert request.body == b'{"nested":[1]}'
    assert request.headers["content-type"] == "application/vnd.example+json"
    request.headers["Content-Length"] = "999"
    request.body = {"new": True}
    assert "content-length" not in request.headers


@pytest.mark.parametrize("body", [{"bad": object()}, {"bad": float("nan")}])
def test_invalid_json_assignment_preserves_body_and_headers(body):
    """验证非法 JSON 赋值保留原请求体和请求头。

    Args:
        body: 待编码的请求体数据。
    """

    request = Request(
        "https://example.com", body=b"old", headers={"Content-Length": "3"}
    )
    with pytest.raises((TypeError, ValueError)):
        request.body = body
    assert request.body == b"old"
    assert request.headers.raw == (("Content-Length", "3"),)


def test_form_body_constructor_and_subsequent_assignment():
    """验证表单构造与后续赋值的一致性。"""

    request = Request(
        "https://example.com",
        method="POST",
        body_type="form",
        body={"name": "a b", "tag": ["a", "b"], "skip": None},
    )
    assert request.body == b"name=a+b&tag=a&tag=b"
    assert request.headers["content-type"] == "application/x-www-form-urlencoded"
    request.body = {"page": 2}
    assert request.body == b"page=2"
    assert request.body_type == "form"


def test_body_type_change_reencodes_snapshot_and_copy():
    """验证切换请求体类型重新编码独立数据快照。"""

    source = {"page": 1}
    request = Request("https://example.com", body=source)
    source["page"] = 99
    request.headers["Content-Length"] = "999"
    copied = request.copy(body_type="form")
    assert copied.body == b"page=1"
    assert "content-length" not in copied.headers
    assert copied.headers["content-type"] == "application/x-www-form-urlencoded"
    assert request.body == b'{"page":1}'
    assert request.headers["content-length"] == "999"
    request.body_type = "form"
    assert request.body == b"page=1"
    assert "content-length" not in request.headers
    request.body_type = "json"
    assert request.body == b'{"page":1}'
    assert request.headers["content-type"] == "application/json"
    request.body_type = "auto"
    request.body = b"raw"
    assert "content-type" not in request.headers


@pytest.mark.parametrize("body_type", ["", "xml", None, 1])
def test_invalid_body_type_is_rejected_atomically(body_type):
    """验证非法编码模式不会改变已有请求。

    Args:
        body_type: 请求体编码模式。
    """

    with pytest.raises(ValueError):
        Request("https://example.com", body_type=body_type)
    request = Request("https://example.com", body={"p": 1})
    with pytest.raises(ValueError):
        request.body_type = body_type
    assert request.body_type == "auto"
    assert request.body == b'{"p":1}'


def test_invalid_form_conversion_preserves_request():
    """验证非法表单转换保留原请求。"""

    request = Request("https://example.com", body={"nested": {"p": 1}})
    before = request.body, request.headers.raw, request.body_type
    with pytest.raises(TypeError):
        request.body_type = "form"
    assert (request.body, request.headers.raw, request.body_type) == before


@pytest.mark.parametrize("body", ["p=1", b"p=1"])
def test_encoded_form_body_is_not_encoded_again(body):
    """验证已编码表单不会再次编码。

    Args:
        body: 待编码的请求体数据。
    """

    request = Request("https://example.com", body_type="form", body=body)
    assert request.body == b"p=1"
    assert request.copy().body == b"p=1"


def test_explicit_content_type_survives_mode_change():
    """验证显式内容类型在编码模式切换后保留。"""

    request = Request(
        "https://example.com",
        body={"p": 1},
        headers={"Content-Type": "application/custom"},
    )
    request.body_type = "form"
    assert request.body == b"p=1"
    assert request.headers["content-type"] == "application/custom"
    assert request.copy().headers["content-type"] == "application/custom"


@pytest.mark.parametrize(
    "value,expected",
    [
        ([1, {"a": True}], b'[1,{"a":true}]'),
        ("hello", b'"hello"'),
        (42, b"42"),
        (False, b"false"),
    ],
)
def test_explicit_json_serializes_values(value, expected):
    """验证显式 JSON 模式序列化支持的值。

    Args:
        value: 当前操作处理的输入值。
        expected: 当前用例使用的 expected 夹具或参数化输入。
    """

    request = Request("https://example.com", body=value, body_type="json")
    assert request.body == expected
    assert request.copy().body == expected
    assert request.headers["content-type"] == "application/json"


def test_auto_json_array_and_raw_text():
    """验证自动 JSON 数组编码与原始文本行为。"""

    assert Request("https://example.com", body=[1, 2]).body == b"[1,2]"
    request = Request(
        "https://example.com",
        body="<root/>",
        body_type="raw",
        headers={"Content-Type": "application/xml"},
    )
    assert request.body == b"<root/>"
    assert request.copy().headers["content-type"] == "application/xml"
    with pytest.raises(TypeError):
        request.body = {"invalid": 1}
    assert request.body == b"<root/>"


def multipart_parts(request):
    """解析请求体中的 multipart 字段供测试断言。

    Args:
        request: 当前 HTTP 请求或 pytest 提供的参数化夹具对象。

    Returns:
        按请求体顺序解析出的 multipart 字段列表。
    """

    message = BytesParser(policy=policy.default).parsebytes(
        ("Content-Type: " + request.headers["Content-Type"] + "\r\n\r\n").encode()
        + request.body
    )
    assert message.is_multipart()
    assert not message.defects
    return list(message.iter_parts())


def test_multipart_round_trip_binary_repeated_fields_and_files():
    """验证 multipart 保留二进制、重复字段和多文件。"""

    binary = bytes(range(256)) + b"\r\n\x00\xff"
    upload = UploadFile("sample.bin", binary)
    request = Request(
        "https://example.com",
        body_type="multipart",
        body={
            "tag": ["a", "b"],
            "file": [upload, UploadFile("empty.txt", b"", "text/plain")],
        },
        headers={"Content-Type": "multipart/form-data; boundary=incorrect"},
    )
    parts = multipart_parts(request)
    assert [part.get_param("name", header="content-disposition") for part in parts] == [
        "tag",
        "tag",
        "file",
        "file",
    ]
    assert [part.get_payload(decode=True) for part in parts] == [
        b"a",
        b"b",
        binary,
        b"",
    ]
    assert parts[2].get_filename() == "sample.bin"
    assert parts[3].get_content_type() == "text/plain"
    copied = request.copy()
    assert copied.body == request.body
    assert copied.headers.raw == request.headers.raw
    upload.content = b"changed"
    request.body_type = "multipart"
    assert multipart_parts(request)[2].get_payload(decode=True) == binary
    copied.body = {"new": "value"}
    assert multipart_parts(copied)[0].get_payload(decode=True) == b"value"


def test_multipart_unicode_names_and_filename_quoting():
    """验证 multipart 中文内容与文件名引号处理。"""

    request = Request(
        "https://example.com",
        body_type="multipart",
        body=[
            ("text", "\u4e2d\u6587"),
            ("file", UploadFile('a"b.txt', "text", "text/plain")),
            ("text", "again"),
        ],
    )
    parts = multipart_parts(request)
    assert parts[0].get_payload(decode=True) == "\u4e2d\u6587".encode()
    assert parts[1].get_filename() == "a%22b.txt"
    assert parts[2].get_payload(decode=True) == b"again"


@pytest.mark.parametrize("body_type", ["auto", "json", "form", "multipart", "raw"])
def test_none_means_no_body(body_type):
    """验证各编码模式中的 None 均表示无请求体。

    Args:
        body_type: 请求体编码模式。
    """

    request = Request("https://example.com", body_type=body_type)
    assert request.body is None
    assert "content-type" not in request.headers


@pytest.mark.parametrize("value", [b"raw", "text", {"nested": {"a": 1}}])
def test_invalid_multipart_assignment_is_atomic(value):
    """验证非法 multipart 赋值不改变原请求。

    Args:
        value: 当前操作处理的输入值。
    """

    request = Request("https://example.com", body_type="multipart", body={"a": "b"})
    before = request.body, request.headers.raw
    with pytest.raises(TypeError):
        request.body = value
    assert (request.body, request.headers.raw) == before


def test_upload_file_validation():
    """验证内存上传文件的名称、内容和媒体类型约束。"""

    with pytest.raises(ValueError):
        UploadFile("bad\r\nname", b"data")
    with pytest.raises(ValueError):
        UploadFile("name", b"data", "text/plain\r\nInjected: yes")
    with pytest.raises(TypeError):
        UploadFile("name", cast(Any, iter([b"data"])))  # 流输入应在运行时被拒绝。
