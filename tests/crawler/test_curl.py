import shlex
import shutil
import subprocess
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import pytest

from bricks import Request, UploadFile


def test_browser_curl_and_defaults():
    """验证浏览器 cURL 导入与默认请求设置。"""

    request = Request.from_curl("""curl 'https://example.com?q=one' \\
      -H 'Accept: application/json' \\
      -H 'X-Test: one' -H 'X-Test: two' \\
      --data-raw '{"page":1}' -b 'session=abc; other=two'""")
    assert request.method == "POST"
    assert request.body == b'{"page":1}'
    assert request.headers.get_all("x-test") == ("one", "two")
    assert request.cookies == {"session": "abc", "other": "two"}
    assert request.allow_redirects is False
    assert request.timeout is None


@pytest.mark.parametrize(
    "body",
    [None, b"", b"\n", b"\\\n", b"@literal", b"line1\r\nline2", b"'$(no); & < > `x`"],
)
def test_round_trip_preserves_encoded_request(body):
    """验证 cURL 往返转换保留已编码请求。

    Args:
        body: 待编码的请求体数据。
    """

    original = Request(
        "https://example.com/path?x=1#frag",
        method="PATCH",
        params={"tag": ["a", "b"]},
        body=body,
        headers=[("X-Test", "one"), ("X-Test", "two"), ("X-Empty", "")],
        cookies={"session": "abc"},
        proxy="http://localhost:8080",
        timeout=4,
    )
    restored = Request.from_curl(original.to_curl())
    assert restored.real_url == original.real_url
    assert restored.method == original.method
    assert restored.body == original.body
    assert restored.headers.raw == original.headers.raw
    assert restored.cookies == original.cookies
    assert restored.timeout == original.timeout
    assert restored.proxy == original.proxy
    assert restored.allow_redirects == original.allow_redirects


def test_short_options_json_get_and_auth():
    """验证 cURL 短选项、JSON、查询参数与基本认证。"""

    request = Request.from_curl(
        "curl -sSL -XPUT -m0 -u user:password --json '{\"x\":1}' https://example.com"
    )
    assert request.method == "PUT"
    assert request.allow_redirects
    assert request.timeout is None
    assert request.headers["Authorization"] == "Basic dXNlcjpwYXNzd29yZA=="
    assert request.headers["Accept"] == "application/json"
    request = Request.from_curl(
        "curl -G --data-urlencode 'q=a b' --data-urlencode 'tag=x/y' https://example.com?old=1"
    )
    assert request.real_url == "https://example.com?old=1&q=a%20b&tag=x%2Fy"
    assert request.method == "GET"
    assert request.body is None


def test_file_input_is_explicit_and_binary_export_round_trips():
    """验证文件内容须显式提供且二进制导出可往返。"""

    request = Request("https://example.com", method="POST", body=bytes(range(256)))
    with pytest.raises(ValueError, match="body_file"):
        request.to_curl()
    command = request.to_curl(body_file="body.bin")
    with pytest.raises(ValueError, match="provide files"):
        Request.from_curl(command)
    assert request.body is not None
    restored = Request.from_curl(command, files={"body.bin": request.body})
    assert restored.body == request.body
    assert "content-type" not in restored.headers
    data = Request.from_curl(
        "curl -d @input https://example.com", files={"input": b"a\r\nb\x00c"}
    )
    assert data.body == b"abc"


def test_multipart_file_import_and_export():
    """验证 multipart 文件上传的 cURL 导入和导出。"""

    request = Request.from_curl(
        "curl -F 'file=@input;filename=report.txt;type=text/plain' https://example.com",
        files={"input": b"hello"},
    )
    assert request.body_type == "multipart"
    assert request.body is not None
    assert b'filename="report.txt"' in request.body
    assert b"hello" in request.body
    restored = Request.from_curl(request.to_curl())
    assert restored.body == request.body
    assert restored.headers.raw == request.headers.raw
    binary = Request(
        "https://example.com",
        body_type="multipart",
        body={"f": UploadFile("x", b"\x00")},
    )
    assert binary.body is not None
    restored = Request.from_curl(
        binary.to_curl(body_file="upload"), files={"upload": binary.body}
    )
    assert restored.body == binary.body


@pytest.mark.parametrize(
    "command",
    [
        "echo curl https://example.com",
        "curl https://example.com https://other.com",
        "curl -k https://example.com",
        "curl --data @secret https://example.com",
        "curl -b cookies.txt https://example.com",
        "curl -H broken https://example.com",
        "curl --data a --json b https://example.com",
        "curl --request",
        "curl https://example.com ; touch /tmp/x",
        "curl --config config",
        "curl --data-raw $'line\\n' https://example.com",
        "curl 'https://example.com/{one,two}'",
    ],
)
def test_unsupported_commands_fail(command):
    """验证不支持的 cURL 命令明确失败。

    Args:
        command: 需要解析的单条 POSIX cURL 命令。
    """

    with pytest.raises(ValueError):
        Request.from_curl(command)


def test_exported_command_against_real_curl():
    """用真实 cURL 验证导出命令的请求内容。"""

    if shutil.which("curl") is None:
        pytest.skip("curl executable is unavailable")
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            """记录本地测试 POST 请求的路径、请求头与内容。"""

            received.append(
                (
                    self.path,
                    self.headers,
                    self.rfile.read(int(self.headers.get("Content-Length", 0))),
                )
            )
            self.send_response(200)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            """屏蔽本地测试 HTTP 服务的常规访问日志。

            Args:
                format: 当前用例使用的 format 夹具或参数化输入。
                *args: 调用协议传入的位置参数。
            """

            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        request = Request(
            f"http://127.0.0.1:{server.server_port}/",
            method="POST",
            body=b"@literal\r\n'$(echo no)'",
            params={"p": 2},
        )
        subprocess.run(
            shlex.split(request.to_curl()), check=True, capture_output=True, timeout=5
        )
        assert received[0][0] == "/?p=2"
        assert received[0][2] == request.body
        assert received[0][1].get("Content-Type") is None
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
