"""供不同下载器复用的本地 HTTP 服务。"""

import gzip
import json
import ssl
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, Thread

import pytest


def _serve(tls_context=None):
    """启动本地 HTTP 服务，并在用例结束后关闭服务和线程。

    Args:
        tls_context: 可选服务端 TLS 上下文，None 使用明文 HTTP。

    Yields:
        服务基础 URL、收到的请求记录和慢请求启动事件。
    """

    seen = []
    slow_started = Event()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"  # 启用持久连接，验证实际连接池复用。

        def do_GET(self):
            """处理本地测试 HTTP 请求并生成当前场景的响应。"""

            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            seen.append((self.command, self.path, self.headers, body))
            headers = []
            status = 200
            data = json.dumps(
                {
                    "path": self.path,
                    "cookie": self.headers.get("Cookie", ""),
                    "body": body.decode(),
                    "peer_port": self.client_address[1],
                }
            ).encode()
            if self.path == "/redirect":
                status = 302
                headers = [
                    ("Location", "/delete"),
                    ("Set-Cookie", "session=abc; Path=/"),
                ]
            elif self.path == "/delete":
                status = 302
                headers = [
                    ("Location", "/echo"),
                    ("Set-Cookie", "session=; Max-Age=0; Path=/"),
                ]
            elif self.path == "/loop":
                status = 302
                headers = [("Location", "/loop")]
            elif self.path == "/post-redirect":
                status = 303
                headers = [("Location", "/echo")]
            elif self.path == "/cross":
                status = 302
                headers = [
                    ("Location", f"http://localhost:{self.server.server_port}/echo")
                ]
            elif self.path == "/gzip":
                data = gzip.compress(b"hello" * 100)
                headers = [("Content-Encoding", "gzip")]
            elif self.path == "/browser-page":
                data = (
                    b'<!doctype html><html><body><h1 id="result">initial</h1>'
                    b'<script>document.getElementById("result").textContent="rendered";'
                    b"document.body.dataset.cookie=document.cookie;</script></body></html>"
                )
                headers = [("Content-Type", "text/html; charset=utf-8")]
            elif self.path == "/slow":
                slow_started.set()
                time.sleep(0.2)
            elif self.path == "/long-slow":
                slow_started.set()
                time.sleep(1.3)
            elif self.path == "/error":
                status = 500
            elif self.path == "/cookies":
                headers = [("Set-Cookie", "a=1; Path=/"), ("Set-Cookie", "b=2; Path=/")]
            elif self.path.startswith("/login/"):
                headers = [
                    ("Set-Cookie", f"account={self.path.rsplit('/', 1)[1]}; Path=/")
                ]
            self.send_response(status)
            for name, value in headers:
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            try:
                if self.command != "HEAD":
                    self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError, ssl.SSLEOFError):
                pass

        do_POST = do_GET
        do_PUT = do_GET
        do_PATCH = do_GET
        do_DELETE = do_GET
        do_OPTIONS = do_GET
        do_HEAD = do_GET

        def log_message(self, *args):
            """屏蔽本地测试 HTTP 服务的常规访问日志。

            Args:
                *args: 调用协议传入的位置参数。
            """

            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    if tls_context is not None:
        httpd.socket = tls_context.wrap_socket(httpd.socket, server_side=True)
    thread = Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        scheme = "https" if tls_context is not None else "http"
        yield f"{scheme}://127.0.0.1:{httpd.server_port}", seen, slow_started
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join()


@pytest.fixture
def server():
    """提供本地明文 HTTP 服务。

    Yields:
        服务地址、请求记录和慢请求启动事件。
    """

    yield from _serve()


@pytest.fixture
def https_server(tmp_path):
    """使用临时自签名证书启动真实 TLS 服务。

    Args:
        tmp_path: pytest 管理的证书目录。

    Yields:
        HTTPS 服务地址、请求记录和慢请求启动事件。
    """

    key = tmp_path / "key.pem"
    cert = tmp_path / "cert.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-days",
            "1",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    context.set_alpn_protocols(["http/1.1"])
    yield from _serve(context)
