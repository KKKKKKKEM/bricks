"""测试共享夹具：本地 HTTP 服务、必需服务检查与租约续租观察。"""

import gzip
import json
import ssl
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Condition, Event, Thread

import pytest


@pytest.fixture(autouse=True)
def require_test_services():
    """CI 标记必须服务时，缺少 Redis 或 Mongo 地址立即失败。"""
    import os

    if os.environ.get("BRICKS_REQUIRE_SERVICES") == "1":
        missing = [
            name
            for name in ("BRICKS_TEST_REDIS_URL", "BRICKS_TEST_MONGO_URL")
            if not os.environ.get(name)
        ]
        if missing:
            pytest.fail(f"required test service URLs are missing: {', '.join(missing)}")


class RenewalWatch:
    """记录续租方法的成功完成时间，让租约测试等待真实续租而不是固定 sleep。

    Attributes:
        _condition: 保护续租记录并唤醒等待方。
        _renewed: 凭据 token 到最近一次成功续租完成时间的映射，单位为 time.monotonic 秒。
    """

    def __init__(self) -> None:
        """创建空记录。"""
        self._condition = Condition()
        self._renewed: dict[str, float] = {}

    def wrap(self, renew):
        """包装续租方法，成功返回后登记凭据的完成时间，失败原样传播。

        Args:
            renew: 原续租方法，第一个参数是带 token 的凭据。

        Returns:
            调用签名与原方法相同的记录包装。
        """

        def recorded(lease, *args, **kwargs):
            """调用原续租方法并登记成功时间。

            Args:
                lease: 当前凭据。
                *args: 原方法的其他位置参数。
                **kwargs: 原方法的关键字参数。

            Returns:
                原方法返回的新凭据。
            """
            renewed = renew(lease, *args, **kwargs)
            with self._condition:
                self._renewed[lease.token] = time.monotonic()
                self._condition.notify_all()
            return renewed

        return recorded

    def wait_past(self, expiry, count=1, timeout=10):
        """等待至少 count 个凭据在 expiry 之后成功续租。

        expiry 应取进入等待前最后一次设定期限的上界，返回时这些凭据已跨过原期限且刚刚续租，
        随后的断言拥有完整租约余量。

        Args:
            expiry: 原期限上界，time.monotonic 秒。
            count: 需要跨期续租的不同凭据数量。
            timeout: 最长等待秒数，只用于失败时结束测试。

        Raises:
            AssertionError: 截止时间内续租数量不足，通常表示租约已丢失或续租停止。
        """
        deadline = time.monotonic() + timeout
        with self._condition:
            while sum(moment > expiry for moment in self._renewed.values()) < count:
                remaining = deadline - time.monotonic()
                assert remaining > 0, "leases were not renewed past their expiry"
                self._condition.wait(remaining)


@pytest.fixture
def renewals(monkeypatch):
    """提供续租观察器工厂，测试结束时恢复被包装的方法。

    Args:
        monkeypatch: pytest 属性替换夹具。

    Returns:
        接收目标对象和续租方法名并返回 RenewalWatch 的函数。
    """

    def watch(target, method="renew"):
        """在目标实例上包装续租方法。

        Args:
            target: 队列等提供续租方法的对象。
            method: 续租方法名，默认 renew。

        Returns:
            记录该方法成功续租的观察器。
        """
        watcher = RenewalWatch()
        monkeypatch.setattr(target, method, watcher.wrap(getattr(target, method)))
        return watcher

    return watch


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
            scheme = "https" if tls_context is not None else "http"
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
                    ("Location", f"{scheme}://localhost:{self.server.server_port}/echo")
                ]
            elif self.path.startswith("/downgrade/"):
                status = 302
                port = self.path.rsplit("/", 1)[1]
                headers = [("Location", f"http://localhost:{port}/echo")]
            elif self.path.startswith("/login-secure/"):
                headers = [
                    (
                        "Set-Cookie",
                        f"account={self.path.rsplit('/', 1)[1]}; Path=/; Secure",
                    )
                ]
            elif self.path == "/subdomain":
                status = 302
                headers = [
                    (
                        "Location",
                        f"{scheme}://sub.localhost:{self.server.server_port}/echo",
                    )
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
