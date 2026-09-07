"""提供带分页、重复链接和 Cookie 校验的本地演示商品站点。"""

from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread


class DemoHandler(BaseHTTPRequestHandler):
    """只提供固定演示页面，不访问外部服务。"""

    protocol_version = "HTTP/1.1"  # 支持会话连接复用。

    def do_GET(self) -> None:
        """返回列表或详情；后续页面要求携带入口设置的 Cookie。"""
        pages = {
            "/catalog": '<a rel="item" href="/product/1">一号</a><a rel="next" href="/catalog?page=2">下一页</a>',
            "/catalog?page=2": '<a rel="item" href="/product/1#duplicate">重复</a><a rel="item" href="/product/2">二号</a><a rel="next" href="/catalog">返回</a>',
            "/product/1": '<meta name="product:name" content="铅笔 &amp; 橡皮"><meta name="product:price" content="3.50">',
            "/product/2": '<meta name="product:name" content="笔记本"><meta name="product:price" content="12.00">',
        }
        status = 200 if self.path in pages else 404
        if self.path != "/catalog" and "demo=active" not in self.headers.get(
            "Cookie", ""
        ):
            status = 403
        body = (pages[self.path] if status == 200 else "request rejected").encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if self.path == "/catalog":
            self.send_header("Set-Cookie", "demo=active; Path=/; HttpOnly")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        """静默处理本地演示访问日志。

        Args:
            format: 原生日志格式。
            *args: 原生日志参数。
        """


@contextmanager
def demo_site() -> Iterator[str]:
    """启动仅监听本机的临时站点，并在退出时关闭。

    Yields:
        起始列表页地址。
    """
    server = ThreadingHTTPServer(("127.0.0.1", 0), DemoHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/catalog"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
