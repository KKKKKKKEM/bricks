"""通过请求路径动态选择下载器的爬虫 Graph 示例。"""

from __future__ import annotations

import argparse
from urllib.parse import urlsplit

from interlace import Graph, Runtime
from bricks import DownloadNode, Request, Response
from bricks.downloaders.curl_cffi import CurlCffiDownloader


def select_download(request: Request) -> str | None:
    """为严格路径选择禁止重定向的下载器。

    Args:
        request: 待下载请求的独立副本。

    Returns:
        strict 下载器名称，其他路径返回 None 以使用节点默认下载器。
    """

    return "strict" if urlsplit(request.url).path.startswith("/strict/") else None


def run(url: str) -> Response:
    """构造下载 Graph 并执行一次真实 HTTP 请求。

    Args:
        url: 待下载的绝对 HTTP 或 HTTPS 地址。

    Returns:
        下载节点输出的 HTTP 响应或传输失败响应。

    Raises:
        ValueError: URL 不合法。
        ExecutionError: Graph 执行失败，网络失败响应不触发此异常。
    """

    node = DownloadNode(
        {"default": CurlCffiDownloader(), "strict": CurlCffiDownloader(max_redirects=0)},
        select=select_download,
    )
    graph = Graph(entrypoint="download").add(download=node)
    with Runtime() as runtime:
        runtime.register("crawl", graph)
        return runtime.run("crawl", Request(url))[0].value


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    response = run(parser.parse_args().url)
    print(response.status_code, response.size())
