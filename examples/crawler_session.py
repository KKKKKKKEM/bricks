"""用 SlotPool 隔离并复用同步 HTTP 会话的事件消费示例。"""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from contextlib import ExitStack
from queue import SimpleQueue
from typing import Any

from interlace import Context, Graph, Node, Ports, Runtime, Slot, SlotPool

from bricks import Downloader, DownloadNode, Request, Response
from bricks.downloaders.curl_cffi import CurlCffiDownloader


def slot_downloaders(slot: Slot) -> Mapping[str, Downloader]:
    """读取当前执行槽的下载器资源。

    Args:
        slot: 当前逻辑执行链使用的槽。

    Returns:
        装配方创建并负责关闭的下载器映射。
    """

    return slot["crawler.downloaders"]


class Collect(Node):
    """将下载结果交付给调用方的线程安全队列。"""

    input_ports = Ports(response=Response)  # 接收下载结果。

    def __init__(self, results: SimpleQueue[Response]) -> None:
        """注入结果队列。

        Args:
            results: 调用方拥有的可并发写入队列。
        """

        self._results = results  # 调用方拥有的结果容器，不保存当前执行状态。

    def execute(self, inputs: Mapping[str, Any], context: Context) -> None:
        """记录本次下载结果。

        Args:
            inputs: response 端口值。
            context: 当前执行上下文。
        """

        self._results.put(inputs["response"])


def run(urls: list[str], concurrency: int = 2) -> list[Response]:
    """创建会话池并消费一组同业务身份的请求。

    Args:
        urls: 同一业务身份的 URL 列表，不混用不同账号。
        concurrency: 独立会话数量和消费者并发上限，默认 2。

    Returns:
        按完成顺序排列的下载结果。

    Raises:
        ValueError: 并发参数或 URL 非法。
        TimeoutError: 事件消费未在 30 秒内完成。
    """

    results: SimpleQueue[Response] = SimpleQueue()
    downloader = CurlCffiDownloader()
    with ExitStack() as resources:

        def make_slot() -> Slot:
            """创建独立会话并将关闭职责登记到外层资源栈。

            Returns:
                保存会话下载器的独立 Slot。
            """

            session = downloader.prepare_session()
            resources.callback(downloader.close_session, session)
            return Slot({"crawler.downloaders": {"default": session}})

        pool = SlotPool(concurrency, factory=make_slot)
        resources.callback(pool.close)
        graph = (
            Graph(entrypoint="download")
            .add(download=DownloadNode(slot_downloaders), collect=Collect(results))
            .connect(
                "download", "collect", source_port="response", target_port="response"
            )
        )
        with Runtime() as runtime:
            runtime.register("crawl", graph)
            runtime.on(
                "crawl.request",
                graph="crawl",
                queue="crawl",
                concurrency=concurrency,
                slots=pool,
            )
            for url in urls:
                runtime.emit("crawl.request", Request(url))
            runtime.wait_idle(timeout=30)
    responses = []
    while not results.empty():
        responses.append(results.get_nowait())
    return responses


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("urls", nargs="+")
    for response in run(parser.parse_args().urls):
        print(response.status_code, response.url, response.size())
