"""将列表分页、详情下载、Items 解析和 JSONL 写入组合为完整爬取流程。"""

from __future__ import annotations

import argparse
import json
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, TextIO
from urllib.parse import urldefrag, urljoin, urlsplit

from interlace import Context, Graph, Node, Output, Ports, Runtime

from bricks import DownloadNode, Items, Request, Response
from bricks.downloaders.curl_cffi import CurlCffiDownloader


@dataclass
class Page:
    """一次页面解析的结果，由当前执行拥有。

    Attributes:
        links: 按页面顺序发现的详情和分页地址，默认空列表。
        items: 当前详情页的记录，列表页为空；不共享可变内容。
    """

    links: list[str] = field(default_factory=list)
    items: Items = field(default_factory=Items)


class PageParser(HTMLParser):
    """解析示例站点的 rel 链接和商品元数据。

    Attributes:
        links: 当前页面拥有的相对链接列表。
        record: 当前页面拥有的商品字段，缺少名称时视为列表页。
    """

    def __init__(self) -> None:
        """为单个页面创建独立解析状态。"""
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []
        self.record: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """读取详情、下一页链接和商品名称、价格元数据。

        Args:
            tag: HTML 标签名。
            attrs: HTMLParser 解码后的属性序列。
        """
        values = dict(attrs)
        if tag == "a" and set((values.get("rel") or "").split()) & {"item", "next"}:
            if href := values.get("href"):
                self.links.append(href)
        if tag == "meta" and values.get("name") in {"product:name", "product:price"}:
            name = values["name"]
            assert name is not None
            self.record[name.removeprefix("product:")] = values.get("content") or ""


def parse_page(response: Response) -> Page:
    """将成功页面转换为待抓取链接和详情记录。

    Args:
        response: 下载节点返回的缓冲响应。

    Returns:
        已解析为绝对 URL 的链接和 Items。

    Raises:
        ValueError: 下载失败、HTTP 状态异常或商品字段不完整。
    """
    if not 200 <= response.status_code < 300:
        raise ValueError(f"download failed: {response.status_code} {response.url}")
    parser = PageParser()
    parser.feed(response.text)
    parser.close()
    items = Items()
    if parser.record:
        if not parser.record.get("name") or not parser.record.get("price"):
            raise ValueError(f"incomplete product: {response.url}")
        items.append({"url": response.url, **parser.record})
    return Page([urljoin(response.url, link) for link in parser.links], items)


class Parse(Node):
    """将 HTTP 响应转换为领域解析结果。"""

    input_ports = Ports(response=Response)  # 当前执行的下载响应。
    output_ports = Ports(page=Page)  # 当前执行拥有的页面结果。

    def execute(self, inputs: Mapping[str, Any], context: Context) -> Output:
        """调用普通解析函数并显式返回 Output。

        Args:
            inputs: response 端口输入。
            context: 当前执行上下文。

        Returns:
            page 端口的页面结果。

        Raises:
            ValueError: 下载或页面内容不符合示例契约。
        """
        return Output(parse_page(inputs["response"]), port="page")


class WriteJsonl(Node):
    """将 Items 写入调用方拥有的文件；实例仅用于串行执行。"""

    input_ports = Ports(page=Page)  # 含待保存 Items 的页面结果。
    output_ports = Ports(page=Page)  # 原结果交还装配循环以继续分页。

    def __init__(self, stream: TextIO) -> None:
        """注入由调用方打开并关闭的文本文件。

        Args:
            stream: UTF-8 输出流，不支持本示例之外的并发写入。
        """
        self._stream = stream  # 借用的可写流，关闭职责属于 run。

    def execute(self, inputs: Mapping[str, Any], context: Context) -> Output:
        """逐条写入 JSONL 并交付页面结果。

        Args:
            inputs: page 端口输入。
            context: 当前执行上下文。

        Returns:
            已保存记录的页面结果。

        Raises:
            OSError: 输出流写入失败。
        """
        page: Page = inputs["page"]
        for record in page.items:
            self._stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        self._stream.flush()
        return Output(page, port="page")


def run(url: str, output: Path, max_pages: int = 20) -> int:
    """在单个业务会话中串行抓取列表和详情并创建 JSONL 文件。

    Args:
        url: 起始列表页的 HTTP 或 HTTPS URL。
        output: 新建输出文件路径；已存在时拒绝覆盖。
        max_pages: 总页面数上限，默认 20，超过上限明确失败。

    Returns:
        实际写入的商品记录数。

    Raises:
        ValueError: 页数上限或起始 URL 非法。
        RuntimeError: 待抓取页面超过上限。
        OSError: 创建或写入文件失败。
        ExecutionError: 下载、解析或存储节点失败，原始异常保留在异常链中。
    """
    if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages < 1:
        raise ValueError("max_pages must be a positive integer")
    start = Request(url).url
    origin = urlsplit(start)
    pending = deque([start])
    seen: set[str] = set()
    count = 0
    factory = CurlCffiDownloader(max_redirects=0, trust_env=False)
    with output.open("x", encoding="utf-8") as stream:
        session = factory.prepare_session()
        try:
            graph = (
                Graph(entrypoint="download")
                .add(
                    download=DownloadNode({"default": session}),
                    parse=Parse(),
                    save=WriteJsonl(stream),
                )
                .connect(
                    "download", "parse", source_port="response", target_port="response"
                )
                .connect("parse", "save", source_port="page", target_port="page")
            )
            with Runtime() as runtime:
                runtime.register("crawl", graph)
                while pending:
                    current = urldefrag(pending.popleft())[0]
                    target = urlsplit(current)
                    if (target.scheme, target.netloc) != (origin.scheme, origin.netloc):
                        continue
                    if current in seen:
                        continue
                    if len(seen) >= max_pages:
                        raise RuntimeError("max_pages exceeded; output is partial")
                    seen.add(current)
                    page = runtime.run("crawl", Request(current))[0].value
                    count += len(page.items)
                    pending.extend(page.links)
        finally:
            factory.close_session(session)
    return count


def main() -> None:
    """解析命令行参数，未指定 URL 时启动本地演示站点。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", nargs="?")
    parser.add_argument("--output", type=Path, default=Path("products.jsonl"))
    parser.add_argument("--max-pages", type=int, default=20)
    args = parser.parse_args()
    if args.url:
        count = run(args.url, args.output, args.max_pages)
    else:
        from examples.crawler_demo import demo_site

        with demo_site() as url:
            count = run(url, args.output, args.max_pages)
    print(f"{count} records -> {args.output}")


if __name__ == "__main__":
    main()
