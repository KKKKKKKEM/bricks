"""使用 Air 抓取本地列表与详情，并在 ACK 前幂等写入 SQLite。"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from urllib.parse import urldefrag, urlsplit

from bricks import Items, Request
from bricks.plugins import storage
from bricks.spider import air

from .crawler_demo import demo_site
from .crawler_pipeline import parse_page


class ProductSpider(air.Spider):
    """四个业务方法组成的列表与详情爬虫。

    Attributes:
        start_url: 本地演示站点入口。
        database: SQLite 文件路径，存储插件每批创建并关闭独立连接。
    """

    def __init__(self, start_url: str, database: Path, **kwargs) -> None:
        """保存业务依赖，并沿用 Spider 的命名参数。

        Args:
            start_url: 列表入口 URL。
            database: SQLite 文件路径。
            kwargs: concurrency、downloader、task_queue 等框架参数。
        """
        super().__init__(**kwargs)
        self.start_url = start_url
        self.database = database

    def make_seeds(self, context: air.InitContext, **kwargs):
        """生成入口种子，提交之后保存恢复记录。

        Args:
            context: 初始化上下文。
            kwargs: record 为上一次保存的 JSON 记录。

        Yields:
            一项入口种子。
        """
        record = kwargs["record"]
        if not record.get("submitted"):
            record["submitted"] = True
            yield {"url": self.start_url}

    def make_request(self, context: air.Context) -> Request:
        """为当前种子构造请求，并携带演示站点约定的固定 Cookie。

        Args:
            context: 当前种子上下文。

        Returns:
            演示 HTTP 请求；同步 SessionDownloader 由 Air 按并发 Slot 自动复用。
        """
        return Request(context.seeds["url"], cookies={"demo": "active"})

    def parse(self, context: air.Context) -> Items:
        """解析页面并把分页、详情种子显式提交到同一个队列。

        Args:
            context: 当前响应上下文。

        Returns:
            当前详情记录，列表页返回空 Items。
        """
        page = parse_page(context.response)
        origin = urlsplit(self.start_url)
        for link in page.links:
            url = urldefrag(link)[0]
            target = urlsplit(url)
            if (target.scheme, target.netloc) == (origin.scheme, origin.netloc):
                context.submit({"url": url})
        return page.items

    def item_pipeline(self, context: air.Context) -> None:
        """提交幂等存储事务后正常返回，由图继续后置插槽和确认收尾。

        Args:
            context: 已解析的当前记录。

        Raises:
            sqlite3.Error: 写入或提交失败，不会确认该次交付。
        """
        context.items = Items(
            [
                {"url": record["url"], "data": json.dumps(record, ensure_ascii=False)}
                for record in context.items
            ]
        )
        storage.to_sqlite("products", self.database, row_keys=["url"])


def run_demo(output: Path) -> tuple[air.RunReport, int]:
    """运行本地演示站点并返回队列和数据库结果。

    Args:
        output: SQLite 文件路径，按 URL 更新现有记录。

    Returns:
        本轮运行快照与数据库记录数。
    """
    with demo_site() as start_url:
        spider = ProductSpider(
            start_url, output, concurrency=2, queue_name="products-demo@v1"
        )
        report = spider.run(kwargs={"timeout": 15})
    database = sqlite3.connect(output)
    try:
        count = database.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        return report, count
    finally:
        database.close()


def main() -> None:
    """运行可复现的本地 Air 示例。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("products.sqlite3"))
    parser.add_argument(
        "--survey",
        action="store_true",
        help="手动消费入口种子并展示结果，跳过默认 SQLite 存储",
    )
    args = parser.parse_args()
    if args.survey:
        with demo_site() as start_url:
            spider = ProductSpider(start_url, args.output, concurrency=2)
            contexts = spider.survey({"url": start_url}, timeout=15)
        print(
            json.dumps(
                [
                    {"seeds": context.seeds, "items": list(context.items)}
                    for context in contexts
                ],
                ensure_ascii=False,
            )
        )
        return
    report, count = run_demo(args.output)
    print(json.dumps({"queue": report.counts, "stored": count}, ensure_ascii=False))


if __name__ == "__main__":
    main()
