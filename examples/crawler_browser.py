"""使用单个浏览器会话重复下载，展示页面/API 模式与 Tab 复用。"""

import argparse
import asyncio
from typing import Literal

from bricks import Request, Response


async def run(
    request: Request,
    *,
    engine: Literal["playwright", "camoufox"] = "playwright",
    mode: Literal["page", "api"] = "page",
    proxy: str | None = None,
    repeat: int = 2,
) -> list[Response]:
    """在同一事件循环内装配和关闭可复用浏览器会话。

    Args:
        request: 每次下载使用的请求，页面模式要求无请求体 GET。
        engine: 浏览器启动后端。
        mode: 页面或 API 模式。
        proxy: 当前 Context 固定代理。
        repeat: 重复次数，默认两次以展示会话复用。

    Returns:
        按调用顺序排列的响应列表。

    Raises:
        ValueError: 后端或重复次数非法。
    """

    if type(repeat) is not int or repeat < 1:
        raise ValueError("repeat must be a positive integer")
    if engine == "playwright":
        from bricks.downloaders.playwright import AsyncPlaywrightDownloader

        downloader = AsyncPlaywrightDownloader(mode=mode)
    elif engine == "camoufox":
        from bricks.downloaders.camoufox import AsyncCamoufoxDownloader

        downloader = AsyncCamoufoxDownloader(mode=mode)
    else:
        raise ValueError("engine must be playwright or camoufox")
    session = await downloader.prepare_session(proxy=proxy)
    try:
        return [await session.fetch(request) for _ in range(repeat)]
    finally:
        await downloader.close_session(session)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    parser.add_argument(
        "--engine", choices=["playwright", "camoufox"], default="playwright"
    )
    parser.add_argument("--mode", choices=["page", "api"], default="page")
    parser.add_argument("--proxy")
    parser.add_argument("--repeat", type=int, default=2)
    parser.add_argument("--method", default="GET")
    parser.add_argument("--data")
    args = parser.parse_args()
    responses = asyncio.run(
        run(
            Request(args.url, method=args.method, body=args.data),
            engine=args.engine,
            mode=args.mode,
            proxy=args.proxy,
            repeat=args.repeat,
        )
    )
    for response in responses:
        print(response.status_code, response.url, response.size())
