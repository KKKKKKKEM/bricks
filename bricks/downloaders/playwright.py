"""Playwright 同步和异步下载器，提供页面与 API 两种模式。"""

from contextlib import AsyncExitStack, ExitStack
from copy import deepcopy
from dataclasses import dataclass
from typing import Literal

from playwright.async_api import Browser as AsyncBrowser, async_playwright
from playwright.sync_api import Browser, sync_playwright

from ._browser import AsyncBrowserDownloader, SyncBrowserDownloader


@dataclass(frozen=True)
class PlaywrightDownloader(SyncBrowserDownloader):
    """默认启动 Chromium；注入外部 Browser 可在多个单 Tab 会话间共享进程。"""

    browser_type: Literal["chromium", "firefox", "webkit"] = (
        "chromium"  # 自建浏览器的引擎。
    )

    def __post_init__(self) -> None:
        """校验浏览器引擎和公共配置。

        Raises:
            ValueError: 引擎名称非法。
        """

        super().__post_init__()
        if self.browser_type not in {"chromium", "firefox", "webkit"}:
            raise ValueError("unsupported Playwright browser_type")

    def _open_browser(self, resources: ExitStack) -> Browser:
        """启动原生同步浏览器或借用外部实例。

        Args:
            resources: 自建驱动和浏览器的清理栈。

        Returns:
            同步 Browser。
        """

        if self.browser is not None:
            return self.browser
        driver = resources.enter_context(sync_playwright())
        options = deepcopy(dict(self.launch_options or {}))
        browser = getattr(driver, self.browser_type).launch(
            headless=self.headless, **options
        )
        resources.callback(browser.close)
        return browser


@dataclass(frozen=True)
class AsyncPlaywrightDownloader(AsyncBrowserDownloader):
    """原生异步 Playwright 下载器，共用单 Tab 双模式会话实现。"""

    browser_type: Literal["chromium", "firefox", "webkit"] = (
        "chromium"  # 自建浏览器引擎。
    )

    def __post_init__(self) -> None:
        """校验引擎名称及异步回调。

        Raises:
            ValueError: 引擎名称非法。
        """

        super().__post_init__()
        if self.browser_type not in {"chromium", "firefox", "webkit"}:
            raise ValueError("unsupported Playwright browser_type")

    async def _open_browser(self, resources: AsyncExitStack) -> AsyncBrowser:
        """在当前事件循环内启动或借用 Browser。

        Args:
            resources: 自建资源的异步清理栈。

        Returns:
            异步 Browser。
        """

        if self.browser is not None:
            return self.browser
        driver = await resources.enter_async_context(async_playwright())
        options = deepcopy(dict(self.launch_options or {}))
        browser = await getattr(driver, self.browser_type).launch(
            headless=self.headless, **options
        )
        resources.push_async_callback(browser.close)
        return browser
