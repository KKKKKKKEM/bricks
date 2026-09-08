"""Camoufox 下载器，共用 Playwright 页面和 API 会话，API 请求不使用 Firefox TLS 栈。"""

from contextlib import AsyncExitStack, ExitStack
from copy import deepcopy
from dataclasses import dataclass

from camoufox.addons import DefaultAddons  # type: ignore[import-untyped]
from camoufox.async_api import AsyncCamoufox  # type: ignore[import-untyped]
from camoufox.sync_api import Camoufox  # type: ignore[import-untyped]
from playwright.async_api import Browser as AsyncBrowser
from playwright.sync_api import Browser

from ._browser import AsyncBrowserDownloader, SyncBrowserDownloader


@dataclass(frozen=True)
class CamoufoxDownloader(SyncBrowserDownloader):
    """使用固定 Camoufox 浏览器版本；os/config 等指纹选项通过 launch_options 传入。"""

    browser_version: str = (
        "152.0.4-beta.30"  # 预先安装的精确浏览器版本，不追随系统活动通道。
    )

    def __post_init__(self) -> None:
        """校验版本选择与启动参数。

        Raises:
            ValueError: 版本为空或 launch_options 重复指定 browser。
        """

        super().__post_init__()
        if (
            not isinstance(self.browser_version, str)
            or not self.browser_version.strip()
        ):
            raise ValueError("browser_version must be a non-empty installed version")
        if "browser" in (self.launch_options or {}):
            raise ValueError("use browser_version instead of launch_options.browser")

    def _open_browser(self, resources: ExitStack) -> Browser:
        """启动固定版本 Camoufox 或借用调用方提供的 Browser。

        Args:
            resources: 自建 Camoufox 资源的关闭栈。

        Returns:
            兼容 Playwright 的同步 Browser。

        Raises:
            TypeError: Camoufox 没有返回 Browser。
        """

        if self.browser is not None:
            return self.browser
        options = deepcopy(dict(self.launch_options or {}))
        options.setdefault("exclude_addons", list(DefaultAddons))
        browser = resources.enter_context(
            Camoufox(headless=self.headless, browser=self.browser_version, **options)
        )
        if not isinstance(browser, Browser):
            raise TypeError("Camoufox must return a Browser, not a persistent context")
        return browser


@dataclass(frozen=True)
class AsyncCamoufoxDownloader(AsyncBrowserDownloader):
    """异步 Camoufox 下载器，Context 与 Tab 的复用方式同 Playwright。"""

    browser_version: str = "152.0.4-beta.30"  # 自建浏览器的固定版本。

    def __post_init__(self) -> None:
        """校验固定版本和异步启动参数。

        Raises:
            ValueError: 版本或保留参数非法。
        """

        super().__post_init__()
        if (
            not isinstance(self.browser_version, str)
            or not self.browser_version.strip()
        ):
            raise ValueError("browser_version must be a non-empty installed version")
        if "browser" in (self.launch_options or {}):
            raise ValueError("use browser_version instead of launch_options.browser")

    async def _open_browser(self, resources: AsyncExitStack) -> AsyncBrowser:
        """在当前循环启动 Camoufox，外部 Browser 不登记关闭。

        Args:
            resources: 自建资源的异步清理栈。

        Returns:
            原生异步 Browser。

        Raises:
            TypeError: 后端返回了持久 Context 而非 Browser。
        """

        if self.browser is not None:
            return self.browser
        options = deepcopy(dict(self.launch_options or {}))
        options.setdefault("exclude_addons", list(DefaultAddons))
        browser = await resources.enter_async_context(
            AsyncCamoufox(
                headless=self.headless, browser=self.browser_version, **options
            )
        )
        if not isinstance(browser, AsyncBrowser):
            raise TypeError("Camoufox must return a Browser, not a persistent context")
        return browser
