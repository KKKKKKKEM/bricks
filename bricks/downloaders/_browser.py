"""Playwright 与 Camoufox 共用的双模式下载和单 Tab 会话。"""

from __future__ import annotations

import asyncio
import inspect
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AsyncExitStack, ExitStack, suppress
from copy import deepcopy
from dataclasses import dataclass, field
from threading import get_ident
from time import monotonic
from types import MappingProxyType
from typing import Any, Literal
from urllib.parse import unquote, urlsplit, urlunsplit

from playwright import async_api as asynchronous
from playwright import sync_api as synchronous

from ..models import Request, Response

Mode = Literal["page", "api"]
ContentMode = Literal["html", "response"]
WaitUntil = Literal["commit", "domcontentloaded", "load", "networkidle"]


def proxy_settings(proxy: str | None) -> dict[str, str] | None:
    """把代理 URL 转成 BrowserContext 的独立代理设置。

    Args:
        proxy: HTTP、HTTPS 或 SOCKS5 代理 URL，None 表示不设置。

    Returns:
        包含 server 和可选认证信息的原生代理参数。

    Raises:
        ValueError: 代理地址、协议或端口非法。
    """

    if proxy is None:
        return None
    parsed = urlsplit(proxy)
    if parsed.scheme not in {"http", "https", "socks5"} or not parsed.hostname:
        raise ValueError("browser proxy must be an HTTP, HTTPS or SOCKS5 URL")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ValueError("browser proxy must not include a path, query or fragment")
    host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    result = {"server": urlunsplit((parsed.scheme, host, "", "", ""))}
    if parsed.username is not None:
        result["username"] = unquote(parsed.username)
    if parsed.password is not None:
        result["password"] = unquote(parsed.password)
    return result


@dataclass(frozen=True)
class BrowserOptions:
    """两种浏览器后端共享的不可变选项。"""

    mode: Mode = "page"  # 默认执行真实页面导航；api 使用关联 APIRequestContext。
    content_mode: ContentMode = (
        "html"  # 页面模式默认返回渲染后 HTML；api 始终返回原始响应体。
    )
    wait_until: WaitUntil = "load"  # 页面导航等待阶段，默认等待 load。
    headless: bool = True  # 仅由装配器创建浏览器时生效，默认无头。
    verify: bool = True  # Context 和 API 模式默认验证 HTTPS 证书。
    max_redirects: int = 20  # 仅作用于 API 模式；页面导航遵循浏览器自身上限。
    launch_options: Mapping[str, Any] | None = (
        None  # 浏览器启动选项快照，不包含 headless 或 proxy。
    )
    context_options: Mapping[str, Any] | None = (
        None  # 每个会话独立 Context 的选项，不覆盖代理与证书策略。
    )

    def __post_init__(self) -> None:
        """校验配置，并复制可变参数以隔离调用方修改。

        Raises:
            TypeError: 参数类型错误。
            ValueError: 模式、等待策略、重定向上限或保留参数非法。
        """

        if self.mode not in {"page", "api"} or self.content_mode not in {
            "html",
            "response",
        }:
            raise ValueError("invalid browser mode or content_mode")
        if self.wait_until not in {"commit", "domcontentloaded", "load", "networkidle"}:
            raise ValueError("invalid browser wait_until")
        if type(self.headless) is not bool or type(self.verify) is not bool:
            raise TypeError("headless and verify must be booleans")
        if type(self.max_redirects) is not int or self.max_redirects < 0:
            raise ValueError("max_redirects must be a non-negative integer")
        for name, reserved in (
            (
                "launch_options",
                {"headless", "proxy", "persistent_context", "user_data_dir"},
            ),
            ("context_options", {"proxy", "ignore_https_errors"}),
        ):
            value = getattr(self, name)
            if value is not None and not isinstance(value, Mapping):
                raise TypeError(f"{name} must be a mapping or None")
            copied = deepcopy(dict(value or {}))
            if reserved.intersection(copied):
                raise ValueError(
                    f"{name} contains reserved parameters: {sorted(reserved.intersection(copied))}"
                )
            object.__setattr__(self, name, MappingProxyType(copied))

    def context_parameters(self, proxy: str | None) -> dict[str, Any]:
        """构造当前会话独立的 Context 设置。

        Args:
            proxy: 当前会话固定代理。

        Returns:
            可直接传入 Browser.new_context 的参数。
        """

        options = deepcopy(dict(self.context_options or {}))
        options["ignore_https_errors"] = not self.verify
        prepared_proxy = proxy_settings(proxy)
        if prepared_proxy is not None:
            options["proxy"] = prepared_proxy
        return options


def validate_request(source: Request, mode: Mode) -> dict[str, str]:
    """检查模式支持的请求契约，避免浏览器静默忽略设置。

    Args:
        source: 当前请求副本。
        mode: 页面导航或 API 请求。

    Returns:
        不含重复名称的请求头映射。

    Raises:
        ValueError: 请求方法、请求体、重定向或头不受当前模式支持。
    """

    names = [name.lower() for name, _ in source.headers.raw]
    if len(names) != len(set(names)):
        raise ValueError("browser downloaders do not support duplicate request headers")
    if mode == "page":
        if source.method != "GET" or source.body is not None:
            raise ValueError("page mode supports bodyless GET navigation; use api mode")
        if not source.allow_redirects:
            raise ValueError(
                "page mode cannot disable navigation redirects; use api mode"
            )
        if {"host", "cookie", "content-length"}.intersection(names):
            raise ValueError(
                "page mode cannot override Host, Cookie or Content-Length; use Request.cookies for cookies"
            )
    elif mode == "api":
        if source.method == "CONNECT":
            raise ValueError("api mode does not support CONNECT tunnels")
    else:
        raise ValueError("mode must be page or api")
    return dict(source.headers.raw)


def _html_headers(headers: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """为生成的 DOM 快照移除失效的实体头。

    Args:
        headers: 最后主文档 HTTP 响应头。

    Returns:
        保留其他元数据、声明 UTF-8 HTML 的响应头。
    """

    stale = {
        "content-type",
        "content-length",
        "content-encoding",
        "content-md5",
        "digest",
        "etag",
    }
    return [(name, value) for name, value in headers if name.lower() not in stale] + [
        ("Content-Type", "text/html; charset=utf-8")
    ]


@dataclass
class BrowserSessionDownloader:
    """同步双模式会话，保留一个 Tab；必须在创建它的线程使用和关闭。"""

    context: synchronous.BrowserContext  # 本会话独占的 Cookie 存储和代理边界。
    options: BrowserOptions  # 装配时固定的配置，不保存当前请求。
    _resources: ExitStack  # Context 及可选自建浏览器/驱动的关闭栈。
    after_load: Callable[[synchronous.Page], None] | None = (
        None  # 页面完成导航后的用户操作回调。
    )
    _page: synchronous.Page | None = field(
        default=None, init=False, repr=False
    )  # 懒创建且跨下载复用的单个 Tab。
    _document: synchronous.Response | None = field(
        default=None, init=False, repr=False
    )  # 当前 Tab 最后主文档响应。
    _thread: int = field(
        default_factory=get_ident, init=False, repr=False
    )  # Playwright 同步 API 所属线程。
    _closed: bool = field(default=False, init=False, repr=False)  # 关闭后不再接受下载。
    _busy: bool = field(
        default=False, init=False, repr=False
    )  # 阻止同一 Tab 的嵌套调用。

    def _check_owner(self) -> None:
        """检查同步驱动的线程归属。

        Raises:
            RuntimeError: 当前线程不是会话创建线程。
        """

        if get_ident() != self._thread:
            raise RuntimeError(
                "synchronous browser session must stay on its creating thread; use async sessions for event-loop execution"
            )

    def close(self) -> None:
        """关闭本会话资源，不关闭外部注入的浏览器。

        Raises:
            RuntimeError: 从错误线程关闭。
        """

        self._check_owner()
        if not self._closed:
            self._closed = True
            try:
                self._resources.close()
            finally:
                self._page = None
                self._document = None

    def _record_document(self, response: synchronous.Response) -> None:
        """跟踪最后一个主文档响应，覆盖回调触发的后续导航。

        Args:
            response: 当前 Tab 收到的原生响应。
        """

        if (
            self._page is not None
            and response.request.is_navigation_request()
            and response.request.frame == self._page.main_frame
        ):
            self._document = response

    def fetch(self, request: Request) -> Response:
        """按装配时选择的模式下载。

        Args:
            request: 当前请求，复制后使用。

        Returns:
            页面快照或 API 响应。
        """

        return self._public_fetch(request, self.options.mode)

    def fetch_page(self, request: Request) -> Response:
        """在会话的同一 Tab 中导航并运行页面脚本。

        Args:
            request: 不含请求体的 GET 请求。

        Returns:
            页面模式响应。
        """

        return self._public_fetch(request, "page")

    def fetch_api(self, request: Request) -> Response:
        """通过关联 APIRequestContext 请求，不使用浏览器 TLS 栈。

        Args:
            request: 当前 HTTP 请求。

        Returns:
            与 Tab 共享 Cookie 的 API 响应。
        """

        return self._public_fetch(request, "api")

    def _public_fetch(self, request: Request, mode: Mode) -> Response:
        """拒绝通过请求改写会话固定代理。

        Args:
            request: 当前请求。
            mode: 本次模式。

        Returns:
            当前下载结果。

        Raises:
            ValueError: 请求指定代理，必须在 prepare_session 中设置。
        """

        source = request.copy()
        if source.proxy is not None:
            raise ValueError(
                "browser session proxy is fixed; configure it in prepare_session"
            )
        return self._fetch(source, mode)

    def _fetch(self, source: Request, mode: Mode) -> Response:
        """串行执行请求，失败时关闭会话以停止残留页面活动。

        Args:
            source: 已复制且已装配代理的请求。
            mode: 本次模式。

        Returns:
            下载结果，导航超时为 status_code=-1。

        Raises:
            RuntimeError: 会话已关闭、线程错误或嵌套调用。
            Exception: 配置、回调和未分类浏览器错误继续传播。
        """

        self._check_owner()
        if self._closed or self._busy:
            raise RuntimeError(
                "browser session is closed or already executing a request"
            )
        headers = validate_request(source, mode)
        self._busy = True
        started = monotonic()
        try:
            if source.cookies:
                self.context.add_cookies(
                    [
                        {"name": name, "value": value, "url": source.real_url}
                        for name, value in source.cookies.items()
                    ]
                )
            result = (
                self._api(source, headers, started)
                if mode == "api"
                else self._navigate(source, headers, started)
            )
            if result.status_code == -1:
                self.close()
            return result
        except BaseException:
            with suppress(Exception):
                self.close()
            raise
        finally:
            self._busy = False

    def _api(
        self, source: Request, headers: dict[str, str], started: float
    ) -> Response:
        """完整缓冲 API 响应并立即释放原生响应内存。

        Args:
            source: 当前请求副本。
            headers: 已验证请求头。
            started: 下载开始时的单调时钟秒数。

        Returns:
            原始响应体和头，原生接口不提供逐跳历史或实际请求快照。
        """

        received = self.context.request.fetch(
            source.real_url,
            method=source.method,
            headers=headers,
            data=source.body,
            timeout=0 if source.timeout is None else source.timeout * 1000,
            fail_on_status_code=False,
            ignore_https_errors=not self.options.verify,
            max_redirects=self.options.max_redirects if source.allow_redirects else 0,
            max_retries=0,
        )
        try:
            return Response(
                received.body(),
                status_code=received.status,
                url=received.url,
                headers=[
                    (item["name"], item["value"]) for item in received.headers_array
                ],
                reason=received.status_text,
                request=source,
                cost=monotonic() - started,
            )
        finally:
            received.dispose()

    def _navigate(
        self, source: Request, headers: dict[str, str], started: float
    ) -> Response:
        """复用 Tab 导航，允许用户回调进行显式页面操作。

        Args:
            source: 当前 GET 请求副本。
            headers: 当前 Tab 的请求头，后续请求会重新设置。
            started: 下载开始时间。

        Returns:
            最后主文档原始响应或渲染后 HTML。

        Raises:
            RuntimeError: 导航没有提供主文档 HTTP 响应。
            Exception: 回调和未分类浏览器错误继续传播。
        """

        if self._page is None or self._page.is_closed():
            self._page = self.context.new_page()
            self._document = None
            self._page.on("response", self._record_document)
        page = self._page
        timeout = 0 if source.timeout is None else source.timeout * 1000
        page.set_default_timeout(timeout)
        page.set_extra_http_headers(headers)
        try:
            page.goto(
                source.real_url, timeout=timeout, wait_until=self.options.wait_until
            )
        except synchronous.TimeoutError as error:
            return Response(
                status_code=-1, error=error, request=source, cost=monotonic() - started
            )
        if self.after_load is not None:
            self.after_load(page)
        received = self._document
        if received is None:
            raise RuntimeError(
                "page navigation did not produce an HTTP document response"
            )
        response_headers = [
            (item["name"], item["value"]) for item in received.headers_array()
        ]
        html = self.options.content_mode == "html"
        content = page.content().encode("utf-8") if html else received.body()
        return Response(
            content,
            status_code=received.status,
            url=page.url if html else received.url,
            headers=_html_headers(response_headers) if html else response_headers,
            encoding="utf-8" if html else None,
            reason=received.status_text,
            request=source,
            cost=monotonic() - started,
        )


@dataclass
class AsyncBrowserSessionDownloader:
    """异步双模式会话，同一个事件循环中复用 Context 和单个 Tab。"""

    context: asynchronous.BrowserContext  # 当前 Slot 独立的代理及 Cookie 边界。
    options: BrowserOptions  # 创建时固定的配置。
    _resources: AsyncExitStack  # Context 及可选自建浏览器/驱动的清理栈。
    after_load: Callable[[asynchronous.Page], Awaitable[None]] | None = (
        None  # 显式异步页面操作。
    )
    _page: asynchronous.Page | None = field(
        default=None, init=False, repr=False
    )  # 懒创建的复用 Tab。
    _document: asynchronous.Response | None = field(
        default=None, init=False, repr=False
    )  # 最后主文档响应。
    _loop: asyncio.AbstractEventLoop = field(
        default_factory=asyncio.get_running_loop, init=False, repr=False
    )  # 会话所属循环。
    _closed: bool = field(default=False, init=False, repr=False)  # 关闭后不可再下载。
    _busy: bool = field(
        default=False, init=False, repr=False
    )  # 阻止多个任务并发操作同一 Tab。

    def _check_owner(self) -> None:
        """检查事件循环归属。

        Raises:
            RuntimeError: 当前事件循环不是创建会话的循环。
        """

        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError(
                "async browser session must stay on its creating event loop"
            )

    async def close(self) -> None:
        """关闭本会话资源，保留外部注入的 Browser。

        Raises:
            RuntimeError: 事件循环错误。
        """

        self._check_owner()
        if not self._closed:
            self._closed = True
            try:
                await self._resources.aclose()
            finally:
                self._page = None
                self._document = None

    def _record_document(self, response: asynchronous.Response) -> None:
        """记录当前 Tab 最后主文档的响应对象。

        Args:
            response: 浏览器响应事件。
        """

        if (
            self._page is not None
            and response.request.is_navigation_request()
            and response.request.frame == self._page.main_frame
        ):
            self._document = response

    async def fetch(self, request: Request) -> Response:
        """按默认模式异步下载。

        Args:
            request: 本次请求。

        Returns:
            页面快照或 API 响应。
        """

        return await self._public_fetch(request, self.options.mode)

    async def fetch_page(self, request: Request) -> Response:
        """在保留的 Tab 中导航和执行脚本。

        Args:
            request: 无请求体的 GET 请求。

        Returns:
            页面模式响应。
        """

        return await self._public_fetch(request, "page")

    async def fetch_api(self, request: Request) -> Response:
        """异步调用关联 APIRequestContext，保留浏览器 Cookie。

        Args:
            request: 当前 HTTP 请求。

        Returns:
            原始 API 响应，不携带浏览器 TLS 指纹。
        """

        return await self._public_fetch(request, "api")

    async def _public_fetch(self, request: Request, mode: Mode) -> Response:
        """检查会话代理边界并复制请求。

        Args:
            request: 本次请求。
            mode: 指定模式。

        Returns:
            下载结果。

        Raises:
            ValueError: 请求设置代理。
        """

        source = request.copy()
        if source.proxy is not None:
            raise ValueError(
                "browser session proxy is fixed; configure it in prepare_session"
            )
        return await self._fetch(source, mode)

    async def _fetch(self, source: Request, mode: Mode) -> Response:
        """串行使用 Context；失败和取消后关闭资源，避免遗留请求改变会话。

        Args:
            source: 当前请求副本。
            mode: 本次模式。

        Returns:
            下载结果。

        Raises:
            RuntimeError: 循环错误、会话关闭或重叠调用。
            asyncio.CancelledError: 清理后继续传播。
            Exception: 配置和未分类浏览器错误继续传播。
        """

        self._check_owner()
        if self._closed or self._busy:
            raise RuntimeError(
                "browser session is closed or already executing a request"
            )
        headers = validate_request(source, mode)
        self._busy = True
        started = monotonic()
        try:
            if source.cookies:
                await self.context.add_cookies(
                    [
                        {"name": name, "value": value, "url": source.real_url}
                        for name, value in source.cookies.items()
                    ]
                )
            result = (
                await self._api(source, headers, started)
                if mode == "api"
                else await self._navigate(source, headers, started)
            )
            if result.status_code == -1:
                await self.close()
            return result
        except BaseException:
            with suppress(Exception):
                await self.close()
            raise
        finally:
            self._busy = False

    async def _api(
        self, source: Request, headers: dict[str, str], started: float
    ) -> Response:
        """缓冲并释放异步 API 响应。

        Args:
            source: 当前请求副本。
            headers: 已验证的请求头。
            started: 下载开始时间。

        Returns:
            原始 API 响应，不伪造重定向历史。
        """

        received = await self.context.request.fetch(
            source.real_url,
            method=source.method,
            headers=headers,
            data=source.body,
            timeout=0 if source.timeout is None else source.timeout * 1000,
            fail_on_status_code=False,
            ignore_https_errors=not self.options.verify,
            max_redirects=self.options.max_redirects if source.allow_redirects else 0,
            max_retries=0,
        )
        try:
            return Response(
                await received.body(),
                status_code=received.status,
                url=received.url,
                headers=[
                    (item["name"], item["value"]) for item in received.headers_array
                ],
                reason=received.status_text,
                request=source,
                cost=monotonic() - started,
            )
        finally:
            await received.dispose()

    async def _navigate(
        self, source: Request, headers: dict[str, str], started: float
    ) -> Response:
        """复用同一 Tab，等待导航和用户页面操作后提取内容。

        Args:
            source: 当前 GET 请求副本。
            headers: 当前 Tab 的请求头。
            started: 下载开始时间。

        Returns:
            页面模式响应或导航超时失败响应。

        Raises:
            RuntimeError: 没有主文档响应。
            asyncio.CancelledError: 取消继续传播。
        """

        if self._page is None or self._page.is_closed():
            self._page = await self.context.new_page()
            self._document = None
            self._page.on("response", self._record_document)
        page = self._page
        timeout = 0 if source.timeout is None else source.timeout * 1000
        page.set_default_timeout(timeout)
        await page.set_extra_http_headers(headers)
        try:
            await page.goto(
                source.real_url, timeout=timeout, wait_until=self.options.wait_until
            )
        except asynchronous.TimeoutError as error:
            return Response(
                status_code=-1, error=error, request=source, cost=monotonic() - started
            )
        if self.after_load is not None:
            await self.after_load(page)
        received = self._document
        if received is None:
            raise RuntimeError(
                "page navigation did not produce an HTTP document response"
            )
        response_headers = [
            (item["name"], item["value"]) for item in await received.headers_array()
        ]
        html = self.options.content_mode == "html"
        content = (
            (await page.content()).encode("utf-8") if html else await received.body()
        )
        return Response(
            content,
            status_code=received.status,
            url=page.url if html else received.url,
            headers=_html_headers(response_headers) if html else response_headers,
            encoding="utf-8" if html else None,
            reason=received.status_text,
            request=source,
            cost=monotonic() - started,
        )


@dataclass(frozen=True)
class SyncBrowserDownloader(BrowserOptions, ABC):
    """同步浏览器装配模板，共用 Context、代理和临时会话生命周期。"""

    browser: synchronous.Browser | None = (
        None  # 可选外部浏览器，不由下载器关闭；多个 Slot 可共用。
    )
    after_load: Callable[[synchronous.Page], None] | None = (
        None  # 每次页面导航后的同步操作。
    )

    def __post_init__(self) -> None:
        """校验同步资源和回调。

        Raises:
            TypeError: 浏览器或回调类别不匹配。
        """

        super().__post_init__()
        if self.browser is not None and not isinstance(
            self.browser, synchronous.Browser
        ):
            raise TypeError("browser must be a synchronous Playwright Browser")
        if self.after_load is not None and (
            not callable(self.after_load)
            or inspect.iscoroutinefunction(self.after_load)
        ):
            raise TypeError("after_load must be a synchronous callable")

    @abstractmethod
    def _open_browser(self, resources: ExitStack) -> synchronous.Browser:
        """取得外部或自建浏览器，由具体后端实现。

        Args:
            resources: 自建资源的关闭栈。

        Returns:
            可用于创建独立 Context 的浏览器。
        """

    def prepare_session(self, *, proxy: str | None = None) -> BrowserSessionDownloader:
        """为当前 Slot 创建独立 Context，首次页面下载再创建 Tab。

        Args:
            proxy: 当前 Context 固定代理，不影响同浏览器其他 Context。

        Returns:
            支持 page/api 且共享 Cookie 的同步会话。
        """

        parameters = self.context_parameters(proxy)
        resources = ExitStack()
        try:
            browser = self._open_browser(resources)
            context = browser.new_context(**parameters)
            resources.callback(context.close)
            return BrowserSessionDownloader(context, self, resources, self.after_load)
        except BaseException:
            resources.close()
            raise

    def close_session(self, session: BrowserSessionDownloader) -> None:
        """在原线程关闭会话，可以重复调用。

        Args:
            session: 已停止使用的会话。
        """

        session.close()

    def fetch(self, request: Request) -> Response:
        """使用独立临时会话下载，结束后释放自建资源。

        Args:
            request: 待复制请求，proxy 用于本次 Context。

        Returns:
            所选模式的响应。
        """

        source = request.copy()
        validate_request(source, self.mode)
        session = self.prepare_session(proxy=source.proxy)
        try:
            return session._fetch(source, self.mode)
        finally:
            self.close_session(session)


@dataclass(frozen=True)
class AsyncBrowserDownloader(BrowserOptions, ABC):
    """异步浏览器装配模板，资源始终在创建它的事件循环内使用。"""

    browser: asynchronous.Browser | None = (
        None  # 外部浏览器由调用方管理，Context 由会话管理。
    )
    after_load: Callable[[asynchronous.Page], Awaitable[None]] | None = (
        None  # 显式异步页面操作。
    )

    def __post_init__(self) -> None:
        """校验异步浏览器和回调类型。

        Raises:
            TypeError: 浏览器或回调类别错误。
        """

        super().__post_init__()
        if self.browser is not None and not isinstance(
            self.browser, asynchronous.Browser
        ):
            raise TypeError("browser must be an asynchronous Playwright Browser")
        if self.after_load is not None and (
            not callable(self.after_load)
            or not inspect.iscoroutinefunction(self.after_load)
        ):
            raise TypeError("after_load must be an async callable")

    @abstractmethod
    async def _open_browser(self, resources: AsyncExitStack) -> asynchronous.Browser:
        """取得当前后端的异步浏览器。

        Args:
            resources: 自建浏览器和驱动的清理栈。

        Returns:
            用于创建会话 Context 的浏览器。
        """

    async def prepare_session(
        self, *, proxy: str | None = None
    ) -> AsyncBrowserSessionDownloader:
        """创建独立 Context 和固定代理，后续页面调用复用一个 Tab。

        Args:
            proxy: 当前 Context 的固定代理。

        Returns:
            可按 Slot 保存的异步会话。
        """

        parameters = self.context_parameters(proxy)
        resources = AsyncExitStack()
        try:
            browser = await self._open_browser(resources)
            creation = asyncio.create_task(browser.new_context(**parameters))
            try:
                context = await asyncio.shield(creation)
            except asyncio.CancelledError:
                # 原生创建操作不能被 Python Future 取消；等待后关闭，避免泄漏外部浏览器的 Context。
                with suppress(Exception):
                    created = await creation
                    await created.close()
                raise
            resources.push_async_callback(context.close)
            return AsyncBrowserSessionDownloader(
                context, self, resources, self.after_load
            )
        except BaseException:
            await resources.aclose()
            raise

    async def close_session(self, session: AsyncBrowserSessionDownloader) -> None:
        """在原事件循环内关闭会话，不关闭外部浏览器。

        Args:
            session: 已停止使用的会话。
        """

        await session.close()

    async def fetch(self, request: Request) -> Response:
        """使用独立临时会话异步下载并完成清理。

        Args:
            request: 待复制请求，proxy 只用于本次 Context。

        Returns:
            页面或 API 模式的响应。

        Raises:
            asyncio.CancelledError: 清理资源后继续传播。
        """

        source = request.copy()
        validate_request(source, self.mode)
        session = await self.prepare_session(proxy=source.proxy)
        try:
            return await session._fetch(source, self.mode)
        finally:
            await self.close_session(session)
