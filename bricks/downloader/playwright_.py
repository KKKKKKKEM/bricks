import asyncio
import inspect
import os.path
import subprocess
from typing import Literal, Union, List, Awaitable, Callable
from urllib import parse
from urllib.parse import urlparse

from loguru import logger

from bricks import Request, Response
from bricks.downloader import AbstractDownloader
from bricks.lib.cookies import Cookies
from bricks.utils import pandora

pandora.require("playwright")
from playwright import async_api  # noqa: E402
from playwright._impl._api_structures import SetCookieParam  # noqa
from playwright._impl._driver import compute_driver_executable, get_driver_env  # noqa


class BrowserContext(async_api.PlaywrightContextManager):

    def __init__(
            self,
            driver: Literal["chromium", "firefox", "webkit"] = "chromium",
            options: dict = None,
            reuse: bool = True,
    ):
        self.browser: async_api.Browser = ...
        self.driver = driver
        self.reuse = reuse
        self.options = options or {}
        self._lock: asyncio.Lock = ...

        super().__init__()

    async def __aenter__(self) -> async_api.Browser:
        if self._lock is ...: self._lock = asyncio.Lock()

        async with self._lock:
            if self.browser is ...:
                manager = await super().__aenter__()
                browser_driver: async_api.BrowserType = getattr(manager, self.driver)
                self.browser = await browser_driver.launch(**self.options)

            return self.browser

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not self.reuse:
            if self._exit_was_called:
                return
            self._exit_was_called = True
            await self._connection.stop_async()


class Downloader(AbstractDownloader):
    """
    async playwright downloader

    """

    def __init__(
            self,
            driver: Literal["chromium", "firefox", "webkit"] = "chromium",
            debug: bool = False,
            scripts: list = None,
            mode: Literal["automation", "api"] = "automation",
            headless: bool = True,
            reuse: bool = True,
    ):
        """

        :param driver:
        """
        self.driver = driver
        self.debug = debug
        self.mode = mode
        self.headless = headless
        self.reuse = reuse
        self.scripts = scripts or []
        self.browser_context: BrowserContext = BrowserContext(driver=driver, reuse=self.reuse)

    def install(self):
        """
        安装 playwright 环境

        :return:
        """
        if self.debug:
            return

        driver_executable = compute_driver_executable()
        subprocess.run(
            [str(driver_executable), "install"], env=get_driver_env()
        )

    async def fetch(self, request: Union[Request, dict]) -> Response:

        res = Response.make_response(request=request)

        # 获取驱动
        driver: Literal["chromium", "firefox", "webkit"] = request.get_options('driver') or self.driver

        # 获取浏览器 launch 配置
        browser_options: dict = request.get_options('browser', {})
        context_options: dict = request.get_options('context', {})
        browser_options.setdefault("headless", self.headless)

        interceptors: dict = request.get_options('interceptors', {})

        # 可用拦截器
        request_interceptors: List[Callable[..., Union[Awaitable[None], None]]] = interceptors.get('request') or []
        response_interceptors: List[Callable[..., Union[Awaitable[None], None]]] = interceptors.get('response') or []
        browser_interceptors: List[Callable[..., Union[Awaitable[None], None]]] = interceptors.get('browser') or []
        context_interceptors: List[Callable[..., Union[Awaitable[None], None]]] = interceptors.get('context') or []
        bgoto_interceptors: List[Callable[..., Union[Awaitable[None], None]]] = interceptors.get('before_goto') or []
        agoto_interceptors: List[Callable[..., Union[Awaitable[None], None]]] = interceptors.get('after_goto') or []

        if self.mode == "automation":
            timeout = 60 * 1000 if request.timeout is ... else request.timeout * 1000
            options = {
                "timeout": timeout,
                "url": request.real_url,
                "wait_until": request.get_options("wait_until") or "networkidle",
                "referer": request.headers.get("referer") or None,

            }

        else:
            timeout = 30 * 1000 if request.timeout is ... else request.timeout * 1000
            options = {
                "url_or_request": request.url,
                "params": request.params,
                "timeout": timeout,
                "method": request.method.upper(),
                "ignore_https_errors": True,
                "data": self.parse_data(request)['data'],
                "headers": request.headers,
                "max_redirects": 999 if request.allow_redirects else 0,

            }

        proxies = self.parse_proxies_for_playwright(request.proxies)

        if self.reuse or request.use_session:
            self.browser_context.reuse = True
            self.browser_context.options = browser_options
            self.browser_context.driver = driver
        else:
            self.browser_context = BrowserContext(driver=driver, options=browser_options, reuse=self.reuse)

        async with self.browser_context as browser:
            for interceptor in browser_interceptors:
                assert inspect.isasyncgenfunction(interceptor)
                await pandora.invoke(
                    interceptor,
                    args=[browser],
                    annotations={
                        type(browser): browser,
                        Request: request
                    },
                    namespace={
                        "browser": browser,
                        "request": request,
                    }
                )

            if request.use_session:
                context = request.get_options("$session")
                if not context:
                    context = await self.get_session(**{
                        "$brwoser": browser,
                        **context_options,
                        "proxy": proxies,
                        "user_agent": request.headers.get("user-agent"),
                    })

            else:

                context = await browser.new_context(**{
                    **context_options,
                    "proxy": proxies,
                    "user_agent": request.headers.get("user-agent"),
                })
                await context.clear_cookies()

            try:
                for interceptor in context_interceptors:
                    assert inspect.isasyncgenfunction(interceptor)
                    await pandora.invoke(
                        interceptor,
                        args=[context],
                        annotations={
                            type(context): context,
                            type(browser): browser,
                            Request: request
                        },
                        namespace={
                            "context": context,
                            "browser": browser,
                            "request": request,
                        }
                    )

                scripts = request.get_options("scripts", [])
                # 为 context 注入脚本
                await self.injection_scripts(context, scripts=scripts)

                # 设置 context 的 Cookie
                if request.cookies:
                    domain = urlparse(request.real_url).hostname
                    cookies = [SetCookieParam(name=k, value=v, domain=domain) for k, v in request.cookies.items()]
                    await context.add_cookies(cookies)

                for interceptor in response_interceptors:
                    assert inspect.isasyncgenfunction(interceptor)
                    context.on('response', interceptor)
                else:
                    context.on('response', self.on_response(page_url=request.real_url, raw_response=res))

                page = await context.new_page()
                async with page:

                    for event, interceptor in request_interceptors:
                        assert inspect.isasyncgenfunction(interceptor)
                        await page.route(event, interceptor)
                    else:
                        await page.route('**', self.on_request(page_url=request.real_url, raw_request=request))

                    for interceptor in bgoto_interceptors:
                        assert inspect.isasyncgenfunction(interceptor)
                        await pandora.invoke(
                            interceptor,
                            args=[page],
                            annotations={
                                type(page): page,
                                type(context): context,
                                type(browser): browser,
                                Request: request
                            },
                            namespace={
                                "page": page,
                                "context": context,
                                "browser": browser,
                                "request": request,
                            }
                        )

                    if self.mode == "automation":
                        response = await page.goto(**options)
                        res.content = await page.content()

                    else:
                        response = await page.request.fetch(**options)
                        res.content = await response.body()

                    res.url = response.url
                    res.headers = response.headers
                    res.status_code = response.status
                    res.cookies = Cookies.by_jar(await context.cookies())

                    for interceptor in agoto_interceptors:
                        assert inspect.isasyncgenfunction(interceptor)
                        await pandora.invoke(
                            interceptor,
                            args=[page],
                            annotations={
                                type(page): page,
                                type(context): context,
                                type(browser): browser,
                                type(response): response,
                                Request: request,
                                Response: res
                            },
                            namespace={
                                "page": page,
                                "context": context,
                                "browser": browser,
                                "request": request,
                                "response": res,
                            }
                        )

                    return res

            finally:
                not request.use_session and await context.close()

    @staticmethod
    def on_response(page_url, raw_response: Response):
        async def inner(response: async_api.Response):
            if page_url == response.url or page_url + "/" == response.url:
                return
            else:
                content = await response.body()
                req_body = parse.unquote_plus(response.request.post_data) if response.request.post_data else None
                raw_response.history.append(
                    Response(
                        content=content,
                        headers=response.headers,
                        url=response.url,
                        status_code=response.status,
                        request=Request(
                            headers=response.request.headers,
                            body=req_body,
                            url=response.request.url,
                            method=response.request.method,
                        ),
                    )
                )

        return inner

    @staticmethod
    def on_request(page_url, raw_request: Request):

        async def inner(route: async_api.Route, request: async_api.Request):
            if page_url == request.url or page_url + "/" == request.url:
                await route.continue_(headers={**request.headers, **raw_request.headers})
            else:
                await route.continue_()

        return inner

    @staticmethod
    def parse_proxies_for_playwright(_proxy: str):
        if not _proxy:
            return None

        if not _proxy.startswith('http'):
            _proxy = 'http://' + _proxy

        o = urlparse(_proxy)

        if not o.username:
            return {'server': o.netloc}
        else:
            return {"username": o.username, "password": o.password, "server": f'{o.hostname}:{o.port}'}

    async def injection_scripts(self, conn: Union[async_api.BrowserContext, async_api.Page], scripts: list = None):
        scripts = scripts or self.scripts
        for script in scripts:
            if os.path.exists(script):
                _script = {'path': script}
            else:
                _script = {'script': script}
            await conn.add_init_script(**_script)

    async def make_session(self, **kwargs):
        browser = kwargs.pop('$brwoser')
        return await browser.new_context(**kwargs)

    async def get_session(self, **options):
        """
        获取当前会话

        :return:
        """
        session = getattr(self.local, f"{self.__class__}$session", None)
        if not session:
            session = await self.make_session(**options)

        return session

    async def clear_session(self):
        if hasattr(self.local, f"{self.__class__}$session"):
            try:
                old_session: async_api.BrowserContext = getattr(self.local, f"{self.__class__}$session")
                await old_session.close()
            except Exception as e:
                logger.error(f'[清空 session 失败] 失败原因: {str(e) or str(e.__class__.__name__)}', error=e)


if __name__ == '__main__':
    downloader = Downloader(mode='api', reuse=False, headless=False)
    downloader.debug = True


    async def main():
        rsp = await downloader.fetch(Request(url="https://httpbin.org/cookies/set?freeform=123", use_session=True))
        print(rsp.cookies)
        rsp = await downloader.fetch(Request(url="https://httpbin.org/cookies", use_session=True))
        print(rsp.text)
        rsp = await downloader.fetch(Request(url="https://httpbin.org/cookies"))
        print(rsp.text)

        rsp = await downloader.fetch(Request(url="https://httpbin.org/cookies", use_session=True))
        print(rsp.text)


    asyncio.run(main())
