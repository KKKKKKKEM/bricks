import functools
from typing import Literal, Union
from urllib.parse import urlparse

from bricks import Request, Response
from bricks.downloader import AbstractDownloader
from bricks.lib.cookies import Cookies
from bricks.utils import pandora

pandora.require("DrissionPage")
from DrissionPage import WebPage  # noqa: E402


class Downloader(AbstractDownloader):
    """
    对 DrissionPage 进行的一层包装
    兼容 Windows / Mac / Linux


    """

    reuse_session_by_default = False

    def __init__(self, mode: Literal["d", "s"] = "s", options: dict = None):
        """ """
        self.mode = mode
        self.options = options or {}

    def fetch(self, request: Union[Request, dict]) -> Response:
        """
        真使用 requests 发送请求并获取响应

        :param request:
        :return: `Response`

        """

        res = Response.make_response(request=request)

        session_or_options = request.get_options("session_or_options") or {}
        options = {
            **self.options,
            **request.options.get("$options", {}),
            "headers": dict(request.headers),
            "cookies": request.cookies,
            "data": self.parse_data(request)["data"],
            "files": request.options.get("files"),
            "auth": request.options.get("auth"),
            "timeout": 5 if request.timeout is ... else request.timeout,
            "allow_redirects": request.allow_redirects,
            "proxies": request.proxies
                       and {"http": request.proxies, "https": request.proxies},  # noqa
            "verify": request.options.get("verify", False),
        }

        reuse_session = self.should_reuse_session(request)
        if reuse_session:
            page = request.get_options("$session") or self.get_session(
                mode=self.mode, session_or_options=session_or_options
            )
        else:
            page = WebPage(mode=self.mode, session_or_options=session_or_options)

        if request.cookies and self.mode == "d":
            domain = urlparse(request.real_url).hostname
            cookies = [
                dict(name=k, value=v, domain=domain) for k, v in request.cookies.items()
            ]
            page.tab.set.cookies(cookies)

        if request.method.upper() == "POST":
            fun = page.post
        elif request.method.upper() == "GET":
            fun = page.get
        else:
            fun = functools.partial(page.session.request, method=request.method.upper())

        try:
            fun(**{**options, "url": request.real_url})
            if page.response:
                response = page.response
            else:
                response = Response.make_response(
                    url=page.url,
                    content=page.raw_data,
                    cookies=page.cookies(),
                )

            def get_cookies(item):
                raw_cookies = item.cookies
                if isinstance(raw_cookies, (dict, Cookies)):
                    return Cookies(raw_cookies)
                return Cookies.by_jar(raw_cookies)

            res.content = response.content
            res.headers = response.headers
            res.cookies = get_cookies(response)
            res.url = response.url
            res.status_code = response.status_code
            res.request = request
            return res
        finally:
            not reuse_session and page.close()

    def make_session(self, **kwargs):
        return WebPage(**kwargs)

    def close_session(self, session: WebPage):
        session.close()


if __name__ == "__main__":
    downloader = Downloader(mode="d")
    # rsp = downloader.fetch(Request(url="https://httpbin.org/cookies/set?freeform=123", use_session=True))
    # print(rsp.cookies)
    rsp = downloader.fetch(
        Request(
            url="https://httpbin.org/cookies",
            use_session=True,
            cookies={"freeform": "4564"},
        )
    )
    # rsp = downloader.fetch(Request(url="https://httpbin.org/get", use_session=True, cookies={"freeform": "123"}, proxies="http://127.0.0.1:7890"))
    print(rsp.text)
