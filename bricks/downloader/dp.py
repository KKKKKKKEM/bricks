import copy
import functools
import urllib
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

        next_url = request.real_url
        _redirect_count = 0
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
            "allow_redirects": False,
            "proxies": request.proxies
                       and {"http": request.proxies, "https": request.proxies},  # noqa
            "verify": request.options.get("verify", False),
        }

        if request.use_session:
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
            while True:
                assert _redirect_count < 999, "已经超过最大重定向次数: 999"

                fun(**{**options, "url": next_url})
                if page.response:
                    response = page.response
                else:
                    response = Response.make_response(
                        url=page.url,
                        content=page.raw_data,
                        cookies=page.cookies(),
                    )

                last_url, next_url = (
                    next_url,
                    response.headers.get("location")
                    or response.headers.get("Location"),
                )
                if request.allow_redirects and next_url:
                    next_url = urllib.parse.urljoin(response.url, next_url)
                    _redirect_count += 1
                    res.history.append(
                        Response(
                            content=response.content,
                            headers=response.headers,
                            cookies=Cookies.by_jar(response.cookies),
                            url=response.url,
                            status_code=response.status_code,
                            request=Request(
                                url=last_url,
                                method=request.method,
                                headers=copy.deepcopy(options.get("headers")),
                            ),
                        )
                    )
                    request.options.get("$referer", False) and options[
                        "headers"
                    ].update(Referer=response.url)

                else:
                    res.content = response.content
                    res.headers = response.headers
                    res.cookies = Cookies.by_jar(response.cookies)
                    res.url = response.url
                    res.status_code = response.status_code
                    res.request = request

                    return res
        finally:
            not request.use_session and page.close()

    def make_session(self, **kwargs):
        return WebPage(**kwargs)


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
