# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 14:14
# @Author  : Kem
# @Desc    :
import contextlib
import copy
import re
import urllib.parse
from typing import Optional, Union

from curl_cffi import requests

from bricks.downloader import AbstractDownloader
from bricks.lib.cookies import Cookies
from bricks.lib.request import Request
from bricks.lib.response import Response

CURL_CODE = re.compile(r"Failed to perform, curl: \((\d+)\)")


class Downloader(AbstractDownloader):
    """
    对 cffi 进行的一层包装, 类似 requests, tls 与浏览器保持一致
    兼容 Windows / Mac / Linux


    """

    def __init__(
            self,
            impersonate: Optional[Union[requests.BrowserType, str]] = None,
            options: Optional[dict] = None,
    ):
        if isinstance(impersonate, requests.BrowserType):
            impersonate = impersonate.value

        self.impersonate = impersonate
        self.options = options or {}

    def fetch(self, request: Union[Request, dict]) -> Response:
        """
        真使用 requests 发送请求并获取响应

        :param request:
        :return: `Response`

        """
        request: Request
        res = Response.make_response(request=request)
        options = {
            **self.options,
            "method": request.method.upper(),
            "headers": request.headers,
            "cookies": request.cookies,
            "data": self.parse_data(request)["data"],
            "files": request.options.get("files"),
            "auth": request.options.get("auth"),
            "timeout": 5 if request.timeout is ... else request.timeout,
            "allow_redirects": False,
            "proxies": request.proxies
                       and {"http": request.proxies, "https": request.proxies},  # noqa
            "verify": request.options.get("verify", False),
            "impersonate": request.options.get("impersonate") or self.impersonate,
            **request.options.get("$options", {}),
        }

        next_url = request.real_url
        _redirect_count = 0

        if request.use_session:
            session = request.get_options("$session") or self.get_session()
            ctx = contextlib.nullcontext()
        else:
            session = requests.Session()
            ctx = session

        with ctx:

            while True:
                assert _redirect_count < 999, "已经超过最大重定向次数: 999"
                response = session.request(**{**options, "url": next_url})
                last_url, next_url = (
                    next_url,
                    response.headers.get("location") or response.headers.get("Location"),
                )
                if request.allow_redirects and next_url:
                    next_url = urllib.parse.urljoin(response.url, next_url)
                    _redirect_count += 1
                    res.history.append(
                        Response(
                            content=response.content,
                            headers=response.headers,
                            cookies=Cookies.by_jar(response.cookies.jar),
                            url=response.url,
                            status_code=response.status_code,
                            request=Request(
                                url=last_url,
                                method=request.method,
                                headers=copy.deepcopy(options.get("headers")),  # type: ignore
                            ),
                        )
                    )
                    request.options.get("$referer", False) and options["headers"].update(
                        Referer=response.url
                    )  # type: ignore

                else:
                    res.content = response.content
                    res.headers = response.headers  # type: ignore
                    res.cookies = Cookies.by_jar(response.cookies.jar)
                    res.url = response.url
                    res.status_code = response.status_code
                    res.request = request

                    return res

    def make_session(self) -> requests.Session:
        return requests.Session()

    def exception(self, request: Request, error: Exception):
        resp = super().exception(request, error)
        code = CURL_CODE.search(str(error))
        if code and code.group(1) in (
                # Could not resolve proxy. The given proxy host could not be resolved.
                "5"
                # Failed to connect() to host or proxy.
                "7"
                # Proxy handshake error
                "97"
        ):
            resp.error = "ProxyError"
        return resp


if __name__ == "__main__":
    downloader = Downloader()
    rsp = downloader.fetch(
        Request(url="https://youtube.com", proxies="http://127.0.0.1:7899", timeout=20)
    )
    print(rsp.error, rsp.reason)
