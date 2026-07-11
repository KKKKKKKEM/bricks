# -*- coding: utf-8 -*-
# @Time    : 2023-12-10 20:56
# @Author  : Kem
# @Desc    : pyhttpx downloader
from __future__ import absolute_import

import warnings
from typing import Union

from bricks.downloader import AbstractDownloader
from bricks.lib.cookies import Cookies
from bricks.lib.request import Request
from bricks.lib.response import Response
from bricks.utils import pandora

warnings.filterwarnings("ignore")

pandora.require("pyhttpx")

import pyhttpx  # noqa: E402


class Downloader(AbstractDownloader):
    """
    对 pyhttpx 进行的一层包装
    兼容 Windows / Mac / Linux


    """

    def __init__(self, options: dict = None):
        self.options = options or {}

    def fetch(self, request: Union[Request, dict]) -> Response:
        """
        真使用 requests 发送请求并获取响应

        :param request:
        :return: `Response`

        """

        res = Response.make_response(request=request)
        options = {
            **self.options,
            **request.options.get("$options", {}),
            "method": request.method.upper(),
            "headers": request.headers,
            "cookies": request.cookies,
            "data": self.parse_data(request)["data"],
            "timeout": 5 if request.timeout is ... else request.timeout,
            "allow_redirects": request.allow_redirects,
            "proxies": request.proxies
                       and {"http": request.proxies, "https": request.proxies},  # noqa
            "verify": request.options.get("verify", False),
        }

        reuse_session = self.should_reuse_session(request)
        if reuse_session:
            session = request.get_options("$session") or self.get_session(
                ja3=request.get_options("ja3"),
                exts_payload=request.get_options("exts_payload"),
                browser_type=request.get_options("browser_type"),
                http2=request.get_options("http2", False),
                shuffle_proto=request.get_options("shuffle_proto", False),
            )
        else:
            session = pyhttpx.HttpSession(
                ja3=request.get_options("ja3"),
                exts_payload=request.get_options("exts_payload"),
                browser_type=request.get_options("browser_type"),
                http2=request.get_options("http2", False),
            )

        try:
            response = session.request(**{**options, "url": request.real_url})

            def get_cookies(item):
                return Cookies.by_jar(
                    [
                        {"name": key, "value": value, "domain": ""}
                        for key, value in item.cookies.items()
                    ]
                )

            res.content = response.content
            res.headers = response.headers
            res.cookies = get_cookies(response)
            res.url = str(getattr(response, "url", request.real_url))
            res.status_code = response.status_code
            res.request = request
            return res
        finally:
            not reuse_session and session.close()

    def make_session(self, **options):
        return pyhttpx.HttpSession(**options)

    def close_session(self, session: pyhttpx.HttpSession):
        session.close()


if __name__ == "__main__":
    downloader = Downloader()
    downloader.debug = True
    rsp = downloader.fetch(
        Request(url="https://httpbin.org/cookies/set?freeform=123", use_session=False)
    )
    # 不知道为什么 pyhttpx 会多一次重定向
    print(rsp.history[0].cookies)
    rsp = downloader.fetch(
        Request(url="https://httpbin.org/cookies", use_session=False)
    )
    print(rsp.text)
