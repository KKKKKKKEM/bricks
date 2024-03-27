# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 14:14
# @Author  : Kem
# @Desc    :

import copy
import urllib.parse
from typing import Union

from curl_cffi import requests

from bricks.downloader import AbstractDownloader
from bricks.lib.cookies import Cookies
from bricks.lib.request import Request
from bricks.lib.response import Response


class Downloader(AbstractDownloader):
    """
    对 cffi 进行的一层包装, 类似 requests, tls 与浏览器保持一致
    兼容 Windows / Mac / Linux


    """

    def __init__(self, impersonate: Union[requests.BrowserType, str] = None):
        if isinstance(impersonate, requests.BrowserType):
            impersonate = impersonate.value

        self.impersonate = impersonate

    def fetch(self, request: Union[Request, dict]) -> Response:
        """
        真使用 requests 发送请求并获取响应

        :param request:
        :return: `Response`

        """

        res = Response.make_response(request=request)
        options = {
            'method': request.method.upper(),
            'headers': request.headers,
            'cookies': request.cookies,
            "data": self.parse_data(request)['data'],
            'files': request.options.get('files'),
            'auth': request.options.get('auth'),
            'timeout': 5 if request.timeout is ... else request.timeout,
            'allow_redirects': False,
            'proxies': request.proxies and {"http": request.proxies, "https": request.proxies},  # noqa
            'verify': request.options.get("verify", False),
            'impersonate': request.options.get("impersonate") or self.impersonate,
            'http_version': request.options.get("http_version"),
        }

        next_url = request.real_url
        _redirect_count = 0

        if request.use_session:
            session = request.get_options("$session") or self.get_session()
        else:
            session = requests

        while True:
            assert _redirect_count < 999, "已经超过最大重定向次数: 999"
            response = session.request(**{**options, "url": next_url})
            last_url, next_url = next_url, response.headers.get('location') or response.headers.get('Location')
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
                            headers=copy.deepcopy(options.get('headers'))
                        )
                    )
                )
                request.options.get('$referer', False) and options['headers'].update(Referer=response.url)

            else:
                res.content = response.content
                res.headers = response.headers
                res.cookies = Cookies.by_jar(response.cookies.jar)
                res.url = response.url
                res.status_code = response.status_code
                res.request = request

                return res

    def make_session(self) -> requests.Session:
        return requests.Session()


if __name__ == '__main__':
    downloader = Downloader()
    rsp = downloader.fetch(Request(url="https://httpbin.org/cookies/set?freeform=123", use_session=True))
    print(rsp.cookies)
    rsp = downloader.fetch(Request(url="https://httpbin.org/cookies", use_session=True))
    print(rsp.text)
