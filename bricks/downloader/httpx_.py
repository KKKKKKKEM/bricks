# -*- coding: utf-8 -*-
# @Time    : 2023-12-10 20:56
# @Author  : Kem
# @Desc    : httpx downloader
from __future__ import absolute_import

import copy
import http.client
import urllib.parse
import warnings
from typing import Union

from bricks.downloader import AbstractDownloader
from bricks.lib.cookies import Cookies
from bricks.lib.request import Request
from bricks.lib.response import Response
from bricks.utils import pandora

warnings.filterwarnings("ignore")
# 设置 requests 最大的响应头的长度 为 1000
http.client._MAXHEADERS = 1000

pandora.require("httpx")

import httpx  # noqa: E402


class Downloader(AbstractDownloader):
    """
    对 httpx 进行的一层包装
    兼容 Windows / Mac / Linux


    """

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
            'follow_redirects': False,
            'proxy': request.proxies,  # noqa
            'verify': request.options.get("verify", False),
        }

        next_url = request.real_url
        _redirect_count = 0
        httpversion: str = request.get_options("httpversion", "1.1")
        if httpversion.startswith('1'):
            http1, http2 = True, False
        else:
            http1, http2 = False, True

        if request.use_session:

            session = request.get_options("$session") or self.get_session(
                proxy=options.pop("proxy", None),
                verify=options.pop("verify", False),
                timeout=options.pop("timeout", 5),
                http1=http1,
                http2=http2,

            )
        else:
            session = httpx.Client(
                proxy=options.pop("proxy", None),
                verify=options.pop("verify", False),
                timeout=options.pop("timeout", 5),
                http1=http1,
                http2=http2,
            )
        try:
            while True:
                assert _redirect_count < 999, "已经超过最大重定向次数: 999"
                response = session.request(**{**options, "url": next_url})
                last_url, next_url = next_url, response.headers.get('location') or response.headers.get('Location')
                if request.allow_redirects and next_url:
                    next_url = urllib.parse.urljoin(str(response.url), next_url)
                    _redirect_count += 1
                    res.history.append(
                        Response(
                            content=response.content,
                            headers=dict(response.headers),
                            cookies=Cookies.by_jar(response.cookies.jar),
                            url=str(response.url),
                            status_code=response.status_code,
                            request=Request(
                                url=last_url,
                                method=request.method,
                                headers=copy.deepcopy(options.get('headers'))
                            )
                        )
                    )
                    request.options.get('$referer', False) and options['headers'].update(Referer=str(response.url))

                else:
                    res.content = response.content
                    res.headers = dict(response.headers)
                    res.cookies = Cookies.by_jar(session.cookies.jar)
                    res.url = response.url
                    res.status_code = response.status_code
                    res.request = request

                    return res
        finally:
            not request.use_session and session.close()

    def make_session(self, **options):
        return httpx.Client(**options)


if __name__ == '__main__':
    downloader = Downloader()
    rsp = downloader.fetch(Request(url="https://httpbin.org/cookies/set?freeform=123", use_session=True))
    print(rsp.cookies)
    rsp = downloader.fetch(Request(url="https://httpbin.org/cookies", use_session=True))
    print(rsp.text)
