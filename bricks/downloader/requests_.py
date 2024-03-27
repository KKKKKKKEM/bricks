# -*- coding: utf-8 -*-
# @Time    : 2023-12-10 20:56
# @Author  : Kem
# @Desc    : requests downloader
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

pandora.require("requests")

import requests  # noqa: E402


class Downloader(AbstractDownloader):
    """
    对 requests 进行的一层包装
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
            'allow_redirects': False,
            'proxies': request.proxies and {"http": request.proxies, "https": request.proxies},  # noqa
            'verify': request.options.get("verify", False),
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
                        cookies=Cookies.by_jar(response.cookies),
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
                res.cookies = Cookies.by_jar(response.cookies)
                res.url = response.url
                res.status_code = response.status_code
                res.request = request

                return res

    def make_session(self):
        return requests.Session()


if __name__ == '__main__':
    downloader = Downloader()
    res = downloader.fetch({"url": "http://www.baidu.com"})
    print(res)
