# -*- coding: utf-8 -*-
# @Time    : 2024-04-03
# @Author  : Kem
# @Desc    : niquests downloader — requests 的现代替代，原生支持 HTTP/1.1 / HTTP/2 / HTTP/3

from __future__ import absolute_import

import contextlib
import copy
import http.client
import urllib.parse
import warnings
from typing import Optional, Union

from bricks.downloader import AbstractDownloader
from bricks.lib.cookies import Cookies
from bricks.lib.request import Request
from bricks.lib.response import Response
from bricks.utils import pandora

warnings.filterwarnings("ignore")
# 设置最大响应头数量为 1000
http.client._MAXHEADERS = 1000  # type: ignore

pandora.require("niquests")

import niquests  # noqa: E402


class Downloader(AbstractDownloader):
    """
    对 niquests 进行的一层包装。

    niquests 是 requests 的 drop-in 替代，额外提供:
    - 原生 HTTP/2 和 HTTP/3 (QUIC) 支持
    - 多路复用 (multiplexed)：同一连接并发多个请求
    - AsyncSession：无缝异步支持
    - 与 requests API 100% 兼容

    兼容 Windows / Mac / Linux
    """

    def __init__(
        self,
        options: Optional[dict] = None,
        disable_http1: bool = False,
        disable_http2: bool = False,
        disable_http3: bool = False,
        multiplexed: bool = False,
    ):
        """
        :param options: 透传给 Session 的额外参数
        :param disable_http1: 禁用 HTTP/1.1（强制 H2/H3）
        :param disable_http2: 禁用 HTTP/2
        :param disable_http3: 禁用 HTTP/3 (QUIC)
        :param multiplexed: 启用多路复用（同一连接并发多请求，需 H2+）
        """
        self.options = options or {}
        self.disable_http1 = disable_http1
        self.disable_http2 = disable_http2
        self.disable_http3 = disable_http3
        self.multiplexed = multiplexed

    def fetch(self, request: Union[Request, dict]) -> Response:
        """
        使用 niquests 发送请求并获取响应

        :param request: Request 对象或包含请求参数的字典
        :return: Response 对象，当 stream=True 时 content 为迭代器
        """
        res = Response.make_response(request=request)

        use_stream = request.get_options("stream", False)
        chunk_size = request.get_options("chunk_size", 8192)

        options = {
            **self.options,
            "method": request.method.upper(),
            "headers": request.headers,
            "cookies": request.cookies,
            "data": self.parse_data(request)["data"],
            "files": request.options.get("files"),
            "auth": request.options.get("auth"),
            "timeout": 5 if request.timeout is ... else request.timeout,
            "allow_redirects": False if not use_stream else request.allow_redirects,
            "proxies": request.proxies
            and {"http": request.proxies, "https": request.proxies},  # noqa
            "verify": request.options.get("verify", False),
            "stream": use_stream,
            **request.options.get("$options", {}),
        }

        next_url = request.real_url
        _redirect_count = 0

        if request.use_session:
            session = request.get_options("$session") or self.get_session(
                disable_http1=self.disable_http1,
                disable_http2=self.disable_http2,
                disable_http3=self.disable_http3,
                multiplexed=self.multiplexed,
            )
            ctx = contextlib.nullcontext()
        else:
            session = niquests.Session(
                disable_http1=self.disable_http1,
                disable_http2=self.disable_http2,
                disable_http3=self.disable_http3,
                multiplexed=self.multiplexed,
            )
            ctx = session

        # 流式下载：不关闭连接，保持迭代器可用
        if use_stream:
            response = session.request(**{**options, "url": request.real_url})
            res.status_code = response.status_code
            res.headers = response.headers
            res.cookies = Cookies.by_jar(response.cookies)
            res.url = response.url
            res.request = request
            res._stream_iterator = response.iter_content(chunk_size=chunk_size)
            return res

        with ctx:
            while True:
                assert _redirect_count < 999, "已经超过最大重定向次数: 999"
                response = session.request(**{**options, "url": next_url})
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

    def make_session(self, **options) -> niquests.Session:
        _opts = {
            "disable_http1": self.disable_http1,
            "disable_http2": self.disable_http2,
            "disable_http3": self.disable_http3,
            "multiplexed": self.multiplexed,
            **options,
        }
        return niquests.Session(**_opts)


if __name__ == "__main__":
    downloader = Downloader()
    res = downloader.fetch({"url": "https://httpbin.org/get"})
    print(res.status_code, res.text[:200])
