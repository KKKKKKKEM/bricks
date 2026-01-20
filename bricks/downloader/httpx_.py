# -*- coding: utf-8 -*-
# @Time    : 2023-12-10 20:56
# @Author  : Kem
# @Desc    : httpx downloader
from __future__ import absolute_import

import contextlib
import copy
import http.client
import re
import urllib.parse
import warnings
from typing import Optional, Union

from bricks.downloader import AbstractDownloader
from bricks.lib.cookies import Cookies
from bricks.lib.headers import Header
from bricks.lib.request import Request
from bricks.lib.response import Response
from bricks.utils import pandora

warnings.filterwarnings("ignore")
# 设置 requests 最大的响应头的长度 为 1000
http.client._MAXHEADERS = 1000  # type: ignore

pandora.require("httpx")

import httpx  # noqa: E402

PROXY_ERROR_PARRTEN = re.compile(r"\[Errno 111\] Connection refused")


class Downloader(AbstractDownloader):
    """
    对 httpx 进行的一层包装
    兼容 Windows / Mac / Linux


    """

    def __init__(self, options: Optional[dict] = None):
        self.options = options or {}

    def fetch(self, request: Union[Request, dict]) -> Response:
        """
        真使用 requests 发送请求并获取响应

        :param request:
        :return: `Response` 对象，当 stream=True 时，Response.content 为迭代器

        """

        res = Response.make_response(request=request)

        # 检查是否需要流式下载
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
            "follow_redirects": False if not use_stream else request.allow_redirects,
            "proxy": request.proxies,  # noqa
            "verify": request.options.get("verify", False),
        }

        next_url = request.real_url
        _redirect_count = 0
        httpversion: str = request.get_options("httpversion", "1.1")
        if httpversion.startswith("1"):
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
        # 流式下载不使用 with 自动关闭
        if use_stream:
            response = session.stream(**{**options, "url": request.real_url})
            response.__enter__()  # 手动进入上下文
            res.status_code = response.status_code
            res.headers = Header(response.headers)
            res.cookies = Cookies.by_jar(session.cookies.jar)
            res.url = str(response.url)
            res.request = request
            # 将迭代器放入 _stream_iterator，外部调用 iter_content() 或访问 content 时才会真正下载
            res._stream_iterator = response.iter_bytes(chunk_size=chunk_size)
            return res

        # 普通下载使用 try-finally 管理
        try:
            while True:
                assert _redirect_count < 999, "已经超过最大重定向次数: 999"
                response = session.request(**{**options, "url": next_url})
                last_url, next_url = (
                    next_url,
                    response.headers.get("location")
                    or response.headers.get("Location"),
                )
                if request.allow_redirects and next_url:
                    next_url = urllib.parse.urljoin(
                        str(response.url), next_url)
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
                                headers=copy.deepcopy(
                                    options.get("headers") or {}),
                            ),
                        )
                    )
                    request.options.get("$referer", False) and options[
                        "headers"
                    ].update(Referer=str(response.url))  # type: ignore

                else:
                    res.content = response.content
                    res.headers = Header(response.headers)
                    res.cookies = Cookies.by_jar(session.cookies.jar)
                    res.url = str(response.url)
                    res.status_code = response.status_code
                    res.request = request

                    return res
        finally:
            not request.use_session and session.close()  # type: ignore

    def make_session(self, **options):
        return httpx.Client(**options)

    def stream_fetch(self, request: Union[Request, dict], chunk_size: int = 8192):
        """
        流式发送请求并返回响应迭代器

        :param request: 请求对象
        :param chunk_size: 每次读取的块大小
        :return: (Response对象, content_iterator) 元组
        """
        request: Request
        httpversion: str = request.get_options("httpversion", "1.1")
        if httpversion.startswith("1"):
            http1, http2 = True, False
        else:
            http1, http2 = False, True

        options = {
            **self.options,
            "method": request.method.upper(),
            "headers": request.headers,
            "cookies": request.cookies,
            "data": self.parse_data(request)["data"],
            "timeout": 5 if request.timeout is ... else request.timeout,
            "follow_redirects": request.allow_redirects,
            "proxy": request.proxies,
            "verify": request.options.get("verify", False),
            **request.options.get("$options", {}),
        }

        if request.use_session:
            session = request.get_options("$session") or self.get_session(
                proxy=options.pop("proxy", None),
                verify=options.pop("verify", False),
                timeout=options.pop("timeout", 5),
                http1=http1,
                http2=http2,
            )
            ctx = contextlib.nullcontext()
        else:
            session = httpx.Client(
                proxy=options.pop("proxy", None),
                verify=options.pop("verify", False),
                timeout=options.pop("timeout", 5),
                http1=http1,
                http2=http2,
            )
            ctx = session

        try:
            with ctx:
                with session.stream(**{**options, "url": request.real_url}) as response:
                    # 构建 Response 对象（不包含 content）
                    res = Response.make_response(
                        content=b"",
                        status_code=response.status_code,
                        headers=dict(response.headers),
                        url=str(response.url),
                        cookies=Cookies.by_jar(response.cookies.jar) if hasattr(
                            response, 'cookies') else None,
                        request=request
                    )

                    # 返回响应对象和内容迭代器
                    return res, response.iter_bytes(chunk_size=chunk_size)
        finally:
            if not request.use_session and isinstance(session, httpx.Client):
                session.close()

    def exception(self, request: Request, error: Exception):
        resp = super().exception(request, error)
        if PROXY_ERROR_PARRTEN.search(str(error)):
            resp.error = "ProxyError"
        return resp


if __name__ == "__main__":
    downloader = Downloader()
    rsp = downloader.fetch(
        Request(
            url="http://127.0.0.1:5555", proxies="http://127.0.0.1:7890", timeout=20
        )
    )
    print(rsp, rsp.error, rsp.reason)
