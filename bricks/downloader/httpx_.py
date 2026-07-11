# -*- coding: utf-8 -*-
# @Time    : 2023-12-10 20:56
# @Author  : Kem
# @Desc    : httpx downloader
from __future__ import absolute_import

import contextlib
import http.client
import re
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
            "follow_redirects": request.allow_redirects,
            "proxy": request.proxies,  # noqa
            "verify": request.options.get("verify", False),
        }

        httpversion: str = request.get_options("httpversion", "1.1")
        if httpversion.startswith("1"):
            http1, http2 = True, False
        else:
            http1, http2 = False, True

        client_options = {
            "proxy": options.pop("proxy", None),
            "verify": options.pop("verify", False),
            "timeout": options.pop("timeout", 5),
            "http1": http1,
            "http2": http2,
        }
        reuse_session = self.should_reuse_session(request)
        if reuse_session:
            session = request.get_options("$session") or self.get_session(
                **client_options,
            )
        else:
            session = httpx.Client(
                **client_options,
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

        # 普通下载使用 try-finally 管理；标准重定向交给客户端处理。
        try:
            response = session.request(**{**options, "url": request.real_url})
            res.content = response.content
            res.headers = Header(response.headers)
            res.cookies = Cookies.by_jar(session.cookies.jar)
            res.url = str(response.url)
            res.status_code = response.status_code
            res.history = self._make_history(response, request)
            res.request = request
            return res
        finally:
            not reuse_session and session.close()  # type: ignore

    @staticmethod
    def _make_history(response, request: Request):
        history = []
        for item in response.history:
            sent = item.request
            history.append(
                Response(
                    content=item.content,
                    headers=dict(item.headers),
                    cookies=Cookies.by_jar(item.cookies.jar),
                    url=str(item.url),
                    status_code=item.status_code,
                    reason=item.reason_phrase,
                    request=Request(
                        url=str(sent.url),
                        method=sent.method,
                        body=sent.content,
                        headers=dict(sent.headers),
                        use_session=request.use_session,
                    ),
                )
            )
        return history

    def make_session(self, **options):
        return httpx.Client(**options)

    def close_session(self, session: httpx.Client):
        session.close()

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

        client_options = {
            "proxy": options.pop("proxy", None),
            "verify": options.pop("verify", False),
            "timeout": options.pop("timeout", 5),
            "http1": http1,
            "http2": http2,
        }
        reuse_session = self.should_reuse_session(request)
        if reuse_session:
            session = request.get_options("$session") or self.get_session(
                **client_options,
            )
            ctx = contextlib.nullcontext()
        else:
            session = httpx.Client(
                **client_options,
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
            if not reuse_session and isinstance(session, httpx.Client):
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
