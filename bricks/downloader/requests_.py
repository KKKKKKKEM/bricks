# -*- coding: utf-8 -*-
# @Time    : 2023-12-10 20:56
# @Author  : Kem
# @Desc    : requests downloader
from __future__ import absolute_import

import contextlib
import http.client
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

    def __init__(self, options: dict = None):
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
            "allow_redirects": request.allow_redirects,
            "proxies": request.proxies
                       and {"http": request.proxies, "https": request.proxies},  # noqa
            "verify": request.options.get("verify", False),
            "stream": use_stream,
        }

        reuse_session = self.should_reuse_session(request)
        if reuse_session:
            session = request.get_options("$session") or self.get_session()
            ctx = contextlib.nullcontext()
        else:
            session = requests.Session()
            ctx = session

        # 流式下载不使用 with 自动关闭，保持连接打开
        if use_stream:
            response = session.request(**{**options, "url": request.real_url})
            res.status_code = response.status_code
            res.headers = response.headers
            res.cookies = Cookies.by_jar(response.cookies)
            res.url = response.url
            res.request = request
            # 将迭代器放入 _stream_iterator，外部调用 iter_content() 或访问 content 时才会真正下载
            res._stream_iterator = response.iter_content(chunk_size=chunk_size)
            return res

        # 普通下载使用 with 管理；标准重定向交给客户端处理。
        with ctx:
            response = session.request(**{**options, "url": request.real_url})
            res.content = response.content
            res.headers = response.headers
            res.cookies = Cookies.by_jar(response.cookies)
            res.url = response.url
            res.status_code = response.status_code
            res.history = self._make_history(response, request)
            res.request = request
            return res

    @staticmethod
    def _make_history(response, request: Request):
        history = []
        for item in response.history:
            prepared = item.request
            history.append(
                Response(
                    content=item.content,
                    headers=item.headers,
                    cookies=Cookies.by_jar(item.cookies),
                    url=item.url,
                    status_code=item.status_code,
                    reason=item.reason,
                    request=Request(
                        url=prepared.url,
                        method=prepared.method,
                        body=prepared.body,
                        headers=dict(prepared.headers),
                        use_session=request.use_session,
                    ),
                )
            )
        return history

    def make_session(self):
        return requests.Session()

    def close_session(self, session: requests.Session):
        session.close()

    def stream_fetch(self, request: Union[Request, dict], chunk_size: int = 8192):
        """
        流式发送请求并返回响应迭代器

        :param request: 请求对象
        :param chunk_size: 每次读取的块大小
        :return: (Response对象, content_iterator) 元组
        """
        request: Request
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
            and {"http": request.proxies, "https": request.proxies},
            "verify": request.options.get("verify", False),
            "stream": True,
        }

        reuse_session = self.should_reuse_session(request)
        if reuse_session:
            session = request.get_options("$session") or self.get_session()
            ctx = contextlib.nullcontext()
        else:
            session = requests.Session()
            ctx = session

        with ctx:
            response = session.request(**{**options, "url": request.real_url})

            # 构建 Response 对象（不包含 content）
            res = Response.make_response(
                content=b"",
                status_code=response.status_code,
                headers=response.headers,
                url=response.url,
                cookies=Cookies.by_jar(response.cookies),
                request=request
            )

            # 返回响应对象和内容迭代器
            return res, response.iter_content(chunk_size=chunk_size)


if __name__ == "__main__":
    downloader = Downloader()
    res = downloader.fetch({"url": "http://www.baidu.com"})
    print(res)
