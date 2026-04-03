# -*- coding: utf-8 -*-
# @Time    : 2024-04-03
# @Author  : Kem
# @Desc    : primp downloader — Rust 实现的高性能 TLS 指纹浏览器伪装客户端

from __future__ import absolute_import

import copy
import re
import urllib.parse
from typing import Optional, Union

from bricks.downloader import AbstractDownloader
from bricks.lib.cookies import Cookies
from bricks.lib.request import Request
from bricks.lib.response import Response
from bricks.utils import pandora

pandora.require("primp")

import primp  # noqa: E402

PROXY_ERROR_PATTERN = re.compile(
    r"(Connection refused|proxy|CONNECT tunnel|ProxyError)",
    re.IGNORECASE,
)


class Downloader(AbstractDownloader):
    """
    对 primp 进行的一层包装。

    primp 是基于 Rust (rquest) 的 HTTP 客户端，支持完整的浏览器 TLS / JA3 / JA4 / HTTP2 指纹伪装:
    - 支持 Chrome / Firefox / Safari / Edge / Opera 等浏览器 profile
    - 独立 impersonate_os 参数 (windows / macos / linux / android / ios)
    - 指纹完整度高于 curl_cffi（JA4_o / JA4_ro 等细粒度字段全匹配）
    - 同时提供同步 Client 和异步 AsyncClient

    兼容 Windows / Mac / Linux
    """

    def __init__(
        self,
        impersonate: Optional[str] = None,
        impersonate_os: Optional[str] = None,
        options: Optional[dict] = None,
    ):
        """
        :param impersonate: 浏览器指纹，如 "chrome_146" / "firefox_148" / "safari_26" / "random"
        :param impersonate_os: 操作系统指纹，如 "windows" / "macos" / "linux" / "random"
        :param options: 透传给 primp.Client 的额外参数
        """
        self.impersonate = impersonate
        self.impersonate_os = impersonate_os
        self.options = options or {}

    def _build_client_kwargs(self, request: Request) -> dict:
        """组装 primp.Client 的构造参数。"""
        timeout = 5 if request.timeout is ... else request.timeout
        if isinstance(timeout, (list, tuple)):
            connect_timeout, read_timeout = timeout
        else:
            connect_timeout, read_timeout = None, None
            timeout = timeout

        kwargs = {
            **self.options,
            "proxy": request.proxies,
            "verify": request.options.get("verify", False),
            "follow_redirects": False,
            "impersonate": request.get_options("impersonate") or self.impersonate,
            "impersonate_os": request.get_options("impersonate_os")
            or self.impersonate_os,
            **request.options.get("$options", {}),
        }

        if connect_timeout is not None:
            kwargs["connect_timeout"] = connect_timeout
            kwargs["read_timeout"] = read_timeout
        else:
            kwargs["timeout"] = timeout

        # primp 不接受 None 的 impersonate，不设置时去掉该键
        if not kwargs.get("impersonate"):
            kwargs.pop("impersonate", None)
        if not kwargs.get("impersonate_os"):
            kwargs.pop("impersonate_os", None)

        return kwargs

    def _do_request(self, client: primp.Client, method: str, url: str, options: dict):
        """通过 primp.Client 发出单次 HTTP 请求。"""
        method = method.upper()
        fn = getattr(client, method.lower())
        return fn(url, **options)

    def fetch(self, request: Union[Request, dict]) -> Response:
        """
        使用 primp 发送请求并获取响应。

        :param request: Request 对象或包含请求参数的字典
        :return: Response 对象
        """
        res = Response.make_response(request=request)

        body_info = self.parse_data(request)
        content_type = body_info.get("type", "")
        body_data = body_info.get("data")

        # 根据 content-type 选择合适的参数名
        if body_data is None:
            req_kwargs = {}
        elif "application/json" in content_type:
            req_kwargs = {
                "content": body_data.encode()
                if isinstance(body_data, str)
                else body_data
            }
        elif "application/x-www-form-urlencoded" in content_type:
            # primp data 参数接受 dict，body 已是 urlencode 字符串，用 content 透传
            req_kwargs = {
                "content": body_data.encode()
                if isinstance(body_data, str)
                else body_data
            }
        else:
            req_kwargs = {
                "content": body_data if isinstance(body_data, bytes) else body_data
            }

        req_kwargs.update(
            {
                "headers": dict(request.headers) if request.headers else None,
                "cookies": dict(request.cookies) if request.cookies else None,
                **request.options.get("$req_options", {}),
            }
        )
        # 去掉 None 值，避免 primp 拒绝
        req_kwargs = {k: v for k, v in req_kwargs.items() if v is not None}

        client_kwargs = self._build_client_kwargs(request)

        next_url = request.real_url
        _redirect_count = 0

        if request.use_session:
            client = request.get_options("$session") or self.get_session(
                **client_kwargs
            )
        else:
            client = primp.Client(**client_kwargs)

        try:
            while True:
                assert _redirect_count < 999, "已经超过最大重定向次数: 999"
                response = self._do_request(
                    client, request.method, next_url, req_kwargs
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
                            headers=dict(response.headers),
                            cookies=Cookies.by_jar(
                                [
                                    {"name": k, "value": v, "domain": ""}
                                    for k, v in response.cookies.items()
                                ]
                            ),
                            url=response.url,
                            status_code=response.status_code,
                            request=Request(
                                url=last_url,
                                method=request.method,
                                headers=copy.deepcopy(req_kwargs.get("headers") or {}),
                            ),
                        )
                    )
                    request.options.get("$referer", False) and req_kwargs.get(
                        "headers", {}
                    ).update(Referer=response.url)
                else:
                    res.content = response.content
                    res.headers = dict(response.headers)
                    res.cookies = Cookies.by_jar(
                        [
                            {"name": k, "value": v, "domain": ""}
                            for k, v in response.cookies.items()
                        ]
                    )
                    res.url = response.url
                    res.status_code = response.status_code
                    res.request = request
                    return res
        finally:
            if not request.use_session:
                try:
                    client.close()
                except Exception:
                    pass

    def make_session(self, **kwargs) -> primp.Client:
        _opts = {
            **self.options,
            **kwargs,
        }
        if self.impersonate and "impersonate" not in _opts:
            _opts["impersonate"] = self.impersonate
        if self.impersonate_os and "impersonate_os" not in _opts:
            _opts["impersonate_os"] = self.impersonate_os
        return primp.Client(**_opts)

    def exception(self, request: Request, error: Exception):
        resp = super().exception(request, error)
        if PROXY_ERROR_PATTERN.search(str(error)):
            resp.error = "ProxyError"
        return resp


if __name__ == "__main__":
    downloader = Downloader(impersonate="chrome_146", impersonate_os="macos")
    rsp = downloader.fetch(Request(url="https://tls.peet.ws/api/all", timeout=20, headers={
        
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0',
    }))

    data = rsp.json()
    print(data)
