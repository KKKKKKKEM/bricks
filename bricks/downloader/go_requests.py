# -*- coding: utf-8 -*-
# @Time    : 2023-12-10 22:07
# @Author  : Kem
# @Desc    :

from __future__ import absolute_import

import contextlib
import copy
import re
import urllib.parse
import warnings
from functools import wraps
from typing import Optional, Union

from bricks.downloader import AbstractDownloader
from bricks.lib.cookies import Cookies
from bricks.lib.headers import Header
from bricks.lib.request import Request
from bricks.lib.response import Response
from bricks.utils import pandora

warnings.filterwarnings("ignore")
pandora.require("requests-go")

import requests_go  # noqa: E402
from requests_go.tls_config import TLSConfig, to_tls_config  # noqa: E402
from requests_go.tls_config import convert_config


def decorator(func):
    @wraps(func)
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:  # noqa
            return None

    return inner


for k, v in convert_config.__dict__.items():  # noqa
    if k.startswith('get_') and callable(v):
        setattr(convert_config, k, decorator(v))

PROXY_ERROR_PARRTEN = re.compile(r"proxyconnect tcp:.*connection refused")


class Downloader(AbstractDownloader):
    """
    对 requests-go 进行的一层包装, 支持手动设置 tls
    兼容 Windows / Mac / Linux


    """

    def __init__(
            self,
            tls_config: Optional[Union[dict, TLSConfig]] = None,
            options: Optional[dict] = None,
    ) -> None:
        self.tls_config = tls_config
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
            **request.options.get("$options", {}),
        }

        tls_config = request.options.get("tls_config")
        if not tls_config:
            tls_config = self.tls_config
        tls_config = self.fmt_tls_config(tls_config)

        tls_config and options.update(tls_config=tls_config)  # type: ignore
        next_url = request.real_url
        _redirect_count = 0
        if request.use_session:
            session = request.get_options("$session") or self.get_session()
            ctx = contextlib.nullcontext()
        else:
            session = requests_go.Session()
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
                            cookies=Cookies.by_jar(response.cookies),
                            url=response.url,
                            status_code=response.status_code,
                            request=Request(
                                url=last_url,
                                method=request.method,
                                headers=copy.deepcopy(options.get("headers") or {}),
                            ),
                        )
                    )
                    request.options.get("$referer", False) and options["headers"].update(
                        Referer=response.url
                    )  # type: ignore

                else:
                    res.content = response.content
                    res.headers = Header(response.headers)
                    res.cookies = Cookies.by_jar(response.cookies)
                    res.url = response.url
                    res.status_code = response.status_code
                    res.request = request

                    return res

    def make_session(self):
        return requests_go.Session()

    @classmethod
    def fmt_tls_config(
            cls, tls_config: Optional[Union[dict, TLSConfig]] = None
    ) -> TLSConfig:
        """
        将 tls_config 直接转为 TLSConfig, 因为有时候直接传 dict 给 request_go 有问题
        :param tls_config:
        :return:
        """
        if not tls_config:
            return tls_config

        if isinstance(tls_config, dict):
            tls_config = to_tls_config(tls_config)
        assert isinstance(
            tls_config, TLSConfig
        ), "tls_config 需要为 dict 或者 TLSConfig"
        return tls_config

    def exception(self, request: Request, error: Exception):
        resp = super().exception(request, error)
        if PROXY_ERROR_PARRTEN.search(str(error)):
            resp.error = "ProxyError"
        return resp


if __name__ == "__main__":
    downloader = Downloader()
    rsp = downloader.fetch(
        Request(
            # url="https://gdupi/api/search/all?sort=rel&pagingIndex=1&pagingSize=40&viewType=list&productSet=total&query=iphone+16+pro&origQuery=iphone+16+pro&adQuery=iphone+16+pro&iq=&eq=&xq=&catId=50000247&minPrice=700000&maxPrice=1400000",
            url="https://www.baidu.com?s=baidu&w=&q=wocao",
            timeout=20,
            # proxies="http://127.0.0.1:7890"
        )
    )
    print(rsp.error, rsp.reason)
