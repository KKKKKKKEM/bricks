from __future__ import absolute_import

import copy
import urllib.parse
import warnings
from typing import Union

from bricks.downloader import AbstractDownloader
from bricks.lib.cookies import Cookies
from bricks.lib.request import Request
from bricks.lib.response import Response
from bricks.utils import pandora

warnings.filterwarnings("ignore")
pandora.require("tls-client")

import tls_client  # noqa: E402


class Downloader(AbstractDownloader):
    """
    对 tls_client 进行的一层包装, 支持手动设置 tls
    兼容 Windows / Mac / Linux


    """

    def __init__(self, tls_config: dict = None, options: dict = None) -> None:
        self.tls_config = tls_config or {}
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
            "timeout_seconds": 5 if request.timeout is ... else request.timeout,
            "allow_redirects": False,
            "proxy": request.proxies,  # noqa
            "insecure_skip_verify": request.options.get("verify", False),
            **request.options.get("$options", {}),
        }

        next_url = request.real_url
        _redirect_count = 0
        tls_config: dict = request.options.get("tls_config")
        if not tls_config:
            tls_config = self.tls_config

        if request.use_session:
            session = request.get_options("$session") or self.get_session(**tls_config)
        else:
            session = tls_client.Session(**tls_config)
        try:
            while True:
                assert _redirect_count < 999, "已经超过最大重定向次数: 999"
                response = session.execute_request(**{**options, "url": next_url})
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
        finally:
            not request.use_session and session.close()

    def make_session(self, **kwargs):
        return tls_client.Session(**kwargs)


if __name__ == "__main__":
    downloader = Downloader()
    rsp = downloader.fetch(
        Request(url="https://httpbin.org/cookies/set?freeform=123", use_session=True)
    )
    print(rsp.cookies)
    rsp = downloader.fetch(Request(url="https://httpbin.org/cookies", use_session=True))
    print(rsp.text)
