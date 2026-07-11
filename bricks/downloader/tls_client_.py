from __future__ import absolute_import

import re
import warnings
from typing import Optional, Union

from bricks.downloader import AbstractDownloader
from bricks.lib.cookies import Cookies
from bricks.lib.request import Request
from bricks.lib.response import Response
from bricks.utils import pandora

warnings.filterwarnings("ignore")
pandora.require("tls-client")

import tls_client  # noqa: E402

PROXY_ERROR_PARRTEN = re.compile(r"dial tcp .*?: connect: connection refused")


class Downloader(AbstractDownloader):
    """
    对 tls_client 进行的一层包装, 支持手动设置 tls
    兼容 Windows / Mac / Linux


    """

    def __init__(
            self, tls_config: Optional[dict] = None, options: Optional[dict] = None
    ) -> None:
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
            "allow_redirects": request.allow_redirects,
            "proxy": request.proxies,  # noqa
            "insecure_skip_verify": request.options.get("verify", False),
            **request.options.get("$options", {}),
        }

        tls_config: dict = request.options.get("tls_config") or {}
        if not tls_config:
            tls_config = self.tls_config

        reuse_session = self.should_reuse_session(request)
        if reuse_session:
            session = request.get_options("$session") or self.get_session(**tls_config)
        else:
            session = tls_client.Session(**tls_config)
        try:
            response = session.execute_request(
                **{**options, "url": request.real_url}
            )
            res.content = response.content  # type: ignore
            res.headers = response.headers  # type: ignore
            res.cookies = Cookies.by_jar(response.cookies)
            res.url = response.url  # type: ignore
            res.status_code = response.status_code  # type: ignore
            res.request = request
            return res
        finally:
            not reuse_session and session.close()  # type: ignore

    def make_session(self, **kwargs):
        return tls_client.Session(**kwargs)

    def close_session(self, session: tls_client.Session):
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
            url="http://127.0.0.1:5555", proxies="http://127.0.0.1:7899", timeout=20
        )
    )
    print(rsp.error, rsp.reason)
