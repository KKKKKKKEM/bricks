"""可选 requests 同步下载器，使用缓冲响应和外部管理的会话。"""

from __future__ import annotations

from dataclasses import dataclass, field
from time import monotonic

import requests

from ..models import Request, Response
from ._options import TransportOptions
from ._requests_common import _response, _sent_request


def _fetch(client: requests.Session, source: Request) -> Response:
    """使用外部 Session 下载，不静默合并重复请求头。

    Args:
        client: 本次调用使用的同步会话。
        source: 已复制的领域请求。

    Returns:
        HTTP 响应或携带网络异常的失败响应。

    Raises:
        ValueError: 请求头重复，requests 无法保持该契约。
        Exception: 配置和程序错误继续传播。
    """

    names = [name.lower() for name, _ in source.headers.raw]
    if len(names) != len(set(names)):
        raise ValueError(
            "requests downloader does not support duplicate request headers"
        )
    started = monotonic()
    try:
        received = client.request(
            source.method,
            source.real_url,
            headers=dict(source.headers.raw),
            cookies=dict(source.cookies),
            data=source.body,
            proxies=dict(client.proxies),
            timeout=source.timeout,
            allow_redirects=source.allow_redirects,
        )
    except (
        requests.ConnectionError,
        requests.Timeout,
        requests.TooManyRedirects,
        requests.exceptions.ChunkedEncodingError,
        requests.exceptions.ContentDecodingError,
    ) as error:
        return Response(
            status_code=-1,
            error=error,
            request=_sent_request(source, error.request)
            if isinstance(error.request, requests.PreparedRequest)
            else source,
            cost=monotonic() - started,
        )
    with received:
        return _response(source, received).copy(
            history=tuple(_response(source, item) for item in received.history),
            cost=monotonic() - started,
        )


class RequestsDownloader(TransportOptions):
    """可选同步下载器，普通 fetch 不跨请求保留会话状态。"""

    def prepare_session(self, *, proxy: str | None = None) -> RequestsSessionDownloader:
        """创建独立 Session，由调用方装配到 Slot。

        Args:
            proxy: 会话固定代理，None 时由 trust_env 决定是否读取环境代理。

        Returns:
            由调用方负责成对关闭的会话下载器。
        """

        client = requests.Session()
        client.verify = self.verify
        client.trust_env = self.trust_env
        client.max_redirects = self.max_redirects
        if proxy is not None:
            client.proxies = {"http": proxy, "https": proxy}
        return RequestsSessionDownloader(client)

    def close_session(self, session: RequestsSessionDownloader) -> None:
        """关闭会话并禁止通过该包装器重新打开连接池。

        Args:
            session: 已无在途请求的会话，允许重复关闭。
        """

        session.client.close()
        session._closed = True

    def fetch(self, request: Request) -> Response:
        """使用独立临时会话完成缓冲下载。

        Args:
            request: 复制后发送的领域请求。

        Returns:
            HTTP 响应或传输失败响应，非传输异常继续传播。
        """

        source = request.copy()
        session = self.prepare_session(proxy=source.proxy)
        try:
            return _fetch(session.client, source)
        finally:
            self.close_session(session)


@dataclass
class RequestsSessionDownloader:
    """复用外部 Session，要求同一 Slot 串行使用。"""

    client: requests.Session  # 调用方拥有的连接池与 Cookie，不跨账号共享。
    _closed: bool = field(
        default=False, init=False, repr=False
    )  # close_session 后置为 True。

    def fetch(self, request: Request) -> Response:
        """使用已有会话，下载结束后保持连接池和 Cookie。

        Args:
            request: 复制后发送的领域请求。

        Returns:
            HTTP 响应或传输失败响应。

        Raises:
            RuntimeError: 会话已通过 close_session 关闭。
            ValueError: 请求指定代理或存在重复请求头。
        """

        if self._closed:
            raise RuntimeError("session is closed")
        source = request.copy()
        if source.proxy is not None:
            raise ValueError(
                "session proxy must be configured when preparing the session"
            )
        return _fetch(self.client, source)
