"""基于 curl_cffi 的默认同步、异步及 Slot 会话下载器。"""

from __future__ import annotations

from dataclasses import dataclass, field
from time import monotonic
from typing import Any

from curl_cffi import CurlECode
from curl_cffi import requests as curl_requests
from curl_cffi.requests.exceptions import RequestException

from ..models import Request, Response
from ._options import TransportOptions

_TRANSPORT_ERRORS = frozenset(
    {
        CurlECode.COULDNT_RESOLVE_PROXY,
        CurlECode.COULDNT_RESOLVE_HOST,
        CurlECode.COULDNT_CONNECT,
        CurlECode.WEIRD_SERVER_REPLY,
        CurlECode.PARTIAL_FILE,
        CurlECode.OPERATION_TIMEDOUT,
        CurlECode.SSL_CONNECT_ERROR,
        CurlECode.TOO_MANY_REDIRECTS,
        CurlECode.GOT_NOTHING,
        CurlECode.SEND_ERROR,
        CurlECode.RECV_ERROR,
        CurlECode.PEER_FAILED_VERIFICATION,
        CurlECode.BAD_CONTENT_ENCODING,
        CurlECode.HTTP2,
        CurlECode.HTTP2_STREAM,
        CurlECode.HTTP3,
        CurlECode.QUIC_CONNECT_ERROR,
        CurlECode.PROXY,
    }
)  # 可转换的网络错误码；配置、内存、回调和未知错误继续传播。


def _response(
    source: Request, received: curl_requests.Response, started: float
) -> Response:
    """转换缓冲响应，不把初始请求冒充重定向后的实际请求。

    Args:
        source: 提交给传输库的独立请求副本。
        received: curl_cffi 已读取的响应。
        started: 本次下载开始的单调时钟秒数。

    Returns:
        最终响应及仅含 URL、状态、响应头的重定向历史。
    """

    return Response(
        received.content,
        status_code=received.status_code,
        url=received.url,
        headers=[(name, value or "") for name, value in received.headers.multi_items()],
        reason=received.reason,
        cost=monotonic() - started,
        request=source,
        history=[
            Response(
                status_code=item.status_code,
                url=item.url,
                headers=[
                    (name, value or "") for name, value in item.headers.multi_items()
                ],
                reason=item.reason,
            )
            for item in received.history
        ],
    )


def _failure(source: Request, error: RequestException, started: float) -> Response:
    """只转换已识别的网络错误。

    Args:
        source: 本次请求的独立副本。
        error: curl_cffi 抛出的请求异常。
        started: 本次下载开始的单调时钟秒数。

    Returns:
        保留原始异常及提交请求的失败响应。

    Raises:
        RequestException: 配置、资源或未知错误，不作为网络失败恢复。
    """

    if error.code not in _TRANSPORT_ERRORS:
        raise error
    return Response(
        status_code=-1, error=error, request=source, cost=monotonic() - started
    )


def _parameters(source: Request, *, impersonate: str | None = None) -> dict[str, Any]:
    """构造同步与异步请求共用的参数。

    Args:
        source: 已复制的领域请求。
        impersonate: 本次请求指纹，None 沿用 Session 默认值。

    Returns:
        包含已编码请求体和逐请求超时、重定向配置的参数。
    """

    return {
        "method": source.method,
        "url": source.real_url,
        "headers": list(source.headers.raw),
        "cookies": dict(source.cookies),
        "content": source.body,
        "timeout": source.timeout,
        "allow_redirects": source.allow_redirects,
        "impersonate": impersonate,
    }


def _fetch(
    client: curl_requests.Session, source: Request, *, impersonate: str | None = None
) -> Response:
    """通过指定同步会话完成一次缓冲下载。

    Args:
        client: 当前调用独占使用、外部负责关闭的会话。
        source: 已复制的领域请求。
        impersonate: 本次请求指纹，None 沿用会话默认值。

    Returns:
        HTTP 响应或传输失败响应。

    Raises:
        Exception: 非传输异常继续传播。
    """

    started = monotonic()
    try:
        received = client.request(**_parameters(source, impersonate=impersonate))
    except RequestException as error:
        return _failure(source, error, started)
    return _response(source, received, started)


async def _afetch(
    client: curl_requests.AsyncSession,
    source: Request,
    *,
    impersonate: str | None = None,
) -> Response:
    """通过异步会话下载并保留取消语义。

    Args:
        client: 在当前事件循环内使用的外部会话。
        source: 已复制的领域请求。
        impersonate: 本次请求指纹，None 沿用会话默认值。

    Returns:
        HTTP 响应或传输失败响应。

    Raises:
        asyncio.CancelledError: 取消直接传播。
        Exception: 非传输异常继续传播。
    """

    started = monotonic()
    try:
        received = await client.request(**_parameters(source, impersonate=impersonate))
    except RequestException as error:
        return _failure(source, error, started)
    return _response(source, received, started)


@dataclass(frozen=True)
class _CurlOptions(TransportOptions):
    """curl_cffi 的不可变传输和指纹设置。"""

    impersonate: str | None = (
        None  # 可选浏览器 TLS 指纹名称，默认不模拟；不代表完整浏览器。
    )

    def __post_init__(self) -> None:
        """校验基础设置和指纹参数类型。

        Raises:
            TypeError: 配置类型错误。
            ValueError: 指纹名称为空或重定向上限非法。
        """

        super().__post_init__()
        if self.impersonate is not None:
            if not isinstance(self.impersonate, str):
                raise TypeError("impersonate must be a string or None")
            if not self.impersonate.strip():
                raise ValueError("impersonate must not be empty")

    def _session_parameters(self, proxy: str | None) -> dict[str, Any]:
        """构造会话参数，并显式禁用不需要的环境代理。

        Args:
            proxy: 会话固定代理，None 表示由 trust_env 决定是否读取环境代理。

        Returns:
            同步与异步 Session 共用的构造参数。
        """

        return {
            "verify": self.verify,
            "trust_env": self.trust_env,
            "max_redirects": self.max_redirects,
            "impersonate": self.impersonate,
            "default_headers": False,
            "proxies": {"all": proxy or ""}
            if proxy is not None or not self.trust_env
            else None,
        }


class CurlCffiDownloader(_CurlOptions):
    """默认同步下载器，每次 fetch 创建独立临时会话。"""

    def prepare_session(self, *, proxy: str | None = None) -> CurlCffiSessionDownloader:
        """创建可随 Slot 串行跨线程使用的独立会话。

        Args:
            proxy: 会话固定代理，默认由 trust_env 决定直连或读取环境。

        Returns:
            调用方拥有并负责关闭的会话下载器。
        """

        return CurlCffiSessionDownloader(
            curl_requests.Session(
                use_thread_local_curl=False, **self._session_parameters(proxy)
            )
        )

    def close_session(self, session: CurlCffiSessionDownloader) -> None:
        """关闭已无在途请求的会话，可重复调用。

        Args:
            session: 本下载器准备的会话。
        """

        session.client.close()

    def fetch(self, request: Request) -> Response:
        """使用临时会话下载，请求结束或失败后关闭资源。

        Args:
            request: 复制后发送的请求，不修改调用方数据。

        Returns:
            HTTP 响应或传输失败响应；配置错误继续传播。
        """

        return self.fetch_with_fingerprint(request, impersonate=None)

    def fetch_with_fingerprint(
        self, request: Request, *, impersonate: str | None
    ) -> Response:
        """通过 requests-like 参数覆盖本次临时会话请求的指纹。

        Args:
            request: 复制后发送的领域请求。
            impersonate: 本次指纹名称，None 沿用下载器默认值。

        Returns:
            HTTP 响应或传输失败响应；配置错误继续传播。
        """

        source = request.copy()
        session = self.prepare_session(proxy=source.proxy)
        try:
            return _fetch(session.client, source, impersonate=impersonate)
        finally:
            self.close_session(session)


class AsyncCurlCffiDownloader(_CurlOptions):
    """默认异步下载器，使用 libcurl 异步传输而非同步线程包装。"""

    async def prepare_session(
        self, *, proxy: str | None = None
    ) -> AsyncCurlCffiSessionDownloader:
        """在当前事件循环内准备独立会话。

        Args:
            proxy: 会话固定代理。

        Returns:
            由调用方成对关闭的异步会话下载器。
        """

        return AsyncCurlCffiSessionDownloader(
            curl_requests.AsyncSession(**self._session_parameters(proxy))
        )

    async def close_session(self, session: AsyncCurlCffiSessionDownloader) -> None:
        """在使用会话的事件循环内关闭资源。

        Args:
            session: 已无在途请求的异步会话。
        """

        if not session._closed:
            await session.client.close()
            session._closed = True

    async def fetch(self, request: Request) -> Response:
        """异步下载并在结束或取消后关闭临时会话。

        Args:
            request: 复制后发送的请求。

        Returns:
            HTTP 响应或传输失败响应。

        Raises:
            asyncio.CancelledError: 取消清理后继续传播。
        """

        return await self.fetch_with_fingerprint(request, impersonate=None)

    async def fetch_with_fingerprint(
        self, request: Request, *, impersonate: str | None
    ) -> Response:
        """异步传递本次请求指纹，完成后关闭临时会话。

        Args:
            request: 复制后发送的领域请求。
            impersonate: 本次指纹名称，None 沿用下载器默认值。

        Returns:
            HTTP 响应或传输失败响应。

        Raises:
            asyncio.CancelledError: 清理临时会话后继续传播。
        """

        source = request.copy()
        session = await self.prepare_session(proxy=source.proxy)
        try:
            return await _afetch(session.client, source, impersonate=impersonate)
        finally:
            await self.close_session(session)


@dataclass(frozen=True)
class CurlCffiSessionDownloader:
    """复用外部同步会话；同一实例必须串行使用。"""

    client: curl_requests.Session  # 调用方创建和关闭的非线程局部会话，每个 Slot 独立。

    def fetch(self, request: Request) -> Response:
        """使用当前 Slot 会话，结束后保持 Cookie 和连接池。

        Args:
            request: 本次请求，复制后使用。

        Returns:
            HTTP 响应或传输失败响应。

        Raises:
            ValueError: 请求指定代理；会话代理必须在准备时配置。
        """

        return self.fetch_with_fingerprint(request, impersonate=None)

    def fetch_with_fingerprint(
        self, request: Request, *, impersonate: str | None
    ) -> Response:
        """在原 Session 上传入请求指纹，保留 Cookie 和默认配置。

        Args:
            request: 复制后发送的领域请求。
            impersonate: 本次指纹名称，None 沿用 Session 默认值。

        Returns:
            HTTP 响应或传输失败响应，连接复用由底层库决定。

        Raises:
            ValueError: 请求指定代理，必须改在会话准备时配置。
        """

        source = request.copy()
        if source.proxy is not None:
            raise ValueError(
                "session proxy must be configured when preparing the session"
            )
        return _fetch(self.client, source, impersonate=impersonate)


@dataclass
class AsyncCurlCffiSessionDownloader:
    """复用当前事件循环中的外部异步会话。"""

    client: curl_requests.AsyncSession  # 调用方拥有的独立会话，不跨事件循环或账号共享。
    _closed: bool = field(
        default=False, init=False, repr=False
    )  # 成对关闭完成标记，确保重复关闭安全。

    async def fetch(self, request: Request) -> Response:
        """异步使用 Slot 会话，不在下载后关闭资源。

        Args:
            request: 本次请求，复制后使用。

        Returns:
            HTTP 响应或传输失败响应。

        Raises:
            ValueError: 请求指定代理。
            asyncio.CancelledError: 取消继续传播，会话由调用方关闭。
        """

        return await self.fetch_with_fingerprint(request, impersonate=None)

    async def fetch_with_fingerprint(
        self, request: Request, *, impersonate: str | None
    ) -> Response:
        """在当前异步 Session 上传入请求指纹，不修改会话默认值。

        Args:
            request: 复制后发送的领域请求。
            impersonate: 本次指纹名称，None 沿用 Session 默认值。

        Returns:
            HTTP 响应或传输失败响应，Cookie 继续保存在同一会话。

        Raises:
            ValueError: 请求指定代理。
            asyncio.CancelledError: 取消继续传播，会话仍由调用方关闭。
        """

        source = request.copy()
        if source.proxy is not None:
            raise ValueError(
                "session proxy must be configured when preparing the session"
            )
        return await _afetch(self.client, source, impersonate=impersonate)
