"""可选 wreq 下载器，支持同步、异步和请求级指纹；需要 Python 3.11 以上。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from time import monotonic
from typing import Any

import wreq
from wreq import blocking

from ..models import Request, Response
from ._options import TransportOptions

_NETWORK_ERRORS = (
    wreq.ConnectionError,
    wreq.ConnectionResetError,
    wreq.ProxyConnectionError,
    wreq.TimeoutError,
    wreq.TlsError,
    wreq.RedirectError,
    wreq.BodyError,
    wreq.DecodingError,
)  # 只转换已识别的传输异常，BuilderError、RustPanic 和取消继续传播。


def _profile(impersonate: str | None) -> wreq.Profile | None:
    """按原生枚举名称解析指纹，不推测别名。

    Args:
        impersonate: 如 Chrome149，None 表示沿用默认配置。

    Returns:
        wreq 原生指纹枚举或 None。

    Raises:
        TypeError: 指纹名称不是字符串。
        ValueError: 指纹名称不受当前 wreq 支持。
    """

    if impersonate is None:
        return None
    if not isinstance(impersonate, str):
        raise TypeError("impersonate must be a string or None")
    profile = getattr(wreq.Profile, impersonate, None)
    if not isinstance(profile, wreq.Profile):
        raise ValueError(f"unsupported wreq fingerprint: {impersonate!r}")
    return profile


def _parameters(
    source: Request, max_redirects: int, impersonate: str | None
) -> dict[str, Any]:
    """构造独立的逐请求传输参数。

    Args:
        source: 本次请求副本。
        max_redirects: 当前会话允许的重定向上限。
        impersonate: 本次请求的原生指纹名称。

    Returns:
        可传给同步或异步 Client.request 的参数。

    Raises:
        ValueError: wreq 不支持当前方法或指纹。
    """

    method = getattr(wreq.Method, source.method, None)
    if method is None:
        raise ValueError(f"wreq does not support method {source.method}")
    headers = wreq.HeaderMap()
    for name, value in source.headers.raw:
        headers.append(name, value)
    parameters: dict[str, Any] = {
        "method": method,
        "url": source.real_url,
        "headers": headers,
        "cookies": dict(source.cookies),
        "default_headers": False,
        "redirect": wreq.Policy.limited(max_redirects)
        if source.allow_redirects
        else wreq.Policy.none(),
    }
    if source.body is not None:
        parameters["body"] = source.body
    if source.timeout is not None:
        parameters["timeout"] = timedelta(seconds=source.timeout)
    profile = _profile(impersonate)
    if profile is not None:
        parameters["emulation"] = profile
    return parameters


def _headers(headers: wreq.HeaderMap) -> list[tuple[str, str]]:
    """解码原始 HTTP 响应头并保留重复项。

    Args:
        headers: 原生响应头映射。

    Returns:
        按 HTTP 字节语义解码的名称和值列表。
    """

    return [(name.decode("ascii"), value.decode("latin-1")) for name, value in headers]


def _response(
    source: Request,
    received: blocking.Response | wreq.Response,
    content: bytes,
    started: float,
) -> Response:
    """转换缓冲响应和有限的重定向记录。

    Args:
        source: 初始提交请求的独立副本。
        received: 原生响应，不包含逐跳请求体。
        content: 已完整读取的响应字节。
        started: 开始下载的单调时钟秒数。

    Returns:
        领域响应，request 保存初始提交内容，不伪造实际发出的自动头。
    """

    return Response(
        content,
        status_code=received.status.as_int(),
        url=received.url,
        headers=_headers(received.headers),
        request=source,
        cost=monotonic() - started,
        history=[
            Response(
                status_code=item.status,
                url=item.previous,
                headers=_headers(item.headers),
            )
            for item in received.history
        ],
    )


@dataclass(frozen=True)
class _WreqOptions(TransportOptions):
    """wreq 的不可变传输选项。"""

    impersonate: str | None = None  # 原生 Profile 名称，默认不指定浏览器指纹。

    def __post_init__(self) -> None:
        """在资源创建前校验传输与指纹设置。

        Raises:
            TypeError: 选项类型错误。
            ValueError: 重定向上限或指纹非法。
        """

        super().__post_init__()
        _profile(self.impersonate)

    def _client_parameters(self, proxy: str | None) -> dict[str, Any]:
        """准备客户端配置，不安装隐式重试或状态码恢复策略。

        Args:
            proxy: 当前会话固定代理。

        Returns:
            两种客户端共用的构造参数。
        """

        options: dict[str, Any] = {
            "tls_verify": self.verify,
            "cookie_store": True,
            "no_proxy": not self.trust_env and proxy is None,
            "raise_for_status": False,
        }
        if proxy is not None:
            options["proxies"] = [wreq.Proxy.all(proxy)]
        profile = _profile(self.impersonate)
        if profile is not None:
            options["emulation"] = profile
        return options


class WreqDownloader(_WreqOptions):
    """通过 blocking.Client 实现同步缓冲下载。"""

    def prepare_session(self, *, proxy: str | None = None) -> WreqSessionDownloader:
        """创建独立客户端，交给调用方按 Slot 管理。

        Args:
            proxy: 固定代理，默认由 trust_env 决定环境代理行为。

        Returns:
            拥有独立 Cookie 和连接池的会话下载器。
        """

        return WreqSessionDownloader(
            blocking.Client(**self._client_parameters(proxy)), self.max_redirects
        )

    def close_session(self, session: WreqSessionDownloader) -> None:
        """关闭没有在途请求的客户端，允许重复关闭。

        Args:
            session: 本实现准备的会话。
        """

        if not session._closed:
            session.client.close()
            session._closed = True

    def fetch(self, request: Request) -> Response:
        """使用独立临时客户端下载。

        Args:
            request: 待复制和发送的请求。

        Returns:
            HTTP 响应或传输失败响应。
        """

        return self.fetch_with_fingerprint(request, impersonate=None)

    def fetch_with_fingerprint(
        self, request: Request, *, impersonate: str | None
    ) -> Response:
        """使用临时客户端发送带本次指纹的请求。

        Args:
            request: 待复制和发送的请求。
            impersonate: 本次指纹，None 沿用下载器默认值。

        Returns:
            HTTP 响应或传输失败响应，非传输异常继续传播。
        """

        source = request.copy()
        session = self.prepare_session(proxy=source.proxy)
        try:
            return session._fetch(source, impersonate)
        finally:
            self.close_session(session)


class AsyncWreqDownloader(_WreqOptions):
    """通过原生异步 Client 下载，不在线程中包装同步网络调用。"""

    async def prepare_session(
        self, *, proxy: str | None = None
    ) -> AsyncWreqSessionDownloader:
        """准备独立的异步会话客户端。

        Args:
            proxy: 会话固定代理。

        Returns:
            由调用方成对关闭的会话下载器。
        """

        return AsyncWreqSessionDownloader(
            wreq.Client(**self._client_parameters(proxy)), self.max_redirects
        )

    async def close_session(self, session: AsyncWreqSessionDownloader) -> None:
        """使用原生同步 close 释放已停止使用的异步客户端。

        Args:
            session: 本实现准备的异步会话。
        """

        if not session._closed:
            session.client.close()
            session._closed = True

    async def fetch(self, request: Request) -> Response:
        """使用独立临时异步会话下载。

        Args:
            request: 本次请求。

        Returns:
            HTTP 响应或传输失败响应。

        Raises:
            asyncio.CancelledError: 取消清理后继续传播。
        """

        return await self.fetch_with_fingerprint(request, impersonate=None)

    async def fetch_with_fingerprint(
        self, request: Request, *, impersonate: str | None
    ) -> Response:
        """异步传递本次指纹，并在完成或取消后关闭临时会话。

        Args:
            request: 本次请求。
            impersonate: 本次指纹，None 沿用默认值。

        Returns:
            HTTP 响应或传输失败响应。

        Raises:
            asyncio.CancelledError: 取消继续传播。
        """

        source = request.copy()
        session = await self.prepare_session(proxy=source.proxy)
        try:
            return await session._fetch(source, impersonate)
        finally:
            await self.close_session(session)


@dataclass
class WreqSessionDownloader:
    """复用外部同步客户端，要求同槽串行调用。"""

    client: blocking.Client  # 调用方负责关闭的独立会话资源。
    max_redirects: int = 20  # 会话的重定向上限，每次请求单独计数。
    _closed: bool = field(
        default=False, init=False, repr=False
    )  # 由装配方关闭后置 True。

    def fetch(self, request: Request) -> Response:
        """使用原会话指纹发送请求。

        Args:
            request: 本次请求。

        Returns:
            HTTP 响应或传输失败响应。
        """

        return self.fetch_with_fingerprint(request, impersonate=None)

    def fetch_with_fingerprint(
        self, request: Request, *, impersonate: str | None
    ) -> Response:
        """在同一客户端上覆盖本次指纹，不修改默认配置。

        Args:
            request: 本次请求，复制后使用。
            impersonate: 本次原生 Profile 名称。

        Returns:
            HTTP 响应或传输失败响应。

        Raises:
            ValueError: 请求设置会话代理或指纹非法。
        """

        source = request.copy()
        if source.proxy is not None:
            raise ValueError(
                "session proxy must be configured when preparing the session"
            )
        return self._fetch(source, impersonate)

    def _fetch(self, source: Request, impersonate: str | None) -> Response:
        """读取完整响应并释放响应句柄。

        Args:
            source: 已复制的请求。
            impersonate: 本次指纹。

        Returns:
            领域响应，程序和配置错误继续传播。
        """

        if self._closed:
            raise RuntimeError("wreq session is closed")
        parameters = _parameters(source, self.max_redirects, impersonate)
        started = monotonic()
        try:
            received = self.client.request(**parameters)
            try:
                # bytes() 完整消费响应后归还连接；原生 close/上下文退出会关闭连接。
                return _response(source, received, received.bytes(), started)
            except BaseException:
                received.close()
                raise
        except _NETWORK_ERRORS as error:
            return Response(
                status_code=-1, error=error, request=source, cost=monotonic() - started
            )


@dataclass
class AsyncWreqSessionDownloader:
    """复用原生异步客户端和 Cookie 存储。"""

    client: wreq.Client  # 调用方持有并负责关闭的会话。
    max_redirects: int = 20  # 单次请求的重定向上限。
    _closed: bool = field(
        default=False, init=False, repr=False
    )  # 关闭后的生命周期标记。

    async def fetch(self, request: Request) -> Response:
        """使用会话原指纹异步下载。

        Args:
            request: 本次请求。

        Returns:
            HTTP 响应或传输失败响应。
        """

        return await self.fetch_with_fingerprint(request, impersonate=None)

    async def fetch_with_fingerprint(
        self, request: Request, *, impersonate: str | None
    ) -> Response:
        """在原异步客户端上覆盖本次请求指纹。

        Args:
            request: 复制后发送的请求。
            impersonate: 本次指纹，None 沿用默认值。

        Returns:
            HTTP 响应或传输失败响应。

        Raises:
            ValueError: 请求指定代理或指纹非法。
            asyncio.CancelledError: 取消继续传播。
        """

        source = request.copy()
        if source.proxy is not None:
            raise ValueError(
                "session proxy must be configured when preparing the session"
            )
        return await self._fetch(source, impersonate)

    async def _fetch(self, source: Request, impersonate: str | None) -> Response:
        """异步缓冲响应并在退出时释放响应资源。

        Args:
            source: 已复制的请求。
            impersonate: 本次指纹。

        Returns:
            领域响应。

        Raises:
            asyncio.CancelledError: 取消继续传播。
        """

        if self._closed:
            raise RuntimeError("wreq session is closed")
        parameters = _parameters(source, self.max_redirects, impersonate)
        started = monotonic()
        try:
            received = await self.client.request(**parameters)
            try:
                # 成功完整读取时由原生库归还连接，失败或取消才显式关闭。
                return _response(source, received, await received.bytes(), started)
            except BaseException:
                await received.close()
                raise
        except _NETWORK_ERRORS as error:
            return Response(
                status_code=-1, error=error, request=source, cost=monotonic() - started
            )
