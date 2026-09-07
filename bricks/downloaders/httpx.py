"""使用 HTTPX 缓冲下载，支持独立请求和由 Slot 装配的外部会话。"""

from __future__ import annotations

from dataclasses import dataclass
from time import monotonic

import httpx

from ..models import Request, Response
from ._options import TransportOptions


def _sent_request(source: Request, sent: httpx.Request) -> Request:
    """将 HTTPX 请求转换为实际发送内容的独立快照。

    Args:
        source: 提供超时、重定向和代理设置的原始请求副本。
        sent: HTTPX 构造的请求；本适配器仅发送内存中的请求体。

    Returns:
        查询参数已合入 URL、Cookie 已写入请求头的 raw 请求快照。
    """

    return Request(
        str(sent.url),
        method=sent.method,  # type: ignore[arg-type]
        headers=sent.headers.multi_items(),
        body=sent.read(),
        body_type="raw",
        timeout=source.timeout,
        allow_redirects=source.allow_redirects,
        proxy=source.proxy,
    )


def _response(source: Request, received: httpx.Response, started: float) -> Response:
    """转换完整响应及重定向历史。

    Args:
        source: 本次下载的原始请求副本。
        received: 已读取响应体的 HTTPX 最终响应。
        started: 下载开始时的单调时钟读数，单位秒。

    Returns:
        包含解压后内容、实际请求快照和重定向历史的领域响应。
    """

    history = [
        Response(
            item.content,
            status_code=item.status_code,
            url=str(item.url),
            headers=item.headers.multi_items(),
            reason=item.reason_phrase,
            cost=item.elapsed.total_seconds(),
            request=_sent_request(source, item.request),
        )
        for item in received.history
    ]
    return Response(
        received.content,
        status_code=received.status_code,
        url=str(received.url),
        headers=received.headers.multi_items(),
        reason=received.reason_phrase,
        cost=monotonic() - started,
        request=_sent_request(source, received.request),
        history=history,
    )


def _failure(source: Request, error: httpx.RequestError, started: float) -> Response:
    """将 HTTPX 传输失败转换为领域失败响应。

    Args:
        source: 本次下载的原始请求副本。
        error: 携带关联请求的 HTTPX 传输异常。
        started: 下载开始时的单调时钟读数，单位秒。

    Returns:
        状态为 -1 的响应，保留异常身份与关联请求，不表示请求未被服务器接收。
    """

    return Response(
        status_code=-1,
        error=error,
        cost=monotonic() - started,
        request=_sent_request(source, error.request),
    )


class HttpxDownloader(TransportOptions):
    """同步 HTTP 下载器，不自动重试，不跨请求共享 Cookie 状态。"""

    def prepare_session(self, *, proxy: str | None = None) -> HttpxSessionDownloader:
        """按当前配置准备独立会话，交由调用方放入 Slot。

        Args:
            proxy: 会话固定代理，默认直连；trust_env 为 True 时仍遵循环境配置。

        Returns:
            持有独立连接池和 Cookie 的会话下载器。

        Raises:
            ValueError: HTTPX 无法接受会话配置。
            httpx.InvalidURL: 代理地址非法。
        """

        return HttpxSessionDownloader(
            httpx.Client(
                verify=self.verify,
                trust_env=self.trust_env,
                max_redirects=self.max_redirects,
                proxy=proxy,
            )
        )

    def close_session(self, session: HttpxSessionDownloader) -> None:
        """关闭本实现准备的同步会话，可重复调用。

        Args:
            session: 已无在途下载的会话下载器。
        """

        session.client.close()

    def fetch(self, request: Request) -> Response:
        """使用独立会话同步下载，结束时关闭会话资源。

        Args:
            request: 待发送的请求；调用开始时复制，不修改调用方数据。

        Returns:
            HTTP 响应或携带 HTTPX 传输异常的失败响应；4xx/5xx 保留原状态码。

        Raises:
            httpx.InvalidURL: HTTPX 无法接受请求或代理地址。
            ValueError: HTTPX 无法接受传输配置。
        """

        source = request.copy()
        started = monotonic()
        with httpx.Client(
            verify=self.verify,
            trust_env=self.trust_env,
            max_redirects=self.max_redirects,
            proxy=source.proxy,
            timeout=source.timeout,
            follow_redirects=source.allow_redirects,
        ) as client:
            prepared = client.build_request(
                source.method,
                source.real_url,
                headers=source.headers.raw,
                cookies=dict(source.cookies),
                content=source.body,
            )
            try:
                received = client.send(prepared)
            except httpx.RequestError as error:
                return _failure(source, error, started)
            return _response(source, received, started)


class AsyncHttpxDownloader(TransportOptions):
    """原生异步 HTTP 下载器，支持任务取消并在退出时关闭独立会话。"""

    async def prepare_session(
        self, *, proxy: str | None = None
    ) -> AsyncHttpxSessionDownloader:
        """准备独立异步会话，交由调用方放入 Slot。

        Args:
            proxy: 会话固定代理，默认直连；环境配置由 trust_env 决定。

        Returns:
            持有独立连接池和 Cookie 的异步会话下载器。

        Raises:
            ValueError: HTTPX 无法接受会话配置。
            httpx.InvalidURL: 代理地址非法。
        """

        return AsyncHttpxSessionDownloader(
            httpx.AsyncClient(
                verify=self.verify,
                trust_env=self.trust_env,
                max_redirects=self.max_redirects,
                proxy=proxy,
            )
        )

    async def close_session(self, session: AsyncHttpxSessionDownloader) -> None:
        """在使用会话的事件循环内关闭资源，可重复调用。

        Args:
            session: 已无在途下载的会话下载器。

        Raises:
            asyncio.CancelledError: 清理被取消，调用方应确保后续完成关闭。
        """

        await session.client.aclose()

    async def fetch(self, request: Request) -> Response:
        """使用独立会话异步下载，不在事件循环中运行同步网络 I/O。

        Args:
            request: 待发送的请求；首次执行时复制，不修改调用方数据。

        Returns:
            HTTP 响应或携带 HTTPX 传输异常的失败响应；4xx/5xx 保留原状态码。

        Raises:
            asyncio.CancelledError: 下载任务被取消，资源清理后继续传播。
            httpx.InvalidURL: HTTPX 无法接受请求或代理地址。
            ValueError: HTTPX 无法接受传输配置。
        """

        source = request.copy()
        started = monotonic()
        async with httpx.AsyncClient(
            verify=self.verify,
            trust_env=self.trust_env,
            max_redirects=self.max_redirects,
            proxy=source.proxy,
            timeout=source.timeout,
            follow_redirects=source.allow_redirects,
        ) as client:
            prepared = client.build_request(
                source.method,
                source.real_url,
                headers=source.headers.raw,
                cookies=dict(source.cookies),
                content=source.body,
            )
            try:
                received = await client.send(prepared)
            except httpx.RequestError as error:
                return _failure(source, error, started)
            return _response(source, received, started)


def _prepare_session(
    client: httpx.Client | httpx.AsyncClient, source: Request
) -> httpx.Request:
    """为外部会话构造请求，不改变会话传输配置。

    Args:
        client: 调用方拥有的会话，Cookie 和连接池在调用间保留。
        source: 本次请求副本，超时和重定向策略逐次生效。

    Returns:
        合并会话默认设置后的 HTTPX 请求。

    Raises:
        ValueError: Request 指定代理；会话代理必须在 Client 构造时配置。
        httpx.InvalidURL: 请求地址非法。
    """

    if source.proxy is not None:
        raise ValueError("session proxy must be configured on the HTTPX client")
    return client.build_request(
        source.method,
        source.real_url,
        headers=source.headers.raw,
        cookies=dict(source.cookies),
        content=source.body,
        timeout=source.timeout,
    )


@dataclass(frozen=True)
class HttpxSessionDownloader:
    """复用外部同步会话；应为每个 Slot 装配独立实例。

    Attributes:
        client: 调用方创建和关闭的 Client；保留 Cookie 与连接池，不保存当前请求。
    """

    client: httpx.Client  # 外部拥有的会话，不得在不同账号的 Slot 之间共享。

    def fetch(self, request: Request) -> Response:
        """使用 Slot 所属会话下载，调用结束后保持会话打开。

        Args:
            request: 待发送请求；复制后使用，不修改调用方数据。

        Returns:
            HTTP 响应或 status_code=-1 的传输失败响应。

        Raises:
            ValueError: 请求含代理配置，或 HTTPX 配置非法。
            httpx.InvalidURL: 请求地址非法。
            RuntimeError: 会话已关闭；其他程序错误继续传播。
        """

        source = request.copy()
        started = monotonic()
        prepared = _prepare_session(self.client, source)
        try:
            received = self.client.send(
                prepared, follow_redirects=source.allow_redirects
            )
        except httpx.RequestError as error:
            return _failure(source, error, started)
        return _response(source, received, started)


@dataclass(frozen=True)
class AsyncHttpxSessionDownloader:
    """复用外部异步会话；每个 Slot 独立，会话使用和关闭必须位于同一事件循环。

    Attributes:
        client: 调用方创建和异步关闭的 AsyncClient；保留 Cookie 与连接池。
    """

    client: httpx.AsyncClient  # 外部拥有的会话，不跨事件循环或账号共享。

    async def fetch(self, request: Request) -> Response:
        """使用 Slot 所属异步会话下载，不在请求结束时关闭会话。

        Args:
            request: 待发送请求；复制后使用，不修改调用方数据。

        Returns:
            HTTP 响应或 status_code=-1 的传输失败响应。

        Raises:
            asyncio.CancelledError: 取消继续传播，会话仍由调用方关闭。
            ValueError: 请求含代理配置，或 HTTPX 配置非法。
            httpx.InvalidURL: 请求地址非法。
            RuntimeError: 会话已关闭或事件循环不匹配；程序错误继续传播。
        """

        source = request.copy()
        started = monotonic()
        prepared = _prepare_session(self.client, source)
        try:
            received = await self.client.send(
                prepared, follow_redirects=source.allow_redirects
            )
        except httpx.RequestError as error:
            return _failure(source, error, started)
        return _response(source, received, started)
