"""使用 HTTPX 缓冲下载，每次调用创建并关闭独立会话。"""

from __future__ import annotations

from dataclasses import dataclass
from time import monotonic

import httpx

from ..models import Request, Response


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


@dataclass(frozen=True)
class _Options:
    """同步与异步下载器共享的不可变会话设置。

    请求超时由 Request 提供，按 HTTPX 网络阶段生效，不表示总下载时限。
    """

    verify: bool = True  # 是否校验 TLS 证书，默认启用。
    trust_env: bool = False  # 是否读取环境代理及证书配置，默认禁用。
    max_redirects: int = 20  # 单次下载允许跟随的最大重定向次数。

    def __post_init__(self) -> None:
        """校验会话设置，避免非法配置进入下载阶段。

        Raises:
            TypeError: verify 或 trust_env 不是布尔值。
            ValueError: max_redirects 不是非负整数。
        """

        if type(self.verify) is not bool or type(self.trust_env) is not bool:
            raise TypeError("verify and trust_env must be booleans")
        if type(self.max_redirects) is not int or self.max_redirects < 0:
            raise ValueError("max_redirects must be a non-negative integer")


class HttpxDownloader(_Options):
    """同步 HTTP 下载器，不自动重试，不跨请求共享 Cookie 状态。"""

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


class AsyncHttpxDownloader(_Options):
    """原生异步 HTTP 下载器，支持任务取消并在退出时关闭独立会话。"""

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
