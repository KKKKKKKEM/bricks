"""requests 与 requests-go 共用的响应和 PreparedRequest 转换。"""

import requests

from ..models import Request, Response


def _sent_request(source: Request, sent: requests.PreparedRequest) -> Request:
    """转换 requests 实际发送的请求快照。

    Args:
        source: 提供执行设置的请求副本。
        sent: requests 已准备好的内存请求。

    Returns:
        URL、请求头和原始请求体快照。
    """

    assert sent.url is not None
    assert sent.method is not None
    return Request(
        sent.url,
        method=sent.method,  # type: ignore[arg-type]
        headers=sent.headers.items(),
        body=sent.body,
        body_type="raw",
        timeout=source.timeout,
        allow_redirects=source.allow_redirects,
        proxy=source.proxy,
    )


def _response(source: Request, received: requests.Response) -> Response:
    """转换单个缓冲响应，保留重复响应头。

    Args:
        source: 本次请求的独立副本。
        received: 已完整接收的 requests 响应。

    Returns:
        含实际请求快照的领域响应，不递归填充历史。
    """

    return Response(
        received.content,
        status_code=received.status_code,
        url=received.url,
        headers=received.raw.headers.items(),
        reason=received.reason,
        cost=received.elapsed.total_seconds(),
        request=_sent_request(source, received.request),
    )
