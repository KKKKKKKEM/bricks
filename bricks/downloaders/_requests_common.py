"""requests 与 requests-go 共用的响应和 PreparedRequest 转换。"""

from http.cookiejar import DefaultCookiePolicy
from urllib.parse import urlsplit

import requests

from ..models import Request, Response
from ..models.cookies import cookie_domain
from ._options import _redirect_failure


def _request_cookies(source: Request) -> requests.cookies.RequestsCookieJar:
    """按初始请求主机及协议约束显式 Cookie。

    Args:
        source: 已复制的领域请求。

    Returns:
        仅匹配初始主机、且 HTTPS Cookie 不发送到明文 HTTP 的 CookieJar。

    Raises:
        ValueError: 初始请求 URL 没有可绑定的主机。

    """
    parsed = urlsplit(source.real_url)
    hostname = parsed.hostname
    if hostname is None:
        raise ValueError("request URL must include a host when sending cookies")
    cookie_host = cookie_domain(hostname)
    cookies = requests.cookies.RequestsCookieJar()
    for name, value in source.cookies.items():
        cookie = requests.cookies.create_cookie(
            name=name,
            value=value,
            domain=cookie_host,
            path="/",
            secure=parsed.scheme.lower() == "https",
        )
        cookie.domain_specified = False
        cookies.set_cookie(cookie)
    return cookies


class _StrictHostCookieSession(requests.Session):
    """在每次请求的合并 CookieJar 上启用严格主机匹配。"""

    def prepare_request(self, request: requests.Request) -> requests.PreparedRequest:
        """准备请求并阻止无 Domain Cookie 匹配子域。

        Args:
            request: requests 的原始请求。

        Returns:
            应用严格 host-only Cookie 策略的已准备请求。
        """

        # prepare_request 会把请求 Jar 合并进默认宽松策略的 Jar，需在合并后收紧。
        prepared = super().prepare_request(request)
        # requests 的重定向复制保留此 Jar，但 types-requests 未声明该内部字段。
        cookie_jar: requests.cookies.RequestsCookieJar = prepared._cookies  # type: ignore[attr-defined]
        cookie_jar.set_policy(
            DefaultCookiePolicy(
                strict_ns_domain=DefaultCookiePolicy.DomainStrictNonDomain
            )
        )
        explicit_cookie_header = any(
            name.lower() == "cookie"
            for headers in (request.headers, self.headers)
            if headers
            for name in headers
        )
        if not explicit_cookie_header:
            prepared.headers.pop("Cookie", None)
            prepared.prepare_cookies(cookie_jar)
        return prepared


def _sent_request(source: Request, sent: requests.PreparedRequest) -> Request:
    """转换 requests 实际发送的请求快照。

    Args:
        source: 提供执行设置的请求副本。
        sent: requests 已准备好的内存请求。

    Returns:
        URL、请求头和原始请求体快照。
    """

    if sent.url is None or sent.method is None:
        raise RuntimeError("prepared request is missing its URL or method")
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
    for item in (*received.history, received):
        failure = _redirect_failure(
            source,
            item.status_code,
            item.url,
            item.raw.headers.items(),
        )
        if failure is not None:
            return failure
    return Response(
        received.content,
        status_code=received.status_code,
        url=received.url,
        headers=received.raw.headers.items(),
        reason=received.reason,
        cost=received.elapsed.total_seconds(),
        request=_sent_request(source, received.request),
    )
