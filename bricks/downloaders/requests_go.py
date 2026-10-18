"""可选 requests-go 同步下载器，通过独立原生会话使用 Go TLS 指纹。"""

from __future__ import annotations

from contextlib import ExitStack
from copy import deepcopy
from dataclasses import dataclass, field
from time import monotonic
from urllib.parse import urlsplit

import requests
import requests_go
from requests_go import tls_config
from requests_go.tls_client.exceptions import TLSClientExeption

from ..models import Request, Response
from ._options import (
    TransportOptions,
    _check_duplicate_headers,
    _ephemeral_session,
    _failed_response,
)
from ._requests_common import (
    _raised_by_redirect,
    _request_cookies,
    _response,
    _sent_request,
    _StrictHostCookieSession,
)


def _restore_host_only_cookies(
    response: requests.Response, **kwargs: object
) -> requests.Response:
    """从 Go adapter 原始响应重建 CookieJar，保留 host-only 属性。

    Args:
        response: requests 包装的原生下载响应。
        **kwargs: requests response hook 传入的其他请求选项。

    Returns:
        Cookie 属性从原始 Set-Cookie 响应头恢复后的响应。

    Raises:
        RuntimeError: adapter 未保留可解析的原始响应，无法保证 Cookie 来源约束。
    """

    raw = response.raw
    if (
        response.request is None
        or raw is None
        or getattr(raw, "_original_response", None) is None
    ):
        raise RuntimeError("requests-go response lacks original Cookie metadata")
    cookies = requests.cookies.RequestsCookieJar()
    requests.cookies.extract_cookies_to_jar(cookies, response.request, raw)
    response.cookies = cookies
    return response


class _ScopedRequestsGoSession(_StrictHostCookieSession, requests_go.Session):
    """在 requests-go 会话中保留共享严格 host-only Cookie 策略。"""


def _preset(name: str) -> tls_config.TLSConfig:
    """复制原生预设，避免不同 Slot 修改或复用模块级对象。

    Args:
        name: requests-go 导出的 TLS_ 预设名称。

    Returns:
        与模块预设独立的 TLSConfig。

    Raises:
        TypeError: 名称不是字符串。
        ValueError: 名称不是可用的 TLS 预设。
    """

    if not isinstance(name, str):
        raise TypeError("impersonate must be a requests-go preset name")
    preset = getattr(tls_config, name, None)
    if not name.startswith("TLS_") or not isinstance(preset, tls_config.TLSConfig):
        raise ValueError(f"unsupported requests-go fingerprint: {name!r}")
    return deepcopy(preset)


@dataclass(frozen=True)
class RequestsGoDownloader(TransportOptions):
    """同步下载器，HTTPS 默认使用 Go Chrome 预设，HTTP 遵循原生 requests 路径。"""

    impersonate: str = "TLS_CHROME_LATEST"  # 原生 TLS 预设名称，不支持浏览器名称别名。

    def __post_init__(self) -> None:
        """在会话创建前校验传输设置和默认指纹。

        Raises:
            TypeError: 设置类型错误。
            ValueError: 指纹或重定向上限非法。
        """

        super().__post_init__()
        _preset(self.impersonate)

    def prepare_session(
        self, *, proxy: str | None = None
    ) -> RequestsGoSessionDownloader:
        """创建独立的 Python 与 Go 会话并登记完整退出清理。

        Args:
            proxy: 会话固定代理，None 时遵循 trust_env。

        Returns:
            由调用方按 Slot 保存并成对关闭的会话下载器。
        """

        resources = ExitStack()
        try:
            client = resources.enter_context(_ScopedRequestsGoSession())
            client.hooks["response"].append(_restore_host_only_cookies)
            client.verify = self.verify
            client.trust_env = self.trust_env
            client.max_redirects = self.max_redirects
            if proxy is not None:
                client.proxies = {"http": proxy, "https": proxy}
            config = _preset(self.impersonate)
            config.id = client.tls_config.id
            client.tls_config = config
            return RequestsGoSessionDownloader(client, resources)
        except BaseException:
            resources.close()
            raise

    def close_session(self, session: RequestsGoSessionDownloader) -> None:
        """通过原生 Session 上下文退出关闭连接并释放 Go 会话。

        Args:
            session: 已无在途请求的会话，重复关闭安全。
        """

        if not session._closed:
            session._resources.close()
            session._closed = True

    def fetch(self, request: Request) -> Response:
        """使用默认指纹和临时会话下载。

        Args:
            request: 复制后发送的领域请求。

        Returns:
            HTTP 响应或可分类的传输失败响应。
        """

        return self.fetch_with_fingerprint(request, impersonate=None)

    def fetch_with_fingerprint(
        self, request: Request, *, impersonate: str | None
    ) -> Response:
        """在临时会话内应用当前 Context 的指纹选项。

        Args:
            request: 本次请求，复制后使用。
            impersonate: 本次原生预设名，None 沿用默认指纹。

        Returns:
            领域响应，配置与不透明的 Go 后端异常继续传播。
        """

        source = request.copy()
        with _ephemeral_session(
            self.prepare_session, self.close_session, source.proxy
        ) as session:
            return session._fetch(source, impersonate)


@dataclass
class RequestsGoSessionDownloader:
    """复用独立 Go 会话和 Python CookieJar，同一 Slot 内必须串行调用。"""

    client: requests_go.Session  # 装配方拥有的原生 Session，不跨账号共享。
    _resources: ExitStack  # 登记 Session.__exit__，同时负责 Python 连接和 Go 会话清理。
    _closed: bool = field(
        default=False, init=False, repr=False
    )  # 完成关闭后拒绝再次 fetch。

    def fetch(self, request: Request) -> Response:
        """使用当前会话的默认 TLS 配置发送请求。

        Args:
            request: 本次请求。

        Returns:
            HTTP 响应或可分类的传输失败响应。
        """

        return self.fetch_with_fingerprint(request, impersonate=None)

    def fetch_with_fingerprint(
        self, request: Request, *, impersonate: str | None
    ) -> Response:
        """在同一原生会话内覆盖请求指纹，不改写会话默认预设。

        Args:
            request: 复制后发送的请求。
            impersonate: 本次 TLS_ 预设名称，None 沿用会话默认值。

        Returns:
            领域响应。

        Raises:
            ValueError: 请求含代理配置，或指纹/请求设置不受支持。
            RuntimeError: 会话已关闭。
        """

        source = request.copy()
        if source.proxy is not None:
            raise ValueError(
                "session proxy must be configured when preparing the session"
            )
        return self._fetch(source, impersonate)

    def _fetch(self, source: Request, impersonate: str | None) -> Response:
        """发送独立配置副本，缓冲响应并清理被原生库替换的适配器。

        Args:
            source: 已复制的领域请求。
            impersonate: 本次指纹名称。

        Returns:
            HTTP 响应或具有明确类型的网络失败响应。

        Raises:
            ValueError: 请求头、HTTPS 超时或证书校验要求超出原生支持边界。
            RuntimeError: 会话已关闭。
            TLSClientExeption: 原生 Go 错误未区分配置与传输，原样传播。
        """

        if self._closed:
            raise RuntimeError("requests-go session is closed")
        names = _check_duplicate_headers(source, "requests-go")
        url = urlsplit(source.real_url)
        if url.scheme == "https":
            if self.client.verify is not False:
                raise ValueError(
                    "requests-go 1.0.9 does not enforce TLS certificate verification; "
                    "HTTPS requires explicit verify=False"
                )
            if "content-length" in names:
                raise ValueError(
                    "requests-go HTTPS generates Content-Length from the request body"
                )
            if source.timeout is None or int(source.timeout) != source.timeout:
                raise ValueError(
                    "requests-go HTTPS timeout must be a positive whole number of seconds"
                )
        config = (
            deepcopy(self.client.tls_config)
            if impersonate is None
            else _preset(impersonate)
        )
        config.id = self.client.tls_config.id
        previous_adapter = self.client.get_adapter("https://")
        started = monotonic()
        try:
            received = self.client.request(
                source.method,
                url.geturl(),
                headers=dict(source.headers.raw),
                cookies=_request_cookies(source),
                data=source.body,
                proxies=dict(self.client.proxies),
                timeout=source.timeout,
                verify=self.client.verify,
                allow_redirects=source.allow_redirects,
                tls_config=config,
            )
            with received:
                return _response(source, received).copy(
                    history=tuple(_response(source, item) for item in received.history),
                    cost=monotonic() - started,
                )
        except (
            requests.ConnectionError,
            requests.Timeout,
            requests.TooManyRedirects,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ContentDecodingError,
        ) as error:
            if any(isinstance(item, TLSClientExeption) for item in error.args):
                raise
            sent = error.request
            request = (
                _sent_request(source, sent)
                if isinstance(sent, requests.PreparedRequest)
                else source
            )
            return _failed_response(source, error, started).copy(request=request)
        except (
            requests.exceptions.InvalidURL,
            requests.exceptions.InvalidSchema,
        ) as error:
            # 只转换重定向目标错误；代理等调用方配置错误继续传播。
            if not _raised_by_redirect(error):
                raise
            return _failed_response(source, error, started)
        finally:
            if previous_adapter is not self.client.get_adapter("https://"):
                previous_adapter.close()
