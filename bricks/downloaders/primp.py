"""可选 primp 下载器，指纹在客户端创建时固定。"""

from __future__ import annotations

from dataclasses import dataclass
from time import monotonic
from typing import Any

import primp

from ..models import Request, Response
from ._options import TransportOptions

_PROFILES = frozenset(
    [
        "chrome",
        "edge",
        "opera",
        "safari",
        "firefox",
        "random",
        "firefox_140",
        "safari_18.5",
        "safari_26",
        "safari_26.3",
        "safari_26.4",
    ]
    + [f"chrome_{version}" for version in range(144, 153)]
    + [f"edge_{version}" for version in range(144, 152)]
    + [f"opera_{version}" for version in range(126, 135)]
    + [f"firefox_{version}" for version in range(146, 152)]
)  # 锁定 primp 2.0.0 公开文档的预设，避免原生接口对未知值随机回退。
_NETWORK_ERRORS = (
    primp.ConnectError,
    primp.TimeoutError,
    primp.DNSError,
    primp.RedirectError,
    primp.BodyError,
    primp.DecodeError,
)
# 只处理已知网络或响应读取错误，构造和未知请求错误继续传播。


def _profile(impersonate: str | None) -> str | None:
    """校验原生指纹名称，禁止静默随机降级。

    Args:
        impersonate: primp 2.0.0 的原生名称，None 不指定指纹。

    Returns:
        已验证的名称或 None。

    Raises:
        TypeError: 名称类型错误。
        ValueError: 指纹不在当前版本公开预设中。
    """

    if impersonate is not None:
        if not isinstance(impersonate, str):
            raise TypeError("impersonate must be a string or None")
        if impersonate not in _PROFILES:
            raise ValueError(f"unsupported primp fingerprint: {impersonate!r}")
    return impersonate


def _parameters(source: Request) -> dict[str, Any]:
    """构造请求参数，明确拒绝 primp 无法表达的重复请求头。

    Args:
        source: 已复制的领域请求。

    Returns:
        两种客户端共用的缓冲读取参数。

    Raises:
        ValueError: 请求包含重复头名称。
    """

    names = [name.lower() for name, _ in source.headers.raw]
    if len(names) != len(set(names)):
        raise ValueError("primp does not support duplicate request headers")
    return {
        "method": source.method,
        "url": source.real_url,
        "headers": dict(source.headers.raw),
        "cookies": dict(source.cookies),
        "content": source.body,
        "timeout": source.timeout,
        "follow_redirects": source.allow_redirects,
        "stream": True,
    }


def _response(
    source: Request,
    received: primp.Response | primp.AsyncResponse,
    content: bytes,
    started: float,
) -> Response:
    """转换响应，不伪造原生接口未提供的历史和重复响应头。

    Args:
        source: 初始提交请求副本。
        received: 原生响应。
        content: 完整读取的响应字节。
        started: 下载开始的单调时钟秒数。

    Returns:
        领域响应，Cookie 仅保留名称和值，不具备完整属性。
    """

    return Response(
        content,
        status_code=received.status_code,
        url=received.url,
        headers=received.headers,
        cookies=received.cookies,
        request=source,
        cost=monotonic() - started,
    )


@dataclass(frozen=True)
class _PrimpOptions:
    """primp 可公开配置的不可变选项，不提供原生缺失的 trust_env 开关。"""

    verify: bool = True  # 默认校验 TLS 证书。
    max_redirects: int = 20  # 单次下载的重定向上限。
    impersonate: str | None = None  # 客户端固定指纹，默认不指定。
    impersonate_os: str | None = None  # 可选设备系统，None 遵循 primp 选择行为。

    def __post_init__(self) -> None:
        """在客户端创建前校验所有公开选项。

        Raises:
            TypeError: verify 或指纹类型错误。
            ValueError: 重定向上限、指纹或系统名称非法。
        """

        TransportOptions(verify=self.verify, max_redirects=self.max_redirects)
        _profile(self.impersonate)
        if self.impersonate_os is not None and self.impersonate_os not in (
            "android",
            "ios",
            "linux",
            "macos",
            "windows",
            "random",
        ):
            raise ValueError("unsupported primp impersonate_os")

    def _client_parameters(
        self, proxy: str | None, impersonate: str | None = None
    ) -> dict[str, Any]:
        """为新客户端构造配置，环境代理行为遵循 primp。

        Args:
            proxy: 显式固定代理，None 时原生库可能读取环境代理。
            impersonate: 本次覆盖指纹，None 沿用下载器默认值。

        Returns:
            同步与异步客户端的构造参数。
        """

        return {
            "verify": self.verify,
            "max_redirects": self.max_redirects,
            "impersonate": _profile(
                self.impersonate if impersonate is None else impersonate
            ),
            "impersonate_os": self.impersonate_os,
            "proxy": proxy,
            "cookie_store": True,
        }


class PrimpDownloader(_PrimpOptions):
    """同步 primp 下载器，普通请求使用独立客户端。"""

    def prepare_session(self, *, proxy: str | None = None) -> PrimpSessionDownloader:
        """创建客户端并将唯一拥有引用交给会话包装器。

        Args:
            proxy: 固定代理。

        Returns:
            可按 Slot 保存的独立会话。
        """

        return PrimpSessionDownloader(primp.Client(**self._client_parameters(proxy)))

    def close_session(self, session: PrimpSessionDownloader) -> None:
        """清除拥有引用，禁止继续通过包装器使用会话。

        Args:
            session: 已无在途请求的会话；外部另存的 Client 引用会延长原生资源寿命。
        """

        session._client = None

    def fetch(self, request: Request) -> Response:
        """使用下载器默认指纹下载。

        Args:
            request: 本次请求。

        Returns:
            HTTP 响应或传输失败响应。
        """

        return self.fetch_with_fingerprint(request, impersonate=None)

    def fetch_with_fingerprint(
        self, request: Request, *, impersonate: str | None
    ) -> Response:
        """为本次请求创建带指定指纹的临时客户端。

        Args:
            request: 待复制的请求。
            impersonate: 原生预设名称，None 沿用默认值。

        Returns:
            HTTP 响应或传输失败响应。
        """

        source = request.copy()
        session = PrimpSessionDownloader(
            primp.Client(**self._client_parameters(source.proxy, impersonate))
        )
        try:
            return session._fetch(source)
        finally:
            self.close_session(session)


class AsyncPrimpDownloader(_PrimpOptions):
    """原生异步 primp 下载器，通过 aread 缓冲响应。"""

    async def prepare_session(
        self, *, proxy: str | None = None
    ) -> AsyncPrimpSessionDownloader:
        """创建独立的异步客户端。

        Args:
            proxy: 固定代理。

        Returns:
            调用方按 Slot 管理的会话。
        """

        return AsyncPrimpSessionDownloader(
            primp.AsyncClient(**self._client_parameters(proxy))
        )

    async def close_session(self, session: AsyncPrimpSessionDownloader) -> None:
        """释放会话包装器的拥有引用，可重复调用。

        Args:
            session: 已停止下载的会话，外部引用仍可能持有原生资源。
        """

        session._client = None

    async def fetch(self, request: Request) -> Response:
        """以默认指纹异步下载。

        Args:
            request: 待复制请求。

        Returns:
            HTTP 响应或传输失败响应。
        """

        return await self.fetch_with_fingerprint(request, impersonate=None)

    async def fetch_with_fingerprint(
        self, request: Request, *, impersonate: str | None
    ) -> Response:
        """在临时客户端构造阶段应用本次指纹。

        Args:
            request: 本次请求。
            impersonate: 原生指纹名称，None 沿用默认值。

        Returns:
            HTTP 响应或传输失败响应。

        Raises:
            asyncio.CancelledError: 取消清理后继续传播。
        """

        source = request.copy()
        session = AsyncPrimpSessionDownloader(
            primp.AsyncClient(**self._client_parameters(source.proxy, impersonate))
        )
        try:
            return await session._fetch(source)
        finally:
            await self.close_session(session)


@dataclass
class PrimpSessionDownloader:
    """持有固定指纹客户端，释放引用即结束包装器的资源所有权。"""

    _client: primp.Client | None  # 本包装器拥有的引用，close_session 后置 None。

    @property
    def client(self) -> primp.Client:
        """取得仍然开放的客户端。

        Returns:
            原生客户端；另存此引用将延长资源寿命。

        Raises:
            RuntimeError: 会话已关闭。
        """

        if self._client is None:
            raise RuntimeError("primp session is closed")
        return self._client

    def fetch(self, request: Request) -> Response:
        """使用原会话下载并保存响应 Cookie。

        Args:
            request: 待复制请求。

        Returns:
            HTTP 响应或传输失败响应。
        """

        return self.fetch_with_fingerprint(request, impersonate=None)

    def fetch_with_fingerprint(
        self, request: Request, *, impersonate: str | None
    ) -> Response:
        """只允许沿用会话已配置的指纹，不隐式重建客户端。

        Args:
            request: 本次请求。
            impersonate: None 或与当前客户端相同的预设名称。

        Returns:
            HTTP 响应或传输失败响应。

        Raises:
            ValueError: 指纹需要变更或请求指定代理。
        """

        if _profile(impersonate) is not None and impersonate != self.client.impersonate:
            raise ValueError(
                "primp session fingerprint is fixed; use crawler.session.reuse=False to change it"
            )
        source = request.copy()
        if source.proxy is not None:
            raise ValueError(
                "session proxy must be configured when preparing the session"
            )
        return self._fetch(source)

    def _fetch(self, source: Request) -> Response:
        """完整读取同步响应并释放响应句柄。

        Args:
            source: 已复制的请求。

        Returns:
            领域响应，配置错误继续传播。
        """

        parameters = _parameters(source)
        started = monotonic()
        try:
            received = self.client.request(**parameters)
            try:
                return _response(source, received, received.read(), started)
            finally:
                received.close()
        except _NETWORK_ERRORS as error:
            return Response(
                status_code=-1, error=error, request=source, cost=monotonic() - started
            )


@dataclass
class AsyncPrimpSessionDownloader:
    """持有独立异步客户端，固定会话指纹。"""

    _client: (
        primp.AsyncClient | None
    )  # 会话拥有引用，关闭后清空，不调用原生空操作退出函数。

    @property
    def client(self) -> primp.AsyncClient:
        """取得未关闭的异步客户端。

        Returns:
            原生客户端引用。

        Raises:
            RuntimeError: 包装器已关闭。
        """

        if self._client is None:
            raise RuntimeError("primp session is closed")
        return self._client

    async def fetch(self, request: Request) -> Response:
        """使用原会话指纹异步下载。

        Args:
            request: 本次请求。

        Returns:
            HTTP 响应或传输失败响应。
        """

        return await self.fetch_with_fingerprint(request, impersonate=None)

    async def fetch_with_fingerprint(
        self, request: Request, *, impersonate: str | None
    ) -> Response:
        """按固定会话指纹下载，禁止暗中重建 Cookie 或连接池。

        Args:
            request: 本次请求。
            impersonate: None 或会话当前的预设名称。

        Returns:
            HTTP 响应或传输失败响应。

        Raises:
            ValueError: 指纹要求变更或请求设置代理。
            asyncio.CancelledError: 取消继续传播。
        """

        if _profile(impersonate) is not None and impersonate != self.client.impersonate:
            raise ValueError(
                "primp session fingerprint is fixed; use crawler.session.reuse=False to change it"
            )
        source = request.copy()
        if source.proxy is not None:
            raise ValueError(
                "session proxy must be configured when preparing the session"
            )
        return await self._fetch(source)

    async def _fetch(self, source: Request) -> Response:
        """异步读取全部响应体，不使用可能阻塞的 content 属性。

        Args:
            source: 已复制的请求。

        Returns:
            领域响应。

        Raises:
            asyncio.CancelledError: 取消继续传播。
        """

        parameters = _parameters(source)
        started = monotonic()
        try:
            received = await self.client.request(**parameters)
            try:
                return _response(source, received, await received.aread(), started)
            finally:
                await received.aclose()
        except _NETWORK_ERRORS as error:
            return Response(
                status_code=-1, error=error, request=source, cost=monotonic() - started
            )
