"""下载器共享的传输配置校验。"""

from collections.abc import AsyncIterator, Callable, Iterable, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from time import monotonic
from typing import Any
from urllib.parse import urljoin, urlsplit

from ..models import Request, Response
from ..models._validation import http_url


def _failed_response(source: Request, error: Exception, started: float) -> Response:
    """创建保留原始异常的传输失败响应。

    Args:
        source: 本次请求的独立副本。
        error: 已识别的网络或重定向异常。
        started: 下载开始时的单调时钟读数。

    Returns:
        状态为 -1 的领域响应。
    """

    return Response(
        status_code=-1,
        error=error,
        request=source,
        cost=monotonic() - started,
    )


def _redirect_failure(
    source: Request,
    status_code: int,
    url: str,
    headers: Iterable[tuple[str, str]],
    started: float | None = None,
) -> Response | None:
    """将无法解析或非 HTTP 重定向目标转换为失败响应。

    Args:
        source: 初始提交请求的独立副本。
        status_code: 收到的响应状态。
        url: 收到响应时的 URL。
        headers: 收到的响应头。
        started: 下载开始时的单调时钟读数；None 时由调用方覆盖耗时。

    Returns:
        无效重定向时返回状态为 -1 的响应，否则返回 None。
    """

    if not source.allow_redirects or not 300 <= status_code < 400:
        return None
    location = next(
        (value for name, value in headers if name.lower() == "location"), None
    )
    if location is None:
        return None
    try:
        target = location if urlsplit(location).scheme else urljoin(url, location)
        http_url(target)
    except (TypeError, ValueError) as error:
        return _failed_response(
            source, error, monotonic() if started is None else started
        )
    return None


def _check_duplicate_headers(source: Request, downloader: str) -> list[str]:
    """校验适配器是否可接收重复请求头，并返回小写名称。

    Args:
        source: 待发送的领域请求。
        downloader: 下载器名称，用于错误信息。

    Returns:
        按原始顺序排列的小写请求头名称。

    Raises:
        ValueError: 当前下载器不能保留重复请求头。
    """

    names = [name.lower() for name, _ in source.headers.raw]
    if len(names) != len(set(names)):
        raise ValueError(f"{downloader} does not support duplicate request headers")
    return names


@contextmanager
def _ephemeral_session(
    prepare: Callable[..., Any], close: Callable[[Any], Any], proxy: str | None
) -> Iterator[Any]:
    """为单次同步请求准备会话并确保关闭。

    Args:
        prepare: 创建会话的绑定方法。
        close: 关闭该会话的绑定方法。
        proxy: 本次请求固定代理。

    Yields:
        仅供当前请求使用的会话。
    """

    session = prepare(proxy=proxy)
    try:
        yield session
    finally:
        close(session)


@asynccontextmanager
async def _async_ephemeral_session(
    prepare: Callable[..., Any], close: Callable[[Any], Any], proxy: str | None
) -> AsyncIterator[Any]:
    """为单次异步请求准备会话并确保关闭。

    Args:
        prepare: 创建会话的异步绑定方法。
        close: 关闭该会话的异步绑定方法。
        proxy: 本次请求固定代理。

    Yields:
        仅供当前请求使用的会话。
    """

    session = await prepare(proxy=proxy)
    try:
        yield session
    finally:
        await close(session)


@dataclass(frozen=True)
class TransportOptions:
    """不可变的 TLS、环境代理和重定向配置。"""

    verify: bool = True  # 是否校验 TLS 证书，默认启用。
    trust_env: bool = False  # 是否读取环境代理，默认禁用；证书行为由具体库决定。
    max_redirects: int = 20  # 单次下载允许跟随的最大重定向次数。

    def __post_init__(self) -> None:
        """在网络调用前校验传输配置。

        Raises:
            TypeError: verify 或 trust_env 不是布尔值。
            ValueError: max_redirects 不是非负整数。
        """

        if type(self.verify) is not bool or type(self.trust_env) is not bool:
            raise TypeError("verify and trust_env must be booleans")
        if type(self.max_redirects) is not int or self.max_redirects < 0:
            raise ValueError("max_redirects must be a non-negative integer")
