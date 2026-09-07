"""爬虫下载器的窄协议，支持结构化替换同步与异步实现。"""

from typing import Protocol, TypeVar, runtime_checkable

from ..models import Request, Response


class Downloader(Protocol):
    """同步下载协议，实现必须可重入。

    仅将传输失败转换为响应，配置错误、程序错误和执行控制信号继续传播。
    """

    def fetch(self, request: Request) -> Response:
        """发送独立请求并返回响应。

        Args:
            request: 本次下载的请求数据，不应保存为共享执行状态。

        Returns:
            HTTP 响应，或 status_code=-1 且携带异常的传输失败响应。

        Raises:
            ExecutionControlError: 引擎取消或超时等控制信号，必须原样传播。
        """
        ...


class AsyncDownloader(Protocol):
    """异步下载协议，与同步协议共享请求、响应和失败语义。"""

    async def fetch(self, request: Request) -> Response:
        """异步发送独立请求并返回响应。

        Args:
            request: 本次下载的请求数据，不应保存为共享执行状态。

        Returns:
            HTTP 响应，或 status_code=-1 且携带异常的传输失败响应。

        Raises:
            asyncio.CancelledError: 异步任务被取消，必须原样传播。
            ExecutionControlError: 引擎执行控制信号，必须原样传播。
        """
        ...


_Session = TypeVar("_Session", bound=Downloader)
_AsyncSession = TypeVar("_AsyncSession", bound=AsyncDownloader)


@runtime_checkable
class FingerprintDownloader(Downloader, Protocol):
    """可选的同步请求级指纹能力，不改变下载器的会话所有权。"""

    def fetch_with_fingerprint(
        self, request: Request, *, impersonate: str | None
    ) -> Response:
        """以本次指纹发送请求，保持会话默认配置不变。

        Args:
            request: 本次下载的请求。
            impersonate: 浏览器指纹名称，None 表示沿用下载器或会话默认值。

        Returns:
            HTTP 响应或传输失败响应。

        Raises:
            TypeError: 指纹值类型错误。
            ValueError: 指纹配置非法。
        """
        ...


@runtime_checkable
class AsyncFingerprintDownloader(AsyncDownloader, Protocol):
    """可选的异步请求级指纹能力，会话可继续复用。"""

    async def fetch_with_fingerprint(
        self, request: Request, *, impersonate: str | None
    ) -> Response:
        """异步发送带本次指纹的请求，不改写会话默认值。

        Args:
            request: 本次下载的请求。
            impersonate: 浏览器指纹名称，None 表示沿用默认值。

        Returns:
            HTTP 响应或传输失败响应。

        Raises:
            asyncio.CancelledError: 取消继续传播。
            TypeError: 指纹类型错误。
            ValueError: 指纹配置非法。
        """
        ...


@runtime_checkable
class SessionDownloader(Downloader, Protocol[_Session]):
    """可选同步会话能力；普通下载器无需实现。

    准备结果仍满足 Downloader，可放入 Slot 的下载器映射；调用方负责成对关闭。
    """

    def prepare_session(self) -> _Session:
        """准备一个独立会话，不与其他准备结果共享业务状态。

        Returns:
            可直接 fetch 的会话下载器；所有权交给调用方。

        Raises:
            Exception: 资源初始化失败，具体异常由实现定义，不转换为 Response。
        """
        ...

    def close_session(self, session: _Session) -> None:
        """关闭已停止使用的会话，重复关闭应安全。

        Args:
            session: 本实现 prepare_session 返回的会话，不能仍有在途调用。

        Raises:
            Exception: 资源释放失败，具体异常由实现定义并继续传播。
        """
        ...


@runtime_checkable
class AsyncSessionDownloader(AsyncDownloader, Protocol[_AsyncSession]):
    """可选异步会话能力；准备、下载和关闭遵守实现的事件循环约束。"""

    async def prepare_session(self) -> _AsyncSession:
        """异步准备一个由调用方拥有的独立会话。

        Returns:
            可直接 await fetch 的会话下载器。

        Raises:
            Exception: 初始化失败，具体异常由实现定义。
            asyncio.CancelledError: 准备被取消，尚未交付的资源由实现清理。
        """
        ...

    async def close_session(self, session: _AsyncSession) -> None:
        """异步关闭已停止使用的会话，重复关闭应安全。

        Args:
            session: 本实现准备的会话，必须已无在途下载。

        Raises:
            Exception: 资源释放失败，具体异常由实现定义。
            asyncio.CancelledError: 关闭被取消，调用方应保证清理完成。
        """
        ...


__all__ = [
    "AsyncDownloader",
    "AsyncFingerprintDownloader",
    "AsyncSessionDownloader",
    "Downloader",
    "FingerprintDownloader",
    "SessionDownloader",
]
