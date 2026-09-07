"""爬虫下载器的窄协议，支持结构化替换同步与异步实现。"""

from typing import Protocol

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


__all__ = ["AsyncDownloader", "Downloader"]
