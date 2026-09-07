"""通过领域选择函数动态选择已注入下载器的同步与异步节点。"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Any, TypeVar

from interlace import AsyncNode, Context, Node, Output, Ports

from ..downloaders import AsyncDownloader, Downloader
from ..models import Request, Response

_D = TypeVar("_D", Downloader, AsyncDownloader)


def _bindings(
    downloaders: Mapping[str, _D], default: str, *, asynchronous: bool
) -> Mapping[str, _D]:
    """校验下载器注册项并冻结名称映射。

    Args:
        downloaders: 注册名称到下载器实例的映射，实例仍由调用方管理。
        default: 默认下载器的注册名称。
        asynchronous: 是否要求所有 fetch 方法使用异步定义。

    Returns:
        与输入映射独立、共享原下载器实例的只读映射。

    Raises:
        TypeError: 参数类型错误、缺少 fetch 或同步异步类别不匹配。
        ValueError: 注册名称非法或默认下载器未注册。
    """

    if not isinstance(downloaders, Mapping):
        raise TypeError("downloaders must be a mapping of names to instances")
    prepared = dict(downloaders)
    for name, downloader in prepared.items():
        if not isinstance(name, str):
            raise TypeError("downloader names must be strings")
        if not name.strip() or name != name.strip():
            raise ValueError(
                "downloader names must be non-empty without outer whitespace"
            )
        fetch = getattr(downloader, "fetch", None)
        if not callable(fetch):
            raise TypeError(f"downloader {name!r} must provide fetch(request)")
        if inspect.iscoroutinefunction(fetch) != asynchronous:
            mode = "async" if asynchronous else "sync"
            raise TypeError(f"downloader {name!r} must provide a {mode} fetch method")
    if not isinstance(default, str):
        raise TypeError("default downloader name must be a string")
    if default not in prepared:
        raise ValueError(f"default downloader {default!r} is not registered")
    return MappingProxyType(prepared)


def _select(
    downloaders: Mapping[str, _D],
    default: str,
    request: Request,
    select: Callable[[Request], str | None] | None,
) -> _D:
    """根据当前请求选择下载器，不修改注册关系。

    Args:
        downloaders: 已校验的只读下载器映射。
        default: 默认下载器名称。
        request: 本次执行的请求副本。
        select: 同步选择函数；未提供或返回 None 时使用默认名称。

    Returns:
        选中的下载器实例。

    Raises:
        TypeError: 选择函数返回值不是字符串或 None。
        ValueError: 选中的名称未注册；选择函数自身的异常继续传播。
    """

    selected = None if select is None else select(request)
    if selected is not None and not isinstance(selected, str):
        raise TypeError("select(request) must return a downloader name or None")
    name = default if selected is None else selected
    if name not in downloaders:
        raise ValueError(f"downloader {name!r} is not registered")
    return downloaders[name]


def _output(response: Response) -> Output:
    """校验下载结果并封装为领域输出。

    Args:
        response: 下载器返回的结果。

    Returns:
        从 response 端口输出的 Response。

    Raises:
        TypeError: 下载器未返回 Response。
    """

    if not isinstance(response, Response):
        raise TypeError("downloader.fetch() must return a Response")
    return Output(response, "response")


class DownloadNode(Node):
    """每次触发按请求选择同步下载器，注入实例由调用方管理。

    Attributes:
        _downloaders: 独立的只读注册映射，实例仍由调用方管理。
    """

    input_ports = Ports(request=Request)  # 接收待下载的 HTTP 请求。
    output_ports = Ports(response=Response)  # 输出 HTTP 响应或传输失败响应。

    def __init__(
        self,
        downloaders: Mapping[str, Downloader],
        *,
        default: str = "default",
        select: Callable[[Request], str | None] | None = None,
    ) -> None:
        """装配同步下载节点并校验注册项。

        Args:
            downloaders: 下载器名称到可重入同步实例的映射。
            default: 默认名称，必须已注册，默认为 default。
            select: 同步、可重入的选择函数，返回名称或 None，不执行网络 I/O。

        Raises:
            TypeError: 选择函数非法，或下载器接口与同步执行类别不匹配。
            ValueError: 注册名称非法或默认名称未注册。
        """

        if select is not None and (
            not callable(select) or inspect.iscoroutinefunction(select)
        ):
            raise TypeError("select must be a synchronous callable or None")
        self._downloaders = _bindings(
            downloaders, default, asynchronous=False
        )  # 只读注册快照。
        self._default = default  # 选择器缺省或返回 None 时使用的名称。
        self._select = select  # 调用方注入的领域选择函数，不保存当前请求状态。

    def execute(self, inputs: Mapping[str, Any], context: Context) -> Output:
        """复制输入请求，选择同步下载器并输出结果。

        Args:
            inputs: 含 request 端口值的输入映射，由 Graph 校验类型。
            context: 当前执行上下文，用于下载前后的协作式检查。

        Returns:
            response 端口上的领域响应。

        Raises:
            TypeError: 选择器或下载器违反返回值契约。
            ValueError: 所选下载器未注册。
            ExecutionControlError: 执行已取消或超时；下载器自身异常继续传播。
        """

        context.checkpoint()
        request = inputs["request"].copy()
        downloader = _select(self._downloaders, self._default, request, self._select)
        response = downloader.fetch(request)
        context.checkpoint()
        return _output(response)


class AsyncDownloadNode(AsyncNode):
    """每次触发按请求选择异步下载器，通过 await 执行网络下载。

    Attributes:
        _downloaders: 独立的只读注册映射，实例仍由调用方管理。
    """

    input_ports = Ports(request=Request)  # 接收待下载的 HTTP 请求。
    output_ports = Ports(response=Response)  # 输出 HTTP 响应或传输失败响应。

    def __init__(
        self,
        downloaders: Mapping[str, AsyncDownloader],
        *,
        default: str = "default",
        select: Callable[[Request], str | None] | None = None,
    ) -> None:
        """装配异步下载节点并校验注册项。

        Args:
            downloaders: 下载器名称到可重入异步实例的映射。
            default: 默认名称，必须已注册，默认为 default。
            select: 同步、可重入的选择函数，返回名称或 None，不执行网络 I/O。

        Raises:
            TypeError: 选择函数非法，或下载器接口与异步执行类别不匹配。
            ValueError: 注册名称非法或默认名称未注册。
        """

        if select is not None and (
            not callable(select) or inspect.iscoroutinefunction(select)
        ):
            raise TypeError("select must be a synchronous callable or None")
        self._downloaders = _bindings(
            downloaders, default, asynchronous=True
        )  # 只读注册快照。
        self._default = default  # 选择器缺省或返回 None 时使用的名称。
        self._select = select  # 同步执行的领域选择函数，不保存当前请求状态。

    async def execute(self, inputs: Mapping[str, Any], context: Context) -> Output:
        """复制输入请求，等待异步下载器并输出结果。

        Args:
            inputs: 含 request 端口值的输入映射，由 Graph 校验类型。
            context: 当前执行上下文，用于下载前后的协作式检查。

        Returns:
            response 端口上的领域响应。

        Raises:
            TypeError: 选择器或下载器违反返回值契约。
            ValueError: 所选下载器未注册。
            asyncio.CancelledError: 异步下载被取消。
            ExecutionControlError: 执行已取消或超时；下载器自身异常继续传播。
        """

        context.checkpoint()
        request = inputs["request"].copy()
        downloader = _select(self._downloaders, self._default, request, self._select)
        response = await downloader.fetch(request)
        context.checkpoint()
        return _output(response)
