"""通过领域选择函数动态选择已注入下载器的同步与异步节点。"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Any, TypeVar

from interlace import AsyncNode, Context, Node, Output, Ports, Slot

from ..downloaders import (
    AsyncDownloader,
    AsyncFingerprintDownloader,
    Downloader,
    FingerprintDownloader,
)
from ..models import Request, Response

_D = TypeVar("_D", Downloader, AsyncDownloader)
_IMPERSONATE_OPTION = "crawler.fingerprint.impersonate"  # 当前执行的预设指纹覆盖键。


def _reuse_session(context: Context) -> bool:
    """读取当前执行的会话策略，不改变 Slot 或请求数据。

    Args:
        context: 含执行配置的公开上下文。

    Returns:
        默认 True；False 使用装配好的临时下载器，指纹不改变会话策略。

    Raises:
        TypeError: 会话开关或指纹值的类型错误。
        ValueError: 指纹名称为空或含首尾空白。
    """

    reuse = context.options.get("crawler.session.reuse", True)
    if type(reuse) is not bool:
        raise TypeError("crawler.session.reuse must be a boolean")
    if _IMPERSONATE_OPTION in context.options:
        impersonate = context.options[_IMPERSONATE_OPTION]
        if impersonate is not None:
            if not isinstance(impersonate, str):
                raise TypeError(f"{_IMPERSONATE_OPTION} must be a string or None")
            if not impersonate.strip() or impersonate != impersonate.strip():
                raise ValueError(
                    f"{_IMPERSONATE_OPTION} must be non-empty without whitespace"
                )
    return reuse


def _resolve(
    source: Mapping[str, _D] | Callable[[Slot], Mapping[str, _D]],
    context: Context,
    default: str,
    *,
    asynchronous: bool,
    isolated: Mapping[str, _D] | None,
) -> Mapping[str, _D]:
    """从当前逻辑执行槽取得并校验下载器映射。

    Args:
        source: 固定映射或从 Slot 读取资源的同步函数。
        context: 提供当前执行槽的公开上下文。
        default: 必须存在的默认下载器名称。
        asynchronous: 是否要求异步下载器。
        isolated: 调用方装配的临时下载器映射，None 表示不支持禁用复用。

    Returns:
        已校验的下载器映射，资源仍由调用方管理。

    Raises:
        RuntimeError: 使用资源解析函数时当前执行没有 Slot。
        TypeError: 解析结果或下载器类别非法。
        ValueError: 注册名称、默认项或临时下载器配置非法。
    """

    if not _reuse_session(context):
        if isolated is None:
            raise ValueError("session reuse disabled without isolated_downloaders")
        return isolated
    if isinstance(source, Mapping):
        return source
    if context.slot is None:
        raise RuntimeError("slot downloaders require an execution Slot")
    return _bindings(source(context.slot), default, asynchronous=asynchronous)


def _source(
    downloaders: Mapping[str, _D] | Callable[[Slot], Mapping[str, _D]],
    default: str,
    *,
    asynchronous: bool,
) -> Mapping[str, _D] | Callable[[Slot], Mapping[str, _D]]:
    """校验固定注册项或 Slot 资源解析函数。

    Args:
        downloaders: 固定映射或同步资源解析函数。
        default: 默认下载器名称。
        asynchronous: 是否要求异步下载器。

    Returns:
        固定映射快照或原资源解析函数。

    Raises:
        TypeError: 输入不是映射或同步函数。
        ValueError: 默认名称为空或固定映射非法。
    """

    if isinstance(downloaders, Mapping):
        return _bindings(downloaders, default, asynchronous=asynchronous)
    if not callable(downloaders) or inspect.iscoroutinefunction(downloaders):
        raise TypeError("downloaders must be a mapping or synchronous Slot resolver")
    if not isinstance(default, str):
        raise TypeError("default downloader name must be a string")
    if not default.strip() or default != default.strip():
        raise ValueError("default downloader name must be non-empty without whitespace")
    return downloaders


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
        _downloaders: 固定注册快照或 Slot 解析函数，资源由调用方管理。
    """

    input_ports = Ports(request=Request)  # 接收待下载的 HTTP 请求。
    output_ports = Ports(response=Response)  # 输出 HTTP 响应或传输失败响应。

    def __init__(
        self,
        downloaders: Mapping[str, Downloader]
        | Callable[[Slot], Mapping[str, Downloader]]
        | None = None,
        *,
        default: str = "default",
        select: Callable[[Request], str | None] | None = None,
        isolated_downloaders: Mapping[str, Downloader] | None = None,
    ) -> None:
        """装配同步下载节点并校验注册项。

        Args:
            downloaders: 同步实例映射或 Slot 解析函数；None 创建默认 curl_cffi 临时下载器。
            default: 默认名称，必须已注册，默认为 default。
            select: 同步、可重入的选择函数，返回名称或 None，不执行网络 I/O。
            isolated_downloaders: 禁用复用时使用的同名下载器映射；各次 fetch 自行关闭临时资源。

        Raises:
            TypeError: 选择函数非法，或下载器接口与同步执行类别不匹配。
            ValueError: 注册名称非法或默认名称未注册。
        """

        if downloaders is None:
            from ..downloaders.curl_cffi import CurlCffiDownloader

            downloaders = {"default": CurlCffiDownloader()}
            if isolated_downloaders is None:
                isolated_downloaders = downloaders
        if select is not None and (
            not callable(select) or inspect.iscoroutinefunction(select)
        ):
            raise TypeError("select must be a synchronous callable or None")
        self._downloaders = _source(
            downloaders, default, asynchronous=False
        )  # 固定注册快照或调用方拥有的 Slot 资源解析函数。
        self._default = default  # 选择器缺省或返回 None 时使用的名称。
        self._select = select  # 调用方注入的领域选择函数，不保存当前请求状态。
        self._isolated_downloaders = (
            None
            if isolated_downloaders is None
            else _bindings(isolated_downloaders, default, asynchronous=False)
        )  # 调用方拥有的临时下载器；仅禁用复用时选择，不自动创建资源。

    def execute(self, inputs: Mapping[str, Any], context: Context) -> Output:
        """复制输入请求，选择同步下载器并输出结果。

        Args:
            inputs: 含 request 端口值的输入映射，由 Graph 校验类型。
            context: 当前执行上下文，用于下载前后的协作式检查。

        Returns:
            response 端口上的领域响应。

        Raises:
            TypeError: 执行配置类型错误、下载器不支持指纹或违反协议。
            ValueError: 所选下载器未注册、临时下载器未装配或指纹值非法。
            RuntimeError: 使用 Slot 解析函数，但当前执行没有 Slot。
            ExecutionControlError: 执行已取消或超时；下载器自身异常继续传播。
        """

        context.checkpoint()
        request = inputs["request"].copy()
        downloaders = _resolve(
            self._downloaders,
            context,
            self._default,
            asynchronous=False,
            isolated=self._isolated_downloaders,
        )
        downloader = _select(downloaders, self._default, request, self._select)
        if _IMPERSONATE_OPTION in context.options:
            if not isinstance(
                downloader, FingerprintDownloader
            ) or inspect.iscoroutinefunction(downloader.fetch_with_fingerprint):
                raise TypeError(
                    "selected downloader does not support synchronous fingerprint requests"
                )
            response = downloader.fetch_with_fingerprint(
                request, impersonate=context.options[_IMPERSONATE_OPTION]
            )
        else:
            response = downloader.fetch(request)
        context.checkpoint()
        return _output(response)


class AsyncDownloadNode(AsyncNode):
    """每次触发按请求选择异步下载器，通过 await 执行网络下载。

    Attributes:
        _downloaders: 固定注册快照或 Slot 解析函数，资源由调用方管理。
    """

    input_ports = Ports(request=Request)  # 接收待下载的 HTTP 请求。
    output_ports = Ports(response=Response)  # 输出 HTTP 响应或传输失败响应。

    def __init__(
        self,
        downloaders: Mapping[str, AsyncDownloader]
        | Callable[[Slot], Mapping[str, AsyncDownloader]]
        | None = None,
        *,
        default: str = "default",
        select: Callable[[Request], str | None] | None = None,
        isolated_downloaders: Mapping[str, AsyncDownloader] | None = None,
    ) -> None:
        """装配异步下载节点并校验注册项。

        Args:
            downloaders: 异步实例映射或 Slot 解析函数；None 创建默认 curl_cffi 临时下载器。
            default: 默认名称，必须已注册，默认为 default。
            select: 同步、可重入的选择函数，返回名称或 None，不执行网络 I/O。
            isolated_downloaders: 禁用复用时使用的同名异步下载器映射；fetch 自行关闭临时资源。

        Raises:
            TypeError: 选择函数非法，或下载器接口与异步执行类别不匹配。
            ValueError: 注册名称非法或默认名称未注册。
        """

        if downloaders is None:
            from ..downloaders.curl_cffi import AsyncCurlCffiDownloader

            downloaders = {"default": AsyncCurlCffiDownloader()}
            if isolated_downloaders is None:
                isolated_downloaders = downloaders
        if select is not None and (
            not callable(select) or inspect.iscoroutinefunction(select)
        ):
            raise TypeError("select must be a synchronous callable or None")
        self._downloaders = _source(
            downloaders, default, asynchronous=True
        )  # 固定注册快照或调用方拥有的 Slot 资源解析函数。
        self._default = default  # 选择器缺省或返回 None 时使用的名称。
        self._select = select  # 同步执行的领域选择函数，不保存当前请求状态。
        self._isolated_downloaders = (
            None
            if isolated_downloaders is None
            else _bindings(isolated_downloaders, default, asynchronous=True)
        )  # 调用方拥有的临时异步下载器，不跨请求保存会话资源。

    async def execute(self, inputs: Mapping[str, Any], context: Context) -> Output:
        """复制输入请求，等待异步下载器并输出结果。

        Args:
            inputs: 含 request 端口值的输入映射，由 Graph 校验类型。
            context: 当前执行上下文，用于下载前后的协作式检查。

        Returns:
            response 端口上的领域响应。

        Raises:
            TypeError: 执行配置类型错误、下载器不支持指纹或违反协议。
            ValueError: 所选下载器未注册、临时下载器未装配或指纹值非法。
            RuntimeError: 使用 Slot 解析函数，但当前执行没有 Slot。
            asyncio.CancelledError: 异步下载被取消。
            ExecutionControlError: 执行已取消或超时；下载器自身异常继续传播。
        """

        context.checkpoint()
        request = inputs["request"].copy()
        downloaders = _resolve(
            self._downloaders,
            context,
            self._default,
            asynchronous=True,
            isolated=self._isolated_downloaders,
        )
        downloader = _select(downloaders, self._default, request, self._select)
        if _IMPERSONATE_OPTION in context.options:
            if not isinstance(
                downloader, AsyncFingerprintDownloader
            ) or not inspect.iscoroutinefunction(downloader.fetch_with_fingerprint):
                raise TypeError(
                    "selected downloader does not support asynchronous fingerprint requests"
                )
            response = await downloader.fetch_with_fingerprint(
                request, impersonate=context.options[_IMPERSONATE_OPTION]
            )
        else:
            response = await downloader.fetch(request)
        context.checkpoint()
        return _output(response)
