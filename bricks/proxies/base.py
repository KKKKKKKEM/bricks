"""代理值、供应源协议及按 Slot 保存的显式生命周期管理器。"""

from __future__ import annotations

import inspect
import time
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from threading import RLock
from typing import Protocol, TypedDict, runtime_checkable
from urllib.parse import quote, urlsplit, urlunsplit

from interlace import Slot

from bricks import Request
from bricks.models._validation import duration


def _completed(callback: Callable[[Proxy], object], proxy: Proxy) -> None:
    """确认供应源的释放回调同步完成，拒绝尚未执行的异步释放。

    Args:
        callback: recover 或 clear 回调。
        proxy: 当前代理。

    Raises:
        TypeError: 返回值不是 None；协程会被关闭，Slot 占用继续保留。
    """
    value = callback(proxy)
    if value is not None:
        if inspect.iscoroutine(value):
            value.close()
        raise TypeError("proxy release callback must return None synchronously")


def format_proxy(
    value: str,
    *,
    scheme: str = "http",
    username: str | None = None,
    password: str | None = None,
) -> str:
    """将明确的代理地址格式化为含协议和可选认证的 URL。

    Args:
        value: 完整代理 URL 或 host:port，支持方括号 IPv6。
        scheme: 缺少协议时使用的协议。
        username: 覆盖 URL 中认证的用户名。
        password: 与用户名一起提供的密码。

    Returns:
        已校验的代理 URL，认证字符按 URL 规则编码。

    Raises:
        ValueError: 地址、协议、端口或认证字段组合非法。
    """
    if (
        not isinstance(value, str)
        or not value
        or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ValueError("proxy must be a non-empty address without whitespace")
    parsed = urlsplit(value if "://" in value else f"{scheme}://{value}")
    if parsed.scheme not in {"http", "https", "socks4", "socks4a", "socks5", "socks5h"}:
        raise ValueError("unsupported proxy scheme")
    if (
        not parsed.hostname
        or parsed.port is None
        or parsed.port == 0
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("proxy requires host and port without path, query or fragment")
    if (username is None) != (password is None):
        raise ValueError("proxy username and password must be supplied together")
    authority = parsed.netloc
    if username is not None and password is not None:
        if not isinstance(username, str) or not isinstance(password, str):
            raise ValueError("proxy credentials must be strings")
        authority = (
            quote(username, safe="")
            + ":"
            + quote(password, safe="")
            + "@"
            + authority.rsplit("@", 1)[-1]
        )
    return urlunsplit((parsed.scheme, authority, "", "", ""))


@dataclass(frozen=True)
class Proxy:
    """一次取得的代理值，计数保存在 Manager 的 Slot 占用中。

    Attributes:
        proxy: 已规范化的地址字符串。
        auth: 可选同步请求认证回调，由请求插件调用，返回 None。
        threshold: 每个 Slot 占用允许的新请求数；None 无次数限制。
        ttl: 占用最长秒数；None 不按时间轮换。不会中断在途重试。
    """

    proxy: str = dataclass_field(repr=False)
    auth: Callable[[Request], None] | None = None
    threshold: int | None = None
    ttl: float | None = None

    def __repr__(self) -> str:
        """返回不含代理认证信息的表示。"""
        parsed = urlsplit(self.proxy)
        host = parsed.hostname or ""
        if parsed.port is not None:
            host = f"[{host}]:{parsed.port}" if ":" in host else f"{host}:{parsed.port}"
        authority = f"***@{host}" if parsed.username is not None else host
        redacted = urlunsplit((parsed.scheme, authority, "", "", ""))
        return f"Proxy(proxy={redacted!r}, auth={self.auth!r}, threshold={self.threshold!r}, ttl={self.ttl!r})"

    def __post_init__(self) -> None:
        """规范地址并校验计数和时长。

        Raises:
            ValueError: 阈值、地址或时长不合法。
            TypeError: 认证回调不可调用。
        """
        object.__setattr__(self, "proxy", format_proxy(self.proxy))
        if self.threshold is not None and (
            type(self.threshold) is not int or self.threshold < 1
        ):
            raise ValueError("threshold must be a positive integer or None")
        object.__setattr__(self, "ttl", duration(self.ttl, "ttl"))
        if self.auth is not None and not callable(self.auth):
            raise TypeError("proxy auth must be callable")


@runtime_checkable
class ProxyProvider(Protocol):
    """同步代理供应源，资源和客户端由装配方管理。"""

    def get(self, timeout: float | None = 30) -> Proxy:
        """取得代理或报告无可用资源。

        Args:
            timeout: 最大等待秒数；None 持续等待。

        Returns:
            独立代理值。

        Raises:
            TimeoutError: 没有可用代理。
        """
        ...

    def recover(self, proxy: Proxy) -> None:
        """归还仍可再次使用的代理。

        Args:
            proxy: 本供应源发出的代理。
        """
        ...

    def clear(self, proxy: Proxy) -> None:
        """丢弃失效代理，不保证外部代理服务删除它。

        Args:
            proxy: 本供应源发出的代理。
        """
        ...


class _FormatOptions(TypedDict):
    """供应源内部固定的地址格式字段。

    Attributes:
        scheme: 缺省协议，必须为字符串。
        username: 可选认证用户名。
        password: 可选认证密码。
    """

    scheme: str
    username: str | None
    password: str | None


class BaseProxy:
    """可选的供应源基类，仅复用地址和策略配置，不使用单例或动态注册。

    Attributes:
        _options: 代理格式化参数，构造后固定。
        _auth: 调用方持有的请求认证回调。
        _threshold: 每次取得代理携带的次数阈值。
        _ttl: 每次取得代理携带的最长占用秒数。
    """

    def __init__(
        self,
        *,
        scheme: str = "http",
        username: str | None = None,
        password: str | None = None,
        auth: Callable[[Request], None] | None = None,
        threshold: int | None = None,
        ttl: float | None = None,
    ) -> None:
        """固定供应源的格式与轮换策略。

        Args:
            scheme: 缺省地址协议。
            username: 可选代理认证用户名。
            password: 可选代理认证密码。
            auth: 额外同步请求认证回调。
            threshold: 正整数新请求阈值；None 不按次数轮换。
            ttl: 正数占用秒数；None 不按时间轮换。
        """
        self._options: _FormatOptions = {
            "scheme": scheme,
            "username": username,
            "password": password,
        }
        self._auth = auth
        self._threshold = threshold
        self._ttl = ttl
        self._make("localhost:1")

    def _make(self, value: str) -> Proxy:
        """根据当前供应源配置构造代理。

        Args:
            value: 原代理地址。

        Returns:
            经过校验的代理值。
        """
        return Proxy(
            format_proxy(value, **self._options), self._auth, self._threshold, self._ttl
        )

    def get(self, timeout: float | None = 30) -> Proxy:
        """由具体供应源实现获取。

        Args:
            timeout: 等待秒数。

        Raises:
            NotImplementedError: 基类没有供应源。
        """
        raise NotImplementedError

    def recover(self, proxy: Proxy) -> None:
        """默认不保留归还的地址，支持回收的供应源显式覆盖。

        Args:
            proxy: 不再占用的代理。
        """

    def clear(self, proxy: Proxy) -> None:
        """默认丢弃当前地址，不修改外部服务。

        Args:
            proxy: 已失效的代理。
        """


@dataclass(repr=False)
class _Holding:
    """保存在 Slot 中的一项管理器占用。

    Attributes:
        owner: 创建此占用的 Manager，用于拒绝命名空间冲突。
        provider: 当前代理的供应源。
        proxy: 当前代理值。
        acquired: 单调时钟取得时间，单位秒。
        uses: 新请求使用次数，默认零。
    """

    owner: Manager
    provider: ProxyProvider
    proxy: Proxy
    acquired: float
    uses: int = 0

    def __repr__(self) -> str:
        """返回不暴露代理凭据的占用表示。"""
        return f"_Holding(owner={self.owner!r}, provider={self.provider!r}, proxy={self.proxy!r}, acquired={self.acquired!r}, uses={self.uses!r})"


class Manager:
    """显式按 Slot 缓存代理，禁止线程局部状态、全局实例和静默直连。

    Attributes:
        providers: 按顺序尝试的借用供应源元组。
        namespace: Slot 中的存储键，默认 crawler.proxy。
        _lock: 保护本实例的占用迁移，不持有独立 Slot 注册表。
    """

    def __init__(
        self, *providers: ProxyProvider, namespace: str = "crawler.proxy"
    ) -> None:
        """装配同步供应源并固定 Slot 命名空间。

        Args:
            providers: 至少一个实现 get/recover/clear 的供应源。
            namespace: 同一 Slot 内不同 Manager 必须使用不同键。

        Raises:
            ValueError: 缺少供应源或命名空间为空。
            TypeError: 供应源不符合协议。
        """
        if not providers or not isinstance(namespace, str) or not namespace:
            raise ValueError("Manager requires providers and a namespace")
        if any(
            isinstance(provider, type) or not isinstance(provider, ProxyProvider)
            for provider in providers
        ):
            raise TypeError("providers must implement ProxyProvider")
        self.providers = tuple(providers)
        self.namespace = namespace
        self._lock = RLock()

    def _holding(self, slot: Slot) -> _Holding | None:
        """读取并检查本管理器拥有的占用。

        Args:
            slot: 当前逻辑槽，禁止缺省或线程回退。

        Returns:
            当前占用或 None。

        Raises:
            TypeError: 没有提供 Slot。
            ValueError: 命名空间被其他对象占用。
        """
        if not isinstance(slot, Slot):
            raise TypeError("proxy Manager requires an explicit Slot")
        holding = slot.get(self.namespace)
        if holding is not None and (
            not isinstance(holding, _Holding) or holding.owner is not self
        ):
            raise ValueError("proxy Slot namespace belongs to another owner")
        return holding

    def get(self, slot: Slot, *, timeout: float | None = 30) -> Proxy:
        """复用当前代理，达到阈值时先回收，再按声明顺序获取。

        Args:
            slot: 当前执行槽。
            timeout: 全部供应源共用的等待秒数；None 持续等待。

        Returns:
            当前代理，不自动增加使用次数。

        Raises:
            TimeoutError: 所有供应源均无可用代理，不回退直连。
            Exception: 供应源或回收错误原样传播。
        """
        duration(timeout, "timeout", allow_zero=True)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            holding = self._holding(slot)
            if holding is not None:
                proxy = holding.proxy
                expired = (
                    proxy.threshold is not None and holding.uses >= proxy.threshold
                ) or (
                    proxy.ttl is not None
                    and time.monotonic() - holding.acquired >= proxy.ttl
                )
                if not expired:
                    return proxy
        if holding is not None:
            self.recover(slot)
        last: TimeoutError | None = None
        for provider in self.providers:
            remaining = (
                None if deadline is None else max(0, deadline - time.monotonic())
            )
            try:
                proxy = provider.get(timeout=remaining)
            except TimeoutError as exc:
                last = exc
                continue
            if not isinstance(proxy, Proxy):
                if inspect.iscoroutine(proxy):
                    proxy.close()
                raise TypeError("ProxyProvider.get must return Proxy")
            with self._lock:
                slot[self.namespace] = _Holding(self, provider, proxy, time.monotonic())
            return proxy
        raise TimeoutError("no proxy is available") from last

    def now(self, slot: Slot) -> Proxy | None:
        """查看当前代理，不获取或轮换。

        Args:
            slot: 当前槽。

        Returns:
            当前代理或 None。
        """
        with self._lock:
            holding = self._holding(slot)
            return None if holding is None else holding.proxy

    def use(self, slot: Slot) -> None:
        """登记一次新请求，轮换延迟到下一次 get，避免改变当前重试资源。

        Args:
            slot: 当前槽。

        Raises:
            RuntimeError: 当前没有代理。
        """
        with self._lock:
            holding = self._holding(slot)
            if holding is None:
                raise RuntimeError("slot has no proxy to use")
            holding.uses += 1

    def recover(self, slot: Slot) -> None:
        """归还当前代理，供应源成功处理后才删除占用。

        Args:
            slot: 当前槽；应在相关请求与会话停止使用后调用。
        """
        with self._lock:
            holding = self._holding(slot)
        if holding is not None:
            _completed(holding.provider.recover, holding.proxy)
            with self._lock:
                if slot.get(self.namespace) is holding:
                    del slot[self.namespace]

    def clear(self, slot: Slot) -> None:
        """丢弃失效代理，不将它放回供应池。

        Args:
            slot: 当前槽；不自动关闭或重建会话。
        """
        with self._lock:
            holding = self._holding(slot)
        if holding is not None:
            _completed(holding.provider.clear, holding.proxy)
            with self._lock:
                if slot.get(self.namespace) is holding:
                    del slot[self.namespace]

    def fresh(self, slot: Slot, *, timeout: float | None = 30) -> Proxy:
        """显式丢弃后重新获取，供业务决定切换代理时调用。

        Args:
            slot: 当前槽。
            timeout: 等待秒数。

        Returns:
            新取得的代理；固定供应源可能返回相同地址。
        """
        self.clear(slot)
        return self.get(slot, timeout=timeout)
