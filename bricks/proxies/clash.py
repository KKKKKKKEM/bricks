"""Clash/Mihomo 控制器适配；一个实例同时只向一个占用交付共享出口。"""

from __future__ import annotations

import inspect
import time
from collections.abc import Callable, Mapping
from threading import RLock
from typing import Any
from urllib.parse import quote, urlsplit

from bricks import Downloader, Request, Response
from bricks.downloaders.curl_cffi import CurlCffiDownloader
from bricks.models._validation import duration, http_url
from bricks.models.request import HttpMethod

from .base import BaseProxy, Proxy


class ClashProxy(BaseProxy):
    """显式查询和轮换控制器节点，不在构造时切换全局模式。

    Attributes:
        key: 控制器完整 HTTP URL。
        selector: 要轮换的选择组。
        _secret: 可选 Bearer 凭据，不写入异常或 repr。
        _downloader: 借用的同步控制请求下载器。
        _address: 明确的代理出口地址；None 从控制器配置读取。
        _match: 过滤节点名称的同步条件。
        _cache_ttl: 节点缓存秒数，零表示每次重新获取。
        _cache_time: 最近刷新单调时间。
        _names: 缓存的候选节点。
        _next: 下次轮换的位置。
        _active: 当前唯一交付，None 表示可轮换。
        _lock: 保护缓存、占用状态与节点轮换。
        _changing: 正在进行网络控制配置修改。
        _getting: 正在读取控制器并切换节点。
    """

    def __init__(
        self,
        key: str,
        *,
        selector: str = "GLOBAL",
        secret: str | None = None,
        proxy: str | None = None,
        downloader: Downloader | None = None,
        match: Callable[[str], bool] | None = None,
        cache_ttl: float = 60,
        **options: Any,
    ) -> None:
        """固定控制器配置，客户端和控制器本身归调用方管理。

        Args:
            key: 完整控制器 URL，不隐式补协议。
            selector: 选择组名称，默认 GLOBAL。
            secret: 可选控制器 Bearer token。
            proxy: 可选明确出口地址，适合端口映射和远程控制器。
            downloader: 同步 HTTP 下载器，默认临时 curl_cffi。
            match: 可选节点筛选条件，默认排除 DIRECT 和 REJECT。
            cache_ttl: 节点缓存秒数。
            options: BaseProxy 的地址格式和次数/时长阈值。
        """
        super().__init__(**options)
        self.key = http_url(key).rstrip("/")
        if not isinstance(selector, str) or not selector:
            raise ValueError("selector must be non-empty")
        self.selector = selector
        self._secret = secret
        self._downloader = CurlCffiDownloader() if downloader is None else downloader
        if not callable(
            getattr(self._downloader, "fetch", None)
        ) or inspect.iscoroutinefunction(self._downloader.fetch):
            raise TypeError("ClashProxy requires a synchronous downloader")
        self._address = None if proxy is None else self._make(proxy).proxy
        self._match = match
        if cache_ttl is None:
            raise TypeError("cache_ttl must be a number")
        duration(cache_ttl, "cache_ttl", allow_zero=True)
        self._cache_ttl = cache_ttl
        self._cache_time = float("-inf")
        self._names: list[str] = []
        self._next = 0
        self._active: Proxy | None = None
        self._lock = RLock()
        self._changing = False
        self._getting = False

    def _command(
        self,
        uri: str,
        *,
        method: HttpMethod = "GET",
        body: Any = None,
        params: Mapping[str, Any] | None = None,
        timeout: float | None = 10,
    ) -> Response:
        """执行一次控制命令，不吞掉配置、网络或响应错误。

        Args:
            uri: 已编码的控制器相对路径。
            method: HTTP 方法。
            body: JSON 请求体，None 不发送。
            params: 查询参数。
            timeout: 本次 HTTP 超时秒数。

        Returns:
            2xx 响应。

        Raises:
            RuntimeError: 控制命令失败。
        """
        headers = (
            {} if self._secret is None else {"Authorization": f"Bearer {self._secret}"}
        )
        request = Request(
            self.key + uri,
            method=method,
            headers=headers,
            params=params,
            timeout=timeout,
            **({} if body is None else {"body": body, "body_type": "json"}),
        )
        response = self._downloader.fetch(request)
        if not isinstance(response, Response):
            raise TypeError("controller downloader must return Response")
        if not 200 <= response.status_code < 300:
            raise RuntimeError(
                f"Clash command failed: method={method}, status={response.status_code}"
            ) from response.error
        return response

    def nodes(self, name: str | None = None) -> dict[str, Any]:
        """查询全部节点或指定节点，不改变选择。

        Args:
            name: 节点名称；None 查询全部节点。

        Returns:
            控制器返回的 JSON 对象。
        """
        uri = "/proxies" if name is None else "/proxies/" + quote(name, safe="")
        return self._command(uri).json()

    @property
    def configs(self) -> dict[str, Any]:
        """读取当前运行配置。

        Returns:
            控制器配置对象。
        """
        return self._command("/configs").json()

    def configure(self, values: Mapping[str, Any]) -> None:
        """显式修改运行配置，禁止在代理被占用时切换配置。

        Args:
            values: PATCH /configs 接受的字段，例如 mode。

        Raises:
            RuntimeError: 已有在途占用。
        """
        with self._lock:
            if self._active is not None or self._changing or self._getting:
                raise RuntimeError("cannot configure a leased Clash controller")
            self._changing = True
        try:
            self._command("/configs", method="PATCH", body=dict(values))
        finally:
            with self._lock:
                self._changing = False
        with self._lock:
            self._cache_time = float("-inf")

    def switch(self, name: str, *, selector: str | None = None) -> None:
        """显式切换选择组，禁止改变已交付请求的共享出口。

        Args:
            name: 目标节点名称。
            selector: 选择组，None 使用构造值。

        Raises:
            RuntimeError: 当前控制器已被占用。
        """
        with self._lock:
            if self._active is not None or self._changing or self._getting:
                raise RuntimeError("cannot switch a leased Clash controller")
            self._changing = True
        try:
            self._command(
                "/proxies/"
                + quote(self.selector if selector is None else selector, safe=""),
                method="PUT",
                body={"name": name},
            )
        finally:
            with self._lock:
                self._changing = False

    def delay(self, name: str, *, url: str, timeout: int = 5000) -> dict[str, Any]:
        """测量节点延迟，按控制器协议使用毫秒。

        Args:
            name: 要测试的节点。
            url: 测试目标的完整 URL。
            timeout: 控制器测量超时毫秒数，默认 5000。

        Returns:
            含 delay 毫秒值的控制器结果。
        """
        if type(timeout) is not int or timeout <= 0:
            raise ValueError("delay timeout must be positive milliseconds")
        return self._command(
            "/proxies/" + quote(name, safe="") + "/delay",
            params={"url": http_url(url), "timeout": timeout},
            timeout=timeout / 1000 + 1,
        ).json()

    def rules(self) -> dict[str, Any]:
        """读取控制器规则。

        Returns:
            原始 JSON 规则对象。
        """
        return self._command("/rules").json()

    def _select_proxy(self, budget: Callable[[], float | None]) -> Proxy:
        """查询、过滤并切换到下一个候选代理。

        Args:
            budget: 返回当前控制请求可用秒数的回调。

        Returns:
            即将交付的代理值。

        Raises:
            TimeoutError: 没有符合条件的节点。
            TypeError: 控制器数据或筛选回调格式非法。
        """
        if not self._names or time.monotonic() - self._cache_time >= self._cache_ttl:
            group = self._command(
                "/proxies/" + quote(self.selector, safe=""), timeout=budget()
            ).json()
            values = group.get("all")
            if not isinstance(values, list) or any(
                not isinstance(name, str) for name in values
            ):
                raise TypeError("Clash selector must expose an all array")
            names = []
            for name in values:
                accepted = (
                    name not in {"DIRECT", "REJECT"}
                    if self._match is None
                    else self._match(name)
                )
                if type(accepted) is not bool:
                    if inspect.iscoroutine(accepted):
                        accepted.close()
                    raise TypeError("Clash node match must return bool synchronously")
                if accepted:
                    names.append(name)
            self._names = names
            self._cache_time = time.monotonic()
        if not self._names:
            raise TimeoutError("Clash selector has no matching nodes")
        address = self._address
        if address is None:
            configs = self._command("/configs", timeout=budget()).json()
            port = configs.get("mixed-port") or configs.get(
                "port" if self._options["scheme"] in {"http", "https"} else "socks-port"
            )
            if type(port) is not int or not 0 < port < 65536:
                raise ValueError(
                    "Clash proxy port is unavailable; provide proxy explicitly"
                )
            host = urlsplit(self.key).hostname
            address = (
                f"[{host}]:{port}"
                if host is not None and ":" in host
                else f"{host}:{port}"
            )
        proxy = self._make(address)
        name = self._names[self._next % len(self._names)]
        self._command(
            "/proxies/" + quote(self.selector, safe=""),
            method="PUT",
            body={"name": name},
            timeout=budget(),
        )
        return proxy

    def get(self, timeout: float | None = 30) -> Proxy:
        """取得独占后轮换节点；已有占用时立即失败，不并发切换同一控制器。

        Args:
            timeout: 本次获取的控制命令预算秒数。

        Returns:
            当前选择节点的共享出口代理。

        Raises:
            TimeoutError: 控制器被占用或没有候选节点。
            TypeError: 控制器节点或筛选条件格式非法。
        """
        duration(timeout, "timeout", allow_zero=True)
        deadline = None if timeout is None else time.monotonic() + timeout

        def budget() -> float | None:
            """取得控制请求的剩余秒数。"""
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise TimeoutError("Clash proxy acquisition timed out")
            return remaining

        with self._lock:
            if self._active is not None or self._changing or self._getting:
                raise TimeoutError(
                    "Clash controller is already leased or changing; use separate controllers for concurrent slots"
                )
            self._getting = True
        try:
            proxy = self._select_proxy(budget)
        except BaseException:
            with self._lock:
                self._getting = False
            raise
        with self._lock:
            self._next += 1
            self._active = proxy
            self._getting = False
        return proxy

    def recover(self, proxy: Proxy) -> None:
        """释放本实例的控制器占用，让下一次获取可以轮换。

        Args:
            proxy: 本实例当前交付的原代理对象。

        Raises:
            ValueError: 凭据不是本实例当前交付。
        """
        with self._lock:
            if self._active is not proxy:
                raise ValueError("proxy is not the active Clash delivery")
            self._active = None

    def clear(self, proxy: Proxy) -> None:
        """释放失败占用，下一次 get 选择后续节点。

        Args:
            proxy: 本实例当前交付的代理对象。
        """
        self.recover(proxy)
