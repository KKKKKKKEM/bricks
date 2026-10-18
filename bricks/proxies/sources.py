"""固定地址、HTTP API 和 Redis 代理供应源。"""

from __future__ import annotations

import inspect
import re
import time
from collections import deque
from collections.abc import Callable, Iterable
from threading import RLock
from typing import Any

from bricks import Downloader, Request, Response
from bricks.downloaders.curl_cffi import CurlCffiDownloader
from bricks.models._validation import duration

from .base import BaseProxy, Proxy


class CustomProxy(BaseProxy):
    """明确使用固定地址的供应源。

    Attributes:
        _proxy: 构造时校验的不可变代理值。
    """

    def __init__(self, key: str, **options: Any) -> None:
        """固定代理地址和认证策略。

        Args:
            key: 完整 URL 或 host:port。
            options: BaseProxy 的显式策略参数。
        """
        super().__init__(**options)
        self._proxy = self._make(key)

    def get(self, timeout: float | None = 30) -> Proxy:
        """立即返回固定地址。

        Args:
            timeout: 校验等待参数，不发生阻塞。

        Returns:
            固定代理值。
        """
        duration(timeout, "timeout", allow_zero=True)
        return self._proxy


def extract_addresses(response: Response) -> list[str]:
    """提取文本 API 返回的 IPv4:port；JSON、IPv6 等格式使用显式解析回调。

    Args:
        response: 成功的供应商响应。

    Returns:
        按出现顺序排列的地址列表。
    """
    addresses = re.findall(
        r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}:\d{1,5}(?!\d)", response.text
    )
    valid = []
    for address in addresses:
        host, port_text = address.rsplit(":", 1)
        octets = host.split(".")
        if all(int(octet) <= 255 for octet in octets) and 0 < int(port_text) < 65536:
            valid.append(address)
    return valid


class ApiProxy(BaseProxy):
    """批量获取并缓存 HTTP API 地址，失败有界等待，不恢复程序错误。

    Attributes:
        _request: 独立供应商请求模板，不是目标站点请求。
        _downloader: 借用的同步下载器，默认临时 curl_cffi。
        _extract: 同步响应解析回调。
        _available: 本实例缓存的可用代理。
        _lock: 串行化补充与归还。
        _interval: 空响应或可重试失败后的等待秒数。
    """

    def __init__(
        self,
        key: str,
        *,
        request: Request | None = None,
        downloader: Downloader | None = None,
        handle_response: Callable[[Response], Iterable[str]] = extract_addresses,
        poll_interval: float = 0.1,
        **options: Any,
    ) -> None:
        """装配供应商请求、解析和格式策略。

        Args:
            key: 供应商 API 的完整 URL。
            request: 可选请求模板，url 必须等于 key。
            downloader: 同步下载器，由调用方管理。
            handle_response: 提取代理地址序列的同步函数。
            poll_interval: 重试或空池等待秒数，默认 0.1。
            options: BaseProxy 策略参数。

        Raises:
            ValueError: 请求地址或间隔非法。
            TypeError: 下载器或解析函数不符合同步协议，或间隔不是数值。
        """
        super().__init__(**options)
        self._request = Request(key, timeout=10) if request is None else request.copy()
        if self._request.url != key:
            raise ValueError("API request URL must equal key")
        self._downloader = CurlCffiDownloader() if downloader is None else downloader
        if not callable(
            getattr(self._downloader, "fetch", None)
        ) or inspect.iscoroutinefunction(self._downloader.fetch):
            raise TypeError("ApiProxy requires a synchronous downloader")
        if not callable(handle_response) or inspect.iscoroutinefunction(
            handle_response
        ):
            raise TypeError("handle_response must be synchronous")
        self._extract = handle_response
        self._available: deque[Proxy] = deque()
        self._lock = RLock()
        if poll_interval is None:
            raise TypeError("poll_interval must be a number")
        duration(poll_interval, "poll_interval")
        self._interval = poll_interval
        self._last_error: BaseException | None = None

    def get(self, timeout: float | None = 30) -> Proxy:
        """获取缓存地址或在截止时间内请求 API。

        Args:
            timeout: 总等待秒数，零只读缓存；None 持续等待。

        Returns:
            已格式化的代理。

        Raises:
            TimeoutError: 期限内没有可用代理。
            RuntimeError: API 返回不可重试的 HTTP 状态。
            TypeError: 解析结果不是地址序列。
        """
        duration(timeout, "timeout", allow_zero=True)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._lock:
                if self._available:
                    return self._available.popleft()
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise TimeoutError(
                    "proxy API produced no available address"
                ) from self._last_error
            budget = self._request.timeout
            if remaining is not None:
                budget = remaining if budget is None else min(budget, remaining)
            response = self._downloader.fetch(self._request.copy(timeout=budget))
            if not isinstance(response, Response):
                if inspect.iscoroutine(response):
                    response.close()
                raise TypeError("proxy downloader must return Response")
            if 200 <= response.status_code < 300:
                values = self._extract(response)
                if inspect.isawaitable(values) or isinstance(values, (str, bytes)):
                    if inspect.iscoroutine(values):
                        values.close()
                    raise TypeError(
                        "proxy parser must return a synchronous address iterable"
                    )
                batch = [self._make(value) for value in values]
                with self._lock:
                    self._available.extend(batch)
                if batch:
                    continue
            elif (
                response.status_code != -1
                and response.status_code != 429
                and not 500 <= response.status_code < 600
            ):
                raise RuntimeError(
                    f"proxy API rejected request: status={response.status_code}"
                )
            if response.status_code == -1 and response.error is not None:
                self._last_error = response.error
            wait = (
                self._interval
                if deadline is None
                else min(self._interval, max(0, deadline - time.monotonic()))
            )
            time.sleep(wait)

    def recover(self, proxy: Proxy) -> None:
        """将仍可用的地址放到本实例缓存尾部。

        Args:
            proxy: 该供应源取得的有效代理。
        """
        with self._lock:
            self._available.append(proxy)


class RedisProxy(BaseProxy):
    """从显式 Redis set/list 弹出地址，借用客户端，不隐式创建连接。

    Attributes:
        client: 调用方持有的同步 redis-py 客户端。
        key: 代理池 Redis 键。
        key_type: set 或 list。
        _interval: 空池轮询秒数。
    """

    def __init__(
        self,
        key: str,
        client: Any,
        *,
        key_type: str = "set",
        poll_interval: float = 0.1,
        **options: Any,
    ) -> None:
        """装配 Redis 池，不导入可选 Redis 库。

        Args:
            key: 池键名。
            client: 同步客户端，网络操作超时由调用方配置。
            key_type: set 使用 SPOP，list 使用 LPOP，默认 set。
            poll_interval: 空池轮询秒数。
            options: BaseProxy 格式与轮换策略。

        Raises:
            ValueError: 键名、类型或间隔非法。
            TypeError: 间隔不是数值。
        """
        super().__init__(**options)
        if not isinstance(key, str) or not key or key_type not in {"set", "list"}:
            raise ValueError("RedisProxy requires key and set/list type")
        self.key = key
        self.client = client
        self.key_type = key_type
        self._original_values: dict[int, str] = {}
        if poll_interval is None:
            raise TypeError("poll_interval must be a number")
        duration(poll_interval, "poll_interval")
        self._interval = poll_interval

    def get(self, timeout: float | None = 30) -> Proxy:
        """原子弹出一项代理，空池按时限轮询。

        Args:
            timeout: 等待秒数，零只尝试一次；None 持续等待。

        Returns:
            代理值；池格式要求 UTF-8 地址字符串。

        Raises:
            TimeoutError: 等待结束仍无代理。
            Exception: Redis 或地址错误原样传播；不声称租约恢复。
        """
        duration(timeout, "timeout", allow_zero=True)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            value = (
                self.client.spop(self.key)
                if self.key_type == "set"
                else self.client.lpop(self.key)
            )
            if value is not None:
                raw = value.decode("utf-8") if isinstance(value, bytes) else value
                proxy = self._make(raw)
                self._original_values[id(proxy)] = raw
                return proxy
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("Redis proxy pool is empty")
            time.sleep(
                self._interval
                if deadline is None
                else min(self._interval, max(0, deadline - time.monotonic()))
            )

    def recover(self, proxy: Proxy) -> None:
        """将仍可用的地址归还池，网络错误原样传播。

        Args:
            proxy: 归还的代理值。
        """
        value = self._original_values.get(id(proxy), proxy.proxy)
        if self.key_type == "set":
            self.client.sadd(self.key, value)
        else:
            self.client.rpush(self.key, value)
        self._original_values.pop(id(proxy), None)

    def clear(self, proxy: Proxy) -> None:
        """丢弃已弹出的原地址而不写回 Redis。

        Args:
            proxy: 当前代理值。
        """
        self._original_values.pop(id(proxy), None)
