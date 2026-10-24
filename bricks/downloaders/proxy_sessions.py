"""按请求代理选择或懒建独立会话的同步下载器。"""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any

from ..models import Request, Response
from . import Downloader, FingerprintDownloader, SessionDownloader


class ProxySessions:
    """在一个 Slot 内按 Request.proxy 路由到各自固定代理的会话。

    每个代理地址（None 表示直连或实现默认策略）对应一个经 prepare_session(proxy=...) 准备的独立会话，
    会话代理在准备时固定，不热切换；Request.proxy 只用于选择会话，转发前清除。超过 max_sessions
    时关闭最久未使用的会话，其 Cookie 与连接随之丢弃。Cookie 按"Slot × 代理"隔离。
    实例由装配方按 Slot 保存并在停止使用后 close，不跨 Slot 共享。

    Attributes:
        factory: 准备与关闭会话的同步会话下载器，由调用方管理。
        max_sessions: 同时保留的会话数量上限，正整数。
        _sessions: 代理地址到会话的映射，按最近使用排序，末尾最新。
        _lock: 保护会话映射与关闭标志，不在下载期间持有。
        _closed: 是否已关闭；关闭后拒绝下载。
    """

    def __init__(self, factory: SessionDownloader[Any], *, max_sessions: int = 4):
        """绑定会话工厂，不立即准备会话。

        Args:
            factory: 实现 prepare_session/close_session 的同步会话下载器。
            max_sessions: 同时保留的会话数量上限，默认 4。

        Raises:
            TypeError: factory 不是同步会话下载器，或 max_sessions 不是整数。
            ValueError: max_sessions 小于 1。
        """

        if not isinstance(factory, SessionDownloader):
            raise TypeError("factory must implement SessionDownloader")
        if type(max_sessions) is not int:
            raise TypeError("max_sessions must be an integer")
        if max_sessions < 1:
            raise ValueError("max_sessions must be at least 1")
        self.factory = factory
        self.max_sessions = max_sessions
        self._sessions: OrderedDict[str | None, Downloader] = OrderedDict()
        self._lock = threading.Lock()
        self._closed = False

    @property
    def proxies(self) -> tuple[str | None, ...]:
        """当前保留会话的代理地址，按最近使用排序。"""

        with self._lock:
            return tuple(self._sessions)

    def fetch(self, request: Request) -> Response:
        """选择 request.proxy 对应的会话发送请求。

        Args:
            request: 待发送请求；本对象不被修改。

        Returns:
            会话返回的响应，关联请求恢复为原始代理。

        Raises:
            RuntimeError: 已关闭。
            Exception: 会话准备、淘汰关闭或下载器自身的异常继续传播。
        """

        session, routed = self._route(request)
        return self._restore(session.fetch(routed), request)

    def fetch_with_fingerprint(
        self, request: Request, *, impersonate: str | None
    ) -> Response:
        """选择会话并发送带本次预设指纹的请求。

        Args:
            request: 待发送请求；本对象不被修改。
            impersonate: 本次预设指纹名称，None 沿用会话默认值。

        Returns:
            会话返回的响应，关联请求恢复为原始代理。

        Raises:
            RuntimeError: 已关闭。
            TypeError: 选中的会话不支持同步指纹请求。
            Exception: 会话准备或下载器自身的异常继续传播。
        """

        session, routed = self._route(request)
        if not isinstance(session, FingerprintDownloader):
            raise TypeError(
                "selected downloader does not support synchronous fingerprint requests"
            )
        response = session.fetch_with_fingerprint(routed, impersonate=impersonate)
        return self._restore(response, request)

    def close(self) -> None:
        """关闭全部已准备会话，重复调用安全。

        Raises:
            BaseException: 首个关闭失败；其余会话仍尝试关闭，后续失败作为 __context__ 链接。
        """

        with self._lock:
            self._closed = True
            sessions = list(self._sessions.values())
            self._sessions.clear()
        failure: BaseException | None = None
        for session in reversed(sessions):
            try:
                self.factory.close_session(session)
            except BaseException as error:
                failure = failure or error
        if failure is not None:
            raise failure

    def _route(self, request: Request) -> tuple[Downloader, Request]:
        """取得或准备代理对应的会话，必要时关闭最久未使用的会话。

        Returns:
            选中的会话与清除代理后的请求副本。

        Raises:
            RuntimeError: 已关闭。
        """

        proxy = request.proxy
        with self._lock:
            if self._closed:
                raise RuntimeError("proxy sessions are closed")
            session = self._sessions.get(proxy)
            if session is not None:
                self._sessions.move_to_end(proxy)
                return session, request.copy(proxy=None)
            evicted = (
                self._sessions.popitem(last=False)[1]
                if len(self._sessions) >= self.max_sessions
                else None
            )
        if evicted is not None:
            self.factory.close_session(evicted)
        session = self.factory.prepare_session(proxy=proxy)
        with self._lock:
            existing = None if self._closed else self._sessions.get(proxy)
            if not self._closed and existing is None:
                self._sessions[proxy] = session
                return session, request.copy(proxy=None)
            closed = self._closed
        # 已关闭，或并发调用先登记了同一代理：关闭多余会话，不覆盖已登记的会话。
        self.factory.close_session(session)
        if closed or existing is None:
            raise RuntimeError("proxy sessions are closed")
        return existing, request.copy(proxy=None)

    @staticmethod
    def _restore(response: Response, request: Request) -> Response:
        """让响应关联的请求快照保留原始代理。"""

        if response.request is None:
            return response
        return response.copy(request=request)


__all__ = ["ProxySessions"]
