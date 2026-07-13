# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 14:17
# @Author  : Kem
# @Desc    :
import inspect
import json
import threading
import time
import urllib.parse
from collections import OrderedDict
from typing import Any, Union

from loguru import logger

from bricks.core import genesis
from bricks.core.intercept import intercept
from bricks.lib.cookies import Cookies
from bricks.lib.request import Request
from bricks.lib.response import Response


class AbstractDownloader(metaclass=genesis.MetaClass):
    reuse_session_by_default = True
    session_pool_maxsize = 32
    debug = False

    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls)
        # Sessions belong to a downloader instance, while threading.local keeps
        # clients from being shared concurrently by different worker threads.
        instance.local = threading.local()
        instance._all_sessions = {}
        instance._all_sessions_lock = threading.RLock()
        instance._session_generation = 0
        return instance

    def fetch(self, request: Request) -> Response:
        """
        发送请求以获得一个响应

        :param request: 请求或包含请求参数的字典
        :return: Response
        """
        raise NotImplementedError

    @staticmethod
    def make_request(attrs: dict) -> Request:
        """
        通过 attrs 创建请求

        :param attrs: 包含请求参数的字典
        :return:
        """
        return Request(**attrs)

    @intercept("fetch")
    def _intercept_fetch(self, func):  # noqa
        def wrapper(request: Union[Request, dict], *args, **kwargs):
            """
            同步下载器的装饰器方法

            :param request:
            :param args:
            :param kwargs:
            :return:
            """
            if isinstance(request, dict):
                request = Request(**request)

            t = time.time()

            try:
                response: Response = func(request, *args, **kwargs)
                response.cost = time.time() - t

            except KeyboardInterrupt as e:
                raise e

            except Exception as e:
                logger.error(
                    f"[请求失败] 失败原因: {str(e) or str(e.__class__.__name__)}"
                )
                self.debug and logger.exception(e)  # type: ignore
                response: Response = self.exception(request, e)

            return response

        async def async_wrapper(request: Union[Request, dict], *args, **kwargs):
            """
            异步下载器的装饰器方法

            :param request:
            :param args:
            :param kwargs:
            :return:
            """
            if isinstance(request, dict):
                request = Request(**request)

            try:
                t = time.time()
                response: Response = await func(request, *args, **kwargs)
                response.cost = time.time() - t

            except Exception as e:
                logger.error(
                    f"[请求失败] 失败原因: {str(e) or str(e.__class__.__name__)}"
                )
                self.debug and logger.exception(e)  # type: ignore
                response: Response = self.exception(request, e)

            return response

        return async_wrapper if inspect.iscoroutinefunction(func) else wrapper

    @classmethod
    def parse_data(cls, request: Request):
        if not request.body:
            return {"data": None, "type": "raw"}
        # 获取请求头中的Content-Type
        content_type = request.headers.get("Content-Type", "").lower()

        # 如果 body 本来就是字符串 / bytes -> 直接使用, 不需要转换
        if isinstance(request.body, (str, bytes)):
            return {"data": request.body, "type": "raw"}

        # 没有传 content-type, 并且 body 不为字符串, 默认设置为 application/json
        if not content_type:
            content_type = "application/json"

        # 根据Content-Type判断并处理请求体
        if "application/json" in content_type:
            try:
                # 如果Content-Type为application/json，则尝试解析JSON请求体
                body = json.dumps(request.body)
                return {"data": body, "type": content_type}

            except ValueError:
                raise ValueError(f"Invalid JSON format, raw: {request.body}")

        elif "application/x-www-form-urlencoded" in content_type:
            # 如果Content-Type为application/x-www-form-urlencoded，则可以处理表单数据
            body = urllib.parse.urlencode(request.body)
            return {"data": body, "type": content_type}

        elif "multipart/form-data" in content_type:
            body = urllib.parse.urlencode(request.body).encode("utf-8")
            return {"data": body, "type": content_type}

        else:
            raise ValueError(f"Unsupported Content-Type: {content_type}")

    def fetch_curl(self, curl_cmd: str):
        """
        发送 curl 命令以获得一个响应

        :param curl_cmd: curl 命令
        :return:
        """
        request = Request.from_curl(curl_cmd)
        return self.fetch(request)

    def make_session(self, **options):
        """
        创建一个会话

        :return:
        """
        raise NotImplementedError

    def close_session(self, session):
        """关闭具体下载器创建的 session。"""
        raise NotImplementedError

    @intercept("make_session")
    def _intercept_make_session(self, func):
        def wrapper(*args, **kwargs):
            """
            同步下载器的装饰器方法

            :param args:
            :param kwargs:
            :return:
            """
            session = func(*args, **kwargs)
            self._store_session(self._session_key(kwargs), session)
            return session

        async def async_wrapper(*args, **kwargs):
            session = await func(*args, **kwargs)
            await self._store_async_session(self._session_key(kwargs), session)
            return session

        return async_wrapper if inspect.iscoroutinefunction(func) else wrapper

    def get_session(self, **options):
        """
        获取当前会话

        :return:
        """
        key = self._session_key(options)
        sessions = self._sessions()
        session = sessions.get(key)
        if session is not None:
            sessions.move_to_end(key)
            return session
        return self.make_session(**options)

    def should_reuse_session(self, request: Request) -> bool:
        if request.use_session is None:
            return self.reuse_session_by_default
        return request.use_session

    def _sessions(self) -> OrderedDict:
        with self._all_sessions_lock:
            generation = getattr(self.local, "session_generation", None)
            sessions = getattr(self.local, "sessions", None)
            if sessions is None or generation != self._session_generation:
                sessions = OrderedDict()
                self.local.sessions = sessions
                self.local.session_generation = self._session_generation
            return sessions

    @classmethod
    def _freeze_session_option(cls, value: Any):
        if isinstance(value, dict):
            items = (
                (cls._freeze_session_option(key), cls._freeze_session_option(item))
                for key, item in value.items()
            )
            return "dict", tuple(sorted(items, key=repr))
        if isinstance(value, list):
            return "list", tuple(cls._freeze_session_option(item) for item in value)
        if isinstance(value, tuple):
            return "tuple", tuple(cls._freeze_session_option(item) for item in value)
        if isinstance(value, set):
            return "set", frozenset(
                cls._freeze_session_option(item) for item in value
            )
        try:
            hash(value)
        except TypeError:
            return "repr", type(value), repr(value)
        return "value", type(value), value

    @classmethod
    def _session_key(cls, options: dict):
        return cls._freeze_session_option(options)

    @staticmethod
    def make_response_history(response, request: Request) -> list:
        history = []
        for item in getattr(response, "history", None) or []:
            prepared = getattr(item, "request", None)
            raw_cookies = getattr(item, "cookies", None)
            if isinstance(raw_cookies, dict):
                cookies = Cookies(raw_cookies)
            else:
                try:
                    cookies = Cookies.by_jar(raw_cookies)
                except (AttributeError, KeyError, TypeError):
                    cookies = Cookies()
            history.append(
                Response(
                    content=getattr(item, "content", b""),
                    headers=dict(getattr(item, "headers", {}) or {}),
                    cookies=cookies,
                    url=str(getattr(item, "url", "")),
                    status_code=getattr(item, "status_code", -1),
                    reason=getattr(
                        item,
                        "reason",
                        getattr(item, "reason_phrase", ""),
                    ),
                    request=Request(
                        url=str(getattr(prepared, "url", "")),
                        method=getattr(prepared, "method", "GET"),
                        body=getattr(
                            prepared,
                            "body",
                            getattr(prepared, "content", None),
                        ),
                        headers=dict(getattr(prepared, "headers", {}) or {}),
                        use_session=request.use_session,
                    ),
                )
            )
        return history

    def _store_session(self, key, session):
        stale = []
        with self._all_sessions_lock:
            sessions = self._sessions()
            old_session = sessions.pop(key, None)
            if (
                old_session is not None
                and old_session is not session
                and all(item is not old_session for item in sessions.values())
            ):
                self._all_sessions.pop(id(old_session), None)
                stale.append(old_session)
            sessions[key] = session
            self._all_sessions[id(session)] = session
            stale.extend(self._evict_sessions(sessions))

        for old_session in stale:
            self.close_session(old_session)

    async def _store_async_session(self, key, session):
        stale = []
        with self._all_sessions_lock:
            sessions = self._sessions()
            old_session = sessions.pop(key, None)
            if (
                old_session is not None
                and old_session is not session
                and all(item is not old_session for item in sessions.values())
            ):
                self._all_sessions.pop(id(old_session), None)
                stale.append(old_session)
            sessions[key] = session
            self._all_sessions[id(session)] = session
            stale.extend(self._evict_sessions(sessions))

        for old_session in stale:
            result = self.close_session(old_session)
            if inspect.isawaitable(result):
                await result

    def _evict_sessions(self, sessions: OrderedDict) -> list:
        stale = []
        maxsize = self.session_pool_maxsize
        if maxsize is None or maxsize <= 0:
            return stale
        while len(sessions) > maxsize:
            _, old_session = sessions.popitem(last=False)
            if any(item is old_session for item in sessions.values()):
                continue
            self._all_sessions.pop(id(old_session), None)
            stale.append(old_session)
        return stale

    def _register_session(self, session):
        """Register a session for cross-thread cleanup."""
        with self._all_sessions_lock:
            self._all_sessions[id(session)] = session

    def _unregister_session(self, session):
        """Unregister a session."""
        with self._all_sessions_lock:
            self._all_sessions.pop(id(session), None)

    def _take_sessions_for_cleanup(self) -> list:
        with self._all_sessions_lock:
            self._session_generation += 1
            sessions = list(self._all_sessions.values())
            self._all_sessions.clear()
            self.local.sessions = OrderedDict()
            self.local.session_generation = self._session_generation
            return sessions

    def clear_session(self):
        for session in self._take_sessions_for_cleanup():
            try:
                self.close_session(session)
            except Exception as e:
                logger.error(
                    "[清空 session 失败] "
                    f"失败原因: {str(e) or str(e.__class__.__name__)}",
                    error=e,
                )

    def exception(self, request: Request, error: Exception):
        """
        错误处理
        """
        return Response.make_response(
            error=error.__class__.__name__,
            reason=str(error),
            url=request.real_url,
            request=request,
            status_code=-1,
        )
