# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 14:17
# @Author  : Kem
# @Desc    :
import inspect
import json
import threading
import time
import urllib.parse
from typing import Any, Union

from loguru import logger

from bricks.core import genesis
from bricks.core.intercept import intercept
from bricks.lib.request import Request
from bricks.lib.response import Response


class AbstractDownloader(metaclass=genesis.MetaClass):
    reuse_session_by_default = True
    debug = False
    _all_sessions: set = set()
    _all_sessions_lock: threading.Lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls)
        # Sessions belong to a downloader instance, while threading.local keeps
        # clients from being shared concurrently by different worker threads.
        instance.local = threading.local()
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
        return sessions.get(key) or self.make_session(**options)

    def should_reuse_session(self, request: Request) -> bool:
        if request.use_session is None:
            return self.reuse_session_by_default
        return request.use_session

    def _sessions(self) -> dict:
        sessions = getattr(self.local, "sessions", None)
        if sessions is None:
            sessions = {}
            self.local.sessions = sessions
        return sessions

    @classmethod
    def _freeze_session_option(cls, value: Any):
        if isinstance(value, dict):
            return tuple(
                sorted(
                    (str(key), cls._freeze_session_option(item))
                    for key, item in value.items()
                )
            )
        if isinstance(value, (list, tuple)):
            return tuple(cls._freeze_session_option(item) for item in value)
        if isinstance(value, set):
            return tuple(
                sorted(cls._freeze_session_option(item) for item in value)
            )
        try:
            hash(value)
        except TypeError:
            return repr(value)
        return value

    @classmethod
    def _session_key(cls, options: dict):
        return cls._freeze_session_option(options)

    def _store_session(self, key, session):
        sessions = self._sessions()
        old_session = sessions.get(key)
        if old_session is not None and old_session is not session:
            self._unregister_session(old_session)
            self.close_session(old_session)
        sessions[key] = session
        self._register_session(session)

    async def _store_async_session(self, key, session):
        sessions = self._sessions()
        old_session = sessions.get(key)
        if old_session is not None and old_session is not session:
            self._unregister_session(old_session)
            result = self.close_session(old_session)
            if inspect.isawaitable(result):
                await result
        sessions[key] = session
        self._register_session(session)

    def _register_session(self, session):
        """Register a session for cross-thread cleanup."""
        with type(self)._all_sessions_lock:
            type(self)._all_sessions.add(session)

    def _unregister_session(self, session):
        """Unregister a session."""
        with type(self)._all_sessions_lock:
            type(self)._all_sessions.discard(session)

    def clear_session(self):
        closed = set()
        # Clear calling thread's local sessions
        sessions = getattr(self.local, "sessions", None)
        if sessions:
            for session in list(sessions.values()):
                closed.add(session)
                try:
                    self.close_session(session)
                except Exception as e:
                    logger.error(
                        "[清空 session 失败] "
                        f"失败原因: {str(e) or str(e.__class__.__name__)}",
                        error=e,
                    )
            sessions.clear()

        # Close remaining registered sessions across all threads
        with type(self)._all_sessions_lock:
            for session in type(self)._all_sessions:
                if session not in closed:
                    try:
                        self.close_session(session)
                    except Exception as e:
                        logger.error(
                            "[清空 session 失败] "
                            f"失败原因: {str(e) or str(e.__class__.__name__)}",
                            error=e,
                        )
            type(self)._all_sessions.clear()

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
