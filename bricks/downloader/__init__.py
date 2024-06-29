# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 14:17
# @Author  : Kem
# @Desc    :
import functools
import inspect
import json
import threading
import time
import urllib.parse
from typing import Union

from loguru import logger

from bricks.core import genesis
from bricks.lib.request import Request
from bricks.lib.response import Response


class AbstractDownloader(metaclass=genesis.MetaClass):
    local = threading.local()
    debug = False

    def fetch(self, request: Union[Request, dict]) -> Response:
        """
        发送请求以获得一个响应

        :param request: 请求或包含请求参数的字典
        :return:
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

    def _when_fetch(self, func):  # noqa

        @functools.wraps(func)
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
                response.rt = time.time() - t

            except KeyboardInterrupt as e:
                raise e

            except Exception as e:
                logger.error(f'[请求失败] 失败原因: {str(e) or str(e.__class__.__name__)}')
                self.debug and logger.exception(e)
                response: Response = Response.make_response(
                    error=e.__class__.__name__,
                    reason=str(e),
                    url=request.real_url,
                    request=request,
                    status_code=-1
                )

            return response

        @functools.wraps(func)
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
                response.rt = time.time() - t

            except Exception as e:
                logger.error(f'[请求失败] 失败原因: {str(e) or str(e.__class__.__name__)}')
                self.debug and logger.exception(e)
                response: Response = Response.make_response(
                    error=e.__class__.__name__,
                    reason=str(e),
                    url=request.real_url,
                    request=request,
                    status_code=-1
                )

            return response

        return async_wrapper if inspect.iscoroutinefunction(func) else wrapper

    @classmethod
    def parse_data(cls, request: Request):
        if not request.body:
            return {
                "data": None,
                "type": "raw"
            }
        # 获取请求头中的Content-Type
        content_type = request.headers.get('Content-Type', '').lower()

        # 如果 body 本来就是字符串 / bytes -> 直接使用, 不需要转换
        if isinstance(request.body, (str, bytes)):
            return {
                "data": request.body,
                "type": "raw"
            }

        # 没有传 content-type, 并且 body 不为字符串, 默认设置为 application/json
        if not content_type:
            content_type = 'application/json'

        # 根据Content-Type判断并处理请求体
        if 'application/json' in content_type:
            try:
                # 如果Content-Type为application/json，则尝试解析JSON请求体
                body = json.dumps(request.body)
                return {
                    "data": body,
                    "type": content_type
                }

            except ValueError:
                raise ValueError(f"Invalid JSON format, raw: {request.body}")

        elif 'application/x-www-form-urlencoded' in content_type:
            # 如果Content-Type为application/x-www-form-urlencoded，则可以处理表单数据
            body = urllib.parse.urlencode(request.body)
            return {
                "data": body,
                "type": content_type
            }

        elif "multipart/form-data" in content_type:
            body = urllib.parse.urlencode(request.body).encode('utf-8')
            return {
                "data": body,
                "type": content_type
            }

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

    def _when_make_session(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            """
            同步下载器的装饰器方法

            :param args:
            :param kwargs:
            :return:
            """
            self.clear_session()
            session = func(*args, **kwargs)
            setattr(self.local, f"{self.__class__}$session", session)
            return session

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            if inspect.iscoroutinefunction(self.clear_session):
                await self.clear_session()  # noqa
            else:
                self.clear_session()

            session = await func(*args, **kwargs)
            setattr(self.local, f"{self.__class__}$session", session)
            return session

        return async_wrapper if inspect.iscoroutinefunction(func) else wrapper

    def get_session(self, **options):
        """
        获取当前会话

        :return:
        """
        return getattr(self.local, f"{self.__class__}$session", None) or self.make_session(**options)

    def clear_session(self):
        if hasattr(self.local, f"{self.__class__}$session"):
            try:
                old_session = getattr(self.local, f"{self.__class__}$session")
                old_session.close()
            except Exception as e:
                logger.error(f'[清空 session 失败] 失败原因: {str(e) or str(e.__class__.__name__)}', error=e)
