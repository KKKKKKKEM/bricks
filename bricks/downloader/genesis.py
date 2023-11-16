# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 14:17
# @Author  : Kem
# @Desc    :
import functools
import inspect
import json
import time
import urllib.parse
from typing import Union

from loguru import logger

from bricks.core import genesis
from bricks.lib.request import Request
from bricks.lib.response import Response


class Downloader(metaclass=genesis.MetaClass):

    def fetch(self, request: (Request, dict)) -> Response:
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
            if isinstance(request, dict): request = Request(**request)

            t = time.time()

            try:
                response: Response = func(request, *args, **kwargs)
                response.rt = time.time() - t

            except KeyboardInterrupt as e:
                raise e

            except Exception as e:
                logger.error(f'[请求失败] 失败原因: {str(e) or str(e.__class__.__name__)}', error=e)
                response: Response = Response.make_response(
                    error=e.__class__.__name__,
                    reason=str(e),
                    url=request.real_url,
                    request=request,
                    status_code=-1
                )

            return response

        @functools.wraps(func)
        async def aswrapper(request: Union[Request, dict], *args, **kwargs):
            """
            异步下载器的装饰器方法

            :param request:
            :param args:
            :param kwargs:
            :return:
            """
            if isinstance(request, dict): request = Request(**request)

            try:
                t = time.time()
                response: Response = await func(request, *args, **kwargs)
                response.rt = time.time() - t

            except Exception as e:
                logger.error(f'[请求失败] 失败原因: {str(e) or str(e.__class__.__name__)}', error=e)
                response: Response = Response.make_response(
                    error=e.__class__.__name__,
                    reason=str(e),
                    url=request.real_url,
                    request=request,
                    status_code=-1
                )

            return response

        return aswrapper if inspect.iscoroutinefunction(func) else wrapper

    @classmethod
    def parse_data(cls, request: Request):
        # 获取请求头中的Content-Type
        content_type = request.headers.get('Content-Type', '').lower()

        if not content_type or not isinstance(request.body, str):
            return {
                "data": request.body,
                "type": "unknown"
            }

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
            raise ValueError(f"Invalid JSON format, raw: {request.body}")
