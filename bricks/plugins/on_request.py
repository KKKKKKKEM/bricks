# -*- coding: utf-8 -*-
# @Time    : 2023-12-06 14:05
# @Author  : Kem
# @Desc    : 针对于 on request 的插件
import inspect
import threading
import time
from typing import Callable, Union

from loguru import logger

from bricks.core import signals
from bricks.core.context import Context
from bricks.lib import proxies
from bricks.lib.proxies import BaseProxy
from bricks.utils import pandora, codes
from bricks.utils.fake import user_agent


class Before:
    """
    请求前

    """

    @classmethod
    def set_proxy(cls, timeout=None):
        """
        为请求设置代理

        :param timeout:
        :return:
        """
        context: Context = Context.get_context()
        request = context.obtain("request")
        if not request.proxies:
            proxy: Union[dict, BaseProxy, str] = request.proxy or context.target.proxy
            if isinstance(proxy, str):
                proxy = {
                    "ref": "bricks.lib.proxies.CustomProxy",
                    "key": proxy
                }

            proxy_ = proxies.manager.get(*pandora.iterable(proxy), timeout=timeout)
            request.proxies = proxy_.proxy
            callable(proxy_.auth) and proxy_.auth(request)
            proxies.manager.use(proxy_)

    @classmethod
    def fake_ua(cls):
        """
        为请求设置随机 ua
        语法:
        "User-Agent": "@chrome" -- 设置 chrome
        "User-Agent": "@random" -- 设置 随机
        "User-Agent": "@edge" -- 设置 edge
        "User-Agent": "@mobile" -- 设置 手机
        "User-Agent": "@pc" -- 设置 PC
        ..... 还有很多语法!, 详情可以看当前源码和 bricks/utils/fake/user_agent.py 实现

        :return:
        """
        context: Context = Context.get_context()
        request = context.obtain("request")
        raw: str = request.headers.get("User-Agent")
        raw = raw or ""
        if raw.startswith("@"):

            if raw.startswith('@random'):
                raw = "@get" + raw[7:]

            if not raw.endswith(")"):
                raw = raw + "()"
            raw = eval(f'user_agent.{raw[1:]}', {"user_agent": user_agent})
            request.headers["User-Agent"] = raw


class After:
    """
    请求后

    """

    @classmethod
    def show_response(cls, handle: Callable = logger.debug, fmt: int = 0):
        """
        展示相应信息

        :param fmt:
        :param handle:
        :return:
        """
        context: Context = Context.get_context()
        request = context.obtain("request")
        response = context.obtain("response")

        if fmt == 0:
            msg = " ".join([
                F"\033[34m[{request.method.upper()}]\033[0m",
                F"\033[33m[{request.retry}]\033[0m",
                F"\033[{response.ok and 32 or 31}m[{response.status_code or response.error}]\033[0m",
                F"\033[37m[{request.proxies or threading.current_thread().name}]\033[0m",
                F"\033[{response.ok and 35 or 31}m[{response.size}]\033[0m",
                F"{response.url or response.request.real_url}",
            ])
        else:
            indent = '\n            '
            header = f"\n{'=' * 50}"
            msg = [
                "",
                F"url: {response.url or request.url}",
                F"method: {request.method.upper()}",
                F"headers: {request.headers}",
                F"status: \033[{response.ok and 32 or 31}m[{response.status_code or response.error}]\033[0m",
                F"proxies: {request.proxies}",
                F"thread: {threading.current_thread().name}",
                ""
            ]
            request.method.upper() != "GET" and msg.insert(3, F"body: {request.body}")

            msg = f'{header}\n{indent.join(msg)}{header}'

        handle(msg)

        return response

    @classmethod
    def bypass(cls):
        """
        通用的判断响应是否成功

        :return:
        """
        context: Context = Context.get_context()
        response = context.obtain("response")
        request = context.obtain("request")
        ok = request.ok
        namespace = {
            "status_code": response.status_code,
            "response": response,
            "request": request,
            "context": context,
        }
        annotations = {
            type(response): response,
            type(request): request,
            type(context): context,
        }
        # None -> 所有状态码, 除了 -1 , -1 代表请求过程中发生了错误
        if ok is None:
            is_pass = response.status_code != -1

        elif isinstance(ok, str):
            is_pass = eval(ok, namespace)

        elif isinstance(ok, dict):
            for match, sig in ok.items():
                if eval(match, namespace):
                    if inspect.isclass(sig) and issubclass(sig, signals.Signal):
                        if sig == signals.Pass:
                            return
                        else:
                            raise sig

                    if callable(sig):
                        pandora.invoke(sig, args=[context], annotations=annotations, namespace=namespace)
                        return
            else:
                is_pass = response.ok

        # 默认 -> response.ok
        else:
            is_pass = response.ok

        if not is_pass:
            raise signals.Retry

    @classmethod
    def conditional_scripts(cls):
        """
        插入条件脚本

        :return:
        """
        context: Context = Context.get_context()
        request = context.obtain("request")
        response = context.obtain("response")
        scripts: dict = request.get_options("$scripts") or {}
        if not scripts:
            return

        obj = codes.Generator(
            flows=[
                (codes.Type.condition, scripts),
            ]
        )
        obj.run({
            **globals(),
            "context": context,
            "time": time,
            "signals": signals,
            "request": request,
            "response": response,
            "logger": logger
        })
