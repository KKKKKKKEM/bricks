# -*- coding: utf-8 -*-
# @Time    : 2023-11-17 17:39
# @Author  : Kem
# @Desc    :


import threading
from typing import Callable

from loguru import logger

from bricks.core import signals
from bricks.lib import proxies
from bricks.spider.air import Context
from bricks.utils import pandora


def set_proxy(context: Context, timeout=None):
    """
    为请求设置代理

    :param context:
    :param timeout:
    :return:
    """
    request = context.request
    proxy = context.request.proxy or context.target.proxy
    proxy_ = proxies.manager.get_proxy(*pandora.iterable(proxy), timeout=timeout)
    request.proxies = proxy_.proxy
    callable(proxy_.auth) and proxy_.auth(request)
    proxies.manager.use_proxy(proxy_)


def show_response(context: Context, handle: Callable = logger.debug, fmt: int = 0):
    """
    展示相应信息

    :param context:
    :param fmt:
    :param handle:
    :return:
    """

    request = context.request
    response = context.response

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


def is_success(context: Context):
    """
    通用的判断响应是否成功

    :param context:
    :type context:
    :return:
    """

    if not context.response.ok:
        raise signals.Retry
