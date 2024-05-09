# -*- coding: utf-8 -*-
# @Time    : 2023-12-12 13:12
# @Author  : Kem
# @Desc    :
import json
import os.path
from typing import Union, Callable, Literal

from loguru import logger

from bricks.lib.items import Items
from bricks.lib.request import Request
from bricks.lib.response import Response
from bricks.spider.air import Spider
from bricks.utils import pandora


def curl2resp(curl_cmd: str, options: dict = None) -> Response:
    """
    curl 转响应

    :param curl_cmd:
    :param options:
    :return:
    """
    request = Request.from_curl(curl_cmd)
    return req2resp(request, options)


def req2resp(request: Request, options: dict = None) -> Response:
    """
    请求转响应

    :param request:
    :param options:
    :return:
    """
    options = options or {}
    spider = Spider(**options)
    return spider.fetch(request, **options)


def resp2items(
        response: Response,
        engine: Union[str, Callable],
        rules: dict,
        rename: dict = None,
        default: dict = None,
        factory: dict = None,
        show: dict = None,
) -> Items:
    """
    响应转换为 items

    :param response:
    :param engine:
    :param rules:
    :param rename:
    :param default:
    :param factory:
    :param show:
    :return:
    """
    items = response.extract(engine=engine, rules=rules)
    pandora.clean_rows(*items, rename=rename, default=default, factory=factory, show=show)
    return Items(items)


def source2items(
        obj: Union[str, bytes, dict, list],
        engine: Union[str, Callable],
        rules: dict,
        rename: dict = None,
        default: dict = None,
        factory: dict = None,
        show: dict = None,
):
    def ensure_bytes(_obj):
        if isinstance(_obj, str):
            return _obj.encode("utf-8")
        return _obj

    if isinstance(obj, (dict, list)):
        obj = json.dumps(obj)
        encoding = "utf-8"
    else:
        encoding = None
    res = Response(content=ensure_bytes(obj), encoding=encoding)

    return resp2items(
        response=res,
        engine=engine,
        rules=rules,
        rename=rename,
        default=default,
        factory=factory,
        show=show
    )


def curl2spider(curl: str, path: str, name: str = "MySpider", form: Literal['form', 'template'] = "form"):
    """
    通过 curl 生成爬虫模板文件

    :param curl:
    :param path: 生成的文件路径
    :param name: 爬虫名称
    :param form: 模板类型
    :return:
    """
    request = Request.from_curl(curl)
    tpath = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tpls", "spider", form + ".tpl")
    with open(tpath) as f:
        tpl = f.read()

    tpl = tpl.format(**{
        "SPIDER": name,
        "URL": request.url,
        "METHOD": request.method,
        "PARAMS": request.params,
        "BODY": request.body,
        "HEADERS": request.headers,
        "COOKIES": dict(request.cookies) if request.cookies else None,
        "OPTIONS": request.options,
        "ALLOW_REDIRECTS": request.allow_redirects,
        "PROXIES": request.proxies,
        "PROXY": request.proxy,
        "MAX_RETRY": request.max_retry,
        "USE_SESSION": request.use_session,

    })
    target_dir = os.path.dirname(path)
    target_dir and not os.path.exists(target_dir) and os.makedirs(target_dir, exist_ok=True)
    with open(path, "w") as f:
        f.write(tpl)

    logger.debug(f'生成成功, 路径为: {path}')
