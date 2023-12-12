# -*- coding: utf-8 -*-
# @Time    : 2023-12-12 13:12
# @Author  : Kem
# @Desc    :
import contextlib
import inspect
import json
from typing import Union, Callable

from bricks.lib.items import Items
from bricks.lib.request import Request
from bricks.lib.response import Response
from bricks.spider.air import Spider
from bricks.utils import pandora


def req2resp(request: Request, spider: Spider = None) -> Response:
    """
    请求转响应

    :param request:
    :param spider:
    :return:
    """
    dispatcher = contextlib.nullcontext()
    if not spider:
        spider = Spider()

    if inspect.iscoroutinefunction(spider.downloader.fetch):
        dispatcher = spider.dispatcher

    with dispatcher:
        context = spider.make_context(
            request=request,
            next=spider.on_request,
            flows={
                spider.on_request: None,
                spider.on_retry: spider.on_request
            },
        )
        context.failure = lambda shutdown: context.flow({"next": None})
        spider.on_consume(context=context)
        return context.response


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
    resp = Response(content=ensure_bytes(obj), encoding=encoding)

    return resp2items(
        response=resp,
        engine=engine,
        rules=rules,
        rename=rename,
        default=default,
        factory=factory,
        show=show
    )


if __name__ == '__main__':
    print(source2items(
        [{"name.1": f"kem{i}", "age": i, "xx": "xx"} for i in range(10)],
        engine="json",
        rules={
            "[]": {
                "name": '`name.1`',
                "age": "age"
            }
        },
        factory={
            "age": lambda x: x + 1
        }
    ))
