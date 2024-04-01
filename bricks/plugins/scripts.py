# -*- coding: utf-8 -*-
# @Time    : 2023-12-10 11:52
# @Author  : Kem
# @Desc    :
from typing import List, Tuple, Any

from loguru import logger

from bricks.core import signals
from bricks.core.context import Context
from bricks.utils import codes


def is_success(match: List[str], pre: List[str] = None, post: List[str] = None, flow: dict = None):
    """
    判断是否成功

    :param match: 条件, 最后结果会赋值给 ISPASS
    :param pre: 前置脚本
    :param post: 后置脚本
    :param flow: 流程流转, 默认为 not ISPASS -> raise signals.Retry
    :return:
    """
    flow = flow or {}
    flow.setdefault("not ISPASS", "raise signals.Retry")
    context: Context = Context.get_context()
    request = context.obtain("request")
    response = context.obtain("response")
    obj = codes.Generator(
        flows=[
            (codes.Type.code, pre),
            (codes.Type.define, ("ISPASS", match)),
            (codes.Type.code, post),
            (codes.Type.choice, flow),
        ]
    )
    obj.run({
        **globals(),
        "context": context,
        "signals": signals,
        "request": request,
        "response": response,
        "logger": logger
    })


def turn_page(
        match: List[str],
        pre: List[str] = None,
        post: List[str] = None,
        flow: dict = None,
        key: str = "page",
        action: str = "+1",
        call_later: bool = False,
        success: bool = False
):
    """
    翻页

    :param match: 条件, 最后结果会赋值给 ISPASS
    :param pre: 前置脚本
    :param post: 后置脚本
    :param flow: 流程流转: 默认为 ISPASS 为 真的时候, 会进行翻页 + 输出日志 + success and 删除种子
    :param key: 种子里面的翻页 key, 默认是 page
    :param action: 翻页操作, 默认是 +1
    :param call_later: 是否将种子提交到队列, 提交的话就是随机机器 随机线程获取种子
    :param success: 是否成功, 翻页之后会删除种子
    :return:
    """
    flow = flow or {}
    context: Context = Context.get_context()
    request = context.obtain("request")
    response = context.obtain("response")
    items = context.obtain("items")
    flow.setdefault("ISPASS", [
        f'context.submit(NEXT_SEEDS, call_later={call_later})',
        f'logger.debug(f"[开始翻页] 当前页面: {{context.seeds[{key!r}]}}, 种子: {{context.seeds}}")',
    ])

    flow.setdefault("not ISPASS", [
        f'logger.debug(f"[停止翻页] 当前页面: {{context.seeds[{key!r}]}}, 种子: {{context.seeds}}")',
    ])

    obj = codes.Generator(
        flows=[
            (codes.Type.code, pre),
            (codes.Type.define, ("ISPASS", match)),
            (codes.Type.define, ("NEXT_SEEDS", f'{{**context.seeds, "{key}": context.seeds["{key}"] {action}}}')),
            (codes.Type.code, post),
            (codes.Type.choice, flow),
            (codes.Type.code, f'{success} and context.success()'),

        ]
    )
    obj.run({
        **globals(),
        "context": context,
        "signals": signals,
        "request": request,
        "response": response,
        "items": items,
        "logger": logger
    })


def inject(flows: List[Tuple[codes.Type, Any]]):
    """
    注入 flows

    :param flows: 里面写片段式代码, 注意缩进
    :return:
    """

    obj = codes.Generator(
        flows=flows
    )
    obj.run({
        **globals(),
        "Context": Context,
        "context": Context.get_context(),
        "signals": signals,
        "logger": logger
    })
