# -*- coding: utf-8 -*-
# @Time    : 2023-12-10 11:52
# @Author  : Kem
# @Desc    :
from typing import List

from loguru import logger

from bricks.core import signals
from bricks.lib.context import Context
from bricks.utils.pandora import CodeGenertor


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
    code = CodeGenertor(
        flows=[
            ("combination", pre),
            ("define", ("ISPASS", match)),
            ("combination", post),
            ("condition", flow),
        ]
    )
    code.run({**globals(), "context": context, "signals": signals})


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
    :param key:
    :param action:
    :param call_later:
    :param success:
    :return:
    """
    flow = flow or {}
    context: Context = Context.get_context()
    flow.setdefault("ISPASS", [
        f'context.submit(NEXT_SEEDS, call_later={call_later})',
        f'logger.debug(f"[开始翻页] 当前页面: {{context.seeds[{key!r}]}}, 种子: {{context.seeds}}")',
        f'{success} and context.success()'
    ])

    flow.setdefault("not ISPASS", [
        f'logger.debug(f"[停止翻页] 当前页面: {{context.seeds[{key!r}]}}, 种子: {{context.seeds}}")',
    ])

    code = CodeGenertor(
        flows=[
            ("combination", pre),
            ("define", ("ISPASS", match)),
            ("define", ("NEXT_SEEDS", f'{{**context.seeds, "page": context.seeds["{key}"] {action}}}')),
            ("combination", post),
            ("condition", flow),
        ]
    )
    code.run({**globals(), "context": context, "signals": signals, "logger": logger})
