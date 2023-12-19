# -*- coding: utf-8 -*-
# @Time    : 2023-12-05 17:09
# @Author  : Kem
# @Desc    :
import hashlib
import os.path
import uuid

from bricks.lib import variable

__all__ = (
    "const",
    "G",
    "T",
    "MACHINE_ID",
    "VERSION",
)

# 当前 机器 ID
MACHINE_ID = hashlib.sha256(uuid.UUID(int=uuid.getnode()).hex[-12:].encode()).hexdigest()

with open(os.path.join(os.path.dirname(__file__), "VERSION"), "r") as f:
    # 当前框架版本
    VERSION = f.read().strip()

# 全局变量
G = variable.VariableG()
# 线程变量
T = variable.VariableT()


class const:  # noqa
    # 事件类型
    ERROR_OCCURRED = 'ERROR_OCCURRED'

    BEFORE_START = 'BEFORE_START'
    BEFORE_CLOSE = 'BEFORE_CLOSE'

    ON_CONSUME = 'ON_CONSUME'

    BEFORE_GET_SEEDS = "BEFORE_GET_SEEDS"
    AFTER_GET_SEEDS = "AFTER_GET_SEEDS"

    BEFORE_PUT_SEEDS = "BEFORE_PUT_SEEDS"
    AFTER_PUT_SEEDS = "AFTER_PUT_SEEDS"

    BEFORE_RETRY = "BEFORE_RETRY"
    AFTER_RETRY = "AFTER_RETRY"

    BEFORE_REQUEST = "BEFORE_REQUEST"
    ON_REQUEST = "ON_REQUEST"
    AFTER_REQUEST = "AFTER_REQUEST"

    ON_PARSE = 'ON_PARSE'
    ON_INIT = 'ON_INIT'

    BEFORE_PIPELINE = "BEFORE_PIPELINE"
    ON_PIPELINE = "ON_PIPELINE"
    AFTER_PIPELINE = "AFTER_PIPELINE"
