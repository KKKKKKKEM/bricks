# -*- coding: utf-8 -*-
# @Time    : 2023-11-13 17:45
# @Author  : Kem
# @Desc    :
import warnings
warnings.filterwarnings("ignore")
import sys

from loguru import logger

from bricks.lib.items import Items  # noqa F401
from bricks.lib.request import Request  # noqa F401
from bricks.lib.response import Response  # noqa F401
from bricks.state import *  # noqa F403


logger.remove(0)
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <7}</level> | <cyan>{file}:{line}</cyan> - <level>{message}</level>",
    colorize=True,
    backtrace=True,  # 异常时打印回溯信息
    diagnose=True,  # 更详细的诊断信息
)
