# -*- coding: utf-8 -*-
# @Time    : 2023-11-13 17:45
# @Author  : Kem
# @Desc    :
import sys

from loguru import logger

from bricks.core.dispatch import Dispatcher, Task  # noqa
from bricks.lib.request import Request
from bricks.lib.response import Response

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level}</level>  | <level>{message}</level>",
    colorize=True,
)
