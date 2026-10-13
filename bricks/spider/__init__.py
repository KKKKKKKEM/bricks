"""代码式爬虫基类及其领域上下文；不转导出编排引擎类型。"""

from .air import RunReport, Spider
from .context import Affinity, Context, HookContext, InitContext, ResponseRejectedError

__all__ = [
    "Affinity",
    "Context",
    "InitContext",
    "HookContext",
    "ResponseRejectedError",
    "RunReport",
    "Spider",
]
