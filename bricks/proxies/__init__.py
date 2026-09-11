"""独立代理供应与按 Slot 管理工具；不创建全局管理器或导入可选客户端。"""

from .base import BaseProxy, Manager, Proxy, ProxyProvider, format_proxy
from .clash import ClashProxy
from .sources import ApiProxy, CustomProxy, RedisProxy
from .subscriptions import cfg_tpl, extract_nodes, fetch_subscription, subscribe

__all__ = [
    "ApiProxy",
    "BaseProxy",
    "ClashProxy",
    "CustomProxy",
    "Manager",
    "Proxy",
    "ProxyProvider",
    "RedisProxy",
    "cfg_tpl",
    "extract_nodes",
    "fetch_subscription",
    "format_proxy",
    "subscribe",
]
