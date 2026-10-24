"""以 Interlace 插件采集 Air 消费图与初始化图的爬虫领域指标。

插件只订阅 NODE_INSPECT 并经 METRIC 通知样本，不改变节点、请求或交付结果；聚合与导出由
`interlace.metrics.Metrics` 等 METRIC 处理函数负责，未安装聚合插件时样本无人接收。
"""

from __future__ import annotations

from interlace.metrics import count, observe
from interlace.plugins import ExtensionAPI, HookBus, plugin
from interlace.plugins.points import NODE_INSPECT, HookPhase, NodeInspection

from bricks.spider.context import Context, InitContext, ResponseRejectedError

__all__ = ["crawler_metrics"]


def _status(code: int) -> str:
    """把响应状态码归为有界标签：2xx 等状态段，传输失败为 error。"""
    if code == -1:
        return "error"
    return f"{code // 100}xx" if 100 <= code < 600 else "other"


def _inspect(hooks: HookBus, inspection: NodeInspection) -> None:
    """从消费图与初始化图的节点边界提取领域指标。

    Args:
        hooks: 当前 Runtime 的钩子总线，用于通知 METRIC。
        inspection: 一次节点调用边界的采集记录。
    """
    call = inspection.call
    context = call.context
    if isinstance(context, InitContext):
        if call.node_id == "submit" and inspection.phase is HookPhase.EXIT:
            seeds = context.seeds
            size = len(seeds) if isinstance(seeds, list) else 1
            count(hooks, "crawler.seeds.submitted", size, source="init")
        return
    if not isinstance(context, Context):
        return
    if inspection.phase is HookPhase.ERROR:
        if isinstance(inspection.value, ResponseRejectedError):
            count(hooks, "crawler.seeds.rejected", stage=call.node_id)
        return
    if inspection.phase is not HookPhase.EXIT:
        return
    if call.node_id == "download":
        response = context.response
        count(hooks, "crawler.requests", status=_status(response.status_code))
        observe(hooks, "crawler.download.seconds", response.cost)
    elif call.node_id == "retry":
        count(hooks, "crawler.retries")
    elif call.node_id == "parse":
        count(hooks, "crawler.items", len(context.items))
    elif call.node_id == "complete":
        count(hooks, "crawler.seeds.completed")


@plugin("bricks.crawler/metrics")
def crawler_metrics(api: ExtensionAPI) -> None:
    """登记爬虫领域指标采集；与 `interlace.metrics.Metrics().plugin` 一同装入 Spider。

    样本包括 crawler.requests{status}、crawler.download.seconds、crawler.retries、crawler.items、
    crawler.seeds.completed、crawler.seeds.rejected{stage} 与 crawler.seeds.submitted{source}；
    后者的 current/any 来源由 Context.submit 记录。

    Args:
        api: 插件扩展接口。
    """
    hooks = api.hooks
    api.on(NODE_INSPECT, lambda inspection: _inspect(hooks, inspection))
