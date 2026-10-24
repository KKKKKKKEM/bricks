"""为 Air Spider 装配初始化图和消费图。"""

from __future__ import annotations

from itertools import pairwise
from typing import Any

from interlace import Graph
from interlace.engine import ExecutionPlan

from bricks import hooks as hook_api

from ._nodes import CrawlNode, InitNode


class GraphAssembly:
    """从 Spider 的领域方法装配两张执行图。

    Attributes:
        spider: 提供业务回调、配置和 Context 工厂的 Air 实例。
    """

    def __init__(self, spider: Any) -> None:
        """保存本次图构造所需的 Spider。

        Args:
            spider: Air Spider 实例。
        """
        self.spider = spider

    def build_graphs(self) -> tuple[Graph, Graph]:
        """构造初始化图和选定分支的消费图。

        Returns:
            初始化图和消费图。
        """
        plan = self.build_crawl_plan(survey=self.spider._survey_results is not None)
        return self.build_init_graph(), self._compose(plan)

    def build_call_graph(self) -> Graph:
        """构造同步调用图：跳过默认存储，完成节点输出种子、请求、响应摘要与 Items。

        Returns:
            使用调用 Context 工厂的独立消费图。
        """
        return self._compose(
            self.build_crawl_plan(call=True), self.spider._call_context
        )

    @staticmethod
    def _compose(plan: ExecutionPlan, context_factory: Any = None) -> Graph:
        """按计划选中的节点与边组装普通 Graph。

        Args:
            plan: 已校验出口的消费计划。
            context_factory: 替代原图的 Context 工厂，None 沿用原图。

        Returns:
            新的未冻结 Graph。
        """
        return Graph.compose(
            nodes={
                name: node
                for name, node in plan.graph.nodes.items()
                if name in plan.nodes
            },
            entrypoint=plan.entrypoint,
            edges=plan.edges,
            context_factory=context_factory or plan.graph.context_factory,
        )

    def build_init_graph(self) -> Graph:
        """装配种子生产、入队与恢复记录流程。

        Returns:
            独立的初始化 Graph，手动消费不会启动此图。
        """
        spider = self.spider
        hook = spider._hook_node
        initial_nodes = {
            "before_make_seeds": hook(
                hook_api.BEFORE_MAKE_SEEDS, initial=True, skip=True
            ),
            "make_seeds": InitNode(spider, "make_seeds", spider._make_seeds),
            "after_make_seeds": hook(hook_api.AFTER_MAKE_SEEDS, initial=True),
            "next_seed": InitNode(spider, "next_seed"),
            "before_put_seeds": hook(
                hook_api.BEFORE_PUT_SEEDS, initial=True, skip=True
            ),
            "submit": InitNode(spider, "submit", spider._put_initial),
            "after_put_seeds": hook(hook_api.AFTER_PUT_SEEDS, initial=True),
            "checkpoint": InitNode(spider, "checkpoint"),
            "complete": InitNode(spider, "complete"),
        }
        initial = Graph(
            entrypoint="before_make_seeds", context_factory=spider._init_context
        ).add(**initial_nodes)
        for source, target in (
            ("before_make_seeds", "make_seeds"),
            ("make_seeds", "after_make_seeds"),
            ("after_make_seeds", "next_seed"),
            ("before_put_seeds", "submit"),
            ("submit", "after_put_seeds"),
            ("after_put_seeds", "checkpoint"),
            ("checkpoint", "next_seed"),
        ):
            initial.connect(
                source, target, source_port="initial", target_port="initial"
            )
        initial.connect(
            "next_seed", "before_put_seeds", source_port="seed", target_port="initial"
        )
        for name in initial_nodes:
            if name != "complete":
                initial.connect(
                    name, "complete", source_port="complete", target_port="initial"
                )
        for source, target in (
            ("before_make_seeds", "after_make_seeds"),
            ("before_put_seeds", "after_put_seeds"),
        ):
            initial.connect(source, target, source_port="skip", target_port="initial")
        return initial

    def build_crawl_plan(
        self, *, survey: bool = False, call: bool = False
    ) -> ExecutionPlan:
        """从共享消费定义选择正式存储、手动消费或同步调用分支。

        Args:
            survey: 是否选择无默认存储并收集 Context 的手动分支。
            call: 是否选择无默认存储、完成节点输出 JSON 调用结果的分支。

        Returns:
            校验完整出口的消费计划。

        Raises:
            TypeError: survey 或 call 不是 bool。
            ValueError: 同时选择 survey 与 call。
        """
        if type(survey) is not bool or type(call) is not bool:
            raise TypeError("survey and call must be boolean")
        if survey and call:
            raise ValueError("survey and call are exclusive")
        spider = self.spider
        hook = spider._hook_node
        crawl_nodes = {
            "before_make_request": hook(
                hook_api.BEFORE_MAKE_REQUEST, entry=True, skip=True
            ),
            "make_request": CrawlNode(spider, "make_request", spider.make_request),
            "after_make_request": hook(hook_api.AFTER_MAKE_REQUEST, retry=True),
            "before_request": hook(hook_api.BEFORE_REQUEST, retry=True, skip=True),
            "download": CrawlNode(
                spider, "download", download=spider.build_download_node()
            ),
            "after_request": hook(hook_api.AFTER_REQUEST, retry=True),
            "before_parse": hook(hook_api.BEFORE_PARSE, skip=True),
            "parse": CrawlNode(spider, "parse", spider.parse),
            "after_parse": hook(hook_api.AFTER_PARSE),
            "before_pipeline": hook(hook_api.BEFORE_PIPELINE, skip=True),
            "pipeline": CrawlNode(spider, "pipeline", spider.item_pipeline),
            "survey_pipeline": CrawlNode(spider, "pipeline", spider._survey_pipeline),
            "after_pipeline": hook(hook_api.AFTER_PIPELINE),
            "retry": CrawlNode(spider, "retry"),
            "before_retry": hook(hook_api.BEFORE_RETRY),
            "retry_prepare": CrawlNode(spider, "retry_prepare", spider.on_retry),
            "after_retry": hook(hook_api.AFTER_RETRY),
            "retry_wait": CrawlNode(spider, "retry_wait"),
            "complete": CrawlNode(
                spider,
                "complete",
                spider._collect_survey if survey else None,
                summarize=spider._call_result if call else None,
            ),
        }
        crawl = Graph(
            entrypoint="before_make_request", context_factory=spider._crawl_context
        ).add(**crawl_nodes)
        normal = (
            "before_make_request",
            "make_request",
            "after_make_request",
            "before_request",
            "download",
            "after_request",
            "before_parse",
            "parse",
            "after_parse",
            "before_pipeline",
            "pipeline",
            "after_pipeline",
            "complete",
        )
        for source, target in pairwise(normal):
            crawl.connect(source, target, source_port="task", target_port="task")
        crawl.connect(
            "before_pipeline", "survey_pipeline", source_port="task", target_port="task"
        )
        crawl.connect(
            "survey_pipeline", "after_pipeline", source_port="task", target_port="task"
        )
        retry_chain = (
            "retry",
            "before_retry",
            "retry_prepare",
            "after_retry",
            "retry_wait",
            "before_request",
        )
        for source, target in pairwise(retry_chain):
            crawl.connect(source, target, source_port="task", target_port="task")
        for name, node in crawl_nodes.items():
            if name != "complete":
                crawl.connect(
                    name, "complete", source_port="complete", target_port="task"
                )
            if "retry" in node.outputs:
                crawl.connect(name, "retry", source_port="retry", target_port="task")
        for source, target in (
            ("before_make_request", "after_make_request"),
            ("before_request", "after_request"),
            ("before_parse", "after_parse"),
            ("before_pipeline", "after_pipeline"),
        ):
            crawl.connect(source, target, source_port="skip", target_port="task")
        excluded = "pipeline" if survey or call else "survey_pipeline"
        return crawl.plan(
            include=set(crawl_nodes) - {excluded},
            outputs={("complete", "summary")},
        )
