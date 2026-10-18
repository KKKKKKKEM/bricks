"""Air 的两图流程、扩展点、失败边界及分布式消费契约。"""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from interlace import ExecutionLimits, Graph, Node, Ports, Slot, SlotPool
from interlace.engine.errors import (
    ExecutionCancelledError,
    ExecutionError,
    ExecutionTimeoutError,
)
from interlace.engine.hooks import NodeHook

from bricks import DownloadNode, Items, Request, Response, hooks, signals
from bricks.downloaders.curl_cffi import AsyncCurlCffiDownloader, CurlCffiDownloader
from bricks.plugins import on_request, scripts
from bricks.proxies import CustomProxy, Manager
from bricks.queues import MemoryTaskQueue
from bricks.spider import Context, Spider


class FakeDownloader:
    """使用 URL 控制响应的同步下载器。

    Attributes:
        calls: 在锁内记录的请求快照。
        lock: 保护当前测试记录和 URL 次数。
        attempts: 各 URL 的下载次数。
    """

    def __init__(self):
        """初始化当前测试拥有的记录。"""
        self.calls = []
        self.lock = threading.RLock()
        self.attempts = {}

    def fetch(self, request):
        """返回固定 JSON，并验证同步下载没有占用异步事件循环。

        Args:
            request: 节点复制后的请求。

        Returns:
            成功、可重试或拒绝响应。

        Raises:
            ValueError: URL 指定程序错误。
        """
        with pytest.raises(RuntimeError):
            asyncio.get_running_loop()
        with self.lock:
            self.calls.append(request)
            attempt = self.attempts[request.url] = self.attempts.get(request.url, 0) + 1
        if request.url.endswith("/boom"):
            raise ValueError("downloader configuration broken")
        status = 200
        if (
            request.url.endswith("/fail")
            or request.url.endswith("/retry")
            and attempt < 3
        ):
            status = 500
        elif request.url.endswith("/forbidden"):
            status = 403
        return Response(
            status_code=status,
            content=json.dumps({"url": request.url}).encode(),
            url=request.url,
            request=request,
        )


class SimpleSpider(Spider):
    """提供可恢复种子源与显式收集器的测试爬虫。

    Attributes:
        seeds: 当前测试的初始种子列表。
        stored: 此测试拥有的存储结果集合。
        contexts: 保存调用上下文以检查不同 firing 的独立性。
        source_closed: 同步种子源 finally 是否执行。
    """

    def __init__(self, seeds=(), **kwargs):
        """装配种子源与默认无网络下载器。

        Args:
            seeds: 初始种子。
            kwargs: Spider 配置。
        """
        kwargs.setdefault("downloader", FakeDownloader())
        super().__init__(**kwargs)
        self.seeds = list(seeds)
        self.stored = []
        self.contexts = []
        self.source_closed = False

    def make_seeds(self, context, **kwargs):
        """从保存的索引继续生产，断点由提交节点在入队后写入。

        Args:
            context: 初始化上下文。
            kwargs: 包含恢复记录的关键字参数。

        Yields:
            每次一条 JSON 种子。
        """
        start = context.record.get("next", 0)
        try:
            for index in range(start, len(self.seeds)):
                context.record = {"next": index + 1}
                yield self.seeds[index]
        finally:
            self.source_closed = True

    def make_request(self, context):
        """根据当前种子显式创建请求。

        Args:
            context: 消费上下文。

        Returns:
            当前请求。
        """
        self.contexts.append(context)
        return Request(context.seeds["url"])

    def parse(self, context):
        """记录当前响应和执行配置。

        Args:
            context: 消费上下文。

        Returns:
            显式 Items。
        """
        self.contexts.append(context)
        return Items(
            [
                {
                    "url": context.response.url,
                    "attempt": context.attempt,
                    "options": dict(context.options),
                }
            ]
        )

    def item_pipeline(self, context):
        """将当前记录追加到测试拥有的收集器。

        Args:
            context: 含 Items 的上下文。
        """
        self.contexts.append(context)
        self.stored.extend(context.items)


def test_two_graphs_stream_seeds_before_source_finishes():
    """首条种子提交并消费后才允许继续生成，证明初始化不会整批物化。"""
    stored = threading.Event()

    class Streaming(SimpleSpider):
        """依赖第一条消费结果继续生产的种子源。"""

        def make_seeds(self, context, **kwargs):
            """提交第一条后等待消费反馈。

            Args:
                context: 初始化上下文。
                kwargs: 包含恢复记录的关键字参数。

            Yields:
                两条独立种子。
            """
            try:
                yield {"url": "https://example.com/first"}
                assert stored.wait(3)
                yield {"url": "https://example.com/second"}
            finally:
                self.source_closed = True

        def item_pipeline(self, context):
            """存储后允许初始化继续。

            Args:
                context: 消费上下文。
            """
            super().item_pipeline(context)
            stored.set()

    spider = Streaming(queue_name="stream@v1")
    with spider:
        assert set(spider.runtime.graphs()) == {"stream@v1.init", "stream@v1.crawl"}
        report = spider.run(kwargs={"timeout": 5})
    assert report.initialization.status == "succeeded"
    assert report.counts == {"pending": 0, "leased": 0, "acked": 2, "dead": 0}
    assert len(spider.stored) == 2 and spider.source_closed
    assert len({id(context) for context in spider.contexts}) == 6
    spider.close()
    with pytest.raises(RuntimeError):
        spider.open()


def test_local_child_claim_hook_waits_for_parent_slot_graph():
    """同槽本地子任务领取 Hook 不得与父 Graph 并发访问 Slot。"""
    child_hook_entered = threading.Event()
    hook_busy_values = []

    class ParentChildSpider(SimpleSpider):
        """提交本地子任务并用有界探针检查领取 Hook 顺序。"""

        def parse(self, context):
            """父任务持有 Slot 时短暂观察子领取 Hook。

            Args:
                context: 当前页面解析上下文。

            Returns:
                当前页面记录。
            """
            if context.seeds["page"] == "parent":
                context.slot["busy"] = True
                context.submit({"url": "https://example.com/child", "page": "child"})
                observed_during_graph = child_hook_entered.wait(0.1)
                context.slot["busy"] = False
                assert not observed_during_graph
            return super().parse(context)

        @hooks.on(hooks.AFTER_GET_SEEDS)
        def after_get_seeds(self, context):
            """记录子任务领取 Hook 看到的 Slot 状态。

            Args:
                context: 已领取任务的 Hook 上下文。
            """
            if context.seeds["page"] == "child":
                hook_busy_values.append(context.slot.get("busy", False))
                child_hook_entered.set()

    spider = ParentChildSpider(concurrency=2)
    spider.submit({"url": "https://example.com/parent", "page": "parent"})
    try:
        report = spider.run(kwargs={"timeout": 5})
        assert report.counts["acked"] == 2
        assert child_hook_entered.is_set()
        assert hook_busy_values == [False]
    finally:
        spider.close()


def test_http_response_dynamic_seeds_and_default_option_boundary(server):
    """真实下载、重复动态种子和执行配置边界通过同一提交入口处理。

    Args:
        server: 本地 HTTP 服务。
    """
    base, seen, _ = server

    class Discovery(SimpleSpider):
        """从父页面发现重复详情种子。"""

        def parse(self, context):
            """对子种子显式提交，缺省配置使用 Spider 默认值。

            Args:
                context: 当前响应上下文。

            Returns:
                当前页面记录。
            """
            if context.seeds["url"].endswith("/parent"):
                first = context.submit({"url": base + "/child"})
                second = context.submit({"url": base + "/child"})
                assert first[0].id == second[0].id
            assert context.response.json()["path"] in ("/parent", "/child")
            return super().parse(context)

    spider = Discovery(
        options={"mode": "default"},
        downloader=CurlCffiDownloader(trust_env=False),
    )
    spider.submit({"url": base + "/parent"}, options={"mode": "parent"})
    report = spider.run(kwargs={"timeout": 5})
    assert report.counts["acked"] == 2
    assert sorted(path for _, path, _, _ in seen) == ["/child", "/parent"]
    records = {record["url"]: record for record in spider.stored}
    assert records[base + "/parent"]["options"] == {"mode": "parent"}
    assert records[base + "/child"]["options"] == {"mode": "default"}


def test_download_retries_are_bounded_before_parse_and_storage():
    """可重试响应通过回边下载，解析和存储只执行一次。"""
    downloader = FakeDownloader()
    spider = SimpleSpider(
        [{"url": "https://example.com/retry"}],
        downloader=downloader,
        retry_delay=0.01,
    )
    report = spider.run(kwargs={"timeout": 5})
    assert report.counts["acked"] == 1
    assert len(downloader.calls) == 3
    assert len({id(request) for request in downloader.calls}) == 3
    assert len(spider.stored) == 1 and spider.stored[0]["attempt"] == 3


@pytest.mark.parametrize("path,expected", [("fail", 3), ("forbidden", 1), ("boom", 1)])
def test_download_failure_rejects_and_does_not_store(path, expected):
    """耗尽、明确拒绝和程序错误都保留失败，不运行存储。

    Args:
        path: 触发失败的 URL 路径。
        expected: 期望下载次数。
    """
    downloader = FakeDownloader()
    spider = SimpleSpider(
        [{"url": f"https://example.com/{path}"}],
        downloader=downloader,
    )
    with pytest.raises(ExecutionError):
        spider.run(kwargs={"timeout": 5})
    assert len(downloader.calls) == expected
    assert spider.stored == []
    assert spider.report().counts["dead"] == 1


def test_pipeline_failure_does_not_ack_or_retry_download():
    """存储异常保留为失败，避免整条链重跑造成重复副作用。"""
    downloader = FakeDownloader()

    class BrokenStore(SimpleSpider):
        """模拟存储异常。"""

        def item_pipeline(self, context):
            """报告存储失败。

            Args:
                context: 当前记录上下文。

            Raises:
                ValueError: 原始存储错误。
            """
            raise ValueError("storage broken")

    spider = BrokenStore(
        [{"url": "https://example.com/one"}],
        downloader=downloader,
    )
    with pytest.raises(ExecutionError, match="storage broken"):
        spider.run(kwargs={"timeout": 5})
    assert spider.report().counts["acked"] == 0
    assert spider.report().counts["dead"] == 1
    assert len(downloader.calls) == 1


def test_hook_order_conditions_mutations_and_instance_isolation():
    """共享 Hook 升序执行，None 保留输出，实例与继承声明隔离。"""

    class Hooked(SimpleSpider):
        """使用混合同步异步回调的爬虫。

        Attributes:
            trace: 当前实例的回调执行记录。
        """

        def __init__(self):
            """准备独立回调记录。"""
            super().__init__([{"url": "https://example.com/one"}])
            self.trace = []

        @hooks.on("BEFORE_REQUEST", index=2)
        def second(self, context):
            """在第一项回调之后修改请求。

            Args:
                context: 当前请求上下文。
            """
            self.trace.append("before-2")
            context.request.headers["X-Hook"] = "yes"

        @hooks.on("BEFORE_REQUEST", index=1)
        async def first(self, context):
            """等待后记录第一项回调。

            Args:
                context: 当前请求上下文。
            """
            await asyncio.sleep(0)
            self.trace.append("before-1")

        @hooks.on("AFTER_REQUEST", index=2)
        def after_second(self, context):
            """验证出口回调也升序执行。

            Args:
                context: 已下载的上下文。
            """
            self.trace.append("after-2")

        @hooks.on("AFTER_REQUEST", index=1)
        def after_first(self, context):
            """记录第一个出口回调。

            Args:
                context: 已下载的上下文。
            """
            self.trace.append("after-1")

        @hooks.on("BEFORE_PARSE", match=lambda context: False)
        def skipped(self, context):
            """条件为假时不执行。

            Args:
                context: 当前响应上下文。
            """
            raise AssertionError("condition was ignored")

    first, second = Hooked(), Hooked()
    first.run(kwargs={"timeout": 5})
    assert first.trace == ["before-1", "before-2", "after-1", "after-2"]
    assert second.trace == []
    second.run(kwargs={"timeout": 5})
    assert second.trace == first.trace
    assert len(first.stored) == len(second.stored) == 1


def test_async_methods_and_async_downloader_preserve_cancellation():
    """异步业务方法和下载器原生等待，超时后先完成协程清理。"""
    cleaned = threading.Event()

    class AsyncFetch:
        """可取消的异步下载器。"""

        async def fetch(self, request):
            """等待直到被执行超时取消。

            Args:
                request: 当前请求。

            Returns:
                正常路径的响应。
            """
            try:
                await asyncio.sleep(10)
                return Response(status_code=200, content=b"ok", url=request.url)
            finally:
                cleaned.set()

    class AsyncSpider(SimpleSpider):
        """异步生成种子集合和构造请求。"""

        async def make_seeds(self, context, **kwargs):
            """异步取得同步可迭代的种子集合。

            Args:
                context: 初始化上下文。
                kwargs: 包含恢复记录的关键字参数。

            Returns:
                种子列表。
            """
            await asyncio.sleep(0)
            return [{"url": "https://example.com/async"}]

        async def make_request(self, context):
            """异步构造请求。

            Args:
                context: 消费上下文。

            Returns:
                Request。
            """
            await asyncio.sleep(0)
            return Request(context.seeds["url"])

    spider = AsyncSpider(
        downloader=AsyncFetch(),
        limits=ExecutionLimits(timeout=0.1),
    )
    with pytest.raises(ExecutionTimeoutError):
        spider.run(kwargs={"timeout": 5})
    assert cleaned.is_set()
    assert spider.report().counts["dead"] == 1


def test_separate_phases_and_custom_context_factory():
    """初始化与消费可分开运行，自定义工厂保留自身构造逻辑。"""
    constructed = []

    class CustomContext(Context):
        """保留用户额外构造信息的上下文。

        Attributes:
            label: 当前调用的自定义业务标签。
        """

        def __init__(self, target, *, base, task, label="custom"):
            """自行构造额外字段并组合基础能力。

            Args:
                base: 基础执行上下文。
                task: 图内任务。
                target: 当前 Spider。
                label: 用户标签。
            """
            super().__init__(target=target, base=base, task=task)
            self.label = label
            constructed.append(self)

    spider = SimpleSpider(
        [{"url": "https://example.com/one"}],
    )
    spider.Context = CustomContext
    with spider:
        initial = spider.run_init()
        assert initial.status == "succeeded" and initial.progress == {"next": 1}
        assert spider.report().counts["pending"] == 1
        assert spider.stored == []
        assert spider.run_init().status == "succeeded"
        assert spider.run_spider(timeout=5).counts["acked"] == 1
    assert all(context.label == "custom" for context in constructed)
    assert len(constructed) == len({id(context) for context in constructed})


def test_partial_initialization_failure_drains_and_can_resume():
    """初始化失败不撤回已提交种子，下次按断点续跑。"""
    queue = MemoryTaskQueue("resume@v1")

    class FailingSource(SimpleSpider):
        """生产一条种子后失败。"""

        def make_seeds(self, context, **kwargs):
            """保存第一条提交后的断点。

            Args:
                context: 初始化上下文。
                kwargs: 包含恢复记录的关键字参数。

            Yields:
                已经可以消费的首条种子。

            Raises:
                ValueError: 原始生产错误。
            """
            try:
                context.record = {"next": 1}
                yield {"url": "https://example.com/one"}
                raise ValueError("source broken")
            finally:
                self.source_closed = True

    first = FailingSource(queue_name="resume@v1", task_queue=queue)
    with pytest.raises(ExecutionError, match="source broken"):
        first.run(kwargs={"timeout": 5})
    assert first.source_closed and len(first.stored) == 1
    assert queue.initialization().status == "failed"
    second = SimpleSpider(
        [{"url": "https://example.com/one"}, {"url": "https://example.com/two"}],
        queue_name="resume@v1",
        task_queue=queue,
    )
    assert second.run(kwargs={"timeout": 5}).counts["acked"] == 2
    assert [record["url"] for record in second.stored] == ["https://example.com/two"]


def test_close_cancels_initialization_and_does_not_steal_borrowed_resources():
    """外部关闭取消初始化并等待其清理，借用的 SlotPool 继续可用。"""
    started = threading.Event()
    pool = SlotPool(1)

    class SlowSource(SimpleSpider):
        """可在下一次迭代检查点停止的源。"""

        def make_seeds(self, context, **kwargs):
            """反复产生可去重的种子，直到外部关闭。

            Args:
                context: 初始化上下文。
                kwargs: 包含恢复记录的关键字参数。

            Yields:
                当前种子。
            """
            try:
                while True:
                    started.set()
                    time.sleep(0.02)
                    yield {"url": "https://example.com/one"}
            finally:
                self.source_closed = True

    spider = SlowSource(slots=pool)
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            running = executor.submit(spider.run_init)
            assert started.wait(3)
            closing = executor.submit(spider.close)
            with pytest.raises(ExecutionCancelledError):
                running.result(5)
            closing.result(5)
        assert spider.source_closed
        lease = pool.try_acquire()
        assert lease is not None
        lease.release()
    finally:
        spider.close()
        pool.close()


def test_unsupported_hook_and_callback_lifecycle_reentry_fail_explicitly():
    """不支持的模板阶段在 use 注册时失败，回调不能阻塞关闭自身。"""
    bad = SimpleSpider()
    with pytest.raises(ValueError, match="unsupported Air"):
        bad.use("template.before_render", {"func": lambda context: None})

    class Reentrant(SimpleSpider):
        """故意从存储回调关闭自身。"""

        def item_pipeline(self, context):
            """尝试非法生命周期重入。

            Args:
                context: 当前记录上下文。
            """
            self.close()

    spider = Reentrant([{"url": "https://example.com/one"}])
    with pytest.raises(ExecutionError, match="cannot run inside"):
        spider.run(kwargs={"timeout": 5})


def test_dev_parameters_seed_shapes_and_record_kwargs():
    """使用 dev 参数、record 关键字、种子批次和 list 解析返回值完成消费。"""
    queue = MemoryTaskQueue("parameters@v1")
    downloader = FakeDownloader()

    class Compatible(Spider):
        """只使用保留的业务方法签名。"""

        def make_seeds(self, context, **kwargs):
            """生成单条和一批种子。

            Args:
                context: 初始化上下文。
                kwargs: 含恢复记录。

            Yields:
                种子或种子列表。
            """
            assert kwargs["record"] is context.record
            assert context.target is self
            assert context.task_queue is queue
            assert context.queue_name == "parameters@v1"
            kwargs["record"]["page"] = 1
            yield {"url": "https://example.com/one"}
            kwargs["record"]["page"] = 2
            yield [
                {"url": "https://example.com/two"},
                {"url": "https://example.com/three"},
            ]

        def parse(self, context):
            """返回显式支持的记录列表。

            Args:
                context: 消费上下文。

            Returns:
                记录列表。
            """
            assert context.target is self and context.task_queue is queue
            assert context.queue_name == self.queue_name
            return [{"url": context.seeds["url"]}]

        def item_pipeline(self, context):
            """正常返回后执行剩余插槽，再确认交付。

            Args:
                context: 当前记录上下文。
            """
            assert isinstance(context.items, Items)

    spider = Compatible(
        concurrency=2,
        downloader=downloader,
        task_queue=queue,
        queue_name="parameters@v1",
        proxy="http://127.0.0.1:7890",
        forever=False,
    )
    assert spider.concurrency == 2 and spider.downloader is downloader
    report = spider.run("all", args=[], kwargs={"timeout": 5})
    assert report.counts["acked"] == 3
    assert report.initialization.progress == {"page": 2}
    assert all(request.proxy == "http://127.0.0.1:7890" for request in downloader.calls)


def test_reusable_response_plugins_registered_with_use():
    """同一个检查函数与展示插件可以注册到多个实例，不依赖 Spider 检查方法。"""
    messages = []
    checks = []

    def accepted(context):
        """自定义业务成功条件，让 403 响应进入解析。

        Args:
            context: 当前响应上下文。

        Returns:
            是否通过业务检查。
        """
        checks.append(context.target.queue_name)
        return context.response.status_code == 403

    class Configured(SimpleSpider):
        """复用声明式插件调用。"""

        def install(self):
            """明确选择要注册的插件及其参数。"""
            self.use(hooks.BEFORE_REQUEST, {"func": on_request.Before.set_proxy})
            self.use(
                hooks.AFTER_REQUEST,
                {
                    "func": on_request.After.bypass,
                    "kwargs": {"ok": accepted},
                    "index": 1,
                },
                hooks.Task(
                    on_request.After.show_response,
                    kwargs={"handle": messages.append, "fmt": 1},
                    index=0,
                ),
            )

    first = Configured(
        [{"url": "https://example.com/forbidden"}], queue_name="plugin-first"
    )
    second = Configured(
        [{"url": "https://example.com/forbidden"}], queue_name="plugin-second"
    )
    assert not hasattr(first, "check_response")
    assert first.run(kwargs={"timeout": 5}).counts["acked"] == 1
    assert second.run(kwargs={"timeout": 5}).counts["acked"] == 1
    assert checks == ["plugin-first", "plugin-second"]
    assert len(messages) == 2 and all("status=403" in message for message in messages)


def test_use_plugins_can_read_isolated_current_context_without_parameter():
    """use 支持零 Context 参数插件，异步等待前后绑定不串任务或泄漏。"""
    seen = []

    def record(marker):
        """通过当前 Context 记录带自有参数的同步插件调用。

        Args:
            marker: 插件注册时提供的业务标记。
        """
        context = Context.get_context()
        seen.append((marker, context.seeds["url"], id(context)))

    async def record_async():
        """验证异步插件在挂起后仍取得自身 Context。"""
        context = Context.get_context()
        await asyncio.sleep(0)
        assert Context.get_context() is context
        seen.append(("async", context.seeds["url"], id(context)))

    def select_second():
        """通过当前 Context 选择第二个种子。"""
        return Context.get_context().seeds["url"].endswith("/second")

    class ImplicitContext(SimpleSpider):
        """直接通过 use 装配隐式 Context 插件。"""

        def install(self):
            """注册同步和异步的零 Context 参数插件。"""
            self.use(
                hooks.AFTER_REQUEST,
                hooks.Task(record, args=("sync",), index=0),
                hooks.Task(record_async, index=1),
                hooks.Task(record, args=("matched",), match=select_second, index=2),
            )

    with pytest.raises(RuntimeError, match="no domain Context is active"):
        Context.get_context()
    spider = ImplicitContext(
        [
            {"url": "https://example.com/first"},
            {"url": "https://example.com/second"},
        ],
        concurrency=2,
    )
    assert spider.run(kwargs={"timeout": 5}).counts["acked"] == 2
    assert {(marker, url) for marker, url, _ in seen} == {
        ("sync", "https://example.com/first"),
        ("async", "https://example.com/first"),
        ("sync", "https://example.com/second"),
        ("async", "https://example.com/second"),
        ("matched", "https://example.com/second"),
    }
    grouped = {}
    for marker, url, identity in seen:
        grouped.setdefault(url, {})[marker] = identity
    assert all(values["sync"] == values["async"] for values in grouped.values())
    assert len({values["sync"] for values in grouped.values()}) == 2
    with pytest.raises(RuntimeError, match="no domain Context is active"):
        Context.get_context()


def test_success_and_failure_signals_skip_download():
    """显式提前成功沿完成端口路由；显式失败不被普通 ACK 吞掉。"""
    downloader = FakeDownloader()

    class StopEarly(SimpleSpider):
        """在请求构造后显式停止。"""

        @hooks.on(hooks.AFTER_MAKE_REQUEST)
        def finish(self, context):
            """根据种子登记最终结果。

            Args:
                context: 当前上下文。
            """
            if context.seeds.get("failed"):
                raise signals.Failure()
            else:
                raise signals.Success()

    first = StopEarly([{"url": "https://example.com/skip"}], downloader=downloader)
    assert first.run(kwargs={"timeout": 5}).counts["acked"] == 1
    assert downloader.calls == [] and first.stored == []
    second = StopEarly(
        [{"url": "https://example.com/skip", "failed": True}], downloader=downloader
    )
    with pytest.raises(ExecutionError, match="seed marked as failed"):
        second.run(kwargs={"timeout": 5})
    assert second.report().counts["dead"] == 1
    assert downloader.calls == []


def test_concurrency_and_forever_close():
    """独立种子并发执行，forever 消费者在空闲时仍等待并可以关闭。"""
    barrier = threading.Barrier(2)
    completed = threading.Event()

    class ParallelFetch(FakeDownloader):
        """两项同步下载必须同时到达。"""

        def fetch(self, request):
            """等待另一个任务，再产生响应。

            Args:
                request: 当前请求。

            Returns:
                固定响应。
            """
            barrier.wait(3)
            return super().fetch(request)

    class Continuous(SimpleSpider):
        """存储两项后通知测试。"""

        def item_pipeline(self, context):
            """追加结果并检查完成数量。

            Args:
                context: 当前记录。
            """
            super().item_pipeline(context)
            if len(self.stored) == 2:
                completed.set()

    spider = Continuous(concurrency=2, downloader=ParallelFetch(), forever=True)
    spider.submit({"url": "https://example.com/a"})
    spider.submit({"url": "https://example.com/b"})
    with ThreadPoolExecutor(max_workers=2) as executor:
        running = executor.submit(spider.run_spider)
        try:
            assert completed.wait(5)
            assert not running.done()
            closing = executor.submit(spider.close)
            with pytest.raises(ExecutionCancelledError):
                running.result(5)
            closing.result(5)
        finally:
            spider.close()
    assert spider.report().counts["acked"] == 2


def _distributed_worker(redis_url, queue_name, prefix, barrier):
    """在独立进程中创建同名爬虫并消费真实 HTTP 种子。

    Args:
        redis_url: 专用测试 Redis。
        queue_name: 共享队列名称。
        prefix: 当前测试键前缀。
        barrier: 两个消费者的启动屏障。
    """
    from redis import Redis

    from bricks.queues.redis import RedisTaskQueue

    client = Redis.from_url(redis_url, socket_timeout=2, socket_connect_timeout=2)
    queue = RedisTaskQueue(client, queue_name, prefix=prefix)

    class Distributed(Spider):
        """只消费已经提交的种子。"""

        def parse(self, context):
            """解析真实 HTTP JSON。

            Args:
                context: 响应上下文。

            Returns:
                当前 URL 对应记录。
            """
            return [
                {"url": context.seeds["url"], "path": context.response.json()["path"]}
            ]

        def item_pipeline(self, context):
            """以 URL 幂等存储，并记录处理进程。

            Args:
                context: 待存储记录。
            """
            for item in context.items:
                client.hset(f"{prefix}:items", item["url"], json.dumps(item))
                client.sadd(f"{prefix}:workers", os.getpid())
            time.sleep(0.04)

    spider = Distributed(
        task_queue=queue,
        queue_name=queue_name,
        downloader=CurlCffiDownloader(trust_env=False),
    )
    try:
        barrier.wait(10)
        spider.run("spider", kwargs={"timeout": 15})
    finally:
        spider.close()
        client.close()


def test_redis_two_process_spiders_consume_http_seeds(server):
    """真实 Redis 中的初始化结果可由两个独立爬虫进程共同消费。

    Args:
        server: 本地 HTTP 服务。
    """
    redis_url = os.environ.get("BRICKS_TEST_REDIS_URL")
    if not redis_url:
        pytest.skip("set BRICKS_TEST_REDIS_URL for distributed Air tests")
    from redis import Redis

    from bricks.queues.redis import RedisTaskQueue

    base, _, _ = server
    queue_name = f"air-{uuid4().hex}"
    prefix = f"bricks-air-test:{uuid4().hex}"
    client = Redis.from_url(redis_url, socket_timeout=2, socket_connect_timeout=2)
    queue = RedisTaskQueue(client, queue_name, prefix=prefix)
    producer = SimpleSpider(
        [{"url": f"{base}/item/{index}"} for index in range(12)],
        task_queue=queue,
        queue_name=queue_name,
    )
    initial = producer.run("init")
    assert initial.status == "succeeded"
    process_context = multiprocessing.get_context("spawn")
    barrier = process_context.Barrier(2)
    workers = [
        process_context.Process(
            target=_distributed_worker, args=(redis_url, queue_name, prefix, barrier)
        )
        for _ in range(2)
    ]
    try:
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(20)
            assert worker.exitcode == 0
        assert client.hlen(f"{prefix}:items") == 12
        assert client.scard(f"{prefix}:workers") == 2
        assert queue.counts()["acked"] == 12
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(5)
        keys = list(client.scan_iter(match=f"{prefix}:*"))
        if keys:
            client.delete(*keys)
        client.close()


def test_producer_only_close_leaves_pending_tasks_without_waiting():
    """未启动消费者的实例关闭时，不等待其他进程才会处理的种子。"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from bricks.spider import Spider\ns = Spider()\ns.submit({'url': 'https://example.com'})\ns.open()\ns.close()\nassert s.report().counts['pending'] == 1\n",
        ],
        text=True,
        capture_output=True,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr


def test_complete_local_air_example_commits_before_ack(tmp_path):
    """列表发现详情后完成幂等 SQLite 存储，再确认各项交付。

    Args:
        tmp_path: 本测试拥有的输出目录。
    """
    from examples.air_spider import run_demo

    report, stored = run_demo(tmp_path / "products.sqlite3")
    assert report.counts == {"pending": 0, "leased": 0, "acked": 4, "dead": 0}
    assert stored == 2


def test_static_class_plugins_and_inheritance_override():
    """静态和类方法沿用装饰方式，显式参数传递且子类覆盖移除父类声明。"""
    trace = []

    class Parent(SimpleSpider):
        """带可覆盖声明的父类。"""

        @hooks.on(hooks.BEFORE_REQUEST)
        def replaced(self, context):
            """此声明应被子类的同名方法替换。

            Args:
                context: 当前上下文。
            """
            raise AssertionError("parent hook was not replaced")

    class Child(Parent):
        """复用普通 Python 描述符的子类。"""

        def replaced(self, context):
            """不带声明的覆盖方法。

            Args:
                context: 当前上下文。
            """

        @staticmethod
        @hooks.on(hooks.AFTER_REQUEST, args=("static",), index=1)
        def static(context, marker):
            """处理已绑定参数的静态插件。

            Args:
                context: 当前上下文。
                marker: 显式注册参数。
            """
            trace.append((marker, context.form))

        @hooks.on(hooks.AFTER_REQUEST, kwargs={"marker": "class"}, index=2)
        @classmethod
        def class_method(cls, context, marker):
            """处理类方法插件。

            Args:
                context: 当前上下文。
                marker: 显式注册参数。
            """
            assert cls is Child
            trace.append((marker, context.form))

    Child([{"url": "https://example.com/one"}]).run(kwargs={"timeout": 5})
    assert trace == [("static", hooks.AFTER_REQUEST), ("class", hooks.AFTER_REQUEST)]


def test_core_hook_replacement_rebinds_domain_inputs():
    """核心 Hook 修改端口后，节点与领域 Context 使用最终输入。"""

    class ReplaceSeed(NodeHook):
        """显式替换 make_request 的种子端口。"""

        def enter(self, call):
            """返回新的端口输入。

            Args:
                call: 原始调用。

            Returns:
                保留调用身份的新输入。
            """
            return call.with_inputs({"seeds": {"url": "https://example.com/replaced"}})

    spider = SimpleSpider()
    work = spider.submit({"url": "https://example.com/original"})
    with spider:
        spider.runtime.attach(
            ReplaceSeed(), graph=spider.crawl_graph_name, node="before_make_request"
        )
        spider.run_spider(timeout=5)
    assert spider.stored[0]["url"] == "https://example.com/replaced"
    assert (
        spider.task_queue.get(work.id).work.inputs["url"]
        == "https://example.com/original"
    )


def test_initialization_lease_is_renewed_during_slow_seed_factory(renewals):
    """同步种子工厂长时间未返回时，调用线程仍保持初始化租约。

    Args:
        renewals: 续租观察器工厂。
    """
    started, release = threading.Event(), threading.Event()
    queue = MemoryTaskQueue("slow-producer")
    watch = renewals(queue, "renew_init")

    class SlowFactory(SimpleSpider):
        """等待测试释放后才返回种子。"""

        def make_seeds(self, context, **kwargs):
            """模拟慢初始化查询。

            Args:
                context: 初始化上下文。
                kwargs: 恢复记录参数。

            Returns:
                一项种子。
            """
            started.set()
            assert release.wait(3)
            return {"url": "https://example.com/one"}

    spider = SlowFactory(task_queue=queue, lease_seconds=0.6)
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            running = executor.submit(spider.run_init)
            try:
                assert started.wait(2)
                expiry = time.monotonic() + 0.6
                watch.wait_past(expiry)
                assert queue.acquire_init("competitor") is None
            finally:
                release.set()
            assert running.result(5).status == "succeeded"
    finally:
        release.set()
        spider.close()


def test_failure_signal_prevents_default_pipeline():
    """失败信号立即结束任务，不运行默认流水线。"""

    class Failed(Spider):
        """使用默认解析和流水线，插件明确登记失败。"""

        def make_seeds(self, context, **kwargs):
            """返回单条种子。

            Args:
                context: 初始化上下文。
                kwargs: 恢复记录。

            Returns:
                种子对象。
            """
            return {"url": "https://example.com/one"}

        @hooks.on(hooks.AFTER_REQUEST)
        def failed(self, context):
            """立即报告失败并跳过后续处理。

            Args:
                context: 当前上下文。
            """
            raise signals.Failure()

    spider = Failed(downloader=FakeDownloader())
    with pytest.raises(ExecutionError, match="seed marked as failed"):
        spider.run(kwargs={"timeout": 5})
    assert spider.report().counts["dead"] == 1


def test_native_async_download_and_business_methods_complete(server):
    """原生异步下载和异步业务方法在同一引擎事件循环完成整条流程。

    Args:
        server: 本地 HTTP 服务。
    """
    base, _, _ = server
    loops = set()
    stored = []

    class AsyncBusiness(Spider):
        """完整异步业务方法。"""

        async def make_seeds(self, context, **kwargs):
            """异步返回单个种子。

            Args:
                context: 初始化上下文。
                kwargs: 恢复记录。

            Returns:
                JSON 种子对象。
            """
            loops.add(id(asyncio.get_running_loop()))
            await asyncio.sleep(0)
            return {"url": base + "/echo"}

        async def make_request(self, context):
            """异步构造请求。

            Args:
                context: 当前种子。

            Returns:
                Request。
            """
            loops.add(id(asyncio.get_running_loop()))
            await asyncio.sleep(0)
            return Request(context.seeds["url"])

        async def parse(self, context):
            """异步解析当前 JSON。

            Args:
                context: 当前响应。

            Returns:
                记录列表。
            """
            loops.add(id(asyncio.get_running_loop()))
            await asyncio.sleep(0)
            return [context.response.json()]

        async def item_pipeline(self, context):
            """异步写入测试收集器。

            Args:
                context: 当前记录。
            """
            loops.add(id(asyncio.get_running_loop()))
            await asyncio.sleep(0)
            stored.extend(context.items)

    spider = AsyncBusiness(downloader=AsyncCurlCffiDownloader(trust_env=False))
    assert spider.run(kwargs={"timeout": 5}).counts["acked"] == 1
    assert len(loops) == 1 and stored[0]["path"] == "/echo"


def test_borrowed_slot_session_is_reused_and_remains_caller_owned(server):
    """本地翻页在多个可用 Slot 中固定会话，关闭后不关闭借用资源。

    Args:
        server: 本地 HTTP 服务。
    """
    base, _, _ = server
    factory = CurlCffiDownloader(trust_env=False)
    sessions = [factory.prepare_session(), factory.prepare_session()]
    pool = SlotPool(
        slots=[
            Slot({"crawler.downloaders": {"default": session}}) for session in sessions
        ]
    )
    stored = []
    slot_ids = []

    class SessionSpider(Spider):
        """为同一业务身份装配一个显式会话槽。"""

        def build_download_node(self):
            """从当前 Slot 解析会话下载器。

            Returns:
                组合会话及临时下载器的节点。
            """
            return DownloadNode(
                lambda slot: slot["crawler.downloaders"],
                isolated_downloaders={"default": factory},
            )

        def make_seeds(self, context, **kwargs):
            """从登录请求开始。

            Args:
                context: 初始化上下文。
                kwargs: 恢复记录。

            Returns:
                一项种子。
            """
            return {"url": base + "/login/account", "page": 1}

        def parse(self, context):
            """登录后提交同队列的后续请求。

            Args:
                context: 当前响应。

            Returns:
                回显记录。
            """
            return [context.response.json()]

        def make_request(self, context):
            """忽略分页业务字段并构造当前 URL 请求。

            Args:
                context: 当前页的消费 Context。

            Returns:
                当前 URL 请求。
            """
            return Request(context.seeds["url"])

        def item_pipeline(self, context):
            """保存回显记录。

            Args:
                context: 当前结果。
            """
            stored.extend(context.items)
            slot_ids.append(id(context.slot))

        def install(self):
            """登录页处理后在当前 Slot 中继续回显页。"""
            super().install()
            self.use(
                hooks.AFTER_PIPELINE,
                {
                    "func": scripts.turn_page,
                    "kwargs": {
                        "match": ["context.seeds['page'] == 1"],
                        "action": lambda: {"url": base + "/echo", "page": 2},
                        "call_later": False,
                    },
                },
            )

    try:
        spider = SessionSpider(downloader=factory, slots=pool)
        assert spider.run(kwargs={"timeout": 5}).counts["acked"] == 2
        assert stored[-1]["cookie"] == "account=account"
        assert len(slot_ids) == 2 and len(set(slot_ids)) == 1
        assert (
            sum(
                session.fetch(Request(base + "/echo")).json()["cookie"]
                == "account=account"
                for session in sessions
            )
            == 1
        )
        assert pool.available == 2
    finally:
        pool.close()
        for session in sessions:
            factory.close_session(session)


def test_framework_manages_slot_sessions_and_context_resources():
    """作者只配置并发，框架按 Slot 创建会话并让持久本地子任务复用资源。"""

    class Session:
        """保存一个 Slot 独占的模拟 Cookie。"""

        def __init__(self, identity):
            """初始化空会话。

            Args:
                identity: 当前会话编号。
            """
            self.identity = identity
            self.cookie = ""

        def fetch(self, request):
            """登录页写 Cookie，后续页返回当前状态。

            Args:
                request: 当前页请求。

            Returns:
                携带会话观察值的响应。
            """
            before = self.cookie
            if request.url.endswith("/login"):
                self.cookie = "account=A"
            return Response(
                json.dumps(
                    {"session": self.identity, "cookie_before": before}
                ).encode(),
                status_code=200,
                request=request,
            )

    class Factory:
        """记录框架创建、临时调用和关闭的会话。"""

        def __init__(self):
            """初始化资源观察集合。"""
            self.sessions = []
            self.closed = []
            self.temporary_calls = 0

        def prepare_session(self, *, proxy=None):
            """为一个 Slot 创建独立会话。

            Args:
                proxy: 框架确定的固定会话代理。

            Returns:
                新会话。
            """
            assert proxy is None
            session = Session(len(self.sessions))
            self.sessions.append(session)
            return session

        def close_session(self, session):
            """记录框架释放的会话。

            Args:
                session: 已停止使用的会话。
            """
            self.closed.append(session)

        def fetch(self, request):
            """拒绝在复用模式下走临时下载路径。

            Args:
                request: 当前请求。

            Raises:
                AssertionError: 框架未使用 Slot Session。
            """
            self.temporary_calls += 1
            raise AssertionError("temporary downloader must not be used")

    observed = []

    class Managed(Spider):
        """不接触 SlotPool 或 DownloadNode 的普通业务爬虫。"""

        def make_seeds(self, context, **kwargs):
            """提供登录页。

            Args:
                context: 初始化 Context。
                kwargs: 初始化记录。

            Returns:
                登录种子。
            """
            return {"url": "https://example.com/login", "page": 1}

        def make_request(self, context):
            """构造当前页请求。

            Args:
                context: 消费 Context。

            Returns:
                当前 URL 请求。
            """
            return Request(context.seeds["url"])

        def parse(self, context):
            """登录后持久化并本地提交第二页。

            Args:
                context: 当前响应 Context。

            Returns:
                会话观察记录。
            """
            if context.seeds["page"] == 1:
                context.submit({"url": "https://example.com/page/2", "page": 2})
            return [context.response.json()]

        def item_pipeline(self, context):
            """跨页写入和读取 Slot 业务资源。

            Args:
                context: 当前页 Context。
            """
            if context.seeds["page"] == 1:
                assert context.install("account", "A", nx=True) == "A"
            observed.append(
                (
                    context.seeds["page"],
                    context.items[0],
                    context.obtain("account"),
                    id(context.slot),
                )
            )

    factory = Factory()
    spider = Managed(downloader=factory, concurrency=2)
    assert spider.run(kwargs={"timeout": 5}).counts["acked"] == 2
    assert len(factory.sessions) == 2
    assert factory.closed == list(reversed(factory.sessions))
    assert factory.temporary_calls == 0
    assert observed[1][1]["cookie_before"] == "account=A"
    assert observed[1][2] == "A"
    assert observed[0][3] == observed[1][3]


def test_context_emit_selects_slot_reuse_for_arbitrary_target_graph():
    """跨图派生可显式选择是否延续 Slot，两种目标 Work 都持久化。"""
    observed = []

    class Collect(Node):
        """记录目标 Graph 的 Slot 身份。"""

        input_ports = Ports(payload=dict)  # 领域事件携带的 JSON 数据。
        output_ports = Ports()  # 只记录观察值。

        def execute(self, inputs, context):
            """保存事件类别和 Slot。

            Args:
                inputs: 含事件 payload 的输入。
                context: 目标 Graph 的核心 Context。
            """
            observed.append((inputs["payload"]["kind"], id(context.slot)))

    class Source(SimpleSpider):
        """从消费 Graph 派生两个不同 Slot 策略的目标 Work。"""

        def parse(self, context):
            """发布本地优先和普通分发事件。

            Args:
                context: 源消费 Context。

            Returns:
                当前页面记录。
            """
            observed.append(("source", id(context.slot)))
            context.emit("child.local", {"kind": "local"}, affinity="current")
            context.emit("child.any", {"kind": "any"}, affinity="any")
            return super().parse(context)

    spider = Source(
        [{"url": "https://example.com/source"}],
        concurrency=3,
        queue_name="context-emit-slot",
    )
    spider.open()
    spider.runtime.register(
        "child@v1", Graph(entrypoint="collect").add(collect=Collect())
    )
    spider.runtime.route("child.local", graph="child@v1", queue=spider.queue_name)
    spider.runtime.route("child.any", graph="child@v1", queue=spider.queue_name)
    assert spider.run(kwargs={"timeout": 5}).counts["acked"] == 3
    identities = dict(observed)
    assert identities["local"] == identities["source"]
    assert set(identities) == {"source", "local", "any"}


def test_survey_consumes_dynamic_seeds_and_keeps_hooks_retries_and_side_effects(
    tmp_path,
):
    """手动消费跳过种子工厂与默认存储，插件写入、重试和新种子仍正常执行。"""
    records = tmp_path / "survey-hooks.txt"

    class Manual(SimpleSpider):
        """带动态翻页和真实插件写入的手动消费爬虫。"""

        def make_seeds(self, context, **kwargs):
            """拒绝手动消费错误调用自动种子源。

            Args:
                context: 初始化上下文。
                kwargs: 初始化参数。

            Raises:
                AssertionError: 手动消费不应生产种子。
            """
            raise AssertionError("survey must not call make_seeds")

        def parse(self, context):
            """第一页提交第二页，保持正常去重和队列消费。

            Args:
                context: 当前响应上下文。

            Returns:
                正常解析结果。
            """
            if context.seeds["page"] == 1:
                context.submit({"url": "https://example.com/child", "page": 2})
            return super().parse(context)

        @hooks.on(hooks.BEFORE_PIPELINE)
        def before_store(self, context):
            """记录存储前插件的实际外部写入。

            Args:
                context: 当前 Items 上下文。
            """
            with records.open("a") as stream:
                stream.write(f"before:{context.seeds['page']}\n")

        @hooks.on(hooks.AFTER_PIPELINE)
        def after_store(self, context):
            """验证后置插件仍在确认前运行并可改变最终结果。

            Args:
                context: 当前 Items 上下文。
            """
            assert context.task_queue.counts()["leased"] == 3 - context.seeds["page"]
            context.items[0]["after"] = True
            with records.open("a") as stream:
                stream.write(f"after:{context.seeds['page']}\n")

    spider = Manual()
    result = spider.survey({"url": "https://example.com/retry", "page": 1}, timeout=5)
    assert [context.seeds["page"] for context in result] == [1, 2]
    assert [context.attempt for context in result] == [3, 1]
    assert all(context.items[0]["after"] for context in result)
    assert len(spider.downloader.calls) == 4
    assert spider.stored == []
    assert records.read_text().splitlines() == [
        "before:1",
        "after:1",
        "before:2",
        "after:2",
    ]
    assert spider.report().counts == {"pending": 0, "leased": 0, "acked": 2, "dead": 0}
    assert spider.report().initialization is None
    with pytest.raises(RuntimeError, match="new Spider"):
        spider.survey()


def test_survey_projection_early_success_and_existing_queue_are_explicit():
    """已装配队列也正常消费，提前 Success 的结果可按指定字段提取。"""
    spider = SimpleSpider()
    spider.submit({"url": "https://example.com/queued"})
    result = spider.survey(extract=["seeds", "items"])
    assert result[0]["seeds"] == {"url": "https://example.com/queued"}
    assert result[0]["items"][0]["attempt"] == 1

    class Early(SimpleSpider):
        """请求构造前明确完成。"""

        @hooks.on(hooks.BEFORE_MAKE_REQUEST)
        def finish(self, context):
            """提前完成当前种子。

            Args:
                context: 当前种子。

            Raises:
                signals.Success: 无需发起请求。
            """
            raise signals.Success()

    early = Early()
    assert early.survey(
        {"url": "https://example.com/early"},
        extract=["seeds", "attempt", "response", "items"],
    ) == [
        {
            "seeds": {"url": "https://example.com/early"},
            "attempt": 0,
            "response": None,
            "items": None,
        }
    ]
    assert early.downloader.calls == []
    assert early.report().counts["acked"] == 1


def test_survey_hook_failure_never_acknowledges_or_calls_default_pipeline():
    """调查同样传播业务错误，后置插件失败不能伪称任务已完成。"""

    class Broken(SimpleSpider):
        """在存储后置插件中失败。"""

        @hooks.on(hooks.AFTER_PIPELINE)
        def fail(self, context):
            """模拟外部写入失败。

            Args:
                context: 当前处理结果。

            Raises:
                OSError: 插件存储失败。
            """
            raise OSError("survey storage hook failed")

    spider = Broken()
    with pytest.raises(ExecutionError, match="survey storage hook failed"):
        spider.survey({"url": "https://example.com/one"})
    assert spider.stored == []
    assert spider.report().counts["acked"] == 0
    assert spider.report().counts["dead"] == 1
    with pytest.raises(RuntimeError, match="closed"):
        spider.open()


def test_crawl_plans_share_hooks_and_expose_validated_storage_branches():
    """两种计划只选择各自的存储操作并保留业务 Hook，裁剪出口不泄露任何控制端口。"""
    spider = SimpleSpider()

    def marker(context):
        """仅用于识别插槽中保留的业务 Hook。

        Args:
            context: 当前记录上下文。
        """

    spider.use(hooks.AFTER_PIPELINE, hooks.Task(marker))
    with pytest.raises(RuntimeError, match="open the spider"):
        spider.build_crawl_plan()
    spider.open()
    normal = spider.build_crawl_plan()
    manual = spider.build_crawl_plan(survey=True)
    assert "pipeline" in normal.nodes and "survey_pipeline" not in normal.nodes
    assert "survey_pipeline" in manual.nodes and "pipeline" not in manual.nodes
    assert normal.outputs == manual.outputs == {("complete", "summary")}
    assert normal.cut_outputs == manual.cut_outputs == frozenset()
    assert "before_pipeline" in manual.nodes and "after_pipeline" in manual.nodes
    for plan in (normal, manual):
        # 插槽节点只公开 Graph 契约，通过其固定声明确认业务 Hook 未在计划中丢失。
        tasks = plan.graph.nodes["after_pipeline"]._tasks
        assert marker in [task.func for task in tasks]
    short = normal.graph.plan(
        include={"parse"},
        entrypoint="parse",
        edges=[],
        outputs={("parse", "task"), ("parse", "complete")},
    )
    assert short.describe()["entrypoint"] == "parse"
    assert short.cut_outputs == short.outputs
    spider.close()


def test_survey_validates_whole_seed_batch_and_lifecycle_before_consumption():
    """非法参数不提交部分种子，活动实例不能切换消费图。"""
    spider = SimpleSpider()
    with pytest.raises(TypeError):
        spider.survey({"url": "https://example.com/one"}, {"bad": object()})
    assert spider.report().counts["pending"] == 0
    with pytest.raises(TypeError, match="extract"):
        spider.survey(extract="items")
    with spider:
        with pytest.raises(RuntimeError, match="new Spider"):
            spider.survey()
    continuous = SimpleSpider(forever=True)
    try:
        with pytest.raises(ValueError, match="forever"):
            continuous.survey()
    finally:
        continuous.close()


def test_survey_local_http_example_follows_pages_without_default_database(tmp_path):
    """真实 HTTP 手动消费继续分页和详情，完成结果完整且未创建默认存储。"""
    database = tmp_path / "survey.sqlite3"
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "examples.air_spider",
            "--survey",
            "--output",
            str(database),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    rows = json.loads(completed.stdout)
    assert len(rows) == 4
    assert sum(len(row["items"]) for row in rows) == 2
    assert not database.exists()


@pytest.mark.parametrize(
    "options",
    [
        {"auth": lambda request: None},
        {"threshold": 1},
        {"ttl": 1},
    ],
)
def test_auto_session_rejects_request_proxy_policies(options):
    """自动固定会话代理拒绝只适用于请求级代理的策略。"""
    spider = SimpleSpider(
        downloader=CurlCffiDownloader(),
        proxy=Manager(CustomProxy("proxy.test:8080", **options)),
    )
    with pytest.raises(ValueError, match="slot_session_reuse=False"):
        spider.open()
    spider.close()
