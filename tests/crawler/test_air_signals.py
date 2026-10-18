"""显式 HookNode、领域信号和非图生命周期/领取边界的行为契约。"""

import asyncio
import time
from contextlib import ExitStack
from threading import Barrier

import pytest
from interlace.engine import Slot, SlotPool
from interlace.engine.errors import ExecutionCancelledError, ExecutionError

from bricks import DownloadNode, Response, hooks, signals
from bricks.downloaders.curl_cffi import CurlCffiDownloader
from bricks.queues import MemoryTaskQueue
from bricks.spider import Spider
from bricks.spider._nodes import HookNode
from bricks.spider.context import ResponseRejectedError


class Echo:
    """返回确定性响应的下载器。

    Attributes:
        calls: 当前实例收到的请求，不被其他测试共享。
    """

    def __init__(self):
        """准备请求记录。"""
        self.calls = []

    def fetch(self, request):
        """记录一次下载。

        Args:
            request: 本次请求副本。

        Returns:
            状态码为 200 的响应。
        """
        self.calls.append(request)
        return Response(b"{}", status_code=200, request=request)


class Example(Spider):
    """用于验证业务流程的最小爬虫。

    Attributes:
        source: 待生产的种子列表。
        stored: 当前实例保存的记录。
    """

    def __init__(self, seeds=None, **kwargs):
        """准备测试源与独立下载器。

        Args:
            seeds: 显式种子列表，None 使用一个默认种子。
            kwargs: Spider 配置。
        """
        kwargs.setdefault("downloader", Echo())
        super().__init__(**kwargs)
        self.source = [{"url": "https://example.com/one"}] if seeds is None else seeds
        self.stored = []

    def make_seeds(self, context, **kwargs):
        """返回测试种子。

        Args:
            context: 初始化上下文。
            kwargs: 恢复记录。

        Returns:
            当前种子列表。
        """
        return self.source

    def parse(self, context):
        """提取最终请求地址。

        Args:
            context: 当前响应上下文。

        Returns:
            一条记录。
        """
        return [{"url": context.request.url}]

    def item_pipeline(self, context):
        """保存本次结果，正常返回让后置插槽继续执行。

        Args:
            context: 当前记录上下文。
        """
        self.stored.extend(context.items)


def test_graph_contains_real_hook_nodes_and_no_legacy_decision_node():
    """图定义直接呈现插槽，不再通过一个隐藏的响应决策节点跳转。"""
    spider = Example()
    with spider:
        initial, crawl = spider.build_graphs()
    assert "check_response" not in crawl.nodes
    assert isinstance(crawl.nodes["before_request"], HookNode)
    assert isinstance(crawl.nodes["after_request"], HookNode)
    assert isinstance(crawl.nodes["before_parse"], HookNode)
    assert isinstance(initial.nodes["before_put_seeds"], HookNode)
    assert isinstance(initial.nodes["after_put_seeds"], HookNode)
    assert "checkpoint" in initial.nodes


def test_use_during_before_start_is_rejected_after_hooks_are_fixed():
    """BEFORE_START 运行时插槽已固定，再注册插件明确失败而不是静默不生效。"""
    spider = Example()

    def register(context):
        """尝试在启动边界追加请求插件。

        Args:
            context: 启动边界。
        """
        context.target.use(hooks.BEFORE_REQUEST, {"func": lambda ctx: None})

    spider.use(hooks.BEFORE_START, {"func": register})
    with pytest.raises(RuntimeError, match="before opening"):
        spider.run()
    assert spider.downloader.calls == []


def test_root_causes_survive_secondary_failures():
    """初始化根因不被消费失败覆盖，with 块异常已有 cause 时不被关闭失败覆盖。"""

    class BrokenInit(Example):
        """先产出一条种子再失败，同时存储也失败。"""

        def make_seeds(self, context, **kwargs):
            """产出一条种子后抛出初始化根因。

            Args:
                context: 初始化上下文。
                kwargs: 恢复记录。

            Yields:
                单条种子。

            Raises:
                ValueError: 初始化根因。
            """
            yield {"url": "https://example.com/one"}
            raise ValueError("init root cause")

        def item_pipeline(self, context):
            """模拟消费存储失败。

            Args:
                context: 当前记录上下文。

            Raises:
                KeyError: 消费错误。
            """
            raise KeyError("pipeline failed")

    def chain(error):
        """展开显式 cause 链。

        Args:
            error: 链首异常。

        Returns:
            按 cause 顺序排列的异常列表。
        """
        found = []
        while error is not None and error not in found:
            found.append(error)
            error = error.__cause__
        return found

    with pytest.raises(ExecutionError) as caught:
        BrokenInit().run()
    assert any(
        isinstance(error, ValueError) and str(error) == "init root cause"
        for error in chain(caught.value)
    )

    def fail_close(context):
        """模拟关闭插槽的二次错误。

        Args:
            context: 关闭边界。

        Raises:
            OSError: 关闭阶段失败。
        """
        raise OSError("close hook failed")

    root = KeyError("root")
    spider = Example()
    spider.use(hooks.BEFORE_CLOSE, {"func": fail_close})
    with pytest.raises(ValueError) as body:
        with spider:
            raise ValueError("body") from root
    assert body.value.__cause__ is root


@pytest.mark.parametrize("signal", [signals.Success, signals.Failure])
def test_terminal_signal_interrupts_callback_and_remaining_hooks(signal):
    """信号不会被 except Exception 吞掉，也不会继续执行当前插槽剩余回调。

    Args:
        signal: 成功或失败信号类型。
    """
    trace = []
    spider = Example()

    def stop(context):
        """在嵌套业务调用边界立即退出。

        Args:
            context: 当前上下文。
        """
        trace.append("entered")
        try:
            raise signal()
        except Exception:
            trace.append("swallowed")
        trace.append("unreachable")

    spider.use(
        hooks.BEFORE_REQUEST,
        {"func": stop},
        {"func": lambda ctx: trace.append("later")},
    )
    if signal is signals.Failure:
        with pytest.raises(ExecutionError, match="seed marked as failed"):
            spider.run()
        assert spider.report().counts["dead"] == 1
    else:
        assert spider.run().counts["acked"] == 1
    assert trace == ["entered"]
    assert spider.downloader.calls == [] and spider.stored == []


def test_pass_skips_remaining_hooks_but_continues_normal_route():
    """Pass 只结束当前插槽，正常解析与存储仍然执行。"""
    trace = []
    spider = Example()

    def accept(context):
        """显式接受本次响应。

        Args:
            context: 当前响应上下文。
        """
        trace.append("accept")
        raise signals.Pass()

    spider.use(
        hooks.AFTER_REQUEST,
        {"func": accept},
        {"func": lambda ctx: trace.append("later")},
    )
    assert spider.run().counts["acked"] == 1
    assert trace == ["accept"] and len(spider.stored) == 1


def test_skip_pipeline_preserves_after_pipeline_slot():
    """Skip 跳过管道业务方法，但仍执行旧版语义中的 AFTER_PIPELINE。"""
    observed = []
    spider = Example()

    def skip(context):
        """跳过当前管道操作。

        Args:
            context: 管道前上下文。
        """
        raise signals.Skip()

    spider.use(hooks.BEFORE_PIPELINE, {"func": skip})
    spider.use(hooks.AFTER_PIPELINE, {"func": lambda ctx: observed.append(ctx.form)})
    assert spider.run().counts["acked"] == 1
    assert spider.stored == [] and observed == [hooks.AFTER_PIPELINE]


def test_skip_download_requires_and_uses_explicit_cached_response():
    """缓存 Hook 提供替代响应后可以跳过下载，缺少响应时明确失败。"""
    spider = Example()

    def cached(context):
        """提供缓存响应后跳过网络请求。

        Args:
            context: 下载前上下文。
        """
        context.response = Response(b"cached", status_code=200, request=context.request)
        raise signals.Skip()

    spider.use(hooks.BEFORE_REQUEST, {"func": cached})
    assert spider.run().counts["acked"] == 1
    assert spider.downloader.calls == [] and len(spider.stored) == 1


def test_async_retry_runs_explicit_retry_slots_and_keeps_request():
    """异步 Hook 的 Retry 沿图内回边执行，重试前后插槽有明确顺序。"""
    trace, slot_ids = [], []
    spider = Example(max_retry=2)

    async def retry(context):
        """前两次下载后通过异步信号触发重试。

        Args:
            context: 当前响应上下文。
        """
        await asyncio.sleep(0)
        trace.append("response")
        slot_ids.append(context.slot.id)
        if context.attempt < 3:
            raise signals.Retry(delay=0.001)

    spider.use(hooks.AFTER_REQUEST, {"func": retry})
    spider.use(hooks.BEFORE_RETRY, {"func": lambda ctx: trace.append("before")})
    spider.use(hooks.AFTER_RETRY, {"func": lambda ctx: trace.append("after")})
    assert spider.run().counts["acked"] == 1
    assert trace == [
        "response",
        "before",
        "after",
        "response",
        "before",
        "after",
        "response",
    ]
    assert len(set(slot_ids)) == 1
    assert len(spider.downloader.calls) == 3 and len(spider.stored) == 1


def test_predownload_retry_is_bounded_even_without_a_network_attempt():
    """下载前反复 Retry 也消耗预算，不能因 attempt 为零而无限循环。"""
    spider = Example(max_retry=2)

    def retry(context):
        """在真正下载前重试。

        Args:
            context: 下载前上下文。
        """
        raise signals.Retry()

    spider.use(hooks.BEFORE_REQUEST, {"func": retry})
    with pytest.raises(ExecutionError, match="retries exhausted"):
        spider.run(kwargs={"timeout": 3})
    assert spider.downloader.calls == [] and spider.report().counts["dead"] == 1


@pytest.mark.parametrize(
    "phase",
    [
        hooks.BEFORE_PARSE,
        hooks.AFTER_PARSE,
        hooks.BEFORE_PIPELINE,
        hooks.AFTER_PIPELINE,
    ],
)
def test_retry_is_rejected_once_parsing_phase_begins(phase):
    """解析及存储阶段不能用 Retry 重放有副作用的流程。

    Args:
        phase: 被验证的插槽。
    """
    spider = Example()

    def retry(context):
        """触发非法阶段重试。

        Args:
            context: 当前上下文。
        """
        raise signals.Retry()

    spider.use(phase, {"func": retry})
    with pytest.raises(ExecutionError, match=f"Retry is not allowed in {phase}"):
        spider.run()
    assert len(spider.downloader.calls) == 1 and spider.report().counts["acked"] == 0


def test_error_hook_pass_stops_callbacks_without_replacing_original_cause():
    """ERROR_OCCURRED 中 Pass 只终止插槽，原异常及原因保留。"""
    original = ValueError("original failure")
    cause = RuntimeError("original cause")
    original.__cause__ = cause
    seen = []
    spider = Example()

    def fail(context):
        """报告原始业务错误。

        Args:
            context: 当前请求上下文。
        """
        raise original

    def stop(context):
        """记录错误后停止当前插槽。

        Args:
            context: 错误边界上下文。
        """
        seen.append(context.error)
        raise signals.Pass()

    spider.use(hooks.BEFORE_REQUEST, {"func": fail})
    spider.use(
        hooks.ERROR_OCCURRED,
        {"func": stop},
        {"func": lambda ctx: seen.append("late")},
    )
    with pytest.raises(ExecutionError, match="original failure"):
        spider.run()
    assert seen == [original]
    assert original.__cause__ is cause


def test_cancellation_does_not_enter_business_error_hooks():
    """引擎取消不会变为普通业务失败或被错误插件恢复。"""
    seen = []
    spider = Example()

    def cancel(context):
        """模拟执行控制取消。

        Args:
            context: 当前上下文。
        """
        raise ExecutionCancelledError("cancelled")

    spider.use(hooks.BEFORE_REQUEST, {"func": cancel})
    spider.use(hooks.ERROR_OCCURRED, {"func": lambda ctx: seen.append(ctx.error)})
    with pytest.raises(ExecutionCancelledError):
        spider.run()
    assert seen == [] and spider.report().counts["acked"] == 0


def test_initial_put_slots_filter_batch_and_checkpoint_after_after_hook():
    """初始化入队前可过滤批次，入队后 Hook 发生在持久检查点写入之前。"""
    observed = []
    spider = Example(
        seeds=[
            [{"url": "https://example.com/keep"}, {"url": "https://example.com/drop"}]
        ]
    )

    def before(context):
        """显式过滤当前批次。

        Args:
            context: 初始化上下文。
        """
        context.seeds = context.seeds[:1]
        context.record["position"] = 1
        observed.append("before")

    def after(context):
        """确认入队已完成而进度尚未提交。

        Args:
            context: 初始化上下文。
        """
        assert context.task_queue.counts()["pending"] == 1
        assert context.task_queue.initialization().progress is None
        observed.append("after")

    spider.use(hooks.BEFORE_PUT_SEEDS, {"func": before})
    spider.use(hooks.AFTER_PUT_SEEDS, {"func": after})
    record = spider.run("init")
    assert record.progress == {"position": 1}
    assert observed == ["before", "after"] and spider.report().counts["pending"] == 1


def test_claim_hooks_have_real_boundaries_and_local_seed_copy():
    """领取前没有 Slot/种子，领取后修改本地副本再交给图执行。"""
    observed = []
    spider = Example(seeds=[])
    work = spider.submit({"url": "https://example.com/original"})

    def before(context):
        """检查实际领取前的上下文。

        Args:
            context: 领取前边界。
        """
        assert context.slot is None
        with pytest.raises(RuntimeError):
            # 属性访问本身就是被测操作：领取前读取种子必须失败。
            context.seeds  # noqa: B018

    def after(context):
        """改变当前交付种子，不修改持久任务。

        Args:
            context: 已领取的上下文。
        """
        assert context.slot is not None and context.lease.attempt == 1
        context.seeds["url"] = "https://example.com/replaced"
        observed.append(context.lease.work.id)

    spider.use(hooks.BEFORE_GET_SEEDS, {"func": before})
    spider.use(hooks.AFTER_GET_SEEDS, {"func": after})
    spider.run("spider", kwargs={"timeout": 3})
    assert observed == [work.id]
    assert spider.stored == [{"url": "https://example.com/replaced"}]
    assert (
        spider.task_queue.get(work.id).work.inputs["url"]
        == "https://example.com/original"
    )


@pytest.mark.parametrize("signal", [signals.Success, signals.Failure])
def test_claim_signal_explicitly_finishes_without_starting_graph(signal):
    """领取后显式终结交付，仍由队列后端校验租约并完成状态写回。

    Args:
        signal: 成功或失败信号。
    """
    spider = Example(seeds=[])
    spider.submit({"url": "https://example.com"})

    def terminate(context):
        """过滤本次已领取任务。

        Args:
            context: 领取后上下文。
        """
        raise signal()

    spider.use(hooks.AFTER_GET_SEEDS, {"func": terminate})
    if signal is signals.Failure:
        with pytest.raises(ResponseRejectedError, match="claimed seed rejected"):
            spider.run("spider")
        assert spider.report().counts["dead"] == 1
    else:
        assert spider.run("spider").counts["acked"] == 1
    assert spider.downloader.calls == []


def test_start_signal_and_close_hook_run_once_without_consuming():
    """启动 Success 跳过业务，关闭插槽仍执行一次且待领取任务保留。"""
    trace = []
    spider = Example(seeds=[])
    spider.submit({"url": "https://example.com"})

    def stop(context):
        """明确不启动当前实例的业务流程。

        Args:
            context: 启动边界。
        """
        trace.append("start")
        raise signals.Success()

    spider.use(hooks.BEFORE_START, {"func": stop})
    spider.use(hooks.BEFORE_CLOSE, {"func": lambda ctx: trace.append("close")})
    assert spider.run().counts["pending"] == 1
    spider.close()
    assert trace == ["start", "close"] and spider.downloader.calls == []


def test_wait_before_claim_keeps_active_delivery_renewed(renewals):
    """领取前 Wait 暂缓新领取，已经执行的任务仍持续续租。

    Args:
        renewals: 续租观察器工厂。
    """
    waits = []

    class Slow(Example):
        """在租约期间执行慢同步存储。"""

        def item_pipeline(self, context):
            """等到在途租约跨过原始期限并完成续租后再结束。

            Args:
                context: 当前结果。
            """
            watch.wait_past(time.monotonic() + 0.3)
            super().item_pipeline(context)

    spider = Slow(
        seeds=[{"url": "https://example.com/1"}, {"url": "https://example.com/2"}],
        concurrency=2,
        lease_seconds=0.3,
    )
    watch = renewals(spider.task_queue)

    def delay(context):
        """有在途交付时延后下一次领取。

        Args:
            context: 领取前上下文。
        """
        if context.task_queue.counts()["leased"]:
            waits.append(1)
            raise signals.Wait(0.02)

    spider.use(hooks.BEFORE_GET_SEEDS, {"func": delay})
    assert spider.run(kwargs={"timeout": 5}).counts["acked"] == 2
    assert waits and len(spider.stored) == 2


@pytest.mark.parametrize("origin", ["hook", "storage"])
def test_claim_timeout_is_not_swallowed_as_an_idle_wait_timeout(origin):
    """领取边界的真实 TimeoutError 原样传播，不被循环等待吞掉。

    Args:
        origin: 错误来自领取前 Hook 或任务存储。
    """
    error = TimeoutError("claim operation timed out")

    class BrokenQueue(MemoryTaskQueue):
        """模拟明确的存储超时。"""

        def claim(self, owner, **kwargs):
            """立即报告存储错误。

            Args:
                owner: 领取方。
                kwargs: 领取参数。

            Raises:
                TimeoutError: 本测试持有的原异常。
            """
            raise error

    queue = BrokenQueue("broken") if origin == "storage" else MemoryTaskQueue("hook")
    spider = Example(seeds=[], task_queue=queue)
    spider.submit({"url": "https://example.com"})

    def fail(context):
        """模拟领取前插件超时。

        Args:
            context: 领取前边界。

        Raises:
            TimeoutError: 本测试持有的原异常。
        """
        raise error

    if origin == "hook":
        spider.use(hooks.BEFORE_GET_SEEDS, {"func": fail})
    with pytest.raises(TimeoutError) as caught:
        spider.run("spider", kwargs={"timeout": 0.5})
    assert caught.value is error
    assert spider.report().counts["pending"] == 1


def test_slot_sessions_cookies_and_fixed_proxy_survive_signal_retries(server):
    """两个并发逻辑任务在真实 HTTP 代理上重试，始终保留各自 Slot 和 Cookie。

    Args:
        server: 充当本地 HTTP 代理端点的测试服务。
    """
    proxy_url, received, _ = server
    gate = Barrier(2)
    trace = {}
    factory = CurlCffiDownloader(trust_env=False)

    class SessionSpider(Example):
        """使用调用方准备的两个独立会话槽。"""

        def build_download_node(self):
            """读取已装配会话。

            Returns:
                基于当前 Slot 的下载节点。
            """
            return DownloadNode(lambda slot: slot["downloaders"])

    with ExitStack() as resources:
        slots = []
        for identity in ("A", "B"):
            session = factory.prepare_session(proxy=proxy_url)
            session.client.cookies.set("account", identity, domain="air-signal.invalid")
            resources.callback(factory.close_session, session)
            slots.append(Slot({"downloaders": {"default": session}}))
        pool = SlotPool(slots=slots)
        resources.callback(pool.close)
        spider = SessionSpider(
            seeds=[
                {"url": "http://air-signal.invalid/one"},
                {"url": "http://air-signal.invalid/two"},
            ],
            concurrency=2,
            slots=pool,
        )

        def start(context):
            """确保两项交付同时占用不同槽。

            Args:
                context: 下载前上下文。
            """
            if context.attempt == 0:
                gate.wait(3)

        def retry(context):
            """记录当前资源并请求两次重试。

            Args:
                context: 当前响应上下文。
            """
            session = context.slot["downloaders"]["default"]
            trace.setdefault(context.seeds["url"], []).append(
                (context.slot.id, id(session), context.response.json()["cookie"])
            )
            if context.attempt < 3:
                raise signals.Retry()

        spider.use(hooks.BEFORE_REQUEST, {"func": start})
        spider.use(hooks.AFTER_REQUEST, {"func": retry})
        assert spider.run(kwargs={"timeout": 8}).counts["acked"] == 2
        assert len(received) == 6 and pool.available == 2
        assert all(
            len(values) == 3 and len(set(values)) == 1 for values in trace.values()
        )
        assert {values[0][2] for values in trace.values()} == {"account=A", "account=B"}


def test_unsupported_hook_and_wait_are_rejected_eagerly():
    """不支持的阶段在 use 时拒绝，Wait(None) 不作为有效延迟。"""
    spider = Example()
    with pytest.raises(ValueError, match="unsupported Air hook stage"):
        spider.use("BEFORE_WORKER_START", {"func": lambda context: None})
    with pytest.raises(ValueError):
        signals.Wait(None)
