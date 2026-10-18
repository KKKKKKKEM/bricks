"""使用两张 Graph 和显式任务队列装配的代码式爬虫基类。"""

from __future__ import annotations

import inspect
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from threading import Event as ThreadEvent
from threading import RLock
from typing import Any, cast

from interlace import Context as EngineContext
from interlace import Graph, Runtime
from interlace.engine import Execution, ExecutionPlan, Slot
from interlace.engine.errors import (
    ExecutionCancelledError,
    ExecutionControlError,
)
from interlace.spi import (
    Delivery,
    DeliveryResult,
    ExecutionLimits,
    SlotProvider,
    Work,
)

from bricks import (
    AsyncDownloader,
    AsyncDownloadNode,
    Downloader,
    DownloadNode,
    Items,
    Request,
    signals,
)
from bricks import hooks as hook_api
from bricks.downloaders import SessionDownloader
from bricks.downloaders.curl_cffi import CurlCffiDownloader
from bricks.hooks import Binding, Task
from bricks.plugins import on_request
from bricks.proxies import Manager, ProxyProvider, format_proxy
from bricks.queues import (
    InitLease,
    InitRecord,
    MemoryTaskQueue,
    QueueBackend,
    TaskLease,
    TaskQueue,
    make_work,
)
from bricks.queues.models import seconds, text

from . import context as domain
from ._calls import external
from ._graphs import GraphAssembly
from ._hooks import run_hooks
from ._initialization import run_initialization
from ._nodes import HookNode
from ._resources import close_preserving, open_resources, release_slot_resources
from .context import (
    Context,
    HookContext,
    InitContext,
)

__all__ = ["Context", "InitContext", "HookContext", "RunReport", "Spider"]


@dataclass(frozen=True)
class RunReport:
    """当前队列的运行快照，不替代分布式 Worker 的错误收集。

    Attributes:
        initialization: 当前初始化记录，未初始化时为 None。
        counts: pending、leased、acked 和 dead 数量，调用方拥有独立副本。
    """

    initialization: InitRecord | None
    counts: dict[str, int]


def _extract_attribute(context: Context, name: str) -> Any:
    """读取 survey 字段；提前成功时尚未生成的领域数据记为 None。

    Args:
        context: 已完成任务的上下文。
        name: 要读取的属性名。

    Returns:
        属性值，或尚不可用时的 None。
    """
    try:
        return getattr(context, name)
    except RuntimeError:
        return None


class Spider:
    """装配种子初始化图和单种子消费图，业务状态通过 Output 传递。

    Attributes:
        task_queue: 注入或自建的种子存储，Redis 客户端仍归调用方管理。
        queue_name: 稳定的队列及图名称前缀，分布式 Worker 必须一致。
        concurrency: 当前进程消费并发上限，默认 1。
        downloader: 借用的下载器，默认临时 curl_cffi 下载。
        proxy: 固定地址或按 Slot 管理的 Manager；供应源和客户端归调用方管理。
        forever: 队列为空时是否持续消费，默认 False。
        _slots: 当前进程的 Slot 提供器，外部注入时不负责关闭。
        _own_slots: 是否由本实例创建 SlotPool。
        _owned_proxy_slots: 自建 Slot 列表，停止执行后归还其中的托管代理。
        _options: 构造时复制的默认 JSON 执行配置。
        _limits: 每个消费执行的限制。
        _init_timeout: 单次初始化执行超时秒数，None 不限制。
        _ttl: 初始化租约及消费交付租约的秒数。
        _max_retries: 下载失败后的额外尝试上限。
        _retry_delay: 下载重试等待秒数。
        _hooks: 当前实例在启动前登记的回调。
        _installed: 是否已经完成一次 install，失败时回滚本次注册。
        _hook_tasks: 打开时固定的各阶段声明，不通过 Runtime.attach 装配业务 Hook。
        _hooks_ready: 是否已执行过 install 并固定插槽声明；之前构造的图会缺少业务 Hook。
        _start_allowed: BEFORE_START 是否允许启动初始化和消费。
        _runtime: 自有 Runtime，尚未打开时为 None。
        _backend: 自有队列消费适配器，存储由调用方管理。
        _auto_session: 是否由框架为每个自建 Slot 装配同步下载会话。
        _poll_interval: QueueBackend 领取循环的轮询间隔秒数。
        _owned_sessions: 框架创建并在执行停止后关闭的会话及其工厂。
        _init_execution: 当前初始化执行，便于 close 协作式取消。
        _operation: 串行化公开运行入口，不限制各种子执行并发。
        _lifecycle: 保护启动、关闭及运行资源引用。
        _close_lock: 串行化关闭并等待资源清理完成。
        _stopping: 通知运行入口退出的标记。
        _state: new、opening、open、closing 或 closed，本实例关闭后不可重开；
            opening 表示插槽已固定、BEFORE_START 及资源装配进行中，打开失败时回到 new。
    """

    Context: type[domain.Context] = (
        Context  # 子类可替换消费上下文类型，并实现自己的 make 方法。
    )
    InitContext: type[domain.InitContext] = InitContext  # 子类可替换初始化上下文类型。

    def __init__(
        self,
        *,
        concurrency: int = 1,
        downloader: Downloader | AsyncDownloader | None = None,
        task_queue: TaskQueue | None = None,
        queue_name: str | None = None,
        proxy: str | ProxyProvider | Manager | None = None,
        forever: bool = False,
        slots: SlotProvider | None = None,
        options: Mapping[str, Any] | None = None,
        limits: ExecutionLimits | None = None,
        max_retry: int = 2,
        retry_delay: float = 0,
        lease_seconds: float = 30,
        init_timeout: float | None = None,
        poll_interval: float = 0.05,
        slot_session_reuse: bool = True,
    ) -> None:
        """沿用业务参数命名，构造时不启动消费者或生成种子。

        Args:
            concurrency: 当前进程消费并发，默认 1。
            downloader: 注入的同步或异步下载器实例，默认 curl_cffi。
            task_queue: 已装配的任务存储，缺省创建独立内存队列。
            queue_name: 队列及图名称前缀，提供队列时必须与其 name 一致。
            proxy: 固定地址、供应源或 Manager，Request 的显式代理优先。
            forever: 队列暂时为空时是否持续消费，默认 False。
            slots: 借用的本进程 Slot 资源池。
            options: 默认 JSON 执行配置。
            limits: 每个消费 Graph 的执行限制。
            max_retry: 首次下载后的额外尝试上限，默认 2。
            retry_delay: 下载重试等待秒数，默认 0。
            lease_seconds: 初始化和交付租约秒数，默认 30。
            poll_interval: 队列轮询间隔秒数，默认 0.05。
            slot_session_reuse: 是否为自建 Slot 自动复用同步会话，默认 True。

        Raises:
            TypeError: 参数不符合公开协议。
            ValueError: 名称、并发、重试次数或时间非法。
        """
        default_name = (
            task_queue.name
            if task_queue is not None
            else f"{type(self).__module__}.{type(self).__name__}"
        )
        self.queue_name = text(
            default_name if queue_name is None else queue_name, "queue_name"
        )
        if task_queue is not None and task_queue.name != self.queue_name:
            raise ValueError("queue_name must match task_queue.name")
        if type(concurrency) is not int or concurrency < 1:
            raise ValueError("concurrency must be a positive integer")
        if type(max_retry) is not int or max_retry < 0:
            raise ValueError("max_retry must be a nonnegative integer")
        if type(forever) is not bool:
            raise TypeError("forever must be boolean")
        if type(slot_session_reuse) is not bool:
            raise TypeError("slot_session_reuse must be boolean")
        if isinstance(proxy, str):
            proxy = format_proxy(proxy)
        elif proxy is not None and not isinstance(proxy, Manager):
            if isinstance(proxy, type) or not isinstance(proxy, ProxyProvider):
                raise TypeError(
                    "proxy must be an address, ProxyProvider, Manager or None"
                )
            proxy = Manager(proxy)
        if slots is not None and not isinstance(slots, SlotProvider):
            raise TypeError("slots must implement SlotProvider")
        self.downloader = CurlCffiDownloader() if downloader is None else downloader
        if isinstance(self.downloader, type) or not callable(
            getattr(self.downloader, "fetch", None)
        ):
            raise TypeError("downloader must be an instance with fetch")
        validated = make_work(self.crawl_graph_name, {}, options=options, limits=limits)
        self.task_queue = (
            MemoryTaskQueue(self.queue_name) if task_queue is None else task_queue
        )
        self.concurrency = concurrency
        self.proxy: str | Manager | None = proxy
        self.forever = forever
        self._slots = slots
        self._own_slots = slots is None
        self._auto_session = (
            slots is None
            and slot_session_reuse
            and not inspect.iscoroutinefunction(self.downloader.fetch)
            and isinstance(self.downloader, SessionDownloader)
            and getattr(self.downloader, "slot_session_reuse", True) is True
        )
        self._owned_sessions: list[tuple[SessionDownloader[Any], Downloader]] = []
        self._owned_proxy_slots: list[Slot] = []
        self._options = dict(validated.options)
        self._limits = validated.limits
        self._init_timeout = None if init_timeout is None else seconds(init_timeout)
        self._ttl = seconds(lease_seconds)
        self._max_retries = max_retry
        self._retry_delay = seconds(retry_delay, positive=False)
        self._poll_interval = seconds(poll_interval)
        self._hooks: list[Binding] = []
        self._installed = False
        self._hook_tasks: dict[str, tuple[Task, ...]] = {}
        self._hooks_ready = False
        self._start_allowed = True
        self._runtime: Runtime | None = None
        self._backend: QueueBackend | None = None
        self._init_execution: Execution | None = None
        self._operation = RLock()
        self._lifecycle = RLock()
        self._close_lock = RLock()
        self._stopping = ThreadEvent()
        self._state = "new"
        self._survey_results: list[Context] | None = (
            None  # 手动消费的完成结果，仅本实例持有。
        )

    @property
    def init_graph_name(self) -> str:
        """返回初始化图的稳定名称。

        Returns:
            名称前缀加 .init。
        """
        return f"{self.queue_name}.init"

    @property
    def crawl_graph_name(self) -> str:
        """返回消费图的稳定名称。

        Returns:
            名称前缀加 .crawl。
        """
        return f"{self.queue_name}.crawl"

    @property
    def local_submission_event_name(self) -> str:
        """返回当前实例用于持久化 Slot 亲和子任务的事件类型。

        Returns:
            与队列名称绑定的本地提交事件类型。
        """
        return f"bricks.air.continue:{self.queue_name}"

    @property
    def runtime(self) -> Runtime:
        """取得已打开的 Runtime，供应用显式装配观察者或额外业务图。

        Returns:
            当前实例拥有的 Runtime。

        Raises:
            RuntimeError: 尚未打开或已经关闭。
        """
        if self._runtime is None or self._state != "open":
            raise RuntimeError("spider runtime is not open")
        return self._runtime

    def make_seeds(self, context: domain.InitContext, **kwargs: Any) -> Any:
        """生成本轮初始种子，允许返回同步迭代器或等待该迭代器的 Awaitable。

        Args:
            context: 带恢复断点的初始化上下文。
            kwargs: 包含上次保存的 record。

        Raises:
            NotImplementedError: 子类尚未实现种子源。
        """
        raise NotImplementedError("implement make_seeds")

    def make_request(self, context: domain.Context) -> Any:
        """将当前种子转换为明确的 Request。

        Args:
            context: 当前种子上下文。

        Returns:
            以 seeds 作为显式 Request 构造参数的请求。

        Raises:
            TypeError: seeds 包含不属于 Request 构造器的字段。
        """
        return Request(**context.seeds)

    def parse(self, context: domain.Context) -> Any:
        """解析当前响应，返回明确的 Items。

        Args:
            context: 已有响应的当前上下文。

        Returns:
            默认请求和响应快照；子类可返回 Items 或 list[dict]。
        """
        return Items(
            {
                "request": context.request.to_curl(),
                "response": {
                    "url": context.response.url,
                    "status_code": context.response.status_code,
                    "text": context.response.text,
                },
            }
        )

    def item_pipeline(self, context: domain.Context) -> Any:
        """默认不执行存储，正常返回后继续 AFTER_PIPELINE 和完成节点。

        Args:
            context: 已有 Items 的当前上下文。

        Returns:
            None；实际持久化由子类或存储插件实现。
        """
        return None

    def _make_work(
        self,
        seed: dict[str, Any],
        *,
        id: str | None = None,
        options: Mapping[str, Any] | None = None,
    ) -> Work:
        """校验种子并构造包含默认配置的稳定 Work。

        Args:
            seed: JSON 种子对象。
            id: 可选业务 ID。
            options: 覆盖默认配置的选项。
        Returns:
            尚未提交的独立 Work。

        Raises:
            TypeError: 种子或配置类型非法。
        """
        if type(seed) is not dict:
            raise TypeError("seed must be a JSON object")
        if options is not None and not isinstance(options, Mapping):
            raise TypeError("options must be a mapping")
        return make_work(
            self.crawl_graph_name,
            seed,
            id=id,
            limits=self._limits,
            options={**self._options, **({} if options is None else options)},
        )

    def _submit(
        self,
        seed: dict[str, Any],
        *,
        id: str | None = None,
        options: Mapping[str, Any] | None = None,
        init: InitLease | None = None,
    ) -> Work:
        """构造并持久化独立种子。

        Args:
            seed: JSON 种子对象。
            id: 可选业务 ID。
            options: 覆盖默认配置的选项。
            init: 初始化图提交时附加的生产权凭据。

        Returns:
            已接受或去重命中的 Work。
        """
        work = self._make_work(seed, id=id, options=options)
        self.task_queue.put(work, init=init)
        return work

    def submit(
        self,
        seed: dict[str, Any],
        *,
        id: str | None = None,
        options: Mapping[str, Any] | None = None,
    ) -> Work:
        """在运行前或运行中显式提交种子。

        Args:
            seed: JSON 种子。
            id: 可选稳定 ID。
            options: 覆盖默认配置的选项。

        Returns:
            已接受或去重命中的 Work。

        Raises:
            RuntimeError: 当前实例正在关闭或已经关闭。
        """
        with self._lifecycle:
            if self._state in ("closing", "closed"):
                raise RuntimeError("spider is closed")
            return self._submit(seed, id=id, options=options)

    def _crawl_context(
        self, base: EngineContext, inputs: Mapping[str, Any]
    ) -> domain.Context:
        """调用领域工厂，不复制基础上下文的内部状态。

        Args:
            base: 引擎基础 Context。
            inputs: 本次端口输入。

        Returns:
            本次独立的 Context。

        Raises:
            TypeError: 工厂返回类型错误。
        """
        context = self.Context.make(base, inputs, self)
        if not isinstance(context, Context):
            if inspect.iscoroutine(context):
                context.close()
            raise TypeError("Spider.Context.make must return an Air Context")
        context._input = inputs
        return context

    def _init_context(
        self, base: EngineContext, inputs: Mapping[str, Any]
    ) -> domain.InitContext:
        """为当前初始化步骤构造带生产权的 Context。

        Args:
            base: 引擎基础 Context。
            inputs: 本次初始化状态。

        Returns:
            当前 InitContext。

        Raises:
            TypeError: 工厂返回类型错误。
        """
        state = inputs["initial"]
        context = self.InitContext(target=self, base=base, initial=state)
        if not isinstance(context, InitContext):
            if inspect.iscoroutine(context):
                context.close()
            raise TypeError("Spider.InitContext must construct an Air InitContext")
        return context

    def _put_initial(self, context: domain.InitContext) -> None:
        """提交一项种子后保存对应断点，回放使用稳定 ID 去重。

        Args:
            context: 当前初始化上下文。
        """
        state = context._initial
        assert state.seed is not None
        batch = state.seed if isinstance(state.seed, list) else [state.seed]
        context.submit(*batch)
        state.produced += len(batch)

    def build_graphs(self) -> tuple[Graph, Graph]:
        """构造初始化图和消费图。

        Returns:
            初始化图与所选消费分支装配的队列消费图。

        Raises:
            RuntimeError: 尚未打开，插件尚未安装且插槽未固定。
        """
        return GraphAssembly(self).build_graphs()

    def _hook_node(self, form: str, **options: Any) -> HookNode:
        """按装配时固定的声明创建独立业务插槽。

        Args:
            form: 插槽名称。
            options: 初始化、入口和合法路由标志。

        Returns:
            不保存执行状态的 HookNode。

        Raises:
            RuntimeError: 尚未打开，插件尚未安装且插槽未固定。
        """
        if not self._hooks_ready:
            # install 只在打开时执行一次；此前构造的图会静默缺少全部业务 Hook。
            raise RuntimeError("open the spider before building graphs")
        return HookNode(self, form, self._hook_tasks.get(form, ()), **options)

    def build_init_graph(self) -> Graph:
        """构造种子初始化图。

        Returns:
            种子生产、入队与断点提交的初始化图。

        Raises:
            RuntimeError: 尚未打开，插件尚未安装且插槽未固定。
        """
        return GraphAssembly(self).build_init_graph()

    def build_crawl_plan(self, *, survey: bool = False) -> ExecutionPlan:
        """选择正式消费或手动 survey 的执行计划。

        Args:
            survey: 是否选择无默认存储的手动分支。

        Returns:
            校验完整出口的消费计划。

        Raises:
            RuntimeError: 尚未打开，插件尚未安装且插槽未固定。
        """
        return GraphAssembly(self).build_crawl_plan(survey=survey)

    @staticmethod
    def _survey_pipeline(context: domain.Context) -> None:
        """手动消费默认不调用业务存储，仍经过正常前后插槽。

        Args:
            context: 当前已经完成解析的上下文。
        """

    def _collect_survey(self, context: domain.Context) -> None:
        """在完成节点收集最终上下文，不提前确认队列任务。

        Args:
            context: 已执行后置插件或明确提前成功的本次上下文。
        """
        with self._lifecycle:
            if self._survey_results is not None:
                self._survey_results.append(context)

    def build_download_node(self) -> DownloadNode | AsyncDownloadNode:
        """按下载器能力自动选择临时或 Slot Session 节点。

        Returns:
            同步 SessionDownloader 使用 Slot resolver，其他实现使用固定映射。
        """
        if inspect.iscoroutinefunction(self.downloader.fetch):
            return AsyncDownloadNode(
                {"default": cast(AsyncDownloader, self.downloader)}
            )
        if self._auto_session:
            return DownloadNode(
                lambda slot: slot["crawler.downloaders"],
                isolated_downloaders={"default": cast(Downloader, self.downloader)},
            )
        return DownloadNode({"default": cast(Downloader, self.downloader)})

    def _make_seeds(self, context: domain.InitContext) -> Any:
        """把恢复记录按 make_seeds(context, **kwargs) 契约传给业务方法。

        Args:
            context: 本轮初始化上下文。

        Returns:
            业务方法返回的种子源或 Awaitable。
        """
        return self.make_seeds(context, record=context.record)

    def on_retry(self, context: domain.Context) -> Any:
        """供业务在重试前调整请求，调度与预算仍由图处理。

        Args:
            context: 含上一次响应的上下文。

        Returns:
            None。
        """
        return None

    def use(self, form: str, *events: Task | dict[str, Any]) -> None:
        """沿用 use(form, *events) 注册可复用插件调用。

        Args:
            form: 阶段名称，例如 hooks.AFTER_REQUEST。
            events: Task 或包含 func/args/kwargs/index/match 的字典。

        Raises:
            RuntimeError: 已经打开或关闭。
            TypeError: 声明不符合 Task 契约。
            ValueError: 阶段名称非法。
        """
        with self._lifecycle:
            if self._state != "new":
                # opening 状态下插槽已固定，BEFORE_START 内注册不会生效，必须明确失败。
                raise RuntimeError("plugins must be registered before opening")
            form = text(form, "form")
            supported = {
                hook_api.BEFORE_MAKE_SEEDS,
                hook_api.AFTER_MAKE_SEEDS,
                hook_api.BEFORE_PUT_SEEDS,
                hook_api.AFTER_PUT_SEEDS,
                hook_api.BEFORE_MAKE_REQUEST,
                hook_api.AFTER_MAKE_REQUEST,
                hook_api.BEFORE_REQUEST,
                hook_api.AFTER_REQUEST,
                hook_api.BEFORE_RETRY,
                hook_api.AFTER_RETRY,
                hook_api.BEFORE_PARSE,
                hook_api.AFTER_PARSE,
                hook_api.BEFORE_PIPELINE,
                hook_api.AFTER_PIPELINE,
                hook_api.BEFORE_START,
                hook_api.BEFORE_CLOSE,
                hook_api.ERROR_OCCURRED,
                hook_api.BEFORE_GET_SEEDS,
                hook_api.AFTER_GET_SEEDS,
            }
            if form not in supported:
                raise ValueError(f"unsupported Air hook stage: {form}")
            registrations = []
            for event in events:
                task = Task(**event) if isinstance(event, dict) else event
                if not isinstance(task, Task):
                    raise TypeError("events must be Task or plugin dictionaries")
                registrations.append(Binding(form, task))
            self._hooks.extend(registrations)

    def install(self) -> None:
        """注册可复用的代理、响应展示和成功检查插件，子类可扩展或替换。"""
        self.use(
            hook_api.BEFORE_REQUEST,
            {"func": on_request.Before.fake_ua, "index": -100},
            {"func": on_request.Before.set_proxy},
        )
        self.use(
            hook_api.AFTER_REQUEST,
            {"func": on_request.After.show_response, "index": -100},
            {"func": on_request.After.conditional_scripts},
            {"func": on_request.After.bypass, "index": 1000},
        )

    def _prepare_hooks(self) -> None:
        """固定支持的插槽和调用顺序，未实现的物理线程插槽在启动时失败。

        Raises:
            ValueError: 阶段没有实际执行位置。
            TypeError: 非图边界注册了异步函数。
        """
        graph_forms = {
            hook_api.BEFORE_MAKE_SEEDS,
            hook_api.AFTER_MAKE_SEEDS,
            hook_api.BEFORE_PUT_SEEDS,
            hook_api.AFTER_PUT_SEEDS,
            hook_api.BEFORE_MAKE_REQUEST,
            hook_api.AFTER_MAKE_REQUEST,
            hook_api.BEFORE_REQUEST,
            hook_api.AFTER_REQUEST,
            hook_api.BEFORE_RETRY,
            hook_api.AFTER_RETRY,
            hook_api.BEFORE_PARSE,
            hook_api.AFTER_PARSE,
            hook_api.BEFORE_PIPELINE,
            hook_api.AFTER_PIPELINE,
        }
        boundary_forms = {
            hook_api.BEFORE_START,
            hook_api.BEFORE_CLOSE,
            hook_api.ERROR_OCCURRED,
            hook_api.BEFORE_GET_SEEDS,
            hook_api.AFTER_GET_SEEDS,
        }
        groups: dict[str, list[Task]] = {}
        for binding in (*hook_api.collect(self), *self._hooks):
            if binding.form not in graph_forms | boundary_forms:
                raise ValueError(f"unsupported Air hook stage: {binding.form}")
            callback = binding.task.func
            if binding.form in boundary_forms and any(
                inspect.iscoroutinefunction(value) or inspect.isasyncgenfunction(value)
                # 可调用实例的异步 __call__ 不被 iscoroutinefunction 识别，需单独检查。
                for value in (callback, getattr(callback, "__call__", None))  # noqa: B004
            ):
                raise TypeError(f"{binding.form} requires synchronous hooks")
            groups.setdefault(binding.form, []).append(binding.task)
        self._hook_tasks = {
            form: tuple(
                sorted(tasks, key=lambda task: 0 if task.index is None else task.index)
            )
            for form, tasks in groups.items()
        }

    def _report_error(
        self,
        source: domain.Context | domain.InitContext | HookContext,
        error: BaseException,
    ) -> None:
        """通知错误插槽并保留原错误；诊断回调不能恢复或替换执行失败。

        Args:
            source: 发生异常的原上下文。
            error: 原异常。

        Raises:
            BaseException: 错误回调再次失败时，以原错误为主异常保留原因链。
        """
        if isinstance(error, ExecutionControlError):
            return
        context = HookContext(self, hook_api.ERROR_OCCURRED, error=error, source=source)
        try:
            run_hooks(
                self,
                self._hook_tasks.get(hook_api.ERROR_OCCURRED, ()),
                context,
                allow_await=False,
            )
        except signals.Pass:
            return
        except BaseException as secondary:
            # 原错误已有 cause 时保留，否则以错误回调的二次异常作为 cause。
            cause = secondary if error.__cause__ is None else error.__cause__
            raise error from cause

    def _boundary(self, context: HookContext) -> None:
        """在真实生命周期或领取边界运行同步插槽。

        Args:
            context: 当前非图上下文。

        Raises:
            BaseException: 除 Pass 外的信号交给对应边界，普通错误继续传播。
        """
        try:
            run_hooks(
                self, self._hook_tasks.get(context.form, ()), context, allow_await=False
            )
        except signals.Pass:
            return
        except Exception as error:
            self._report_error(context, error)
            raise

    def _before_claim(self, queue: TaskQueue) -> float | None:
        """在领取前调用插槽，Wait 延后轮询而不停止在途续租。

        Args:
            queue: 本次领取的借用存储。

        Returns:
            None 表示可以领取，非负秒数表示延后本次领取。

        Raises:
            TypeError: 无任务的边界使用了任务级控制信号。
        """
        del queue
        context = HookContext(self, hook_api.BEFORE_GET_SEEDS)
        try:
            self._boundary(context)
        except signals.Wait as signal:
            return signal.duration
        except signals.Skip:
            return 0.05
        except signals.Signal as signal:
            raise TypeError(
                f"{type(signal).__name__} is not allowed before claiming"
            ) from signal
        return None

    def _after_claim(
        self, lease: TaskLease, delivery: Delivery
    ) -> DeliveryResult | None:
        """在已领取且获得 Slot 的交付边界运行 Hook，然后进入消费图。

        Args:
            lease: 当前存储交付凭据。
            delivery: Interlace 提供的公开本地交付。

        Returns:
            None 继续图执行；Success/Failure 显式终结本次交付。

        Raises:
            TypeError: 领取后使用不支持的信号。
        """
        context = HookContext(
            self,
            hook_api.AFTER_GET_SEEDS,
            lease=replace(lease, work=delivery.work),
            slot=None if delivery.slot_lease is None else delivery.slot_lease.slot,
        )
        try:
            self._boundary(context)
        except signals.Success:
            return DeliveryResult.ack()
        except signals.Failure as signal:
            error = domain.ResponseRejectedError(str(signal) or "claimed seed rejected")
            self._report_error(context, error)
            return DeliveryResult.reject(error)
        except signals.Signal as signal:
            raise TypeError(
                f"{type(signal).__name__} is not allowed after claiming"
            ) from signal
        return None

    def open(self) -> Spider:
        """装配 Runtime、两张图及回调，尚不启动消费。

        Returns:
            当前实例。

        Raises:
            RuntimeError: 已经关闭或从自身回调重入。
            BaseException: 图或资源装配失败，会关闭本次已创建的资源。
        """
        external(self)
        with self._lifecycle:
            if self._state == "open":
                return self
            if self._state != "new":
                raise RuntimeError("spider is closed")
            if not self._installed:
                original = list(self._hooks)
                try:
                    self.install()
                except BaseException:
                    self._hooks = original
                    raise
                self._installed = True
            self._prepare_hooks()
            self._hooks_ready = True
            self._state = "opening"
            try:
                try:
                    self._boundary(HookContext(self, hook_api.BEFORE_START))
                except signals.Success:
                    self._start_allowed = False
                except signals.Failure as signal:
                    raise domain.ResponseRejectedError(
                        str(signal) or "Spider startup rejected"
                    ) from signal
                except signals.Signal as signal:
                    raise TypeError(
                        f"{type(signal).__name__} is not allowed in BEFORE_START"
                    ) from signal
                initial, crawl = self.build_graphs()
                open_resources(self, initial, crawl)
            except BaseException:
                self._state = "new"
                raise
        return self

    def _consume(self) -> None:
        """启动当前实例的消费者，同一配置重复调用保持幂等。"""
        with self._lifecycle:
            self.runtime.consume(
                self.task_queue.name, concurrency=self.concurrency, slots=self._slots
            )

    def _run_init(self) -> InitRecord | None:
        """申请生产权并驱动初始化图。"""
        return run_initialization(self)

    def run_init(self) -> InitRecord | None:
        """只运行初始化；资源保留供后续消费或上下文退出关闭。

        Returns:
            当前初始化记录。
        """
        external(self)
        with self._operation:
            self.open()
            if not self._start_allowed:
                return self.task_queue.initialization()
            return self._run_init()

    def report(self) -> RunReport:
        """读取初始化与任务交付状态，关闭后仍可查询借用存储。

        Returns:
            当前队列的独立快照。
        """
        return RunReport(self.task_queue.initialization(), self.task_queue.counts())

    def run_spider(self, *, timeout: float | None = None) -> RunReport:
        """消费已有种子直到当前队列排空，不隐式生成初始种子。

        Args:
            timeout: 等待排空的秒数，None 不限制；等待超时不会强杀业务函数。

        Returns:
            当前队列状态。

        Raises:
            BaseException: 消费执行、等待或存储失败。
        """
        external(self)
        with self._operation:
            self.open()
            if not self._start_allowed:
                return self.report()
            self._consume()
            self._wait_idle(timeout)
            return self.report()

    def survey(
        self,
        *seeds: dict[str, Any],
        extract: Sequence[str] | None = None,
        timeout: float | None = None,
    ) -> list[Any]:
        """在新实例中手动消费种子，跳过默认存储并收集完成上下文。

        请求、重试、解析、前后插件和动态提交保持普通消费行为。默认实例使用
        独立内存队列；显式注入队列时也会消费其原有任务。结束后关闭本实例，
        不调用 make_seeds，不提供无副作用或事务回滚保证。

        Args:
            seeds: 显式 JSON 种子，可为零条以消费已装配队列。
            extract: 返回 Context 的指定属性映射；None 或空序列返回 Context。
            timeout: 等待排空的秒数，None 不限制。

        Returns:
            按完成顺序收集的上下文或属性映射，包含动态提交后消费完成的种子。
            返回的 Context 仅用于读取结果，所属执行和自建资源已经关闭。

        Raises:
            RuntimeError: 实例已打开、已关闭或从自身回调重入。
            TypeError: 种子、提取字段或超时类型不正确。
            ValueError: forever=True 或字段为空。
            BaseException: 消费、提取或资源清理失败。
        """
        external(self)
        if isinstance(extract, (str, bytes)) or (
            extract is not None and not isinstance(extract, Sequence)
        ):
            raise TypeError("extract must be a sequence of attribute names")
        fields = tuple(
            text(name, "extract field") for name in (() if extract is None else extract)
        )
        if timeout is not None:
            seconds(timeout, positive=False)
        # 先校验整批输入，避免后面的非法种子导致前面仅部分入队。
        for seed in seeds:
            if type(seed) is not dict:
                raise TypeError("seeds must be JSON objects")
            make_work(
                self.crawl_graph_name, seed, options=self._options, limits=self._limits
            )
        with self._operation:
            with self._lifecycle:
                if self._state != "new":
                    raise RuntimeError("survey requires a new Spider instance")
                if self.forever:
                    raise ValueError("survey requires forever=False")
                results: list[Context] = []
                self._survey_results = results
            primary: BaseException | None = None
            try:
                self.open()
                if self._start_allowed:
                    for seed in seeds:
                        self.submit(seed)
                    self.run_spider(timeout=timeout)
            except BaseException as exc:
                primary = exc
                raise
            finally:
                close_preserving(self, primary)
            return (
                [
                    {name: _extract_attribute(context, name) for name in fields}
                    for context in results
                ]
                if fields
                else list(results)
            )

    def _wait_idle(self, timeout: float | None) -> None:
        """按总预算等待队列静止，保留真实存储和插槽超时。

        Args:
            timeout: 总等待秒数，None 不限制。

        Raises:
            ExecutionCancelledError: 外部正在关闭 Spider。
            TimeoutError: 总等待超时，或原始存储、插槽超时。
            BaseException: 执行和存储失败原样传播。
        """
        deadline = (
            None
            if timeout is None
            else time.monotonic() + seconds(timeout, positive=False)
        )
        while True:
            if self._stopping.is_set():
                raise ExecutionCancelledError("spider is stopping")
            remaining = (
                None if deadline is None else max(0, deadline - time.monotonic())
            )
            # 使用总等待预算，原生存储/插槽 TimeoutError 不能被当成轮询超时吞掉。
            self.runtime.wait_idle(remaining)
            if self._stopping.is_set():
                raise ExecutionCancelledError("spider is stopping")
            if not self.forever:
                return
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("continuous spider wait timed out")
            self._stopping.wait(0.05 if remaining is None else min(0.05, remaining))

    def run(
        self,
        task_name: str = "all",
        args: list[Any] | tuple[Any, ...] | None = None,
        kwargs: Mapping[str, Any] | None = None,
    ) -> Any:
        """沿用 run(task_name, args, kwargs) 分派运行入口，结束后关闭资源。

        Args:
            task_name: all、init 或 spider。
            args: 传给对应运行方法的位置参数。
            kwargs: 传给对应运行方法的关键字参数。

        Returns:
            对应运行方法的结果。

        Raises:
            ValueError: 运行模式不受支持。
            TypeError: 参数容器类型错误。
        """
        external(self)
        methods: dict[str, Callable[..., Any]] = {
            "all": self.run_all,
            "init": self.run_init,
            "spider": self.run_spider,
        }
        if task_name not in methods:
            raise ValueError("task_name must be all, init or spider")
        if args is not None and not isinstance(args, (tuple, list)):
            raise TypeError("args must be a list or tuple")
        if kwargs is not None and not isinstance(kwargs, Mapping):
            raise TypeError("kwargs must be a mapping")
        primary: BaseException | None = None
        try:
            return methods[task_name](
                *(() if args is None else args), **({} if kwargs is None else kwargs)
            )
        except BaseException as exc:
            primary = exc
            raise
        finally:
            close_preserving(self, primary)

    def run_all(self, *, timeout: float | None = None) -> RunReport:
        """先启动消费，再生产种子，排空后关闭本实例资源。

        Args:
            timeout: 等待其他初始化者及消费排空的最大秒数，None 不限制。

        Returns:
            初始化结束且队列排空时的快照。

        Raises:
            BaseException: 初始化或消费失败，已接受的种子仍可排空，原失败继续传播。
        """
        external(self)
        if timeout is not None:
            seconds(timeout, positive=False)
        with self._operation:
            failure: BaseException | None = None
            primary: BaseException | None = None
            try:
                self.open()
                if not self._start_allowed:
                    return self.report()
                self._consume()
                deadline = None if timeout is None else time.monotonic() + timeout
                initial: InitRecord | None = None
                try:
                    initial = self._run_init()
                    while initial is not None and initial.status == "running":
                        if self._stopping.wait(0.05):
                            raise ExecutionCancelledError("spider is stopping")
                        if deadline is not None and time.monotonic() >= deadline:
                            raise TimeoutError("waiting for initialization timed out")
                        initial = self._run_init()
                    if initial is not None and initial.status == "failed":
                        raise RuntimeError(f"initialization failed: {initial.error}")
                except Exception as exc:
                    failure = exc
                try:
                    self._wait_idle(
                        None
                        if deadline is None
                        else max(0, deadline - time.monotonic())
                    )
                except BaseException as exc:
                    if failure is None:
                        raise
                    # 初始化失败已有 cause 时保留真实根因，不以消费错误覆盖。
                    if failure.__cause__ is None:
                        raise failure from exc
                    raise failure  # noqa: B904
                if failure is not None:
                    raise failure
                return self.report()
            except BaseException as exc:
                primary = exc
                raise
            finally:
                close_preserving(self, primary)

    def close(self) -> None:
        """停止领取，协作式取消初始化，等待已接受工作并关闭自有资源。

        框架自动创建的同步 Session 在本方法内关闭；注入的 SlotPool、显式会话和
        Redis 客户端由调用方在本方法返回后关闭。downloader 工厂本身不被关闭。

        Raises:
            RuntimeError: 从自身业务回调重入关闭。
            BaseException: 资源关闭失败，其他自有资源仍会尝试关闭。
        """
        external(self)
        failures: list[BaseException] = []
        with self._lifecycle:
            if self._state == "closed":
                return
            self._stopping.set()
            if self._init_execution is not None:
                self._init_execution.cancel()
            stopping_backend = self._backend
        # 先停止领取并排空本地交付，使正在等待 Runtime 静止的运行线程能够退出。
        # 不能先等待 _operation，否则其他生产者留在队列中的任务可能造成关闭死锁。
        if stopping_backend is not None:
            try:
                stopping_backend.close()
            except BaseException as exc:
                failures.append(exc)
        # 先通知，再等待运行入口退出；运行线程自身的 finally 也可以重入 RLock 关闭。
        with self._operation, self._close_lock:
            with self._lifecycle:
                if self._state == "closed":
                    if failures:
                        raise failures[0]
                    return
                self._state = "closing"
                backend, runtime = self._backend, self._runtime
            for resource in (backend, runtime):
                if resource is not None:
                    try:
                        resource.close()
                    except BaseException as exc:
                        failures.append(exc)
            if self._installed:
                try:
                    self._boundary(HookContext(self, hook_api.BEFORE_CLOSE))
                except signals.Success:
                    pass
                except signals.Failure as signal:
                    failures.append(
                        domain.ResponseRejectedError(
                            str(signal) or "Spider close rejected"
                        )
                    )
                except signals.Signal as signal:
                    failures.append(
                        TypeError(
                            f"{type(signal).__name__} is not allowed in BEFORE_CLOSE"
                        )
                    )
                except BaseException as exc:
                    failures.append(exc)
            failures.extend(release_slot_resources(self))
            with self._lifecycle:
                self._state = "closed"
            if failures:
                raise failures[0]

    def __enter__(self) -> Spider:
        """打开资源，允许分开运行生产和消费。

        Returns:
            当前 Spider。
        """
        return self.open()

    def __exit__(
        self, exc_type: Any, exc: BaseException | None, traceback: Any
    ) -> None:
        """关闭资源，块内异常优先，清理错误保留为 cause。

        Args:
            exc_type: 块内异常类型。
            exc: 块内原始异常。
            traceback: 原始异常栈。
        """
        del exc_type
        try:
            self.close()
        except BaseException as cleanup:
            if exc is None:
                raise
            # 块内异常已有 cause 时保留真实根因，不以清理错误覆盖。
            if exc.__cause__ is None:
                raise exc.with_traceback(traceback) from cleanup
            raise exc.with_traceback(traceback)  # noqa: B904
