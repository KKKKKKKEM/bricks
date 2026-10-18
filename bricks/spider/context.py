"""Air 的图内任务载体和领域上下文，执行能力委托给 Interlace。"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, TypeVar

from interlace import Context as EngineContext
from interlace import Event
from interlace.engine import Slot
from interlace.spi import Work

from bricks import Items, Request, Response
from bricks.proxies import Manager
from bricks.queues import InitLease, TaskLease, TaskQueue
from bricks.queues.models import LocalSubmission

if TYPE_CHECKING:
    from .air import Spider


_ContextT = TypeVar("_ContextT", bound="CurrentContext")
_CURRENT_CONTEXT: ContextVar[CurrentContext | None] = ContextVar(
    "bricks.spider.current_context", default=None
)  # 当前同步调用链或异步任务的领域 Context，不跨回调保留。
_RESOURCES = "bricks.context.resources"  # Slot 内由业务按名称共享的资源映射。
Affinity = Literal["current", "any"]


def _affinity(value: Affinity) -> Affinity:
    """校验派生任务的 Slot 亲和策略。

    Args:
        value: current 延续当前 Slot，any 交给任意 Worker 领取。

    Returns:
        已校验的原字符串。

    Raises:
        ValueError: 亲和策略不存在。
    """
    if value not in ("current", "any"):
        raise ValueError("affinity must be current or any")
    return value


class CurrentContext:
    """提供按线程及异步任务隔离的当前领域 Context。"""

    @classmethod
    def get_context(cls: type[_ContextT]) -> _ContextT:
        """取得当前正在执行的同类领域 Context。

        Returns:
            当前回调绑定的消费、初始化或生命周期 Context。

        Raises:
            RuntimeError: 当前不在回调中，或 Context 类别不匹配。
        """
        current = _CURRENT_CONTEXT.get()
        if current is None:
            raise RuntimeError("no domain Context is active")
        if not isinstance(current, cls):
            raise RuntimeError(
                f"current domain Context is {type(current).__name__}, not {cls.__name__}"
            )
        return current


def activate_context(context: CurrentContext) -> Token[CurrentContext | None]:
    """为当前调用链绑定领域 Context。

    Args:
        context: 即将传给业务函数或 Hook 的领域 Context。

    Returns:
        用于精确恢复上一层绑定的 ContextVar token。
    """
    return _CURRENT_CONTEXT.set(context)


def reset_context(token: Token[CurrentContext | None]) -> None:
    """恢复进入当前回调之前的领域 Context。

    Args:
        token: activate_context 返回的绑定凭据。
    """
    _CURRENT_CONTEXT.reset(token)


class ResponseRejectedError(RuntimeError):
    """响应被拒绝或已耗尽下载重试次数。"""


@dataclass
class _Task:
    """当前消费 Graph 内沿 Output 传递的单种子状态。

    Attributes:
        seed: 当前执行独有的 JSON 种子副本。
        request: 已构造的请求，初始 None。
        response: 本次下载响应，初始 None。
        items: 解析结果，初始 None。
        attempt: 已经发起的下载次数，初始 0。
        retries: 已消耗的重试次数，初始零，包括下载前触发的重试。
        retry_delay: 本次重试等待秒数；None 使用 Spider 配置。
    """

    seed: dict[str, Any]
    request: Request | None = None
    response: Response | None = None
    items: Items | None = None
    attempt: int = 0
    retries: int = 0
    retry_delay: float | None = None
    retry_reason: str | None = None  # 最近一次重试触发原因，耗尽时用于诊断。


@dataclass
class Initialization:
    """仅在本进程初始化图中传递的生产状态。

    Attributes:
        lease: 此轮生产权凭据，外层调用负责续租。
        progress: 用户定义的 JSON 断点，在每个种子提交之后保存。
        iterator: 本轮同步种子迭代器，由初始化流程关闭。
        seed: 当前待提交种子，初始 None。
        produced: 本轮已提交的种子数，包含去重命中。
    """

    lease: InitLease
    progress: Any = None
    iterator: Iterator[Any] | None = None
    seed: dict[str, Any] | list[dict[str, Any]] | None = None
    produced: int = 0


@dataclass
class HookContext(CurrentContext):
    """启停、领取和错误插槽的上下文，不伪造 Graph execution。

    Attributes:
        target: 当前 Spider。
        form: 当前边界插槽名称。
        lease: 领取后可见的任务凭据，其他边界为 None；禁止自行确认。
        slot: 本地交付已经分配的 Slot，领取前及启停时为 None。
        error: 当前报告的原异常，非错误插槽为 None。
        source: 发生异常的图内或边界上下文，非错误插槽为 None。
    """

    target: Spider
    form: str
    lease: TaskLease | None = None
    slot: Slot | None = None
    error: BaseException | None = None
    source: Context | InitContext | HookContext | None = None

    @property
    def task_queue(self) -> TaskQueue:
        """取得借用的任务存储。

        Returns:
            当前 Spider 的队列。
        """
        return self.target.task_queue

    @property
    def queue_name(self) -> str:
        """取得当前队列名称。

        Returns:
            队列名称。
        """
        return self.target.queue_name

    @property
    def seeds(self) -> dict[str, Any]:
        """读取已领取任务的本地种子副本，不回写存储。

        Returns:
            当前交付的种子对象。

        Raises:
            RuntimeError: 此边界尚未领取任务。
            TypeError: 任务输入不是种子对象。
        """
        if self.lease is None:
            raise RuntimeError("seeds are unavailable before claiming a task")
        if type(self.lease.work.inputs) is not dict:
            raise TypeError("Air queue input must be a JSON object")
        return self.lease.work.inputs


class Context(CurrentContext, EngineContext):
    """沿用 seeds/request/response/items/target 的消费上下文。

    Attributes:
        target: 当前 Spider，仅保存装配与业务方法，不保存当前请求状态。
        form: 当前业务步骤或插件阶段名称。
        _task: 当前输入携带的图内状态。
        _input: 工厂看到的原始输入，用于识别后续 Hook 的显式替换。
    """

    def __init__(
        self,
        target: Spider,
        *,
        base: EngineContext,
        task: _Task,
        form: str = "ON_CONSUME",
    ) -> None:
        """组合执行能力并绑定当前业务状态。

        Args:
            target: 当前 Spider。
            base: 引擎提供的基础 Context。
            task: 当前图内任务。
            form: 当前阶段名称。
        """
        super().__init__(parent=base)
        self.target = target
        self.form = form
        self._task = task
        self._input: Mapping[str, Any] = {}

    @classmethod
    def make(
        cls,
        base: EngineContext,
        inputs: Mapping[str, Any],
        target: Spider | None = None,
    ) -> Context:
        """由当前端口构造上下文，子类可重写自身 make 逻辑。

        Args:
            base: 当前基础上下文。
            inputs: seeds 或 task 端口输入。
            target: 当前 Spider。

        Returns:
            本次独立的领域上下文。

        Raises:
            TypeError: 缺少 target 或输入不符合领域类型。
        """
        if target is None:
            raise TypeError("Context requires target")
        task = inputs.get("task")
        if task is None:
            seeds = inputs["seeds"]
            if type(seeds) is not dict:
                raise TypeError("crawler seeds must be a JSON object")
            task = _Task(seeds)
        if not isinstance(task, _Task):
            raise TypeError("crawler task input is invalid")
        context = cls(target=target, base=base, task=task)
        context._input = inputs
        return context

    def _bind(self, inputs: Mapping[str, Any]) -> None:
        """跟随核心 Hook 显式替换后的输入，保留同一个 Context 实例。

        Args:
            inputs: 所有 enter Hook 执行后的输入。
        """
        key = "task" if "task" in inputs else "seeds"
        if inputs[key] is not self._input.get(key):
            self._task = inputs[key] if key == "task" else _Task(inputs[key])
        self._input = inputs

    @property
    def seeds(self) -> dict[str, Any]:
        """读取当前种子。

        Returns:
            当前交付拥有的 JSON 对象，修改不回写队列。
        """
        return self._task.seed

    @seeds.setter
    def seeds(self, value: dict[str, Any]) -> None:
        """替换当前图内种子，不修改已入队的原始任务。

        Args:
            value: 新的种子字典。

        Raises:
            TypeError: 值不是字典。
        """
        if type(value) is not dict:
            raise TypeError("seeds must be a dictionary")
        self._task.seed = value

    @property
    def task_queue(self) -> TaskQueue:
        """读取当前爬虫的任务存储。

        Returns:
            调用方装配的队列。
        """
        return self.target.task_queue

    @property
    def queue_name(self) -> str:
        """读取当前任务队列名称。

        Returns:
            队列名称。
        """
        return self.target.queue_name

    def clear_proxy(self) -> None:
        """显式丢弃当前请求代理，使下次请求重新选择地址。

        Raises:
            RuntimeError: 自动会话固定了代理，或托管代理缺少 Slot。
        """
        if (
            self.options.get("crawler.session.reuse", True) is True
            and self.slot is not None
            and "crawler.session.proxy" in self.slot
        ):
            raise RuntimeError("session proxy cannot be hot-switched")
        manager = self.target.proxy
        if isinstance(manager, Manager):
            if self.slot is None:
                raise RuntimeError("managed proxies require a Slot")
            manager.clear(self.slot)
        self.request.proxy = None

    def emit(
        self,
        event_type: str,
        payload: Any = None,
        *,
        affinity: Affinity = "current",
    ) -> Event:
        """发布跨图事件并选择是否延续当前 Slot。

        Args:
            event_type: 已在 Runtime 注册路由的领域事件类型。
            payload: 目标 Graph 的输入数据。
            affinity: current 使用本地 emitter；any 通过 Runtime 普通发布。

        Returns:
            已被 Router 接受的 Event。

        Raises:
            ValueError: affinity 不是 current 或 any。
            RuntimeError: 普通发布时 Spider Runtime 尚未打开。
        """
        if _affinity(affinity) == "current":
            return super().emit(event_type, payload)
        return self.target.runtime.emit(event_type, payload)

    def install(self, name: str, value: Any, *, nx: bool = False) -> Any:
        """把业务资源写入当前 Slot，供后续取得同一 Slot 的 Context 使用。

        Args:
            name: 当前业务定义的非空资源名称。
            value: 进程内资源值，不跨队列或进程序列化。
            nx: True 时保留已存在资源并返回旧值。

        Returns:
            实际保存在当前 Slot 中的资源值。

        Raises:
            RuntimeError: 当前执行没有 Slot。
            ValueError: 资源名称非法。
            TypeError: nx 不是布尔值。
        """
        if not isinstance(name, str) or not name:
            raise ValueError("resource name must be a non-empty string")
        if type(nx) is not bool:
            raise TypeError("nx must be boolean")
        if self.slot is None:
            raise RuntimeError("context resources require an execution Slot")
        resources = self.slot.setdefault(_RESOURCES, {})
        if not isinstance(resources, dict):
            raise TypeError("context resource namespace must be a dictionary")
        if nx and name in resources:
            return resources[name]
        resources[name] = value
        return value

    def obtain(self, name: str, default: Any = None) -> Any:
        """读取当前 Slot 中的具名业务资源。

        Args:
            name: 业务资源名称。
            default: 资源不存在时的返回值。

        Returns:
            Slot 资源或 default。

        Raises:
            ValueError: 名称非法。
        """
        if not isinstance(name, str) or not name:
            raise ValueError("resource name must be a non-empty string")
        if self.slot is None:
            return default
        resources = self.slot.get(_RESOURCES, {})
        if not isinstance(resources, dict):
            raise TypeError("context resource namespace must be a dictionary")
        return resources.get(name, default)

    @property
    def request(self) -> Request:
        """取得已经构造的请求。

        Returns:
            当前图内请求。

        Raises:
            RuntimeError: 当前阶段尚未构造请求。
        """
        if self._task.request is None:
            raise RuntimeError("request is not available before make_request")
        return self._task.request

    @request.setter
    def request(self, value: Request) -> None:
        """显式替换当前请求。

        Args:
            value: 新请求。

        Raises:
            TypeError: 值不是 Request。
        """
        if not isinstance(value, Request):
            raise TypeError("make_request must return Request")
        self._task.request = value

    @property
    def response(self) -> Response:
        """取得当前下载响应。

        Returns:
            当前响应。

        Raises:
            RuntimeError: 尚未完成下载。
        """
        if self._task.response is None:
            raise RuntimeError("response is not available before download")
        return self._task.response

    @response.setter
    def response(self, value: Response) -> None:
        """显式替换响应，供下载与 Hook 使用。

        Args:
            value: 新响应。

        Raises:
            TypeError: 值不是 Response。
        """
        if not isinstance(value, Response):
            raise TypeError("download must return Response")
        self._task.response = value

    @property
    def items(self) -> Items:
        """取得已解析的记录。

        Returns:
            当前记录容器。

        Raises:
            RuntimeError: 尚未完成解析。
        """
        if self._task.items is None:
            raise RuntimeError("items are not available before parse")
        return self._task.items

    @items.setter
    def items(self, value: Items | list[dict[str, Any]]) -> None:
        """接受明确声明的 Items 或记录列表。

        Args:
            value: 新记录容器或记录列表。

        Raises:
            TypeError: 值不是 Items 或记录列表。
        """
        if isinstance(value, list):
            value = Items(value)
        if not isinstance(value, Items):
            raise TypeError("parse must return Items or list[dict]")
        self._task.items = value

    @property
    def attempt(self) -> int:
        """取得已发起的下载次数。

        Returns:
            首次下载前为 0，之后从 1 递增。
        """
        return self._task.attempt

    @property
    def retries(self) -> int:
        """取得已消耗的重试预算次数，包括首次下载前的 Retry。

        Returns:
            从零开始的重试次数，与实际下载 attempt 分开统计。
        """
        return self._task.retries

    def submit(
        self,
        *obj: dict[str, Any],
        affinity: Affinity = "current",
        id: str | None = None,
        options: Mapping[str, Any] | None = None,
    ) -> list[Work]:
        """显式提交一个或多个独立种子，返回已接受任务描述。

        Args:
            obj: JSON 种子对象序列。
            affinity: current 持久化后延续当前 Slot；any 等待任意 Worker 领取。
            id: 单种子提交时可指定的业务 ID。
            options: 覆盖 Spider 默认值的执行配置。

        Returns:
            已接受或去重命中的 Work 列表，不返回未来执行的 Context。

        Raises:
            ValueError: 多个种子同时指定一个 ID。
        """
        self.checkpoint()
        affinity = _affinity(affinity)
        if id is not None and len(obj) != 1:
            raise ValueError("explicit id requires exactly one seed")
        works = [self.target._make_work(seed, id=id, options=options) for seed in obj]
        for work in works:
            if affinity == "current":
                self.emit(
                    self.target.local_submission_event_name,
                    LocalSubmission(work),
                    affinity="current",
                )
            else:
                self.task_queue.put(work)
        return works


class InitContext(CurrentContext, EngineContext):
    """初始化上下文，提供 target、seeds、task_queue、queue_name 和 record。

    Attributes:
        target: 当前 Spider。
        form: 当前初始化或插件阶段。
        _initial: 本轮图内生产状态。
    """

    def __init__(
        self,
        target: Spider,
        *,
        base: EngineContext,
        initial: Initialization,
        form: str = "ON_INIT",
    ) -> None:
        """组合执行能力与本轮生产状态。

        Args:
            target: 当前 Spider。
            base: 基础执行上下文。
            initial: 本轮生产状态。
            form: 当前阶段名称。
        """
        super().__init__(parent=base)
        self.target = target
        self.form = form
        self._initial = initial

    @property
    def seeds(self) -> Any:
        """读取当前待投放种子。

        Returns:
            当前种子或批次，尚未迭代时为 None。
        """
        return self._initial.seed

    @seeds.setter
    def seeds(self, value: dict[str, Any] | list[dict[str, Any]]) -> None:
        """替换当前初始化批次，实际入队时仍执行严格 JSON 校验。

        Args:
            value: 单个种子或种子列表。

        Raises:
            TypeError: 不是种子对象或对象列表。
        """
        if type(value) is not dict and not (
            type(value) is list and all(type(seed) is dict for seed in value)
        ):
            raise TypeError("make_seeds must yield a JSON object or a list of objects")
        self._initial.seed = value

    @property
    def task_queue(self) -> TaskQueue:
        """取得本轮使用的任务存储。

        Returns:
            当前队列。
        """
        return self.target.task_queue

    @property
    def queue_name(self) -> str:
        """取得本轮队列名称。

        Returns:
            队列名称。
        """
        return self.target.queue_name

    @property
    def record(self) -> Any:
        """读取恢复记录，提交成功后保存其当前内容。

        Returns:
            用户定义的 JSON 断点对象。
        """
        return self._initial.progress

    @record.setter
    def record(self, value: Any) -> None:
        """更新下一次提交后保存的记录。

        Args:
            value: 用户定义的 JSON 记录。
        """
        self._initial.progress = value

    def submit(
        self,
        *obj: dict[str, Any],
        id: str | None = None,
        options: Mapping[str, Any] | None = None,
    ) -> list[Work]:
        """使用初始化租约提交额外种子。

        Args:
            obj: 一个或多个 JSON 种子。
            id: 单个种子可指定业务 ID。
            options: 执行配置覆盖。

        Returns:
            已接受或去重命中的 Work 列表。

        Raises:
            ValueError: 多种子同时指定单一 ID。
        """
        self.checkpoint()
        if id is not None and len(obj) != 1:
            raise ValueError("explicit id requires exactly one seed")
        return [
            self.target._submit(seed, id=id, options=options, init=self._initial.lease)
            for seed in obj
        ]
