"""配置式爬虫：以 Download 切分的步骤段复用 Air 消费图，每段完成后持久化下一段。

每个 Work 只执行一段：make_request 渲染本段 Download，AFTER_REQUEST 末尾执行下载后的 Task，
parse 与 item_pipeline 执行本段解析和存储。段末在 AFTER_PIPELINE 开头经 context.submit
原子持久化下一段任务，再随当前交付确认；进程中断时未确认的段由租约恢复，
已持久化的下一段不会丢失，重放时按稳定 Work ID 去重。
"""

from __future__ import annotations

import copy
import inspect
import math
import re
from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from functools import partial
from typing import Any, cast, get_args

from interlace.spi import Work

from bricks import Items, Request, hooks
from bricks.hooks import Task
from bricks.models.body import BodyInput, BodyType
from bricks.models.headers import HeaderInput
from bricks.models.options import FORM_STAGE
from bricks.models.request import HttpMethod, QueryInput
from bricks.models.response import Engine
from bricks.parsers.rules import RuleSet

from . import air
from ._hooks import run_hooks
from .context import Affinity, _affinity

__all__ = [
    "Config",
    "Context",
    "Download",
    "Init",
    "Layout",
    "Parse",
    "Pipeline",
    "Spider",
    "Task",
]

_PLACEHOLDER = re.compile(r"\{(\w+)\}")  # 按种子字段渲染的 {name} 占位符。
_ENGINES = frozenset(get_args(Engine))  # Parse 接受的内置解析引擎名称。


def _render(value: Any, seeds: Mapping[str, Any]) -> Any:
    """按种子渲染字符串、映射和序列中的 {name} 占位符。

    整个字符串恰好是一个占位符时保留种子值的原类型，否则以 str() 拼接；
    映射只渲染值，其他类型原样返回。

    Args:
        value: 待渲染的配置值。
        seeds: 提供占位符取值的种子。

    Returns:
        渲染后的新值，不修改原配置。

    Raises:
        KeyError: 占位符引用了种子中不存在的字段。
    """

    def lookup(name: str) -> Any:
        """读取占位符对应的种子字段。"""
        try:
            return seeds[name]
        except KeyError:
            raise KeyError(f"seed has no field {name!r} for placeholder") from None

    if isinstance(value, str):
        whole = _PLACEHOLDER.fullmatch(value)
        if whole is not None:
            return lookup(whole.group(1))
        return _PLACEHOLDER.sub(lambda match: str(lookup(match.group(1))), value)
    if isinstance(value, Mapping):
        return {key: _render(item, seeds) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_render(item, seeds) for item in value)
    return value


def _call(task: Task, context: air.Context) -> Any:
    """按 Task 的 Context 绑定规则调用步骤函数。

    Args:
        task: 固定函数与参数的调用描述。
        context: 当前领域上下文。

    Returns:
        函数结果或 Awaitable。
    """
    prefix = (context,) if task._with_context is not False else ()
    return task.func(*prefix, *task.args, **task.kwargs)


def _then(value: Any, after: Callable[[Any], Any]) -> Any:
    """同步结果立即转换，Awaitable 等待后再转换。

    Args:
        value: 步骤结果或 Awaitable。
        after: 结果转换函数。

    Returns:
        转换结果或等待转换结果的 Awaitable。
    """
    if not inspect.isawaitable(value):
        return after(value)

    async def awaited() -> Any:
        """等待步骤完成后执行转换。"""
        return after(await value)

    return awaited()


def _chain(steps: Iterable[Callable[[], Any]]) -> Awaitable[None] | None:
    """按顺序执行步骤；遇到第一个 Awaitable 后在协程内继续剩余步骤。

    Args:
        steps: 返回 None 或 Awaitable 的零参数步骤。

    Returns:
        全部同步完成时为 None，否则为等待剩余步骤的 Awaitable。
    """
    iterator = iter(steps)
    for step in iterator:
        value = step()
        if inspect.isawaitable(value):

            async def rest(first: Awaitable[Any]) -> None:
                """等待已启动步骤，再顺序推进剩余步骤。"""
                await first
                for following in iterator:
                    result = following()
                    if inspect.isawaitable(result):
                        await result

            return rest(value)
    return None


def _none(value: Any, label: str) -> None:
    """确认步骤按约定返回 None。

    Raises:
        TypeError: 步骤返回了其他值。
    """
    if value is not None:
        raise TypeError(f"{label} must return None")


@dataclass(frozen=True)
class Layout:
    """记录整理规则，依次补默认值、隐藏字段、工厂转换和重命名。

    Attributes:
        rename: 旧字段名到新字段名，字段不存在时跳过。
        default: 缺失字段的默认值，字符串中的 {name} 按种子渲染，每条记录独立复制。
        factory: 字段名到单参数函数，参数为当前值（缺失时为 None），返回值写回该字段。
        show: 字段名到是否保留，False 删除该字段。
    """

    rename: Mapping[str, str] = field(default_factory=dict)
    default: Mapping[str, Any] = field(default_factory=dict)
    factory: Mapping[str, Callable[[Any], Any]] = field(default_factory=dict)
    show: Mapping[str, bool] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """校验并冻结各项规则。

        Raises:
            TypeError: 规则不是映射，或重命名目标、工厂函数、显示标志类型错误。
        """
        for name in ("rename", "default", "factory", "show"):
            value = getattr(self, name)
            if not isinstance(value, Mapping):
                raise TypeError(f"layout {name} must be a mapping")
            object.__setattr__(self, name, dict(value))
        if any(type(target) is not str for target in self.rename.values()):
            raise TypeError("layout rename targets must be strings")
        if any(not callable(function) for function in self.factory.values()):
            raise TypeError("layout factory values must be callable")
        if any(type(flag) is not bool for flag in self.show.values()):
            raise TypeError("layout show values must be booleans")

    def apply(self, rows: Iterable[dict[str, Any]], seeds: Mapping[str, Any]) -> None:
        """原地整理记录。

        Args:
            rows: 待整理的记录，调用方负责必要的复制。
            seeds: 渲染 default 占位符的种子。

        Raises:
            KeyError: default 占位符引用了不存在的种子字段。
        """
        default = _render(self.default, seeds)
        for row in rows:
            for key, value in default.items():
                if key not in row:
                    row[key] = copy.deepcopy(value)
            for key, visible in self.show.items():
                if not visible:
                    row.pop(key, None)
            for key, function in self.factory.items():
                row[key] = function(row.get(key))
            for old, new in self.rename.items():
                if old in row:
                    row[new] = row.pop(old)


@dataclass(frozen=True)
class Download:
    """一段流程的起点：按种子渲染请求，字符串中的 {name} 读取同名种子字段。

    Attributes:
        url: 绝对 HTTP(S) 地址模板。
        method: HTTP 方法，默认 GET。
        params: 查询参数模板，None 表示无参数。
        headers: 请求头模板，None 表示无额外请求头。
        cookies: 本次请求 Cookie 模板。
        body: 请求体模板，None 表示无请求体。
        body_type: 请求体编码方式，语义同 Request。
        timeout: 请求时限秒数，默认 30，None 不限时。
        allow_redirects: 是否跟随重定向。
        proxy: 显式代理地址模板，None 交给代理插件选择。
        each: True 时对上一段的每条 Items 各执行一次，种子为上一段种子合并该记录；
            False 时沿用上一段种子。首段不能使用。
        affinity: 进入本段的任务派发方式；current 持久化后优先在当前 Slot 继续，
            any 只入队等待任意 Worker 领取。
        max_retry: 本段下载重试上限；None 沿用默认 5，执行配置 crawler.retry.max 优先。
        ok: 本段成功判断规则，语法同 bypass 的 ok，可以是 callable；None 使用默认状态码规则，
            执行配置 crawler.response.ok 优先。
    """

    url: str
    method: HttpMethod = "GET"
    params: QueryInput | None = None
    headers: HeaderInput | None = None
    cookies: Mapping[str, str] | None = None
    body: BodyInput = None
    body_type: BodyType = "auto"
    timeout: float | None = 30
    allow_redirects: bool = True
    proxy: str | None = None
    each: bool = False
    affinity: Affinity = "current"
    max_retry: int | None = None
    ok: Any = None

    def __post_init__(self) -> None:
        """校验模板与派发配置。

        Raises:
            TypeError: url 不是字符串或 each 不是布尔值。
            ValueError: affinity 不是 current 或 any，或 max_retry 不是非负整数。
        """
        if not isinstance(self.url, str):
            raise TypeError("download url must be a string")
        if type(self.each) is not bool:
            raise TypeError("download each must be boolean")
        _affinity(self.affinity)
        if self.max_retry is not None and (
            type(self.max_retry) is not int or self.max_retry < 0
        ):
            raise ValueError("download max_retry must be a nonnegative integer")

    def to_request(self, seeds: Mapping[str, Any]) -> Request:
        """按种子渲染模板并构造独立请求。

        Args:
            seeds: 当前种子。

        Returns:
            新构造的请求。

        Raises:
            KeyError: 占位符引用了不存在的种子字段。
            TypeError: 渲染后的字段类型不符合 Request。
            ValueError: 渲染后的字段值不合法。
        """
        return Request(
            _render(self.url, seeds),
            method=self.method,
            params=_render(self.params, seeds),
            headers=_render(self.headers, seeds),
            cookies=_render(self.cookies, seeds),
            body=_render(self.body, seeds),
            body_type=self.body_type,
            timeout=self.timeout,
            allow_redirects=self.allow_redirects,
            proxy=_render(self.proxy, seeds),
        )


@dataclass(frozen=True)
class Parse:
    """本段的解析步骤：内置引擎按规则提取，或调用自定义函数返回 Items。

    Attributes:
        func: json、jsonpath、css、xpath、regex 之一，或返回 Items/list[dict] 的函数；
            函数可显式首参接收 Context，或通过 Context.get_context() 读取。
        rules: 内置引擎的提取规则，语法同 `bricks.parsers.match`；函数形式必须为 None。
        args: 函数形式追加的位置参数。
        kwargs: 函数形式追加的关键字参数。
        layout: 解析后原地整理记录的规则。
        _task: 函数形式的调用描述；引擎形式为 None。
    """

    func: Engine | Callable[..., Any]
    rules: RuleSet | None = None
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)
    layout: Layout | None = None
    _task: Task | None = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        """区分引擎与函数两种形式并校验参数组合。

        Raises:
            TypeError: func 既不是受支持的引擎名也不可调用，或 layout 类型错误。
            ValueError: 引擎缺少 rules、携带函数参数，或函数形式提供了 rules。
        """
        if self.layout is not None and not isinstance(self.layout, Layout):
            raise TypeError("parse layout must be Layout or None")
        if isinstance(self.func, str):
            if self.func not in _ENGINES:
                raise ValueError(f"unsupported parse engine: {self.func!r}")
            if self.rules is None:
                raise ValueError("parse engine requires rules")
            if self.args or self.kwargs:
                raise ValueError("parse engine does not accept args or kwargs")
            object.__setattr__(self, "_task", None)
            return
        if self.rules is not None:
            raise ValueError("parse function does not accept rules")
        object.__setattr__(self, "_task", Task(self.func, self.args, self.kwargs))


@dataclass(frozen=True)
class Pipeline:
    """本段的存储步骤，函数返回 None 表示继续。

    Attributes:
        func: 存储函数，可显式首参接收 Context 或通过 Context.get_context() 读取。
        args: 追加的位置参数。
        kwargs: 追加的关键字参数。
        layout: 只作用于本步骤 Items 副本的整理规则，步骤结束后恢复原 Items。
        _task: 固定函数与参数的调用描述。
    """

    func: Callable[..., Any]
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)
    layout: Layout | None = None
    _task: Task = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        """校验函数与整理规则。

        Raises:
            TypeError: 函数、参数或 layout 类型错误。
        """
        if self.layout is not None and not isinstance(self.layout, Layout):
            raise TypeError("pipeline layout must be Layout or None")
        object.__setattr__(self, "_task", Task(self.func, self.args, self.kwargs))


@dataclass(frozen=True)
class Init:
    """初始种子来源：静态种子，或以 record 关键字接收恢复记录的种子函数。

    Attributes:
        source: 种子对象、种子列表，或返回它们及其迭代器的同步函数；函数以
            ``source(*args, record=record, **kwargs)`` 调用，可直接使用 `bricks.plugins.make_seeds`。
        args: 函数形式追加的位置参数。
        kwargs: 函数形式追加的关键字参数。
        layout: 逐条整理种子的规则，default 占位符按该种子自身渲染。
    """

    source: dict[str, Any] | list[dict[str, Any]] | Callable[..., Any]
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)
    layout: Layout | None = None

    def __post_init__(self) -> None:
        """校验来源形式并冻结参数。

        Raises:
            TypeError: 来源不是种子、种子列表或可调用对象，或参数类型错误。
            ValueError: 静态种子携带了函数参数。
        """
        if self.layout is not None and not isinstance(self.layout, Layout):
            raise TypeError("init layout must be Layout or None")
        if not isinstance(self.args, (list, tuple)) or not isinstance(
            self.kwargs, Mapping
        ):
            raise TypeError("init args and kwargs must be a sequence and mapping")
        if callable(self.source):
            if inspect.iscoroutinefunction(self.source):
                raise TypeError("init source must be synchronous")
        elif type(self.source) is dict or (
            type(self.source) is list
            and all(type(seed) is dict for seed in self.source)
        ):
            if self.args or self.kwargs:
                raise ValueError("static init seeds do not accept args or kwargs")
        else:
            raise TypeError("init source must be a seed, a seed list or a callable")
        object.__setattr__(self, "args", tuple(self.args))
        object.__setattr__(self, "kwargs", dict(self.kwargs))

    def produce(self, record: Any) -> Iterator[dict[str, Any] | list[dict[str, Any]]]:
        """产出种子或种子批次，静态来源每次深复制。

        Args:
            record: 初始化恢复记录，函数来源负责读取和更新。

        Yields:
            经过整理的单个种子或种子列表。

        Raises:
            TypeError: 函数返回值不是种子、种子列表或其迭代器。
        """
        if callable(self.source):
            produced = self.source(*self.args, record=record, **self.kwargs)
        else:
            produced = copy.deepcopy(self.source)
        if produced is None:
            return
        if type(produced) is dict:
            produced = (produced,)
        if not isinstance(produced, Iterable) or isinstance(produced, (str, bytes)):
            raise TypeError("init source must return seeds or an iterable of seeds")
        for batch in produced:
            if self.layout is not None:
                for seed in batch if type(batch) is list else (batch,):
                    if type(seed) is not dict:
                        raise TypeError("init seeds must be JSON objects")
                    self.layout.apply((seed,), seed)
            yield batch


Step = Download | Parse | Pipeline | Task


@dataclass(frozen=True)
class Config:
    """配置式爬虫的完整声明。

    Attributes:
        spider: 以 Download 开头的步骤序列；每个 Download 开启一段，段内可含 Task、
            至多一个 Parse 以及位于 Parse 之后的 Pipeline。
        init: 按顺序产出初始种子的来源。
        events: 阶段名到额外插件调用，打开时经 Spider.use 注册。
    """

    spider: Sequence[Step]
    init: Sequence[Init] = ()
    events: Mapping[str, Sequence[Task | dict[str, Any]]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """固定步骤与来源序列。

        Raises:
            TypeError: 序列或映射类型错误，或含未知步骤与来源。
        """
        if not isinstance(self.spider, Sequence) or not isinstance(self.init, Sequence):
            raise TypeError("form config spider and init must be sequences")
        if not isinstance(self.events, Mapping):
            raise TypeError("form config events must be a mapping")
        if any(not isinstance(item, Init) for item in self.init):
            raise TypeError("form config init items must be Init")
        object.__setattr__(self, "spider", tuple(self.spider))
        object.__setattr__(self, "init", tuple(self.init))
        object.__setattr__(
            self, "events", {form: tuple(tasks) for form, tasks in self.events.items()}
        )


@dataclass(frozen=True)
class _Stage:
    """由一个 Download 开启的步骤段。

    Attributes:
        download: 本段请求模板。
        request: 下载后、解析前的 Task，在 AFTER_REQUEST 末尾执行并允许 Retry。
        parse: 本段解析步骤；None 表示本段 Items 为空。
        parsed: 解析后、首个 Pipeline 前的 Task。
        pipeline: 按声明顺序执行的 Pipeline 与 Task。
    """

    download: Download
    request: tuple[Task, ...]
    parse: Parse | None
    parsed: tuple[Task, ...]
    pipeline: tuple[Pipeline | Task, ...]


def _compile(steps: Sequence[Step]) -> tuple[_Stage, ...]:
    """把扁平步骤序列切分为段并校验段内顺序。

    Args:
        steps: Config.spider 声明。

    Returns:
        按序排列的步骤段。

    Raises:
        TypeError: 含未知步骤类型。
        ValueError: 未以 Download 开头、首段使用 each、单段多个 Parse 或 Parse 位于 Pipeline 之后。
    """
    if not steps or not isinstance(steps[0], Download):
        raise ValueError("form spider steps must start with Download")
    if steps[0].each:
        raise ValueError("the first Download cannot use each=True")
    stages: list[dict[str, Any]] = []
    for step in steps:
        if isinstance(step, Download):
            stages.append(
                {
                    "download": step,
                    "request": [],
                    "parse": None,
                    "parsed": [],
                    "pipeline": [],
                }
            )
            continue
        current = stages[-1]
        if isinstance(step, Parse):
            if current["parse"] is not None or current["pipeline"]:
                raise ValueError("each stage allows one Parse before its Pipelines")
            current["parse"] = step
        elif isinstance(step, Pipeline):
            current["pipeline"].append(step)
        elif isinstance(step, Task):
            if current["pipeline"]:
                current["pipeline"].append(step)
            elif current["parse"] is not None:
                current["parsed"].append(step)
            else:
                current["request"].append(step)
        else:
            raise TypeError(f"unknown form step: {type(step).__name__}")
    return tuple(
        _Stage(
            download=stage["download"],
            request=tuple(stage["request"]),
            parse=stage["parse"],
            parsed=tuple(stage["parsed"]),
            pipeline=tuple(stage["pipeline"]),
        )
        for stage in stages
    )


class Context(air.Context):
    """配置式爬虫的消费上下文，提供当前段号与段级重试、成功规则，submit 默认留在当前段。"""

    @property
    def max_retry(self) -> int:
        """取得重试上限：执行配置优先，其次本段 Download.max_retry，最后为默认 5。

        Returns:
            首次下载之后允许的额外尝试次数。
        """
        if self.crawler_options.retry_max is None:
            limit = cast("Spider", self.target)._stage(self).download.max_retry
            if limit is not None:
                return limit
        return super().max_retry

    @property
    def ok_rule(self) -> Any:
        """取得成功判断规则：执行配置优先，其次本段 Download.ok。

        Returns:
            规则；均未配置时为 None。
        """
        rule = super().ok_rule
        if rule is None:
            return cast("Spider", self.target)._stage(self).download.ok
        return rule

    @property
    def stage(self) -> int:
        """读取当前 Work 所在的步骤段序号。

        Returns:
            从 0 开始的段序号。

        Raises:
            ValueError: Work 中保存的段号不是非负整数。
        """
        return self.crawler_options.form_stage

    def submit(
        self,
        *obj: dict[str, Any],
        affinity: Affinity = "current",
        id: str | None = None,
        options: Mapping[str, Any] | None = None,
    ) -> list[Work]:
        """提交种子；未显式指定段号时留在当前段，例如翻页。

        Args:
            obj: JSON 种子对象序列。
            affinity: current 持久化后延续当前 Slot；any 等待任意 Worker 领取。
            id: 单种子提交时可指定的业务 ID。
            options: 覆盖 Spider 默认值的执行配置，可含 crawler.form.stage。

        Returns:
            已接受或去重命中的 Work 列表。
        """
        merged = {FORM_STAGE: self.stage, **({} if options is None else options)}
        return super().submit(*obj, affinity=affinity, id=id, options=merged)


_FormContext = Context  # Spider 类体内的 Context 属性遮蔽模块名，方法注解使用此别名。


class Spider(air.Spider):
    """以 Config 声明流程的爬虫，复用 Air 的队列、重试、插件与资源装配。

    子类以类属性或属性提供 config；打开时编译一次步骤段，此后修改声明不影响运行中实例。

    Attributes:
        Context: 消费上下文类型，提供 stage 与段内 submit。
        _stages: 打开时编译的步骤段，打开前为空。
    """

    Context = Context
    _stages: tuple[_Stage, ...] = ()

    @property
    def config(self) -> Config:
        """返回流程声明，子类必须提供。

        Raises:
            NotImplementedError: 子类尚未声明 config。
        """
        raise NotImplementedError("define form config")

    def install(self) -> None:
        """注册默认插件、配置中的 events，以及段内 Task 和段间推进插槽。

        Raises:
            TypeError: config 不是 Config 或步骤类型错误。
            ValueError: 步骤顺序不合法。
        """
        super().install()
        config = self.config
        if not isinstance(config, Config):
            raise TypeError("form spider config must be Config")
        self._stages = _compile(config.spider)
        for form, events in config.events.items():
            self.use(form, *events)
        # 下载后的 Task 排在其他响应检查之后，推进下一段排在用户后置插件之前。
        self.use(hooks.AFTER_REQUEST, Task(self._request_tasks, index=math.inf))
        self.use(hooks.AFTER_PIPELINE, Task(self._advance, index=-math.inf))

    def _stage(self, context: air.Context) -> _Stage:
        """读取当前 Work 对应的步骤段。

        Raises:
            TypeError: 上下文不是 form.Context。
            ValueError: 段号超出配置范围。
        """
        if not isinstance(context, Context):
            raise TypeError("form spider requires form.Context")
        index = context.stage
        if index >= len(self._stages):
            raise ValueError(f"unknown form stage {index}")
        return self._stages[index]

    def make_seeds(self, context: air.InitContext, **kwargs: Any) -> Any:
        """按声明顺序产出全部 Init 来源的种子。

        Args:
            context: 初始化上下文。
            kwargs: 含恢复记录 record。

        Returns:
            种子或种子批次迭代器。
        """
        record = kwargs["record"]
        return (
            batch for source in self.config.init for batch in source.produce(record)
        )

    def make_request(self, context: air.Context) -> Request:
        """按当前段的 Download 渲染请求。

        Args:
            context: 当前上下文。

        Returns:
            本段请求。
        """
        return self._stage(context).download.to_request(context.seeds)

    def _request_tasks(self, context: _FormContext) -> Awaitable[None] | None:
        """在 AFTER_REQUEST 末尾执行本段下载后的 Task，允许 Retry 重新下载。

        Args:
            context: 当前上下文。

        Returns:
            None 或等待异步 Task 的 Awaitable。
        """
        return run_hooks(self, self._stage(context).request, context)

    def parse(self, context: air.Context) -> Any:
        """执行本段解析与解析后的 Task，返回最终 Items。

        Args:
            context: 已有响应的当前上下文。

        Returns:
            本段 Items 或等待它的 Awaitable。
        """
        stage = self._stage(context)
        step = stage.parse

        def assign(value: Any) -> None:
            """保存解析结果并按 layout 原地整理。"""
            context.items = value
            if step is not None and step.layout is not None:
                step.layout.apply(context.items, context.seeds)

        def run_parse() -> Any:
            """调用引擎或自定义解析函数。"""
            if step is None:
                return assign(Items())
            if step._task is None:
                assert isinstance(step.func, str) and step.rules is not None
                return assign(context.response.extract(step.func, step.rules))
            return _then(_call(step._task, context), assign)

        steps: list[Callable[[], Any]] = [
            run_parse,
            *(partial(run_hooks, self, (task,), context) for task in stage.parsed),
        ]
        return _then(_chain(steps), lambda _: context.items)

    def item_pipeline(self, context: air.Context) -> Awaitable[None] | None:
        """按声明顺序执行本段 Pipeline 与 Task。

        Args:
            context: 已有 Items 的当前上下文。

        Returns:
            None 或等待异步步骤的 Awaitable。
        """
        stage = self._stage(context)

        def run(step: Pipeline | Task) -> Any:
            """执行单个存储步骤或 Task。"""
            if isinstance(step, Task):
                return run_hooks(self, (step,), context)
            backup = context.items
            if step.layout is not None:
                shaped = backup.copy()
                step.layout.apply(shaped, context.seeds)
                context.items = shaped
            try:
                value = _call(step._task, context)
            except BaseException:
                context.items = backup
                raise
            if not inspect.isawaitable(value):
                context.items = backup
                return _none(value, "pipeline")

            async def awaited() -> None:
                """等待存储完成后恢复原 Items。"""
                try:
                    result = await value
                finally:
                    context.items = backup
                _none(result, "pipeline")

            return awaited()

        return _chain(partial(run, step) for step in stage.pipeline)

    def _advance(self, context: _FormContext) -> None:
        """持久化下一段任务；末段不产生任务。

        each=True 时为每条 Items 合并当前种子各提交一个任务，否则提交当前种子。
        提交与当前交付的确认不是同一事务，重放时按稳定 Work ID 去重。

        Args:
            context: 已完成本段存储的上下文。
        """
        index = context.stage
        if index + 1 >= len(self._stages):
            return
        download = self._stages[index + 1].download
        seeds = (
            [{**context.seeds, **item} for item in context.items]
            if download.each
            else [context.seeds]
        )
        if seeds:
            context.submit(
                *seeds,
                affinity=download.affinity,
                options={FORM_STAGE: index + 1},
            )
