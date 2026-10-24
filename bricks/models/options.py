"""任务级 `crawler.*` 与实例级 `spider.*` 点分配置的统一解析与校验。"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

PREFIX = "crawler."  # Bricks 保留的执行配置命名空间，未知键明确拒绝。
SESSION_REUSE = "crawler.session.reuse"  # 是否复用 Slot 会话，布尔值，默认 True。
IMPERSONATE = "crawler.fingerprint.impersonate"  # 当前执行的预设指纹覆盖名称。
RETRY_MAX = "crawler.retry.max"  # 当前任务的下载重试上限，非负整数，缺省为 5。
RETRY_DELAY = "crawler.retry.delay"  # 下载重试等待秒数，非负有限数，默认 0。
PROXY_ROTATE = "crawler.proxy.rotate"  # 重试时 set_proxy 是否更换已用代理，默认 True。
PROXY_RETRY = "crawler.proxy.retry"  # 代理错误不消耗重试预算的独立上限，默认 5。
RESPONSE_OK = "crawler.response.ok"  # 当前任务的成功判断规则，语法同 bypass 的 ok。
RESPONSE_SCRIPTS = "crawler.response.scripts"  # conditional_scripts 的条件到脚本映射。
FORM_STAGE = "crawler.form.stage"  # form 爬虫的当前步骤段序号，从 0 开始。
_KEYS = frozenset(
    (
        SESSION_REUSE,
        IMPERSONATE,
        RETRY_MAX,
        RETRY_DELAY,
        PROXY_ROTATE,
        PROXY_RETRY,
        RESPONSE_OK,
        RESPONSE_SCRIPTS,
        FORM_STAGE,
    )
)  # 当前版本支持的全部 crawler.* 键。
DEFAULT_RETRY_MAX = 5  # 执行配置与 form 段级均未指定时的下载重试上限。

SETTINGS_PREFIX = "spider."  # 实例级配置命名空间，只在构造 Spider 时读取，不写入任务。
RUN_STRICT = "spider.run.strict"  # 排空后是否以汇总异常报告业务拒绝，默认 False。
INIT_TIMEOUT = "spider.init.timeout"  # 单次初始化执行超时秒数，默认 None 不限制。
QUEUE_LEASE = "spider.queue.lease"  # 初始化与交付租约秒数，默认 30。
QUEUE_POLL = "spider.queue.poll"  # 队列空轮询间隔秒数，默认 0.05。
AUTO_SESSION = "spider.session.reuse"  # 是否为自建 Slot 自动装配同步会话，默认 True。
SESSION_PROXIES = "spider.session.proxies"  # 每个 Slot 按代理保留的会话上限，默认 4。
_SETTINGS = frozenset(
    (RUN_STRICT, INIT_TIMEOUT, QUEUE_LEASE, QUEUE_POLL, AUTO_SESSION, SESSION_PROXIES)
)  # 当前版本支持的全部 spider.* 键。


def _seconds(value: Any, label: str, *, positive: bool) -> float:
    """校验有限秒数，拒绝布尔值。

    Args:
        value: 待校验的秒数。
        label: 错误信息中的配置键。
        positive: 是否必须严格大于零。

    Returns:
        浮点秒数。

    Raises:
        ValueError: 不是有限数，或不满足取值范围。
    """

    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(f"{label} must be a finite number")
    if value < 0 or (positive and value == 0):
        raise ValueError(
            f"{label} must be positive" if positive else f"{label} must be nonnegative"
        )
    return float(value)


def _count(value: Any, label: str, *, minimum: int) -> int:
    """校验整数配置，拒绝布尔值。

    Args:
        value: 待校验的整数。
        label: 错误信息中的配置键。
        minimum: 允许的最小值。

    Returns:
        原整数。

    Raises:
        ValueError: 不是整数或小于 minimum。
    """

    if type(value) is not int or value < minimum:
        raise ValueError(f"{label} must be an integer of at least {minimum}")
    return value


def _script_mapping(value: Any, label: str) -> Mapping[str, str]:
    """校验 JSON 可传输的条件到脚本映射并返回只读副本。

    Args:
        value: 待校验的映射。
        label: 错误信息中的配置键。

    Returns:
        键和值均为字符串的只读映射副本。

    Raises:
        TypeError: 不是映射，或键、值不是字符串。
    """

    if not isinstance(value, Mapping) or any(
        not isinstance(key, str) or not isinstance(item, str)
        for key, item in value.items()
    ):
        raise TypeError(f"{label} must map string conditions to string scripts")
    return MappingProxyType(dict(value))


@dataclass(frozen=True, slots=True)
class CrawlerOptions:
    """一次 Graph execution 中 Bricks 解释的 `crawler.*` 配置快照。

    普通执行配置仍是 JSON 映射；本类型只负责一次性解析、校验和生成 Bricks 的键，
    `crawler.` 前缀之外的业务键原样留给调用方。

    Attributes:
        session_reuse: 是否复用 Slot 会话，默认 True；False 使用节点装配的临时下载器。
        impersonate: 预设指纹覆盖名称，None 表示不覆盖。
        retry_max: 下载重试上限，None 回退 form 段级上限或 DEFAULT_RETRY_MAX。
        retry_delay: 下载重试等待秒数，默认 0；Retry(delay=...) 优先。
        proxy_rotate: 重试时 set_proxy 是否丢弃已用于下载的代理并重新选择，默认 True。
        proxy_retry: 代理错误不消耗 retry_max 的独立上限，默认 5；0 表示不单独计数。
        response_ok: 可信成功判断表达式或条件到脚本映射，None 使用默认状态码规则。
        response_scripts: conditional_scripts 的条件到脚本只读映射，默认空。
        form_stage: form 爬虫当前步骤段序号，默认 0。
    """

    session_reuse: bool = True
    impersonate: str | None = None
    retry_max: int | None = None
    retry_delay: float = 0
    proxy_rotate: bool = True
    proxy_retry: int = 5
    response_ok: str | Mapping[str, str] | None = None
    response_scripts: Mapping[str, str] = field(
        default_factory=lambda: MappingProxyType({})
    )
    form_stage: int = 0

    def __post_init__(self) -> None:
        """校验各字段的类型与取值，并冻结映射字段。

        Raises:
            TypeError: 字段类型错误。
            ValueError: 指纹名称为空或含首尾空白，重试上限、等待秒数、代理重试上限或段号为负数。
        """

        if type(self.session_reuse) is not bool:
            raise TypeError(f"{SESSION_REUSE} must be a boolean")
        if self.impersonate is not None:
            if not isinstance(self.impersonate, str):
                raise TypeError(f"{IMPERSONATE} must be a string or None")
            if not self.impersonate.strip() or self.impersonate != (
                self.impersonate.strip()
            ):
                raise ValueError(f"{IMPERSONATE} must be non-empty without whitespace")
        if self.retry_max is not None and (
            type(self.retry_max) is not int or self.retry_max < 0
        ):
            raise ValueError(f"{RETRY_MAX} must be a nonnegative integer")
        _seconds(self.retry_delay, RETRY_DELAY, positive=False)
        if type(self.proxy_rotate) is not bool:
            raise TypeError(f"{PROXY_ROTATE} must be a boolean")
        _count(self.proxy_retry, PROXY_RETRY, minimum=0)
        if self.response_ok is not None and not isinstance(self.response_ok, str):
            object.__setattr__(
                self, "response_ok", _script_mapping(self.response_ok, RESPONSE_OK)
            )
        object.__setattr__(
            self,
            "response_scripts",
            _script_mapping(self.response_scripts, RESPONSE_SCRIPTS),
        )
        if type(self.form_stage) is not int or self.form_stage < 0:
            raise ValueError(f"{FORM_STAGE} must be a non-negative integer")

    @classmethod
    def parse(cls, options: Mapping[str, Any]) -> CrawlerOptions:
        """从执行配置读取全部 `crawler.*` 键，拒绝未定义的键与实例级 `spider.*` 键。

        Args:
            options: 当前执行配置或待提交的任务配置。

        Returns:
            已校验的配置快照；缺省键使用字段默认值。

        Raises:
            TypeError: options 不是映射，或配置值类型错误。
            ValueError: 存在未知 `crawler.*` 键或任何 `spider.*` 键，或配置值非法。
        """

        if not isinstance(options, Mapping):
            raise TypeError("options must be a mapping")
        instance = sorted(
            key
            for key in options
            if isinstance(key, str) and key.startswith(SETTINGS_PREFIX)
        )
        if instance:
            raise ValueError(
                "spider settings are instance-level and cannot be set per task: "
                + ", ".join(instance)
            )
        unknown = sorted(
            key
            for key in options
            if isinstance(key, str) and key.startswith(PREFIX) and key not in _KEYS
        )
        if unknown:
            raise ValueError(f"unknown crawler options: {', '.join(unknown)}")
        return cls(
            session_reuse=options.get(SESSION_REUSE, True),
            impersonate=options.get(IMPERSONATE),
            retry_max=options.get(RETRY_MAX),
            retry_delay=options.get(RETRY_DELAY, 0),
            proxy_rotate=options.get(PROXY_ROTATE, True),
            proxy_retry=options.get(PROXY_RETRY, 5),
            response_ok=options.get(RESPONSE_OK),
            response_scripts=options.get(RESPONSE_SCRIPTS, {}),
            form_stage=options.get(FORM_STAGE, 0),
        )

    def to_options(self) -> dict[str, Any]:
        """生成只含非默认字段的 JSON 执行配置，可与业务键合并后提交。

        Returns:
            调用方拥有的新字典；映射字段转换为普通字典。
        """

        values: dict[str, Any] = {}
        if self.session_reuse is not True:
            values[SESSION_REUSE] = self.session_reuse
        if self.impersonate is not None:
            values[IMPERSONATE] = self.impersonate
        if self.retry_max is not None:
            values[RETRY_MAX] = self.retry_max
        if self.retry_delay:
            values[RETRY_DELAY] = self.retry_delay
        if self.proxy_rotate is not True:
            values[PROXY_ROTATE] = self.proxy_rotate
        if self.proxy_retry != 5:
            values[PROXY_RETRY] = self.proxy_retry
        if self.response_ok is not None:
            values[RESPONSE_OK] = (
                self.response_ok
                if isinstance(self.response_ok, str)
                else dict(self.response_ok)
            )
        if self.response_scripts:
            values[RESPONSE_SCRIPTS] = dict(self.response_scripts)
        if self.form_stage:
            values[FORM_STAGE] = self.form_stage
        return values


@dataclass(frozen=True, slots=True)
class SpiderSettings:
    """Spider 构造时读取的 `spider.*` 实例级配置，不写入任务也不参与 Work ID。

    Attributes:
        strict: 队列排空后是否以 RejectedTasksError 汇总报告本进程业务拒绝，默认 False。
        init_timeout: 单次初始化执行超时秒数，默认 None 不限制。
        lease: 初始化与交付租约秒数，默认 30。
        poll: 队列空轮询间隔秒数，默认 0.05。
        session_reuse: 是否为自建 Slot 自动装配并复用同步会话，默认 True。
        session_proxies: 自动会话模式下每个 Slot 按代理地址保留的会话上限，默认 4。
    """

    strict: bool = False
    init_timeout: float | None = None
    lease: float = 30
    poll: float = 0.05
    session_reuse: bool = True
    session_proxies: int = 4

    def __post_init__(self) -> None:
        """校验各字段的类型与取值，并把秒数规范为浮点数。

        Raises:
            TypeError: 布尔字段类型错误。
            ValueError: 秒数不是正的有限数，或会话上限不是正整数。
        """

        if type(self.strict) is not bool:
            raise TypeError(f"{RUN_STRICT} must be a boolean")
        if type(self.session_reuse) is not bool:
            raise TypeError(f"{AUTO_SESSION} must be a boolean")
        if self.init_timeout is not None:
            object.__setattr__(
                self,
                "init_timeout",
                _seconds(self.init_timeout, INIT_TIMEOUT, positive=True),
            )
        object.__setattr__(
            self, "lease", _seconds(self.lease, QUEUE_LEASE, positive=True)
        )
        object.__setattr__(self, "poll", _seconds(self.poll, QUEUE_POLL, positive=True))
        _count(self.session_proxies, SESSION_PROXIES, minimum=1)

    @classmethod
    def split(cls, options: Mapping[str, Any]) -> tuple[SpiderSettings, dict[str, Any]]:
        """把 Spider 的 options 拆成实例级配置与随任务持久化的执行配置。

        Args:
            options: 构造 Spider 时传入的点分配置。

        Returns:
            已校验的实例级配置，以及去除 `spider.*` 键后的新执行配置字典。

        Raises:
            TypeError: options 不是映射，或配置值类型错误。
            ValueError: 存在未知 `spider.*` 键，或配置值非法。
        """

        if not isinstance(options, Mapping):
            raise TypeError("options must be a mapping")
        settings = {
            key: value
            for key, value in options.items()
            if isinstance(key, str) and key.startswith(SETTINGS_PREFIX)
        }
        unknown = sorted(key for key in settings if key not in _SETTINGS)
        if unknown:
            raise ValueError(f"unknown spider settings: {', '.join(unknown)}")
        rest = {key: value for key, value in options.items() if key not in settings}
        return (
            cls(
                strict=settings.get(RUN_STRICT, False),
                init_timeout=settings.get(INIT_TIMEOUT),
                lease=settings.get(QUEUE_LEASE, 30),
                poll=settings.get(QUEUE_POLL, 0.05),
                session_reuse=settings.get(AUTO_SESSION, True),
                session_proxies=settings.get(SESSION_PROXIES, 4),
            ),
            rest,
        )

    def to_options(self) -> dict[str, Any]:
        """生成只含非默认字段的 `spider.*` 配置，可与执行配置合并后传给 Spider。

        Returns:
            调用方拥有的新字典。
        """

        values: dict[str, Any] = {}
        if self.strict:
            values[RUN_STRICT] = True
        if self.init_timeout is not None:
            values[INIT_TIMEOUT] = self.init_timeout
        if self.lease != 30:
            values[QUEUE_LEASE] = self.lease
        if self.poll != 0.05:
            values[QUEUE_POLL] = self.poll
        if self.session_reuse is not True:
            values[AUTO_SESSION] = False
        if self.session_proxies != 4:
            values[SESSION_PROXIES] = self.session_proxies
        return values
