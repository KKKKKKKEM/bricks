"""爬虫执行配置 `crawler.*` 命名空间的统一解析与校验。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

PREFIX = "crawler."  # Bricks 保留的执行配置命名空间，未知键明确拒绝。
SESSION_REUSE = "crawler.session.reuse"  # 是否复用 Slot 会话，布尔值，默认 True。
IMPERSONATE = "crawler.fingerprint.impersonate"  # 当前执行的预设指纹覆盖名称。
RETRY_MAX = "crawler.retry.max"  # 当前任务的下载重试上限，非负整数。
RESPONSE_OK = "crawler.response.ok"  # 当前任务的成功判断规则，语法同 bypass 的 ok。
RESPONSE_SCRIPTS = "crawler.response.scripts"  # conditional_scripts 的条件到脚本映射。
FORM_STAGE = "crawler.form.stage"  # form 爬虫的当前步骤段序号，从 0 开始。
_KEYS = frozenset(
    (SESSION_REUSE, IMPERSONATE, RETRY_MAX, RESPONSE_OK, RESPONSE_SCRIPTS, FORM_STAGE)
)  # 当前版本支持的全部 crawler.* 键。


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
        retry_max: 下载重试上限，None 回退 form 段级或 Spider 默认值。
        response_ok: 可信成功判断表达式或条件到脚本映射，None 使用默认状态码规则。
        response_scripts: conditional_scripts 的条件到脚本只读映射，默认空。
        form_stage: form 爬虫当前步骤段序号，默认 0。
    """

    session_reuse: bool = True
    impersonate: str | None = None
    retry_max: int | None = None
    response_ok: str | Mapping[str, str] | None = None
    response_scripts: Mapping[str, str] = field(
        default_factory=lambda: MappingProxyType({})
    )
    form_stage: int = 0

    def __post_init__(self) -> None:
        """校验各字段的类型与取值，并冻结映射字段。

        Raises:
            TypeError: 字段类型错误。
            ValueError: 指纹名称为空或含首尾空白，重试上限或段号为负数。
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
        """从执行配置读取全部 `crawler.*` 键，拒绝当前版本未定义的键。

        Args:
            options: 当前执行配置或待提交的任务配置。

        Returns:
            已校验的配置快照；缺省键使用字段默认值。

        Raises:
            TypeError: options 不是映射，或配置值类型错误。
            ValueError: 存在未知 `crawler.*` 键，或配置值非法。
        """

        if not isinstance(options, Mapping):
            raise TypeError("options must be a mapping")
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
