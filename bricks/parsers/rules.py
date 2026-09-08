"""与解析引擎无关的字段规则、连续提取、多源收集和显式记录展开。"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum
from itertools import product
from types import MappingProxyType
from typing import Any, Literal, TypeAlias

from .base import Parser, check_expression


class _Missing(Enum):
    """以稳定身份表示没有提取值，复制后保持同一成员。"""

    VALUE = "missing"  # 唯一缺失标记，不与 JSON null 或假值混同。


MISSING = _Missing.VALUE  # 公共缺失标记，供自定义解析器和规则使用。


class MissingValueError(ValueError):
    """必填规则未得到值，区别于表达式错误和转换失败。"""


def _check_parser(parser: Parser | None) -> None:
    """校验注入对象提供协议要求的两个可调用方法。

    Args:
        parser: 解析器实例或继承当前解析器的 None。

    Raises:
        TypeError: 注入对象不是解析器实例。
    """
    if parser is not None and (
        isinstance(parser, type)
        or not callable(getattr(parser, "prepare", None))
        or not callable(getattr(parser, "extract", None))
    ):
        raise TypeError("parser must provide prepare() and extract()")


@dataclass(frozen=True)
class Rule:
    """描述一个字段的显式取值与处理顺序，可在不同解析器间复用。

    Attributes:
        expression: 非空查询表达式。
        parser: 当前规则的解析器，默认 None 继承调用方；实例归调用方所有。
        mode: value 保留原结果，first 要求列表并取首项；默认 value。
        default: 缺失时复制的默认值，默认 MISSING 表示省略字段。
        required: 默认 False；True 时缺失报错，不能同时设置 default。
        when: 默认 None；接收当前源，返回假时省略字段，不应用默认值。
        before: 默认 None；在提取前显式转换当前源，不缓存转换结果。
        transform: 默认 None；默认值处理后转换最终值，异常原样传播。
        missing: 默认 None 仅识别 MISSING；可显式增加缺失判定。
        options: 提取选项的独立快照，构造及每次调用均复制嵌套值。
    """

    expression: str
    parser: Parser | None = None
    mode: Literal["value", "first"] = "value"
    default: Any = MISSING
    required: bool = False
    when: Callable[[Any], bool] | None = None
    before: Callable[[Any], Any] | None = None
    transform: Callable[[Any], Any] | None = None
    missing: Callable[[Any], bool] | None = None
    options: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """校验规则配置并隔离选项和默认值。

        Raises:
            TypeError: 解析器、回调或选项类型错误。
            ValueError: 表达式、模式或默认值与必填约束冲突。
        """
        check_expression(self.expression)
        _check_parser(self.parser)
        if self.mode not in ("value", "first"):
            raise ValueError("mode must be 'value' or 'first'")
        if type(self.required) is not bool:
            raise TypeError("required must be a bool")
        if self.required and self.default is not MISSING:
            raise ValueError("required and default are mutually exclusive")
        for callback in (self.when, self.before, self.transform, self.missing):
            if callback is not None and not callable(callback):
                raise TypeError("rule callbacks must be callable")
        if not isinstance(self.options, Mapping) or any(
            not isinstance(key, str) for key in self.options
        ):
            raise TypeError("options must be a mapping with string keys")
        object.__setattr__(
            self, "options", MappingProxyType(deepcopy(dict(self.options)))
        )
        object.__setattr__(self, "default", deepcopy(self.default))


@dataclass(frozen=True)
class Constant:
    """为每条记录提供独立的常量值。

    Attributes:
        value: 构造时复制的值，每次提取再复制，不共享可变记录内容。
    """

    value: Any

    def __post_init__(self) -> None:
        """复制常量配置，使调用方后续修改原值不影响规则。"""
        object.__setattr__(self, "value", deepcopy(self.value))


@dataclass(frozen=True)
class Group:
    """按顺序选择第一个非缺失候选，不捕获候选执行中的异常。

    Attributes:
        rules: 非空值规则序列，支持 Rule、Constant、Group、Pipeline 和
            Collect；构造时保存为元组，各候选读取同一当前源。
        default: 所有候选缺失时复制的默认值，默认 MISSING 省略字段。
        required: 默认 False；True 时全部缺失报错，不能同时设置 default。
    """

    rules: Sequence[ValueRule]
    default: Any = MISSING
    required: bool = False

    def __post_init__(self) -> None:
        """固定候选顺序并校验缺失策略。

        Raises:
            TypeError: 候选不是值规则或 required 不是布尔值。
            ValueError: 候选为空或缺失策略冲突。
        """
        candidates = _freeze_values(self.rules)
        if not candidates:
            raise ValueError("Group requires at least one rule")
        if type(self.required) is not bool:
            raise TypeError("required must be a bool")
        if self.required and self.default is not MISSING:
            raise ValueError("required and default are mutually exclusive")
        object.__setattr__(self, "rules", candidates)
        object.__setattr__(self, "default", deepcopy(self.default))


@dataclass(frozen=True)
class Pipeline:
    """依次执行值规则或同步转换函数，将上一步结果作为下一步输入。

    Attributes:
        steps: 非空步骤序列，构造时固定为元组；步骤接受值规则或单参数
            同步函数。每步只执行一次，MISSING 终止后续步骤。未指定解析器
            的 Rule 始终继承调用处的解析器，不继承前一步的覆盖配置。
            输入和中间值不隐式复制，转换函数及其状态由调用方管理。
    """

    steps: Sequence[ValueRule | Callable[[Any], Any]]

    def __post_init__(self) -> None:
        """固定步骤顺序，拒绝空链和非规则、非函数步骤。

        Raises:
            TypeError: steps 不是序列或包含不支持的步骤。
            ValueError: 没有步骤。
        """
        if not isinstance(self.steps, Sequence) or isinstance(self.steps, (str, bytes)):
            raise TypeError("Pipeline.steps must be a sequence")
        steps = tuple(self.steps)
        if not steps:
            raise ValueError("Pipeline requires at least one step")
        if any(
            not isinstance(step, _VALUE_RULE_TYPES) and not callable(step)
            for step in steps
        ):
            raise TypeError("Pipeline accepts only value rules or callables")
        object.__setattr__(self, "steps", steps)


@dataclass(frozen=True)
class Collect:
    """在同一当前源上按序执行所有值规则，收集非缺失结果。

    Attributes:
        rules: 值规则序列，构造时固定为元组；允许为空，每个规则执行一次。
        mode: 默认 values 保留各规则结果的形状；concat 要求非缺失结果均为
            list 并拼接一层，不递归展开、不去重、不删除列表内部的 MISSING。
            两种模式均跳过整个规则返回的 MISSING，全部缺失时返回 []。
            返回新的外层列表，内部可变值仍遵循来源规则的所有权。
    """

    rules: Sequence[ValueRule]
    mode: Literal["values", "concat"] = "values"

    def __post_init__(self) -> None:
        """固定规则顺序并校验收集模式。

        Raises:
            TypeError: rules 不是值规则序列。
            ValueError: mode 不受支持。
        """
        if self.mode not in ("values", "concat"):
            raise ValueError("Collect.mode must be 'values' or 'concat'")
        object.__setattr__(self, "rules", _freeze_values(self.rules))


@dataclass(frozen=True)
class Rows:
    """将显式选中的列表逐项映射成记录，不执行隐式笛卡尔积。

    Attributes:
        select: 返回列表的表达式或值规则；字符串使用当前解析器。
        fields: 相对每个选中对象执行的记录规则，支持映射、Rows、Product
            或拼接序列；构造时固定结构，各对象的结果按选择顺序追加。
        parser: 行选择和字段默认使用的解析器，None 继承外层实例。
    """

    select: str | ValueRule
    fields: RuleSet
    parser: Parser | None = None

    def __post_init__(self) -> None:
        """校验行选择规则并保存字段结构快照。

        Raises:
            TypeError: 行选择、字段结构或解析器类型不合法。
        """
        if isinstance(self.select, str):
            object.__setattr__(self, "select", Rule(self.select))
        elif not isinstance(self.select, _VALUE_RULE_TYPES):
            raise TypeError("Rows.select must be an expression or value rule")
        _check_parser(self.parser)
        object.__setattr__(self, "fields", _freeze_rules(self.fields))


@dataclass(frozen=True)
class Product:
    """在同一当前源上执行各分支，将结果按笛卡尔积合并成记录。

    配合 Rows 可逐层展开子记录并继承父字段；每个分支只执行一次，
    合并仅发生在记录顶层，同名嵌套对象不递归合并。

    Attributes:
        branches: 有序记录规则序列，构造时递归固定；空序列产生一条空记录。
        keep_empty: 默认 False，任一空分支令结果为空；True 将空分支视为
            一条空记录，保留其他分支字段，不为缺失字段补 None。
        on_conflict: 同名字段处理，默认 raise 报错，first 保留先出现字段，
            last 使用后出现字段；值相同也属于冲突。
    """

    branches: Sequence[RuleSet]
    keep_empty: bool = False
    on_conflict: Literal["raise", "first", "last"] = "raise"

    def __post_init__(self) -> None:
        """校验组合策略并保存分支规则快照。

        Raises:
            TypeError: 分支不是规则序列或 keep_empty 不是布尔值。
            ValueError: 冲突策略不受支持。
        """
        if not isinstance(self.branches, Sequence) or isinstance(
            self.branches, (str, bytes)
        ):
            raise TypeError("Product.branches must be a sequence of record rules")
        if type(self.keep_empty) is not bool:
            raise TypeError("keep_empty must be a bool")
        if self.on_conflict not in ("raise", "first", "last"):
            raise ValueError("on_conflict must be 'raise', 'first', or 'last'")
        object.__setattr__(
            self, "branches", tuple(_freeze_rules(branch) for branch in self.branches)
        )


ValueRule: TypeAlias = Rule | Constant | Group | Pipeline | Collect
Field: TypeAlias = "str | ValueRule | Rows | Product | Mapping[str, Any]"
RuleSet: TypeAlias = "Mapping[str, Field] | Rows | Product | Sequence[RuleSet]"
_VALUE_RULE_TYPES = (
    Rule,
    Constant,
    Group,
    Pipeline,
    Collect,
)  # 可递归组合的值规则类型。


def _freeze_values(rules: Sequence[ValueRule]) -> tuple[ValueRule, ...]:
    """验证值规则序列并固定结构，不接受裸表达式或隐式回调。

    Args:
        rules: 值规则序列，不包含记录规则。

    Returns:
        保留声明顺序的规则元组。

    Raises:
        TypeError: 输入不是序列或包含非值规则。
    """
    if not isinstance(rules, Sequence) or isinstance(rules, (str, bytes)):
        raise TypeError("value rules must be a sequence")
    result = tuple(rules)
    if any(not isinstance(rule, _VALUE_RULE_TYPES) for rule in result):
        raise TypeError("expected Rule, Constant, Group, Pipeline, or Collect")
    return result


def _freeze_rules(rules: RuleSet) -> RuleSet:
    """递归固定记录规则，同一规则结构可用于根、行内部或组合分支。

    Args:
        rules: 字段映射、Rows、Product 或按顺序拼接的记录规则序列。

    Returns:
        已验证的只读映射、规则实例或固定规则元组。

    Raises:
        TypeError: 不是支持的记录规则，即使源中没有数据也必须报错。
    """
    if isinstance(rules, (Rows, Product)):
        return rules
    if isinstance(rules, Mapping):
        return _freeze_fields(rules)
    if isinstance(rules, Sequence) and not isinstance(rules, (str, bytes)):
        return tuple(_freeze_rules(rule) for rule in rules)
    raise TypeError("record rules must be a mapping, Rows, Product, or a sequence")


def _freeze_fields(fields: Mapping[str, Any]) -> Mapping[str, Any]:
    """验证字段映射并递归固定嵌套结构，裸字符串是表达式简写。

    Args:
        fields: 输出字段名到字段规则的映射。

    Returns:
        独立的只读映射；规则实例本身不被修改或绑定引擎。

    Raises:
        TypeError: 字段名或规则类型不合法。
    """
    if not isinstance(fields, Mapping):
        raise TypeError("fields must be a mapping")
    result: dict[str, Any] = {}
    for name, value in fields.items():
        if not isinstance(name, str):
            raise TypeError("field names must be strings")
        if isinstance(value, str):
            value = Rule(value)
        elif isinstance(value, Mapping):
            value = _freeze_fields(value)
        elif not isinstance(value, (*_VALUE_RULE_TYPES, Rows, Product)):
            raise TypeError(f"invalid rule for field {name!r}; use Constant for values")
        result[name] = value
    return MappingProxyType(result)


def _fallback(value: Any, default: Any, required: bool, label: str) -> Any:
    """只对缺失标记应用必填约束或独立默认值。

    Args:
        value: 当前结果或 MISSING。
        default: 缺失默认值。
        required: 是否必须取得值。
        label: 缺失错误的表达式说明。

    Returns:
        已有结果、默认值副本或 MISSING。

    Raises:
        MissingValueError: 必填规则缺失。
    """
    if value is not MISSING:
        return value
    if required:
        raise MissingValueError(f"required value is missing: {label}")
    return deepcopy(default)


def _apply(source: Any, rule: Rule, parser: Parser) -> Any:
    """执行条件、源转换、提取、数量选择、缺失处理和结果转换。

    Args:
        source: 当前文档或行对象。
        rule: 字段或行选择规则。
        parser: 当前默认解析器。

    Returns:
        处理后的值或 MISSING。

    Raises:
        TypeError: first 模式收到非列表结果。
        MissingValueError: 必填字段缺失。
    """
    if rule.when is not None and not rule.when(source):
        return MISSING
    engine = rule.parser if rule.parser is not None else parser
    prepared = rule.before(source) if rule.before is not None else source
    value = engine.extract(
        engine.prepare(prepared), rule.expression, **deepcopy(dict(rule.options))
    )
    if rule.mode == "first" and value is not MISSING:
        if not isinstance(value, list):
            raise TypeError(f"first mode requires a list: {rule.expression}")
        value = value[0] if value else MISSING
    if value is not MISSING and rule.missing is not None and rule.missing(value):
        value = MISSING
    value = _fallback(value, rule.default, rule.required, rule.expression)
    if value is not MISSING and rule.transform is not None:
        value = rule.transform(value)
    return value


def _records(source: Any, rows: Rows, parser: Parser) -> list[dict[str, Any]]:
    """显式展开列表，在每个对象作用域内执行记录规则。

    Args:
        source: 当前文档或行对象。
        rows: 行选择及映射、组合或拼接规则。
        parser: 外层默认解析器。

    Returns:
        按选择顺序产生的记录列表，缺失选择返回空列表。

    Raises:
        TypeError: 行选择返回非列表值。
    """
    engine = rows.parser if rows.parser is not None else parser
    selection = rows.select
    assert not isinstance(selection, str)
    selected = _value(source, selection, engine, "Rows.select")
    if selected is MISSING:
        return []
    if not isinstance(selected, list):
        raise TypeError("Rows selection must return a list")
    return [
        record for item in selected for record in _evaluate(item, rows.fields, engine)
    ]


def _product(source: Any, schema: Product, parser: Parser) -> list[dict[str, Any]]:
    """每个分支提取一次，再按声明顺序组装独立的结果记录。

    Args:
        source: 所有分支共同使用的当前源，不使用其他分支的提取结果。
        schema: 分支、空分支处理及冲突策略。
        parser: 当前默认解析器。

    Returns:
        最右分支变化最快的记录列表；每条合并记录深复制，避免组合之间
        共享可变字段。结果数量为各分支记录数之积。

    Raises:
        ValueError: raise 策略遇到重名字段；其他提取或复制异常原样传播。
    """
    factors = [_evaluate(source, branch, parser) for branch in schema.branches]
    if schema.keep_empty:
        factors = [factor if factor else [{}] for factor in factors]
    # product 直接枚举最终组合，避免构造逐层增长的中间笛卡尔积。
    result = []
    for combination in product(*factors):
        merged: dict[str, Any] = {}
        for index, record in enumerate(combination):
            for name, value in record.items():
                if name in merged:
                    if schema.on_conflict == "raise":
                        raise ValueError(
                            f"Product field conflict at branch {index + 1}: {name!r}"
                        )
                    if schema.on_conflict == "first":
                        continue
                merged[name] = value
        result.append(deepcopy(merged))
    return result


def _value(source: Any, rule: ValueRule, parser: Parser, label: str) -> Any:
    """递归执行值规则，保持串联、候选回退与同源收集的作用域边界。

    Args:
        source: 当前文档、行对象或上一步的结果。
        rule: 单个值规则。
        parser: 调用处默认解析器，不随单条 Rule 的覆盖而改变。
        label: 用于必填及收集类型错误的字段或选择位置。

    Returns:
        提取结果或 MISSING；收集始终返回新列表。

    Raises:
        TypeError: concat 收到了非列表结果。
        MissingValueError: 候选或字段必填约束未满足；其他异常原样传播。
    """
    if isinstance(rule, Rule):
        return _apply(source, rule, parser)
    if isinstance(rule, Constant):
        return deepcopy(rule.value)
    if isinstance(rule, Group):
        value = MISSING
        for candidate in rule.rules:
            value = _value(source, candidate, parser, label)
            if value is not MISSING:
                break
        return _fallback(value, rule.default, rule.required, label)
    if isinstance(rule, Pipeline):
        value = source
        for step in rule.steps:
            if value is MISSING:
                break
            value = (
                _value(value, step, parser, label)
                if isinstance(step, _VALUE_RULE_TYPES)
                else step(value)
            )
        return value
    result = []
    for index, candidate in enumerate(rule.rules):
        value = _value(source, candidate, parser, label)
        if value is MISSING:
            continue
        if rule.mode == "values":
            result.append(value)
        else:
            if not isinstance(value, list):
                raise TypeError(
                    f"Collect concat requires a list at {label}, rule {index + 1}"
                )
            result.extend(value)
    return result


def _record(source: Any, fields: Mapping[str, Any], parser: Parser) -> dict[str, Any]:
    """将字段规则应用到同一源对象，嵌套映射保留对象结构。

    Args:
        source: 当前文档或行对象。
        fields: 已验证的字段映射。
        parser: 当前默认解析器。

    Returns:
        单条记录，仅省略结果为 MISSING 的字段。
    """
    result: dict[str, Any] = {}
    for name, rule in fields.items():
        if isinstance(rule, _VALUE_RULE_TYPES):
            value = _value(source, rule, parser, name)
        elif isinstance(rule, Rows):
            value = _records(source, rule, parser)
        elif isinstance(rule, Product):
            value = _product(source, rule, parser)
        else:
            value = _record(source, rule, parser)
        if value is not MISSING:
            result[name] = value
    return result


def _evaluate(source: Any, rules: RuleSet, parser: Parser) -> list[dict[str, Any]]:
    """在当前源作用域内执行已验证的记录规则，不重新解析整个文档。

    Args:
        source: 当前文档或 Rows 选中的单个对象。
        rules: 已固定的字段映射、Rows、Product 或拼接序列。
        parser: 当前默认解析器。

    Returns:
        按规则声明及源选择顺序生成的记录列表。
    """
    if isinstance(rules, Mapping):
        return [_record(source, rules, parser)]
    if isinstance(rules, Rows):
        return _records(source, rules, parser)
    if isinstance(rules, Product):
        return _product(source, rules, parser)
    return [record for rule in rules for record in _evaluate(source, rule, parser)]


def match(source: Any, rules: RuleSet, *, parser: Parser) -> list[dict[str, Any]]:
    """使用窄解析协议执行单组或多组批量规则，不依赖具体解析器类别。

    Args:
        source: 原输入或准备好的文档，当前默认解析器负责准备。
        rules: 映射生成一条记录，Rows 显式展开，Product 合并笛卡尔积，
            多组序列依次拼接；各类记录规则支持递归组合。
        parser: 提供 prepare 和 extract 的解析器实例，无需继承 BaseParser。

    Returns:
        有序字典记录列表；空规则序列返回 []，空映射生成 [{}]。

    Raises:
        TypeError: 解析器、规则或列表展开结果类型不合法。
        ValueError: Product 使用 raise 策略时出现同名字段。
        MissingValueError: 必填值缺失；其他提取或回调异常原样传播。
    """
    _check_parser(parser)
    if parser is None:
        raise TypeError("parser is required")
    schema = _freeze_rules(rules)
    if isinstance(schema, tuple) and not schema:
        return []
    prepared = parser.prepare(source)
    return _evaluate(prepared, schema, parser)
