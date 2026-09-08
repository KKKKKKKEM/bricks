"""验证连续提取与同源收集的作用域、缺失、组合和复用契约。"""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from typing import Any

import pytest
from cssselect import SelectorError

import bricks
from bricks.parsers import (
    MISSING,
    Collect,
    Constant,
    CssParser,
    Group,
    JmesPathParser,
    JsonPathParser,
    MissingValueError,
    Pipeline,
    Product,
    RegexParser,
    Rows,
    Rule,
    XPathParser,
    match,
)


def test_pipeline_extracts_html_json_and_regex_in_order() -> None:
    """链中各解析器接收前一步结果，JSON 解码由独立步骤显式完成。"""
    source = '<script type="application/json">{"price": "CNY 12.50"}</script>'
    price = Pipeline(
        (
            Rule("script", mode="first", options={"output": "text"}),
            json.loads,
            Rule("price", parser=JmesPathParser()),
            Rule(r"\d+\.\d+", parser=RegexParser(), mode="first"),
            float,
        )
    )
    assert CssParser().match(source, {"price": price}) == [{"price": 12.5}]


def test_pipeline_does_not_decode_json_or_rebind_default_parser() -> None:
    """覆盖仅限当前 Rule，字符串仍是值且不会自动成为 JSON 文档。"""
    parser = JmesPathParser()
    rules = {
        "literal": Pipeline((Rule("payload"), Rule("$", parser=JsonPathParser()))),
        "name": Pipeline(
            (
                Rule("payload"),
                json.loads,
                Rule("$", parser=JsonPathParser(), mode="first"),
                Rule("name"),
            )
        ),
    }
    assert parser.match({"payload": '{"name":"A"}'}, rules) == [
        {
            "literal": ['{"name":"A"}'],
            "name": "A",
        }
    ]


@pytest.mark.parametrize("value", [None, False, 0, "", [], {}])
def test_pipeline_passes_every_nonmissing_value(value: Any) -> None:
    """空值和假值仍交给后续步骤，不被隐式视为缺失。

    Args:
        value: 当前验证的有效值。
    """
    chain = Pipeline((Constant(value), lambda result: [result]))
    assert JmesPathParser().match({}, {"value": chain}) == [{"value": [value]}]


def test_pipeline_stops_on_missing_and_group_can_fallback() -> None:
    """缺失跳过后续函数，单步默认值可继续，整链回退交给 Group。"""
    calls: list[Any] = []
    missing = Pipeline((Rule("$.absent", mode="first"), calls.append))
    assert JsonPathParser().match({}, {"value": missing}) == [{}]
    assert calls == []
    assert JsonPathParser().match(
        {},
        {
            "value": Group((missing, Constant("fallback"))),
        },
    ) == [{"value": "fallback"}]
    assert JsonPathParser().match(
        {},
        {
            "value": Pipeline(
                (
                    Rule("$.absent", mode="first", default=" fallback "),
                    str.strip,
                )
            ),
        },
    ) == [{"value": "fallback"}]
    assert JsonPathParser().match(
        {},
        {
            "value": Pipeline((lambda source: MISSING, calls.append)),
        },
    ) == [{}]
    assert calls == []
    with pytest.raises(MissingValueError, match="value"):
        JsonPathParser().match({}, {"value": Group((missing,), required=True)})
    with pytest.raises(MissingValueError):
        JsonPathParser().match(
            {},
            {
                "value": Group(
                    (Pipeline((Rule("$.absent", mode="first", required=True),)),),
                    default="unused",
                ),
            },
        )


def test_collect_preserves_shapes_and_only_skips_whole_missing_results() -> None:
    """收集保留空值、嵌套列表、重复项与内部缺失位置。"""
    values = [None, False, 0, "", [], {"x": 1}, [[1]], [MISSING], "same", "same"]
    rules = Collect((Constant(MISSING), *(Constant(value) for value in values)))
    assert JmesPathParser().match({}, {"values": rules}) == [{"values": values}]
    assert JmesPathParser().match(
        {},
        {
            "empty": Collect(()),
            "missing": Collect((Constant(MISSING),)),
            "fallback": Group((Collect(()), Constant("unused"))),
        },
    ) == [{"empty": [], "missing": [], "fallback": []}]


def test_collect_concat_flattens_exactly_one_level() -> None:
    """拼接只展开每条规则的外层列表，空列表不阻止后续收集。"""
    rule = Collect(
        (
            Constant([]),
            Constant(MISSING),
            Constant([1, [2, 3], None, MISSING]),
            Constant([1, False, 0]),
        ),
        mode="concat",
    )
    assert JmesPathParser().match({}, {"values": rule}) == [
        {
            "values": [1, [2, 3], None, MISSING, 1, False, 0],
        }
    ]
    assert JmesPathParser().match({}, {"empty": Collect((), mode="concat")}) == [
        {"empty": []},
    ]


@pytest.mark.parametrize("value", [None, False, 0, "abc", {"x": 1}, (1, 2)])
def test_collect_concat_rejects_nonlists(value: Any) -> None:
    """拼接不按字符串、映射或其他可迭代类型隐式展开。

    Args:
        value: 不允许拼接的非列表结果。
    """
    with pytest.raises(TypeError, match="phones, rule 2"):
        JmesPathParser().match(
            {},
            {
                "phones": Collect((Constant([]), Constant(value)), mode="concat"),
            },
        )


def test_collect_combines_parsers_and_pipelines_from_the_same_source() -> None:
    """CSS、XPath 与 JSON 链各自读取当前页面，结果按声明顺序收集。"""
    source = (
        '<main><a data-phone="138">138</a>'
        '<meta name="phone" content="010">'
        '<script>{"phones": ["400", "138"]}</script></main>'
    )
    rules = {
        "phones": Collect(
            (
                Rule("a", options={"attribute": "data-phone"}),
                Rule("//meta/@content", parser=XPathParser()),
                Pipeline(
                    (
                        Rule("script", mode="first", options={"output": "text"}),
                        json.loads,
                        Rule("phones", parser=JmesPathParser()),
                    )
                ),
            ),
            mode="concat",
        ),
    }
    assert CssParser().match(source, rules) == [
        {"phones": ["138", "010", "400", "138"]}
    ]


def test_nested_pipeline_collect_and_group_use_the_correct_current_source() -> None:
    """链内收集共享中间值，候选回退和嵌套链不改变外层其他字段的源。"""
    rule = Pipeline(
        (
            Rule("payload"),
            Collect(
                (
                    Group(
                        (
                            Rule("missing", missing=lambda value: value is None),
                            Rule("a"),
                        )
                    ),
                    Pipeline((Rule("b"), str.upper)),
                    Collect((Rule("a"),)),
                )
            ),
            lambda values: {"joined": "/".join(values[:2]), "nested": values[2]},
            Rule("@"),
        )
    )
    assert JmesPathParser().match(
        {"payload": {"a": "x", "b": "y"}, "id": 1},
        {
            "result": rule,
            "id": "id",
        },
    ) == [{"result": {"joined": "x/Y", "nested": ["x"]}, "id": 1}]


def test_collect_executes_each_rule_once_and_group_short_circuits() -> None:
    """收集按序执行所有规则一次，Group 命中后跳过后续候选。"""
    calls: list[str] = []

    def observe(value: str) -> str:
        """记录规则结果以验证调用次数和顺序。

        Args:
            value: 当前规则结果。

        Returns:
            原结果。
        """
        calls.append(value)
        return value

    rule = Collect(
        (
            Pipeline((Rule("a"), observe)),
            Group((Rule("b", transform=observe), Rule("c", transform=observe))),
            Rule("c", transform=observe),
        )
    )
    assert JmesPathParser().match({"a": "A", "b": "B", "c": "C"}, {"v": rule}) == [
        {"v": ["A", "B", "C"]},
    ]
    assert calls == ["A", "B", "C"]


def test_pipeline_rows_and_collect_product_keep_parent_scopes() -> None:
    """跨格式链选择商品后，子层收集与规格组合只使用对应商品的源。"""
    source = (
        '<script>{"products": ['
        + json.dumps(
            {
                "sku": "A",
                "colors": ["red", "blue"],
                "sizes": ["S", "M"],
            }
        )
        + ","
        + json.dumps(
            {
                "sku": "B",
                "colors": ["black"],
                "sizes": ["L"],
            }
        )
        + "]}</script>"
    )
    select = Pipeline(
        (
            Rule(
                "script", parser=CssParser(), mode="first", options={"output": "text"}
            ),
            json.loads,
            Rule("products"),
        )
    )
    records = CssParser().match(
        source,
        Rows(
            select,
            Product(
                (
                    {"sku": "sku", "meta": {"ids": Collect((Rule("sku"),))}},
                    Rows(Collect((Rule("colors"),), mode="concat"), {"color": "@"}),
                    Rows("sizes", {"size": "@"}),
                )
            ),
            parser=JmesPathParser(),
        ),
    )
    expected = [
        {"sku": sku, "meta": {"ids": [sku]}, "color": color, "size": size}
        for sku, colors, sizes in (
            ("A", ["red", "blue"], ["S", "M"]),
            ("B", ["black"], ["L"]),
        )
        for color in colors
        for size in sizes
    ]
    assert records == expected
    records[0]["meta"]["ids"].append("changed")
    assert records[1]["meta"]["ids"] == ["A"]
    assert CssParser().match("<main/>", Rows(select, {}, parser=JmesPathParser())) == []
    with pytest.raises(TypeError, match="must return a list"):
        JmesPathParser().match({}, Rows(Pipeline((Constant("abc"),)), {}))


@pytest.mark.parametrize(
    "error", [ValueError("bad callback"), asyncio.CancelledError()]
)
@pytest.mark.parametrize("wrapper", [Pipeline, Collect])
def test_composition_propagates_callback_errors_and_cancellation(
    error: BaseException,
    wrapper: Any,
) -> None:
    """转换错误和取消保持原异常身份，不被缺失或回退策略吞掉。

    Args:
        error: 回调抛出的异常实例。
        wrapper: 当前验证的组合规则类型。
    """
    calls: list[Any] = []

    def fail(value: Any) -> Any:
        """抛出指定异常以检查透传身份。

        Args:
            value: 当前步骤的输入。

        Raises:
            BaseException: 测试指定的异常。
        """
        raise error

    composed = wrapper((Rule("@", transform=fail), Pipeline((calls.append,))))
    with pytest.raises(type(error)) as caught:
        JmesPathParser().match({}, {"v": Group((composed,), default="unused")})
    assert caught.value is error
    assert calls == []


def test_composition_keeps_native_query_and_decode_errors() -> None:
    """已收集结果和空结果均不会掩盖后续的原生解析异常。"""
    with pytest.raises(SelectorError):
        CssParser().match("<p/>", {"v": Collect((Rule("missing"), Rule("[")))})
    with pytest.raises(json.JSONDecodeError):
        JmesPathParser().match(
            "invalid",
            {
                "v": Group((Pipeline((json.loads,)),), default="unused"),
            },
        )


class _MappingParser:
    """无需继承基类的字典解析器，不保存当前源或查询状态。"""

    def prepare(self, source: Any) -> Any:
        """直接使用当前输入。

        Args:
            source: 调用方拥有的输入。

        Returns:
            原输入。
        """
        return source

    def extract(self, source: Any, expression: str, **options: Any) -> Any:
        """按键读取值，缺失使用公共标记。

        Args:
            source: 待查询字典。
            expression: 字典键。
            **options: 不支持附加选项。

        Returns:
            原值或 MISSING。

        Raises:
            TypeError: 传入附加选项。
        """
        if options:
            raise TypeError("unsupported options")
        return source.get(expression, MISSING)


def test_composition_reuses_rules_with_third_party_parsers_and_threads() -> None:
    """结构化解析器可替换且同一规则并发复用时不持有执行状态。"""
    parser = _MappingParser()
    rule = Pipeline((Rule("payload"), Collect((Rule("id"), Constant([])))))

    def run(index: int) -> list[Any]:
        """对独立输入复用同一规则。

        Args:
            index: 当前输入标识。

        Returns:
            提取出的值列表。
        """
        return match({"payload": {"id": index}}, {"v": rule}, parser=parser)[0]["v"]

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(run, range(20)))
    assert results == [[index, []] for index in range(20)]
    results[0][1].append("changed")
    assert results[1][1] == []
    assert match({"payload": {"id": 1}}, {"v": rule}, parser=JmesPathParser()) == [
        {"v": [1, []]},
    ]


def test_composition_freezes_sequences_and_copies_defaults_and_constants() -> None:
    """构造后修改原序列不影响规则，输出常量和默认值逐次隔离。"""
    steps = [Rule("missing", default=[])]
    pipeline = Pipeline(steps)
    rules = [pipeline, Constant({"tags": []})]
    collect = Collect(rules)
    steps.clear()
    rules.clear()
    first = match({}, {"v": collect}, parser=_MappingParser())[0]["v"]
    first[0].append(1)
    first[1]["tags"].append(2)
    assert match({}, {"v": collect}, parser=_MappingParser()) == [
        {"v": [[], {"tags": []}]}
    ]
    with pytest.raises(FrozenInstanceError):
        collect.mode = "concat"  # type: ignore[misc]


def test_collect_returns_a_new_list_but_preserves_input_value_ownership() -> None:
    """收集不深复制原文档节点或容器，外层列表本身逐次新建。"""
    source = {"payload": {"tags": []}}
    rule = Collect((Rule("payload"),))
    first = JmesPathParser().match(source, {"v": rule})[0]["v"]
    second = JmesPathParser().match(source, {"v": rule})[0]["v"]
    assert first is not second
    assert first[0] is second[0] is source["payload"]


@pytest.mark.parametrize("wrapper", [Pipeline, Collect, Group])
@pytest.mark.parametrize(
    "invalid", ["x", {"x": Rule("x")}, 1, None, ["x"], [Rows("x", {})]]
)
def test_composition_rejects_invalid_configuration(wrapper: Any, invalid: Any) -> None:
    """无数据执行前就拒绝不支持的容器和步骤，不猜测裸字符串含义。

    Args:
        wrapper: 当前验证的组合规则类型。
        invalid: 非法序列或成员。
    """
    with pytest.raises(TypeError):
        wrapper(invalid)


def test_composition_empty_pipeline_modes_and_public_boundary() -> None:
    """空链和无效模式明确失败，新增类型只从解析子包导出。"""
    with pytest.raises(ValueError):
        Pipeline(())
    with pytest.raises(ValueError):
        Collect((), mode="flatten")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Collect((str.strip,))  # type: ignore[arg-type]
    assert not hasattr(bricks, "Pipeline")
    assert not hasattr(bricks, "Collect")
