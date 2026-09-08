"""验证批量解析中的笛卡尔组合、父子关联和多层记录组装。"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from jmespath.exceptions import JMESPathError

from bricks.parsers import (
    MISSING,
    Constant,
    CssParser,
    Group,
    JmesPathParser,
    JsonPathParser,
    MissingValueError,
    Parser,
    Product,
    RegexParser,
    Rows,
    Rule,
    XPathParser,
    match,
)


def test_product_combines_two_by_three_in_declared_order() -> None:
    """两个分支的两条与三条记录生成六条，最右分支变化最快。"""
    source = {"colors": ["red", "blue"], "sizes": ["S", "M", "L"]}
    result = JmesPathParser().match(
        source,
        Product((Rows("colors", {"color": "@"}), Rows("sizes", {"size": "@"}))),
    )
    assert result == [
        {"color": "red", "size": "S"},
        {"color": "red", "size": "M"},
        {"color": "red", "size": "L"},
        {"color": "blue", "size": "S"},
        {"color": "blue", "size": "M"},
        {"color": "blue", "size": "L"},
    ]
    assert JmesPathParser().match(
        {"values": [1, 1]}, Product((Rows("values", {"value": "@"}), {}))
    ) == [{"value": 1}, {"value": 1}]


def test_parent_child_and_grandchild_records_keep_their_scope() -> None:
    """跨三层展开时继承本链父字段，不把其他订单或商品的子记录配过来。"""
    source = {
        "shop": "store",
        "orders": [
            {
                "id": "o1",
                "items": [
                    {"sku": "A", "variants": [{"size": "S"}, {"size": "M"}]},
                    {"sku": "B", "variants": [{"size": "L"}]},
                ],
            },
            {
                "id": "o2",
                "items": [
                    {"sku": "C", "variants": [{"size": "XL"}]},
                ],
            },
        ],
    }
    rules = Product(
        (
            {"shop": "shop"},
            Rows(
                "orders",
                Product(
                    (
                        {"order_id": "id"},
                        Rows(
                            "items",
                            Product(
                                (
                                    {"sku": "sku"},
                                    Rows("variants", {"size": "size"}),
                                )
                            ),
                        ),
                    )
                ),
            ),
        )
    )
    assert JmesPathParser().match(source, rules) == [
        {"shop": "store", "order_id": "o1", "sku": "A", "size": "S"},
        {"shop": "store", "order_id": "o1", "sku": "A", "size": "M"},
        {"shop": "store", "order_id": "o1", "sku": "B", "size": "L"},
        {"shop": "store", "order_id": "o2", "sku": "C", "size": "XL"},
    ]


def test_many_parents_do_not_use_string_prefixes_for_correlation() -> None:
    """十二个父对象按真实作用域关联子记录，索引 1 不会与 10 或 11 混同。"""
    source = {
        "parents": [{"id": index, "children": [index * 10]} for index in range(12)]
    }
    rules = Rows(
        "parents",
        Product(
            (
                {"parent": "id"},
                Rows("children", {"child": "@"}),
            )
        ),
    )
    assert JmesPathParser().match(source, rules) == [
        {"parent": index, "child": index * 10} for index in range(12)
    ]


def test_css_sibling_product_and_xpath_fields_share_current_element() -> None:
    """CSS 在每个商品内交叉选择尺寸与颜色，XPath 字段使用相同当前节点。"""
    source = """<main>
      <article id="A"><b class="size">S</b><b class="size">M</b>
        <i>red</i><i>blue</i><i>green</i></article>
      <article id="B"><b class="size">L</b><i>black</i></article>
    </main>"""
    rules = Rows(
        "article",
        Product(
            (
                {"sku": Rule("string(@id)", parser=XPathParser())},
                Rows(".size", {"size": Rule("string(.)", parser=XPathParser())}),
                Rows("i", {"color": Rule("string(.)", parser=XPathParser())}),
            )
        ),
    )
    assert CssParser().match(source, rules) == [
        {"sku": "A", "size": "S", "color": "red"},
        {"sku": "A", "size": "S", "color": "blue"},
        {"sku": "A", "size": "S", "color": "green"},
        {"sku": "A", "size": "M", "color": "red"},
        {"sku": "A", "size": "M", "color": "blue"},
        {"sku": "A", "size": "M", "color": "green"},
        {"sku": "B", "size": "L", "color": "black"},
    ]


def test_jsonpath_and_regex_can_use_record_composition() -> None:
    """JSONPath 与正则均通过公共组装逻辑运行，不要求引擎特殊分支。"""
    assert JsonPathParser().match(
        {"orders": [{"id": 0, "lines": [{"sku": None}, {"sku": "B"}]}]},
        Rows(
            "$.orders[*]",
            Product(
                (
                    {"id": Rule("$.id", mode="first")},
                    Rows("$.lines[*]", {"sku": Rule("$.sku", mode="first")}),
                )
            ),
        ),
    ) == [{"id": 0, "sku": None}, {"id": 0, "sku": "B"}]
    assert RegexParser().match(
        "a=1 b=2",
        Rows(
            r"\w+=\d+",
            Product(
                (
                    {"raw": Rule(".+", mode="first")},
                    {
                        "number": Rule(
                            r"=(\d+)", mode="first", options={"group": 1}, transform=int
                        )
                    },
                )
            ),
        ),
    ) == [{"raw": "a=1", "number": 1}, {"raw": "b=2", "number": 2}]


def test_sequences_concatenate_at_root_in_rows_and_in_product_branches() -> None:
    """规则序列始终表示拼接，包括商品行内以及笛卡尔积的某个分支。"""
    source = {"items": [{"id": 1}, {"id": 2}]}
    assert JmesPathParser().match(
        source,
        [
            {"kind": Constant("header")},
            Rows(
                "items",
                [
                    {"id": "id"},
                    Product(
                        ({"id": "id"}, [{"tag": Constant("x")}, {"tag": Constant("y")}])
                    ),
                ],
            ),
            Product(({"kind": Constant("footer")},)),
        ],
    ) == [
        {"kind": "header"},
        {"id": 1},
        {"id": 1, "tag": "x"},
        {"id": 1, "tag": "y"},
        {"id": 2},
        {"id": 2, "tag": "x"},
        {"id": 2, "tag": "y"},
        {"kind": "footer"},
    ]
    assert JmesPathParser().match(source, Rows("items", [])) == []


@pytest.mark.parametrize("keep_empty", [False, True])
def test_empty_product_branches_and_identity(keep_empty: bool) -> None:
    """空分支策略明确；零个分支及一条空字典记录均遵循乘积单位元。

    Args:
        keep_empty: 是否将空分支作为一条空记录保留。
    """
    parser = JmesPathParser()
    assert parser.match({}, Product((), keep_empty=keep_empty)) == [{}]
    assert parser.match({}, Product(({}, {}), keep_empty=keep_empty)) == [{}]
    result = parser.match(
        {"items": []},
        Product(
            (
                {"parent": Constant(1)},
                Rows("items", {"x": "x"}),
            ),
            keep_empty=keep_empty,
        ),
    )
    assert result == ([{"parent": 1}] if keep_empty else [])
    assert parser.match({}, Product(([], []), keep_empty=keep_empty)) == (
        [{}] if keep_empty else []
    )
    assert parser.match(
        {},
        Product(
            (
                {"id": Constant(1)},
                Rows(Rule("@", when=lambda source: False), {}),
            ),
            keep_empty=keep_empty,
        ),
    ) == ([{"id": 1}] if keep_empty else [])


def test_keep_empty_preserves_only_the_matching_parent_and_is_locally_scoped() -> None:
    """无子项时可保留本父记录；可选分支策略不影响外层必需分支。"""
    source = {"orders": [{"id": 1, "lines": ["A"]}, {"id": 2, "lines": []}]}
    assert JmesPathParser().match(
        source,
        Rows(
            "orders",
            Product(
                (
                    {"id": "id"},
                    Rows("lines", {"sku": "@"}),
                ),
                keep_empty=True,
            ),
        ),
    ) == [{"id": 1, "sku": "A"}, {"id": 2}]
    assert (
        JmesPathParser().match(
            source,
            Product(
                (
                    {"kind": Constant("summary")},
                    Product((Rows("absent || `[]`", {}),), keep_empty=True),
                    Rows("absent || `[]`", {}),
                )
            ),
        )
        == []
    )


@pytest.mark.parametrize("value", [None, False, 0, "", [], {"nested": 1}])
def test_even_equal_field_values_conflict(value: Any) -> None:
    """冲突按字段是否存在判断，值相同或为假值时也不能静默覆盖。

    Args:
        value: 两个分支共同产生的字段值。
    """
    with pytest.raises(ValueError, match="branch 2: 'field'"):
        JmesPathParser().match(
            {},
            Product(
                (
                    {"field": Constant(value)},
                    {"field": Constant(value)},
                )
            ),
        )


@pytest.mark.parametrize(
    "strategy, expected",
    [
        ("first", {"parent": True}),
        ("last", {"child": True}),
    ],
)
def test_conflict_strategy_overwrites_whole_fields(
    strategy: str, expected: Any
) -> None:
    """覆盖策略按分支顺序选择完整字段，嵌套字典不递归合并。

    Args:
        strategy: 保留先出现或后出现字段的策略。
        expected: 预期保留的字段值。
    """
    rules = Rows(
        "parents",
        Product(
            (
                {"meta": "meta"},
                Rows("children", {"meta": "meta"}),
            ),
            on_conflict=strategy,
        ),
    )  # type: ignore[arg-type]
    source = {
        "parents": [{"meta": {"parent": True}, "children": [{"meta": {"child": True}}]}]
    }
    assert JmesPathParser().match(source, rules) == [{"meta": expected}]


def test_missing_fields_do_not_conflict_or_replace_existing_values() -> None:
    """省略字段不参与冲突；候选与默认值仍按原有规则执行。"""
    source = {"id": 0}
    rules = Product(
        (
            {"id": Rule("$.id", mode="first")},
            {
                "id": Rule("$.missing", mode="first"),
                "name": Group((Rule("$.missing", mode="first"),), default="name"),
            },
        )
    )
    assert JsonPathParser().match(source, rules) == [{"id": 0, "name": "name"}]


def test_product_results_isolate_mutable_fields_and_source_data() -> None:
    """重复组装的父字段、默认值及常量在记录之间独立，不修改源数据。"""
    source = {"meta": {"tags": []}, "items": [1, 2]}
    rules = Product(
        (
            {
                "meta": "meta",
                "constant": Constant([]),
                "default": Rule(
                    "missing", missing=lambda value: value is None, default=[]
                ),
            },
            Rows("items", {"item": "@"}),
        )
    )
    result = JmesPathParser().match(source, rules)
    result[0]["meta"]["tags"].append("changed")
    result[0]["constant"].append("changed")
    result[0]["default"].append("changed")
    assert result[1] == {"meta": {"tags": []}, "constant": [], "default": [], "item": 2}
    assert source["meta"] == {"tags": []}
    assert JmesPathParser().match(source, rules)[0] == {
        "meta": {"tags": []},
        "constant": [],
        "default": [],
        "item": 1,
    }


def test_product_as_a_field_keeps_its_assembled_records_nested() -> None:
    """字段中的 Product 生成列表，只有记录位置的 Product 才展开外层记录。"""
    source = {"left": [1, 2], "right": [3]}
    product = Product((Rows("left", {"a": "@"}), Rows("right", {"b": "@"})))
    assert JmesPathParser().match(source, {"pairs": product}) == [
        {"pairs": [{"a": 1, "b": 3}, {"a": 2, "b": 3}]},
    ]


def test_product_evaluates_each_branch_once_before_assembly() -> None:
    """转换调用次数只取决于各分支记录数，不随笛卡尔积结果重复执行。"""
    calls: list[int] = []  # 本测试拥有的转换调用记录。

    def observe(value: int) -> int:
        """记录当前值并保持提取结果。

        Args:
            value: 当前分支的字段值。

        Returns:
            原值。
        """
        calls.append(value)
        return value

    result = JmesPathParser().match(
        {"a": [1, 2], "b": [3, 4, 5]},
        Product(
            (
                Rows("a", {"a": Rule("@", transform=observe)}),
                Rows("b", {"b": Rule("@", transform=observe)}),
            )
        ),
    )
    assert len(result) == 6
    assert calls == [1, 2, 3, 4, 5]


@pytest.mark.parametrize("keep_empty", [False, True])
def test_empty_factor_does_not_hide_errors_in_other_branches(keep_empty: bool) -> None:
    """空分支不短路其他分支的语法错误、必填失败或非法行选择。

    Args:
        keep_empty: 当前 Product 的空分支策略。
    """
    parser = JmesPathParser()
    with pytest.raises(JMESPathError):
        parser.match({}, Product(([], {"x": "["}), keep_empty=keep_empty))
    with pytest.raises(MissingValueError):
        parser.match(
            {},
            Product(
                (
                    [],
                    {
                        "x": Rule(
                            "missing",
                            missing=lambda value: value is None,
                            required=True,
                        ),
                    },
                ),
                keep_empty=keep_empty,
            ),
        )
    with pytest.raises(TypeError, match="must return a list"):
        parser.match({}, Product(([], Rows("missing", {})), keep_empty=keep_empty))


def test_cancellation_and_callback_errors_propagate_from_nested_product() -> None:
    """多层组装仍原样传播取消及回调异常，不通过保留空分支吞掉失败。"""
    for error in (asyncio.CancelledError(), ValueError("callback failed")):

        def fail(value: Any) -> Any:
            """抛出当前测试的原始异常。

            Args:
                value: 当前字段值。

            Raises:
                BaseException: 当前测试指定的异常。
            """
            raise error

        rules = Rows(
            "items",
            Product(
                (
                    {"parent": Constant("parent")},
                    {"x": Rule("@", transform=fail)},
                ),
                keep_empty=True,
            ),
        )
        with pytest.raises(type(error)) as caught:
            JmesPathParser().match({"items": [1]}, rules)
        assert caught.value is error


class KeyParser:
    """只实现窄协议的第三方键解析器，不继承内置类且不保存当前源。"""

    def prepare(self, source: Any) -> Any:
        """直接使用输入字典。

        Args:
            source: 当前字典。

        Returns:
            原字典。
        """
        return source

    def extract(self, source: Any, expression: str, **options: Any) -> Any:
        """读取指定字段，缺失时返回公共标记。

        Args:
            source: 当前字典。
            expression: 字段名。
            **options: 不支持附加选项。

        Returns:
            原字段值或 MISSING。

        Raises:
            TypeError: 传入额外选项。
        """
        if options:
            raise TypeError("unsupported options")
        return source.get(expression, MISSING)


def test_protocol_replacement_and_concurrent_composition_reuse() -> None:
    """同一组装规则在内置与第三方解析器间并发复用，不绑定任何引擎。"""
    parser: Parser = KeyParser()
    rules = Rows(
        "parents", Product(({"id": "id"}, Rows("children", {"value": "value"})))
    )

    def run(index: int) -> list[dict[str, Any]]:
        """在独立输入上复用同一规则和解析器实例。

        Args:
            index: 当前父记录标识。

        Returns:
            继承当前父字段的子记录列表。
        """
        source = {"parents": [{"id": index, "children": [{"value": index * 10}]}]}
        return match(source, rules, parser=parser if index % 2 else JmesPathParser())

    with ThreadPoolExecutor(max_workers=4) as executor:
        result = list(executor.map(run, range(12)))
    assert result == [[{"id": index, "value": index * 10}] for index in range(12)]
    assert rules.parser is None


def test_composition_snapshots_and_invalid_configuration() -> None:
    """嵌套组合结构保存构造时快照，错误配置在遇到空数据前也必须失败。"""
    parent = {"parent": Constant(1)}
    children = [{"child": Constant(2)}]
    branches = [parent, children]
    rules = Product(branches)
    parent.clear()
    children.clear()
    branches.clear()
    assert match({}, rules, parser=KeyParser()) == [{"parent": 1, "child": 2}]
    with pytest.raises(TypeError):
        Product({"field": Rule("x")})  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Product((Rows("empty", {}), [{"invalid": 1}]))  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Product((), keep_empty="yes")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Product((), on_conflict="ignore")  # type: ignore[arg-type]
