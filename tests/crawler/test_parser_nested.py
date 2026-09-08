"""使用不规则业务数据验证多层匹配、局部策略和记录关联。"""

from typing import Any, Literal

import pytest

from bricks.parsers import (
    Constant,
    JmesPathParser,
    JsonPathParser,
    Product,
    Rows,
    Rule,
)


def _catalog() -> dict[str, Any]:
    """创建包含同名店铺、同名商品和不同层级空列表的独立数据。

    Returns:
        每次调用新建的四层店铺、商品、规格和标签数据。
    """
    return {
        "shops": [
            {
                "name": "same",
                "region": "north",
                "markets": ["cn", "us"],
                "products": [
                    {
                        "sku": "A",
                        "variants": [
                            {"code": "a1", "tags": ["hot", "new"]},
                            {"code": "a2", "tags": []},
                        ],
                    },
                    {"sku": "B", "variants": []},
                    {"sku": "C", "variants": [{"code": "c1", "tags": ["old"]}]},
                ],
            },
            {
                "name": "same",
                "region": "south",
                "markets": [],
                "products": [
                    {"sku": "A", "variants": [{"code": "b1", "tags": ["one"]}]},
                ],
            },
            {"name": "empty", "region": "east", "markets": ["jp"], "products": []},
            {"name": "all-empty", "region": "west", "markets": [], "products": []},
        ]
    }


def _expected_catalog(
    source: dict[str, Any], *, keep_shops: bool, keep_variants: bool, keep_tags: bool
) -> list[dict[str, Any]]:
    """直接按业务层级循环生成预期记录，不调用解析器或通用组装函数。

    Args:
        source: 店铺及其商品数据。
        keep_shops: 店铺层是否保留空商品或市场分支。
        keep_variants: 商品层是否保留没有规格记录的商品。
        keep_tags: 规格层是否保留没有标签的规格。

    Returns:
        按店铺、商品、规格、标签、市场顺序生成的扁平记录。
    """
    expected = []
    for shop in source["shops"]:
        goods = []
        for item in shop["products"]:
            variants = []
            for variant in item["variants"]:
                if variant["tags"]:
                    for tag in variant["tags"]:
                        variants.append({"variant": variant["code"], "tag": tag})
                elif keep_tags:
                    variants.append({"variant": variant["code"]})
            if variants:
                for variant_record in variants:
                    goods.append({"sku": item["sku"], **variant_record})
            elif keep_variants:
                goods.append({"sku": item["sku"]})
        if not goods:
            if not keep_shops:
                continue
            goods = [{}]
        for good in goods:
            record = {"shop": shop["name"], "region": shop["region"], **good}
            if shop["markets"]:
                for market in shop["markets"]:
                    expected.append({**record, "market": market})
            elif keep_shops:
                expected.append(record)
    return expected


@pytest.mark.parametrize("language", ["jmespath", "jsonpath"])
@pytest.mark.parametrize("keep_shops", [False, True])
@pytest.mark.parametrize("keep_variants", [False, True])
@pytest.mark.parametrize("keep_tags", [False, True])
def test_ragged_catalog_matches_independent_business_loops(
    language: str, keep_shops: bool, keep_variants: bool, keep_tags: bool
) -> None:
    """两种查询语言下逐条核对四层关联及三个局部空分支策略的全部组合。

    Args:
        language: 使用的 JSON 查询语言。
        keep_shops: 店铺层空分支策略。
        keep_variants: 商品层空分支策略。
        keep_tags: 规格层空分支策略。
    """

    def field(name: str) -> Rule:
        """以当前语言读取一个对象字段或标量根值。

        Args:
            name: 字段名，@ 表示当前标量。

        Returns:
            保留字段原始值的规则。
        """
        if language == "jmespath":
            return Rule(name)
        return Rule("$" if name == "@" else f"$.{name}", mode="first")

    def select(name: str) -> str:
        """以当前语言选择指定列表中的各成员。

        Args:
            name: 当前对象的列表字段名。

        Returns:
            返回列表成员的查询表达式。
        """
        return name if language == "jmespath" else f"$.{name}[*]"

    source = _catalog()
    schema = Rows(
        select("shops"),
        Product(
            (
                {"shop": field("name"), "region": field("region")},
                Rows(
                    select("products"),
                    Product(
                        (
                            {"sku": field("sku")},
                            Rows(
                                select("variants"),
                                Product(
                                    (
                                        {"variant": field("code")},
                                        Rows(select("tags"), {"tag": field("@")}),
                                    ),
                                    keep_empty=keep_tags,
                                ),
                            ),
                        ),
                        keep_empty=keep_variants,
                    ),
                ),
                Rows(select("markets"), {"market": field("@")}),
            ),
            keep_empty=keep_shops,
        ),
    )
    parser = JmesPathParser() if language == "jmespath" else JsonPathParser()
    result = parser.match(source, schema)
    assert result == _expected_catalog(
        source, keep_shops=keep_shops, keep_variants=keep_variants, keep_tags=keep_tags
    )
    assert source == _catalog()


@pytest.mark.parametrize("outer", ["first", "last"])
@pytest.mark.parametrize("inner", ["first", "last"])
def test_conflict_policy_is_local_at_each_parent_level(
    outer: Literal["first", "last"], inner: Literal["first", "last"]
) -> None:
    """外层覆盖策略不能改变子层的字段选择，缺失与显式 null 分别处理。

    Args:
        outer: 根对象与父记录合并时的冲突策略。
        inner: 父记录与子记录合并时的冲突策略。
    """
    source = {
        "name": "root",
        "parents": [
            {
                "name": "parent",
                "children": [{"name": "child"}, {}, {"name": None}],
            }
        ],
    }
    field = Rule("$.name", mode="first")
    schema = Product(
        (
            {"name": field},
            Rows(
                "$.parents[*]",
                Product(
                    (
                        {"name": field},
                        Rows("$.children[*]", {"name": field}),
                    ),
                    on_conflict=inner,
                ),
            ),
        ),
        on_conflict=outer,
    )
    names = ["child", "parent", None] if inner == "last" else ["parent"] * 3
    if outer == "first":
        names = ["root"] * 3
    assert JsonPathParser().match(source, schema) == [{"name": name} for name in names]


def test_inner_conflict_is_not_suppressed_by_outer_overwrite_policy() -> None:
    """外层允许覆盖时，内层默认冲突依然必须失败。"""
    rules = Product(
        (
            {"name": Constant("root")},
            Rows(
                "parents",
                Product(
                    (
                        {"name": "name"},
                        Rows("children", {"name": "name"}),
                    )
                ),
            ),
        ),
        on_conflict="last",
    )
    with pytest.raises(ValueError, match="field conflict"):
        JmesPathParser().match(
            {"parents": [{"name": "parent", "children": [{"name": "child"}]}]}, rules
        )


def test_nested_field_lists_remain_nested_beside_flattened_rows() -> None:
    """同一记录中的嵌套组合和普通列表不因旁边的扁平展开而再次展开。"""
    source = {
        "groups": [
            {"id": "A", "left": [1, 2], "right": [3, 4], "labels": ["x", "y"]},
            {"id": "B", "left": [5], "right": [], "labels": ["z"]},
        ]
    }
    rules = Rows(
        "groups",
        Product(
            (
                {
                    "id": "id",
                    "labels": "labels",
                    "details": {
                        "pairs": Product(
                            (
                                Rows("left", {"a": "@"}),
                                Rows("right", {"b": "@"}),
                            )
                        )
                    },
                },
                Rows("labels", {"label": "@"}),
            )
        ),
    )
    pairs = [{"a": 1, "b": 3}, {"a": 1, "b": 4}, {"a": 2, "b": 3}, {"a": 2, "b": 4}]
    assert JmesPathParser().match(source, rules) == [
        {"id": "A", "labels": ["x", "y"], "details": {"pairs": pairs}, "label": "x"},
        {"id": "A", "labels": ["x", "y"], "details": {"pairs": pairs}, "label": "y"},
        {"id": "B", "labels": ["z"], "details": {"pairs": []}, "label": "z"},
    ]


def test_null_empty_object_and_missing_fields_do_not_remove_selected_children() -> None:
    """列表中实际存在的 null 和空对象各占一条子记录，不等同于空列表。"""
    source = {"parents": [{"id": 0, "children": [{}, None, {"id": False}]}]}
    rules = Rows(
        "$.parents[*]",
        Product(
            (
                {"parent_id": Rule("$.id", mode="first")},
                Rows(
                    "$.children[*]",
                    {
                        "child_id": Rule("$.id", mode="first"),
                        "raw": Rule("$", mode="first"),
                    },
                ),
            )
        ),
    )
    assert JsonPathParser().match(source, rules) == [
        {"parent_id": 0, "raw": {}},
        {"parent_id": 0, "raw": None},
        {"parent_id": 0, "child_id": False, "raw": {"id": False}},
    ]


def test_nested_rows_without_parent_product_only_concatenate_selected_children() -> (
    None
):
    """连续 Rows 只下钻并拼接结果；没有显式父字段组合时不自动继承。"""
    source = {
        "groups": [
            {"id": "A", "children": [{"id": 1}, {"id": 2}]},
            {"id": "B", "children": []},
            {"id": "C", "children": [{"id": 3}]},
        ]
    }
    assert JmesPathParser().match(
        source,
        Rows(
            "groups",
            Rows(
                "children",
                [
                    {"child": "id"},
                    {"duplicate": "id"},
                ],
            ),
        ),
    ) == [
        {"child": 1},
        {"duplicate": 1},
        {"child": 2},
        {"duplicate": 2},
        {"child": 3},
        {"duplicate": 3},
    ]


@pytest.mark.parametrize("invalid", [None, {}, "text", 0, False])
def test_invalid_nested_selection_is_not_hidden_by_empty_sibling(invalid: Any) -> None:
    """空同级分支不能隐藏深层非法列表类型，默认空结果策略也不吞配置错误。

    Args:
        invalid: 不能作为 Rows 选择结果的标量或对象。
    """
    source = {"parents": [{"id": 1, "children": invalid}]}
    rules = Product(
        (
            [],
            Rows(
                "parents",
                Product(({"id": "id"}, Rows("children", {})), keep_empty=True),
            ),
        )
    )
    with pytest.raises(TypeError, match="must return a list"):
        JmesPathParser().match(source, rules)


def test_dotted_and_unicode_keys_do_not_participate_in_row_correlation() -> None:
    """源字段中的点号和中文仅由表达式处理，不作为组装路径分隔符。"""
    source = {
        "父.列表": [
            {"父.编号": "甲", "子.列表": [{"子.值": 1}, {"子.值": 2}]},
            {"父.编号": "乙", "子.列表": [{"子.值": 3}]},
        ]
    }
    rules = Rows(
        '"父.列表"',
        Product(
            (
                {"父.编号": '"父.编号"'},
                Rows('"子.列表"', {"子.值": '"子.值"'}),
            )
        ),
    )
    assert JmesPathParser().match(source, rules) == [
        {"父.编号": "甲", "子.值": 1},
        {"父.编号": "甲", "子.值": 2},
        {"父.编号": "乙", "子.值": 3},
    ]
