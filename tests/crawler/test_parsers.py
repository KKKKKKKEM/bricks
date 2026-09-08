"""验证独立解析器、批量规则和第三方结构化替换契约。"""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from io import BytesIO, StringIO
import json
import re
from typing import Any

from cssselect import SelectorError
from jmespath.exceptions import JMESPathError
from jsonpath.exceptions import JSONPathError
from lxml import etree
import pytest

from bricks.parsers import (
    MISSING,
    BaseParser,
    Constant,
    CssParser,
    Group,
    JmesPathParser,
    JsonPathParser,
    MissingValueError,
    Parser,
    RegexParser,
    Rows,
    Rule,
    XPathParser,
    match,
)


def test_css_single_queries_and_prepared_document() -> None:
    """CSS 保留节点、空白和缺失属性位置，准备结果可反复查询。"""
    parser = CssParser()
    document = parser.prepare('<div><a href="/1"> A <b>B</b> </a><a>C</a></div>')
    assert parser.prepare(document) is document
    assert len(parser.extract(document, "a")) == 2
    assert parser.extract(document, "a", output="text") == [" A B ", "C"]
    assert parser.extract(document, "a", attribute="href") == ["/1", MISSING]
    assert parser.extract(document, "b", output="html") == ["<b>B</b>"]
    assert parser.extract_first(document, "a", attribute="href") == "/1"
    assert parser.extract_first(document, ".missing", default="fallback") == "fallback"
    assert parser.extract(document, ".missing") == []
    with pytest.raises(ValueError):
        parser.extract(document, "a", attribute="href", output="text")
    with pytest.raises(SelectorError):
        parser.extract(document, "a::text")
    with pytest.raises(ValueError):
        parser.prepare("")
    with pytest.raises(TypeError):
        parser.prepare({})


def test_markup_encoding_and_explicit_xml_namespaces(tmp_path) -> None:
    """字节编码、XML 命名空间和实体隔离遵循显式文档配置。

    Args:
        tmp_path: pytest 提供的临时目录。
    """
    html = "<p>中文内容</p>".encode("gb18030")
    assert CssParser(encoding="gb18030").extract(html, "p", output="text") == [
        "中文内容"
    ]
    xml = '<r xmlns:x="urn:items"><x:item id="1">甲</x:item></r>'
    assert XPathParser(format="xml").extract(
        xml, "//x:item/text()", namespaces={"x": "urn:items"}
    ) == ["甲"]
    assert CssParser(format="xml").extract(
        xml, "x|item", namespaces={"x": "urn:items"}, attribute="id"
    ) == ["1"]
    secret = tmp_path / "secret.txt"
    secret.write_text("never-expand-this", encoding="utf-8")
    document = f'<!DOCTYPE r [<!ENTITY x SYSTEM "{secret.as_uri()}">]><r>&x;</r>'
    with pytest.raises(ValueError, match="DOCTYPE"):
        XPathParser(format="xml").extract(document, "string(.)")
    with pytest.raises(etree.XMLSyntaxError):
        XPathParser(format="xml").prepare("<r>")


@pytest.mark.parametrize("form", ["text", "utf16", "element", "child"])
@pytest.mark.parametrize(
    "parser, expression, options",
    [
        (XPathParser(format="xml"), "string(.)", {}),
        (XPathParser(format="xml"), "string(//item/@attr)", {}),
        (CssParser(format="xml"), "item", {"output": "text"}),
        (CssParser(format="xml"), "item", {"attribute": "attr"}),
    ],
)
def test_xml_rejects_internal_entities_before_queries(
    form: str, parser: BaseParser, expression: str, options: dict[str, Any]
) -> None:
    """原文、不同编码及现成子元素均不能将内部实体带入查询。

    Args:
        form: 当前输入形式。
        parser: XML 模式的 CSS 或 XPath 解析器。
        expression: 提取文本或属性的表达式。
        options: 当前查询的输出选项。
    """
    xml = (
        '<!DOCTYPE r [<!ENTITY x "expanded-content">]>'
        '<r><item attr="&x;">&x;</item></r>'
    )
    source: Any = xml
    if form == "utf16":
        source = xml.encode("utf-16")
    elif form in ("element", "child"):
        source = etree.fromstring(xml, parser=etree.XMLParser(resolve_entities=False))
        if form == "child":
            source = source[0]
    with pytest.raises(ValueError, match="DOCTYPE"):
        parser.prepare(source)
    with pytest.raises(ValueError, match="DOCTYPE"):
        parser.extract(source, expression, **options)
    with pytest.raises(ValueError, match="DOCTYPE"):
        parser.match(source, {"value": Rule(expression, options=options)})


@pytest.mark.parametrize(
    "doctype",
    [
        "<!DOCTYPE r>",
        '<!DOCTYPE r SYSTEM "file:///nonexistent-bricks.dtd">',
        "<!DOCTYPE r [<!ENTITY % declaration '<!ENTITY x \"value\">'>%declaration;]>",
    ],
)
def test_xml_rejects_doctype_even_without_entity_references(doctype: str) -> None:
    """空声明、外部子集及参数实体声明均按相同规则拒绝。

    Args:
        doctype: 当前文档类型声明。
    """
    with pytest.raises(ValueError, match="DOCTYPE"):
        XPathParser(format="xml").prepare(doctype + "<r/>")


def test_xml_keeps_standard_references_and_literal_doctype_text() -> None:
    """标准字符引用和普通文本不误判为自定义实体声明，HTML 行为保留。"""
    source = '<r attr="&amp;&#65;&#x42;">&lt;&#65;<![CDATA[<!DOCTYPE r>]]></r>'
    parser = XPathParser(format="xml")
    document = parser.prepare(source)
    assert parser.prepare(document) is document
    assert parser.extract(document, "string(.)") == "<A<!DOCTYPE r>"
    assert parser.extract(document, "string(@attr)") == "&AB"
    assert CssParser(format="xml").extract(document, "r", attribute="attr") == ["&AB"]
    assert CssParser().extract("<!DOCTYPE html><p>A</p>", "p", output="text") == ["A"]


def test_xpath_scalar_results_and_variables() -> None:
    """XPath 数字、布尔、文本保持标量，首项入口拒绝误用。"""
    parser = XPathParser()
    source = '<ul><li id="1"> A </li><li id="2">B</li></ul>'
    assert parser.extract(source, "count(//li)") == 2.0
    assert parser.extract(source, "boolean(//missing)") is False
    assert parser.extract(source, "string(//li[1])") == " A "
    assert parser.extract(source, "//li[@id=$id]/text()", variables={"id": "2"}) == [
        "B"
    ]
    assert type(parser.extract(source, "//li/text()")[0]) is str
    with pytest.raises(TypeError):
        parser.extract_first(source, "string(//li[1])")


@pytest.mark.parametrize("source", [None, False, 0, "hello", '"quoted"', "", [], {}])
def test_json_root_values_remain_decoded_values(source: Any) -> None:
    """所有 JSON 根值保持原类型，字符串不被猜测为文档文本。

    Args:
        source: 已解码 JSON 根值。
    """
    assert JmesPathParser().extract(source, "@") == source
    assert JsonPathParser().prepare(source) is source
    assert JsonPathParser().extract(source, "$") == [source]


@pytest.mark.parametrize("stream_type", [StringIO, BytesIO])
def test_jsonpath_rejects_streams_without_consuming_them(stream_type: Any) -> None:
    """单次及多字段查询在读取输入流前报类型错误，保留游标和所有权。

    Args:
        stream_type: 文本或字节内存流构造器。
    """
    data = '{"id":1,"name":"A"}'
    with stream_type(data if stream_type is StringIO else data.encode()) as source:
        parser = JsonPathParser()
        with pytest.raises(TypeError, match="decoded JSON"):
            parser.prepare(source)
        with pytest.raises(TypeError, match="decoded JSON"):
            parser.extract(source, "$.id")
        with pytest.raises(TypeError, match="decoded JSON"):
            parser.match(source, {"id": "$.id", "name": "$.name"})
        assert source.tell() == 0
        assert not source.closed


@pytest.mark.parametrize(
    "source", [b"{}", bytearray(b"{}"), memoryview(b"{}"), (), object()]
)
def test_jsonpath_rejects_undecoded_and_unsupported_roots(source: Any) -> None:
    """字节和非 JSON 根类型不得被当成空查询或隐式解码输入。

    Args:
        source: 不支持的输入根值。
    """
    parser = JsonPathParser()
    with pytest.raises(TypeError, match="decoded JSON"):
        parser.prepare(source)
    with pytest.raises(TypeError, match="decoded JSON"):
        parser.extract(source, "$")
    with pytest.raises(TypeError, match="decoded JSON"):
        parser.match(source, {"root": "$"})


def test_jsonpath_rejects_file_like_mapping_before_read() -> None:
    """映射子类也不能通过底层库的文件接口触发隐式读取。"""

    class ReadableDict(dict[str, Any]):
        """同时提供映射和文件读取接口的测试输入。"""

        def read(self) -> str:
            """禁止测试输入被实际读取。

            Raises:
                AssertionError: 校验未能阻止底层库读取输入。
            """
            raise AssertionError("input must not be read")

    with pytest.raises(TypeError, match="decoded JSON"):
        JsonPathParser().extract(ReadableDict(id=1), "$.id")


def test_json_queries_null_missing_arrays_and_filters() -> None:
    """JSONPath 保留缺失与 null 的区别，JMESPath 如实保留原生语义。"""
    source = json.loads('{"null": null, "rows": [{"id": 1}, {"id": 2}], "empty": []}')
    jmes = JmesPathParser()
    path = JsonPathParser()
    assert jmes.extract(source, "rows[?id > `1`].id") == [2]
    assert jmes.extract(source, "null") is None
    assert jmes.extract(source, "missing") is None
    assert path.extract(source, "$.null") == [None]
    assert path.extract(source, "$.missing") == []
    assert path.extract(source, "$.empty") == [[]]
    assert path.extract(source, "$.rows[?@.id > 1].id") == [2]
    assert path.extract(source, "$..id") == [1, 2]
    with pytest.raises(JSONPathError):
        path.extract(source, "rows[*]")
    with pytest.raises(TypeError):
        jmes.extract_first(source, "null")


def test_regex_explicit_groups_and_no_implicit_serialization() -> None:
    """捕获组形状由 group 决定，非法组即使无匹配也必须报错。"""
    parser = RegexParser()
    expression = r"(?P<key>\w+)=(?P<value>\d+)"
    assert parser.extract("a=1 b=2", expression) == ["a=1", "b=2"]
    assert parser.extract("a=1 b=2", expression, group="value") == ["1", "2"]
    assert parser.extract("a=1", expression, group=None) == [{"key": "a", "value": "1"}]
    assert parser.extract("b", r"(a)?b", group=1) == [None]
    assert parser.extract("ABC", "abc", flags=re.I) == ["ABC"]
    with pytest.raises(IndexError):
        parser.extract("no matches", expression, group=3)
    with pytest.raises(TypeError):
        parser.extract({}, expression)
    with pytest.raises(TypeError):
        parser.extract("a", "a", group=True)


def test_css_match_multiple_batches_nested_rows_and_mixed_parsers() -> None:
    """同一文档批量抽取记录、嵌套列表和不同引擎字段，不隐式组合分支。"""
    source = """<main>
    <article><h2>A</h2><a href="/a">link</a><i>x</i><i>y</i></article>
    <article><h2>B</h2><i>z</i></article>
    </main>"""
    parser = CssParser()
    records = parser.match(
        source,
        [
            {"count": Rule("count(//article)", parser=XPathParser())},
            Rows(
                "article",
                {
                    "title": Rule("h2", mode="first", options={"output": "text"}),
                    "url": Rule(
                        "a", mode="first", default=None, options={"attribute": "href"}
                    ),
                    "tags": Rows(
                        "i", {"name": Rule("string(.)", parser=XPathParser())}
                    ),
                    "nested": {"constant": Constant("value")},
                },
            ),
        ],
    )
    assert records == [
        {"count": 2.0},
        {
            "title": "A",
            "url": "/a",
            "tags": [{"name": "x"}, {"name": "y"}],
            "nested": {"constant": "value"},
        },
        {
            "title": "B",
            "url": None,
            "tags": [{"name": "z"}],
            "nested": {"constant": "value"},
        },
    ]


def test_match_json_rows_preserve_arrays_and_explicit_null_policy() -> None:
    """记录展开显式进行，数组字段和假值保留，null 回退由规则声明。"""
    parser = JmesPathParser()
    rows = [{"id": 0, "active": False, "name": "", "tags": []}, {"id": 2}]
    result = parser.match(
        {"data": rows},
        Rows(
            "data",
            {
                "id": "id",
                "active": "active",
                "name": "name",
                "tags": "tags",
                "missing": Rule(
                    "absent", missing=lambda value: value is None, default="fallback"
                ),
            },
        ),
    )
    assert result[0] == {
        "id": 0,
        "active": False,
        "name": "",
        "tags": [],
        "missing": "fallback",
    }
    assert result[1]["active"] is None
    path = JsonPathParser()
    assert path.match({"x": None}, {"x": Rule("$.x", mode="first", default=1)}) == [
        {"x": None}
    ]
    assert path.match({}, {"x": Rule("$.x", mode="first", default=1)}) == [{"x": 1}]
    assert path.match({}, {"x": Rule("$.x", mode="first")}) == [{}]
    assert path.match({}, {"x": "$.x"}) == [{"x": []}]
    assert path.match(
        {"data": rows},
        Rows(Rule("$.data", mode="first"), {"id": Rule("$.id", mode="first")}),
    ) == [{"id": 0}, {"id": 2}]


def test_group_conditions_defaults_and_transform_order() -> None:
    """候选按条件和缺失选择，默认值也经过转换，零和空文本可被选中。"""
    parser = JsonPathParser()
    rules = {
        "price": Group(
            (
                Rule("$.unused", mode="first", when=lambda source: False),
                Rule("$.price", mode="first", transform=int),
                Rule("$.fallback", mode="first"),
            ),
            required=True,
        ),
        "default": Rule("$.absent", mode="first", default="2", transform=int),
        "skipped": Rule("$.price", when=lambda source: False, default="unused"),
        "empty": Group((Rule("$.name", mode="first"),), default="unused"),
    }
    assert parser.match({"price": "0", "name": ""}, rules) == [
        {"price": 0, "default": 2, "empty": ""}
    ]
    assert parser.match({}, {"x": Group((Rule("$.x", mode="first"),), default=[])}) == [
        {"x": []}
    ]
    with pytest.raises(MissingValueError):
        parser.match({}, rules)
    with pytest.raises(MissingValueError):
        parser.match({}, {"x": Rule("$.x", mode="first", required=True)})


def test_rule_source_conversion_is_explicit() -> None:
    """解析器切换通过 before 明确转换源，不隐式序列化节点。"""
    records = CssParser().match(
        "<p>price=12</p>",
        Rows(
            "p",
            {
                "price": Rule(
                    r"\d+",
                    parser=RegexParser(),
                    before=lambda node: "".join(node.itertext()),
                    mode="first",
                    transform=int,
                ),
            },
        ),
    )
    assert records == [{"price": 12}]


class MappingParser:
    """无需继承任何内置类的第三方解析器，支持任意字段名直接查找。"""

    def prepare(self, source: Any) -> dict[str, Any]:
        """接收 JSON 对象文本或已准备映射。

        Args:
            source: JSON 对象文本或字典。

        Returns:
            已解码的字典。
        """
        return json.loads(source) if isinstance(source, str) else source

    def extract(self, source: Any, expression: str, **options: Any) -> Any:
        """直接读取键，用公共缺失标记报告不存在的字段。

        Args:
            source: JSON 对象文本或字典。
            expression: 字典键名。
            **options: 不接受额外选项。

        Returns:
            对应字段或 MISSING。

        Raises:
            TypeError: 传入额外选项。
        """
        if options:
            raise TypeError("options are not supported")
        return self.prepare(source).get(expression, MISSING)


def test_protocol_only_parser_supports_complete_match_and_reuse() -> None:
    """同一规则跨内置及第三方解析器并发复用，不被首次调用绑定引擎。"""
    parser: Parser = MappingParser()
    assert isinstance(parser, Parser)
    assert not isinstance(parser, BaseParser)
    rules = Rows("data", {"value": Rule("value", default=[]), "constant": Constant([])})
    assert match('{"data": [{"value": 1}, {}]}', rules, parser=parser) == [
        {"value": 1, "constant": []},
        {"value": [], "constant": []},
    ]
    rule = Rule("value")
    sources = [{"value": i} for i in range(20)]

    def run(source: dict[str, int]) -> list[dict[str, Any]]:
        """在并发调用中复用同一条未绑定规则。

        Args:
            source: 当前任务的数据。

        Returns:
            当前任务的提取记录。
        """
        engine = parser if source["value"] % 2 else JmesPathParser()
        return match(source, {"value": rule}, parser=engine)

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(run, sources))
    assert results == [[source] for source in sources]
    assert rule.parser is None


def test_configuration_snapshots_and_per_record_defaults() -> None:
    """构造输入和逐条结果中的默认值与常量不相互污染。"""
    defaults: list[int] = []
    fields = {"x": Rule("x", default=defaults), "constant": Constant(defaults)}
    schema = Rows("data", fields)
    fields.clear()
    defaults.append(9)
    result = match({"data": [{}, {}]}, schema, parser=MappingParser())
    result[0]["x"].append(1)
    result[0]["constant"].append(2)
    assert result[1] == {"x": [], "constant": []}
    assert match({"data": [{}]}, schema, parser=MappingParser()) == [
        {"x": [], "constant": []}
    ]
    with pytest.raises(FrozenInstanceError):
        schema.parser = MappingParser()  # type: ignore[misc]


@pytest.mark.parametrize(
    "parser, source, expression, error",
    [
        (CssParser(), "<p/>", "[", SelectorError),
        (XPathParser(), "<p/>", "[", etree.XPathError),
        (JmesPathParser(), {}, "[", JMESPathError),
        (JsonPathParser(), {}, "$[", JSONPathError),
        (RegexParser(), "", "[", re.error),
    ],
)
def test_invalid_expressions_propagate_through_groups(
    parser: BaseParser, source: Any, expression: str, error: type[Exception]
) -> None:
    """表达式错误不因默认值或候选组而被吞掉。

    Args:
        parser: 内置解析器。
        source: 对应格式的输入。
        expression: 非法表达式。
        error: 原生错误类型。
    """
    with pytest.raises(error):
        parser.match(source, {"x": Group((Rule(expression),), default="unused")})


def test_callback_errors_and_cancellation_propagate_by_identity() -> None:
    """转换失败及控制类异常原样传播，不尝试后续候选或默认值。"""
    for error in (ValueError("conversion failed"), KeyboardInterrupt()):

        def fail(value: Any) -> Any:
            """抛出当前测试的原始异常。

            Args:
                value: 已取得的值。

            Raises:
                BaseException: 当前测试指定的异常。
            """
            raise error

        with pytest.raises(type(error)) as caught:
            match(
                {"x": 1},
                {"x": Group((Rule("x", transform=fail),), default=2)},
                parser=MappingParser(),
            )
        assert caught.value is error


@pytest.mark.parametrize(
    "parser, source, expression",
    [
        (CssParser(), "<p/>", "p"),
        (XPathParser(), "<p/>", "//p"),
        (JmesPathParser(), {}, "@"),
        (JsonPathParser(), {}, "$"),
        (RegexParser(), "x", "x"),
    ],
)
def test_unknown_options_and_empty_expressions_fail(
    parser: BaseParser, source: Any, expression: str
) -> None:
    """未知选项和空表达式明确报错，不被当作成功或缺失。

    Args:
        parser: 内置解析器。
        source: 对应格式的输入。
        expression: 有效表达式。
    """
    with pytest.raises(TypeError):
        parser.extract(source, expression, unknown=True)
    with pytest.raises(ValueError):
        parser.extract(source, " ")


def test_invalid_rules_and_empty_batches() -> None:
    """错误规则即使没有数据也在配置阶段失败，空批次和空记录区别明确。"""
    parser = MappingParser()
    assert match({}, [], parser=parser) == []
    assert match({}, {}, parser=parser) == [{}]
    assert match({"data": []}, Rows("data", {"x": "x"}), parser=parser) == []
    with pytest.raises(TypeError):
        Rows("data", {"x": 1})
    with pytest.raises(TypeError):
        match({}, {"x": ("x", 1)}, parser=parser)
    with pytest.raises(ValueError):
        Rule("x", required=True, default=1)
    with pytest.raises(ValueError):
        Group(())
    with pytest.raises(TypeError):
        match({}, {}, parser="json")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        match({"data": None}, Rows("data", {}), parser=parser)
    with pytest.raises(TypeError):
        match({"x": "text"}, {"x": Rule("x", mode="first")}, parser=parser)
