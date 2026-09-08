"""基于 lxml 的 HTML/XML 文档准备、CSS 选择和 XPath 查询。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from lxml import etree
from lxml.cssselect import CSSSelector

from .base import BaseParser, check_expression
from .rules import MISSING


@dataclass(frozen=True)
class _MarkupParser(BaseParser):
    """共用显式文档格式和编码配置，不保存当前文档树。

    Attributes:
        format: html 容错解析或 xml 严格解析，默认 html，不自动猜测。
        encoding: 字节输入的显式编码，默认 None 由 lxml 读取文档声明。
    """

    format: Literal["html", "xml"] = "html"
    encoding: str | None = None

    def __post_init__(self) -> None:
        """校验文档格式与编码配置。

        Raises:
            ValueError: 文档格式不支持。
            TypeError: 编码不是字符串或 None。
        """
        if self.format not in ("html", "xml"):
            raise ValueError("format must be 'html' or 'xml'")
        if self.encoding is not None and not isinstance(self.encoding, str):
            raise TypeError("encoding must be a string or None")

    def prepare(self, source: Any) -> etree._Element:
        """解析文档内容或复用现有元素；XML 文档返回前拒绝 DOCTYPE。

        Args:
            source: str、bytes 或 lxml 元素；文本只作为内容，不读取路径。

        Returns:
            文档根元素或传入的原元素，树的所有权归调用方。

        Raises:
            TypeError: 输入类型不支持。
            ValueError: HTML 无法生成文档，或 XML 所属文档包含 DOCTYPE。
            etree.XMLSyntaxError: XML 格式错误。
        """
        if isinstance(source, etree._Element):
            root = source
        elif not isinstance(source, (str, bytes)):
            raise TypeError("markup source must be str, bytes, or an lxml element")
        elif self.format == "xml":
            root = etree.fromstring(
                source,
                parser=etree.XMLParser(
                    encoding=self.encoding,
                    resolve_entities=False,
                    load_dtd=False,
                    no_network=True,
                ),
            )
        else:
            root = etree.HTML(
                source,
                parser=etree.HTMLParser(encoding=self.encoding, no_network=True),
            )
            if root is None:
                raise ValueError("HTML source does not contain a document")
        # 未展开的实体仍可能被 XPath 或属性访问求值，查询前拒绝其声明来源。
        if self.format == "xml" and root.getroottree().docinfo.doctype:
            raise ValueError("XML DOCTYPE declarations are not supported")
        return root


class CssParser(_MarkupParser):
    """使用标准 CSS 选择元素，支持显式文本、属性和 HTML 输出。"""

    def extract(self, source: Any, expression: str, **options: Any) -> list[Any]:
        """选择元素并按明确选项生成等长列表，不自动去除空白。

        Args:
            source: 文档文本、字节或已准备的元素。
            expression: cssselect 支持的 CSS 选择器，不支持 ::text/::attr。
            **options: output 为 element/text/html，默认 element；attribute
                为属性名且不能与非默认 output 同用；namespaces 为命名空间映射。

        Returns:
            按文档顺序的列表；缺失属性使用 MISSING 保留元素位置。

        Raises:
            TypeError: 输入或选项类型不合法，或存在未知选项。
            ValueError: 输出模式不合法或选项冲突。
        """
        check_expression(expression)
        return self._select(self.prepare(source), expression, **options)

    def _select(
        self,
        root: etree._Element,
        expression: str,
        *,
        output: Literal["element", "text", "html"] = "element",
        attribute: str | None = None,
        namespaces: Mapping[str, str] | None = None,
    ) -> list[Any]:
        """执行 CSS 选择，并显式投影节点内容。

        Args:
            root: 已准备的文档或子元素。
            expression: CSS 表达式。
            output: 元素、后代文本或外层 HTML。
            attribute: 属性名，默认 None 不提取属性。
            namespaces: XML/CSS 命名空间映射。

        Returns:
            与命中元素数量一致的结果列表。

        Raises:
            TypeError: 属性名类型错误。
            ValueError: 输出模式或属性配置冲突。
        """
        if output not in ("element", "text", "html"):
            raise ValueError("output must be 'element', 'text', or 'html'")
        if attribute is not None:
            if not isinstance(attribute, str):
                raise TypeError("attribute must be a string")
            if not attribute or output != "element":
                raise ValueError(
                    "attribute requires a nonempty name and default output"
                )
        elements = CSSSelector(
            expression,
            translator=self.format,
            namespaces=dict(namespaces) if namespaces is not None else None,
        )(root)
        if attribute is not None:
            return [element.attrib.get(attribute, MISSING) for element in elements]
        if output == "text":
            return ["".join(element.itertext()) for element in elements]
        if output == "html":
            return [
                etree.tostring(
                    element, encoding="unicode", method=self.format, with_tail=False
                )
                for element in elements
            ]
        return list(elements)


class XPathParser(_MarkupParser):
    """执行 XPath 1.0，保留节点集合和字符串、数字、布尔标量。"""

    def extract(self, source: Any, expression: str, **options: Any) -> Any:
        """执行 XPath 表达式，不把字符串结果拆成字符或自动 strip。

        Args:
            source: 文档文本、字节或已准备的元素。
            expression: XPath 1.0 表达式。
            **options: namespaces 为前缀映射，variables 为显式 XPath 变量映射。

        Returns:
            原生列表、str、float 或 bool；文本不保留 smart-string 树引用。

        Raises:
            TypeError: 选项类型错误或存在未知选项。
            etree.XPathError: 表达式、命名空间或变量错误。
        """
        check_expression(expression)
        return self._select(self.prepare(source), expression, **options)

    @staticmethod
    def _select(
        root: etree._Element,
        expression: str,
        *,
        namespaces: Mapping[str, str] | None = None,
        variables: Mapping[str, Any] | None = None,
    ) -> Any:
        """编译 XPath 并使用独立变量映射求值。

        Args:
            root: 当前文档或子元素。
            expression: XPath 表达式。
            namespaces: 前缀到 URI 的映射。
            variables: 变量名到值的映射。

        Returns:
            XPath 原生求值结果。
        """
        query = etree.XPath(
            expression,
            namespaces=dict(namespaces) if namespaces is not None else None,
            smart_strings=False,
        )
        return query(root, **dict(variables or {}))
