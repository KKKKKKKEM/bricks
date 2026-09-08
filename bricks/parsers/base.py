"""独立解析协议和可选的批量提取便捷基类。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .rules import RuleSet


@runtime_checkable
class Parser(Protocol):
    """可结构化替换的解析接口，不要求继承内置实现。

    prepare 必须接受自己的准备结果，不能在共享实例中保存当前文档。
    extract 接受原输入或准备结果，保留查询语言的结果类型，不吞异常。
    文档与提取出的可变对象由调用方管理；并发调用期间不得修改文档。
    """

    def prepare(self, source: Any) -> Any:
        """准备可重复查询的文档，不将输入字符串解释为文件路径或 URL。

        Args:
            source: 原始输入或本解析器已准备的文档。

        Returns:
            当前调用使用的文档；已准备的输入可以原样返回。
        """
        ...

    def extract(self, source: Any, expression: str, **options: Any) -> Any:
        """执行一个表达式并保留其原始结果形状。

        Args:
            source: 原始输入或已准备的文档。
            expression: 非空表达式。
            **options: 当前解析器明确支持的选项，未知选项必须报错。

        Returns:
            查询语言定义的结果，不自动压缩单元素列表。
        """
        ...


class BaseParser(ABC):
    """为解析实现提供可选的取首项和批量规则入口，不保存执行状态。"""

    def prepare(self, source: Any) -> Any:
        """默认将无需预处理的输入原样交给表达式提取器。

        Args:
            source: 待查询的输入。

        Returns:
            原始输入。
        """
        return source

    @abstractmethod
    def extract(self, source: Any, expression: str, **options: Any) -> Any:
        """由具体实现执行单个表达式。

        Args:
            source: 原始输入或已准备的文档。
            expression: 非空表达式。
            **options: 具体实现支持的查询选项。

        Returns:
            表达式的原始结果。
        """
        raise NotImplementedError

    def extract_first(
        self, source: Any, expression: str, *, default: Any = None, **options: Any
    ) -> Any:
        """从列表结果中取首项，不把字符串、字典或标量视为集合。

        Args:
            source: 原始输入或已准备的文档。
            expression: 非空表达式。
            default: 空列表的返回值，默认 None。
            **options: 传给 extract 的选项。

        Returns:
            列表首项或空列表对应的默认值。

        Raises:
            TypeError: 表达式返回的不是列表。
        """
        result = self.extract(source, expression, **options)
        if not isinstance(result, list):
            raise TypeError("extract_first requires a list result")
        return result[0] if result else default

    def match(self, source: Any, rules: RuleSet) -> list[dict[str, Any]]:
        """使用公共规则执行器完成批量记录提取。

        Args:
            source: 原始输入或已准备的文档。
            rules: 字段映射、Rows、Product 或按顺序拼接的多组规则。

        Returns:
            普通字典记录列表，不自动包装为领域 Items。
        """
        from .rules import match

        return match(source, rules, parser=self)


def check_expression(expression: str) -> None:
    """检查表达式类型和非空约束，不修改表达式内容。

    Args:
        expression: 待执行的表达式。

    Raises:
        TypeError: 表达式不是字符串。
        ValueError: 表达式为空或只有空白。
    """
    if not isinstance(expression, str):
        raise TypeError("expression must be a string")
    if not expression.strip():
        raise ValueError("expression must not be empty")
