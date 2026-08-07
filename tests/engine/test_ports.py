from typing import Any

import pytest

from bricks.engine import Ports, is_type_compatible


class BaseValue:
    """测试使用的基础数据类型。"""


class ChildValue(BaseValue):
    """测试使用的派生数据类型。"""


class OtherValue:
    """测试使用的不相关数据类型。"""


def test_ports_preserve_names_types_and_order() -> None:
    """验证 Ports 按声明顺序保存名称和类型。"""

    ports = Ports(first=BaseValue, second=ChildValue)

    assert tuple(ports) == ("first", "second")
    assert ports["first"] is BaseValue
    assert ports["second"] is ChildValue


def test_ports_reject_non_class_types() -> None:
    """验证 Ports 拒绝 list[str] 等非普通 Python class。"""

    with pytest.raises(TypeError, match="must be a Python class"):
        Ports(items=list[str])  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="must be a Python class"):
        Ports(items=Any)


def test_type_compatibility_is_directional() -> None:
    """验证派生输出可以连接基础输入，反向连接不安全。"""

    assert is_type_compatible(ChildValue, BaseValue)
    assert is_type_compatible(BaseValue, BaseValue)
    assert not is_type_compatible(BaseValue, ChildValue)
    assert not is_type_compatible(ChildValue, OtherValue)


def test_object_accepts_any_concrete_output_type() -> None:
    """验证 object 输入可以接收任意具体输出类型。"""

    assert is_type_compatible(ChildValue, object)
    assert not is_type_compatible(object, ChildValue)
