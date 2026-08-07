import pytest

from bricks.engine import (
    InputAvailability,
    InputPolicy,
    InputSelection,
    InputToken,
    NodeInputs,
)


def availability(**queues: list[InputToken]) -> InputAvailability:
    """创建策略测试使用的输入可用情况。

    参数：
        **queues: input port 到 FIFO token 列表的映射。

    返回：
        对应的只读 InputAvailability。
    """

    return InputAvailability(queues)


def test_node_inputs_are_copied_and_read_only() -> None:
    """验证 NodeInputs 会复制调用方数据并只读暴露。"""

    values = {"left": 1, "right": 2}
    inputs = NodeInputs(values)
    values["left"] = 3

    assert dict(inputs) == {"left": 1, "right": 2}
    with pytest.raises(TypeError):
        inputs["left"] = 3  # type: ignore[index]


def test_node_inputs_support_single_value_convenience() -> None:
    """验证单输入构造和读取方法减少普通 Node 的样板代码。"""

    inputs = NodeInputs.from_value("value")

    assert inputs["default"] == "value"
    assert inputs.single() == "value"


def test_node_inputs_single_rejects_multiple_values() -> None:
    """验证 single() 不会隐藏多输入错误。"""

    inputs = NodeInputs({"left": 1, "right": 2})

    with pytest.raises(ValueError, match="expected one input"):
        inputs.single()


def test_all_policy_waits_for_every_port() -> None:
    """验证 all 策略在全部端口就绪后按声明顺序选择输入。"""

    policy = InputPolicy.all()
    waiting = availability(
        left=[InputToken(1, "left")],
        right=[],
    )
    ready = availability(
        left=[InputToken(1, "left")],
        right=[InputToken(2, "right")],
    )

    assert policy.select(waiting) is None
    assert policy.select(ready) == InputSelection(("left", "right"))


def test_any_policy_selects_globally_oldest_token() -> None:
    """验证 any 策略优先消费全局最早到达的 token。"""

    policy = InputPolicy.any()
    inputs = availability(
        message=[InputToken(20, "message")],
        cancel=[InputToken(10, "cancel")],
    )

    assert policy.select(inputs) == InputSelection(("cancel",))


def test_input_availability_requires_fifo_sequence_order() -> None:
    """验证输入可用情况拒绝乱序 token 队列。"""

    with pytest.raises(ValueError, match="FIFO sequence order"):
        availability(
            value=[
                InputToken(2, "second"),
                InputToken(1, "first"),
            ]
        )


def test_input_token_rejects_boolean_sequence() -> None:
    """验证布尔值不会被误认为合法整数 sequence。"""

    with pytest.raises(TypeError, match="must be an integer"):
        InputToken(True, "value")


def test_groups_use_and_inside_group_and_or_between_groups() -> None:
    """验证输入策略使用组内 AND、组间 OR 的统一语义。"""

    policy = InputPolicy.groups(
        ("users", "orders"),
        ("cancel",),
    )
    inputs = availability(
        users=[InputToken(1, "users")],
        orders=[InputToken(5, "orders")],
        cancel=[InputToken(3, "cancel")],
    )

    assert policy.select(inputs) == InputSelection(("cancel",))


def test_group_declaration_order_breaks_equal_sequence_ties() -> None:
    """验证两个输入组同时就绪时使用声明顺序稳定选择。"""

    policy = InputPolicy.groups(("left",), ("right",))
    inputs = availability(
        left=[InputToken(1, "left")],
        right=[InputToken(1, "right")],
    )

    assert policy.select(inputs) == InputSelection(("left",))


def test_policy_requires_every_declared_port_to_be_covered() -> None:
    """验证策略不能遗留永远不会被消费的声明端口。"""

    policy = InputPolicy.require("left")

    with pytest.raises(ValueError, match="does not cover ports"):
        policy.groups_for(("left", "right"))


def test_policy_rejects_unknown_ports() -> None:
    """验证策略不能引用 Node 没有声明的 input port。"""

    policy = InputPolicy.require("missing")

    with pytest.raises(ValueError, match="unknown ports"):
        policy.groups_for(("declared",))


def test_policy_rejects_string_as_an_input_group() -> None:
    """验证 groups() 不会把单个字符串误拆成字符端口。"""

    with pytest.raises(TypeError, match="iterable of port names"):
        InputPolicy.groups("left")


def test_on_start_only_accepts_zero_input_ports() -> None:
    """验证 on_start 只适用于零输入 Source Node。"""

    policy = InputPolicy.on_start()

    assert policy.select(availability()) == InputSelection(())
    with pytest.raises(ValueError, match="requires zero input ports"):
        policy.groups_for(("default",))
