"""Engine 内部共享的基础参数校验。"""


def require_non_empty_string(value: object, label: str) -> str:
    """校验一个值是非空字符串。

    参数：
        value: 需要校验的值。
        label: 用于异常信息的参数名称。

    返回：
        校验通过的原字符串。

    异常：
        TypeError: value 不是字符串。
        ValueError: value 是空字符串或只包含空白字符。
    """

    if not isinstance(value, str):
        raise TypeError(f"{label} must be a string")
    if not value.strip():
        raise ValueError(f"{label} must not be empty")
    return value

