import pytest

from bricks.engine import ExecutionContext


def test_execution_context_copies_and_freezes_metadata() -> None:
    """验证 Context 会复制并冻结调用方传入的 metadata。"""

    metadata = {"tenant": "alpha"}
    context = ExecutionContext(
        run_id="run-1",
        task_id="task-1",
        flow="default",
        metadata=metadata,
    )
    metadata["tenant"] = "changed"

    assert context.metadata["tenant"] == "alpha"
    with pytest.raises(TypeError):
        context.metadata["tenant"] = "changed"  # type: ignore[index]


@pytest.mark.parametrize("field", ["run_id", "task_id", "flow"])
def test_execution_context_requires_non_empty_identity(field: str) -> None:
    """验证 Context 拒绝空身份字段。

    参数：
        field: 本轮参数化测试需要置空的字段名。
    """

    values = {
        "run_id": "run-1",
        "task_id": "task-1",
        "flow": "default",
    }
    values[field] = ""

    with pytest.raises(ValueError, match=f"{field} must not be empty"):
        ExecutionContext(**values)
