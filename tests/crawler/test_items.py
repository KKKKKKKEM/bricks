from typing import Any, cast

import pytest

from bricks import Items


def test_sequence_operations_copy_nested_input():
    """验证记录容器的序列操作复制嵌套输入。"""

    source = {"nested": {"values": [1]}}
    items = Items(source)
    items.append(source)
    items.insert(0, source)
    source["nested"]["values"].append(2)
    assert all(row["nested"]["values"] == [1] for row in items)
    items[0]["nested"]["values"].append(3)
    assert items[1]["nested"]["values"] == [1]
    items[1:2] = [{"title": "replacement"}]
    del items[-1]
    assert len(items) == 2


@pytest.mark.parametrize(
    "operation",
    [
        lambda items: items.copy(),
        lambda items: items[:],
        lambda items: items.select("nested"),
        lambda items: items.drop("other"),
        lambda items: items.filter(lambda row: True),
        lambda items: items.map(lambda row: row),
        lambda items: items.unique("nested"),
        lambda items: items.rename({"other": "renamed"}),
    ],
)
def test_all_transforms_isolate_nested_results(operation):
    """验证各记录转换结果与原始嵌套数据隔离。

    Args:
        operation: 当前用例使用的 operation 夹具或参数化输入。
    """

    items = Items({"nested": [1], "other": 2})
    result = operation(items)
    assert isinstance(result, Items)
    result[0]["nested"].append(3)
    assert items[0]["nested"] == [1]


def test_export_values_and_defaults_are_independent():
    """验证导出值与默认值相互独立。"""

    items = Items([{"data": [1]}, {}, {"data": None}])
    exported = items.to_list()
    exported[0]["data"].append(2)
    values = items.values("data", default=[])
    values[0].append(3)
    assert items[0]["data"] == [1]
    assert values[1:] == [[], None]
    defaults = Items([{}, {}]).values("missing", default=[])
    defaults[0].append(1)
    assert defaults[1] == []


def test_callbacks_do_not_mutate_original_records():
    """验证转换回调不能通过副本修改原记录。"""

    items = Items([{"values": [1]}, {"values": [2]}])

    def mutate(row):
        """修改回调取得的记录副本，验证原集合隔离。

        Args:
            row: 当前用例使用的 row 夹具或参数化输入。

        Returns:
            已经修改的记录副本。
        """

        row["values"].append(3)
        return row

    filtered = items.filter(mutate)
    mapped = items.map(mutate)
    assert filtered == items
    assert mapped == [{"values": [1, 3]}, {"values": [2, 3]}]
    assert items == [{"values": [1]}, {"values": [2]}]
    with pytest.raises(TypeError):
        items.map(cast(Any, lambda row: None))


def test_rename_is_simultaneous_and_rejects_collisions():
    """验证字段同时重命名且拒绝名称冲突。"""

    items = Items([{"a": 1, "b": 2}, {"a": 3}])
    assert items.rename({"a": "b", "b": "a"}) == [{"b": 1, "a": 2}, {"b": 3}]
    assert items.rename({"missing": "a"}) == items
    with pytest.raises(ValueError, match="conflict"):
        items.rename({"a": "b"})
    with pytest.raises(ValueError, match="conflict"):
        items.rename({"a": "x", "b": "x"})
    assert items == [{"a": 1, "b": 2}, {"a": 3}]


def test_unique_preserves_first_record_and_supports_nested_values():
    """验证去重保留首条记录并支持嵌套字段。"""

    items = Items(
        [
            {"url": "/1", "title": "first"},
            {"url": "/1", "title": "second"},
            {"url": "/2", "title": "third"},
        ]
    )
    assert items.unique("url").values("title") == ["first", "third"]
    assert len(items.unique("url", "title")) == 3
    nested = Items([{"key": {"a": [1, 2]}}, {"key": {"a": [1, 2]}}, {"key": [1]}])
    assert len(nested.unique("key")) == 2
    assert len(nested.unique()) == 2
    with pytest.raises(KeyError):
        items.unique("missing")


def test_update_all_does_not_share_nested_values_between_rows():
    """验证批量更新不会在不同行之间共享嵌套值。"""

    items = Items([{}, {}])
    values: dict[str, list[int]] = {"data": []}
    items.update_all(values)
    values["data"].append(1)
    items[0]["data"].append(2)
    assert items[1]["data"] == []


@pytest.mark.parametrize("keys", [[{1}, frozenset({1})], [frozenset({1}), {1}]])
def test_unique_compares_equal_keys_across_hashability(keys):
    """验证可哈希性不同但相等的字段仍能去重。

    Args:
        keys: 当前用例使用的 keys 夹具或参数化输入。
    """

    items = Items([{"key": key, "position": i} for i, key in enumerate(keys)])
    result = items.unique("key")
    assert result.values("position") == [0]
    assert type(result[0]["key"]) is type(keys[0])


def test_invalid_bulk_records_leave_original_unchanged():
    """验证批量非法记录不会部分修改原集合。"""

    items = Items({"a": 1})
    invalid = cast(Any, [{"b": 2}, {1: "bad key"}])
    with pytest.raises(TypeError):
        items.extend(invalid)
    with pytest.raises(TypeError):
        items[:] = invalid
    assert items == [{"a": 1}]


def test_projection_missing_fields_and_empty_collections():
    """验证字段投影对缺失字段和空集合的行为。"""

    items = Items([{"a": 1, "b": 2}, {"b": 3}])
    assert items.columns == ("a", "b")
    assert items.select("a", "missing") == [{"a": 1}, {}]
    assert items.drop("a", "missing") == [{"b": 2}, {"b": 3}]
    assert Items().unique().to_list() == []
    assert Items().values("missing") == []
