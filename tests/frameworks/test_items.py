from typing import Any, cast

import pytest

from bricks.frameworks.crawler import Items


def test_sequence_operations_copy_nested_input():
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
    items = Items({"nested": [1], "other": 2})
    result = operation(items)
    assert isinstance(result, Items)
    result[0]["nested"].append(3)
    assert items[0]["nested"] == [1]


def test_export_values_and_defaults_are_independent():
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
    items = Items([{"values": [1]}, {"values": [2]}])

    def mutate(row):
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
    items = Items([{"a": 1, "b": 2}, {"a": 3}])
    assert items.rename({"a": "b", "b": "a"}) == [{"b": 1, "a": 2}, {"b": 3}]
    assert items.rename({"missing": "a"}) == items
    with pytest.raises(ValueError, match="conflict"):
        items.rename({"a": "b"})
    with pytest.raises(ValueError, match="conflict"):
        items.rename({"a": "x", "b": "x"})
    assert items == [{"a": 1, "b": 2}, {"a": 3}]


def test_unique_preserves_first_record_and_supports_nested_values():
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
    items = Items([{}, {}])
    values: dict[str, list[int]] = {"data": []}
    items.update_all(values)
    values["data"].append(1)
    items[0]["data"].append(2)
    assert items[1]["data"] == []


def test_invalid_bulk_records_leave_original_unchanged():
    items = Items({"a": 1})
    invalid = cast(Any, [{"b": 2}, {1: "bad key"}])
    with pytest.raises(TypeError):
        items.extend(invalid)
    with pytest.raises(TypeError):
        items[:] = invalid
    assert items == [{"a": 1}]


def test_projection_missing_fields_and_empty_collections():
    items = Items([{"a": 1, "b": 2}, {"b": 3}])
    assert items.columns == ("a", "b")
    assert items.select("a", "missing") == [{"a": 1}, {}]
    assert items.drop("a", "missing") == [{"b": 2}, {"b": 3}]
    assert Items().unique().to_list() == []
    assert Items().values("missing") == []
