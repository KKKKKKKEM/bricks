"""验证完整示例的分页、会话、记录输出和明确失败行为。"""

import json

import pytest
from interlace.engine.errors import ExecutionError

from examples.crawler_demo import demo_site
from examples.crawler_pipeline import run


def test_pipeline_pagination_session_and_jsonl(tmp_path):
    """真实下载四个页面并校验 Cookie、分页去重和两条详情记录。

    Args:
        tmp_path: pytest 拥有的临时输出目录。
    """
    output = tmp_path / "products.jsonl"
    with demo_site() as url:
        assert run(url, output, max_pages=4) == 2
    records = [json.loads(line) for line in output.read_text().splitlines()]
    assert [(row["name"], row["price"]) for row in records] == [
        ("铅笔 & 橡皮", "3.50"),
        ("笔记本", "12.00"),
    ]
    assert [row["url"].rsplit("/", 1)[1] for row in records] == ["1", "2"]
    with pytest.raises(FileExistsError):
        run(url, output)
    assert len(output.read_text().splitlines()) == 2


def test_pipeline_limit_and_download_failure(tmp_path):
    """达到页面上限或下载返回错误时明确失败，不报告完整成功。

    Args:
        tmp_path: pytest 拥有的临时输出目录。
    """
    with demo_site() as url:
        with pytest.raises(RuntimeError, match="max_pages exceeded"):
            run(url, tmp_path / "partial.jsonl", max_pages=2)
        with pytest.raises(ExecutionError, match="download failed: 403") as caught:
            run(url + "/missing", tmp_path / "failed.jsonl")
        assert isinstance(caught.value.__cause__, ValueError)
    assert len((tmp_path / "partial.jsonl").read_text().splitlines()) == 1
    assert (tmp_path / "failed.jsonl").read_text() == ""
