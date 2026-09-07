"""验证爬虫框架与独立编排微内核的包边界。"""

from importlib.util import find_spec

import bricks
from interlace import AsyncNode, Node


def test_crawler_api_uses_interlace_node_types() -> None:
    """下载节点继承已安装 Interlace 的类型，顶层仅导出领域接口。"""

    assert bricks.__all__ == [
        "AsyncDownloader",
        "AsyncDownloadNode",
        "Cookies",
        "Downloader",
        "DownloadNode",
        "Items",
        "Request",
        "Response",
        "UploadFile",
    ]
    assert issubclass(bricks.DownloadNode, Node)
    assert issubclass(bricks.AsyncDownloadNode, AsyncNode)
    assert not hasattr(bricks, "Runtime")
    assert not hasattr(bricks, "Graph")


def test_crawler_does_not_ship_an_embedded_runtime() -> None:
    """爬虫发行包不包含旧引擎或领域框架兼容入口。"""

    for module in ("engine", "runtime", "spi", "plugins", "adapters", "frameworks"):
        assert find_spec(f"bricks.{module}") is None
