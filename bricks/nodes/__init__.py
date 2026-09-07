"""按领域行为组织的爬虫节点包。"""

from .download import AsyncDownloadNode, DownloadNode

__all__ = ["AsyncDownloadNode", "DownloadNode"]
