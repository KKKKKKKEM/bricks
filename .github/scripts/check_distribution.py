"""在上传前拒绝 PyPI 不接受的直接 URL 依赖，不修改发行包元数据。"""

from __future__ import annotations

import re
import sys
from email.parser import BytesParser
from pathlib import Path
from tarfile import open as open_tar
from zipfile import ZipFile

_DIRECT_REFERENCE = re.compile(
    r"^\s*[A-Za-z0-9][A-Za-z0-9._-]*(?:\s*\[[^]]*\])?\s*@"
)  # PEP 508 名称及可选 extras 后的 @ 表示直接引用，不匹配 marker 内的文本。


def check_distribution(path: Path) -> None:
    """检查发行包的真实依赖元数据，发现直接引用时阻止上传。

    Args:
        path: 本次构建的 wheel 或 sdist 路径。

    Raises:
        ValueError: 元数据数量不正确或依赖含直接 URL。
        OSError: 发行包无法读取。
    """
    if path.suffix == ".whl":
        with ZipFile(path) as archive:
            metadata_paths = [
                name
                for name in archive.namelist()
                if name.endswith(".dist-info/METADATA")
            ]
            if len(metadata_paths) != 1:
                raise ValueError(f"{path}: expected exactly one wheel METADATA")
            metadata = BytesParser().parsebytes(archive.read(metadata_paths[0]))
    else:
        with open_tar(path, "r:gz") as archive:
            metadata_paths = [
                member
                for member in archive.getmembers()
                if member.name.endswith("/PKG-INFO")
            ]
            if len(metadata_paths) != 1:
                raise ValueError(f"{path}: expected exactly one sdist PKG-INFO")
            stream = archive.extractfile(metadata_paths[0])
            if stream is None:
                raise ValueError(f"{path}: cannot read sdist PKG-INFO")
            metadata = BytesParser().parsebytes(stream.read())
    references = [
        value
        for value in metadata.get_all("Requires-Dist", [])
        if _DIRECT_REFERENCE.match(value)
    ]
    if references:
        raise ValueError(
            f"{path}: PyPI rejects direct URL dependencies: {references}. "
            "Publish the dependency to the package index first, then update "
            "pyproject.toml and uv.lock to its verified release. "
            "Do not remove dependencies from distribution metadata to bypass this check."
        )


def main() -> None:
    """检查所有显式传入的 wheel；未提供产物或检查失败时非零退出。"""
    if len(sys.argv) < 2:
        raise SystemExit("usage: check_distribution.py WHEEL [WHEEL ...]")
    for value in sys.argv[1:]:
        check_distribution(Path(value))


if __name__ == "__main__":
    main()
