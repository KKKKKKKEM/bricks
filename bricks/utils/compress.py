import os
import tarfile
import zipfile
from pathlib import Path

from loguru import logger


def with_gz(source: str, output: str = None):
    """
    根据输入是文件还是目录，压缩为 gz 格式，保持内部目录结构。

    :param source: 需要压缩的目录路径 (字符串)。
    :param output: 压缩后文件的名称（包括路径），默认为源目录名+.tar.gz。如未提供，则默认命名。
    """
    # 确保源目录存在
    source = Path(source)
    if not source.exists():
        raise ValueError(f"源目录不存在: {source}")

    # 如果没有指定输出文件名，则默认为源目录名+.tar.gz
    if output is None:
        output = Path(str(source.with_suffix('.tar.gz').resolve()))
    else:
        output = Path(output)

    # 使用tarfile创建tar.gz归档
    with tarfile.open(output, 'w:gz') as tar:
        # 添加目录到归档，保持内部结构，arcname确保根目录正确
        tar.add(str(source), arcname=os.path.basename(str(source)), recursive=True)

    logger.debug(f"[gz] 成功压缩至: {output}")


def with_zip(source: str, output: str = None):
    """
    根据输入是文件还是目录，压缩为zip格式，保持内部目录结构。

    :param source: 需要压缩的文件或目录路径 (字符串)。
    :param output: 压缩后文件的名称（包括路径），默认根据输入类型命名。如未提供，文件则为文件名+.zip，目录则为目录名+.zip。
    :return: 成功或失败的信息
    """
    source = Path(source)
    if not source.exists():
        raise ValueError(f"源目录不存在: {source}")

    if output is None:
        output = Path(str(source.with_suffix(".zip").resolve()))
    else:
        output = Path(output)

    if source.is_dir():
        # 如果是目录
        dir_name = source.name
        base_name = ""

    elif source.is_file():
        # 如果是文件
        dir_name = source.parent
        base_name = source.name

    else:
        raise ValueError(f"未知的路径类型: {source}")

    with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as zipf:
        if source.is_dir():
            # 遍历目录，递归添加文件和子目录到zip归档
            for root, dirs, files in os.walk(source):
                for file in files:
                    # 获取相对于源目录的路径，确保在zip中保持相同结构
                    relative_path = os.path.relpath(os.path.join(root, file), str(dir_name))
                    zipf.write(os.path.join(root, file), relative_path)
        elif source.is_file():
            # 直接添加文件到zip归档
            zipf.write(source, arcname=base_name)

    logger.debug(f"[zip] 成功压缩至: {output}")
