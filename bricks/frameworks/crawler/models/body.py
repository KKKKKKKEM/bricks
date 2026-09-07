"""内存请求体编码，不接管文件资源或承担网络传输。"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Literal

from urllib3.fields import RequestField
from urllib3.filepost import encode_multipart_formdata

from ._validation import entries, token

BodyType = Literal["auto", "json", "form", "multipart", "raw"]
BodyInput = (
    bytes
    | str
    | dict[str, Any]
    | list[Any]
    | tuple[Any, ...]
    | int
    | float
    | bool
    | None
)


class UploadFile:
    """内存中的 multipart 上传文件，文本内容按 UTF-8 编码。

    Attributes:
        filename: 上传时使用的文件名称。
        content: 当前保存的内容字节。
        content_type: 上传文件的媒体类型。
    """

    def __init__(
        self,
        filename: str,
        content: bytes | str,
        content_type: str = "application/octet-stream",
    ) -> None:
        """构造内存上传文件。

        Args:
            filename: 上传文件名。
            content: 文件字节或文本，文本按 UTF-8 编码。
            content_type: 文件媒体类型，默认 application/octet-stream。

        Raises:
            TypeError: 内容不是字节或文本，不接受文件流。
            ValueError: 文件名或媒体类型不合法。
        """

        if not isinstance(filename, str) or not filename:
            raise ValueError("filename must be a non-empty string")
        if any(ord(char) < 32 or ord(char) == 127 for char in filename):
            raise ValueError("filename must not contain control characters")
        if not isinstance(content, (bytes, str)):
            raise TypeError("file content must be bytes or text, not a stream")
        if not isinstance(content_type, str) or content_type.count("/") != 1:
            raise ValueError("file content_type must be a type/subtype media type")
        for part in content_type.split("/"):
            token(part, "file content_type")
        self.filename = filename
        self.content = content.encode("utf-8") if isinstance(content, str) else content
        self.content_type = content_type


def encode_multipart(value: BodyInput) -> tuple[bytes, str]:
    """编码内存表单和上传文件，并生成匹配的 multipart 类型头。

    Args:
        value: 有序表单字段或映射，字段值可以包含 UploadFile。

    Returns:
        请求体字节和包含实际 boundary 的 Content-Type。

    Raises:
        TypeError: 参数类型或接口实现不符合当前契约。
    """

    if not isinstance(value, (Mapping, list, tuple)):
        raise TypeError("multipart body must be a mapping or a sequence of field pairs")
    fields: list[RequestField] = []
    for name, group in entries(value, "multipart"):
        if not isinstance(name, str):
            raise TypeError("multipart field names must be strings")
        values = group if isinstance(group, (list, tuple)) else (group,)
        for item in values:
            if item is None:
                continue
            if isinstance(item, UploadFile):
                # 编码前重新校验可变的上传文件描述。
                file = UploadFile(item.filename, item.content, item.content_type)
                field = RequestField(name, file.content, filename=file.filename)
                field.make_multipart(content_type=file.content_type)
            else:
                if not isinstance(item, (str, bytes, int, float, bool)):
                    raise TypeError(
                        "multipart values must be scalar fields or UploadFile"
                    )
                data = item if isinstance(item, (str, bytes)) else str(item)
                field = RequestField(name, data)
                field.make_multipart()
            fields.append(field)
    return encode_multipart_formdata(fields)


def encode_json(value: BodyInput) -> bytes:
    """编码紧凑 UTF-8 JSON，拒绝非有限数值。

    Args:
        value: 需要按严格 JSON 规则序列化的值。

    Returns:
        紧凑 JSON 的 UTF-8 编码字节。
    """

    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")
