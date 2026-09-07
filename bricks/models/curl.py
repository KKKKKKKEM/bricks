"""单条 POSIX cURL 命令的严格转换器，不执行命令。"""

from __future__ import annotations

import argparse
import base64
import shlex
from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, NoReturn
from urllib.parse import quote_from_bytes, urlsplit

from urllib3.fields import guess_content_type

from .body import UploadFile
from .headers import MutableHeaders

if TYPE_CHECKING:
    from .request import Request


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        """将命令行解析错误转换为 ValueError，避免终止进程。

        Args:
            message: 异常说明或示例使用的消息文本。

        Raises:
            ValueError: 参数值或字段组合不合法。
        """

        raise ValueError(f"invalid or unsupported curl command: {message}")


def _file(name: str, files: Mapping[str, bytes]) -> bytes:
    """从调用方提供的文件映射读取字节，不访问文件系统。

    Args:
        name: 注册或查找使用的名称。
        files: 文件名称到内容字节的显式映射，不访问本地文件系统。

    Returns:
        调用方显式提供的文件内容字节。

    Raises:
        TypeError: 参数类型或接口实现不符合当前契约。
        ValueError: 参数值或字段组合不合法。
    """

    if name not in files:
        raise ValueError(
            f"provide files[{name!r}] explicitly; no files or stdin are read"
        )
    value = files[name]
    if not isinstance(value, bytes):
        raise TypeError("files values must be bytes")
    return value


def _split_command(command: str) -> list[str]:
    # 移除单引号外的续行标记，同时保留内容中的真实换行。
    """解析单条 POSIX 命令，保留引号内数据并处理续行。

    Args:
        command: 需要解析的单条 POSIX cURL 命令。

    Returns:
        经过引号和续行处理的命令参数列表。

    Raises:
        ValueError: 参数值或字段组合不合法。
    """

    prepared: list[str] = []
    quote = ""
    index = 0
    while index < len(command):
        char = command[index]
        if not quote and command[index : index + 2] in ("$'", '$"'):
            raise ValueError("Bash-specific quoting is unsupported; use POSIX quotes")
        if char == "\\" and quote != "'" and index + 1 < len(command):
            following = command[index + 1]
            if following != "\n":
                prepared.extend((char, following))
            index += 2
            continue
        if char in ("'", '"'):
            if not quote:
                quote = char
            elif quote == char:
                quote = ""
        prepared.append(char)
        index += 1
    return shlex.split("".join(prepared), posix=True)


def parse_curl(
    command: str, files: Mapping[str, bytes] | None = None
) -> dict[str, Any]:
    """解析单条 cURL 命令并生成 Request 构造参数，不执行命令或读取文件。

    Args:
        command: 需要解析的单条 POSIX cURL 命令。
        files: 文件名称到内容字节的显式映射，不访问本地文件系统。

    Returns:
        可直接用于 Request 构造器的关键字参数。

    Raises:
        TypeError: 参数类型或接口实现不符合当前契约。
        ValueError: 参数值或字段组合不合法。
    """

    if not isinstance(command, str):
        raise TypeError("curl command must be text")
    tokens = _split_command(command)
    if not tokens or tokens.pop(0) != "curl":
        raise ValueError("expected a single command starting with curl")
    files = {} if files is None else files
    parser = _Parser(add_help=False, allow_abbrev=False)
    parser.add_argument("urls", nargs="*")
    for flags, dest in [
        (("-X", "--request"), "method"),
        (("-x", "--proxy"), "proxy"),
        (("-m", "--max-time"), "timeout"),
        (("-u", "--user"), "user"),
        (("-A", "--user-agent"), "agent"),
        (("-e", "--referer"), "referer"),
        (("--url",), "url"),
    ]:
        parser.add_argument(*flags, dest=dest)
    for flags, dest in [
        (("-H", "--header"), "headers"),
        (("-b", "--cookie"), "cookies"),
        (("-d", "--data", "--data-ascii"), "data"),
        (("--data-raw",), "raw"),
        (("--data-binary",), "binary"),
        (("--data-urlencode",), "urlencode"),
        (("--json",), "json"),
        (("-F", "--form"), "form"),
        (("--form-string",), "form_string"),
    ]:
        parser.add_argument(*flags, dest=dest, action="append", default=[])
    parser.add_argument("-L", "--location", dest="redirects", action="store_true")
    parser.add_argument("--no-location", dest="redirects", action="store_false")
    parser.add_argument("-G", "--get", action="store_true")
    parser.add_argument("-I", "--head", action="store_true")
    for flags in [
        ("-s", "--silent"),
        ("-S", "--show-error"),
        ("-i", "--include"),
        ("-v", "--verbose"),
        ("-q", "--disable"),
        ("-g", "--globoff"),
    ]:
        parser.add_argument(*flags, action="store_true")
    args = parser.parse_intermixed_args(tokens)
    urls = args.urls + ([args.url] if args.url is not None else [])
    if len(urls) != 1:
        raise ValueError("exactly one HTTP(S) URL is required")
    parsed_url = urlsplit(urls[0])
    if not args.globoff and any(
        char in parsed_url.path + parsed_url.query for char in "{}[]"
    ):
        raise ValueError("URL globbing is unsupported; use --globoff for a literal URL")
    headers = MutableHeaders()
    suppressed: set[str] = set()
    for header in args.headers:
        if ":" in header:
            name, value = header.split(":", 1)
            if not value.strip():
                suppressed.add(name.lower())
                continue
            headers.add(name, value.lstrip())
        elif header.endswith(";"):
            headers.add(header[:-1], "")
        else:
            raise ValueError("headers must be inline name:value pairs")
    for name, value in [("User-Agent", args.agent), ("Referer", args.referer)]:
        if value is not None:
            headers.setdefault(name, value)
    if args.user is not None:
        if ":" not in args.user:
            raise ValueError("--user requires explicit username:password")
        encoded = base64.b64encode(args.user.encode()).decode("ascii")
        headers.setdefault("Authorization", "Basic " + encoded)
    cookies: dict[str, str] = {}
    for cookie in args.cookies:
        if "=" not in cookie:
            raise ValueError("cookie files and cookie-engine settings are unsupported")
        for field in cookie.split(";"):
            if field.strip():
                name, value = field.strip().split("=", 1)
                cookies[name] = value

    kinds = [
        name
        for name in (
            "data",
            "raw",
            "binary",
            "urlencode",
            "json",
            "form",
            "form_string",
        )
        if getattr(args, name)
    ]
    if len(kinds) > 1:
        raise ValueError("mixed body option families are unsupported; use one family")
    explicit_content_type = "Content-Type" in headers
    body: Any = None
    body_type = "raw"
    if kinds:
        kind = kinds[0]
        values = getattr(args, kind)
        if kind in ("form", "form_string"):
            body = []
            body_type = "multipart"
            for value in values:
                if "=" not in value:
                    raise ValueError("form fields must be name=value")
                name, data = value.split("=", 1)
                if kind == "form" and data.startswith("@"):
                    parts = data[1:].split(";")
                    path = parts.pop(0)
                    if "," in path or '"' in path:
                        raise ValueError(
                            "use separate -F options for each simple file path"
                        )
                    settings = {}
                    for part in parts:
                        key, separator, setting = part.partition("=")
                        if not separator or key not in ("type", "filename"):
                            raise ValueError("unsupported multipart file attribute")
                        settings[key] = setting
                    data = UploadFile(
                        settings.get("filename", PurePosixPath(path).name),
                        _file(path, files),
                        settings.get("type", guess_content_type(path)),
                    )
                elif kind == "form" and data.startswith("<"):
                    data = _file(data[1:], files)
                elif kind == "form" and (";" in data or data.startswith("(")):
                    raise ValueError("use --form-string for literal form fields")
                body.append((name, data))
        else:
            chunks = []
            for value in values:
                if kind == "urlencode":
                    if "=" in value:
                        name, data = value.split("=", 1)
                        chunk = (name + "=" if name else "") + quote_from_bytes(
                            data.encode(), safe=""
                        )
                    elif "@" in value:
                        name, path = value.split("@", 1)
                        chunk = (name + "=" if name else "") + quote_from_bytes(
                            _file(path, files), safe=""
                        )
                    else:
                        chunk = quote_from_bytes(value.encode(), safe="")
                    chunks.append(chunk.encode())
                else:
                    from_file = kind != "raw" and value.startswith("@")
                    chunk_bytes = (
                        _file(value[1:], files) if from_file else value.encode()
                    )
                    if kind == "data" and from_file:
                        chunk_bytes = (
                            chunk_bytes.replace(b"\r", b"")
                            .replace(b"\n", b"")
                            .replace(b"\0", b"")
                        )
                    chunks.append(chunk_bytes)
            body = (b"" if kind == "json" else b"&").join(chunks)
            headers.setdefault(
                "Content-Type",
                "application/json"
                if kind == "json"
                else "application/x-www-form-urlencoded",
            )
            if kind == "json":
                headers.setdefault("Accept", "application/json")
    url = urls[0]
    if args.get and body is not None:
        if body_type == "multipart":
            raise ValueError("--get cannot be combined with multipart")
        base, marker, fragment = url.partition("#")
        url = (
            base
            + ("&" if "?" in base else "?")
            + body.decode("utf-8")
            + (marker + fragment)
        )
        body = None
        if not explicit_content_type:
            headers.pop("Content-Type", None)
    if args.head and kinds and not args.get:
        raise ValueError("--head with a request body is unsupported")
    for name in suppressed:
        headers.pop(name, None)
    timeout = None if args.timeout is None else float(args.timeout)
    return dict(
        url=url,
        method=args.method
        or ("HEAD" if args.head else "GET" if args.get or body is None else "POST"),
        headers=headers,
        cookies=cookies,
        body=body,
        body_type=body_type,
        timeout=None if timeout == 0 else timeout,
        allow_redirects=args.redirects,
        proxy=args.proxy,
    )


def render_curl(request: Request, body_file: str | None = None) -> str:
    """生成 POSIX cURL 命令，请求体文件仅作为引用，不写入文件。

    Args:
        request: 当前 HTTP 请求或 pytest 提供的参数化夹具对象。
        body_file: cURL 命令引用的请求体文件路径，本操作不写文件。

    Returns:
        经过 POSIX shell 引用处理的单条 cURL 命令。

    Raises:
        ValueError: 参数值或字段组合不合法。
    """

    args = [
        "curl",
        "-q",
        "--globoff",
        "--request",
        request.method,
        "--url",
        request.real_url,
    ]
    if request.allow_redirects:
        args.append("--location")
    if request.timeout is not None:
        args.extend(("--max-time", str(request.timeout)))
    if request.proxy is not None:
        args.extend(("--proxy", request.proxy))
    for name, value in request.headers.raw:
        args.extend(("--header", f"{name}: {value}" if value else f"{name};"))
    if request.cookies:
        args.extend(
            (
                "--cookie",
                "; ".join(f"{name}={value}" for name, value in request.cookies.items()),
            )
        )
    if request.body is not None:
        if "Content-Type" not in request.headers:
            args.extend(("--header", "Content-Type:"))
        if body_file is not None:
            if not body_file or "\0" in body_file:
                raise ValueError("body_file must be a non-empty path")
            args.extend(("--data-binary", "@" + body_file))
        else:
            try:
                text = request.body.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ValueError(
                    "binary body requires an explicit body_file path"
                ) from exc
            if "\0" in text:
                raise ValueError("NUL bytes require an explicit body_file path")
            args.extend(("--data-raw", text))
    if any("\0" in arg for arg in args):
        raise ValueError("curl arguments cannot contain NUL bytes")
    return shlex.join(args)
