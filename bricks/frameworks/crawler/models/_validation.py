"""Shared field validation and normalization for crawler models."""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Iterator, Mapping
from types import MappingProxyType
from typing import Any
from urllib.parse import urlsplit

_TOKEN = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")


def entries(
    values: Mapping[str, Any] | Iterable[tuple[str, Any]], label: str
) -> Iterator[tuple[Any, Any]]:
    if isinstance(values, Mapping):
        yield from values.items()
        return
    if isinstance(values, (str, bytes)):
        raise TypeError(f"{label} must be a mapping or an iterable of pairs")
    for pair in values:
        if not isinstance(pair, (tuple, list)) or len(pair) != 2:
            raise TypeError(f"{label} entries must be key/value pairs")
        yield pair[0], pair[1]


def token(value: str, label: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{label} must be a string")
    if not _TOKEN.fullmatch(value):
        raise ValueError(f"{label} must be an HTTP token")
    return value


def http_url(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("url must be a string")
    if any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("url must not contain whitespace or control characters")
    parsed = urlsplit(value)
    if parsed.scheme.lower() not in ("http", "https") or not parsed.hostname:
        raise ValueError("url must be an absolute HTTP or HTTPS URL")
    parsed.port
    return value


def duration(
    value: float | None, label: str, *, allow_zero: bool = False
) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{label} must be a number or None")
    if not math.isfinite(value) or value < 0 or (value == 0 and not allow_zero):
        raise ValueError(
            f"{label} must be finite and {'non-negative' if allow_zero else 'positive'}"
        )
    return float(value)


def cookies(values: Mapping[str, str] | None) -> Mapping[str, str]:
    if values is None:
        return MappingProxyType({})
    if not isinstance(values, Mapping):
        raise TypeError("cookies must be a mapping")
    result: dict[str, str] = {}
    for name, value in values.items():
        token(name, "cookie name")
        if not isinstance(value, str):
            raise TypeError("cookie values must be strings")
        if any(char in value for char in "\r\n\x00"):
            raise ValueError("cookie values must not contain CR, LF or NUL")
        result[name] = value
    return MappingProxyType(result)


def request_cookies(values: Mapping[str, str] | None) -> Mapping[str, str]:
    result = cookies(values)
    for value in result.values():
        if any(
            ord(char) < 0x21 or ord(char) > 0x7E or char in '",;\\' for char in value
        ):
            raise ValueError(
                "request cookie values must contain only HTTP cookie octets"
            )
    return result
