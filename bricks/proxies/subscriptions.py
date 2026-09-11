"""显式解析 Clash/VMess 订阅并生成配置，不执行 YAML 对象或动态代码。"""

from __future__ import annotations

import base64
import copy
import inspect
import json
from collections.abc import Mapping, Sequence
from typing import Any

from bricks import Downloader, Request, Response
from bricks.downloaders.curl_cffi import CurlCffiDownloader


def _decode(value: str) -> str:
    """解码标准或 URL 安全的 Base64 文本，兼容无填充订阅。

    Args:
        value: Base64 文本。

    Returns:
        UTF-8 文本。

    Raises:
        ValueError: 编码内容非法。
    """
    normalized = "".join(value.split())
    return base64.b64decode(
        normalized + "=" * (-len(normalized) % 4), altchars=b"-_", validate=True
    ).decode("utf-8")


def extract_nodes(content: str, *, form: str = "clash") -> list[dict[str, Any]]:
    """解析明确提供的订阅内容，绝不把文本当作文件或 URL 自动读取。

    Args:
        content: Clash YAML 或 Base64 VMess 订阅文本。
        form: clash 或 v2ray；v2ray 只包含 vmess 链接。

    Returns:
        独立 Clash 节点字典列表。

    Raises:
        ValueError: 格式、节点字段或 VMess 类型不支持。
        ImportError: Clash YAML 未安装 clash extra。
    """
    if not isinstance(content, str):
        raise TypeError("subscription content must be text")
    if form == "clash":
        import yaml

        data = yaml.safe_load(content)
        nodes = data.get("proxies", []) if isinstance(data, dict) else None
        if not isinstance(nodes, list) or any(
            not isinstance(node, dict)
            or not isinstance(node.get("name"), str)
            or not node["name"]
            for node in nodes
        ):
            raise ValueError("Clash subscription requires named proxy objects")
        return copy.deepcopy(nodes)
    if form != "v2ray":
        raise ValueError("subscription form must be clash or v2ray")
    nodes = []
    for link in _decode(content).split():
        if not link.startswith("vmess://"):
            raise ValueError("v2ray subscriptions currently support vmess links")
        value = json.loads(_decode(link[len("vmess://") :]))
        tls = value.get("tls", "")
        if tls not in ("", "tls", False, True):
            raise ValueError("unsupported VMess TLS mode")
        node = {
            "name": value["ps"],
            "server": value["add"],
            "port": int(value["port"]),
            "type": "vmess",
            "uuid": value["id"],
            "alterId": int(value.get("aid", 0)),
            "cipher": value.get("scy", "auto"),
            "tls": tls in ("tls", True),
            "skip-cert-verify": False,
        }
        network = value.get("net", "tcp")
        if network == "ws":
            node.update(
                {
                    "network": "ws",
                    "ws-opts": {
                        "path": value.get("path", "/"),
                        "headers": {"Host": value.get("host", "")},
                    },
                }
            )
        elif network != "tcp":
            raise ValueError("only TCP and WebSocket VMess transports are supported")
        if value.get("sni"):
            node["servername"] = value["sni"]
        nodes.append(node)
    return nodes


def fetch_subscription(
    request: Request, *, form: str = "clash", downloader: Downloader | None = None
) -> list[dict[str, Any]]:
    """显式下载订阅内容后解析，下载错误原样报告。

    Args:
        request: 明确的订阅请求，含调用方设置的凭据和超时。
        form: clash 或 v2ray。
        downloader: 借用同步下载器，默认临时 curl_cffi。

    Returns:
        已解析节点。

    Raises:
        RuntimeError: 订阅响应不是 2xx。
    """
    response = (CurlCffiDownloader() if downloader is None else downloader).fetch(
        request.copy()
    )
    if not isinstance(response, Response):
        if inspect.iscoroutine(response):
            response.close()
        raise TypeError("subscription downloader must return Response synchronously")
    if not 200 <= response.status_code < 300:
        raise RuntimeError(
            f"subscription request failed: status={response.status_code}"
        ) from response.error
    return extract_nodes(response.text, form=form)


def cfg_tpl(*, http_port: int = 7890, socks_port: int = 7891) -> dict[str, Any]:
    """生成仅监听本机的独立 Clash 配置模板。

    Args:
        http_port: HTTP 代理端口。
        socks_port: SOCKS 代理端口。

    Returns:
        可由调用方继续修改的配置字典。
    """
    if any(
        type(port) is not int or not 0 < port < 65536
        for port in (http_port, socks_port)
    ):
        raise ValueError("proxy ports must be in 1..65535")
    return {
        "port": http_port,
        "socks-port": socks_port,
        "allow-lan": False,
        "bind-address": "127.0.0.1",
        "mode": "rule",
        "external-controller": "127.0.0.1:9090",
        "proxies": [],
        "proxy-groups": [{"name": "Proxy", "type": "select", "proxies": []}],
        "rules": ["MATCH,Proxy"],
    }


def subscribe(
    *groups: Sequence[Mapping[str, Any]], http_port: int = 7890, socks_port: int = 7891
) -> str:
    """合并已解析的订阅节点并生成 YAML，不隐式联网或忽略失败来源。

    Args:
        groups: extract_nodes 或 fetch_subscription 得到的节点集合。
        http_port: HTTP 代理端口。
        socks_port: SOCKS 代理端口。

    Returns:
        独立配置的 YAML 文本；重名节点获得稳定数字后缀。

    Raises:
        ValueError: 没有节点或节点名称非法。
    """
    import yaml

    config = cfg_tpl(http_port=http_port, socks_port=socks_port)
    used: set[str] = set()
    for group in groups:
        for original in group:
            node = copy.deepcopy(dict(original))
            base = node.get("name")
            if not isinstance(base, str) or not base:
                raise ValueError("proxy nodes require a non-empty name")
            name, index = base, 1
            while name in used:
                name = f"{base}-{index}"
                index += 1
            used.add(name)
            node["name"] = name
            config["proxies"].append(node)
    if not used:
        raise ValueError("subscription contains no proxy nodes")
    config["proxy-groups"][0]["proxies"] = [node["name"] for node in config["proxies"]]
    return yaml.safe_dump(config, allow_unicode=True, sort_keys=False)
