# 第十章：代理管理

`bricks.proxies` 保留固定、API、Redis、Clash 供应源和管理工具的业务能力。
调用方显式创建对象；模块不创建全局 `manager`、单例、下载会话或客户端。

## 在 Air 中使用

```python
from bricks.proxies import ApiProxy

provider = ApiProxy(
    "https://proxy-provider.example/api",
    threshold=10,
    handle_response=lambda response: response.json()["addresses"],
)
spider = MySpider(proxy=provider)
spider.run()
```

`proxy` 接受地址字符串、`ProxyProvider` 或 `Manager`。供应源自动装配成实例级 Manager，
缓存保存在当前 Slot。`Request.proxy` 的显式地址优先；已选地址在同一请求重试时保留。
`threshold` 是允许的新请求次数，`ttl` 是最大占用秒数，两者都可以为 None。
达到阈值不会中断正在重试的请求，下一次获取代理时才轮换。
业务明确判定代理失效时，可在 `on_retry(context)` 中调用 `context.clear_proxy()`；
它先丢弃管理器占用，再清除请求地址，下一次下载前重新选择。默认重试不会调用它。
此方法不关闭或热切调用方装配的固定代理会话。

| 供应源 | 行为 |
| --- | --- |
| `CustomProxy(key)` | 固定 URL 或 host:port，支持显式认证编码 |
| `ApiProxy(key, request=..., downloader=..., handle_response=...)` | API 批量缓冲，默认提取 IPv4:port；其他响应格式用同步解析函数 |
| `RedisProxy(key, client, key_type='set')` | 从显式客户端的 set/list 原子弹出地址，recover 归还 |
| `ClashProxy(key, selector=..., proxy=...)` | 控制选择组轮换共享出口，同一实例拒绝多个未归还占用 |

统一 `BaseProxy` 参数包括 `scheme`、`username`、`password`、同步 `auth(request)`、`threshold`、`ttl`。
自定义供应源只需实现 `get(timeout)->Proxy`、`recover(proxy)`、`clear(proxy)`，不必继承基类。
无代理时明确抛出 TimeoutError，不回退直连。程序、配置、认证、解析和存储异常继续传播。
API 只重试网络失败、429、5xx 和空地址结果；可配置有限等待，取消仍遵循同步协作式边界。

## 显式 Slot 管理

```python
from interlace import Slot
from bricks.proxies import CustomProxy, Manager

manager = Manager(CustomProxy("localhost:7890", threshold=10))
slot = Slot()
proxy = manager.get(slot, timeout=5)
manager.use(slot)
assert manager.now(slot) is proxy
manager.recover(slot)
```

`get` 取得或复用当前代理，`use` 登记一次使用，`now` 只查看；`recover` 归还可用代理，
`clear` 丢弃失效代理，`fresh` 丢弃后获取。回收失败时保留占用并报告错误。
同一 Slot 的不同 Manager 必须提供不同 `namespace`，不会覆盖彼此的资源。
多个供应源可按顺序作为 fallback；只有 TimeoutError 才尝试下一项，等待预算共同使用。

Air 自建 Slot 的代理在停止执行后归还。借用 Slot 时，应在 Spider 停止后由装配方调用
`manager.recover(slot)`；管理器不会枚举其他进程的 Slot 或关闭借用客户端。
Redis 代理供应源使用弹出/归还语义，不是任务队列租约，进程退出可能需要外部代理池补充。

## 与会话配合

代理管理工具不会热切已有 HTTP 或浏览器会话的代理。会话仍在创建时固定地址：

```python
from bricks.downloaders.curl_cffi import CurlCffiDownloader

factory = CurlCffiDownloader()
proxy = manager.get(slot)
session = factory.prepare_session(proxy=proxy.proxy)
slot["crawler.downloaders"] = {"default": session}
```

此时使用 [Slot 会话下载节点](03-crawler-download.md)，不要同时给 `Request.proxy` 赋值，
也不要再让 Air 的请求代理插件覆盖它。完整装配顺序是取得代理、创建会话、运行、关闭会话、归还代理。
默认临时下载器不跨请求保留 Cookie；相同 Slot 本身不代表已经创建持久会话。
更换代理应在原会话不再使用后明确重建，Cookie 是否迁移由业务决定。

## Clash 控制与订阅

```python
from bricks.proxies import ClashProxy

provider = ClashProxy(
    "http://127.0.0.1:9090",
    selector="Proxy",
    proxy="http://127.0.0.1:7890",
    threshold=10,
)
nodes = provider.nodes()
configs = provider.configs
# 只有显式调用才修改模式；构造函数不会修改运行配置。
provider.configure({"mode": "rule"})
```

`nodes`、`configs`、`rules` 查询控制器；`switch`、`configure` 显式修改。
`delay(name, url=..., timeout=5000)` 的 timeout 和结果 delay 使用毫秒，遵循
[Mihomo 控制 API](https://wiki.metacubex.one/en/api/)。

一个控制器的选路会影响共享出口。同一实例已有占用时，第二次 get 立即失败，switch/configure 也会拒绝。
并发账号应装配独立控制器或独立出口，不能用多个 Slot 包装同一全局选择组来宣称隔离。
本实例无法阻止外部应用或其他进程切换该控制器。框架固定的是代理配置，不承诺供应商出口 IP 不轮换。

YAML 工具通过 `uv sync --locked --extra clash` 安装。

```python
from bricks import Request
from bricks.proxies import extract_nodes, fetch_subscription, subscribe

# 已有 YAML 或 Base64 VMess 内容，用纯解析函数。
nodes = extract_nodes(subscription_text, form="clash")
# 网络订阅必须显式调用，不把内容字符串自动解释成 URL 或路径。
remote_nodes = fetch_subscription(
    Request("https://provider.example/subscription", timeout=20), form="clash"
)
config_text = subscribe(nodes, remote_nodes, http_port=7890, socks_port=7891)
```

Clash 使用 safe_load；VMess 订阅支持 TCP/WebSocket，其他传输明确失败。
配置合并复制节点，重名使用稳定数字后缀，默认仅监听本机，不静默忽略错误订阅。

[返回目录](README.md) · [下一章：从 dev 迁移](11-dev-migration.md)
