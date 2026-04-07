# RPC 模式

Bricks 提供两层 RPC 能力：

| 层次 | 入口 | 适用场景 |
|---|---|---|
| **爬虫专用层** | `bricks.spider.addon.Rpc` | 将爬虫转为"按需抓取 API"，内置 execute/submit 语义 |
| **通用层** | `bricks.rpc` | 将任意 Python 对象的方法暴露为远程服务 |

---

# 一、爬虫专用 RPC（`bricks.spider.addon.Rpc`）

通过 `Rpc.wrap()` 将爬虫包装后，外部系统可以向爬虫提交单个种子并同步获取抓取结果，实现"爬虫即 API"。

---

## 核心概念

| 组件 | 说明 |
|---|---|
| `Rpc.wrap(SpiderClass)` | 将爬虫类转化为 RPC 服务对象 |
| `rpc.serve(mode, ident)` | 同时启动爬虫后台运行 + RPC 服务器监听，**两者合一** |
| `rpc.execute(seeds)` | **同步**提交种子，阻塞等待返回 Context 结果 |
| `rpc.submit(seeds)` | **异步**投放种子到队列，不等待结果 |

---

## 快速入门：HTTP RPC

### 服务端

```python
from bricks import Request
from bricks.spider import air
from bricks.spider.addon import Rpc
from bricks.spider.air import Context


class MySpider(air.Spider):

    def make_seeds(self, context: Context, **kwargs):
        return []  # RPC 模式下初始种子由客户端提交，make_seeds 可返回空

    def make_request(self, context: Context) -> Request:
        seeds = context.seeds
        return Request(
            url="https://api.example.com/item",
            params={"id": seeds["id"]},
        )

    def parse(self, context: Context):
        return context.response.extract(
            engine="json",
            rules={"data": {"id": "id", "title": "title"}},
        )

    def item_pipeline(self, context: Context):
        context.success()


if __name__ == "__main__":
    # 1. 将 Spider 转化为 Rpc 对象
    rpc = Rpc.wrap(MySpider, attrs={"concurrency": 5})

    # 2. 同时启动爬虫 + RPC 服务器（阻塞运行）
    rpc.serve(mode="http", ident=8080)
```

### 客户端

使用内置 `Client` 调用 `execute` 方法，传入种子，同步获取结果：

```python
from bricks.rpc.http_ import service

client = service.Client("http://localhost:8080")

# 调用 execute 方法，提交种子并等待抓取完成
result = client.rpc("execute", {"id": "12345"})

print(result.code)    # 0 = 成功
print(result.data)    # 抓取结果（dict），包含 data/type/seeds 三个字段
# {"data": "...", "type": "$response", "seeds": {"id": "12345"}}
```

---

## `$futureType` — 控制返回内容

在种子里加入 `$futureType` 键，可以控制 `execute()` 返回的阶段：

| `$futureType` 值 | 返回内容 | 说明 |
|---|---|---|
| `"$response"`（默认）| `response.text` | 返回原始响应文本，在 `parse` 之前截止 |
| `"$request"` | `request.curl` | 只生成请求，不发送，返回 curl 命令字符串 |
| `"$items"` | 解析后的 `items` 列表 | 返回 `parse` 的结果，在 `item_pipeline` 之前截止 |

```python
# 只获取原始响应（默认）
result = client.rpc("execute", {"id": "12345", "$futureType": "$response"})
# result.data → {"data": "<html>...", "type": "$response", "seeds": {...}}

# 获取解析后的数据
result = client.rpc("execute", {"id": "12345", "$futureType": "$items"})
# result.data → {"data": [{"id": "12345", "title": "..."}], "type": "$items", "seeds": {...}}

# 只生成请求不发送
result = client.rpc("execute", {"id": "12345", "$futureType": "$request"})
# result.data → {"data": "curl ...", "type": "$request", "seeds": {...}}
```

---

## `execute` vs `submit`

```python
# execute：同步等待，返回抓取结果
result = client.rpc("execute", {"id": "12345"})
print(result.data)  # 拿到结果后才继续

# submit：异步投放，不等待
client.rpc("submit", {"id": "12345"})
# 种子已投入队列，爬虫后台处理，客户端立即返回
```

---

## RPC 消息格式（底层）

如果不使用内置 `Client`，可以直接发送 HTTP 请求，但必须遵守以下格式：

### 请求格式

```json
{
    "method": "execute",
    "data": "{\"args\": [{\"id\": \"12345\", \"$futureType\": \"$items\"}], \"kwargs\": {}}",
    "request_id": "可选UUID"
}
```

- `data` 字段是 **JSON 编码的字符串**，内容为 `{"args": [...], "kwargs": {...}}`
- `execute` 的第一个位置参数是种子 dict

### 响应格式

```json
{
    "code": 0,
    "message": "success",
    "data": "{\"data\": [...], \"type\": \"$items\", \"seeds\": {\"id\": \"12345\"}}",
    "request_id": "..."
}
```

- `data` 字段是 JSON 编码的字符串，需再次 `json.loads` 解析
- `code` 非 0 时表示错误（400/404/500 等）

### PING 心跳

发送 `{"method": "PING", "data": "{}"}` 会收到 `{"data": "PONG", "code": 0, ...}`

---

## 各传输模式

### HTTP 模式

```python
rpc = Rpc.wrap(MySpider, attrs={"concurrency": 5})
rpc.serve(mode="http", ident=8080)
# ident = 端口号，0 表示随机端口
```

依赖：`pip install aiohttp==3.10.11`

客户端：
```python
from bricks.rpc.http_ import service
client = service.Client("http://localhost:8080")
result = client.rpc("execute", {"id": "1"})
```

---

### Redis 模式

```python
rpc = Rpc.wrap(MySpider, attrs={"concurrency": 5})
rpc.serve(mode="redis", ident="redis://127.0.0.1:6379/0")
# 启动后日志会打印实际连接字符串（含 server_id），客户端需要它
# 例如：Redis RPC Server started at: redis://127.0.0.1:6379/0?server_id=abc123
```

Redis 模式使用 **Redis List**（`BLPOP`/`RPUSH`），**不是 Pub/Sub**。

客户端需要提供 `server_id`（从服务启动日志或 `on_server_started` 回调获取）：

```python
from bricks.rpc.redis_ import service

client = service.Client("redis://127.0.0.1:6379/0?server_id=abc123")
result = client.rpc("execute", {"id": "1"})
```

---

### WebSocket 模式

```python
rpc = Rpc.wrap(MySpider, attrs={"concurrency": 5})
rpc.serve(mode="websocket", ident=8765)
```

依赖：`pip install websockets==13.1`

客户端：
```python
from bricks.rpc.websocket_ import service
client = service.Client("ws://localhost:8765")
result = client.rpc("execute", {"id": "1"})
client.close()
```

---

## `Rpc.wrap()` 参数说明

```python
rpc = Rpc.wrap(
    spider=MySpider,       # 爬虫类（不是实例）
    attrs={                # Spider.__init__ 的参数
        "concurrency": 5,
        "downloader": ...,
    },
    modded={},             # 魔改 Spider 类的方法
    ctx_modded={},         # 魔改 Context 类的方法
    mocker=None,           # 自定义 Mocker（控制任务完成逻辑）
    ensure_local=True,     # 强制使用本地队列（推荐开启）
)
```

---

## 注意事项

> **事件部分失效**：RPC 模式会在特定阶段提前截止任务，导致部分事件不触发：
>
> - `$request` 模式：只有 `BEFORE_REQUEST` 事件生效
> - `$response` 模式：`BEFORE_REQUEST`、`AFTER_REQUEST` 生效
> - `$items` 模式：`BEFORE_REQUEST`、`AFTER_REQUEST` 生效
>
> `BEFORE_PIPELINE`、`AFTER_PIPELINE` 等存储阶段的事件在 RPC 模式下**不会触发**。

---

## 下一步

- [爬虫基类](spiders.md)
- [事件系统](events.md)
- [存储插件](storage.md)

---

# 二、通用 RPC（`bricks.rpc`）

`bricks.rpc` 是一个通用的方法分派框架，与爬虫无关。你可以将**任意 Python 对象**的公共方法暴露为远程服务，客户端通过 JSON 消息调用这些方法并获取返回值。

---

## 快速入门

```python
from bricks.rpc import serve

class Calculator:
    def add(self, a: int, b: int) -> int:
        return a + b

    def greet(self, name: str) -> str:
        return f"Hello, {name}!"

# 阻塞运行，暴露 Calculator 的所有公共方法
serve(Calculator(), mode="http", ident=8080)
```

```python
from bricks.rpc.http_ import service

client = service.Client("http://localhost:8080")

result = client.rpc("add", 1, 2)           # 位置参数
result = client.rpc("greet", name="Tom")   # 关键字参数

print(result.code)   # 0 = 成功
print(result.data)   # 返回值（已自动 json.loads 解析）
```

---

## `serve()` 函数签名

```python
from bricks.rpc import serve

serve(
    *obj,                        # 要暴露的对象（一个或多个）
    mode: str = "http",          # "http" | "websocket" | "socket" | "grpc" | "redis"
    concurrency: int = 10,       # 线程池大小
    ident: Any = 0,              # HTTP/WS/Socket: 端口号；Redis: 连接字符串
    on_server_started=None,      # 服务启动后的回调 fn(identity: str)
    **kwargs                     # 透传给各协议服务器的额外参数（如 uri）
)
```

---

## RPC 消息格式（所有协议通用）

### 请求格式

```json
{
    "method": "方法名",
    "data": "{\"args\": [位置参数...], \"kwargs\": {\"关键字\": \"参数\"}}",
    "request_id": "可选UUID"
}
```

- `data` 字段是 **JSON 编码的字符串**，内容为 `{"args": [...], "kwargs": {...}}`
- `request_id` 可选，不传由服务端自动生成

### 响应格式

```json
{
    "code": 0,
    "message": "success",
    "data": "\"返回值的JSON字符串\"",
    "request_id": "与请求相同的ID"
}
```

- `code`：`0` 成功，`400`/`404`/`500` 等为错误
- `data`：返回值的 JSON 编码字符串（内置 Client 会自动 `json.loads` 解析）

### PING 心跳

```json
{"method": "PING", "data": "{}"}
→ {"data": "PONG", "code": 0, ...}
```

---

## 各传输模式

### HTTP 模式

```python
serve(my_obj, mode="http", ident=8080)
serve(my_obj, mode="http", ident=8080, uri="/api/rpc")  # 自定义路径，默认 /rpc
```

依赖：`pip install aiohttp==3.10.11`

客户端：

```python
from bricks.rpc.http_ import service

client = service.Client("http://localhost:8080")
result = client.rpc("my_method", arg1, key=val)
print(result.code, result.data)
```

手动发送原始请求（不用内置 Client）：

```python
import json, requests

resp = requests.post("http://localhost:8080/rpc", json={
    "method": "add",
    "data": json.dumps({"args": [1, 2], "kwargs": {}}),
})
# {"code": 0, "data": "3", "message": "success", "request_id": "..."}
```

---

### Redis 模式

使用 **Redis List**（`BLPOP`/`RPUSH`），不是 Pub/Sub。

```python
serve(my_obj, mode="redis", ident="redis://127.0.0.1:6379/0")
# 日志输出：Redis RPC Server started at: redis://...?server_id=abc123
```

客户端需要从启动日志或 `on_server_started` 回调中获取 `server_id`：

```python
from bricks.rpc.redis_ import service

client = service.Client("redis://127.0.0.1:6379/0?server_id=abc123")
result = client.rpc("my_method", arg1, key=val)
```

---

### WebSocket 模式

```python
serve(my_obj, mode="websocket", ident=8765)
```

依赖：`pip install websockets==13.1`

客户端：

```python
from bricks.rpc.websocket_ import service

client = service.Client("ws://localhost:8765")
result = client.rpc("my_method", arg1, key=val)
client.close()
```

---

## 暴露多个对象

```python
class ServiceA:
    def hello(self) -> str:
        return "from A"

class ServiceB:
    def world(self) -> str:
        return "from B"

serve(ServiceA(), ServiceB(), mode="http", ident=8080)
```

客户端调用方式：

```python
client.rpc("hello")           # → ServiceA.hello（顺序查找，先匹配先用）
client.rpc("world")           # → ServiceB.world
client.rpc("ServiceA.hello")  # → 明确指定 ClassName.method_name
```
