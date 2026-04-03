# RPC 模式

Bricks 支持将爬虫暴露为远程可调用的服务。通过 RPC 模式，外部系统可以向爬虫发送种子、触发抓取任务、并获取结果，而无需在同一进程内运行。

---

## RPC 模式概览

| 模式 | 模块 | 协议 | 特点 |
|---|---|---|---|
| HTTP | `bricks.rpc.http_` | HTTP/JSON | 最通用，无额外依赖 |
| Redis | `bricks.rpc.redis_` | Redis Pub/Sub | 低延迟，适合内网 |
| WebSocket | `bricks.rpc.websocket_` | WebSocket | 双向通信，实时推送 |
| Socket | `bricks.rpc.socket_` | TCP Socket | 低级别，高性能 |
| gRPC | `bricks.rpc.grpc_` | gRPC | 强类型，跨语言 |

---

## 快速入门：HTTP RPC

### 服务端（爬虫侧）

在爬虫中开启 HTTP RPC 服务后，外部可以通过 HTTP 请求向爬虫提交任务：

```python
from bricks import Request, const
from bricks.core import events, signals
from bricks.spider import air
from bricks.spider.air import Context


class MySpider(air.Spider):

    def make_seeds(self, context: Context, **kwargs):
        # RPC 模式下，种子由外部通过 API 提交，make_seeds 可以只返回空列表
        return []

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
        # 处理结果...
        print(context.items)
        context.success()


if __name__ == "__main__":
    spider = MySpider()

    # 启动爬虫的同时开启 HTTP RPC 服务
    spider.run(
        rpc={
            "ref": "bricks.rpc.http_",   # 使用 HTTP 协议
            "port": 8080,                 # 监听端口
        }
    )
```

### 客户端（调用侧）

任何 HTTP 客户端都可以向爬虫提交任务：

```python
import requests

# 向爬虫提交一个种子
response = requests.post(
    "http://localhost:8080/rpc",
    json={
        "method": "put_seeds",           # 投放种子
        "params": {
            "seeds": [{"id": "12345"}],  # 种子数据
            "name": "MySpider",           # 爬虫名称
        }
    }
)
print(response.json())
```

---

## RPC 请求格式

所有 RPC 请求使用统一的 JSON 格式：

```json
{
    "method": "方法名",
    "params": {
        "参数名": "参数值"
    },
    "request_id": "可选的请求ID，用于追踪"
}
```

### 支持的方法

| 方法名 | 说明 | 参数 |
|---|---|---|
| `put_seeds` | 投放种子到任务队列 | `seeds`: 种子列表，`name`: 队列名 |
| `get_seeds` | 从队列中取出种子 | `name`: 队列名，`count`: 数量 |
| `get_status` | 获取爬虫运行状态 | 无 |
| `pause` | 暂停爬虫 | 无 |
| `resume` | 恢复爬虫 | 无 |
| `stop` | 停止爬虫 | 无 |

### 响应格式

```json
{
    "code": 0,
    "message": "success",
    "data": "返回数据",
    "request_id": "与请求相同的ID"
}
```

---

## HTTP RPC 服务详解

### 启动配置

```python
spider.run(
    rpc={
        "ref": "bricks.rpc.http_",
        "port": 8080,          # 监听端口，0 为随机端口
        "concurrency": 10,     # 最大并发处理数
        "uri": "/rpc",         # API 路径，默认 /rpc
    }
)
```

依赖安装：
```bash
pip install aiohttp
```

### 投放种子示例

```python
import requests

# 投放多个种子
resp = requests.post("http://localhost:8080/rpc", json={
    "method": "put_seeds",
    "params": {
        "seeds": [
            {"id": "1", "type": "article"},
            {"id": "2", "type": "article"},
            {"id": "3", "type": "article"},
        ],
        "name": "my_spider_queue"
    }
})

# 查询队列状态
resp = requests.post("http://localhost:8080/rpc", json={
    "method": "get_status"
})
print(resp.json())
```

---

## Redis RPC

适合在内网服务间通信，通过 Redis 的 Pub/Sub 传递任务：

```python
spider.run(
    rpc={
        "ref": "bricks.rpc.redis_",
        "host": "127.0.0.1",
        "port": 6379,
        "channel": "spider_rpc_channel",  # Redis 频道名
    }
)
```

客户端向 Redis 频道发送消息：

```python
import redis
import json

r = redis.Redis()
r.publish("spider_rpc_channel", json.dumps({
    "method": "put_seeds",
    "params": {
        "seeds": [{"id": "100"}],
        "name": "my_queue"
    }
}))
```

---

## WebSocket RPC

支持双向通信，服务端可以主动推送消息给客户端：

```python
spider.run(
    rpc={
        "ref": "bricks.rpc.websocket_",
        "port": 8765,
    }
)
```

客户端使用 WebSocket 连接：

```python
import asyncio
import websockets
import json

async def main():
    async with websockets.connect("ws://localhost:8765") as ws:
        # 发送任务
        await ws.send(json.dumps({
            "method": "put_seeds",
            "params": {"seeds": [{"id": "1"}], "name": "queue"}
        }))
        # 接收响应
        response = await ws.recv()
        print(json.loads(response))

asyncio.run(main())
```

---

## 在 `form.Spider` 中使用 RPC

`form.Spider` 同样支持 RPC 模式：

```python
from bricks.spider import form


class MyFormSpider(form.Spider):

    @property
    def config(self) -> form.Config:
        return form.Config(
            spider=[
                form.Download(url="https://api.example.com/item/{id}"),
                form.Parse(func="json", kwargs={"rules": {"data": {"id": "id"}}}),
                form.Pipeline(func=lambda ctx: print(ctx.items), success=True),
            ],
        )


if __name__ == "__main__":
    MyFormSpider().run(
        rpc={"ref": "bricks.rpc.http_", "port": 9090}
    )
```

---

## 完整爬虫 + RPC 示例

以下是一个完整的示例，展示如何将爬虫作为后台服务运行，并通过 HTTP 接口按需触发抓取：

```python
# spider_server.py
from bricks import Request, const
from bricks.core import events, signals
from bricks.spider import air
from bricks.spider.air import Context
from loguru import logger


class ItemSpider(air.Spider):

    def make_seeds(self, context: Context, **kwargs):
        return []  # RPC 模式下初始种子为空

    def make_request(self, context: Context) -> Request:
        return Request(
            url=f"https://api.example.com/item/{context.seeds['id']}",
            headers={"User-Agent": "@chrome"},
        )

    def parse(self, context: Context):
        return context.response.extract(
            engine="json",
            rules={"data": {"id": "id", "title": "title", "price": "price"}},
        )

    def item_pipeline(self, context: Context):
        logger.info(f"爬取完成: {context.items}")
        context.success()

    @staticmethod
    @events.on(const.AFTER_REQUEST)
    def check(context: Context):
        if not context.response.ok:
            raise signals.Retry


if __name__ == "__main__":
    ItemSpider(concurrency=5).run(
        rpc={"ref": "bricks.rpc.http_", "port": 8080}
    )
```

```python
# client.py
import requests

# 批量提交待爬取的商品 ID
item_ids = [101, 102, 103, 104, 105]

resp = requests.post("http://localhost:8080/rpc", json={
    "method": "put_seeds",
    "params": {
        "seeds": [{"id": str(id)} for id in item_ids],
        "name": "ItemSpider"
    }
})
print(f"提交结果: {resp.json()}")
```
