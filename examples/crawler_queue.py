"""演示任务队列的独立生产者、消费者和本地组合运行。"""

from __future__ import annotations

import argparse
import json
from contextlib import ExitStack

from interlace import Context, Graph, Node, Runtime
from interlace.runtime import LocalInfrastructurePlugin

from bricks.queues import MemoryTaskQueue, QueueBackend, TaskQueue, make_work


class PrintSeed(Node):
    """打印领取到的 JSON 种子，不把日志输出当作幂等存储。"""

    inputs = {"seed": dict}  # 队列传来的 JSON 种子数据。
    outputs = {}  # 本示例通过标准输出展示处理过程。

    def execute(self, inputs, context: Context) -> None:
        """处理当前种子并展示当前进程的 Slot 存在性。

        Args:
            inputs: 含 seed 端口的输入映射。
            context: 当前节点独立的执行上下文。
        """
        print(
            json.dumps(
                {"seed": inputs["seed"], "has_slot": context.slot is not None},
                ensure_ascii=False,
            )
        )


def produce(queue: TaskQueue, count: int) -> None:
    """通过初始化租约投放稳定 ID 的种子，并逐项保存断点。

    Args:
        queue: 调用方拥有的任务存储。
        count: 此轮种子总数。

    Raises:
        ValueError: 已有断点来自不同规模的初始化。
        LeaseLostError: 生产者已失去写入权。
    """
    lease = queue.acquire_init("example-producer")
    if lease is None:
        return
    record = queue.initialization()
    progress = None if record is None else record.progress
    if progress is not None and progress["count"] != count:
        queue.finish_init(lease, success=False, error="initialization count changed")
        raise ValueError("use a new queue name when changing the initialization count")
    start = 0 if progress is None else progress["next"]
    for index in range(start, count):
        lease = queue.renew_init(lease)
        queue.put(make_work("crawl@v1", {"page": index + 1}), init=lease)
        queue.save_init(lease, {"next": index + 1, "count": count})
    queue.finish_init(lease, success=True)


def consume(queue: TaskQueue, concurrency: int) -> None:
    """装配当前进程的 Runtime，并等待已有任务排空。

    Args:
        queue: 调用方拥有的任务存储。
        concurrency: 此进程的执行并发。
    """
    backend = QueueBackend({queue.name: queue})
    try:
        with Runtime(plugins=[LocalInfrastructurePlugin(tasks=backend)]) as runtime:
            runtime.register(
                "crawl@v1", Graph(entrypoint="print").add(print=PrintSeed())
            )
            runtime.consume(queue.name, concurrency=concurrency)
            runtime.wait_idle(timeout=30)
    finally:
        backend.close()


def main() -> None:
    """读取命令行配置，在本地或 Redis 上运行队列示例。

    Raises:
        ValueError: 数量或并发非法。
        RuntimeError: 队列或消费执行失败。
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--redis-url")
    parser.add_argument("--queue", default="queue-demo-v1")
    parser.add_argument(
        "--role", choices=("both", "produce", "consume"), default="both"
    )
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--concurrency", type=int, default=2)
    args = parser.parse_args()
    if args.count < 0 or args.concurrency < 1:
        raise ValueError("count must be nonnegative and concurrency must be positive")
    if not args.redis_url and args.role != "both":
        parser.error("separate processes require --redis-url")
    with ExitStack() as resources:
        queue: TaskQueue
        if args.redis_url:
            from redis import Redis

            from bricks.queues.redis import RedisTaskQueue

            client = Redis.from_url(
                args.redis_url, socket_timeout=2, socket_connect_timeout=2
            )
            resources.callback(client.close)
            queue = RedisTaskQueue(client, args.queue, prefix="bricks:demo")
        else:
            queue = MemoryTaskQueue(args.queue)
        if args.role in ("both", "produce"):
            produce(queue, args.count)
        if args.role in ("both", "consume"):
            consume(queue, args.concurrency)
        print(json.dumps(queue.counts(), ensure_ascii=False))


if __name__ == "__main__":
    main()
