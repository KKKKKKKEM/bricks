"""流式消费多个 terminal Output，并在结束后读取完整结果。"""

from __future__ import annotations

import asyncio

from bricks import Graph, Node, Output, Ports, Runtime


class ProduceMany(Node):
    input_ports = Ports(count=int)
    output_ports = Ports(item=int)

    def execute(self, inputs, context):
        del context
        return tuple(Output(value, "item") for value in range(inputs["count"]))


def make_graph() -> Graph:
    return Graph(entrypoint="produce").add(produce=ProduceMany())


def run(count: int = 4) -> tuple[list[int], list[int]]:
    """同步迭代 Execution，并保留最终结果快照。"""

    with Runtime() as runtime:
        runtime.register("output-stream", make_graph())
        execution = runtime.start("output-stream", count, output_buffer=2)

        streamed = [output.value for output in execution]
        completed = [output.value for output in execution.result()]
        return streamed, completed


async def arun(count: int = 4) -> tuple[list[int], list[int]]:
    """异步迭代便利接口，并直接 await 另一项 Execution。"""

    with Runtime() as runtime:
        runtime.register("output-stream", make_graph())
        streamed = [
            output.value
            async for output in runtime.aiter(
                "output-stream",
                count,
                output_buffer=2,
            )
        ]

        completed = await runtime.start("output-stream", count)
        return streamed, [output.value for output in completed]


if __name__ == "__main__":
    sync_stream, sync_result = run()
    async_stream, async_result = asyncio.run(arun())
    print("sync stream:", sync_stream)
    print("sync result:", sync_result)
    print("async stream:", async_stream)
    print("async result:", async_result)
