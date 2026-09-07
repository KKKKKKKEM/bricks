"""流式消费多个 terminal Output，并在结束后读取完整结果。"""

from __future__ import annotations

import asyncio

from bricks import Graph, Node, Output, Ports, Runtime


class ProduceMany(Node):
    """逐项产生终端输出以展示流式交付的示例节点。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
    """

    input_ports = Ports(count=int)
    output_ports = Ports(item=int)

    def execute(self, inputs, context):
        """根据当前输入执行节点行为，并返回声明端口上的输出。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            item 端口上从 0 到 count - 1 的整数 Output 元组。
        """

        del context
        return tuple(Output(value, "item") for value in range(inputs["count"]))


def make_graph() -> Graph:
    """构造示例使用的 Graph 定义。

    Returns:
        本次操作得到的 Graph 实例。
    """

    return Graph(entrypoint="produce").add(produce=ProduceMany())


def run(count: int = 4) -> tuple[list[int], list[int]]:
    """同步迭代 Execution，并保留最终结果快照。

    Args:
        count: 示例需要产生的输出数量。

    Returns:
        流式消费得到的整数列表，以及同一执行的完整结果列表。
    """

    with Runtime() as runtime:
        runtime.register("output-stream", make_graph())
        execution = runtime.start("output-stream", count, output_buffer=2)

        streamed = [output.value for output in execution]
        completed = [output.value for output in execution.result()]
        return streamed, completed


async def arun(count: int = 4) -> tuple[list[int], list[int]]:
    """异步迭代便利接口，并直接 await 另一项 Execution。

    Args:
        count: 示例需要产生的输出数量。

    Returns:
        异步流式消费得到的整数列表，以及另一项执行的完整结果列表。
    """

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
