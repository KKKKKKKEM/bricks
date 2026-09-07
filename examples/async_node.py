"""异步 Node：Graph 语义不变，Node 自己可以 await I/O。"""

from __future__ import annotations

import asyncio

from bricks import AsyncNode, Graph, Output, Ports, Runtime


class DelayedUpper(AsyncNode):
    """异步等待后将输入文本转为大写的示例节点。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
    """

    input_ports = Ports(text=str)
    output_ports = Ports(result=str)

    async def execute(self, inputs, context):
        """根据当前输入执行节点行为，并返回声明端口上的输出。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            result 端口上的大写文本 Output。
        """

        del context
        await asyncio.sleep(0.01)
        return Output(inputs["text"].upper(), "result")


def run(text: str = "asynchronous") -> str:
    """同步执行 Graph 并返回终端输出。

    Args:
        text: 示例需要转换的输入文本。

    Returns:
        当前记录携带的数据值。
    """

    graph = Graph(entrypoint="upper").add(upper=DelayedUpper())
    with Runtime() as runtime:
        runtime.register("async-node", graph)
        return runtime.run("async-node", text)[0].value


if __name__ == "__main__":
    print(run())
