"""异步 Node：Graph 语义不变，Node 自己可以 await I/O。"""

from __future__ import annotations

import asyncio

from bricks import AsyncNode, Graph, Output, Ports, Runtime


class DelayedUpper(AsyncNode):
    input_ports = Ports(text=str)
    output_ports = Ports(result=str)

    async def execute(self, inputs, context):
        del context
        await asyncio.sleep(0.01)
        return Output(inputs["text"].upper(), "result")


def run(text: str = "asynchronous") -> str:
    graph = Graph(entrypoint="upper").add(upper=DelayedUpper())
    with Runtime() as runtime:
        runtime.register("async-node", graph)
        return runtime.run("async-node", text)[0].value


if __name__ == "__main__":
    print(run())
