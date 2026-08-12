"""事件路由：一张 Graph 发布事件，另一张 Graph 在队列中消费它。"""

from __future__ import annotations

from bricks import Graph, Node, Ports, Runtime


class Publish(Node):
    input_ports = Ports(message=str)
    output_ports = Ports()

    def execute(self, inputs, context):
        context.emit("message.created", inputs["message"])


class Collect(Node):
    input_ports = Ports(message=str)
    output_ports = Ports()

    def __init__(self, received: list[str]) -> None:
        self._received = received

    def execute(self, inputs, context):
        del context
        self._received.append(inputs["message"])


def run(message: str = "hello") -> list[str]:
    """发布事件，等待路由后的 Graph 完成。"""

    received: list[str] = []
    producer = Graph(entrypoint="publish").add(publish=Publish())
    consumer = Graph(entrypoint="collect").add(collect=Collect(received))
    with Runtime() as runtime:
        runtime.register("producer", producer)
        runtime.register("consumer", consumer)
        runtime.on(
            "message.created",
            graph="consumer",
            queue="messages",
            concurrency=2,
        )
        runtime.run("producer", message)
        runtime.wait_idle()
    return received


if __name__ == "__main__":
    print(run())
