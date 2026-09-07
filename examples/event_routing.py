"""事件路由：一张 Graph 发布事件，另一张 Graph 在队列中消费它。"""

from __future__ import annotations

from bricks import Graph, Node, Ports, Runtime


class Publish(Node):
    """通过上下文发布跨图事件的示例节点。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
    """

    input_ports = Ports(message=str)
    output_ports = Ports()

    def execute(self, inputs, context):
        """将输入消息发布为跨图事件，不产生本图 Output。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。
        """

        context.emit("message.created", inputs["message"])


class Collect(Node):
    """将事件输入追加到调用方消息列表的示例节点。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
        _received: 示例收集的消息列表，由调用方传入并用于观察结果。
    """

    input_ports = Ports(message=str)
    output_ports = Ports()

    def __init__(self, received: list[str]) -> None:
        """保存调用方提供的消息收集列表，供消费节点追加结果。

        Args:
            received: 已经接收的响应或示例累计的消息集合。
        """

        self._received = received

    def execute(self, inputs, context):
        """将消息追加到调用方提供的收集列表，不产生 Output。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。
        """

        del context
        self._received.append(inputs["message"])


def run(message: str = "hello") -> list[str]:
    """发布事件，等待路由后的 Graph 完成。

    Args:
        message: 需要从生产 Graph 路由到消费 Graph 的消息文本。

    Returns:
        消费 Graph 接收到的消息列表。
    """

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
