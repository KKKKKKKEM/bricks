"""线性 Graph：清理文本，然后转换为大写。"""

from __future__ import annotations

from bricks import Graph, Node, Output, Ports, Runtime


class Strip(Node):
    """移除输入文本首尾空白的示例节点。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
    """

    input_ports = Ports(text=str)
    output_ports = Ports(text=str)

    def execute(self, inputs, context):
        """根据当前输入执行节点行为，并返回声明端口上的输出。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            text 端口上已去除首尾空白的文本 Output。
        """

        del context
        return Output(inputs["text"].strip(), "text")


class Upper(Node):
    """将输入文本转换为大写的示例节点。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
    """

    input_ports = Ports(text=str)
    output_ports = Ports(result=str)

    def execute(self, inputs, context):
        """根据当前输入执行节点行为，并返回声明端口上的输出。

        Args:
            inputs: 入口数据或按端口名称组织的输入映射。
            context: 当前调用的执行或插件上下文。

        Returns:
            result 端口上的大写文本 Output。
        """

        del context
        return Output(inputs["text"].upper(), "result")


def run(text: str = "  hello, bricks  ") -> str:
    """执行一条两节点的数据流，并返回未连接的终端输出。

    Args:
        text: 示例需要转换的输入文本。

    Returns:
        当前记录携带的数据值。
    """

    graph = (
        Graph(entrypoint="strip")
        .add(strip=Strip(), upper=Upper())
        .connect("strip", "upper", source_port="text", target_port="text")
    )
    with Runtime() as runtime:
        runtime.register("linear", graph)
        return runtime.run("linear", text)[0].value


if __name__ == "__main__":
    print(run())
