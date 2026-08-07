"""最小图运行示例。"""

from bricks import GraphBuilder, Machine


def build_graph():
    builder = GraphBuilder("approval", initial="draft")
    builder.action("draft")
    builder.terminal("approved")
    builder.transition("draft", "approve", "approved")
    return builder.build()


def main() -> None:
    machine = Machine(build_graph())
    machine.start()
    machine.dispatch("approve")
    print(machine.status.value)


if __name__ == "__main__":
    main()
