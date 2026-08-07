"""通过 BaseNode 继承扩展节点语义。"""

from dataclasses import dataclass
from typing import ClassVar

from bricks import Machine
from bricks.engine.graph import BaseNode, GraphBuilder


@dataclass(frozen=True)
class ApprovalNode(BaseNode):
    kind: ClassVar[str] = "approval"
    role: str = "reviewer"

    def enter(self, context, event, executor):
        context.set("approval_role", self.role)
        return super().enter(context, event, executor)


def build_graph():
    builder = GraphBuilder("custom-node", initial="approval")
    builder.add_node(ApprovalNode("approval", role="owner"))
    builder.terminal("done")
    builder.transition("approval", "approved", "done")
    return builder.build()


def main() -> None:
    machine = Machine(build_graph())
    machine.start()
    machine.dispatch("approved")
    print(machine.context.get("approval_role"))


if __name__ == "__main__":
    main()
