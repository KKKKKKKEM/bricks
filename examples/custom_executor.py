"""演示如何替换 ActionExecutor，而不修改 Graph 和 Machine 的编排逻辑。"""

import inspect

from bricks import GraphBuilder, Machine, Status


class RecordingExecutor:
    """一个只负责记录调用并执行 Action 的最小执行器。"""

    def __init__(self):
        self.calls = []

    def execute(self, action, context, event):
        self.calls.append((context.run_id, event.name))
        return action(context, event)

    async def execute_async(self, action, context, event):
        self.calls.append((context.run_id, event.name))
        result = action(context, event)
        if inspect.isawaitable(result):
            return await result
        return result


builder = GraphBuilder("custom-executor", initial="ready")


def prepare(context, event):
    context.set("prepared", True)


def commit(context, event):
    return {"committed": True}


builder.action("ready", prepare)
builder.terminal("done", commit)
builder.transition("ready", "finish", "done")

executor = RecordingExecutor()
machine = Machine(builder.build(), executor=executor)
machine.start()
machine.dispatch("finish")

assert machine.status is Status.COMPLETED
assert [event for _, event in executor.calls] == ["__start__", "finish"]
assert machine.context.data == {"prepared": True, "committed": True}

for run_id, event in executor.calls:
    print(run_id, event)
