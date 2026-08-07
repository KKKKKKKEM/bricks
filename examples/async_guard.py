"""演示异步 Guard 只在异步运行入口中求值。"""

import asyncio

from bricks import GraphBuilder, Machine, Status


async def allowed(context, event):
    await asyncio.sleep(0)
    return context.get("approved", False)


builder = GraphBuilder("async-guard", initial="start")
builder.action("start", lambda context, event: {"approved": True})
builder.terminal("done")
builder.transition("start", "finish", "done", guard=allowed)
graph = builder.build()


async def main():
    machine = Machine(graph)
    await machine.start_async()
    await machine.dispatch_async("finish")
    assert machine.status is Status.COMPLETED
    print(machine.status.value)


asyncio.run(main())
