"""演示显式配置节点异常的自动重试。"""

from bricks import GraphBuilder, Machine, Status
from bricks.engine.policies import RetryPolicy


attempts = []


def work(context, event):
    attempts.append(context.attempt)
    if len(attempts) == 1:
        raise ConnectionError("temporary network error")


builder = GraphBuilder("exception-retry", initial="work")
builder.terminal("work", work)
machine = Machine(
    builder.build(),
    retry_policy=RetryPolicy(
        max_attempts=2,
        retry_on=(ConnectionError,),
    ),
)

machine.start()
assert machine.status is Status.WAITING
machine.resume_retry()

assert machine.status is Status.COMPLETED
assert attempts == [0, 1]
print(machine.status.value)
