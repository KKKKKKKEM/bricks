"""驱动初始化图并维护种子生产租约。"""

from __future__ import annotations

import time
from typing import Any
from uuid import uuid4

from interlace import Execution
from interlace.engine.errors import ExecutionCancelledError

from bricks.queues import InitRecord
from bricks.spider.context import Initialization


def run_initialization(spider: Any) -> InitRecord | None:
    """申请生产权、驱动初始化图并定期续租。

    Args:
        spider: Air Spider 实例。

    Returns:
        当前初始化记录；生产权由其他 Worker 持有时返回其快照。

    Raises:
        BaseException: 生产、续租、取消或断点写入失败。
    """
    lease = spider.task_queue.acquire_init(uuid4().hex, lease_seconds=spider._ttl)
    if lease is None:
        return spider.task_queue.initialization()
    previous = spider.task_queue.initialization()
    state = Initialization(
        lease,
        {} if previous is None or previous.progress is None else previous.progress,
    )
    execution: Execution | None = None
    failure: BaseException | None = None
    try:
        with spider._lifecycle:
            if spider._stopping.is_set():
                raise ExecutionCancelledError("spider is stopping")
            execution = spider.runtime.start(
                spider.init_graph_name,
                state,
                timeout=spider._init_timeout,
                options=spider._options,
            )
            spider._init_execution = execution
        heartbeat = time.monotonic() + spider._ttl / 3
        while not execution.wait(min(0.05, max(0, heartbeat - time.monotonic()))):
            if spider._stopping.is_set():
                execution.cancel()
            if time.monotonic() >= heartbeat:
                lease = spider.task_queue.renew_init(lease, lease_seconds=spider._ttl)
                heartbeat = time.monotonic() + spider._ttl / 3
        execution.result()
    except BaseException as error:
        failure = error
        if execution is not None:
            execution.cancel()
            execution.wait()
    finally:
        with spider._lifecycle:
            spider._init_execution = None
        if state.iterator is not None:
            close = getattr(state.iterator, "close", None)
            if close is not None:
                try:
                    close()
                except BaseException as error:
                    failure = failure or error
    try:
        if failure is None:
            spider.task_queue.save_init(lease, state.progress)
            spider.task_queue.finish_init(lease, success=True)
        else:
            spider.task_queue.finish_init(lease, success=False, error=str(failure))
    except BaseException as cleanup:
        if failure is not None:
            raise failure from cleanup
        raise
    if failure is not None:
        raise failure
    return spider.task_queue.initialization()
