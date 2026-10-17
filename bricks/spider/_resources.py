"""管理 Spider 自有 Slot、下载会话、代理和运行时资源。"""

from __future__ import annotations

from typing import Any, NoReturn, cast

from interlace import Graph, Runtime, Slot, SlotPool
from interlace.runtime import EventRouter, GraphWorker
from interlace.spi import SlotProvider

from bricks.downloaders import SessionDownloader
from bricks.proxies import Manager
from bricks.queues import QueueBackend


def _raise_primary_with_cleanup(
    primary: BaseException, cleanup_errors: list[BaseException]
) -> NoReturn:
    """传播主错误，并将首个清理错误作为 cause。

    Args:
        primary: 主操作的异常。
        cleanup_errors: 清理阶段按发生顺序收集的异常。

    Raises:
        BaseException: 始终抛出主错误；原 cause 为空时以首个清理错误作为 cause。
    """
    if (
        cleanup_errors
        and primary is not cleanup_errors[0]
        and primary.__cause__ is None
    ):
        raise primary from cleanup_errors[0]
    raise primary


def close_preserving(spider: Any, primary: BaseException | None) -> None:
    """关闭 Spider，并保留运行错误及清理异常因果关系。

    Args:
        spider: Air Spider 实例。
        primary: 已发生的运行异常；None 表示仅处理关闭结果。
    """
    cleanup_errors: list[BaseException] = []
    try:
        spider.close()
    except BaseException as error:
        cleanup_errors.append(error)
    if primary is not None:
        _raise_primary_with_cleanup(primary, cleanup_errors)
    if cleanup_errors:
        raise cleanup_errors[0]


def close_owned_sessions(spider: Any) -> list[BaseException]:
    """逆序关闭框架创建的同步下载会话。

    Args:
        spider: Air Spider 实例。

    Returns:
        未吞掉的资源关闭错误。
    """
    failures: list[BaseException] = []
    while spider._owned_sessions:
        factory, session = spider._owned_sessions.pop()
        try:
            factory.close_session(session)
        except BaseException as error:
            failures.append(error)
    return failures


def release_slot_resources(spider: Any) -> list[BaseException]:
    """关闭会话、归还托管代理并关闭自建 SlotPool。

    Args:
        spider: Air Spider 实例。

    Returns:
        按资源释放顺序收集的异常。
    """
    failures = close_owned_sessions(spider)
    if isinstance(spider.proxy, Manager):
        for slot in spider._owned_proxy_slots:
            try:
                spider.proxy.recover(slot)
            except BaseException as error:
                failures.append(error)
    spider._owned_proxy_slots.clear()
    if spider._own_slots and spider._slots is not None:
        try:
            spider._slots.close()
        except BaseException as error:
            failures.append(error)
    return failures


def open_resources(spider: Any, initial: Graph, crawl: Graph) -> None:
    """构造并注册 Runtime 及其自有消费资源，失败时统一回滚。

    Args:
        spider: Air Spider 实例。
        initial: 已构造的初始化图。
        crawl: 已构造的消费图。

    Raises:
        BaseException: 装配失败，原异常继续传播，清理错误作为 cause。
    """
    slots: SlotProvider | None = spider._slots
    backend: QueueBackend | None = None
    runtime: Runtime | None = None

    def make_slot() -> Slot:
        """创建自建槽并按需自动装配同步下载会话。"""
        slot = Slot()
        spider._owned_proxy_slots.append(slot)
        if spider._auto_session:
            factory = cast(SessionDownloader[Any], spider.downloader)
            proxy = (
                spider.proxy.get(slot) if isinstance(spider.proxy, Manager) else None
            )
            address = proxy.proxy if proxy is not None else spider.proxy
            if proxy is not None and any(
                value is not None for value in (proxy.auth, proxy.threshold, proxy.ttl)
            ):
                raise ValueError(
                    "automatic session proxy cannot use auth, threshold or ttl; "
                    "set slot_session_reuse=False"
                )
            session = factory.prepare_session(proxy=cast(str | None, address))
            slot["crawler.downloaders"] = {"default": session}
            slot["crawler.session.proxy"] = cast(str | None, address)
            spider._owned_sessions.append((factory, session))
        return slot

    try:
        if slots is None:
            slots = SlotPool(spider.concurrency, factory=make_slot)
        backend = QueueBackend(
            {spider.task_queue.name: spider.task_queue},
            lease_seconds=spider._ttl,
            poll_interval=spider._poll_interval,
            before_claim=spider._before_claim,
            before_execute=spider._after_claim,
        )
        router = EventRouter(publisher=backend)
        worker = GraphWorker(
            consumer=backend,
            emit=router.publish,
            emit_local=router.publish_local,
        )
        runtime = Runtime(router=router, worker=worker)
        runtime.register(spider.init_graph_name, initial)
        runtime.register(spider.crawl_graph_name, crawl)
        runtime.route(
            spider.local_submission_event_name,
            graph=spider.crawl_graph_name,
            queue=spider.task_queue.name,
        )
    except BaseException as primary:
        cleanup_errors: list[BaseException] = []
        for resource in (runtime, backend):
            if resource is not None:
                try:
                    resource.close()
                except BaseException as error:
                    cleanup_errors.append(error)
        cleanup_errors.extend(release_slot_resources(spider))
        _raise_primary_with_cleanup(primary, cleanup_errors)

    assert slots is not None and runtime is not None and backend is not None
    spider._slots = slots
    spider._backend = backend
    spider._runtime = runtime
    spider._state = "open"
