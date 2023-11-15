# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 14:09
# @Author  : Kem
# @Desc    :
import datetime
import math
from typing import Optional, List, Union

from loguru import logger

from bricks import CONST
from bricks.core import signals
from bricks.core.genesis import Chaos
from bricks.downloader import primordial
from bricks.lib.counter import FastWriteCounter
from bricks.lib.queues import TaskQueue, LocalQueue
from bricks.utils import universal


class Spider(Chaos):

    def __init__(
            self,
            concurrency: Optional[int] = 1,
            survey: Optional[Union[dict, List[dict]]] = None,
            downloader: Optional[Union[str, primordial.Downloader]] = None,
            task_queue: Optional[TaskQueue] = None,
            queue_name: Optional[str] = "",
            proxy: Optional[str] = "",
            **kwargs
    ) -> None:
        self.concurrency = concurrency
        self.survey = survey
        self.downloader = downloader
        self.task_queue = LocalQueue() if not task_queue or survey else task_queue
        self.queue_name = queue_name or self.__class__.__name__
        self.proxy = proxy
        self._total_number_of_requests = FastWriteCounter()  # 发起请求总数量
        self._number_of_failure_requests = FastWriteCounter()  # 发起请求失败数量

        for k, v in kwargs.items():
            self.set_attr(k, v)

    def run_init(self):
        """
        初始化
        :return:
        """
        task_queue: TaskQueue = self.get_attr("init.task_queue", self.task_queue)
        queue_name: str = self.get_attr("init.queue_name", self.queue_name)

        # 本地的初始化记录 -> 启动传入的
        local_init_record: dict = self.get_attr('init.record') or {}
        # 云端的初始化记录 -> 初始化的时候会存储(如果支持的话)
        remote_init_record = task_queue.obtain(queue_name, {"action": "get-init-record"}) or {}

        # 初始化记录信息
        init_record: dict = {
            **local_init_record,
            **remote_init_record,
            "queue_name": queue_name,
            "task_queue": task_queue,
            "identifier": CONST.MEACHINE_ID,
        }

        # 设置一个启动时间, 防止被覆盖
        init_record.setdefault("start", str(datetime.datetime.now()))

        # 获取已经初始化的总量
        total = int(init_record.setdefault('total', 0))
        # 获取已经初始化的去重数量
        success = int(init_record.setdefault('success', 0))
        # 获取机器的唯一标识
        identifier = CONST.MEACHINE_ID

        # 判断是否有初始化权限
        has_permission = task_queue.obtain(queue_name, {"action": "get-permission", "identifier": identifier})
        if not has_permission:
            return

        # 初始化模式: ignore/reset/continue
        init_mode = self.get_attr('init.mode')
        # 初始化总数量阈值 -> 大于这个数量停止初始化
        init_total_size = self.get_attr('init.total.size', math.inf)
        # 当前初始化总量阈值 -> 大于这个数量停止初始化
        init_count_size = self.get_attr('init.count.size', math.inf)
        # 初始化成功数量阈值 (去重) -> 大于这个数量停止初始化
        init_success_size = self.get_attr('init.success.size', math.inf)
        # 初始化队列最大数量 -> 大于这个数量暂停初始化
        init_queue_size: int = self.get_attr("init.queue.size", 100000)

        # 当前已经初始化的数量, 从 0 开始
        count = 0

        if init_mode == 'ignore':
            # 忽略模式, 什么都不做
            logger.debug("[停止投放] 忽略模式, 不进行种子初始化")
            return init_record

        elif init_mode == 'reset':
            # 重置模式, 清空初始化记录和队列种子
            task_queue.obtain(queue_name, {"action": "reset-init-record"})
            logger.debug("[开始投放] 重置模式, 清空初始化队列及记录")

        elif init_mode == 'continue':
            # 继续模式, 从上次初始化的位置开始
            logger.debug("[开始投放] 继续模式, 从上次初始化的位置开始")
            task_queue.obtain(queue_name, {"action": "continue-init-record"})

        else:
            # 未知模式, 默认为重置模式
            logger.debug("[开始投放] 未知模式, 默认为重置模式")

        for seeds in universal.invoke(
                func=self.make_seeds,
                kwargs={
                    "record": init_record,
                    "task_queue": task_queue,
                    "queue_name": queue_name,
                }
        ):
            seeds = universal.iterable(seeds)

            seeds = seeds[0:min([
                init_count_size - count,
                init_total_size - total,
                init_success_size - success,
                len(seeds)
            ])]

            try:
                fettle = self.put_seeds(**{

                    "seeds": seeds,
                    "maxsize": init_queue_size,
                    "where": "init",
                    "task_queue": task_queue,
                    "queue_name": queue_name,

                })
            except signals.Signal:
                pass

            else:
                size = len(universal.iterable(seeds))
                total += size
                success += fettle
                count += fettle
                output = f"[投放成功] 总量: {success}/{total}; 当前: {fettle}/{size}; 目标: {queue_name}"
                init_record.update({
                    "total": total,
                    "success": success,
                    "output": output,
                    "update": str(datetime.datetime.now()),
                })

                task_queue.store(queue_name, {"action": "set-init-record", "record": init_record})
                logger.debug(output)

                if total >= init_total_size or count >= init_count_size or success >= init_success_size:
                    init_record.update(finish=str(datetime.datetime.now()))
                    return init_record

        else:
            init_record.update(finish=str(datetime.datetime.now()))
            task_queue.store(queue_name, {"action": "backup-init-record", "record": init_record})
            return init_record

    def make_seeds(self):
        raise NotImplementedError

    def put_seeds(self, **kwargs):
        """
        将种子放入容器

        :param kwargs:
        :return:
        """

        maxsize = kwargs.pop('maxsize', None)
        task_queue: TaskQueue = kwargs.pop('task_queue', None) or self.task_queue
        queue_name: str = kwargs.pop('queue_name', None) or self.queue_name
        seeds = kwargs.pop('seeds', {})
        priority = kwargs.pop('priority', False)
        task_queue.continue_(queue_name, maxsize=maxsize, interval=1)
        return task_queue.put(queue_name, *universal.iterable(seeds), priority=priority, **kwargs)
