# -*- coding: utf-8 -*-
# @Time    : 2023-11-13 23:05
# @Author  : Kem
# @Desc    :
import asyncio
import threading
import time

from bricks.core.dispatch import Task, Dispatcher

"""

本调度器超好用的功能：

1. 支持设置最大并发数，可以根据当前提交的任务数量自动新增 / 关闭 worker
2. worker 支持手动暂停和关闭
3. 提交的任务不管你是异步任务还是同步任务都能 hold 住
4. 提交之后返回 future, 想要等待结果只需要 future.result() 即可，你也可以取消任务, 避免异步编程

"""
if __name__ == '__main__':
    dispatcher = Dispatcher(max_workers=1)
    dispatcher.start()


    async def demo(j):
        while True:
            await asyncio.sleep(1)
            print(j)


    def demo2(j, con=None):
        con = con or (lambda: True)
        while con():
            print(f'{threading.current_thread()} -- {j}')
            time.sleep(1)


    tasks = []
    for i in range(2):
        t = dispatcher.submit_task(Task(demo, args=[i]), timeout=None)
        tasks.append(t)
    #
    time.sleep(2)
    fu = dispatcher.submit_task(Task(demo2, args=[999, lambda: True]), timeout=-1)
    fu2 = dispatcher.submit_task(Task(demo2, args=["xxx", lambda: True]), timeout=-1)
    # time.sleep(1)
    print("dispatcher.running", dispatcher.running)
    time.sleep(3)

    print(fu2.cancel())
    print(tasks[0].cancel())
    # print(tasks[0])
    while True:
        time.sleep(1)
        print("dispatcher.running", dispatcher.running)
    # 暂停一个 worker
    # dispatcher.pause_worker("worker-0")
    # print('暂停')
    #
    # time.sleep(5)
    # # 唤醒一个 worker
    # dispatcher.awake_worker("worker-0")
    # time.sleep(5)
    # # 停止一个 worker
    # dispatcher.stop_worker("worker-0")
    # time.sleep(5)
    # dispatcher.stop()
    # print(dispatcher.loop.is_running())
