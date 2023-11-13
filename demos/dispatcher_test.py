# -*- coding: utf-8 -*-
# @Time    : 2023-11-13 23:05
# @Author  : Kem
# @Desc    :
import asyncio
import threading
import time

from bricks import Task, Dispatcher

if __name__ == '__main__':
    dispatcher = Dispatcher(concurrents=2)
    dispatcher.start()


    async def demo(j):
        await asyncio.sleep(1)
        print(j)


    def demo2(j):
        while True:
            print(threading.current_thread(), j)
            time.sleep(1)


    for i in range(2):
        dispatcher.submit_task(Task(demo2, args=[i]), timeout=5)
    #
    time.sleep(2)
    # 暂停一个 worker
    dispatcher.pause_worker("worker-0")
    print('暂停')

    time.sleep(5)
    # 唤醒一个 worker
    dispatcher.awake_worker("worker-0")
    time.sleep(5)
    # 停止一个 worker
    dispatcher.stop_worker("worker-0")
    time.sleep(5)
    dispatcher.shutdown()
    print(dispatcher.loop.is_running())
