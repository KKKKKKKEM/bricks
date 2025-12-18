# -*- coding: utf-8 -*-
# @Time    : 2025-12-18
# @Author  : Assistant
# @Desc    : SQLiteQueue 测试示例
import multiprocessing
import os
import threading
import time

from bricks.lib.queues import SQLiteQueue


def test_basic_operations():
    """测试基本操作"""
    print("=" * 50)
    print("测试基本操作")
    print("=" * 50)

    # 创建队列实例
    queue = SQLiteQueue(db_path="./test_queue.db")

    # 清空队列
    queue.clear("test_queue")

    # 1. 测试 put 操作
    print("\n1. 测试 put 操作")
    count = queue.put("test_queue", {"id": 1, "name": "task1"}, {
                      "id": 2, "name": "task2"})
    print(f"添加了 {count} 个任务")

    # 2. 测试 size 操作
    print("\n2. 测试 size 操作")
    size = queue.size("test_queue")
    print(f"队列大小: {size}")

    # 3. 测试 get 操作
    print("\n3. 测试 get 操作")
    item = queue.get("test_queue")
    print(f"获取的任务: {item}")

    # 4. 测试队列状态
    print("\n4. 测试队列状态")
    print(f"current 队列大小: {queue.size('test_queue', qtypes=('current',))}")
    print(f"temp 队列大小: {queue.size('test_queue', qtypes=('temp',))}")

    # 5. 测试 reverse 操作
    print("\n5. 测试 reverse 操作")
    queue.reverse("test_queue")
    print(f"翻转后 current 队列大小: {queue.size('test_queue', qtypes=('current',))}")

    # 6. 测试 remove 操作
    print("\n6. 测试 remove 操作")
    removed = queue.remove("test_queue", {"id": 1, "name": "task1"})
    print(f"移除了 {removed} 个任务")
    print(f"当前队列大小: {queue.size('test_queue', qtypes=('current',))}")

    # 清理
    queue.clear("test_queue")
    queue.close()
    print("\n基本操作测试完成!")


def test_persistence():
    """测试持久化"""
    print("\n" + "=" * 50)
    print("测试持久化")
    print("=" * 50)

    db_path = "./test_persistence.db"

    # 第一次：添加数据
    print("\n1. 添加数据到队列")
    queue1 = SQLiteQueue(db_path=db_path)
    queue1.clear("persist_queue")
    queue1.put("persist_queue", {"id": 1, "data": "persistent data 1"})
    queue1.put("persist_queue", {"id": 2, "data": "persistent data 2"})
    queue1.put("persist_queue", {"id": 3, "data": "persistent data 3"})
    size1 = queue1.size("persist_queue")
    print(f"添加了 {size1} 个任务")
    queue1.close()

    # 模拟程序重启
    print("\n2. 模拟程序重启...")
    time.sleep(1)

    # 第二次：读取数据
    print("\n3. 从队列读取数据")
    queue2 = SQLiteQueue(db_path=db_path)
    size2 = queue2.size("persist_queue")
    print(f"重启后队列大小: {size2}")

    items = []
    for i in range(size2):
        item = queue2.get("persist_queue")
        items.append(item)
        print(f"读取任务 {i + 1}: {item}")

    # 清理
    queue2.clear("persist_queue")
    queue2.close()

    if os.path.exists(db_path):
        os.remove(db_path)

    print("\n持久化测试完成!")


def worker_thread(queue, thread_id, count):
    """多线程测试工作函数"""
    for i in range(count):
        data = {"thread_id": thread_id, "task_id": i, "timestamp": time.time()}
        queue.put("thread_test", data)
        time.sleep(0.01)


def test_multithread():
    """测试多线程安全"""
    print("\n" + "=" * 50)
    print("测试多线程安全")
    print("=" * 50)

    db_path = "./test_multithread.db"
    queue = SQLiteQueue(db_path=db_path)
    queue.clear("thread_test")

    # 启动多个线程同时写入
    threads = []
    thread_count = 5
    tasks_per_thread = 20

    print(f"\n启动 {thread_count} 个线程，每个线程写入 {tasks_per_thread} 个任务")
    start_time = time.time()

    for i in range(thread_count):
        t = threading.Thread(target=worker_thread,
                             args=(queue, i, tasks_per_thread))
        threads.append(t)
        t.start()

    # 等待所有线程完成
    for t in threads:
        t.join()

    elapsed = time.time() - start_time
    total_size = queue.size("thread_test")

    print(f"\n所有线程完成，耗时: {elapsed:.2f}秒")
    print(f"预期任务数: {thread_count * tasks_per_thread}")
    print(f"实际任务数: {total_size}")
    print(
        f"数据一致性: {'通过' if total_size == thread_count * tasks_per_thread else '失败'}")

    # 清理
    queue.clear("thread_test")
    queue.close()

    if os.path.exists(db_path):
        os.remove(db_path)

    print("\n多线程安全测试完成!")


def worker_process(db_path, process_id, count):
    """多进程测试工作函数"""
    queue = SQLiteQueue(db_path=db_path)
    for i in range(count):
        data = {"process_id": process_id,
                "task_id": i, "timestamp": time.time()}
        queue.put("process_test", data)
        time.sleep(0.01)
    queue.close()


def test_multiprocess():
    """测试多进程安全"""
    print("\n" + "=" * 50)
    print("测试多进程安全")
    print("=" * 50)

    db_path = "./test_multiprocess.db"
    queue = SQLiteQueue(db_path=db_path)
    queue.clear("process_test")
    queue.close()

    # 启动多个进程同时写入
    processes = []
    process_count = 3
    tasks_per_process = 10

    print(f"\n启动 {process_count} 个进程，每个进程写入 {tasks_per_process} 个任务")
    start_time = time.time()

    for i in range(process_count):
        p = multiprocessing.Process(
            target=worker_process, args=(db_path, i, tasks_per_process)
        )
        processes.append(p)
        p.start()

    # 等待所有进程完成
    for p in processes:
        p.join()

    elapsed = time.time() - start_time

    # 检查结果
    queue = SQLiteQueue(db_path=db_path)
    total_size = queue.size("process_test")

    print(f"\n所有进程完成，耗时: {elapsed:.2f}秒")
    print(f"预期任务数: {process_count * tasks_per_process}")
    print(f"实际任务数: {total_size}")
    print(
        f"数据一致性: {'通过' if total_size == process_count * tasks_per_process else '失败'}")

    # 清理
    queue.clear("process_test")
    queue.close()

    if os.path.exists(db_path):
        os.remove(db_path)

    print("\n多进程安全测试完成!")


def test_smart_reverse():
    """测试智能翻转"""
    print("\n" + "=" * 50)
    print("测试智能翻转")
    print("=" * 50)

    queue = SQLiteQueue(db_path="./test_smart_reverse.db")
    queue.clear("smart_queue")

    # 场景1: current 为空，failure 有数据
    print("\n场景1: current 为空，failure 有数据")
    queue.put("smart_queue", {"task": 1}, qtypes="failure")
    queue.put("smart_queue", {"task": 2}, qtypes="failure")
    print(f"failure 大小: {queue.size('smart_queue', qtypes=('failure',))}")
    print(f"current 大小: {queue.size('smart_queue', qtypes=('current',))}")

    reversed1 = queue.smart_reverse("smart_queue")
    print(f"智能翻转结果: {reversed1}")
    print(f"翻转后 current 大小: {queue.size('smart_queue', qtypes=('current',))}")
    print(f"翻转后 failure 大小: {queue.size('smart_queue', qtypes=('failure',))}")

    queue.clear("smart_queue")

    # 场景2: current 为空，temp 有数据，running == 0
    print("\n场景2: current 为空，temp 有数据")
    queue.put("smart_queue", {"task": 3}, qtypes="temp")
    queue.put("smart_queue", {"task": 4}, qtypes="temp")
    print(f"temp 大小: {queue.size('smart_queue', qtypes=('temp',))}")
    print(f"current 大小: {queue.size('smart_queue', qtypes=('current',))}")

    reversed2 = queue.smart_reverse("smart_queue")
    print(f"智能翻转结果: {reversed2}")
    print(f"翻转后 current 大小: {queue.size('smart_queue', qtypes=('current',))}")
    print(f"翻转后 temp 大小: {queue.size('smart_queue', qtypes=('temp',))}")

    # 清理
    queue.clear("smart_queue")
    queue.close()
    print("\n智能翻转测试完成!")


if __name__ == "__main__":
    # 运行所有测试
    test_basic_operations()
    test_persistence()
    test_multithread()
    test_multiprocess()
    test_smart_reverse()

    print("\n" + "=" * 50)
    print("所有测试完成!")
    print("=" * 50)
