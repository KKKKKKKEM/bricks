#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试媒体下载器的 stream 模式和 cffi 下载器
"""
import os
import sys

# 确保可以导入 bricks
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bricks import Request
from bricks.downloader.cffi import Downloader as CffiDownloader
from bricks.utils.media_downloader import DownloadTask, MediaDownloader


def test_basic_download():
    """测试基础下载功能"""
    print("=" * 60)
    print("测试1: 基础下载（使用 cffi 下载器 + stream 模式）")
    print("=" * 60)

    downloader = MediaDownloader()

    # 显示下载进度
    def progress_callback(downloaded, total, speed):
        if total > 0:
            percent = (downloaded / total) * 100
            print(
                f"\r进度: {percent:.1f}% ({downloaded}/{total} bytes) 速度: {speed:.2f}MB/s", end='')
        else:
            print(f"\r已下载: {downloaded} bytes 速度: {speed:.2f}MB/s", end='')

    result = downloader.download_url(
        url="https://httpbin.org/bytes/102400",  # 100KB
        save_dir="./test_downloads",
        filename="test_stream.bin",
        progress_callback=lambda p: progress_callback(p.downloaded, p.total or 0, p.speed_mbps),
    )

    print(f"\n下载结果: {'成功' if result.ok else '失败'}")

    # 验证文件
    if result.ok:
        file_path = "./test_downloads/test_stream.bin"
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            print(f"文件大小: {size} bytes")
            os.remove(file_path)
            print("测试文件已清理")

    print()
    return result.ok


def test_batch_download():
    """测试批量下载（并发任务）"""
    print("=" * 60)
    print("测试2: 批量下载")
    print("=" * 60)

    downloader = MediaDownloader(chunk_size=256 * 1024)

    tasks = [
        DownloadTask(
            request=Request(url="https://httpbin.org/bytes/20480"),
            save_dir="./test_downloads",
            filename="batch_1.bin",
        ),
        DownloadTask(
            request=Request(url="https://httpbin.org/bytes/30720"),
            save_dir="./test_downloads",
            filename="batch_2.bin",
        ),
        DownloadTask(
            request=Request(url="https://httpbin.org/bytes/40960"),
            save_dir="./test_downloads",
            filename="batch_3.bin",
        ),
    ]

    report = downloader.download_many(tasks, concurrency=3)
    success = report.failed == 0
    print(f"\n批量下载结果: {'成功' if success else '失败'}")
    for r in report.results:
        print(f"  {r.url} -> {r.path}: {'成功' if r.ok else '失败'}")

    # 清理测试文件
    for task in tasks:
        if os.path.exists(task.path):
            os.remove(task.path)

    print()
    return success


def test_stream_interface():
    """测试 $options.stream 参数"""
    print("=" * 60)
    print("测试3: stream 模式（通过 $options 参数）")
    print("=" * 60)

    downloader = CffiDownloader()

    try:
        request = Request(
            url="https://httpbin.org/bytes/10240",  # 10KB
            method="GET",
        )
        request.put_options("stream", True)
        request.put_options("chunk_size", 1024)

        response = downloader.fetch(request)

        print(f"响应状态码: {response.status_code}")
        print(
            f"Content-Length: {response.headers.get('Content-Length', '未知')}")

        # 使用 iter_content() 进行流式读取
        print("使用 iter_content() 进行流式读取...")

        total_size = 0
        for chunk in response.iter_content(chunk_size=1024):
            total_size += len(chunk)

        print(f"实际读取: {total_size} bytes")
        print("stream 模式测试成功！")
        return True

    except Exception as e:
        print(f"stream 模式测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """运行所有测试"""
    print("\n媒体下载器功能测试\n")

    results = {
        "基础下载 (cffi + stream)": test_basic_download(),
        "批量下载": test_batch_download(),
        "stream 模式 ($options)": test_stream_interface(),
    }

    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)

    for test_name, success in results.items():
        status = "✓ 通过" if success else "✗ 失败"
        print(f"{test_name}: {status}")

    # 清理测试目录
    if os.path.exists("./test_downloads"):
        try:
            os.rmdir("./test_downloads")
            print("\n测试目录已清理")
        except:
            pass

    all_passed = all(results.values())
    print(f"\n总体结果: {'全部通过' if all_passed else '部分失败'}")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
