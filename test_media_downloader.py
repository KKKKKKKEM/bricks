#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试媒体下载器的 stream 模式和 cffi 下载器
"""
import os
import sys

from bricks.utils.media_downloader import MediaDownloader

# 确保可以导入 bricks
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


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

    success = downloader.download(
        url="https://httpbin.org/bytes/102400",  # 100KB
        save_path="./test_downloads",
        filename="test_stream.bin",
        progress_callback=progress_callback
    )

    print(f"\n下载结果: {'成功' if success else '失败'}")

    # 验证文件
    if success:
        file_path = "./test_downloads/test_stream.bin"
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            print(f"文件大小: {size} bytes")
            os.remove(file_path)
            print("测试文件已清理")

    print()
    return success


def test_concurrent_download():
    """测试并发下载"""
    print("=" * 60)
    print("测试2: 并发分块下载")
    print("=" * 60)

    downloader = MediaDownloader()

    success = downloader.download(
        url="https://httpbin.org/bytes/1048576",  # 1MB
        save_path="./test_downloads",
        filename="test_concurrent.bin",
        chunk_size=256 * 1024,  # 256KB
        max_workers=4,
        progress_callback=lambda d, t, s: print(
            f"\r进度: {d*100//t if t>0 else 0}% 速度: {s:.2f}MB/s", end=''
        ) if d == t or d % 102400 == 0 else None  # 每100KB更新一次
    )

    print(f"\n下载结果: {'成功' if success else '失败'}")

    # 清理测试文件
    if success:
        file_path = "./test_downloads/test_concurrent.bin"
        if os.path.exists(file_path):
            os.remove(file_path)
            print("测试文件已清理")

    print()
    return success


def test_stream_interface():
    """测试 $options.stream 参数"""
    print("=" * 60)
    print("测试3: stream 模式（通过 $options 参数）")
    print("=" * 60)

    from bricks import Request
    from bricks.downloader.cffi import Downloader as CffiDownloader

    downloader = CffiDownloader()

    try:
        request = Request(
            url="https://httpbin.org/bytes/10240",  # 10KB
            method="GET",
        )
        request.options["stream"] = True
        request.options["chunk_size"] = 1024

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
        "并发分块下载": test_concurrent_download(),
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
