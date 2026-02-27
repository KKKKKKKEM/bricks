#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
媒体下载器使用示例

展示如何使用 MediaDownloader 进行各种场景的媒体文件下载
"""
import time

from bricks import Request
from bricks.utils.media_downloader import DownloadTask, MediaDownloader


def example_simple_download():
    """示例1: 简单下载"""
    print("=== 示例1: 简单下载 ===")

    downloader = MediaDownloader()

    # 简单下载一个文件 - 使用 Lorem Picsum 的示例图片
    result = downloader.download_url(
        url="https://picsum.photos/200/300",
        save_dir="./downloads",
        filename="test_image.jpg",
    )

    print(f"下载结果: {'成功' if result.ok else '失败'}")
    print()


def example_download_with_progress():
    """示例2: 带进度显示的下载"""
    print("=== 示例2: 带进度显示的下载 ===")

    def progress_callback(p):
        """进度回调函数"""
        if p.total:
            percent = (p.downloaded / p.total) * 100
            print(f"\r进度: {percent:.1f}% ({p.downloaded}/{p.total} bytes) 速度: {p.speed_mbps:.2f} MB/s", end="")
        else:
            print(f"\r已下载: {p.downloaded} bytes 速度: {p.speed_mbps:.2f} MB/s", end="")

    downloader = MediaDownloader()

    # 下载一个小文件示例 - Python logo
    task = DownloadTask.from_url(
        url="https://www.python.org/static/favicon.ico",
        save_dir="./downloads",
        filename="python_favicon.ico",
        progress_callback=progress_callback,
        show_progress=False,  # 使用自定义回调，不需要 tqdm
    )
    result = downloader.download(task)

    print(f"\n下载结果: {'成功' if result.ok else '失败'}")
    print()




def example_resume_download():
    """示例4: 断点续传"""
    print("=== 示例4: 断点续传 ===")

    downloader = MediaDownloader()

    # 第一次下载（可能中断）
    print("开始第一次下载...")
    # 下载一个图片用于测试断点续传
    request = Request(url="https://picsum.photos/1500/2000")
    task = DownloadTask(
        request=request,
        save_dir="./downloads",
        filename="resume_test.jpg",
        resume=True,  # 启用断点续传
    )

    # 第一次下载
    downloader.download(task)

    # 模拟第二次继续下载
    print("\n继续下载...")
    result = downloader.download(task)

    print(f"下载结果: {'成功' if result.ok else '失败'}")
    print()


def example_batch_download():
    """示例5: 批量下载"""
    print("=== 示例5: 批量下载 ===")

    downloader = MediaDownloader()

    # 定义多个下载任务 - 使用不同尺寸的图片
    tasks = [
        DownloadTask(
            request=Request(url="https://picsum.photos/400/300"),
            save_dir="./downloads/batch",
            filename="image1.jpg"
        ),
        DownloadTask(
            request=Request(url="https://picsum.photos/500/400"),
            save_dir="./downloads/batch",
            filename="image2.jpg"
        ),
        DownloadTask(
            request=Request(url="https://picsum.photos/600/500"),
            save_dir="./downloads/batch",
            filename="image3.jpg"
        )
    ]

    # 批量下载，最多同时3个任务
    report = downloader.download_many(tasks, concurrency=3)

    print("批量下载结果:")
    for r in report.results:
        print(f"  {r.url} -> {r.path}: {'成功' if r.ok else '失败'}")
    print()


def example_custom_headers():
    """示例6: 使用自定义请求头"""
    print("=== 示例6: 使用自定义请求头 ===")

    downloader = MediaDownloader()

    # 下载 Python 官网的 logo (SVG)
    request = Request(
        url="https://www.python.org/static/community_logos/python-logo.png",
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "Referer": "https://www.python.org",
            "Accept": "image/*"
        }
    )
    task = DownloadTask(
        request=request,
        save_dir="./downloads",
        filename="python_logo.png",
    )
    result = downloader.download(task)

    print(f"下载结果: {'成功' if result.ok else '失败'}")
    print()




def example_download_video():
    """示例9: 下载视频文件（模拟）"""
    print("=== 示例9: 下载视频文件 ===")


    downloader = MediaDownloader()

    # 模拟下载大文件 - 使用高分辨率图片
    request = Request(
        url="https://cdn.pixabay.com/video/2023/04/08/158043-815940653.mp4?download")
    task = DownloadTask(
        request=request,
        save_dir="./downloads/videos",
        filename="158043.mp4",
        resume=True,
        skip_existing=False,
    )

    result = downloader.download(task)

    print(f"\n视频下载结果: {'成功' if result.ok else '失败'}")
    print()


def run_all_examples():
    """运行所有示例"""
    examples = [
        example_simple_download,
        example_download_with_progress,
        example_resume_download,
        example_batch_download,
        example_custom_headers,
        example_download_video,
    ]

    for i, example in enumerate(examples, 1):
        try:
            example()
        except Exception as e:
            print(f"示例 {i} 执行出错: {e}\n")

        # 稍微延迟一下
        if i < len(examples):
            time.sleep(0.5)


if __name__ == "__main__":
    # 运行单个示例
    # example_simple_download()
    # example_download_with_progress()
    # example_concurrent_download()
    # example_resume_download()
    # example_batch_download()
    # example_custom_headers()
    # example_with_custom_request()
    # example_integrate_with_spider()
    # example_download_video()

    # 运行所有示例
    print("媒体下载器使用示例\n")
    print("=" * 60)
    # run_all_examples()
    example_download_video()
    print("=" * 60)
    print("\n所有示例执行完成！")
