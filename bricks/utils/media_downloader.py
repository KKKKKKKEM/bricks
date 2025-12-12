# -*- coding: utf-8 -*-
# @Time    : 2025-12-12
# @Author  : Kem
# @Desc    : 媒体文件下载器 - 支持断点续传、流式下载、进度显示
import copy
import json
import os
import pathlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Union

from loguru import logger

from bricks.downloader import AbstractDownloader
from bricks.downloader.cffi import Downloader as CffiDownloader
from bricks.lib.request import Request


@dataclass
class DownloadTask:
    """下载任务"""
    request: Request
    save_path: str
    filename: Optional[str] = None
    resume: bool = True  # 是否支持断点续传
    skip_existing: bool = True  # 是否跳过已下载
    progress_callback: Optional[Callable[[
        int, int, float], None]] = None  # 进度回调

    def __post_init__(self):
        if not self.filename:
            self.filename = os.path.basename(
                self.request.real_url.split('?')[0]) or f"file_{int(time.time())}"

    @property
    def full_path(self) -> str:
        """完整文件路径"""
        return os.path.join(self.save_path, self.filename or "")

    @property
    def temp_path(self) -> str:
        """临时文件路径"""
        return f"{self.full_path}.download"

    @property
    def meta_path(self) -> str:
        """元数据文件路径"""
        return f"{self.full_path}.meta"


class DownloadMeta:
    """下载元数据管理"""

    def __init__(self, task: DownloadTask):
        self.task = task
        self.total_size: int = 0
        self.downloaded_size: int = 0
        self.support_range: bool = False
        self.etag: Optional[str] = None
        self.last_modified: Optional[str] = None

    def load(self) -> bool:
        """加载元数据"""
        if not os.path.exists(self.task.meta_path):
            return False

        try:
            with open(self.task.meta_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self.total_size = data['total_size']
            self.downloaded_size = data['downloaded_size']
            self.support_range = data['support_range']
            self.etag = data.get('etag')
            self.last_modified = data.get('last_modified')

            return True
        except Exception as e:
            logger.warning(f"加载元数据失败: {e}")
            return False

    def save(self):
        """保存元数据"""
        try:
            data = {
                'total_size': self.total_size,
                'downloaded_size': self.downloaded_size,
                'support_range': self.support_range,
                'etag': self.etag,
                'last_modified': self.last_modified,
            }

            with open(self.task.meta_path, 'w', encoding='utf-8') as f:
                json.dump(data, f)
        except Exception as e:
            logger.error(f"保存元数据失败: {e}")

    def cleanup(self):
        """清理元数据文件"""
        try:
            if os.path.exists(self.task.meta_path):
                os.remove(self.task.meta_path)
        except Exception as e:
            logger.warning(f"清理元数据文件失败: {e}")


class MediaDownloader:
    """
    媒体文件下载器

    特性：
    - 支持断点续传
    - 流式下载，内存占用低
    - 支持进度回调
    - 自动跳过已下载文件
    - 集成现有下载器特性

    示例：
        downloader = MediaDownloader()

        # 简单下载
        request = Request(url="https://example.com/video.mp4")
        downloader.download(
            request=request,
            save_path="./downloads"
        )

        # 带进度回调
        downloader.download(
            request=request,
            save_path="./downloads",
            progress_callback=lambda downloaded, total, speed:
                print(f"已下载: {downloaded}/{total} 速度: {speed:.2f}MB/s")
        )
    """

    def __init__(self, downloader: Optional[AbstractDownloader] = None, chunk_size: int = 8192):
        """
        初始化媒体下载器

        :param downloader: 使用的下载器实例，默认使用 CffiDownloader
        :param chunk_size: 下载时的块大小，默认 8192 字节
        """
        self.downloader = downloader or CffiDownloader()
        self.chunk_size = chunk_size

    @staticmethod
    def _format_size(size: int) -> str:
        """格式化文件大小"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024:
                return f"{size:.2f}{unit}"
            size /= 1024
        return f"{size:.2f}PB"

    @staticmethod
    def _format_time(seconds: float) -> str:
        """格式化时间"""
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            return f"{int(seconds // 60)}m{int(seconds % 60)}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h{minutes}m"

    @classmethod
    def _default_progress_callback(cls, downloaded: int, total: int, speed: float):
        """默认进度回调函数"""
        if total > 0:
            percent = (downloaded / total) * 100
            # 进度条长度
            bar_length = 40
            filled_length = int(bar_length * downloaded // total)
            bar = '█' * filled_length + '░' * (bar_length - filled_length)

            # 格式化大小
            downloaded_str = cls._format_size(downloaded)
            total_str = cls._format_size(total)

            # 计算剩余时间
            if speed > 0:
                remaining_bytes = total - downloaded
                eta_seconds = remaining_bytes / (speed * 1024 * 1024)
                eta_str = cls._format_time(eta_seconds)
            else:
                eta_str = "--"

            print(
                f"\r[{bar}] {percent:.1f}% {downloaded_str}/{total_str} "
                f"{speed:.2f}MB/s ETA: {eta_str}",
                end=''
            )
        else:
            # 未知总大小时的显示
            downloaded_str = cls._format_size(downloaded)
            print(
                f"\r↓ {downloaded_str} @ {speed:.2f}MB/s",
                end=''
            )

    def download(
        self,
        request: Request,
        save_path: str,
        filename: Optional[str] = None,
        resume: bool = True,
        skip_existing: bool = True,
        progress_callback: Optional[Callable[[int, int, float], None]] = None,
    ) -> bool:
        """
        下载文件

        :param request: 请求对象
        :param save_path: 保存目录
        :param filename: 文件名，默认从 URL 解析
        :param resume: 是否支持断点续传
        :param skip_existing: 是否跳过已存在的文件
        :param progress_callback: 进度回调函数 (downloaded, total, speed)，如果为 None 则使用默认回调
        :return: 是否下载成功
        """
        # 如果没有提供 progress_callback，使用默认的
        if progress_callback is None:
            progress_callback = self._default_progress_callback

        task = DownloadTask(
            save_path=save_path,
            filename=filename,
            resume=resume,
            skip_existing=skip_existing,
            progress_callback=progress_callback,
            request=request
        )

        return self.download_task(task)

    def download_task(self, task: DownloadTask) -> bool:
        """
        执行下载任务

        :param task: 下载任务
        :return: 是否下载成功
        """
        # 创建保存目录
        pathlib.Path(task.save_path).mkdir(parents=True, exist_ok=True)
        task.progress_callback = task.progress_callback or self._default_progress_callback

        # 检查文件是否已存在
        if task.skip_existing and os.path.exists(task.full_path):
            logger.info(f"文件已存在，跳过下载: {task.full_path}")
            return True

        try:
            # 获取文件信息
            meta = self._get_file_info(task)
            if meta is None:
                return False

            # 使用单线程流式下载
            return self._download_single(task, meta)

        except Exception as e:
            logger.error(f"下载失败: {e}")
            return False

    def _get_file_info(self, task: DownloadTask) -> Optional[DownloadMeta]:
        """获取文件信息"""
        meta = DownloadMeta(task)

        # 尝试加载已有的元数据
        if task.resume and meta.load():
            logger.info(
                f"加载断点续传信息，已下载: {meta.downloaded_size}/{meta.total_size}")

            # 验证文件是否有变化
            if self._verify_file_unchanged(task, meta):
                return meta
            else:
                logger.warning("远程文件已更改，重新下载")
                meta.cleanup()

        # 发送 HEAD 请求获取文件信息
        try:
            # 深拷贝 Request 对象避免修改原对象（包括 headers 等可变对象）
            request = copy.deepcopy(task.request)
            request.method = "HEAD"

            response = self.downloader.fetch(request)
            if response.status_code == 405:
                logger.warning("服务器不支持 HEAD 请求，将使用流式下载")
                meta.total_size = 0
                meta.support_range = False
                return meta

            if response.status_code not in [200, 206]:
                logger.warning(f"HEAD 请求失败: {response.status_code}，尝试直接下载")
                meta.total_size = 0
                meta.support_range = False
                return meta

            # 解析文件大小
            content_length = response.headers.get('Content-Length')
            if not content_length:
                logger.warning("无法获取文件大小，将使用流式下载")
                meta.total_size = 0
            else:
                meta.total_size = int(content_length)

            # 检查是否支持 Range 请求
            accept_ranges = response.headers.get('Accept-Ranges', '').lower()
            meta.support_range = accept_ranges == 'bytes'

            # 保存 ETag 和 Last-Modified 用于验证
            meta.etag = response.headers.get('ETag')
            meta.last_modified = response.headers.get('Last-Modified')

            logger.info(
                f"文件大小: {meta.total_size} bytes, "
                f"支持断点续传: {meta.support_range}"
            )

            return meta

        except Exception as e:
            logger.warning(f"HEAD 请求异常: {e}，将使用流式下载")
            # 发生异常时也返回 meta，使用流式下载
            meta.total_size = 0
            meta.support_range = False
            return meta

    def _verify_file_unchanged(self, task: DownloadTask, meta: DownloadMeta) -> bool:
        """验证远程文件是否未改变"""
        try:
            # 深拷贝 Request 对象避免修改原对象（包括 headers 等可变对象）
            request = copy.deepcopy(task.request)
            request.method = "HEAD"

            response = self.downloader.fetch(request)

            # 比较 ETag
            if meta.etag and response.headers.get('ETag'):
                return meta.etag == response.headers.get('ETag')

            # 比较 Last-Modified
            if meta.last_modified and response.headers.get('Last-Modified'):
                return meta.last_modified == response.headers.get('Last-Modified')

            # 比较文件大小
            content_length = response.headers.get('Content-Length')
            if content_length:
                return meta.total_size == int(content_length)

            return False

        except Exception:
            return False

    def _download_single(self, task: DownloadTask, meta: DownloadMeta) -> bool:
        """单线程下载"""
        logger.info(f"下载: {task.request.real_url}")

        try:
            # 构建请求
            headers = dict(task.request.headers or {})

            # 如果支持断点续传且有已下载内容
            if meta.support_range and os.path.exists(task.temp_path):
                downloaded_size = os.path.getsize(task.temp_path)
                if downloaded_size > 0:
                    headers['Range'] = f'bytes={downloaded_size}-'
                    meta.downloaded_size = downloaded_size
                    logger.info(f"从 {downloaded_size} 字节处继续下载")

            # 深拷贝 Request 对象避免修改原对象（包括 headers 等可变对象）
            request = copy.deepcopy(task.request)
            request.method = "GET"
            request.headers.update(headers)
            request.put_options("stream", True)
            request.put_options("chunk_size", self.chunk_size)

            response = self.downloader.fetch(request)

            if response.status_code not in [200, 206]:
                logger.error(f"下载失败: {response.status_code}")
                return False

            # 更新总大小
            if meta.total_size == 0:
                content_length = response.headers.get('Content-Length')
                if content_length:
                    meta.total_size = int(content_length) + \
                        meta.downloaded_size

            # 流式写入文件
            mode = 'ab' if meta.downloaded_size > 0 else 'wb'
            start_time = time.time()
            last_update = start_time

            with open(task.temp_path, mode) as f:
                # 使用 iter_content() 进行流式写入
                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    if chunk:
                        f.write(chunk)
                        meta.downloaded_size += len(chunk)

                        # 更新进度
                        current_time = time.time()
                        if task.progress_callback and (current_time - last_update) >= 0.5:
                            elapsed = current_time - start_time
                            speed = meta.downloaded_size / elapsed / \
                                (1024 * 1024) if elapsed > 0 else 0
                            task.progress_callback(
                                meta.downloaded_size, meta.total_size, speed)
                            last_update = current_time

            # 最终进度回调
            if task.progress_callback:
                elapsed = time.time() - start_time
                speed = meta.total_size / elapsed / \
                    (1024 * 1024) if elapsed > 0 else 0
                task.progress_callback(meta.total_size, meta.total_size, speed)
                # 输出换行，避免下一行输出混乱
                print()

            # 重命名文件
            os.rename(task.temp_path, task.full_path)
            logger.info(f"下载完成: {task.full_path}")

            return True

        except Exception as e:
            logger.error(f"单线程下载失败: {e}")
            if task.resume:
                meta.save()
            return False

    def batch_download(
        self,
        tasks: List[Union[DownloadTask, dict]],
        max_workers: int = 3,
        stop_on_error: bool = False
    ) -> Dict[str, bool]:
        """
        批量下载

        :param tasks: 下载任务列表
        :param max_workers: 最大并发下载数
        :param stop_on_error: 遇到错误是否停止
        :return: 每个任务的下载结果
        """
        results = {}

        # 转换字典为 DownloadTask
        task_list = []
        for task in tasks:
            if isinstance(task, dict):
                task = DownloadTask(**task)
            task_list.append(task)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(self.download_task, task): task
                for task in task_list
            }

            for future in as_completed(future_to_task):
                task = future_to_task[future]
                task_key = f"{task.request.real_url} -> {task.full_path}"

                try:
                    success = future.result()
                    results[task_key] = success

                    if not success and stop_on_error:
                        logger.error("遇到错误，停止批量下载")
                        executor.shutdown(wait=False)
                        break

                except Exception as e:
                    logger.error(f"任务执行异常: {e}")
                    results[task_key] = False

                    if stop_on_error:
                        executor.shutdown(wait=False)
                        break

        return results
