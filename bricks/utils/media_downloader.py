# -*- coding: utf-8 -*-
# @Time    : 2025-12-12
# @Author  : Kem
# @Desc    : 媒体文件下载器 - 支持断点续传、流式下载、进度显示
import copy
import csv
import json
import os
import pathlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Union

from bricks.utils import pandora

pandora.require("tqdm")

from loguru import logger  # noqa: E402
from tqdm import tqdm  # noqa: E402

from bricks.downloader import AbstractDownloader  # noqa: E402
from bricks.downloader.cffi import Downloader as CffiDownloader  # noqa: E402
from bricks.lib.request import Request  # noqa: E402


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
        return f"{self.full_path}.part"

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

    def __init__(
        self,
        downloader: Optional[AbstractDownloader] = None,
        chunk_size: int = 8192,
        prepare_row: Optional[Callable[[
            Dict[str, Any]], Dict[str, Any]]] = None,
        prepare_url: Optional[Callable[[
            str, str, Dict[str, Any]], str]] = None,
        prepare_filename: Optional[Callable[[
            Optional[str], str, Dict[str, Any]], Optional[str]]] = None,
        prepare_headers: Optional[Callable[[
            Dict[str, str], str, str, Dict[str, Any]], Dict[str, str]]] = None,
        resolve_output_path: Optional[Callable[[
            pathlib.Path, str, str, Optional[str], Dict[str, Any]], pathlib.Path]] = None,
        on_result: Optional[Callable[[bool, str, str, pathlib.Path,
                                      Dict[str, Any], Optional[Exception]], None]] = None,
    ):
        """
        初始化媒体下载器

        :param downloader: 使用的下载器实例，默认使用 CffiDownloader
        :param chunk_size: 下载时的块大小，默认 8192 字节
        :param prepare_row: 行数据预处理回调 (row) -> row
        :param prepare_url: URL 预处理回调 (url, media_id, row) -> url
        :param prepare_filename: 文件名预处理回调 (filename, media_id, row) -> filename
        :param prepare_headers: 请求头预处理回调 (headers, url, media_id, row) -> headers
        :param resolve_output_path: 自定义输出路径回调 (download_dir, url, media_id, filename, row) -> path
        :param on_result: 下载结果回调 (success, url, media_id, path, row, error) -> None
        """
        self.downloader = downloader or CffiDownloader()
        self.chunk_size = chunk_size

        # Hook 回调函数
        self.prepare_row = prepare_row
        self.prepare_url = prepare_url
        self.prepare_filename = prepare_filename
        self.prepare_headers = prepare_headers
        self.resolve_output_path = resolve_output_path
        self.on_result = on_result

        # 线程本地存储，用于管理每个线程的 tqdm 进度条位置
        self._thread_local = threading.local()
        self._position_lock = threading.Lock()
        self._position_map: Dict[int, int] = {}
        self._next_position = 0

        # 配置 logger 与 tqdm 兼容
        logger.remove()
        logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)

    def _get_position(self) -> int:
        """获取当前线程的 tqdm 进度条位置"""
        tid = threading.get_ident()
        with self._position_lock:
            pos = self._position_map.get(tid)
            if pos is None:
                pos = self._next_position
                self._next_position += 1
                self._position_map[tid] = pos
            return pos

    @staticmethod
    def _safe_rename_file(src: pathlib.Path, dst: pathlib.Path) -> bool:
        """安全地重命名文件（原子操作）

        Args:
            src: 源文件路径
            dst: 目标文件路径

        Returns:
            是否成功重命名
        """
        try:
            src.rename(dst)
            return True
        except (FileNotFoundError, OSError) as e:
            logger.warning(f"文件重命名失败: {e}")
            return False

    def download(
        self,
        request: Request,
        save_path: str,
        filename: Optional[str] = None,
        resume: bool = True,
        skip_existing: bool = True,
        progress_callback: Optional[Callable[[int, int, float], None]] = None,
        show_progress: bool = True,
        media_id: Optional[str] = None,
        row_data: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        下载文件

        :param request: 请求对象
        :param save_path: 保存目录
        :param filename: 文件名，默认从 URL 解析
        :param resume: 是否支持断点续传
        :param skip_existing: 是否跳过已存在的文件
        :param progress_callback: 进度回调函数 (downloaded, total, speed)，为 None 时使用 tqdm 进度条
        :param show_progress: 是否显示进度条（仅在 progress_callback 为 None 时有效）
        :param media_id: 媒体ID，用于 hook 回调
        :param row_data: 行数据，用于 hook 回调
        :return: 是否下载成功
        """
        row_data = row_data or {}
        media_id = media_id or ""
        url = request.real_url

        # 应用 URL hook
        if self.prepare_url:
            try:
                url = self.prepare_url(url, media_id, row_data)
                request = copy.deepcopy(request)
                request.url = url
            except Exception as e:
                logger.error(f"prepare_url 失败: {e}")
                if self.on_result:
                    self.on_result(False, url, media_id, pathlib.Path(
                        save_path) / (filename or ""), row_data, e)
                return False

        # 应用文件名 hook
        if self.prepare_filename:
            try:
                filename = self.prepare_filename(filename, media_id, row_data)
            except Exception as e:
                logger.error(f"prepare_filename 失败: {e}")
                if self.on_result:
                    self.on_result(False, url, media_id, pathlib.Path(
                        save_path) / (filename or ""), row_data, e)
                return False

        # 应用自定义路径 hook
        if self.resolve_output_path:
            try:
                full_path = self.resolve_output_path(pathlib.Path(
                    save_path), url, media_id, filename, row_data)
                save_path = str(full_path.parent)
                filename = full_path.name
            except Exception as e:
                logger.error(f"resolve_output_path 失败: {e}")
                if self.on_result:
                    self.on_result(False, url, media_id, pathlib.Path(
                        save_path) / (filename or ""), row_data, e)
                return False

        # 应用请求头 hook
        if self.prepare_headers:
            try:
                headers = dict(request.headers or {})
                headers = self.prepare_headers(
                    headers, url, media_id, row_data)
                request = copy.deepcopy(request)
                request.headers.update(headers)
            except Exception as e:
                logger.error(f"prepare_headers 失败: {e}")
                if self.on_result:
                    self.on_result(False, url, media_id, pathlib.Path(
                        save_path) / (filename or ""), row_data, e)
                return False

        task = DownloadTask(
            save_path=save_path,
            filename=filename,
            resume=resume,
            skip_existing=skip_existing,
            progress_callback=progress_callback,
            request=request
        )

        # 保存额外信息到任务对象
        task.__dict__['show_progress'] = show_progress
        task.__dict__['media_id'] = media_id
        task.__dict__['row_data'] = row_data

        result = self.download_task(task)

        # 调用结果回调
        if self.on_result:
            error = None if result else Exception("下载失败")
            self.on_result(result, url, media_id, pathlib.Path(
                task.full_path), row_data, error)

        return result

    def download_task(self, task: DownloadTask) -> bool:
        """
        执行下载任务

        :param task: 下载任务
        :return: 是否下载成功
        """
        # 创建保存目录
        pathlib.Path(task.save_path).mkdir(parents=True, exist_ok=True)

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

    @staticmethod
    def custom_bar_format(d):
        progress = f"{d['n_fmt']}/{d['total_fmt']}"
        return (
            f"{d['desc']:30} │ {d['bar']:25} {d['percentage']:6.2f}% │ "
            f"{progress:>12} │ {d['rate_fmt']:>10} │ {d['elapsed']:>5} │ {d['remaining']:>5}"
        )

    def _download_single(self, task: DownloadTask, meta: DownloadMeta) -> bool:
        """单线程下载（支持断点续传和智能重试）"""
        logger.info(f"开始下载: {task.request.real_url}")

        # 跟踪服务器是否支持断点续传
        # None: 未知, True: 支持(返回206), False: 不支持(返回200)
        supports_resume = None
        max_retries = 3

        for attempt in range(max_retries):
            try:
                # 构建请求头
                headers = dict(task.request.headers or {})

                # 检查是否有已下载内容
                downloaded_size = 0
                if os.path.exists(task.temp_path):
                    downloaded_size = os.path.getsize(task.temp_path)
                    if downloaded_size > 0 and meta.support_range:
                        headers['Range'] = f'bytes={downloaded_size}-'
                        logger.info(f"从 {downloaded_size} 字节处继续下载")

                # 深拷贝 Request 对象避免修改原对象
                request = copy.deepcopy(task.request)
                request.method = "GET"
                request.headers.update(headers)
                request.put_options("stream", True)
                request.put_options("chunk_size", self.chunk_size)

                response = self.downloader.fetch(request)

                # 处理不支持断点续传的情况（服务器返回 200 而非 206）
                if downloaded_size > 0 and response.status_code == 200:
                    logger.warning("服务器不支持断点续传，删除临时文件重新下载")
                    supports_resume = False
                    if os.path.exists(task.temp_path):
                        os.remove(task.temp_path)
                    downloaded_size = 0
                    headers.pop('Range', None)
                    continue  # 重新下载

                # 记录服务器是否支持断点续传
                if downloaded_size > 0 and response.status_code == 206:
                    supports_resume = True
                elif response.status_code == 200:
                    if supports_resume is None:
                        supports_resume = True  # 保守策略，假设支持

                # 处理 429 限流
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        wait_time = int(retry_after)
                    else:
                        wait_time = max(10, 2 ** (attempt + 3))

                    logger.warning(
                        f"遇到 429 限流，等待 {wait_time} 秒后重试 (尝试 {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error("请求被限流 (429)，已达最大重试次数")
                        return False

                if response.status_code not in [200, 206]:
                    logger.error(f"下载失败: {response.status_code}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return False

                # 更新总大小
                content_length = response.headers.get('Content-Length')
                if content_length:
                    total_size = int(content_length) + downloaded_size
                else:
                    total_size = 0

                meta.total_size = total_size
                meta.downloaded_size = downloaded_size

                # 流式写入文件
                mode = 'ab' if downloaded_size > 0 else 'wb'

                # 准备文件名用于进度显示
                file_name = os.path.basename(task.full_path)
                desc_width = 25
                resume_icon = "⏯️ " if downloaded_size > 0 else "📥"
                icon_width = 3
                filename_width = desc_width - icon_width

                if len(file_name) > filename_width:
                    file_name = file_name[:filename_width - 3] + "..."
                file_name = file_name.ljust(filename_width)
                desc = f"{resume_icon} {file_name}"

                show_progress = task.__dict__.get('show_progress', True)
                use_tqdm = task.progress_callback is None and show_progress

                with open(task.temp_path, mode) as f:
                    if use_tqdm:
                        # 使用 tqdm 进度条
                        tqdm_total = total_size if total_size > 0 else None
                        with tqdm(
                            total=tqdm_total,
                            initial=downloaded_size,
                            unit="B",
                            unit_scale=True,
                            unit_divisor=1024,
                            desc=desc,
                            position=self._get_position(),
                            bar_format='{desc:25} │ {bar:25} {percentage:6.2f}% │ {n_fmt:>6}/{total_fmt:<6}({rate_fmt:>5})  │ {elapsed:>5} / {remaining:>5}',
                            ncols=155,
                            colour='green',
                            leave=True,  # 进度条是否保留
                            ascii=" ░▒▓█",
                        ) as pbar:
                            for chunk in response.iter_content(chunk_size=self.chunk_size):
                                if chunk:
                                    f.write(chunk)
                                    pbar.update(len(chunk))
                                    meta.downloaded_size += len(chunk)
                    else:
                        # 使用自定义回调或无进度显示
                        start_time = time.time()
                        last_update = start_time

                        for chunk in response.iter_content(chunk_size=self.chunk_size):
                            if chunk:
                                f.write(chunk)
                                meta.downloaded_size += len(chunk)

                                # 更新进度
                                if task.progress_callback:
                                    current_time = time.time()
                                    if (current_time - last_update) >= 0.5:
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
                            task.progress_callback(
                                meta.total_size, meta.total_size, speed)

                    # 确保数据完全写入磁盘
                    f.flush()
                    os.fsync(f.fileno())

                # 安全重命名文件
                temp_path = pathlib.Path(task.temp_path)
                full_path = pathlib.Path(task.full_path)

                if self._safe_rename_file(temp_path, full_path):
                    logger.info(f"下载完成: {task.full_path}")
                    # 清理元数据
                    meta.cleanup()
                    return True

                # 重命名失败，检查最终文件是否已存在（可能是竞态条件）
                if full_path.exists() and full_path.stat().st_size > 0:
                    logger.info(f"下载完成（文件已存在）: {task.full_path}")
                    meta.cleanup()
                    return True

                logger.error(f"文件重命名失败: {task.temp_path} -> {task.full_path}")
                return False

            except Exception as e:
                logger.error(f"下载异常 (尝试 {attempt + 1}/{max_retries}): {e}")

                if attempt < max_retries - 1:
                    # 还有重试机会，保存元数据
                    if task.resume:
                        meta.save()
                    time.sleep(2 ** attempt)
                    continue
                else:
                    # 最后一次尝试失败
                    if supports_resume is False:
                        logger.info("不支持断点续传，删除临时文件")
                        if os.path.exists(task.temp_path):
                            try:
                                os.remove(task.temp_path)
                            except OSError:
                                pass
                    else:
                        logger.info("保留临时文件供下次续传")
                        if task.resume:
                            meta.save()
                    return False

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

    def download_from_csv(
        self,
        csv_file: str,
        save_path: str,
        url_column: str = "url",
        filename_column: Optional[str] = None,
        media_id_column: Optional[str] = None,
        max_workers: int = 3,
        max_tasks: Optional[int] = None,
        skip_existing: bool = True,
        resume: bool = True,
        show_progress: bool = True,
        stop_on_error: bool = False,
        filter_row: Optional[Callable[[Dict[str, Any], int], bool]] = None,
    ) -> Dict[str, Any]:
        """
        从 CSV 文件批量下载

        :param csv_file: CSV 文件路径
        :param save_path: 保存目录
        :param url_column: URL 列名
        :param filename_column: 文件名列名（可选）
        :param media_id_column: 媒体 ID 列名（可选）
        :param max_workers: 最大并发数
        :param max_tasks: 最大任务数（None 表示不限制）
        :param skip_existing: 是否跳过已存在的文件
        :param resume: 是否支持断点续传
        :param show_progress: 是否显示进度条
        :param stop_on_error: 遇到错误是否停止
        :param filter_row: 行过滤回调 (row, row_number) -> bool，返回 True 表示处理该行
        :return: 统计信息字典
        """
        csv_path = pathlib.Path(csv_file)
        if not csv_path.exists():
            logger.error(f"CSV 文件不存在: {csv_file}")
            return {"error": "CSV file not found"}

        logger.info(f"开始从 CSV 导入下载任务: {csv_file}")
        logger.info(f"保存目录: {save_path}")
        logger.info(f"并发数: {max_workers}")

        processed_count = 0
        success_count = 0
        failed_count = 0
        skipped_count = 0

        # 创建保存目录
        pathlib.Path(save_path).mkdir(parents=True, exist_ok=True)

        def _process_row(row: Dict[str, Any], row_number: int) -> Optional[bool]:
            """处理单行数据，返回 None 表示跳过，True/False 表示成功/失败"""
            nonlocal processed_count, success_count, failed_count, skipped_count

            try:
                # 应用行预处理
                if self.prepare_row:
                    try:
                        row = self.prepare_row(row)
                    except Exception as e:
                        logger.warning(f"第 {row_number} 行 prepare_row 失败: {e}")
                        return False

                # 应用行过滤
                if filter_row:
                    try:
                        if not filter_row(row, row_number):
                            logger.debug(f"第 {row_number} 行被过滤")
                            return None
                    except Exception as e:
                        logger.warning(f"第 {row_number} 行 filter_row 失败: {e}")
                        return False

                # 提取必要信息
                url = row.get(url_column)
                if not url:
                    logger.warning(f"第 {row_number} 行缺少 URL 列: {url_column}")
                    return None

                filename = row.get(
                    filename_column) if filename_column else None
                media_id = row.get(
                    media_id_column) if media_id_column else str(row_number)

                # 创建请求对象
                request = Request(url=url)

                # 下载文件
                result = self.download(
                    request=request,
                    save_path=save_path,
                    filename=filename,
                    resume=resume,
                    skip_existing=skip_existing,
                    show_progress=show_progress,
                    media_id=media_id,
                    row_data=row,
                )

                return result

            except Exception as e:
                logger.error(f"第 {row_number} 行处理异常: {e}")
                return False

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []

            try:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)

                    for row_number, row in enumerate(reader, start=1):
                        if max_tasks and processed_count >= max_tasks:
                            logger.info(f"已达到最大任务数限制: {max_tasks}")
                            break

                        processed_count += 1

                        # 提交任务
                        future = executor.submit(_process_row, row, row_number)
                        futures.append(future)

                # 等待所有任务完成
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result is None:
                            skipped_count += 1
                        elif result:
                            success_count += 1
                        else:
                            failed_count += 1
                            if stop_on_error:
                                logger.error("遇到错误，停止批量下载")
                                executor.shutdown(
                                    wait=False, cancel_futures=True)
                                break
                    except Exception as e:
                        logger.error(f"任务执行异常: {e}")
                        failed_count += 1
                        if stop_on_error:
                            executor.shutdown(wait=False, cancel_futures=True)
                            break

            except Exception as e:
                logger.error(f"读取 CSV 文件失败: {e}")
                return {"error": str(e)}

        # 统计信息
        stats = {
            "total": processed_count,
            "success": success_count,
            "failed": failed_count,
            "skipped": skipped_count,
        }

        logger.info("=" * 60)
        logger.info("下载统计：")
        logger.info(f"  总计: {stats['total']}")
        logger.info(f"  成功: {stats['success']}")
        logger.info(f"  失败: {stats['failed']}")
        logger.info(f"  跳过: {stats['skipped']}")
        logger.info("=" * 60)

        return stats
