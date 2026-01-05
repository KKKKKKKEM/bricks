# -*- coding: utf-8 -*-
# @Time    : 2025-12-12
# @Author  : Kem
# @Desc    : 媒体文件下载器 - 支持断点续传、流式下载、进度显示
from __future__ import annotations

import copy
import csv
import json
import os
import pathlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Union

from loguru import logger

from bricks.downloader import AbstractDownloader
from bricks.downloader.cffi import Downloader as CffiDownloader
from bricks.lib.request import Request
from bricks.utils import pandora

pandora.require("tqdm")
from tqdm import tqdm  # noqa: E402


@dataclass(frozen=True)
class DownloadProgress:
    downloaded: int
    total: Optional[int]
    speed_mbps: float
    elapsed: float


@dataclass(frozen=True)
class DownloadResult:
    ok: bool
    url: str
    path: pathlib.Path
    status_code: Optional[int] = None
    skipped: bool = False
    resumed: bool = False
    bytes_downloaded: int = 0
    total_bytes: Optional[int] = None
    attempts: int = 1
    error: Optional[str] = None


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 3
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 30.0

    def backoff_seconds(self, attempt_index: int) -> float:
        # attempt_index: 0,1,2...
        seconds = self.backoff_base_seconds * (2 ** attempt_index)
        return min(self.backoff_max_seconds, seconds)


@dataclass
class DownloadTask:
    """
    一个下载任务（可用于单个下载 / 批量下载 / CSV 导入）。

    设计目标：对用户来说是“最少字段 + 清晰语义”，对实现来说是“所有状态都能推导”。
    """

    request: Request
    save_dir: pathlib.Path
    filename: Optional[str] = None

    resume: bool = True
    skip_existing: bool = True
    verify_remote: bool = True

    show_progress: bool = True
    progress_callback: Optional[Callable[[DownloadProgress], None]] = None

    media_id: str = ""
    row_data: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.save_dir, pathlib.Path):
            self.save_dir = pathlib.Path(self.save_dir)
        if not self.filename:
            self.filename = _default_filename_from_url(self.request.real_url)

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(self.save_dir) / (self.filename or "")

    @property
    def temp_path(self) -> pathlib.Path:
        return pathlib.Path(f"{self.path}.part")

    @property
    def meta_path(self) -> pathlib.Path:
        return pathlib.Path(f"{self.path}.meta")

    @classmethod
    def from_url(
        cls,
        url: str,
        save_dir: Union[str, pathlib.Path],
        filename: Optional[str] = None,
        *,
        headers: Optional[Dict[str, str]] = None,
        cookies: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        proxies: Optional[str] = None,
        **kwargs: Any,
    ) -> "DownloadTask":
        request = Request(
            url=url,
            headers=headers or {},
            cookies=cookies, # type: ignore
            params=params,
            proxies=proxies,
        )
        return cls(request=request, save_dir=pathlib.Path(save_dir), filename=filename, **kwargs)


@dataclass(frozen=True)
class BatchReport:
    planned: int
    results: List[DownloadResult]

    @property
    def completed(self) -> int:
        return len(self.results)

    @property
    def success(self) -> int:
        return sum(1 for r in self.results if r.ok and not r.skipped)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if not r.ok)

    @property
    def skipped(self) -> int:
        return sum(1 for r in self.results if r.skipped)


@dataclass(frozen=True)
class Hooks:
    """
    可选钩子：保持扩展能力，但不把 MediaDownloader.__init__ 参数炸开。
    """

    prepare_task: Optional[Callable[[DownloadTask], DownloadTask]] = None
    prepare_request: Optional[Callable[[Request, DownloadTask], Request]] = None
    resolve_path: Optional[Callable[[DownloadTask], pathlib.Path]] = None
    on_result: Optional[Callable[[DownloadResult, DownloadTask], None]] = None


@dataclass
class _DownloadMeta:
    total_size: Optional[int] = None
    downloaded_size: int = 0
    etag: Optional[str] = None
    last_modified: Optional[str] = None

    @classmethod
    def load(cls, path: pathlib.Path) -> Optional["_DownloadMeta"]:
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return cls(
                total_size=data.get("total_size"),
                downloaded_size=int(data.get("downloaded_size", 0) or 0),
                etag=data.get("etag"),
                last_modified=data.get("last_modified"),
            )
        except Exception as e:
            logger.warning(f"加载元数据失败: {e}")
            return None

    def save(self, path: pathlib.Path) -> None:
        try:
            tmp = pathlib.Path(f"{path}.tmp")
            tmp.write_text(
                json.dumps(
                    {
                        "total_size": self.total_size,
                        "downloaded_size": self.downloaded_size,
                        "etag": self.etag,
                        "last_modified": self.last_modified,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            tmp.replace(path)
        except Exception as e:
            logger.warning(f"保存元数据失败: {e}")


class _TqdmPositions:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._positions: Dict[int, int] = {}
        self._next = 0

    def get(self) -> int:
        tid = threading.get_ident()
        with self._lock:
            if tid not in self._positions:
                self._positions[tid] = self._next
                self._next += 1
            return self._positions[tid]


class MediaDownloader:
    """
    目标：提供一个“好用、可组合、低副作用”的下载器。

    关键点：
    - API 统一：单个下载 / 批量下载 / CSV 下载都围绕 DownloadTask 与 DownloadResult
    - 进度统一：progress_callback 收到 DownloadProgress；未提供回调则可用 tqdm（可选依赖）
    - 断点续传：使用 .part + .meta；支持 HEAD 校验（etag/last-modified/length）与 206/416 分支
    """

    def __init__(
        self,
        downloader: Optional[AbstractDownloader] = None,
        *,
        chunk_size: int = 8192,
        retry: Optional[RetryPolicy] = None,
        hooks: Optional[Hooks] = None,
    ) -> None:
        self.downloader = downloader or CffiDownloader()
        self.chunk_size = chunk_size
        self.retry = retry or RetryPolicy()
        self.hooks = hooks or Hooks()
        self._tqdm_positions = _TqdmPositions()

    def download(
        self,
        task: DownloadTask,
    ) -> DownloadResult:
        if self.hooks.prepare_task:
            task = self.hooks.prepare_task(task)

        out_path = self.hooks.resolve_path(task) if self.hooks.resolve_path else task.path
        if out_path != task.path:
            task = copy.deepcopy(task)
            task.save_dir = out_path.parent
            task.filename = out_path.name
        task.save_dir.mkdir(parents=True, exist_ok=True)

        if task.skip_existing and task.path.exists():
            result = DownloadResult(
                ok=True,
                url=task.request.real_url,
                path=task.path,
                skipped=True,
                bytes_downloaded=task.path.stat().st_size,
            )
            if self.hooks.on_result:
                self.hooks.on_result(result, task)
            return result

        if not task.resume:
            _safe_unlink(task.temp_path)
            _safe_unlink(task.meta_path)

        meta = _DownloadMeta.load(task.meta_path) if task.resume else None
        if meta and task.temp_path.exists():
            try:
                meta.downloaded_size = task.temp_path.stat().st_size
            except OSError:
                meta.downloaded_size = 0

        # 可选：HEAD 校验，避免把旧 .part 续到新文件上
        if task.resume and task.verify_remote and meta and (meta.etag or meta.last_modified or meta.total_size):
            head = self._head(task.request)
            if head is not None and not _remote_matches_meta(head, meta):
                logger.warning("远程文件已更改，清理断点续传缓存并重新下载")
                _safe_unlink(task.temp_path)
                _safe_unlink(task.meta_path)
                meta = None

        result = self._download_with_retries(task, meta)
        if self.hooks.on_result:
            self.hooks.on_result(result, task)
        return result

    def download_url(
        self,
        url: str,
        save_dir: Union[str, pathlib.Path],
        filename: Optional[str] = None,
        **kwargs: Any,
    ) -> DownloadResult:
        return self.download(DownloadTask.from_url(url, save_dir, filename, **kwargs))

    def download_many(
        self,
        tasks: Sequence[DownloadTask],
        *,
        concurrency: int = 3,
        stop_on_error: bool = False,
    ) -> BatchReport:
        results: List[DownloadResult] = []
        if not tasks:
            return BatchReport(planned=0, results=results)

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            future_to_task = {executor.submit(self.download, t): t for t in tasks}
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    res = future.result()
                except Exception as e:
                    res = DownloadResult(
                        ok=False,
                        url=task.request.real_url,
                        path=task.path,
                        error=str(e),
                    )
                    if self.hooks.on_result:
                        self.hooks.on_result(res, task)
                results.append(res)
                if stop_on_error and not res.ok:
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
        return BatchReport(planned=len(tasks), results=results)

    def download_from_csv(
        self,
        csv_file: Union[str, pathlib.Path],
        *,
        save_dir: Union[str, pathlib.Path],
        url_column: str = "url",
        filename_column: Optional[str] = None,
        concurrency: int = 3,
        max_tasks: Optional[int] = None,
        resume: bool = True,
        skip_existing: bool = True,
        show_progress: bool = True,
        stop_on_error: bool = False,
        prepare_row: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
        filter_row: Optional[Callable[[Dict[str, Any], int], bool]] = None,
    ) -> BatchReport:
        csv_path = pathlib.Path(csv_file)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        tasks: List[DownloadTask] = []
        with csv_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row_number, row in enumerate(reader, start=1):
                if max_tasks is not None and len(tasks) >= max_tasks:
                    break
                if prepare_row:
                    row = prepare_row(row)
                if filter_row and not filter_row(row, row_number):
                    continue
                url = row.get(url_column)
                if not url:
                    continue
                filename = row.get(filename_column) if filename_column else None
                task = DownloadTask.from_url(
                    url=str(url),
                    save_dir=save_dir,
                    filename=filename,
                    resume=resume,
                    skip_existing=skip_existing,
                    show_progress=show_progress,
                    row_data=row,
                    media_id=str(row_number),
                )
                tasks.append(task)

        return self.download_many(tasks, concurrency=concurrency, stop_on_error=stop_on_error)

    def _download_with_retries(self, task: DownloadTask, meta: Optional[_DownloadMeta]) -> DownloadResult:
        last_error: Optional[str] = None
        last_status: Optional[int] = None

        for attempt in range(self.retry.attempts):
            try:
                if attempt > 0:
                    time.sleep(self.retry.backoff_seconds(attempt - 1))

                return self._download_once(task, meta, attempt=attempt + 1)

            except Exception as e:
                last_error = str(e) or e.__class__.__name__
                last_status = None
                if task.resume:
                    meta_to_save = meta or _DownloadMeta()
                    meta_to_save.downloaded_size = task.temp_path.stat().st_size if task.temp_path.exists() else 0
                    meta_to_save.save(task.meta_path)

        return DownloadResult(
            ok=False,
            url=task.request.real_url,
            path=task.path,
            status_code=last_status,
            attempts=self.retry.attempts,
            error=last_error or "下载失败",
        )

    def _download_once(self, task: DownloadTask, meta: Optional[_DownloadMeta], *, attempt: int) -> DownloadResult:
        resumed = False
        downloaded = task.temp_path.stat().st_size if task.temp_path.exists() else 0

        headers = dict(task.request.headers or {})
        requested_range = False
        if task.resume and downloaded > 0:
            headers["Range"] = f"bytes={downloaded}-"
            requested_range = True

        request = copy.deepcopy(task.request)
        request.method = "GET"
        request.headers.update(headers)
        request.put_options("stream", True)
        request.put_options("chunk_size", self.chunk_size)
        if self.hooks.prepare_request:
            request = self.hooks.prepare_request(request, task)

        response = self.downloader.fetch(request)
        status_code = getattr(response, "status_code", None)

        # 429：尊重 Retry-After
        if status_code == 429:
            retry_after = response.headers.get("Retry-After")
            if retry_after and str(retry_after).isdigit():
                time.sleep(int(retry_after))
            raise RuntimeError("请求被限流 (429)")

        # Range 不可满足：可能 .part 已完整 or 服务器不接受该 Range
        if requested_range and status_code == 416 and downloaded > 0:
            total = _parse_total_from_content_range(response.headers.get("Content-Range"))
            if total is not None and downloaded >= total:
                if _atomic_finalize(task.temp_path, task.path):
                    _safe_unlink(task.meta_path)
                    return DownloadResult(
                        ok=True,
                        url=task.request.real_url,
                        path=task.path,
                        status_code=status_code,
                        resumed=True,
                        bytes_downloaded=downloaded,
                        total_bytes=total,
                        attempts=attempt,
                    )
            _safe_unlink(task.temp_path)
            _safe_unlink(task.meta_path)
            raise RuntimeError("Range 请求无效 (416)，已清理缓存并将重试全量下载")

        # 服务器不支持断点续传：清理 .part 重新下载
        if requested_range and status_code == 200 and downloaded > 0:
            _safe_unlink(task.temp_path)
            _safe_unlink(task.meta_path)
            downloaded = 0
            resumed = False
        elif status_code == 206 and downloaded > 0:
            resumed = True

        if status_code not in (200, 206):
            raise RuntimeError(f"下载失败: status_code={status_code}")

        total = _infer_total_bytes(response.headers, downloaded, status_code)

        meta_to_save = meta or _DownloadMeta()
        meta_to_save.total_size = total
        meta_to_save.etag = response.headers.get("ETag")
        meta_to_save.last_modified = response.headers.get("Last-Modified")

        started_at = time.time()
        last_report_at = started_at

        temp_mode = "ab" if (task.resume and downloaded > 0 and task.temp_path.exists()) else "wb"
        task.temp_path.parent.mkdir(parents=True, exist_ok=True)

        pbar = None
        if task.progress_callback is None and task.show_progress and tqdm is not None:
            pbar = tqdm(
                total=total,
                initial=downloaded,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=task.filename or "download",
                position=self._tqdm_positions.get(),
                leave=True,
            )

        try:
            with task.temp_path.open(temp_mode) as f:
                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    if not chunk:
                        continue
                    f.write(chunk)
                    downloaded += len(chunk)
                    meta_to_save.downloaded_size = downloaded

                    if pbar is not None:
                        pbar.update(len(chunk))

                    now = time.time()
                    if task.progress_callback and (now - last_report_at) >= 0.5:
                        elapsed = now - started_at
                        speed = downloaded / elapsed / (1024 * 1024) if elapsed > 0 else 0.0
                        task.progress_callback(
                            DownloadProgress(
                                downloaded=downloaded,
                                total=total,
                                speed_mbps=speed,
                                elapsed=elapsed,
                            )
                        )
                        last_report_at = now

                f.flush()
                os.fsync(f.fileno())

            # 最终回调
            if task.progress_callback:
                elapsed = time.time() - started_at
                speed = downloaded / elapsed / (1024 * 1024) if elapsed > 0 else 0.0
                task.progress_callback(
                    DownloadProgress(downloaded=downloaded, total=total, speed_mbps=speed, elapsed=elapsed)
                )

        except Exception:
            if task.resume:
                meta_to_save.save(task.meta_path)
            raise

        finally:
            if pbar is not None:
                try:
                    pbar.close()
                except Exception:
                    pass

        if not _atomic_finalize(task.temp_path, task.path):
            raise RuntimeError(f"文件落盘失败: {task.temp_path} -> {task.path}")

        _safe_unlink(task.meta_path)
        return DownloadResult(
            ok=True,
            url=task.request.real_url,
            path=task.path,
            status_code=status_code,
            resumed=resumed,
            bytes_downloaded=downloaded,
            total_bytes=total,
            attempts=attempt,
        )

    def _head(self, request: Request) -> Optional[Dict[str, Any]]:
        try:
            head_req = copy.deepcopy(request)
            head_req.method = "HEAD"
            response = self.downloader.fetch(head_req)
            if response.status_code not in (200, 206):
                return None
            return {
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "content_length": _safe_int(response.headers.get("Content-Length")),
            }
        except Exception:
            return None


def _default_filename_from_url(url: str) -> str:
    try:
        path = url.split("?", 1)[0]
        name = os.path.basename(path)
        if name:
            return name
    except Exception:
        pass
    return f"file_{int(time.time())}"


def _safe_unlink(path: pathlib.Path) -> None:
    try:
        if path.exists():
            path.unlink()
    except OSError:
        pass


def _atomic_finalize(temp_path: pathlib.Path, final_path: pathlib.Path) -> bool:
    try:
        # 覆盖式落盘（若 final 已存在，交由上层 skip_existing 控制）
        temp_path.replace(final_path)
        return True
    except FileNotFoundError:
        return False
    except OSError:
        # 竞态：final 可能已存在
        try:
            return final_path.exists() and final_path.stat().st_size > 0
        except OSError:
            return False


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _parse_total_from_content_range(content_range: Optional[str]) -> Optional[int]:
    if not content_range:
        return None
    # bytes 0-1023/2048  or bytes */2048
    try:
        if "/" not in content_range:
            return None
        total_part = content_range.split("/", 1)[1].strip()
        if not total_part or total_part == "*":
            return None
        return int(total_part)
    except Exception:
        return None


def _infer_total_bytes(headers: Mapping[str, Any], downloaded: int, status_code: Optional[int]) -> Optional[int]:
    if status_code == 206:
        total = _parse_total_from_content_range(headers.get("Content-Range"))
        if total is not None:
            return total
        length = _safe_int(headers.get("Content-Length"))
        return (length + downloaded) if length is not None else None
    length = _safe_int(headers.get("Content-Length"))
    return length


def _remote_matches_meta(head: Dict[str, Any], meta: _DownloadMeta) -> bool:
    if meta.etag and head.get("etag"):
        return meta.etag == head.get("etag")
    if meta.last_modified and head.get("last_modified"):
        return meta.last_modified == head.get("last_modified")
    if meta.total_size is not None and head.get("content_length") is not None:
        return meta.total_size == head.get("content_length")
    return False
