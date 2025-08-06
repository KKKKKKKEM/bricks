#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自定义下载器示例

这个示例展示了如何基于 bricks 框架创建自定义下载器，
包括继承 AbstractDownloader 类和实现特定功能。
"""

import time
import random
import json
from typing import Union, Optional
from bricks import Request, Response
from bricks.downloader import AbstractDownloader
from bricks.lib.headers import Header
from bricks.lib.cookies import Cookies


class LoggingDownloader(AbstractDownloader):
    """
    带日志记录功能的下载器
    继承自 AbstractDownloader，添加详细的请求日志
    """
    
    def __init__(self, base_downloader=None, log_file: Optional[str] = None):
        """
        初始化日志下载器
        
        :param base_downloader: 基础下载器实例
        :param log_file: 日志文件路径
        """
        from bricks.downloader.requests_ import Downloader as RequestsDownloader
        
        self.base_downloader = base_downloader or RequestsDownloader()
        self.log_file = log_file
        self.request_count = 0
    
    def fetch(self, request: Union[Request, dict]) -> Response:
        """
        发送请求并记录日志
        """
        if isinstance(request, dict):
            request = Request(**request)
        
        self.request_count += 1
        start_time = time.time()
        
        # 记录请求开始
        self._log_request_start(request)
        
        try:
            # 使用基础下载器发送请求
            response = self.base_downloader.fetch(request)
            end_time = time.time()
            
            # 记录请求完成
            self._log_request_complete(request, response, end_time - start_time)
            
            return response
            
        except Exception as e:
            end_time = time.time()
            self._log_request_error(request, e, end_time - start_time)
            raise
    
    def _log_request_start(self, request: Request):
        """记录请求开始"""
        log_entry = {
            "timestamp": time.time(),
            "request_id": self.request_count,
            "event": "request_start",
            "method": request.method,
            "url": request.real_url,
            "headers": dict(request.headers),
            "cookies": request.cookies,
            "body_size": len(str(request.body)) if request.body else 0
        }
        self._write_log(log_entry)
    
    def _log_request_complete(self, request: Request, response: Response, duration: float):
        """记录请求完成"""
        log_entry = {
            "timestamp": time.time(),
            "request_id": self.request_count,
            "event": "request_complete",
            "status_code": response.status_code,
            "response_size": len(response.content) if response.content else 0,
            "duration": duration,
            "success": response.ok
        }
        self._write_log(log_entry)
    
    def _log_request_error(self, request: Request, error: Exception, duration: float):
        """记录请求错误"""
        log_entry = {
            "timestamp": time.time(),
            "request_id": self.request_count,
            "event": "request_error",
            "error_type": error.__class__.__name__,
            "error_message": str(error),
            "duration": duration
        }
        self._write_log(log_entry)
    
    def _write_log(self, log_entry: dict):
        """写入日志"""
        log_line = json.dumps(log_entry, ensure_ascii=False)
        print(f"[LOG] {log_line}")
        
        if self.log_file:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(log_line + "\n")


class RateLimitDownloader(AbstractDownloader):
    """
    带速率限制的下载器
    控制请求频率，避免对目标服务器造成过大压力
    """
    
    def __init__(self, base_downloader=None, requests_per_second: float = 1.0):
        """
        初始化速率限制下载器
        
        :param base_downloader: 基础下载器实例
        :param requests_per_second: 每秒允许的请求数
        """
        from bricks.downloader.requests_ import Downloader as RequestsDownloader
        
        self.base_downloader = base_downloader or RequestsDownloader()
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0
    
    def fetch(self, request: Union[Request, dict]) -> Response:
        """
        发送请求，遵守速率限制
        """
        if isinstance(request, dict):
            request = Request(**request)
        
        # 计算需要等待的时间
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_interval:
            wait_time = self.min_interval - time_since_last
            print(f"[RATE LIMIT] 等待 {wait_time:.2f} 秒...")
            time.sleep(wait_time)
        
        # 更新最后请求时间
        self.last_request_time = time.time()
        
        # 发送请求
        return self.base_downloader.fetch(request)


class RetryDownloader(AbstractDownloader):
    """
    带智能重试功能的下载器
    根据不同的错误类型采用不同的重试策略
    """
    
    def __init__(self, base_downloader=None, max_retries: int = 3, backoff_factor: float = 1.0):
        """
        初始化重试下载器
        
        :param base_downloader: 基础下载器实例
        :param max_retries: 最大重试次数
        :param backoff_factor: 退避因子
        """
        from bricks.downloader.requests_ import Downloader as RequestsDownloader
        
        self.base_downloader = base_downloader or RequestsDownloader()
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
    
    def fetch(self, request: Union[Request, dict]) -> Response:
        """
        发送请求，支持智能重试
        """
        if isinstance(request, dict):
            request = Request(**request)
        
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                response = self.base_downloader.fetch(request)
                
                # 检查是否需要重试（基于状态码）
                if self._should_retry(response, attempt):
                    if attempt < self.max_retries:
                        wait_time = self._calculate_wait_time(attempt)
                        print(f"[RETRY] 第 {attempt + 1} 次重试，等待 {wait_time:.2f} 秒...")
                        time.sleep(wait_time)
                        continue
                
                return response
                
            except Exception as e:
                last_exception = e
                
                if attempt < self.max_retries and self._should_retry_exception(e):
                    wait_time = self._calculate_wait_time(attempt)
                    print(f"[RETRY] 异常重试第 {attempt + 1} 次，等待 {wait_time:.2f} 秒...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise
        
        # 如果所有重试都失败了
        if last_exception:
            raise last_exception
    
    def _should_retry(self, response: Response, attempt: int) -> bool:
        """判断是否应该重试（基于响应）"""
        # 5xx 服务器错误和 429 限流错误需要重试
        return response.status_code in [429, 500, 502, 503, 504] and attempt < self.max_retries
    
    def _should_retry_exception(self, exception: Exception) -> bool:
        """判断是否应该重试（基于异常）"""
        # 网络相关异常需要重试
        retry_exceptions = ["ConnectionError", "Timeout", "ReadTimeout", "ConnectTimeout"]
        return any(exc_type in str(type(exception)) for exc_type in retry_exceptions)
    
    def _calculate_wait_time(self, attempt: int) -> float:
        """计算等待时间（指数退避）"""
        base_wait = self.backoff_factor * (2 ** attempt)
        # 添加随机抖动
        jitter = random.uniform(0, 0.1 * base_wait)
        return base_wait + jitter


class CacheDownloader(AbstractDownloader):
    """
    带缓存功能的下载器
    缓存响应内容，避免重复请求
    """
    
    def __init__(self, base_downloader=None, cache_ttl: int = 300):
        """
        初始化缓存下载器
        
        :param base_downloader: 基础下载器实例
        :param cache_ttl: 缓存生存时间（秒）
        """
        from bricks.downloader.requests_ import Downloader as RequestsDownloader
        
        self.base_downloader = base_downloader or RequestsDownloader()
        self.cache = {}
        self.cache_ttl = cache_ttl
    
    def fetch(self, request: Union[Request, dict]) -> Response:
        """
        发送请求，支持缓存
        """
        if isinstance(request, dict):
            request = Request(**request)
        
        # 生成缓存键
        cache_key = self._generate_cache_key(request)
        
        # 检查缓存
        if cache_key in self.cache:
            cached_response, timestamp = self.cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                print(f"[CACHE] 命中缓存: {request.real_url}")
                return cached_response
            else:
                # 缓存过期，删除
                del self.cache[cache_key]
        
        # 发送请求
        response = self.base_downloader.fetch(request)
        
        # 缓存响应（只缓存成功的响应）
        if response.ok:
            self.cache[cache_key] = (response, time.time())
            print(f"[CACHE] 缓存响应: {request.real_url}")
        
        return response
    
    def _generate_cache_key(self, request: Request) -> str:
        """生成缓存键"""
        # 基于 URL、方法和主要参数生成键
        key_parts = [
            request.method,
            request.real_url,
            str(sorted(request.headers.items())) if request.headers else "",
            str(request.body) if request.body else ""
        ]
        return "|".join(key_parts)
    
    def clear_cache(self):
        """清空缓存"""
        self.cache.clear()
        print("[CACHE] 缓存已清空")


class ProxyRotationDownloader(AbstractDownloader):
    """
    代理轮换下载器
    自动轮换代理IP，提高请求成功率
    """
    
    def __init__(self, base_downloader=None, proxy_list: list = None):
        """
        初始化代理轮换下载器
        
        :param base_downloader: 基础下载器实例
        :param proxy_list: 代理列表
        """
        from bricks.downloader.requests_ import Downloader as RequestsDownloader
        
        self.base_downloader = base_downloader or RequestsDownloader()
        self.proxy_list = proxy_list or [
            "http://127.0.0.1:7890",
            "http://127.0.0.1:7891",
            "http://127.0.0.1:7892"
        ]
        self.current_proxy_index = 0
        self.proxy_failures = {}  # 记录代理失败次数
    
    def fetch(self, request: Union[Request, dict]) -> Response:
        """
        发送请求，自动轮换代理
        """
        if isinstance(request, dict):
            request = Request(**request)
        
        # 选择可用的代理
        proxy = self._get_next_proxy()
        if proxy:
            request.proxies = proxy
            print(f"[PROXY] 使用代理: {proxy}")
        
        try:
            response = self.base_downloader.fetch(request)
            
            # 请求成功，重置该代理的失败计数
            if proxy and proxy in self.proxy_failures:
                self.proxy_failures[proxy] = 0
            
            return response
            
        except Exception as e:
            # 记录代理失败
            if proxy:
                self.proxy_failures[proxy] = self.proxy_failures.get(proxy, 0) + 1
                print(f"[PROXY] 代理失败: {proxy}, 失败次数: {self.proxy_failures[proxy]}")
            
            raise
    
    def _get_next_proxy(self) -> Optional[str]:
        """获取下一个可用代理"""
        if not self.proxy_list:
            return None
        
        # 过滤掉失败次数过多的代理
        available_proxies = [
            proxy for proxy in self.proxy_list
            if self.proxy_failures.get(proxy, 0) < 3
        ]
        
        if not available_proxies:
            # 所有代理都失败了，重置失败计数
            self.proxy_failures.clear()
            available_proxies = self.proxy_list
        
        # 轮换选择代理
        proxy = available_proxies[self.current_proxy_index % len(available_proxies)]
        self.current_proxy_index += 1
        
        return proxy


def demo_custom_downloaders():
    """演示自定义下载器的使用"""
    print("=== 自定义下载器演示 ===")
    
    # 1. 日志下载器
    print("\n1. 日志下载器演示:")
    logging_downloader = LoggingDownloader(log_file="requests.log")
    request = Request(url="https://httpbin.org/get")
    response = logging_downloader.fetch(request)
    print(f"响应状态码: {response.status_code}")
    
    # 2. 速率限制下载器
    print("\n2. 速率限制下载器演示:")
    rate_limit_downloader = RateLimitDownloader(requests_per_second=0.5)  # 每2秒一个请求
    
    for i in range(3):
        start_time = time.time()
        response = rate_limit_downloader.fetch(Request(url="https://httpbin.org/get"))
        end_time = time.time()
        print(f"请求 {i+1} 完成，耗时: {end_time - start_time:.2f}秒")
    
    # 3. 重试下载器
    print("\n3. 重试下载器演示:")
    retry_downloader = RetryDownloader(max_retries=2)
    
    # 模拟可能失败的请求
    try:
        response = retry_downloader.fetch(Request(url="https://httpbin.org/status/500"))
    except Exception as e:
        print(f"重试后仍然失败: {e}")
    
    # 4. 缓存下载器
    print("\n4. 缓存下载器演示:")
    cache_downloader = CacheDownloader(cache_ttl=60)
    
    # 第一次请求
    start_time = time.time()
    response1 = cache_downloader.fetch(Request(url="https://httpbin.org/delay/1"))
    time1 = time.time() - start_time
    print(f"第一次请求耗时: {time1:.2f}秒")
    
    # 第二次请求（应该命中缓存）
    start_time = time.time()
    response2 = cache_downloader.fetch(Request(url="https://httpbin.org/delay/1"))
    time2 = time.time() - start_time
    print(f"第二次请求耗时: {time2:.2f}秒")
    
    # 5. 代理轮换下载器
    print("\n5. 代理轮换下载器演示:")
    proxy_downloader = ProxyRotationDownloader(
        proxy_list=["http://proxy1:8080", "http://proxy2:8080"]  # 示例代理
    )
    
    try:
        response = proxy_downloader.fetch(Request(url="https://httpbin.org/ip"))
        print(f"代理请求成功: {response.status_code}")
    except Exception as e:
        print(f"代理请求失败（预期的，因为代理不存在）: {e}")


def create_composite_downloader():
    """创建组合下载器示例"""
    print("\n=== 组合下载器演示 ===")
    
    # 创建一个组合了多种功能的下载器
    from bricks.downloader.requests_ import Downloader as RequestsDownloader
    
    base = RequestsDownloader()
    
    # 层层包装
    cached = CacheDownloader(base, cache_ttl=300)
    rate_limited = RateLimitDownloader(cached, requests_per_second=2.0)
    with_retry = RetryDownloader(rate_limited, max_retries=2)
    logged = LoggingDownloader(with_retry)
    
    print("创建了组合下载器：日志 -> 重试 -> 速率限制 -> 缓存 -> 基础下载器")
    
    # 使用组合下载器
    request = Request(url="https://httpbin.org/get")
    response = logged.fetch(request)
    print(f"组合下载器响应: {response.status_code}")


if __name__ == "__main__":
    """运行自定义下载器示例"""
    print("Bricks 自定义下载器示例")
    print("=" * 50)
    
    try:
        demo_custom_downloaders()
        create_composite_downloader()
        
        print("\n" + "=" * 50)
        print("自定义下载器示例运行完成！")
        print("\n自定义下载器的优势:")
        print("- 可以添加特定的业务逻辑")
        print("- 支持功能组合和扩展")
        print("- 便于调试和监控")
        print("- 提高代码复用性")
        
    except Exception as e:
        print(f"运行示例时出错: {e}")
        import traceback
        traceback.print_exc()
