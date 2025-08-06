#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高级下载器使用示例

这个示例展示了 bricks 框架中下载器的高级功能，包括代理、重试、超时等。
"""

import time

from bricks import Request
from bricks.downloader.requests_ import Downloader as RequestsDownloader


def proxy_usage():
    """代理使用示例"""
    print("=== 代理使用示例 ===")

    downloader = RequestsDownloader()

    # 使用代理的请求（注意：这里使用的是示例代理，实际使用时需要替换为真实代理）
    request = Request(
        url="https://httpbin.org/ip",
        proxies="http://127.0.0.1:7890",  # 示例代理地址
        timeout=10,
    )

    try:
        response = downloader.fetch(request)
        print(f"通过代理获取的IP信息: {response.json()}")
    except Exception as e:
        print(f"代理请求失败（这是正常的，因为示例代理可能不存在）: {e}")

    # 不使用代理的对比请求
    request_no_proxy = Request(url="https://httpbin.org/ip")
    response_no_proxy = downloader.fetch(request_no_proxy)
    print(f"直连获取的IP信息: {response_no_proxy.json()}")


def retry_mechanism():
    """重试机制示例"""
    print("\n=== 重试机制示例 ===")

    downloader = RequestsDownloader()
    downloader.debug = True

    # 模拟可能失败的请求
    request = Request(
        url="https://httpbin.org/status/500",  # 这个端点总是返回 500 错误
        max_retry=3,  # 最大重试 3 次
        timeout=5,
    )

    start_time = time.time()
    response = downloader.fetch(request)
    end_time = time.time()

    print(f"请求状态码: {response.status_code}")
    print(f"总耗时: {end_time - start_time:.2f}秒")
    print(f"重试次数: {request.retry}")


def timeout_handling():
    """超时处理示例"""
    print("\n=== 超时处理示例 ===")

    downloader = RequestsDownloader()

    # 短超时请求
    request = Request(
        url="https://httpbin.org/delay/3",  # 这个端点会延迟 3 秒响应
        timeout=1,  # 设置 1 秒超时
    )

    start_time = time.time()
    response = downloader.fetch(request)
    end_time = time.time()

    print(f"请求状态码: {response.status_code}")
    print(f"实际耗时: {end_time - start_time:.2f}秒")
    print(f"错误信息: {response.error}")

    # 正常超时请求
    request_normal = Request(
        url="https://httpbin.org/delay/2",
        timeout=5,  # 设置 5 秒超时
    )

    response_normal = downloader.fetch(request_normal)
    print(f"正常请求状态码: {response_normal.status_code}")


def custom_success_condition():
    """自定义成功条件示例"""
    print("\n=== 自定义成功条件示例 ===")

    downloader = RequestsDownloader()

    # 自定义成功条件：即使是 404 也认为是成功的
    request = Request(
        url="https://httpbin.org/status/404",
        ok="200 <= response.status_code < 500",  # 自定义成功条件
    )

    response = downloader.fetch(request)

    print(f"状态码: {response.status_code}")
    print(f"是否成功: {response.ok}")
    print(f"自定义成功条件生效")


def redirect_handling():
    """重定向处理示例"""
    print("\n=== 重定向处理示例 ===")

    downloader = RequestsDownloader()

    # 允许重定向的请求
    request_with_redirect = Request(
        url="https://httpbin.org/redirect/3",  # 会重定向 3 次
        allow_redirects=True,
    )

    response = downloader.fetch(request_with_redirect)

    print(f"最终状态码: {response.status_code}")
    print(f"最终URL: {response.url}")
    print(f"重定向历史数量: {len(response.history)}")

    for i, hist_resp in enumerate(response.history):
        print(f"  重定向 {i + 1}: {hist_resp.status_code} -> {hist_resp.url}")

    # 禁止重定向的请求
    request_no_redirect = Request(
        url="https://httpbin.org/redirect/1", allow_redirects=False
    )

    response_no_redirect = downloader.fetch(request_no_redirect)
    print(f"\n禁止重定向 - 状态码: {response_no_redirect.status_code}")
    print(f"Location 头: {response_no_redirect.headers.get('Location')}")


def response_data_extraction():
    """响应数据提取示例"""
    print("\n=== 响应数据提取示例 ===")

    downloader = RequestsDownloader()

    # 获取 JSON 数据
    request = Request(url="https://httpbin.org/json")
    response = downloader.fetch(request)

    print("JSON 数据提取:")
    json_data = response.json()
    print(f"完整 JSON: {json_data}")

    # 使用 JMESPath 提取特定字段
    slideshow_title = response.get("slideshow.title")
    print(f"幻灯片标题: {slideshow_title}")

    slides = response.get("slideshow.slides")
    print(f"幻灯片数量: {len(slides) if slides else 0}")

    # 提取第一个幻灯片的标题
    first_slide_title = response.get_first("slideshow.slides[0].title")
    print(f"第一个幻灯片标题: {first_slide_title}")


def cookie_management():
    """Cookie 管理示例"""
    print("\n=== Cookie 管理示例 ===")

    downloader = RequestsDownloader()

    # 设置多个 cookies
    request1 = Request(
        url="https://httpbin.org/cookies/set",
        params={"user": "demo", "theme": "dark", "lang": "zh"},
        use_session=True,
    )

    response1 = downloader.fetch(request1)
    print(f"设置 Cookie 后的响应: {response1.status_code}")

    # 查看设置的 cookies
    request2 = Request(url="https://httpbin.org/cookies", use_session=True)

    response2 = downloader.fetch(request2)
    cookies_data = response2.json()
    print(f"当前 Cookies: {cookies_data.get('cookies', {})}")

    # 手动添加 cookie 到请求
    request3 = Request(
        url="https://httpbin.org/cookies",
        cookies={"manual_cookie": "test_value", "another": "value2"},
    )

    response3 = downloader.fetch(request3)
    print(f"手动 Cookie 请求结果: {response3.json().get('cookies', {})}")


def file_download_simulation():
    """文件下载模拟示例"""
    print("\n=== 文件下载模拟示例 ===")

    downloader = RequestsDownloader()

    # 下载二进制数据（模拟图片下载）
    request = Request(
        url="https://httpbin.org/image/png", headers={"Accept": "image/png"}
    )

    response = downloader.fetch(request)

    print(f"响应状态码: {response.status_code}")
    print(f"Content-Type: {response.headers.get('Content-Type')}")
    print(f"内容长度: {len(response.content)} 字节")
    print(f"是否为二进制数据: {isinstance(response.content, bytes)}")

    # 模拟保存文件
    if response.ok and isinstance(response.content, bytes):
        print("文件下载成功（模拟保存）")
        # 实际使用时可以这样保存：
        # with open("downloaded_image.png", "wb") as f:
        #     f.write(response.content)


def custom_headers_and_user_agent():
    """自定义请求头和 User-Agent 示例"""
    print("\n=== 自定义请求头和 User-Agent 示例 ===")

    downloader = RequestsDownloader()

    # 模拟不同浏览器的请求
    user_agents = {
        "Chrome": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Firefox": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Safari": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    }

    for browser, ua in user_agents.items():
        request = Request(
            url="https://httpbin.org/user-agent",
            headers={
                "User-Agent": ua,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
            },
        )

        response = downloader.fetch(request)
        detected_ua = response.json().get("user-agent", "")
        print(f"{browser} User-Agent 检测结果: {detected_ua[:50]}...")


def spider_with_downloader_example():
    """爬虫使用下载器示例"""
    print("\n=== 爬虫使用下载器示例 ===")

    # 导入爬虫相关模块
    try:
        from bricks.downloader.httpx_ import Downloader as HttpxDownloader
        from bricks.downloader.requests_ import Downloader as RequestsDownloader
        from bricks.spider.form import Parse, Spider
        from bricks.spider.form import Request as SpiderRequest

        print("1. 创建自定义下载器配置的爬虫")

        # 创建带自定义配置的下载器
        custom_downloader = RequestsDownloader()
        custom_downloader.debug = True  # 开启调试模式

        # 创建爬虫实例，传入自定义下载器
        spider = Spider(
            downloader=custom_downloader,  # 传入自定义下载器
            concurrency=2,  # 并发数
            interval=1.0,  # 请求间隔
        )

        # 定义解析函数
        def parse_httpbin_data(response):
            """解析 httpbin 响应数据"""
            data = response.json()
            print(f"  解析到的数据: {data.get('url', 'N/A')}")
            print(f"  User-Agent: {data.get('headers', {}).get('User-Agent', 'N/A')}")
            return {
                "url": data.get("url"),
                "user_agent": data.get("headers", {}).get("User-Agent"),
            }

        # 添加爬取任务
        spider.add_task(
            SpiderRequest(
                url="https://httpbin.org/get",
                params={"spider": "demo", "downloader": "requests"},
                headers={"X-Spider": "Bricks-Demo"},
            ),
            Parse(func=parse_httpbin_data),
        )

        print("2. 运行爬虫（使用自定义下载器）")
        results = list(spider.run())
        print(f"爬取结果数量: {len(results)}")

        print("\n3. 使用不同下载器的爬虫对比")

        # 使用 httpx 下载器的爬虫
        httpx_downloader = HttpxDownloader()
        httpx_spider = Spider(downloader=httpx_downloader, concurrency=1)

        httpx_spider.add_task(
            SpiderRequest(
                url="https://httpbin.org/get",
                params={"downloader": "httpx"},
                headers={"X-Downloader": "Httpx"},
            ),
            Parse(func=parse_httpbin_data),
        )

        print("使用 Httpx 下载器的爬虫:")
        httpx_results = list(httpx_spider.run())
        print(f"Httpx 爬虫结果数量: {len(httpx_results)}")

        print("\n4. 爬虫下载器配置示例")

        # 展示不同下载器配置
        downloader_configs = {
            "高性能配置": {
                "downloader": "httpx",
                "options": {"timeout": 30, "verify": False},
                "concurrency": 10,
                "interval": 0.1,
            },
            "稳定性配置": {
                "downloader": "requests",
                "options": {"timeout": 60, "verify": True},
                "concurrency": 3,
                "interval": 1.0,
            },
            "反检测配置": {
                "downloader": "tls_client",
                "options": {"ja3_string": "custom_ja3"},
                "concurrency": 1,
                "interval": 2.0,
            },
        }

        print("不同场景的爬虫下载器配置:")
        for config_name, config in downloader_configs.items():
            print(f"\n{config_name}:")
            for key, value in config.items():
                print(f"  {key}: {value}")

    except ImportError as e:
        print(f"爬虫模块导入失败: {e}")
        print("这是正常的，因为可能缺少某些依赖")

        # 提供模拟示例
        print("\n模拟爬虫使用下载器的代码结构:")
        print("""
# 基本用法
from bricks.spider import Spider
from bricks.downloader.requests_ import Downloader

# 创建自定义下载器
downloader = Downloader()
downloader.debug = True

# 创建爬虫，传入下载器
spider = Spider(downloader=downloader)

# 添加任务并运行
spider.add_task(request, parse_func)
results = list(spider.run())
        """)

    except Exception as e:
        print(f"爬虫示例执行失败: {e}")


if __name__ == "__main__":
    """运行所有高级示例"""
    print("Bricks 下载器高级功能示例")
    print("=" * 50)

    try:
        proxy_usage()
        retry_mechanism()
        timeout_handling()
        custom_success_condition()
        redirect_handling()
        response_data_extraction()
        cookie_management()
        file_download_simulation()
        custom_headers_and_user_agent()
        spider_with_downloader_example()

        print("\n" + "=" * 50)
        print("所有高级示例运行完成！")

    except Exception as e:
        print(f"运行示例时出错: {e}")
        import traceback

        traceback.print_exc()
