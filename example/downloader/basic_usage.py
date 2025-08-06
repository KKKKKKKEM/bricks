#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基础下载器使用示例

这个示例展示了如何使用 bricks 框架中的各种下载器进行网络请求。
"""

from bricks import Request
from bricks.downloader.httpx_ import Downloader as HttpxDownloader
from bricks.downloader.requests_ import Downloader as RequestsDownloader


def basic_get_request():
    """基础 GET 请求示例"""
    print("=== 基础 GET 请求示例 ===")

    # 创建下载器实例
    downloader = RequestsDownloader()

    # 创建请求对象
    request = Request(
        url="https://httpbin.org/get",
        params={"key": "value", "test": "demo"}
    )

    # 发送请求
    response = downloader.fetch(request)

    print(f"状态码: {response.status_code}")
    print(f"响应URL: {response.url}")
    print(f"响应内容: {response.text[:200]}...")
    print(f"请求耗时: {response.cost:.2f}秒")

    return response


def basic_post_request():
    """基础 POST 请求示例"""
    print("\n=== 基础 POST 请求示例 ===")

    downloader = RequestsDownloader()

    # JSON 格式的 POST 请求
    request = Request(
        url="https://httpbin.org/post",
        method="POST",
        headers={"Content-Type": "application/json"},
        body={"name": "张三", "age": 25, "city": "北京"}
    )

    response = downloader.fetch(request)

    print(f"状态码: {response.status_code}")
    print(f"响应内容: {response.json()}")

    return response


def form_data_request():
    """表单数据请求示例"""
    print("\n=== 表单数据请求示例 ===")

    downloader = RequestsDownloader()

    # 表单格式的 POST 请求
    request = Request(
        url="https://httpbin.org/post",
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        body={"username": "user123", "password": "pass456", "remember": "true"}
    )

    response = downloader.fetch(request)

    print(f"状态码: {response.status_code}")
    print(f"表单数据: {response.json().get('form', {})}")

    return response


def request_with_headers_and_cookies():
    """带请求头和 Cookie 的请求示例"""
    print("\n=== 带请求头和 Cookie 的请求示例 ===")

    downloader = RequestsDownloader()

    request = Request(
        url="https://httpbin.org/headers",
        headers={
            "User-Agent": "Bricks-Demo/1.0",
            "Accept": "application/json",
            "Authorization": "Bearer demo-token"
        },
        cookies={"session_id": "abc123", "user_pref": "dark_mode"}
    )

    response = downloader.fetch(request)

    print(f"状态码: {response.status_code}")
    print(f"服务器收到的请求头: {response.json().get('headers', {})}")

    return response


def session_usage():
    """会话使用示例"""
    print("\n=== 会话使用示例 ===")

    downloader = RequestsDownloader()

    # 第一个请求：设置 cookie
    request1 = Request(
        url="https://httpbin.org/cookies/set?demo=session_test",
        use_session=True  # 使用会话
    )

    response1 = downloader.fetch(request1)
    print(f"设置 Cookie 响应: {response1.status_code}")

    # 第二个请求：验证 cookie 是否保持
    request2 = Request(
        url="https://httpbin.org/cookies",
        use_session=True  # 继续使用同一会话
    )

    response2 = downloader.fetch(request2)
    print(f"获取 Cookie 响应: {response2.json()}")

    return response1, response2


def error_handling():
    """错误处理示例"""
    print("\n=== 错误处理示例 ===")

    downloader = RequestsDownloader()
    downloader.debug = True  # 开启调试模式

    # 请求一个不存在的地址
    request = Request(
        url="https://nonexistent-domain-12345.com",
        timeout=5
    )

    response = downloader.fetch(request)

    print(f"状态码: {response.status_code}")
    print(f"错误信息: {response.error}")
    print(f"错误原因: {response.reason}")

    return response


def compare_downloaders():
    """比较不同下载器的示例"""
    print("\n=== 比较不同下载器示例 ===")

    url = "https://httpbin.org/get"
    request = Request(url=url)

    # 使用 requests 下载器
    requests_downloader = RequestsDownloader()
    requests_response = requests_downloader.fetch(request)
    print(f"Requests 下载器 - 状态码: {requests_response.status_code}, 耗时: {requests_response.cost:.3f}秒")

    # 使用 httpx 下载器
    httpx_downloader = HttpxDownloader()
    httpx_response = httpx_downloader.fetch(request)
    print(f"Httpx 下载器 - 状态码: {httpx_response.status_code}, 耗时: {httpx_response.cost:.3f}秒")

    return requests_response, httpx_response


def curl_import_example():
    """从 curl 命令导入请求示例"""
    print("\n=== 从 curl 命令导入请求示例 ===")

    # curl 命令字符串
    curl_cmd = """
    curl -X POST 'https://httpbin.org/post' \
    -H 'Content-Type: application/json' \
    -H 'User-Agent: curl/7.68.0' \
    -d '{"message": "Hello from curl!", "timestamp": "2024-01-01"}'
    """

    # 从 curl 命令创建请求
    request = Request.from_curl(curl_cmd)

    print(f"解析的请求URL: {request.url}")
    print(f"请求方法: {request.method}")
    print(f"请求头: {dict(request.headers)}")
    print(f"请求体: {request.body}")

    # 发送请求
    downloader = RequestsDownloader()
    response = downloader.fetch(request)

    print(f"响应状态码: {response.status_code}")
    print(f"响应内容: {response.json()}")

    return response


if __name__ == "__main__":
    """运行所有示例"""
    print("Bricks 下载器使用示例")
    print("=" * 50)

    try:
        # 运行各种示例
        basic_get_request()
        basic_post_request()
        form_data_request()
        request_with_headers_and_cookies()
        session_usage()
        error_handling()
        compare_downloaders()
        curl_import_example()

        print("\n" + "=" * 50)
        print("所有示例运行完成！")

    except Exception as e:
        print(f"运行示例时出错: {e}")
        import traceback

        traceback.print_exc()
