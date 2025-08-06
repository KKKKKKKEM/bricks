#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
浏览器下载器使用示例

这个示例展示了如何使用 bricks 框架中的浏览器类下载器，如 Playwright 和 DrissionPage。
这些下载器可以处理 JavaScript 渲染的页面。
"""

import asyncio
from bricks import Request, Response


def drission_page_example():
    """DrissionPage 下载器示例"""
    print("=== DrissionPage 下载器示例 ===")
    
    try:
        from bricks.downloader.dp import Downloader as DPDownloader
        
        # 创建 DrissionPage 下载器
        downloader = DPDownloader(mode="s")  # s 模式表示 session 模式
        
        # 简单的页面请求
        request = Request(url="https://httpbin.org/html")
        response = downloader.fetch(request)
        
        print(f"状态码: {response.status_code}")
        print(f"页面标题提取: {response.html.xpath('//title/text()')}")
        print(f"响应内容长度: {len(response.text)}")
        
        # 带 JavaScript 的页面（模拟）
        js_request = Request(
            url="https://httpbin.org/get",
            options={
                "wait_time": 2,  # 等待页面加载
                "execute_script": "console.log('Page loaded');"
            }
        )
        
        js_response = downloader.fetch(js_request)
        print(f"JavaScript 页面状态码: {js_response.status_code}")
        
    except ImportError:
        print("DrissionPage 未安装，跳过此示例")
        print("安装命令: pip install DrissionPage")
    except Exception as e:
        print(f"DrissionPage 示例执行失败: {e}")


async def playwright_example():
    """Playwright 下载器示例"""
    print("\n=== Playwright 下载器示例 ===")
    
    try:
        from bricks.downloader.playwright_ import Downloader as PlaywrightDownloader
        
        # 创建 Playwright 下载器
        downloader = PlaywrightDownloader(
            driver="chromium",  # 使用 Chromium
            headless=True,      # 无头模式
            debug=False
        )
        
        # 安装浏览器（如果需要）
        # downloader.install()
        
        # 简单页面请求
        request = Request(url="https://httpbin.org/html")
        response = await downloader.fetch(request)
        
        print(f"状态码: {response.status_code}")
        print(f"页面内容长度: {len(response.text)}")
        
        # 带 JavaScript 执行的请求
        js_request = Request(
            url="https://httpbin.org/get",
            options={
                "scripts": [
                    "console.log('Playwright script executed');",
                    "document.title = 'Modified by Playwright';"
                ],
                "wait_for": "networkidle",  # 等待网络空闲
                "timeout": 10000
            }
        )
        
        js_response = await downloader.fetch(js_request)
        print(f"JavaScript 执行后状态码: {js_response.status_code}")
        
    except ImportError:
        print("Playwright 未安装，跳过此示例")
        print("安装命令: pip install playwright")
    except Exception as e:
        print(f"Playwright 示例执行失败: {e}")


def browser_automation_simulation():
    """浏览器自动化模拟示例"""
    print("\n=== 浏览器自动化模拟示例 ===")
    
    # 模拟表单填写和提交
    form_data = {
        "username": "demo_user",
        "password": "demo_pass",
        "email": "demo@example.com"
    }
    
    print("模拟浏览器自动化操作:")
    print(f"1. 填写表单数据: {form_data}")
    print("2. 等待页面加载完成")
    print("3. 执行 JavaScript 脚本")
    print("4. 截取页面截图")
    print("5. 提取页面数据")
    
    # 实际的浏览器下载器会执行这些操作
    print("\n注意: 浏览器下载器适用于以下场景:")
    print("- 需要 JavaScript 渲染的 SPA 应用")
    print("- 需要模拟用户交互的场景")
    print("- 需要处理复杂的前端逻辑")
    print("- 需要截图或 PDF 生成")


def spa_page_example():
    """SPA 页面处理示例"""
    print("\n=== SPA 页面处理示例 ===")
    
    # 模拟处理单页应用
    spa_config = {
        "url": "https://example-spa.com",
        "wait_for_selector": ".content-loaded",  # 等待特定元素出现
        "wait_time": 3,  # 额外等待时间
        "execute_scripts": [
            "window.scrollTo(0, document.body.scrollHeight);",  # 滚动到底部
            "document.querySelector('.load-more').click();"      # 点击加载更多
        ]
    }
    
    print("SPA 页面处理配置:")
    for key, value in spa_config.items():
        print(f"  {key}: {value}")
    
    print("\n处理流程:")
    print("1. 导航到 SPA 页面")
    print("2. 等待关键元素加载")
    print("3. 执行 JavaScript 交互")
    print("4. 等待动态内容加载")
    print("5. 提取最终页面数据")


def mobile_browser_simulation():
    """移动端浏览器模拟示例"""
    print("\n=== 移动端浏览器模拟示例 ===")
    
    mobile_configs = {
        "iPhone": {
            "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15",
            "viewport": {"width": 375, "height": 667},
            "device_scale_factor": 2
        },
        "Android": {
            "user_agent": "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36",
            "viewport": {"width": 360, "height": 640},
            "device_scale_factor": 3
        }
    }
    
    print("移动端设备模拟配置:")
    for device, config in mobile_configs.items():
        print(f"\n{device}:")
        for key, value in config.items():
            print(f"  {key}: {value}")
    
    print("\n移动端特殊处理:")
    print("- 触摸事件模拟")
    print("- 屏幕方向变化")
    print("- 移动端特有的 CSS 媒体查询")
    print("- 移动端性能优化")


def anti_detection_techniques():
    """反检测技术示例"""
    print("\n=== 反检测技术示例 ===")
    
    anti_detection_config = {
        "stealth_mode": True,
        "random_user_agent": True,
        "disable_webdriver_flag": True,
        "random_viewport": True,
        "disable_automation_flags": True,
        "custom_navigator_properties": {
            "webdriver": False,
            "plugins": ["Chrome PDF Plugin", "Chrome PDF Viewer"],
            "languages": ["zh-CN", "zh", "en"]
        }
    }
    
    print("反检测配置:")
    for key, value in anti_detection_config.items():
        print(f"  {key}: {value}")
    
    print("\n反检测技术包括:")
    print("- 隐藏 webdriver 标识")
    print("- 随机化浏览器指纹")
    print("- 模拟真实用户行为")
    print("- 随机延迟和间隔")
    print("- 代理 IP 轮换")


def performance_optimization():
    """性能优化示例"""
    print("\n=== 浏览器下载器性能优化 ===")
    
    optimization_config = {
        "disable_images": True,      # 禁用图片加载
        "disable_css": False,        # 保留 CSS（可能影响 JS 执行）
        "disable_fonts": True,       # 禁用字体加载
        "disable_plugins": True,     # 禁用插件
        "block_resources": [         # 阻止特定资源
            "*.jpg", "*.png", "*.gif",
            "google-analytics.com",
            "facebook.com/tr"
        ],
        "cache_enabled": True,       # 启用缓存
        "timeout": 30,              # 设置超时
        "max_concurrent": 5         # 最大并发数
    }
    
    print("性能优化配置:")
    for key, value in optimization_config.items():
        print(f"  {key}: {value}")
    
    print("\n优化效果:")
    print("- 减少网络传输")
    print("- 加快页面加载")
    print("- 降低内存使用")
    print("- 提高并发能力")


async def run_async_examples():
    """运行异步示例"""
    await playwright_example()


if __name__ == "__main__":
    """运行所有浏览器下载器示例"""
    print("Bricks 浏览器下载器使用示例")
    print("=" * 50)
    
    try:
        # 同步示例
        drission_page_example()
        browser_automation_simulation()
        spa_page_example()
        mobile_browser_simulation()
        anti_detection_techniques()
        performance_optimization()
        
        # 异步示例
        print("\n运行异步示例...")
        asyncio.run(run_async_examples())
        
        print("\n" + "=" * 50)
        print("所有浏览器下载器示例运行完成！")
        
    except Exception as e:
        print(f"运行示例时出错: {e}")
        import traceback
        traceback.print_exc()
