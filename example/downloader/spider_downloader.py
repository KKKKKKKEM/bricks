#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬虫使用下载器示例

这个示例展示了如何在 bricks 爬虫中使用不同的下载器，
包括下载器的配置、传递和在爬虫中的实际应用。
"""

import time
from bricks import Request, Response
from bricks.downloader.requests_ import Downloader as RequestsDownloader
from bricks.downloader.httpx_ import Downloader as HttpxDownloader


def basic_spider_downloader():
    """基础爬虫下载器使用示例"""
    print("=== 基础爬虫下载器使用示例 ===")
    
    try:
        from bricks.spider.form import Spider
        from bricks.spider.form import Request as SpiderRequest, Parse
        
        # 1. 创建自定义下载器
        downloader = RequestsDownloader()
        downloader.debug = True  # 开启调试模式
        
        # 2. 创建爬虫实例，传入下载器
        spider = Spider(
            downloader=downloader,  # 关键：传入自定义下载器
            concurrency=2,          # 并发数
            interval=0.5,           # 请求间隔（秒）
        )
        
        # 3. 定义数据解析函数
        def parse_basic_data(response):
            """解析基础数据"""
            try:
                data = response.json()
                result = {
                    "url": data.get("url"),
                    "method": data.get("method"),
                    "headers": data.get("headers", {}),
                    "args": data.get("args", {}),
                    "status": "success"
                }
                print(f"  ✓ 解析成功: {result['url']}")
                return result
            except Exception as e:
                print(f"  ✗ 解析失败: {e}")
                return {"status": "error", "error": str(e)}
        
        # 4. 添加爬取任务
        spider.add_task(
            SpiderRequest(
                url="https://httpbin.org/get",
                params={"test": "spider", "downloader": "requests"},
                headers={"User-Agent": "Bricks-Spider/1.0"}
            ),
            Parse(func=parse_basic_data)
        )
        
        # 5. 运行爬虫
        print("开始运行爬虫...")
        results = list(spider.run())
        print(f"爬取完成，共获得 {len(results)} 条结果")
        
        return results
        
    except ImportError as e:
        print(f"Spider 模块导入失败: {e}")
        print("显示模拟代码结构:")
        print("""
# 基本使用方式
from bricks.spider.form import Spider
from bricks.downloader.requests_ import Downloader

# 创建下载器
downloader = Downloader()

# 创建爬虫，传入下载器
spider = Spider(downloader=downloader)

# 添加任务
spider.add_task(request, parse_function)

# 运行爬虫
results = list(spider.run())
        """)


def compare_different_downloaders():
    """比较不同下载器在爬虫中的表现"""
    print("\n=== 比较不同下载器在爬虫中的表现 ===")
    
    try:
        from bricks.spider.form import Spider
        from bricks.spider.form import Request as SpiderRequest, Parse
        
        # 测试 URL
        test_url = "https://httpbin.org/delay/1"  # 1秒延迟的测试接口
        
        def parse_performance_data(response):
            """解析性能数据"""
            try:
                data = response.json()
                return {
                    "url": data.get("url"),
                    "cost": getattr(response, 'cost', 0),
                    "status_code": response.status_code,
                    "downloader": "unknown"
                }
            except:
                return {"error": "parse_failed"}
        
        downloaders = {
            "Requests": RequestsDownloader(),
            "Httpx": HttpxDownloader(),
        }
        
        results = {}
        
        for name, downloader in downloaders.items():
            print(f"\n测试 {name} 下载器:")
            
            # 创建使用特定下载器的爬虫
            spider = Spider(
                downloader=downloader,
                concurrency=1,  # 单线程测试，便于比较
                interval=0.1
            )
            
            # 添加测试任务
            spider.add_task(
                SpiderRequest(
                    url=test_url,
                    params={"downloader": name.lower()},
                    headers={"X-Test-Downloader": name}
                ),
                Parse(func=parse_performance_data)
            )
            
            # 测试性能
            start_time = time.time()
            spider_results = list(spider.run())
            end_time = time.time()
            
            results[name] = {
                "total_time": end_time - start_time,
                "results_count": len(spider_results),
                "avg_response_time": spider_results[0].get('cost', 0) if spider_results else 0
            }
            
            print(f"  总耗时: {results[name]['total_time']:.2f}秒")
            print(f"  结果数量: {results[name]['results_count']}")
            print(f"  平均响应时间: {results[name]['avg_response_time']:.2f}秒")
        
        # 性能对比总结
        print(f"\n性能对比总结:")
        for name, metrics in results.items():
            print(f"{name}: 总耗时 {metrics['total_time']:.2f}s, 响应时间 {metrics['avg_response_time']:.2f}s")
            
    except ImportError:
        print("Spider 模块未找到，显示对比示例代码:")
        print("""
# 下载器性能对比示例
downloaders = {
    "requests": RequestsDownloader(),
    "httpx": HttpxDownloader(),
    "tls_client": TLSDownloader()
}

for name, downloader in downloaders.items():
    spider = Spider(downloader=downloader)
    # 运行相同的测试任务
    results = list(spider.run())
    # 比较性能指标
        """)


def spider_downloader_best_practices():
    """爬虫下载器最佳实践"""
    print("\n=== 爬虫下载器最佳实践 ===")
    
    best_practices = {
        "选择合适的下载器": [
            "requests: 稳定性好，兼容性强，适合大多数场景",
            "httpx: 性能更好，支持异步，适合高并发场景", 
            "tls_client: 反检测能力强，适合需要绕过检测的场景",
            "playwright: 支持 JavaScript，适合 SPA 应用"
        ],
        "配置优化": [
            "合理设置超时时间，避免长时间等待",
            "使用连接池和会话复用，提高性能",
            "配置合适的重试机制，提高成功率",
            "设置合理的并发数，避免对目标服务器造成压力"
        ],
        "错误处理": [
            "捕获网络异常，实现优雅降级",
            "记录详细的错误日志，便于调试",
            "实现智能重试，区分不同类型的错误",
            "监控请求成功率，及时发现问题"
        ],
        "反检测策略": [
            "随机化 User-Agent 和请求头",
            "使用代理 IP 轮换",
            "控制请求频率，模拟人类行为",
            "使用真实浏览器指纹"
        ]
    }
    
    for category, practices in best_practices.items():
        print(f"\n{category}:")
        for practice in practices:
            print(f"  • {practice}")
    
    print(f"\n推荐的爬虫下载器配置模板:")
    print("""
# 生产环境推荐配置
from bricks.spider.form import Spider
from bricks.downloader.requests_ import Downloader

# 创建下载器
downloader = Downloader()
downloader.debug = False  # 生产环境关闭调试

# 创建爬虫
spider = Spider(
    downloader=downloader,
    concurrency=5,        # 适中的并发数
    interval=1.0,         # 1秒间隔，避免过于频繁
    max_workers=10,       # 最大工作线程数
    timeout=30,           # 30秒超时
)
    """)


if __name__ == "__main__":
    """运行所有爬虫下载器示例"""
    print("Bricks 爬虫下载器使用示例")
    print("=" * 50)
    
    try:
        basic_spider_downloader()
        compare_different_downloaders()
        spider_downloader_best_practices()
        
        print("\n" + "=" * 50)
        print("所有爬虫下载器示例运行完成！")
        print("\n总结:")
        print("- 爬虫可以通过 downloader 参数传入自定义下载器")
        print("- 不同下载器适用于不同的爬取场景")
        print("- 合理配置下载器可以显著提升爬虫性能")
        print("- 自定义下载器可以添加特定的业务逻辑")
        
    except Exception as e:
        print(f"运行示例时出错: {e}")
        import traceback
        traceback.print_exc()
