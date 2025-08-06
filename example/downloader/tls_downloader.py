#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TLS 下载器使用示例

这个示例展示了如何使用 bricks 框架中的 TLS 相关下载器，
包括 tls_client、cffi、curl 等，用于处理需要特定 TLS 指纹的网站。
"""
import requests_go

from bricks import Request, Response


def tls_client_example():
    """TLS Client 下载器示例"""
    print("=== TLS Client 下载器示例 ===")

    try:
        from bricks.downloader.tls_client_ import Downloader as TLSDownloader

        # 创建 TLS Client 下载器
        tls_config = {
            "ja3_string": "771,4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53,0-23-65281-10-11-35-16-5-13-18-51-45-43-27-17513,29-23-24,0",
            "h2_settings": {
                "HEADER_TABLE_SIZE": 65536,
                "MAX_CONCURRENT_STREAMS": 1000,
                "INITIAL_WINDOW_SIZE": 6291456,
                "MAX_HEADER_LIST_SIZE": 262144,
            },
            "h2_settings_order": [
                "HEADER_TABLE_SIZE",
                "MAX_CONCURRENT_STREAMS",
                "INITIAL_WINDOW_SIZE",
                "MAX_HEADER_LIST_SIZE",
            ],
            "supported_signature_algorithms": [
                "ECDSAWithP256AndSHA256",
                "PSSWithSHA256",
                "PKCS1WithSHA256",
                "ECDSAWithP384AndSHA384",
                "PSSWithSHA384",
                "PKCS1WithSHA384",
                "PSSWithSHA512",
                "PKCS1WithSHA512",
            ],
            "supported_versions": ["1.3", "1.2"],
            "key_share_curves": ["X25519", "P-256", "P-384", "P-521"],
            "cert_compression_algo": "brotli",
            "pseudo_header_order": [":method", ":authority", ":scheme", ":path"],
            "connection_flow": 15663105,
            "header_order": [
                "accept",
                "user-agent",
                "accept-encoding",
                "accept-language",
            ],
        }

        downloader = TLSDownloader(tls_config=tls_config)

        # 发送请求
        request = Request(
            url="https://httpbin.org/get",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
            },
        )

        response = downloader.fetch(request)
        print(f"TLS Client 状态码: {response.status_code}")
        print(f"响应头: {dict(response.headers)}")

    except ImportError:
        print("tls_client 未安装，跳过此示例")
        print("安装命令: pip install tls_client")
    except Exception as e:
        print(f"TLS Client 示例执行失败: {e}")


def cffi_downloader_example():
    """CFFI 下载器示例（模拟浏览器 TLS 指纹）"""
    print("\n=== CFFI 下载器示例 ===")

    try:
        from bricks.downloader.cffi import Downloader as CFfiDownloader

        # 创建 CFFI 下载器，模拟 Chrome 浏览器
        downloader = CFfiDownloader(
            impersonate="chrome110",  # 模拟 Chrome 110
            options={"timeout": 30, "verify": False},
        )

        # 测试请求
        request = Request(
            url="https://httpbin.org/headers",
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            },
        )

        response = downloader.fetch(request)
        print(f"CFFI 状态码: {response.status_code}")

        # 检查服务器接收到的请求头
        headers_data = response.json()
        print(
            f"服务器接收到的 User-Agent: {headers_data.get('headers', {}).get('User-Agent', 'N/A')}"
        )

    except ImportError:
        print("cffi 相关库未安装，跳过此示例")
        print("安装命令: pip install curl_cffi")
    except Exception as e:
        print(f"CFFI 示例执行失败: {e}")


def curl_downloader_example():
    """Curl 下载器示例"""
    print("\n=== Curl 下载器示例 ===")

    try:
        from bricks.downloader.curl import Downloader as CurlDownloader

        # 创建 Curl 下载器
        downloader = CurlDownloader()

        # 配置 TLS 相关选项
        request = Request(
            url="https://httpbin.org/get",
            headers={"User-Agent": "curl/7.68.0", "Accept": "*/*"},
            options={
                "ssl_version": "TLSv1.2",
                "ciphers": "ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS",
                "http_version": "2.0",
            },
        )

        response = downloader.fetch(request)
        print(f"Curl 状态码: {response.status_code}")
        print(f"HTTP 版本: {response.headers.get('HTTP-Version', 'Unknown')}")

    except ImportError:
        print("pycurl 未安装，跳过此示例")
        print("安装命令: pip install pycurl")
    except Exception as e:
        print(f"Curl 示例执行失败: {e}")


def go_requests_example():
    """Go-Requests 下载器示例"""
    print("\n=== Go-Requests 下载器示例 ===")

    try:
        from bricks.downloader.go_requests import Downloader as GoRequestsDownloader

        tls = requests_go.tls_config.TLSConfig()
        tls.ja3 = "771,4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53,16-18-5-27-0-13-11-43-45-35-51-23-10-65281-17513-21,29-23-24,0"
        tls.pseudo_header_order = [
            ":method",
            ":authority",
            ":scheme",
            ":path",
        ]
        tls.tls_extensions.cert_compression_algo = ["brotli"]
        tls.tls_extensions.supported_signature_algorithms = [
            "ecdsa_secp256r1_sha256",
            "rsa_pss_rsae_sha256",
            "rsa_pkcs1_sha256",
            "ecdsa_secp384r1_sha384",
            "rsa_pss_rsae_sha384",
            "rsa_pkcs1_sha384",
            "rsa_pss_rsae_sha512",
            "rsa_pkcs1_sha512"
        ]
        tls.tls_extensions.supported_versions = [
            "GREASE",
            "1.3",
            "1.2"
        ]
        tls.tls_extensions.psk_key_exchange_modes = [
            "PskModeDHE"
        ]
        tls.tls_extensions.key_share_curves = [
            "GREASE",
            "X25519"
        ]
        tls.http2_settings.settings = {
            "HEADER_TABLE_SIZE": 65536,
            "ENABLE_PUSH": 0,
            "MAX_CONCURRENT_STREAMS": 1000,
            "INITIAL_WINDOW_SIZE": 6291456,
            "MAX_HEADER_LIST_SIZE": 262144
        }
        tls.http2_settings.settings_order = [
            "HEADER_TABLE_SIZE",
            "ENABLE_PUSH",
            "MAX_CONCURRENT_STREAMS",
            "INITIAL_WINDOW_SIZE",
            "MAX_HEADER_LIST_SIZE"
        ]
        tls.http2_settings.connection_flow = 15663105

        downloader = GoRequestsDownloader(tls_config=tls)

        request = Request(
            url="https://httpbin.org/get",
            headers={"User-Agent": "Go-http-client/2.0", "Accept": "application/json"},
        )

        response = downloader.fetch(request)
        print(f"Go-Requests 状态码: {response.status_code}")
        print(f"TLS 版本: {response.headers.get('TLS-Version', 'Unknown')}")

    except ImportError:
        print("requests-go 未安装，跳过此示例")
        print("安装命令: pip install requests-go")
    except Exception as e:
        print(f"Go-Requests 示例执行失败: {e}")


def ja3_fingerprint_example():
    """JA3 指纹示例"""
    print("\n=== JA3 指纹配置示例 ===")

    # 常见浏览器的 JA3 指纹
    ja3_fingerprints = {
        "Chrome_110": "771,4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53,0-23-65281-10-11-35-16-5-13-18-51-45-43-27-17513,29-23-24,0",
        "Firefox_109": "771,4865-4867-4866-49195-49199-52393-52392-49196-49200-49162-49161-49171-49172-51-57-47-53,0-23-65281-10-11-35-16-5-51-43-13-45-28-21,29-23-24-25-256-257,0",
        "Safari_16": "771,4865-4866-4867-49196-49195-52393-49200-49199-52392-49162-49161-49172-49171-157-156-53-47,65281-0-23-35-13-5-18-16-30032-11-10,29-23-30-25-24,0",
        "Edge_110": "771,4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53,23-65281-10-11-35-16-5-13-18-51-45-43-27-17513-21,29-23-24,0",
    }

    print("常见浏览器 JA3 指纹:")
    for browser, ja3 in ja3_fingerprints.items():
        print(f"\n{browser}:")
        print(f"  JA3: {ja3[:50]}...")

    print("\nJA3 指纹组成部分:")
    print("1. TLS 版本")
    print("2. 支持的加密套件列表")
    print("3. 扩展列表")
    print("4. 椭圆曲线列表")
    print("5. 椭圆曲线点格式列表")


def http2_settings_example():
    """HTTP/2 设置示例"""
    print("\n=== HTTP/2 设置示例 ===")

    http2_configs = {
        "Chrome": {
            "HEADER_TABLE_SIZE": 65536,
            "MAX_CONCURRENT_STREAMS": 1000,
            "INITIAL_WINDOW_SIZE": 6291456,
            "MAX_HEADER_LIST_SIZE": 262144,
        },
        "Firefox": {
            "HEADER_TABLE_SIZE": 65536,
            "MAX_CONCURRENT_STREAMS": 1000,
            "INITIAL_WINDOW_SIZE": 131072,
            "MAX_FRAME_SIZE": 16777215,
        },
        "Safari": {
            "HEADER_TABLE_SIZE": 4096,
            "MAX_CONCURRENT_STREAMS": 100,
            "INITIAL_WINDOW_SIZE": 2097152,
            "MAX_HEADER_LIST_SIZE": 8192,
        },
    }

    print("不同浏览器的 HTTP/2 设置:")
    for browser, settings in http2_configs.items():
        print(f"\n{browser}:")
        for key, value in settings.items():
            print(f"  {key}: {value}")


def anti_fingerprinting_techniques():
    """反指纹识别技术示例"""
    print("\n=== 反指纹识别技术 ===")

    techniques = {
        "TLS 指纹随机化": [
            "随机选择 JA3 指纹",
            "动态调整加密套件顺序",
            "随机化扩展列表",
        ],
        "HTTP/2 指纹混淆": [
            "随机化 SETTINGS 帧参数",
            "调整窗口大小",
            "修改头部压缩设置",
        ],
        "请求头随机化": ["随机化 User-Agent", "调整 Accept 头顺序", "添加随机自定义头"],
        "时序特征混淆": ["随机请求间隔", "模拟人类行为模式", "添加随机延迟"],
    }

    print("反指纹识别技术:")
    for category, methods in techniques.items():
        print(f"\n{category}:")
        for method in methods:
            print(f"  - {method}")


def performance_comparison():
    """性能对比示例"""
    print("\n=== TLS 下载器性能对比 ===")

    performance_data = {
        "requests": {"speed": "★★★☆☆", "compatibility": "★★★★★", "stealth": "★☆☆☆☆"},
        "httpx": {"speed": "★★★★☆", "compatibility": "★★★★☆", "stealth": "★☆☆☆☆"},
        "tls_client": {"speed": "★★★★★", "compatibility": "★★★☆☆", "stealth": "★★★★★"},
        "curl_cffi": {"speed": "★★★★☆", "compatibility": "★★★★☆", "stealth": "★★★★☆"},
        "pycurl": {"speed": "★★★★★", "compatibility": "★★★☆☆", "stealth": "★★★☆☆"},
        "go_requests": {"speed": "★★★★★", "compatibility": "★★★☆☆", "stealth": "★★★★☆"},
    }

    print("下载器性能对比:")
    print(f"{'下载器':<12} {'速度':<8} {'兼容性':<10} {'隐蔽性':<8}")
    print("-" * 40)

    for downloader, metrics in performance_data.items():
        print(
            f"{downloader:<12} {metrics['speed']:<8} {metrics['compatibility']:<10} {metrics['stealth']:<8}"
        )

    print("\n选择建议:")
    print("- 高性能需求: tls_client, pycurl, go_requests")
    print("- 高兼容性需求: requests, httpx")
    print("- 反检测需求: tls_client, curl_cffi")
    print("- 平衡选择: curl_cffi, go_requests")


if __name__ == "__main__":
    """运行所有 TLS 下载器示例"""
    print("Bricks TLS 下载器使用示例")
    print("=" * 50)

    try:
        tls_client_example()
        cffi_downloader_example()
        curl_downloader_example()
        go_requests_example()
        ja3_fingerprint_example()
        http2_settings_example()
        anti_fingerprinting_techniques()
        performance_comparison()

        print("\n" + "=" * 50)
        print("所有 TLS 下载器示例运行完成！")
        print("\n注意事项:")
        print("- 某些下载器需要额外安装依赖")
        print("- TLS 指纹配置需要根据目标网站调整")
        print("- 建议在测试环境中验证配置效果")

    except Exception as e:
        print(f"运行示例时出错: {e}")
        import traceback

        traceback.print_exc()
