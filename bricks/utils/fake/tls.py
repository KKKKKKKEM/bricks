"""
TLS 指纹转换工具
用于将浏览器 TLS 指纹转换为 curl_cffi 可用的参数
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from curl_cffi import requests


@dataclass
class TLSFingerprint:
    """TLS 指纹数据类"""
    ja3: str = ""
    akamai: str = ""
    user_agent: str = ""
    extra_fp: Dict[str, Any] = field(default_factory=dict)


class TLSFingerprintParser:
    """TLS 指纹解析器"""

    # ============ 签名算法映射表 ============
    SIGNATURE_ALGORITHMS = {
        # RSA PKCS#1 v1.5
        "0201": "rsa_pkcs1_sha1",
        "0401": "rsa_pkcs1_sha256",
        "0501": "rsa_pkcs1_sha384",
        "0601": "rsa_pkcs1_sha512",

        # ECDSA
        "0203": "ecdsa_sha1",
        "0403": "ecdsa_secp256r1_sha256",
        "0503": "ecdsa_secp384r1_sha384",
        "0603": "ecdsa_secp521r1_sha512",

        # RSA-PSS RSAE (TLS 1.3)
        "0804": "rsa_pss_rsae_sha256",
        "0805": "rsa_pss_rsae_sha384",
        "0806": "rsa_pss_rsae_sha512",

        # EdDSA (TLS 1.3)
        "0807": "ed25519",
        "0808": "ed448",

        # RSA-PSS PSS (TLS 1.3)
        "0809": "rsa_pss_pss_sha256",
        "080a": "rsa_pss_pss_sha384",
        "080b": "rsa_pss_pss_sha512",

        # Legacy/Deprecated
        "0101": "rsa_md5",
        "0102": "dsa_md5",
        "0202": "dsa_sha1",
        "0301": "rsa_sha224",
        "0302": "dsa_sha224",
        "0303": "ecdsa_sha224",
        "0402": "dsa_sha256",
        "0502": "dsa_sha384",
        "0602": "dsa_sha512",

        # Brainpool (RFC 8734)
        "081a": "ecdsa_brainpoolP256r1tls13_sha256",
        "081b": "ecdsa_brainpoolP384r1tls13_sha384",
        "081c": "ecdsa_brainpoolP512r1tls13_sha512",

        # SM2 (中国国密)
        "0708": "sm2sig_sm3",

        # GOST (俄罗斯)
        "0709": "gostr34102012_256a",
        "070a": "gostr34102012_256b",
        "070b": "gostr34102012_256c",
        "070c": "gostr34102012_256d",
        "070d": "gostr34102012_512a",
        "070e": "gostr34102012_512b",
        "070f": "gostr34102012_512c",
    }

    # GREASE 值
    GREASE_VALUES = {
        "0a0a", "1a1a", "2a2a", "3a3a", "4a4a",
        "5a5a", "6a6a", "7a7a", "8a8a", "9a9a",
        "aaaa", "baba", "caca", "dada", "eaea", "fafa"
    }

    # ============ 椭圆曲线映射表 ============
    ELLIPTIC_CURVES = {
        "0017": "secp256r1",
        "0018": "secp384r1",
        "0019": "secp521r1",
        "001d": "x25519",
        "001e": "x448",
        "0100": "ffdhe2048",
        "0101": "ffdhe3072",
        "0102": "ffdhe4096",
        "0103": "ffdhe6144",
        "0104": "ffdhe8192",
        # GREASE curves
        "2a2a": "GREASE",
        "1a1a": "GREASE",
    }

    # ============ TLS 版本映射 ============
    TLS_VERSIONS = {
        "0300": "SSL3.0",
        "0301": "TLS1.0",
        "0302": "TLS1.1",
        "0303": "TLS1.2",
        "0304": "TLS1.3",
    }

    # ============ 密码套件映射表 ============
    CIPHER_SUITES = {
        # TLS 1.3
        "1301": "TLS_AES_128_GCM_SHA256",
        "1302": "TLS_AES_256_GCM_SHA384",
        "1303": "TLS_CHACHA20_POLY1305_SHA256",
        "1304": "TLS_AES_128_CCM_SHA256",
        "1305": "TLS_AES_128_CCM_8_SHA256",

        # TLS 1.2 ECDHE
        "c02b": "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
        "c02c": "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
        "c02f": "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        "c030": "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "cca8": "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
        "cca9": "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
        "c013": "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
        "c014": "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
        "c009": "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
        "c00a": "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
        "c027": "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
        "c028": "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
        "c023": "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
        "c024": "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",

        # TLS 1.2 DHE
        "009e": "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
        "009f": "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384",
        "0033": "TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
        "0039": "TLS_DHE_RSA_WITH_AES_256_CBC_SHA",
        "0067": "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256",
        "006b": "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256",
        "ccaa": "TLS_DHE_RSA_WITH_CHACHA20_POLY1305_SHA256",

        # TLS 1.2 RSA
        "009c": "TLS_RSA_WITH_AES_128_GCM_SHA256",
        "009d": "TLS_RSA_WITH_AES_256_GCM_SHA384",
        "002f": "TLS_RSA_WITH_AES_128_CBC_SHA",
        "0035": "TLS_RSA_WITH_AES_256_CBC_SHA",
        "003c": "TLS_RSA_WITH_AES_128_CBC_SHA256",
        "003d": "TLS_RSA_WITH_AES_256_CBC_SHA256",

        # Legacy
        "000a": "TLS_RSA_WITH_3DES_EDE_CBC_SHA",
        "0005": "TLS_RSA_WITH_RC4_128_SHA",
        "0004": "TLS_RSA_WITH_RC4_128_MD5",
    }

    # ============ TLS 扩展映射表 ============
    TLS_EXTENSIONS = {
        "0000": "server_name",
        "0001": "max_fragment_length",
        "0002": "client_certificate_url",
        "0003": "trusted_ca_keys",
        "0004": "truncated_hmac",
        "0005": "status_request",
        "0006": "user_mapping",
        "0007": "client_authz",
        "0008": "server_authz",
        "0009": "cert_type",
        "000a": "supported_groups",
        "000b": "ec_point_formats",
        "000c": "srp",
        "000d": "signature_algorithms",
        "000e": "use_srtp",
        "000f": "heartbeat",
        "0010": "application_layer_protocol_negotiation",
        "0011": "status_request_v2",
        "0012": "signed_certificate_timestamp",
        "0013": "client_certificate_type",
        "0014": "server_certificate_type",
        "0015": "padding",
        "0016": "encrypt_then_mac",
        "0017": "extended_master_secret",
        "0018": "token_binding",
        "0019": "cached_info",
        "001a": "tls_lts",
        "001b": "compress_certificate",
        "001c": "record_size_limit",
        "001d": "pwd_protect",
        "001e": "pwd_clear",
        "001f": "password_salt",
        "0020": "ticket_pinning",
        "0021": "tls_cert_with_extern_psk",
        "0022": "delegated_credentials",
        "0023": "session_ticket",
        "0024": "TLMSP",
        "0025": "TLMSP_proxying",
        "0026": "TLMSP_delegate",
        "0027": "supported_ekt_ciphers",
        "0029": "pre_shared_key",
        "002a": "early_data",
        "002b": "supported_versions",
        "002c": "cookie",
        "002d": "psk_key_exchange_modes",
        "002f": "certificate_authorities",
        "0030": "oid_filters",
        "0031": "post_handshake_auth",
        "0032": "signature_algorithms_cert",
        "0033": "key_share",
        "0034": "transparency_info",
        "0035": "connection_id_deprecated",
        "0036": "connection_id",
        "0037": "external_id_hash",
        "0038": "external_session_id",
        "0039": "quic_transport_parameters",
        "003a": "ticket_request",
        "003b": "dnssec_chain",
        "ff01": "renegotiation_info",

        # 常见私有扩展
        "5500": "token_binding",
        "fe0d": "encrypted_client_hello",  # ECH
        "44cd": "delegated_credentials",
    }

    @classmethod
    def is_grease(cls, value: str) -> bool:
        """判断是否为 GREASE 值"""
        value = value.lower()
        # GREASE 模式: 0x?a?a
        if len(value) == 4:
            return value in cls.GREASE_VALUES or (value[1] == 'a' and value[3] == 'a')
        return False

    @classmethod
    def parse_signature_algorithms(cls, ja4_r: str) -> List[str]:
        """从 ja4_r 解析签名算法列表"""
        if not ja4_r:
            return []

        parts = ja4_r.split("_")
        if len(parts) < 4:
            return []

        sig_algs_hex = parts[-1].split(",")
        result = []

        for code in sig_algs_hex:
            code = code.lower().strip()
            if not code or cls.is_grease(code):
                continue

            alg_name = cls.SIGNATURE_ALGORITHMS.get(code)
            if alg_name:
                result.append(alg_name)

        return result

    @classmethod
    def parse_ja3(cls, ja3_text: str) -> Dict[str, Any]:
        """
        解析 JA3 指纹字符串
        格式: TLSVersion,Ciphers,Extensions,EllipticCurves,EllipticCurvePointFormats
        """
        if not ja3_text:
            return {}

        parts = ja3_text.split(",")
        if len(parts) != 5:
            return {"raw": ja3_text}

        tls_version, ciphers, extensions, curves, point_formats = parts

        return {
            "raw": ja3_text,
            "tls_version": tls_version,
            "tls_version_name": cls.TLS_VERSIONS.get(
                hex(int(tls_version))[2:].zfill(4), tls_version
            ) if tls_version.isdigit() else tls_version,
            "ciphers": ciphers.split("-") if ciphers else [],
            "extensions": extensions.split("-") if extensions else [],
            "elliptic_curves": curves.split("-") if curves else [],
            "point_formats": point_formats.split("-") if point_formats else [],
            "has_grease": any(
                cls.is_grease(c) for c in
                (ciphers.split("-") + extensions.split("-") + curves.split("-"))
                if c
            ),
        }

    @classmethod
    def parse_akamai(cls, akamai_text: str) -> Dict[str, Any]:
        """
        解析 Akamai HTTP/2 指纹
        格式: SETTINGS|WINDOW_UPDATE|PRIORITY|PSEUDO_HEADER_ORDER
        """
        if not akamai_text:
            return {}

        parts = akamai_text.split("|")
        if len(parts) != 4:
            return {"raw": akamai_text}

        settings, window_update, priority, header_order = parts

        # 解析 SETTINGS
        settings_dict = {}
        if settings:
            for item in settings.split(";"):
                if ":" in item:
                    k, v = item.split(":", 1)
                    settings_dict[k] = v

        return {
            "raw": akamai_text,
            "settings": settings_dict,
            "window_update": window_update,
            "priority": priority,
            "header_order": header_order.split(",") if header_order else [],
        }

    @classmethod
    def parse_fingerprint(cls, fingerprint: Dict[str, Any]) -> TLSFingerprint:
        """
        解析浏览器指纹 JSON，返回 curl_cffi 可用的参数
        
        Args:
            fingerprint: 从 tls.browserleaks.com/json 等网站获取的指纹数据
            
        Returns:
            TLSFingerprint 数据类
        """
        result = TLSFingerprint()

        # 提取 JA3
        result.ja3 = fingerprint.get("ja3_text", "")

        # 提取 Akamai
        result.akamai = fingerprint.get("akamai_text", "")

        # 提取 User-Agent
        result.user_agent = fingerprint.get("user_agent", "")

        # 从 ja4_r 解析签名算法
        ja4_r = fingerprint.get("ja4_r", "")
        sig_algs = cls.parse_signature_algorithms(ja4_r)
        if sig_algs:
            result.extra_fp["tls_signature_algorithms"] = sig_algs

        # 检测是否需要 GREASE
        ja3_info = cls.parse_ja3(result.ja3)
        if ja3_info.get("has_grease"):
            result.extra_fp["tls_grease"] = True

        return result


class TLSClient:
    """TLS 客户端封装"""

    def __init__(
        self,
        fingerprint: Optional[Dict[str, Any]] = None,
        ja3: Optional[str] = None,
        akamai: Optional[str] = None,
        extra_fp: Optional[Dict[str, Any]] = None,
        user_agent: Optional[str] = None,
        impersonate: Optional[str] = None,
        proxy: Optional[str] = None,
        timeout: int = 30,
        verify: bool = True,
    ):
        """
        初始化 TLS 客户端
        
        Args:
            fingerprint: 完整的浏览器指纹字典 (来自 tls.browserleaks.com/json)
            ja3: JA3 指纹字符串
            akamai: Akamai HTTP/2 指纹字符串
            extra_fp: 额外的指纹参数
            user_agent: User-Agent 字符串
            impersonate: curl_cffi 内置的浏览器模拟 (如 "chrome120", "edge131")
            proxy: 代理地址 (如 "http://127.0.0.1:7890")
            timeout: 超时时间（秒）
            verify: 是否验证 SSL 证书
        """
        self.proxy = proxy
        self.timeout = timeout
        self.verify = verify
        self.impersonate = impersonate

        # 如果提供了完整指纹，解析它
        if fingerprint:
            parsed = TLSFingerprintParser.parse_fingerprint(fingerprint)
            self.ja3 = ja3 or parsed.ja3
            self.akamai = akamai or parsed.akamai
            self.user_agent = user_agent or parsed.user_agent
            self.extra_fp = {**(parsed.extra_fp or {}), **(extra_fp or {})}
        else:
            self.ja3 = ja3
            self.akamai = akamai
            self.user_agent = user_agent
            self.extra_fp = extra_fp or {}

        # 默认请求头
        self.default_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"macOS"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        }

        if self.user_agent:
            self.default_headers["User-Agent"] = self.user_agent

    def _build_request_kwargs(self, **kwargs) -> Dict[str, Any]:
        """构建请求参数"""
        request_kwargs = {
            "timeout": kwargs.pop("timeout", self.timeout),
            "verify": kwargs.pop("verify", self.verify),
        }

        # 代理
        if self.proxy:
            request_kwargs["proxy"] = self.proxy

        # 浏览器模拟（优先级最高）
        if self.impersonate:
            request_kwargs["impersonate"] = self.impersonate
        else:
            # 使用自定义指纹
            if self.ja3:
                request_kwargs["ja3"] = self.ja3
            if self.akamai:
                request_kwargs["akamai"] = self.akamai
            if self.extra_fp:
                request_kwargs["extra_fp"] = self.extra_fp

        # 合并请求头
        headers = {**self.default_headers}
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))
        request_kwargs["headers"] = headers

        # 其他参数
        request_kwargs.update(kwargs)

        return request_kwargs

    def get(self, url: str, **kwargs) -> requests.Response:
        """GET 请求"""
        return requests.get(url, **self._build_request_kwargs(**kwargs))

    def post(self, url: str, **kwargs) -> requests.Response:
        """POST 请求"""
        return requests.post(url, **self._build_request_kwargs(**kwargs))

    def put(self, url: str, **kwargs) -> requests.Response:
        """PUT 请求"""
        return requests.put(url, **self._build_request_kwargs(**kwargs))

    def delete(self, url: str, **kwargs) -> requests.Response:
        """DELETE 请求"""
        return requests.delete(url, **self._build_request_kwargs(**kwargs))

    def patch(self, url: str, **kwargs) -> requests.Response:
        """PATCH 请求"""
        return requests.patch(url, **self._build_request_kwargs(**kwargs))

    def head(self, url: str, **kwargs) -> requests.Response:
        """HEAD 请求"""
        return requests.head(url, **self._build_request_kwargs(**kwargs))

    def options(self, url: str, **kwargs) -> requests.Response:
        """OPTIONS 请求"""
        return requests.options(url, **self._build_request_kwargs(**kwargs))

    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        """通用请求方法"""
        return requests.request(method, url, **self._build_request_kwargs(**kwargs))

    def test_fingerprint(self, url: str = "https://tls.browserleaks.com/json") -> Dict[str, Any]:
        """测试当前指纹"""
        response = self.get(url)
        return response.json()

    @classmethod
    def from_browserleaks(cls, url: str = "https://tls.browserleaks.com/json", **kwargs) -> "TLSClient":
        """
        从浏览器获取指纹并创建客户端（需要先手动获取指纹）
        
        使用方法:
        1. 在浏览器中打开 https://tls.browserleaks.com/json
        2. 复制返回的 JSON
        3. 传入该 JSON 创建客户端
        """
        raise NotImplementedError(
            "请先在浏览器中访问 https://tls.browserleaks.com/json 获取指纹，"
            "然后使用 TLSClient(fingerprint=your_fingerprint) 创建客户端"
        )


# ============ 便捷函数 ============

def create_client_from_fingerprint(fingerprint: Dict[str, Any], **kwargs) -> TLSClient:
    """从指纹字典创建客户端"""
    return TLSClient(fingerprint=fingerprint, **kwargs)


def create_chrome_client(**kwargs) -> TLSClient:
    """创建 Chrome 浏览器客户端"""
    return TLSClient(impersonate="chrome124", **kwargs)


def create_edge_client(**kwargs) -> TLSClient:
    """创建 Edge 浏览器客户端"""
    return TLSClient(impersonate="edge127", **kwargs)


def create_safari_client(**kwargs) -> TLSClient:
    """创建 Safari 浏览器客户端"""
    return TLSClient(impersonate="safari17_0", **kwargs)


def create_firefox_client(**kwargs) -> TLSClient:
    """创建 Firefox 浏览器客户端"""
    return TLSClient(impersonate="firefox120", **kwargs)


# ============ 使用示例 ============

if __name__ == "__main__":
    # 示例 1: 使用完整的浏览器指纹
    browser_fingerprint = {
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
        "ja4": "t13d1516h2_8daaf6152771_d8a2da3f94cd",
        "ja4_r": "t13d1516h2_002f,0035,009c,009d,1301,1302,1303,c013,c014,c02b,c02c,c02f,c030,cca8,cca9_0005,000a,000b,000d,0012,0017,001b,0023,002b,002d,0033,44cd,fe0d,ff01_0403,0804,0401,0503,0805,0501,0806,0601",
        "ja3_hash": "882c675e0c2d5eca078c85edc6e0dca0",
        "ja3_text": "771,4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53,0-11-16-10-27-35-18-23-51-43-17613-65037-13-65281-45-5,4588-29-23-24,0",
        "akamai_hash": "52d84b11737d980aef856699f885ca86",
        "akamai_text": "1:65536;2:0;4:6291456;6:262144|15663105|0|m,a,s,p"
    }

    print("=" * 60)
    print("示例 1: 使用完整的浏览器指纹")
    print("=" * 60)

    # 解析指纹
    parsed = TLSFingerprintParser.parse_fingerprint(browser_fingerprint)
    print(f"\n解析结果:")
    print(f"  JA3: {parsed.ja3[:50]}...")
    print(f"  Akamai: {parsed.akamai}")
    print(f"  User-Agent: {parsed.user_agent[:50]}...")
    print(f"  签名算法: {parsed.extra_fp.get('tls_signature_algorithms', [])}")

    # 创建客户端并测试
    client = TLSClient(fingerprint=browser_fingerprint)
    print(f"\n测试指纹...")
    result = client.test_fingerprint()
    print(f"  返回的 JA3 Hash: {result.get('ja3_hash')}")
    print(f"  原始 JA3 Hash: {browser_fingerprint.get('ja3_hash')}")
    print(
        f"  匹配: {result.get('ja3_hash') == browser_fingerprint.get('ja3_hash')}")

    # 示例 2: 使用内置浏览器模拟
    print("\n" + "=" * 60)
    print("示例 2: 使用内置浏览器模拟")
    print("=" * 60)

    chrome_client = create_chrome_client()
    result = chrome_client.test_fingerprint()
    print(f"\n  Chrome JA3 Hash: {result.get('ja3_hash')}")
    print(f"  User-Agent: {result.get('user_agent', '')[:60]}...")

    # 示例 3: 解析 JA3 详情
    print("\n" + "=" * 60)
    print("示例 3: 解析 JA3 详情")
    print("=" * 60)

    ja3_info = TLSFingerprintParser.parse_ja3(browser_fingerprint["ja3_text"])
    print(f"\n  TLS 版本: {ja3_info.get('tls_version_name')}")
    print(f"  密码套件数量: {len(ja3_info.get('ciphers', []))}")
    print(f"  扩展数量: {len(ja3_info.get('extensions', []))}")
    print(f"  椭圆曲线: {ja3_info.get('elliptic_curves', [])}")
    print(f"  包含 GREASE: {ja3_info.get('has_grease')}")

    # 示例 4: 解析 Akamai 详情
    print("\n" + "=" * 60)
    print("示例 4: 解析 Akamai 详情")
    print("=" * 60)

    akamai_info = TLSFingerprintParser.parse_akamai(
        browser_fingerprint["akamai_text"])
    print(f"\n  HTTP/2 Settings: {akamai_info.get('settings')}")
    print(f"  Window Update: {akamai_info.get('window_update')}")
    print(f"  Header Order: {akamai_info.get('header_order')}")
