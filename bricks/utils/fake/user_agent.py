# -*- coding: utf-8 -*-
# @Time    : 2023-12-06 16:48
# @Author  : Kem
# @Desc    :
import random
import re
import string
from datetime import datetime, timedelta
from typing import List, Optional, Union

_re_hash = re.compile(r"#")
_re_perc = re.compile(r"%")
_re_excl = re.compile(r"!")
_re_at = re.compile(r"@")
_re_qm = re.compile(r"\?")
_re_cir = re.compile(r"\^")

language_locale_codes = {
    "aa": ("DJ", "ER", "ET"),
    "af": ("ZA",),
    "ak": ("GH",),
    "am": ("ET",),
    "an": ("ES",),
    "apn": ("IN",),
    "ar": (
        "AE",
        "BH",
        "DJ",
        "DZ",
        "EG",
        "EH",
        "ER",
        "IL",
        "IN",
        "IQ",
        "JO",
        "KM",
        "KW",
        "LB",
        "LY",
        "MA",
        "MR",
        "OM",
        "PS",
        "QA",
        "SA",
        "SD",
        "SO",
        "SS",
        "SY",
        "TD",
        "TN",
        "YE",
    ),
    "as": ("IN",),
    "ast": ("ES",),
    "ayc": ("PE",),
    "az": ("AZ", "IN"),
    "be": ("BY",),
    "bem": ("ZM",),
    "ber": ("DZ", "MA"),
    "bg": ("BG",),
    "bhb": ("IN",),
    "bho": ("IN",),
    "bn": ("BD", "IN"),
    "bo": ("CN", "IN"),
    "br": ("FR",),
    "brx": ("IN",),
    "bs": ("BA",),
    "byn": ("ER",),
    "ca": ("AD", "ES", "FR", "IT"),
    "ce": ("RU",),
    "ckb": ("IQ",),
    "cmn": ("TW",),
    "crh": ("UA",),
    "cs": ("CZ",),
    "csb": ("PL",),
    "cv": ("RU",),
    "cy": ("GB",),
    "da": ("DK",),
    "de": ("AT", "BE", "CH", "DE", "LI", "LU"),
    "doi": ("IN",),
    "dv": ("MV",),
    "dz": ("BT",),
    "el": ("GR", "CY"),
    "en": (
        "AG",
        "AU",
        "BW",
        "CA",
        "DK",
        "GB",
        "HK",
        "IE",
        "IN",
        "NG",
        "NZ",
        "PH",
        "SG",
        "US",
        "ZA",
        "ZM",
        "ZW",
    ),
    "eo": ("US",),
    "es": (
        "AR",
        "BO",
        "CL",
        "CO",
        "CR",
        "CU",
        "DO",
        "EC",
        "ES",
        "GT",
        "HN",
        "MX",
        "NI",
        "PA",
        "PE",
        "PR",
        "PY",
        "SV",
        "US",
        "UY",
        "VE",
    ),
    "et": ("EE",),
    "eu": ("ES", "FR"),
    "fa": ("IR",),
    "ff": ("SN",),
    "fi": ("FI",),
    "fil": ("PH",),
    "fo": ("FO",),
    "fr": ("CA", "CH", "FR", "LU"),
    "fur": ("IT",),
    "fy": ("NL", "DE"),
    "ga": ("IE",),
    "gd": ("GB",),
    "gez": ("ER", "ET"),
    "gl": ("ES",),
    "gu": ("IN",),
    "gv": ("GB",),
    "ha": ("NG",),
    "hak": ("TW",),
    "he": ("IL",),
    "hi": ("IN",),
    "hne": ("IN",),
    "hr": ("HR",),
    "hsb": ("DE",),
    "ht": ("HT",),
    "hu": ("HU",),
    "hy": ("AM",),
    "ia": ("FR",),
    "id": ("ID",),
    "ig": ("NG",),
    "ik": ("CA",),
    "is": ("IS",),
    "it": ("CH", "IT"),
    "iu": ("CA",),
    "iw": ("IL",),
    "ja": ("JP",),
    "ka": ("GE",),
    "kk": ("KZ",),
    "kl": ("GL",),
    "km": ("KH",),
    "kn": ("IN",),
    "ko": ("KR",),
    "kok": ("IN",),
    "ks": ("IN",),
    "ku": ("TR",),
    "kw": ("GB",),
    "ky": ("KG",),
    "lb": ("LU",),
    "lg": ("UG",),
    "li": ("BE", "NL"),
    "lij": ("IT",),
    "ln": ("CD",),
    "lo": ("LA",),
    "lt": ("LT",),
    "lv": ("LV",),
    "lzh": ("TW",),
    "mag": ("IN",),
    "mai": ("IN",),
    "mg": ("MG",),
    "mhr": ("RU",),
    "mi": ("NZ",),
    "mk": ("MK",),
    "ml": ("IN",),
    "mn": ("MN",),
    "mni": ("IN",),
    "mr": ("IN",),
    "ms": ("MY",),
    "mt": ("MT",),
    "my": ("MM",),
    "nan": ("TW",),
    "nb": ("NO",),
    "nds": ("DE", "NL"),
    "ne": ("NP",),
    "nhn": ("MX",),
    "niu": ("NU", "NZ"),
    "nl": ("AW", "BE", "NL"),
    "nn": ("NO",),
    "nr": ("ZA",),
    "nso": ("ZA",),
    "oc": ("FR",),
    "om": ("ET", "KE"),
    "or": ("IN",),
    "os": ("RU",),
    "pa": ("IN", "PK"),
    "pap": ("AN", "AW", "CW"),
    "pl": ("PL",),
    "ps": ("AF",),
    "pt": ("BR", "PT"),
    "quz": ("PE",),
    "raj": ("IN",),
    "ro": ("RO",),
    "ru": ("RU", "UA"),
    "rw": ("RW",),
    "sa": ("IN",),
    "sat": ("IN",),
    "sc": ("IT",),
    "sd": ("IN", "PK"),
    "se": ("NO",),
    "shs": ("CA",),
    "si": ("LK",),
    "sid": ("ET",),
    "sk": ("SK",),
    "sl": ("SI",),
    "so": ("DJ", "ET", "KE", "SO"),
    "sq": ("AL", "ML"),
    "sr": ("ME", "RS"),
    "ss": ("ZA",),
    "st": ("ZA",),
    "sv": ("FI", "SE"),
    "sw": ("KE", "TZ"),
    "szl": ("PL",),
    "ta": ("IN", "LK"),
    "tcy": ("IN",),
    "te": ("IN",),
    "tg": ("TJ",),
    "th": ("TH",),
    "the": ("NP",),
    "ti": ("ER", "ET"),
    "tig": ("ER",),
    "tk": ("TM",),
    "tl": ("PH",),
    "tn": ("ZA",),
    "tr": ("CY", "TR"),
    "ts": ("ZA",),
    "tt": ("RU",),
    "ug": ("CN",),
    "uk": ("UA",),
    "unm": ("US",),
    "ur": ("IN", "PK"),
    "uz": ("UZ",),
    "ve": ("ZA",),
    "vi": ("VN",),
    "wa": ("BE",),
    "wae": ("CH",),
    "wal": ("ET",),
    "wo": ("SN",),
    "xh": ("ZA",),
    "yi": ("US",),
    "yo": ("NG",),
    "yue": ("HK",),
    "zh": ("CN", "HK", "SG", "TW"),
    "zu": ("ZA",),
}


def numerify(text="###"):
    text = _re_hash.sub(lambda x: str(random.randint(0, 9)), text)
    text = _re_perc.sub(lambda x: str(random.randint(1, 9)), text)
    text = _re_excl.sub(
        lambda x: str(random.randint(0, 1)
                      and random.randint(0, 9) or ""), text
    )
    text = _re_at.sub(
        lambda x: str(random.randint(0, 1)
                      and random.randint(1, 9) or ""), text
    )
    return text


def _choice(elements):
    """普通随机选择"""
    return random.choice(elements)


def _weighted_choice(choices):
    """加权随机选择

    :param choices: 元组列表，格式为 [(value, weight), ...]
                   或直接传入值列表（等权重）
    :return: 随机选择的值
    """
    if not choices:
        raise ValueError("choices cannot be empty")

    # 如果是普通列表/元组，直接使用 random.choice
    if not isinstance(choices[0], (tuple, list)):
        return random.choice(choices)

    # 如果是加权列表
    values = [c[0] for c in choices]
    weights = [c[1] for c in choices]
    return random.choices(values, weights=weights, k=1)[0]


def randomtimes(start, end, frmt="%Y-%m-%d %H:%M:%S"):
    stime = datetime.strptime(start, frmt)
    etime = datetime.strptime(end, frmt)
    return (random.random() * (etime - stime) + stime).strftime(frmt)


def locale():
    """Generate a random underscored i18n locale code (e.g. en_US).

    :sample:
    """
    language_code = _choice(list(language_locale_codes.keys()))
    return (
        language_code
        + "_"
        + _choice(
            language_locale_codes[language_code],
        )
    )


user_agents = (
    "chrome",
    "firefox",
    "internet_explorer",
    "opera",
    "safari",
    "wechat",
    "edge",
)

# 格式: (value, weight) - weight 越大，出现概率越高
windows_platform_tokens = [
    ("Windows NT 10.0; Win64; x64", 50),  # 64位 Windows 10 (最常见)
    ("Windows NT 11.0; Win64; x64", 45),  # 64位 Windows 11 (逐渐普及)
    ("Windows NT 10.0; WOW64", 5),  # 32位应用在64位 Windows 10（少见）
]

linux_processors = [
    ("x86_64", 85),  # 64 位 Intel/AMD 架构（最常见）
    ("aarch64", 15),  # 64 位 ARM 架构（如树莓派 4、服务器）
]

mac_processors = [
    ("Intel", 60),  # Intel 处理器的 Mac（仍有一定占比）
    ("Intel Mac OS X", 40),  # 更标准的 Intel Mac 标识
]

android_versions = [
    ("10", 5),   # Android 10 (较少)
    ("11", 10),  # Android 11
    ("12", 20),  # Android 12
    ("13", 30),  # Android 13 (常见)
    ("14", 30),  # Android 14 (常见)
    ("15", 5),   # Android 15 (最新)
]

apple_devices = ("iPhone", "iPad")

ios_versions = [
    # iOS 版本权重分布：新版本权重更高
    ("15.7", 3),
    ("15.8", 2),
    ("16.0", 2),
    ("16.1", 3),
    ("16.2", 3),
    ("16.3", 3),
    ("16.4", 4),
    ("16.5", 5),
    ("16.6", 5),
    ("16.7", 6),
    ("16.7.2", 4),
    ("17.0", 3),
    ("17.1", 4),
    ("17.2", 5),
    ("17.3", 6),
    ("17.4", 8),
    ("17.5", 10),
    ("17.6", 12),
    ("17.6.1", 8),
    ("18.0", 5),
    ("18.1", 8),
    ("18.1.1", 6),
    ("18.2", 4),
]

mac_versions = [
    # macOS 版本权重分布
    ("10.15.7", 2),  # Catalina (最后一个 10.x)
    ("11.7", 2),
    ("11.7.2", 2),  # Big Sur
    ("12.6", 4),
    ("12.7", 5),
    ("12.7.1", 3),  # Monterey
    ("13.5", 6),
    ("13.6", 8),
    ("13.6.2", 6),
    ("13.7", 5),  # Ventura
    ("14.3", 8),
    ("14.4", 10),
    ("14.5", 12),
    ("14.6", 10),
    ("14.7", 8),  # Sonoma
    ("15.0", 4),
    ("15.1", 6),
    ("15.1.1", 4),
    ("15.2", 3),  # Sequoia
]

dalvik_versions = (
    "1.1",
    "1.2",
    "1.4",
    "1.5",
    "1.6",
    "2.0",
    "2.1",
)

phone_models = [
    # 权重根据市场占有率和新旧程度分配
    # Apple iPhone 系列 (高端市场主导)
    ("iPhone 16,1", 10),  # iPhone 15 Pro
    ("iPhone 16,2", 12),  # iPhone 15 Pro Max
    ("iPhone 15,2", 8),   # iPhone 14 Pro
    ("iPhone 15,3", 9),   # iPhone 14 Pro Max
    ("iPhone 14,2", 6),   # iPhone 13 Pro
    ("iPhone 14,3", 7),   # iPhone 13 Pro Max
    ("iPhone 13,2", 5),   # iPhone 12
    ("iPhone 13,3", 4),   # iPhone 12 Pro
    # Samsung Galaxy 系列 (全球占有率最高)
    ("SM-S928B", 8),   # Galaxy S24 Ultra
    ("SM-S926B", 7),   # Galaxy S24+
    ("SM-S921B", 9),   # Galaxy S24
    ("SM-S918B", 6),   # Galaxy S23 Ultra
    ("SM-S916B", 5),   # Galaxy S23+
    ("SM-S911B", 7),   # Galaxy S23
    ("SM-A546B", 12),  # Galaxy A54 (中端畅销)
    ("SM-A536B", 10),  # Galaxy A53
    ("SM-G991B", 4),   # Galaxy S21
    # Xiaomi 系列 (中国和印度市场主力)
    ("23078PND5G", 6),  # Xiaomi 14 Pro
    ("23049PCD8G", 8),  # Xiaomi 14
    ("2311DRK48C", 5),  # Xiaomi 13 Ultra
    ("2210132C", 6),    # Xiaomi 13 Pro
    ("2211133C", 7),    # Xiaomi 13
    ("2203121C", 5),    # Xiaomi 12 Pro
    ("23124RN87C", 10),  # Redmi Note 13 Pro (性价比之王)
    ("23090RA98C", 8),  # Redmi Note 12 Pro+
    ("23013RK75C", 4),  # Poco F5 Pro
    # Huawei 系列 (国内高端市场)
    ("HMA-AL00", 4),  # Mate 60 Pro
    ("NOH-AN00", 5),  # Mate 50 Pro
    ("ELS-AN00", 3),  # P60 Pro
    ("ALN-AL10", 3),  # P50 Pro
    # Google Pixel 系列
    ("Pixel 9 Pro", 3),
    ("Pixel 9", 4),
    ("Pixel 8 Pro", 3),
    ("Pixel 8", 5),
    ("Pixel 8a", 4),
    ("Pixel 7a", 3),
    # OnePlus 系列
    ("CPH2581", 4),  # OnePlus 12
    ("CPH2451", 5),  # OnePlus 11
    ("CPH2449", 3),  # OnePlus 10 Pro
    # Oppo 系列
    ("PHT110", 3),   # Find X7 Ultra
    ("PJD110", 4),   # Find X7 Pro
    ("PHN110", 3),   # Find X6 Pro
    # Vivo 系列
    ("V2309A", 4),   # X100 Pro
    ("V2285A", 3),   # X90 Pro+
    ("V2227A", 4),   # X90 Pro
    # Realme 系列
    ("RMX3890", 3),  # GT5 Pro
    ("RMX3851", 4),  # GT5
    # Honor 系列
    ("PGT-AN10", 3),  # Magic 6 Pro
    ("LGE-AN00", 4),  # Magic 5 Pro
]

nettypes = (
    "5G",
    "4G",
    "WIFI",
    "3G",
    "2G",
)


def windows_platform_token():
    """
    生成 Windows 平台令牌用于 user-agent

    :return:
    """
    return _weighted_choice(windows_platform_tokens)


def mac_platform_token():
    """
    生成 Mac 平台令牌用于 user-agent

    :return:
    """
    processor = _weighted_choice(mac_processors)
    mac_ver = _weighted_choice(mac_versions).replace(".", "_")
    # 如果 processor 已经包含 "Mac OS X"，直接使用；否则添加格式
    if "Mac OS X" in processor:
        return "Macintosh; {} {}".format(processor, mac_ver)
    else:
        return "Macintosh; {} Mac OS X {}".format(processor, mac_ver)


def linux_platform_token():
    """
    生成 Linux 平台令牌用于 user-agent

    :return:
    """
    return "X11; Linux {}".format(_weighted_choice(linux_processors))


def android_platform_token():
    """
    生成 Android 平台令牌用于 user-agent

    :return:
    """
    return "Android {}".format(_weighted_choice(android_versions))


def _android_build_id():
    # 生成类似于 TP1A.220624.014 的 Build ID
    letters = "TPQRSUVA"  # 常见前缀字母
    prefix = (
        random.choice(list(letters))
        + random.choice(list(letters))
        + str(random.randint(1, 9))
        + random.choice(["A", "B"])
    )
    y = random.choice(["21", "22", "23", "24", "25"])  # 2021-2025
    m = random.choice(
        ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    )
    d = random.choice(["01", "05", "10", "15", "20", "24", "30"])  # 常见补丁日
    patch = f"{y}{m}{d}"
    tail = f"{random.randint(1, 999):03d}"
    return f"{prefix}.{patch}.{tail}"


def android_platform_token_full():
    """
    更真实的 Android 平台 token，包含 Linux; Android 版本; 机型 Build/ID
    例如: Linux; Android 13; SM-G991B Build/TP1A.220624.014
    """
    version = _weighted_choice(android_versions)
    model = _weighted_choice(phone_models)
    return f"Linux; Android {version}; {model} Build/{_android_build_id()}"


def safari_webkit_version():
    """返回更真实的 Safari WebKit 版本（现代版本）"""
    # 2024-2025 年的 WebKit 版本主要在 615-619 之间，新版本权重更高
    versions = [
        ("615.1.26", 3),
        ("616.1.27", 5),        # Safari 17.2+
        ("617.1.15", 8),        # Safari 17.4+
        ("617.1.15.11.12", 6),
        ("617.2.6", 5),
        ("618.1.15", 15),       # Safari 18.x (最常见)
        ("618.1.15.11.14", 12),
        ("618.2.12", 10),
        ("619.1.1", 8),         # Safari 18.2+
    ]
    return _weighted_choice(versions)


def ios_platform_token():
    """
    生成 IOS 平台令牌用于 user-agent

    :return:
    """
    device = _choice(apple_devices)
    ios_ver = _weighted_choice(ios_versions).replace(".", "_")

    # iPhone 和 iPad 的格式略有不同
    if device == "iPhone":
        return "{0}; CPU {0} OS {1} like Mac OS X".format(device, ios_ver)
    else:  # iPad
        return "{0}; CPU OS {1} like Mac OS X".format(device, ios_ver)


def wechat_platform_token():
    """
    生成 WeChat 平台令牌用于 user-agent

    :return:
    """
    # 微信版本主要在 8.0.x 版本段
    wechat_versions = [
        ("8.0.49", 8),
        ("8.0.50", 10),
        ("8.0.51", 6),
        ("8.0.52", 4),
    ]
    version = _weighted_choice(wechat_versions)
    return f"MicroMessenger/{version} NetType/{_choice(nettypes)}  Language/{_choice(['en', 'zh_CN'])}"


def android(
    android_version_from="11",
    brand: Optional[Union[str, List[str]]] = None,
    prefix: str = "Dalvik",
):
    """
    生成 dalvik 平台令牌用于 user-agent

    :return:
    """
    dalvik_version = f"{random.randint(1, 2)}.{random.randint(0, 9)}"
    # 过滤出符合条件的版本（带权重）
    valid_versions = [(v, w)
                      for v, w in android_versions if v >= android_version_from]
    android_version = _weighted_choice(
        valid_versions) if valid_versions else android_version_from
    if brand and not isinstance(brand, list):
        brand = [brand]
    brand = _choice(brand) if brand else _weighted_choice(phone_models)
    return f"{prefix}/{dalvik_version} (Linux; U; Android {android_version}; {brand} Build/{''.join(random.choices(string.hexdigits, k=4))}.{randomtimes('20000101', datetime.today().strftime('%Y%m%d'), '%Y%m%d')}.{numerify()})"


def internet_explorer():
    """
    生成 internet_explorer 的 user-agent

    警告: IE 已于 2022 年退役，生成 IE UA 可能增加被识别风险。
    建议使用 Edge 或 Chrome 代替。

    :return:
    """
    tmplt = "Mozilla/5.0 (compatible; MSIE {0}.0; {1}; Trident/{2}.{3})"
    return tmplt.format(
        random.randint(8, 10),
        windows_platform_token(),
        random.randint(5, 7),
        random.randint(0, 1),
    )


def opera(
    device="all",
    version_from=100,
    version_end=110,
    chrome_version_from=120,
    chrome_version_end=132,
):
    """
    生成 opera 的 user-agent

    :param version_end:
    :param version_from:
    :param chrome_version_end:
    :param chrome_version_from:
    :param device: 设备类型, 可选: all/mobile/pc
    :return:
    """
    bld = _re_qm.sub(lambda x: _choice(
        string.ascii_letters), numerify("##?###"))

    opera_version = f"{random.randint(version_from, version_end)}.0.{random.randint(1000, 9999)}.{random.randint(0, 999)}"

    tmp_pc = "({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko) Chrome/{chrome_ver} Safari/{saf_version} OPR/{opera_ver}"
    opera_mobile = f"Opera/{random.randint(9, 12)}.{random.randint(10, 99)} ({_choice([android_platform_token(), ios_platform_token()])}; Opera Mobile/{bld}; U; {locale().replace('_', '-')}) Presto/{opera_version} Version/{random.randint(10, 12)}.02"
    saf_version = f"{random.randint(531, 537)}.{random.randint(0, 36)}"
    _v = random.randint(chrome_version_from, chrome_version_end)
    # 优化版本号生成
    build_num = random.randint(5000, 6999)
    patch_num = random.randint(0, 200)
    chrome_version = f"{_v}.0.{build_num}.{patch_num}"

    # Opera 现在基于 Chromium，不再生成 Presto 引擎的 UA
    platforms_pc = [
        tmp_pc.format(
            platform=windows_platform_token(),
            saf_version=saf_version,
            chrome_ver=chrome_version,
            opera_ver=opera_version,
        ),
        tmp_pc.format(
            platform=mac_platform_token(),
            saf_version=saf_version,
            chrome_ver=chrome_version,
            opera_ver=opera_version,
        ),
        tmp_pc.format(
            platform=linux_platform_token(),
            saf_version=saf_version,
            chrome_ver=chrome_version,
            opera_ver=opera_version,
        ),
    ]
    platforms_mobile = [opera_mobile]
    platforms_all = [*platforms_mobile, *platforms_pc]
    return "Mozilla/5.0 " + _choice(locals()[f"platforms_{device}"])


def chrome(device="all", version_from=120, version_end=132, webview=False):
    """
    生成 chrome 的 user-agent

    :param version_end:
    :param version_from: 最小版本号
    :param device: 设备类型, 可选: all/mobile/pc
    :param webview: 是否生成 WebView UA (仅 Android)
    :return:
    """

    # Android Chrome 可以是普通浏览器或 WebView
    tmp_android = "({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko) Chrome/{version} Mobile Safari/{saf_version}"
    tmp_android_wv = "({platform}; wv) AppleWebKit/{saf_version} (KHTML, like Gecko) Version/4.0 Chrome/{version} Mobile Safari/{saf_version}"
    tmp_iphone = "({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko) CriOS/{version} Mobile/{bld} Safari/{saf_version}"
    tmp_normal = "({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko) Chrome/{version} Safari/{saf_version}"
    saf_version = "537.36"
    bld = _re_qm.sub(lambda x: _choice(
        string.ascii_letters), numerify("##?###"))

    _v = random.randint(version_from, version_end)
    # 优化算法：第三位为 4位数，第四位为 2-3 位数
    build_num = random.randint(5000, 6999)  # Chrome 典型的 build 号段
    patch_num = random.randint(0, 200)       # patch 号通常较小
    version = f"{_v}.0.{build_num}.{patch_num}"

    # 根据 webview 参数或随机选择是否使用 WebView 格式
    use_webview = webview or (
        device in ["mobile", "all"] and random.random() < 0.15)  # 15% 概率生成 WebView UA
    android_template = tmp_android_wv if use_webview else tmp_android

    platforms_mobile = [
        android_template.format(
            platform=android_platform_token_full(),
            saf_version=saf_version,
            version=version,
        ),
        tmp_iphone.format(
            platform=ios_platform_token(),
            saf_version=saf_version,
            version=version,
            bld=bld,
        ),
    ]
    platforms_pc = [
        tmp_normal.format(
            platform=windows_platform_token(),
            saf_version=saf_version,
            version=version,
        ),
        tmp_normal.format(
            platform=mac_platform_token(),
            saf_version=saf_version,
            version=version,
        ),
        tmp_normal.format(
            platform=linux_platform_token(),
            saf_version=saf_version,
            version=version,
        ),
    ]
    platforms_all = [*platforms_mobile, *platforms_pc]
    return "Mozilla/5.0 " + _choice(locals()[f"platforms_{device}"])


def firefox(device="all", version_from=120, version_end=135):
    """
    生成 firefox 的 user-agent

    :param version_end: 版本开始
    :param version_from: 版本结束
    :param device: 设备类型, 可选: all/mobile/pc
    :return:
    """

    tmp_android = "({platform}; Mobile; rv:{version}) Gecko/{version} Firefox/{version}"
    # PC 版不应该有 Mobile
    tmp_pc = "({platform}; rv:{version}) Gecko/{ver} Firefox/{version}"
    tmp_iphone = "({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko) FxiOS/{version} Mobile/{bld} Safari/{saf_version}"
    saf_version = f"{random.randint(531, 537)}.{random.randint(0, 36)}"
    bld = _re_qm.sub(lambda x: _choice(
        string.ascii_letters), numerify("##?###"))
    ver = (
        datetime.today()
        - timedelta(
            days=random.randint(
                0, (datetime.today() - datetime(2018, 1, 1)).days)
        )
    ).strftime("%Y%m%d")

    version = f"{random.randint(version_from, version_end)}.{random.randint(0, 3)}.{random.randint(0, 3)}"

    platforms_mobile = [
        tmp_android.format(
            platform=android_platform_token_full(), version=version),
        tmp_iphone.format(
            platform=ios_platform_token(),
            saf_version=saf_version,
            version=version,
            bld=bld,
        ),
    ]
    platforms_pc = [
        tmp_pc.format(
            platform=windows_platform_token(),
            ver=ver,
            version=version,
        ),
        tmp_pc.format(
            platform=mac_platform_token(),
            ver=ver,
            version=version,
        ),
        tmp_pc.format(
            platform=linux_platform_token(),
            ver=ver,
            version=version,
        ),
    ]
    platforms_all = [*platforms_mobile, *platforms_pc]
    return "Mozilla/5.0 " + _choice(locals()[f"platforms_{device}"])


def edge(device="all", version_from=120, version_end=132):
    """
    生成 edge 的 user-agent

    :param version_end:
    :param version_from:
    :param device: 设备类型, 可选: all/mobile/pc
    :return:
    """
    _v = random.randint(version_from, version_end)
    version = f"{_v}.0.{int(_v * (49 + random.random()))}.{random.randint(0, 100)}"

    return (
        chrome(device=device, version_from=version_from, version_end=version_end)
        + f" Edg/{version}"
    )


def safari(device="all"):
    """
    生成 safari 的 user-agent

    :param device: 设备类型, 可选: all/mobile/pc
    :return:
    """
    bld = _re_qm.sub(lambda x: _choice(
        string.ascii_letters), numerify("##?###"))
    saf_version = safari_webkit_version()

    # Safari 版本号应该与系统版本匹配
    # macOS 14.x -> Safari 17.x, macOS 15.x -> Safari 18.x
    # iOS 17.x -> Safari 17.x, iOS 18.x -> Safari 18.x
    def get_safari_version(os_version):
        """根据系统版本获取对应的 Safari 版本"""
        major_version = int(os_version.split('.')[0])
        if major_version >= 18:  # iOS 18 or macOS 15
            return f"18.{random.randint(0, 2)}"
        # iOS 17 or macOS 14
        elif major_version >= 17 or (major_version >= 14 and '.' in os_version):
            return f"17.{random.randint(0, 6)}"
        # iOS 16 or macOS 13
        elif major_version >= 16 or (major_version >= 13 and '.' in os_version):
            return f"16.{random.randint(0, 6)}"
        # iOS 15 or macOS 12
        elif major_version >= 15 or (major_version >= 12 and '.' in os_version):
            return f"15.{random.randint(0, 6)}"
        else:
            return f"{random.randint(14, 18)}.{random.randint(0, 6)}"

    tmp_mac = "({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko) Version/{safari_version} Safari/{saf_version}"
    tmp_ios = "({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko) Version/{safari_version} Mobile/{bld} Safari/{saf_version}"

    # 为 mobile 生成
    ios_ver = _weighted_choice(ios_versions)
    platforms_mobile = [
        tmp_ios.format(
            platform=ios_platform_token(),
            saf_version=saf_version,
            safari_version=get_safari_version(ios_ver),
            bld=bld,
        ),
    ]

    # 为 PC 生成
    mac_ver = _weighted_choice(mac_versions)
    platforms_pc = [
        tmp_mac.format(
            platform=mac_platform_token(),
            saf_version=saf_version,
            safari_version=get_safari_version(mac_ver),
        ),
    ]
    platforms_all = [*platforms_mobile, *platforms_pc]
    return "Mozilla/5.0 " + _choice(locals()[f"platforms_{device}"])


def wechat(device="all", chrome_version_from=120, chrome_version_end=132):
    """
    生成 wechat 的 user-agent

    :param chrome_version_end:
    :param chrome_version_from:
    :param device: 设备类型, 可选: all/mobile/pc
    :return:
    """
    saf_version = "537.36"
    bld = _re_qm.sub(lambda x: _choice(
        string.ascii_letters), numerify("##?###"))
    tmplt = (
        "({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko)"
        " Chrome/{chrome_ver} Safari/{saf_version}"
    )
    tmplt_ios = (
        "({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko)"
        " CriOS/{chrome_ver} Mobile/{bld} Safari/{saf_version}"
    )
    tmp_xcx = "({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko) Mobile/{bld}"
    _v = random.randint(chrome_version_from, chrome_version_end)
    # 优化版本号生成
    build_num = random.randint(5000, 6999)
    patch_num = random.randint(0, 200)
    chrome_version = f"{_v}.0.{build_num}.{patch_num}"

    platforms_pc = [
        tmplt.format(
            platform=windows_platform_token(),
            saf_version=saf_version,
            chrome_ver=chrome_version,
        )
        + " "
        + wechat_platform_token(),
        tmplt.format(
            platform=mac_platform_token(),
            saf_version=saf_version,
            chrome_ver=chrome_version,
        )
        + " "
        + wechat_platform_token(),
    ]
    platforms_mobile = [
        tmplt.format(
            platform="Linux; {}".format(android_platform_token()),
            saf_version=saf_version,
            chrome_ver=chrome_version,
        )
        + " "
        + wechat_platform_token(),
        tmplt_ios.format(
            platform=ios_platform_token(),
            saf_version=saf_version,
            chrome_ver=chrome_version,
            bld=bld,
        )
        + " "
        + wechat_platform_token(),
        tmp_xcx.format(platform=ios_platform_token(),
                       saf_version=saf_version, bld=bld)
        + f" {wechat_platform_token()} Branch/Br_trunk MiniProgramEnv/Mac",
    ]
    platforms_all = [*platforms_mobile, *platforms_pc]
    return "Mozilla/5.0 " + _choice(locals()[f"platforms_{device}"])


def mobile(*apps):
    """
    生成 mobile 的 user-agent

    :param apps: app 类型, 可选: 'chrome', 'firefox', 'internet_explorer', 'opera', 'safari', 'wechat', 'edge'
    :return:
    """
    while True:
        ua = get(*apps)
        if any(
            [
                ua.lower().__contains__("mobile"),
                ua.lower().__contains__("android"),
                ua.lower().__contains__("iphone"),
                ua.lower().__contains__("ipad"),
            ]
        ):
            return ua
        else:
            pass


def pc(*apps):
    """
    生成 pc 的 user-agent

    :param apps: app 类型, 可选: 'chrome', 'firefox', 'internet_explorer', 'opera', 'safari', 'wechat', 'edge'
    :return:
    """

    while True:
        ua = get(*apps)
        if not any(
            [
                ua.lower().__contains__("mobile"),
                ua.lower().__contains__("android"),
                ua.lower().__contains__("iphone"),
                ua.lower().__contains__("ipad"),
            ]
        ):
            return ua
        else:
            pass


def get(*apps):
    """
    随机生成一个 user-agent

    :param apps: app 类型, 可选: 'chrome', 'firefox', 'internet_explorer', 'opera', 'safari', 'wechat', 'edge'
    :return:
    """
    name = _choice(apps or user_agents)
    return globals()[name]()


if __name__ == "__main__":
    # 简单的样例输出，便于快速检查格式
    for fn in [chrome, firefox, safari, edge, opera, wechat, android, pc, mobile]:
        try:
            print(fn.__name__, "->", str(fn())[:180])
        except Exception as e:
            print(fn.__name__, "error:", e)
