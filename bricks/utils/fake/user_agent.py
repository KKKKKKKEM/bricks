# -*- coding: utf-8 -*-
# @Time    : 2023-12-06 16:48
# @Author  : Kem
# @Desc    :
import random
import re
import string
from datetime import datetime, timedelta

_re_hash = re.compile(r'#')
_re_perc = re.compile(r'%')
_re_excl = re.compile(r'!')
_re_at = re.compile(r'@')
_re_qm = re.compile(r'\?')
_re_cir = re.compile(r'\^')

language_locale_codes = {
    'aa': ('DJ', 'ER', 'ET'), 'af': ('ZA',), 'ak': ('GH',), 'am': ('ET',),
    'an': ('ES',), 'apn': ('IN',),
    'ar': ('AE', 'BH', 'DJ', 'DZ', 'EG', 'EH', 'ER', 'IL', 'IN',
           'IQ', 'JO', 'KM', 'KW', 'LB', 'LY', 'MA', 'MR', 'OM',
           'PS', 'QA', 'SA', 'SD', 'SO', 'SS', 'SY', 'TD', 'TN',
           'YE'),
    'as': ('IN',), 'ast': ('ES',), 'ayc': ('PE',), 'az': ('AZ', 'IN'),
    'be': ('BY',), 'bem': ('ZM',), 'ber': ('DZ', 'MA'), 'bg': ('BG',),
    'bhb': ('IN',), 'bho': ('IN',), 'bn': ('BD', 'IN'), 'bo': ('CN', 'IN'),
    'br': ('FR',), 'brx': ('IN',), 'bs': ('BA',), 'byn': ('ER',),
    'ca': ('AD', 'ES', 'FR', 'IT'), 'ce': ('RU',), 'ckb': ('IQ',),
    'cmn': ('TW',), 'crh': ('UA',), 'cs': ('CZ',), 'csb': ('PL',),
    'cv': ('RU',), 'cy': ('GB',), 'da': ('DK',),
    'de': ('AT', 'BE', 'CH', 'DE', 'LI', 'LU'), 'doi': ('IN',),
    'dv': ('MV',), 'dz': ('BT',), 'el': ('GR', 'CY'),
    'en': ('AG', 'AU', 'BW', 'CA', 'DK', 'GB', 'HK', 'IE', 'IN', 'NG',
           'NZ', 'PH', 'SG', 'US', 'ZA', 'ZM', 'ZW'),
    'eo': ('US',),
    'es': ('AR', 'BO', 'CL', 'CO', 'CR', 'CU', 'DO', 'EC', 'ES', 'GT',
           'HN', 'MX', 'NI', 'PA', 'PE', 'PR', 'PY', 'SV', 'US', 'UY', 'VE',
           ), 'et': ('EE',), 'eu': ('ES', 'FR'), 'fa': ('IR',),
    'ff': ('SN',), 'fi': ('FI',), 'fil': ('PH',), 'fo': ('FO',),
    'fr': ('CA', 'CH', 'FR', 'LU'), 'fur': ('IT',), 'fy': ('NL', 'DE'),
    'ga': ('IE',), 'gd': ('GB',), 'gez': ('ER', 'ET'), 'gl': ('ES',),
    'gu': ('IN',), 'gv': ('GB',), 'ha': ('NG',), 'hak': ('TW',),
    'he': ('IL',), 'hi': ('IN',), 'hne': ('IN',), 'hr': ('HR',),
    'hsb': ('DE',), 'ht': ('HT',), 'hu': ('HU',), 'hy': ('AM',),
    'ia': ('FR',), 'id': ('ID',), 'ig': ('NG',), 'ik': ('CA',),
    'is': ('IS',), 'it': ('CH', 'IT'), 'iu': ('CA',), 'iw': ('IL',),
    'ja': ('JP',), 'ka': ('GE',), 'kk': ('KZ',), 'kl': ('GL',),
    'km': ('KH',), 'kn': ('IN',), 'ko': ('KR',), 'kok': ('IN',),
    'ks': ('IN',), 'ku': ('TR',), 'kw': ('GB',), 'ky': ('KG',),
    'lb': ('LU',), 'lg': ('UG',), 'li': ('BE', 'NL'), 'lij': ('IT',),
    'ln': ('CD',), 'lo': ('LA',), 'lt': ('LT',), 'lv': ('LV',),
    'lzh': ('TW',), 'mag': ('IN',), 'mai': ('IN',), 'mg': ('MG',),
    'mhr': ('RU',), 'mi': ('NZ',), 'mk': ('MK',), 'ml': ('IN',),
    'mn': ('MN',), 'mni': ('IN',), 'mr': ('IN',), 'ms': ('MY',),
    'mt': ('MT',), 'my': ('MM',), 'nan': ('TW',), 'nb': ('NO',),
    'nds': ('DE', 'NL'), 'ne': ('NP',), 'nhn': ('MX',),
    'niu': ('NU', 'NZ'), 'nl': ('AW', 'BE', 'NL'), 'nn': ('NO',),
    'nr': ('ZA',), 'nso': ('ZA',), 'oc': ('FR',), 'om': ('ET', 'KE'),
    'or': ('IN',), 'os': ('RU',), 'pa': ('IN', 'PK'),
    'pap': ('AN', 'AW', 'CW'), 'pl': ('PL',), 'ps': ('AF',),
    'pt': ('BR', 'PT'), 'quz': ('PE',), 'raj': ('IN',), 'ro': ('RO',),
    'ru': ('RU', 'UA'), 'rw': ('RW',), 'sa': ('IN',), 'sat': ('IN',),
    'sc': ('IT',), 'sd': ('IN', 'PK'), 'se': ('NO',), 'shs': ('CA',),
    'si': ('LK',), 'sid': ('ET',), 'sk': ('SK',), 'sl': ('SI',),
    'so': ('DJ', 'ET', 'KE', 'SO'), 'sq': ('AL', 'ML'), 'sr': ('ME', 'RS'),
    'ss': ('ZA',), 'st': ('ZA',), 'sv': ('FI', 'SE'), 'sw': ('KE', 'TZ'),
    'szl': ('PL',), 'ta': ('IN', 'LK'), 'tcy': ('IN',), 'te': ('IN',),
    'tg': ('TJ',), 'th': ('TH',), 'the': ('NP',), 'ti': ('ER', 'ET'),
    'tig': ('ER',), 'tk': ('TM',), 'tl': ('PH',), 'tn': ('ZA',),
    'tr': ('CY', 'TR'), 'ts': ('ZA',), 'tt': ('RU',), 'ug': ('CN',),
    'uk': ('UA',), 'unm': ('US',), 'ur': ('IN', 'PK'), 'uz': ('UZ',),
    've': ('ZA',), 'vi': ('VN',), 'wa': ('BE',), 'wae': ('CH',),
    'wal': ('ET',), 'wo': ('SN',), 'xh': ('ZA',), 'yi': ('US',),
    'yo': ('NG',), 'yue': ('HK',), 'zh': ('CN', 'HK', 'SG', 'TW'),
    'zu': ('ZA',),
}


def numerify(text='###'):
    text = _re_hash.sub(
        lambda x: str(random.randint(0, 9)),
        text)
    text = _re_perc.sub(
        lambda x: str(random.randint(1, 9)),
        text)
    text = _re_excl.sub(
        lambda x: str(random.randint(0, 1) and random.randint(0, 9) or ""),
        text)
    text = _re_at.sub(
        lambda x: str(random.randint(0, 1) and random.randint(1, 9) or ""),
        text)
    return text


def _choice(elements):
    return random.choice(elements)


def randomtimes(start, end, frmt="%Y-%m-%d %H:%M:%S"):
    stime = datetime.strptime(start, frmt)
    etime = datetime.strptime(end, frmt)
    return (random.random() * (etime - stime) + stime).strftime(frmt)


def locale():
    """Generate a random underscored i18n locale code (e.g. en_US).

    :sample:
    """
    language_code = _choice(list(language_locale_codes.keys()))
    return language_code + '_' + _choice(
        language_locale_codes[language_code],
    )


user_agents = (
    'chrome', 'firefox', 'internet_explorer', 'opera', 'safari', 'wechat', 'edge'
)

windows_platform_tokens = (
    "Windows NT 11.0",  # Windows 11
    "Windows NT 10.0",  # Windows 10
    "Windows NT 6.3",  # Windows 8.1
    "Windows NT 6.2",  # Windows 8
    "Windows NT 6.1",  # Windows 7
    "Windows NT 6.0",  # Windows Vista
    "Windows NT 5.2",  # Windows Server 2003/Windows XP x64 Edition
    "Windows NT 5.1",  # Windows XP
    "Windows NT 5.0",  # Windows 2000
    "Windows 98",  # Windows 98
    "Windows 95",  # Windows 95
    "Windows CE",  # Windows CE
    # 64位和32位区分
    "Windows NT 10.0; Win64; x64",  # 64位Windows 10
    "Windows NT 10.0; WOW64",  # 32位应用在64位Windows 10
    # 其他可能的变体
    "Windows NT 6.1; Win64; x64",  # 64位Windows 7
    "Windows NT 6.1; WOW64",  # 32位应用在64位Windows 7
    "Windows NT 11.0; Win64; x64",  # 64位Windows 11
    "Windows NT 11.0; WOW64"  # 32位应用在64位Windows 11
)

linux_processors = (
    "i686",  # 表示使用 32 位的 Intel 处理器架构
    "x86_64",  # 表示使用 64 位的 Intel 或 AMD 处理器架构
    "armv7l",  # 表示使用 ARMv7 架构的处理器，常见于旧一些的移动设备
    "i586",  # 表示较早期的 32 位 Intel 处理器架构
    "aarch64",  # 表示 64 位的 ARM 处理器架构（ARM64架构）
    "armv8",  # 表示 ARMv8 架构的处理器，也是一种 64 位 ARM 架构
)

mac_processors = (
    "Intel",  # 表示使用 Intel 处理器的 Mac
    "PPC",  # 表示使用 PowerPC 处理器的 Mac
    "U; Intel",  # 另一种表示 Intel 处理器的方式
    "U; PPC",  # 另一种表示 PowerPC 处理器的方式
    "ARM",  # 表示使用 ARM 架构处理器的 Mac，如 Apple Silicon M1
    "U; ARM"  # 另一种表示 ARM 架构处理器的方式
)

android_versions = (
    "4.0", "4.0.1", "4.0.2", "4.0.3", "4.0.4",  # Ice Cream Sandwich
    "4.1", "4.1.1", "4.1.2",  # Jelly Bean
    "4.2", "4.2.1", "4.2.2",  # Jelly Bean
    "4.3", "4.3.1",  # Jelly Bean
    "4.4", "4.4.1", "4.4.2", "4.4.3", "4.4.4",  # KitKat
    "5.0", "5.0.1", "5.0.2",  # Lollipop
    "5.1", "5.1.1",  # Lollipop
    "6.0", "6.0.1",  # Marshmallow
    "7.0",  # Nougat
    "7.1", "7.1.1", "7.1.2",  # Nougat
    "8.0.0",  # Oreo
    "8.1.0",  # Oreo
    "9",  # Pie
    "10",  # Android 10
    "11",  # Android 11
    "12",  # Android 12
    "12L",  # Android 12L
    "13"  # Android 13
)

apple_devices = ('iPhone', 'iPad')

ios_versions = (
    "9.0", "9.1", "9.2", "9.2.1", "9.3", "9.3.1", "9.3.2", "9.3.3", "9.3.4", "9.3.5",  # iOS 9 系列
    "10.0", "10.0.1", "10.0.2", "10.1", "10.1.1", "10.2", "10.2.1", "10.3", "10.3.1", "10.3.2", "10.3.3",
    # iOS 10 系列
    "11.0", "11.0.1", "11.0.2", "11.0.3", "11.1", "11.1.1", "11.1.2", "11.2", "11.2.1", "11.2.2", "11.2.5",
    "11.2.6", "11.3", "11.3.1", "11.4", "11.4.1",  # iOS 11 系列
    "12.0", "12.0.1", "12.1", "12.1.1", "12.1.2", "12.1.3", "12.1.4", "12.2", "12.3", "12.3.1", "12.3.2", "12.4",
    "12.4.1", "12.4.2", "12.4.3", "12.4.4", "12.4.5", "12.4.6", "12.4.7", "12.4.8",  # iOS 12 系列
    "13.0", "13.1", "13.1.1", "13.1.2", "13.1.3", "13.2", "13.2.1", "13.2.2", "13.2.3", "13.3", "13.3.1", "13.4",
    "13.4.1", "13.5", "13.5.1", "13.6", "13.6.1", "13.7",  # iOS 13 系列
    "14.0", "14.0.1", "14.1", "14.2", "14.2.1", "14.3", "14.4", "14.4.1", "14.4.2", "14.5", "14.5.1", "14.6",
    "14.7", "14.7.1", "14.8",  # iOS 14 系列
    "15.0", "15.0.1", "15.0.2", "15.1", "15.1.1", "15.2", "15.2.1", "15.3", "15.3.1", "15.4", "15.4.1", "15.5",
    "15.6", "15.6.1", "15.7",  # iOS 15 系列
    "16.0", "16.1"  # iOS 16 系列
)

mac_versions = (
    "10.7", "10.7.1", "10.7.2", "10.7.3", "10.7.4", "10.7.5",  # Lion
    "10.8", "10.8.1", "10.8.2", "10.8.3", "10.8.4", "10.8.5",  # Mountain Lion
    "10.9", "10.9.1", "10.9.2", "10.9.3", "10.9.4", "10.9.5",  # Mavericks
    "10.10", "10.10.1", "10.10.2", "10.10.3", "10.10.4", "10.10.5",  # Yosemite
    "10.11", "10.11.1", "10.11.2", "10.11.3", "10.11.4", "10.11.5", "10.11.6",  # El Capitan
    "10.12", "10.12.1", "10.12.2", "10.12.3", "10.12.4", "10.12.5", "10.12.6",  # Sierra
    "10.13", "10.13.1", "10.13.2", "10.13.3", "10.13.4", "10.13.5", "10.13.6",  # High Sierra
    "10.14", "10.14.1", "10.14.2", "10.14.3", "10.14.4", "10.14.5", "10.14.6",  # Mojave
    "10.15", "10.15.1", "10.15.2", "10.15.3", "10.15.4", "10.15.5", "10.15.6", "10.15.7",  # Catalina
    "11.0", "11.0.1", "11.1", "11.2", "11.2.1", "11.2.2", "11.2.3", "11.3", "11.3.1", "11.4", "11.5", "11.5.1",
    "11.5.2", "11.6", "11.6.1",  # Big Sur
    "12.0", "12.0.1", "12.1", "12.2", "12.2.1", "12.3", "12.3.1", "12.4", "12.5",  # Monterey
    "13.0", "13.1"  # Ventura
)

dalvik_versions = (
    '1.1',
    '1.2',
    '1.4',
    '1.5',
    '1.6',
    '2.0',
    '2.1',
)

phone_models = (
    # Apple iPhone 系列
    "iPhone 13,1",  # iPhone 12 Mini
    "iPhone 13,2",  # iPhone 12
    "iPhone 13,3",  # iPhone 12 Pro
    "iPhone 13,4",  # iPhone 12 Pro Max
    "iPhone 12,1",  # iPhone 11
    "iPhone 12,3",  # iPhone 11 Pro
    "iPhone 12,5",  # iPhone 11 Pro Max
    "iPhone 11,8",  # iPhone XR
    "iPhone 10,3",  # iPhone X
    "iPhone 10,6",  # iPhone X
    "iPhone 9,1",  # iPhone 7
    "iPhone 9,3",  # iPhone 7
    "iPhone 8,1",  # iPhone 6S
    # ... 更多 iPhone 型号

    # Samsung Galaxy 系列
    "SM-G991B",  # Samsung Galaxy S21
    "SM-G996B",  # Samsung Galaxy S21+
    "SM-G998B",  # Samsung Galaxy S21 Ultra
    "SM-G970F",  # Samsung Galaxy S10e
    "SM-G973F",  # Samsung Galaxy S10
    "SM-G975F",  # Samsung Galaxy S10+
    # ... 更多 Samsung 型号

    # Huawei 系列
    "ELE-L29",  # Huawei P30
    "VOG-L29",  # Huawei P30 Pro
    "MAR-LX1A",  # Huawei P30 Lite
    "P40 Pro",  # 华为 P40 Pro
    "P40",  # 华为 P40
    "P30 Pro",  # 华为 P30 Pro
    "P30",  # 华为 P30
    "Mate 40 Pro",  # 华为 Mate 40 Pro
    "Mate 40",  # 华为 Mate 40
    "Mate 30 Pro",  # 华为 Mate 30 Pro
    "Mate 30",  # 华为 Mate 30
    "Mate 20 Pro",  # 华为 Mate 20 Pro
    "Mate 20",  # 华为 Mate 20
    "Nova 7",  # 华为 Nova 7
    "Nova 6",  # 华为 Nova 6
    "Nova 5T",  # 华为 Nova 5T
    "Honor 30",  # 荣耀 30
    "Honor 20",  # 荣耀 20
    "Honor 10",  # 荣耀 10
    # ... 更多 Huawei 型号

    # Xiaomi 系列
    "M2102K1AC",  # Xiaomi Mi 11 Ultra
    "M2011K2C",  # Xiaomi Mi 11
    "M2007J3SC",  # Xiaomi Mi 10 Ultra
    "Mi 10",  # 小米 Mi 10
    "Mi 10 Pro",  # 小米 Mi 10 Pro
    "Mi 11",  # 小米 Mi 11
    "Mi 11 Ultra",  # 小米 Mi 11 Ultra
    "Mi 9",  # 小米 Mi 9
    "Mi 9 SE",  # 小米 Mi 9 SE
    "Mi 8",  # 小米 Mi 8
    "Redmi Note 9",  # 红米 Note 9
    "Redmi Note 8",  # 红米 Note 8
    "Redmi Note 7",  # 红米 Note 7
    "Redmi 9",  # 红米 9
    "Redmi 8",  # 红米 8
    "Redmi 7",  # 红米 7
    "Poco F3",  # Poco F3
    "Poco X3 NFC",  # Poco X3 NFC
    "Poco M3",  # Poco M3
    # ... 更多 Xiaomi 型号

    # OnePlus 系列
    "IN2023",  # OnePlus 8 Pro
    "IN2013",  # OnePlus 8
    "IN2003",  # OnePlus 8T
    # ... 更多 OnePlus 型号

    # Google Pixel 系列
    "Pixel 5",  # Google Pixel 5
    "Pixel 4A",  # Google Pixel 4A
    "Pixel 4",  # Google Pixel 4
    # ... 更多 Pixel 型号

    # Oppo 系列
    "CPH2173",  # Oppo Find X3 Pro
    "CPH2025",  # Oppo Find X2 Pro
    # ... 更多 Oppo 型号

    # Vivo 系列
    "V2035",  # Vivo Y31
    "V2031",  # Vivo Y20G
    # ... 更多 Vivo 型号

    # Realme 系列
    "RMX2151",  # Realme 7 Pro
    "RMX2040",  # Realme 6
    # ... 更多 Realme 型号

    # Nokia 系列
    "TA-1053",  # Nokia 6
    "TA-1043",  # Nokia 7 Plus
    # ... 更多 Nokia 型号

    # LG 系列
    "LM-V600",  # LG V60 ThinQ
    "LM-G900",  # LG Velvet
    # ... 更多 LG 型号

    # Sony 系列
    "XQ-AS72",  # Sony Xperia 1 III
    "XQ-AT52",  # Sony Xperia 5 II
    # ... 更多 Sony 型号

    # Motorola 系列
    "XT1962-1",  # Motorola Moto G7
    "XT2041-4",  # Motorola Moto G Power
    # ... 更多 Motorola 型号

    # Asus 系列
    "ZS661KS",  # Asus ROG Phone 3
    "ZS590KS",  # Asus Zenfone 8
    # ... 更多 Asus 型号

    # 更多其他品牌和型号
    # ...
)

nettypes = (
    '4G',
    'WIFI',
    '2G',
    '3G',
)


def windows_platform_token():
    """
    生成 Windows 平台令牌用于 user-agent

    :return:
    """
    return _choice(windows_platform_tokens)


def mac_platform_token():
    """
    生成 Mac 平台令牌用于 user-agent

    :return:
    """

    return 'Macintosh; {} Mac OS X {} {}'.format(
        random.randint(10, 11),
        _choice(mac_processors),
        _choice(mac_versions).replace('.', '_')
    )


def linux_platform_token():
    """
    生成 Linux 平台令牌用于 user-agent

    :return:
    """
    return 'X11; Linux {}'.format(_choice(linux_processors))


def android_platform_token():
    """
    生成 Android 平台令牌用于 user-agent

    :return:
    """
    return 'Android {}'.format(_choice(android_versions))


def ios_platform_token():
    """
    生成 IOS 平台令牌用于 user-agent

    :return:
    """
    return '{0}; CPU {0} OS {1} like Mac OS X'.format(
        _choice(apple_devices),
        _choice(ios_versions).replace('.', '_'),
    )


def wechat_platform_token():
    """
    生成 WeChat 平台令牌用于 user-agent

    :return:
    """
    return f'MicroMessenger/{random.randint(5, 9)}.{random.randint(0, 9)}.{random.randint(0, 9)} NetType/{_choice(nettypes)}  Language/{_choice(["en", "zh_CN"])}'


def android(
        android_version_from="8.0",
        brand: str = None,
        prefix: str = "Dalvik"
):
    """
    生成 dalvik 平台令牌用于 user-agent

    :return:
    """
    dalvik_version = f'{random.randint(1, 2)}.{random.randint(0, 9)}'
    android_version = _choice(
        [i for i in android_versions if i > android_version_from] or [android_version_from]
    )
    if brand and not isinstance(brand, list): brand = [brand]
    brand = _choice(brand or phone_models)
    return f'{prefix}/{dalvik_version} (Linux; U; Android {android_version}; {brand} Build/{"".join(random.choices(string.hexdigits, k=4))}.{randomtimes("20000101", datetime.today().strftime("%Y%m%d"), "%Y%m%d")}.{numerify()})'


def internet_explorer():
    """
    生成 internet_explorer 的 user-agent

    :return:
    """
    tmplt = 'Mozilla/5.0 (compatible; MSIE {0}.0; {1}; Trident/{2}.{3})'
    return tmplt.format(
        random.randint(8, 10),
        windows_platform_token(),
        random.randint(5, 7),
        random.randint(0, 1)
    )


def opera(
        device='all',
        version_from=15,
        version_end=77,
        chrome_version_from=90,
        chrome_version_end=120,
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
    bld = _re_qm.sub(lambda x: _choice(string.ascii_letters), numerify('##?###'))

    opera_version = f'{random.randint(version_from, version_end)}.0.{random.randint(1000, 9999)}.{random.randint(0, 999)}'

    tmp_pc = '({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko) Chrome/{chrome_ver} Safari/{saf_version} OPR/{opera_ver}'
    opera_mobile = f'Opera/{random.randint(9, 12)}.{random.randint(10, 99)} ({_choice([android_platform_token(), ios_platform_token()])}; Opera Mobile/{bld}; U; {locale().replace("_", "-")}) Presto/{opera_version} Version/{random.randint(10, 12)}.02'
    saf_version = f'{random.randint(531, 537)}.{random.randint(0, 36)}'
    _v = random.randint(chrome_version_from, chrome_version_end)
    chrome_version = f"{_v}.0.{int(_v * (49 + random.random()))}.{random.randint(0, 100)}"

    platforms_pc = [
        'Opera/{}.{}.({}; {}) Presto/2.{}.{} Version/{}.00'.format(
            random.randint(9, 12),
            random.randint(10, 99),
            random.choice([
                linux_platform_token(),
                windows_platform_token(),
            ]),
            locale().replace('_', '-'),
            random.randint(9, 12),
            random.randint(160, 400),
            random.randint(10, 12),
            random.randint(10, 12),
        ),
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
        )

    ]
    platforms_mobile = [
        opera_mobile
    ]
    platforms_all = [*platforms_mobile, *platforms_pc]
    return 'Mozilla/5.0 ' + _choice(locals()[f'platforms_{device}'])


def chrome(device='all', version_from=90, version_end=120):
    """
    生成 chrome 的 user-agent

    :param version_end:
    :param version_from: 最小版本号
    :param device: 设备类型, 可选: all/mobile/pc
    :return:
    """

    tmp_android = '({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko) Chrome/{version} Mobile Safari/{saf_version}'
    tmp_iphone = '({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko) CriOS/{version} Mobile/{bld} Safari/{saf_version}'
    tmp_normal = '({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko) Chrome/{version} Safari/{saf_version}'
    saf_version = f'{random.randint(531, 537)}.{random.randint(0, 36)}'
    bld = _re_qm.sub(lambda x: _choice(string.ascii_letters), numerify('##?###'))

    _v = random.randint(version_from, version_end)
    version = f"{_v}.0.{int(_v * (49 + random.random()))}.{random.randint(0, 100)}"

    platforms_mobile = [
        tmp_android.format(
            platform=android_platform_token(),
            saf_version=saf_version,
            version=version,
        ),
        tmp_iphone.format(
            platform=ios_platform_token(),
            saf_version=saf_version,
            version=version,
            bld=bld
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
    return 'Mozilla/5.0 ' + _choice(locals()[f'platforms_{device}'])


def firefox(device='all', version_from=90, version_end=120):
    """
    生成 firefox 的 user-agent

    :param version_end: 版本开始
    :param version_from: 版本结束
    :param device: 设备类型, 可选: all/mobile/pc
    :return:
    """

    tmp_android = '({platform}; Mobile; rv:{version}) Gecko/{version} Firefox/{version}'
    tmp_normal = '({platform}; Mobile; rv:{version}) Gecko/{ver} Firefox/{version}'
    tmp_iphone = '({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko) FxiOS/{version} Mobile/{bld} Safari/{saf_version}'
    saf_version = f'{random.randint(531, 537)}.{random.randint(0, 36)}'
    bld = _re_qm.sub(lambda x: _choice(string.ascii_letters), numerify('##?###'))
    ver = (datetime.today() - timedelta(
        days=random.randint(0, (datetime.today() - datetime(2018, 1, 1)).days))).strftime('%Y%m%d')

    version = f"{random.randint(version_from, version_end)}.{random.randint(0, 3)}.{random.randint(0, 3)}"

    platforms_mobile = [
        tmp_android.format(
            platform=android_platform_token(),
            version=version
        ),
        tmp_iphone.format(
            platform=ios_platform_token(),
            saf_version=saf_version,
            version=version,
            bld=bld
        ),

    ]
    platforms_pc = [
        tmp_normal.format(
            platform=windows_platform_token(),
            ver=ver,
            version=version,
        ),
        tmp_normal.format(
            platform=mac_platform_token(),
            ver=ver,
            version=version,
        ),
        tmp_normal.format(
            platform=linux_platform_token(),
            ver=ver,
            version=version,
        ),
    ]
    platforms_all = [*platforms_mobile, *platforms_pc]
    return 'Mozilla/5.0 ' + _choice(locals()[f'platforms_{device}'])


def edge(device='all', version_from=90, version_end=120):
    """
    生成 edge 的 user-agent

    :param version_end:
    :param version_from:
    :param device: 设备类型, 可选: all/mobile/pc
    :return:
    """
    _v = random.randint(version_from, version_end)
    version = f"{_v}.0.{int(_v * (49 + random.random()))}.{random.randint(0, 100)}"

    return chrome(
        device=device,
        version_from=version_from,
        version_end=version_end
    ) + f' Edg/{version}'


def safari(device='all'):
    """
    生成 safari 的 user-agent

    :param device: 设备类型, 可选: all/mobile/pc
    :return:
    """
    bld = _re_qm.sub(lambda x: _choice(string.ascii_letters), numerify('##?###'))
    saf_version = f'{random.randint(531, 537)}.{random.randint(0, 36)}'
    tmp_mac = '({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko) Version/{mac_version} Safari/{saf_version}'
    tmp_ios = '({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko) Version/{mac_version} Mobile/{bld} Safari/{saf_version}'

    platforms_mobile = [
        tmp_ios.format(
            platform=ios_platform_token(),
            saf_version=saf_version,
            mac_version=_choice(mac_versions),
            bld=bld
        ),

    ]
    platforms_pc = [
        tmp_mac.format(
            platform=mac_platform_token(),
            saf_version=saf_version,
            mac_version=_choice(mac_versions)
        ),
    ]
    platforms_all = [*platforms_mobile, *platforms_pc]
    return 'Mozilla/5.0 ' + _choice(locals()[f'platforms_{device}'])


def wechat(device='all', chrome_version_from=90, chrome_version_end=120):
    """
    生成 wechat 的 user-agent

    :param chrome_version_end:
    :param chrome_version_from:
    :param device: 设备类型, 可选: all/mobile/pc
    :return:
    """
    saf_version = f'{random.randint(531, 537)}.{random.randint(0, 36)}'
    bld = _re_qm.sub(lambda x: _choice(string.ascii_letters), numerify('##?###'))
    tmplt = '({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko)' \
            ' Chrome/{chrome_ver} Safari/{saf_version}'
    tmplt_ios = '({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko)' \
                ' CriOS/{chrome_ver} Mobile/{bld} Safari/{saf_version}'
    tmp_xcx = '({platform}) AppleWebKit/{saf_version} (KHTML, like Gecko) Mobile/{bld}'
    _v = random.randint(chrome_version_from, chrome_version_end)
    chrome_version = f"{_v}.0.{int(_v * (49 + random.random()))}.{random.randint(0, 100)}"

    platforms_pc = [
        tmplt.format(
            platform=windows_platform_token(),
            saf_version=saf_version,
            chrome_ver=chrome_version
        ) + " " + wechat_platform_token(),
        tmplt.format(
            platform=mac_platform_token(),
            saf_version=saf_version,
            chrome_ver=chrome_version
        ) + " " + wechat_platform_token(),

    ]
    platforms_mobile = [
        tmplt.format(
            platform='Linux; {}'.format(android_platform_token()),
            saf_version=saf_version,
            chrome_ver=chrome_version
        ) + " " + wechat_platform_token(),
        tmplt_ios.format(
            platform=ios_platform_token(),
            saf_version=saf_version,
            chrome_ver=chrome_version,
            bld=bld
        ) + " " + wechat_platform_token(),
        tmp_xcx.format(
            platform=ios_platform_token(),
            saf_version=saf_version,
            bld=bld
        ) + f' {wechat_platform_token()} Branch/Br_trunk MiniProgramEnv/Mac',
    ]
    platforms_all = [*platforms_mobile, *platforms_pc]
    return 'Mozilla/5.0 ' + _choice(locals()[f'platforms_{device}'])


def mobile(*apps):
    """
    生成 mobile 的 user-agent

    :param apps: app 类型, 可选: 'chrome', 'firefox', 'internet_explorer', 'opera', 'safari', 'wechat', 'edge'
    :return:
    """
    while True:
        ua = get(*apps)
        if any([
            ua.lower().__contains__('mobile'),
            ua.lower().__contains__('android'),
            ua.lower().__contains__('iphone'),
            ua.lower().__contains__('ipad'),
        ]):
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
        if not any([
            ua.lower().__contains__('mobile'),
            ua.lower().__contains__('android'),
            ua.lower().__contains__('iphone'),
            ua.lower().__contains__('ipad'),
        ]):
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
