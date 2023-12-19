# -*- coding: utf-8 -*-
# @Time    : 2023-12-19 16:41
# @Author  : Kem
# @Desc    : 随机生成工具

import random as random_
import string


def random(length: int, base_str: str = ""):
    """
    生成一个指定长度的随机字符串

    :param base_str: 基础的 str table, 不传为默认的
    :param length: 字符串长度
    :return:
    """
    random_str = ''
    base_str = base_str or string.ascii_letters + string.digits
    for i in range(length):
        random_str += base_str[random_.randint(0, len(base_str) - 1)]
    return random_str


def num(length, start=0, end=None):
    """
    生成一个随机的数字

    :param length: 长度
    :param start: 最小值
    :param end: 最大值
    :return:
    """
    end = end or int('9' * length)
    return f'{random_.randint(start, end):0{length}d}'


def letters(length, base_str: str = ""):
    """
    生成一个随机的字符串

    :param base_str:
    :param length: 长度
    :return:
    """
    return random(length, base_str=base_str)


def hexdigits(length: int = 16):
    """
    生成随机的 hex

    :param length:
    :return:
    """
    return random(length, base_str=string.hexdigits)


def mac():
    """
    生成随机 mac 地址

    :return:
    """
    macstring = "0123456789abcdef" * 12
    macstringlist = random_.sample(macstring, 12)
    return "{0[0]}{0[1]}:{0[2]}{0[3]}:{0[4]}{0[5]}:{0[6]}{0[7]}:{0[8]}{0[9]}:{0[10]}{0[11]}".format(macstringlist)


if __name__ == '__main__':
    print(hexdigits())
