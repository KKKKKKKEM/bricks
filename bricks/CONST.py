# -*- coding: utf-8 -*-
# @Time    : 2023-11-15 17:42
# @Author  : Kem
# @Desc    :
import hashlib
import uuid

# 当前 机器 ID
MEACHINE_ID = hashlib.sha256(uuid.UUID(int=uuid.getnode()).hex[-12:].encode()).hexdigest()

# 当前框架版本
VERSION = "0.0.1"
