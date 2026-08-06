# -*- coding: utf-8 -*-
# @Time    : 2023-12-11 13:14
# @Author  : Kem
# @Desc    :
import json
import os
import threading
import time
import uuid
from collections import defaultdict

from loguru import logger

from bricks.lib.queues import Item, TaskQueue
from bricks.lib.queues.smart import SmartQueue
from bricks.utils import pandora


class LocalQueue(TaskQueue):
    def __init__(self) -> None:
        self._box = dict()
        self._container = getattr(self, "_container", None) or defaultdict(SmartQueue)
        self._locks = defaultdict(threading.Lock)
        self._status = defaultdict(threading.Event)
        self._init_leases = {}
        self._init_epochs = defaultdict(int)

    def __str__(self):
        return "<LocalQueue>"

    def size(
            self, *names: str, qtypes: tuple = ("current", "temp", "failure"), **kwargs
    ) -> int:
        if not names:
            return 0
        else:
            names = [self.name2key(name, _) for name in names for _ in qtypes]

        count = 0

        for name in names:
            count += self._container[name].qsize()

        return count

    def reverse(self, name: str, **kwargs) -> bool:
        qtypes = kwargs.pop("qtypes", None) or ["temp", "failure"]

        dest = self.name2key(name, "current")
        for qtype in qtypes:
            _queue = self.name2key(name, qtype)
            with self._container[_queue].mutex:
                self._container[dest].put(*self._container[_queue].queue)
                self._container[_queue].queue.clear()
        return True

    def smart_reverse(self, name: str, status=0) -> bool:
        tc = self.size(name, qtypes=("temp",))
        cc = self.size(name, qtypes=("current",))
        fc = self.size(name, qtypes=("failure",))

        if cc == 0 and fc != 0:
            qtypes = ["failure"]
            need_reverse = True

        elif cc == 0 and fc == 0 and tc != 0 and status == 0:
            qtypes = ["temp"]
            need_reverse = True

        else:
            need_reverse = False
            qtypes = []

        if need_reverse:
            self.reverse(name, qtypes=qtypes)
            return True
        else:
            return False

    def merge(self, dest: str, *queues: str, **kwargs):
        with self._container[queues].mutex:
            for _queue in queues:
                self._container[dest].put(*self._container[_queue].queue)
                self._container[_queue].queue.clear()
        return True

    def replace(self, name: str, *values, **kwargs):
        qtypes = kwargs.pop("qtypes", ["current", "temp", "failure"])
        count = 0
        for old, new in values:
            for qtype in pandora.iterable(qtypes):
                if self.remove(name, old, qtypes=qtype):
                    count += self.put(name, new, qtypes=qtype)

        return count

    def remove(self, name: str, *values, **kwargs):
        backup = kwargs.pop("backup", None)
        backup and self.put(name, *values, qtypes=backup)
        name = self.name2key(name, kwargs.get("qtypes", "temp"))
        return self._container[name].remove(*values)

    def put(self, name: str, *values, **kwargs):
        init_lease = kwargs.pop("init_lease", None) or {}
        qname = self.name2key(name, kwargs.pop("qtypes", "current"))
        unique = kwargs.pop("unique", None)
        priority = kwargs.pop("priority", None)
        timeout = kwargs.pop("timeout", None)
        limit = kwargs.pop("limit", 0)
        head = bool(priority)

        def write():
            return self._container[qname].put(
                *values,
                block=True,
                timeout=timeout,
                unique=unique,
                limit=limit,
                head=head,
            )

        if not init_lease:
            return write()

        record_key = self.name2key(name, "record")
        with self._locks[record_key]:
            current = self._init_leases.get(record_key)
            if not current or any(
                str(current.get(field)) != str(init_lease.get(field))
                for field in ("owner_id", "lease_id", "fencing_token")
            ):
                return 0
            return write()

    def get(self, name: str, count: int = 1, **kwargs) -> Item:
        pop_key = self.name2key(name, "current")
        add_key = self.name2key(name, "temp")
        tail = kwargs.pop("tail", False)
        items = self._container[pop_key].get(
            block=False, timeout=None, count=count, tail=tail
        )
        items is not None and self._container[add_key].put(*pandora.iterable(items))
        return items

    def clear(
            self, *names, qtypes=("current", "temp", "failure", "lock", "record"), **kwargs
    ):
        for name in names:
            for qtype in qtypes:
                self._container.pop(self.name2key(name, qtype), None)

    def command(self, name: str, order: dict):
        key = self.name2key(name, "record")

        def lease_matches(lease):
            current = self._init_leases.get(key)
            if not lease.get("owner_id") and not lease.get("lease_id"):
                return True
            return bool(
                current
                and current.get("owner_id") == lease.get("owner_id")
                and current.get("lease_id") == lease.get("lease_id")
                and str(current.get("fencing_token")) == str(lease.get("fencing_token"))
            )

        def current_lease():
            return {
                "owner_id": order.get("owner_id") or str(uuid.uuid4()),
                "lease_id": order.get("lease_id") or str(uuid.uuid4()),
            }

        def get_permission():
            with self._locks[key]:
                record = json.loads(os.environ.get(key) or "{}")
                init_state = record.get("init_state")
                init_running = init_state == self.INIT_RUNNING or (
                    not init_state and str(record.get("status")) == "1"
                )
                if init_state == self.INIT_FAILED_FINAL:
                    return {"state": False, "msg": "初始化失败且不允许重试"}
                if not init_running and not self.is_empty(name):
                    return {"state": False, "msg": "已投完且存在种子没有消费完毕"}

                current = self._init_leases.get(key)
                lease = current_lease()
                if current:
                    if (
                        current.get("owner_id") == lease["owner_id"]
                        and current.get("lease_id") == lease["lease_id"]
                    ):
                        return {"state": True, "msg": "成功获取权限", **current}
                    return {"state": False, "msg": "存在其他活跃的初始化机器"}

                self._init_epochs[key] += 1
                lease["fencing_token"] = self._init_epochs[key]
                lease["lease_token"] = "|".join(
                    [
                        lease["owner_id"],
                        lease["lease_id"],
                        str(lease["fencing_token"]),
                    ]
                )
                self._init_leases[key] = lease
                return {"state": True, "msg": "成功获取权限", **lease}

        def set_init():
            lease = order.get("lease") or order
            with self._locks[key]:
                if not lease_matches(lease):
                    return False
                record = json.loads(os.environ.get(key) or "{}")
                record.update(
                    {
                        "time": int(time.time() * 1000),
                        "status": 1,
                        "init_state": self.INIT_RUNNING,
                        "owner_id": lease.get("owner_id", ""),
                        "lease_id": lease.get("lease_id", ""),
                        "fencing_token": lease.get("fencing_token", ""),
                    }
                )
                record.pop("stop_heartbeat", None)
                record.pop("init_error", None)
                os.environ[key] = json.dumps(record, default=str)
                self._status[key].set()
                return True

        def is_init():
            if not self.is_empty(name):
                return True
            record = json.loads(os.environ.get(key) or "{}")
            return record.get("init_state") == self.INIT_RUNNING or str(
                record.get("status")
            ) == "1"

        def validate_init():
            lease = order.get("lease") or order
            with self._locks[key]:
                return lease_matches(lease)

        def set_record():
            lease = order.get("lease") or order
            with self._locks[key]:
                if not lease_matches(lease):
                    return False
                current = json.loads(os.environ.get(key) or "{}")
                current.update(order["record"] or {})
                os.environ[key] = json.dumps(current, default=str)
                return True

        def reset_init_record():
            with self._locks[key]:
                os.environ.pop(key, None)
                self._init_leases.pop(key, None)
                self._status[key].clear()
                self.clear(name)
            return True

        def wait_for_init_start():
            while self.is_empty(name):
                record = json.loads(os.environ.get(key) or "{}")
                if record.get("init_state") in {
                    self.INIT_SUCCEEDED,
                    self.INIT_FAILED_RETRYABLE,
                    self.INIT_FAILED_FINAL,
                }:
                    return
                if self._status[key].is_set():
                    return
                time.sleep(1)
                logger.debug("等待初始化开始")

        def release_init():
            lease = order.get("lease") or order
            with self._locks[key]:
                if not lease_matches(lease):
                    return False
                record = json.loads(os.environ.get(key) or "{}")
                record.update(
                    {
                        "status": 0,
                        "init_state": order.get(
                            "init_state", self.INIT_SUCCEEDED
                        ),
                        "finish": order.get("finish", str(time.time())),
                    }
                )
                if order.get("init_error"):
                    record["init_error"] = order["init_error"]
                if record["init_state"] in {
                    self.INIT_FAILED_RETRYABLE,
                    self.INIT_FAILED_FINAL,
                }:
                    os.environ[key] = json.dumps(record, default=str)
                else:
                    os.environ.pop(key, None)
                self._init_leases.pop(key, None)
                self._status[key].clear()
                return True

        def get_record():
            record = json.loads(os.environ.get(key) or "{}")
            if record.get("status") == 0 and record.get("init_state") not in {
                self.INIT_FAILED_RETRYABLE,
                self.INIT_FAILED_FINAL,
            }:
                os.environ.pop(key, None)
                return {}
            else:
                return record

        actions = {
            self.COMMANDS.GET_PERMISSION: lambda: get_permission(),
            self.COMMANDS.GET_RECORD: get_record,
            self.COMMANDS.CONTINUE_RECORD: lambda: self.reverse(name),
            self.COMMANDS.SET_RECORD: set_record,
            self.COMMANDS.RESET_INIT: reset_init_record,
            self.COMMANDS.WAIT_INIT: wait_for_init_start,
            self.COMMANDS.SET_INIT: lambda: set_init(),
            self.COMMANDS.IS_INIT: lambda: is_init(),
            self.COMMANDS.VALIDATE_INIT: lambda: validate_init(),
            self.COMMANDS.RELEASE_INIT: release_init,
        }
        action = order["action"]
        if action in actions:
            return actions[action]()
