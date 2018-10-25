import os
import sys
import prctl
import signal
import time
import asyncio
import uuid
import inspect
import random
import concurrent.futures
import multiprocessing as mp
from influxdb import InfluxDBClient
from typing import Any

from futile.number import ensure_int, ensure_float
from futile.aio import aio_wrap
from futile.log import get_logger
from futile.queue import queue_mget
from futile.process import run_process


_metrics_queue = mp.Queue()
_debug = False
_batch_size = 10
# this thread should stop running in the forked process
_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=1, thread_name_prefix="metrics"
)
_prefix = None
_directly = False
_db = None


def _exit_emit_loop():
    time.sleep(0.5)
    sys.stderr.write("metrics exiting...\n")
    sys.stderr.flush()


def _emit_loop():
    sys.stderr.write("metrics starting... pid=%d\n" % os.getpid())
    sys.stderr.flush()
    from futile.signals import handle_exit

    with handle_exit(_exit_emit_loop):
        # TODO drain the queue, when SystemExit receives
        count = 0
        while True:
            points = queue_mget(_metrics_queue, _batch_size)
            # points = [_metrics_queue.get()]
            if _debug:
                count += len(points)
                sys.stderr.write(
                    "%s got %s point, total_count=%s\n"
                    % (time.time(), len(points), count)
                )
                sys.stderr.flush()
                # sys.stderr.write(str(points) + "\n")
                # sys.stderr.flush()
            _db.write_points(points, time_precision="s")


def init(*, prefix=None, batch_size=10, debug=False, directly=False, **kwargs):

    global _prefix
    _prefix = prefix
    global _batch_size
    _batch_size = batch_size
    global _debug
    _debug = debug
    global _directly
    _directly = directly
    global _db

    if _prefix is None:
        raise ValueError("Metric prefix not set")

    if _debug:
        _db = InfluxDBClient(database="crawl")
    else:
        _db = InfluxDBClient(
            host=os.environ.get("INFLUXDB_HOST"),
            # udp_port=int(os.environ.get("INFLUXDB_UDP_PORT")),
            udp_port=int(os.environ.get("INFLUXDB_UDP_PORT")),
            database=os.environ.get("INFLUXDB_DATABASE"),
            use_udp=True,
        )

    frm = inspect.stack()[1]
    mod = inspect.getmodule(frm[0])
    caller_module = mod.__name__
    if caller_module != "__main__":
        get_logger("metrics").error(
            "metrics called from %s, call metrics.init in __main__ module!",
            caller_module,
        )
        return

    if not _directly:
        run_process(_emit_loop, auto_quit=True)

    get_logger("metrics").info("metrics init successfully")


def _emit(measurement, tags, fields, timestamp=None):
    if timestamp is None:
        timestamp = int(time.time())
    # TODO 这里应该再加入一些基础信息到 tags 中
    point = dict(measurement=measurement, tags=tags, fields=fields, time=timestamp)
    if _directly:
        _db.write_points([point], time_precision="s")
    else:
        _metrics_queue.put(point)


def _random():
    """
    返回一个随机数作为唯一标示, 由于 influxdb 的 max-values-per-tag 限制, 不能使用过多的
    值, 所以这里选择了 65536 个随机数, 每秒最多打这么多点.

    最好的方式当然是采用一个序列来循环使用, 但是那样要涉及锁, 太复杂了
    """
    return random.randint(1, 65536)


def emit_counter(key: str, count: int, tags: dict = None, timestamp=None):
    """
    counter 不能被覆盖, 所以添加一个 _random tag 来作为唯一标识
    """
    if tags is None:
        tags = {}
    tags["_random"] = _random()
    tags["_key"] = key
    count = ensure_int(count)
    fields = dict(_count=count)
    _emit(_prefix + ".counter", tags, fields, timestamp=timestamp)


def emit_store(key: str, value: Any, tags: dict = None, timestamp=None, directly=False):
    if tags is None:
        tags = {}
    fields = {key: value}
    _emit(_prefix + ".store", tags, fields, timestamp=timestamp)


def emit_timer(key: str, duration: float, tags: dict = None, timestamp=None):
    """
    延迟数据需要统计 pct99 等, 不能彼此覆盖
    """
    if tags is None:
        tags = {}
    tags["_random"] = _random()
    tags["_key"] = key
    duration = ensure_float(duration)
    fields = dict(_duration=duration)
    _emit(_prefix + ".timer", tags, fields, timestamp=timestamp)


aemit_counter = aio_wrap(executor=_executor)(emit_counter)
aemit_store = aio_wrap(executor=_executor)(emit_store)
aemit_timer = aio_wrap(executor=_executor)(emit_timer)
