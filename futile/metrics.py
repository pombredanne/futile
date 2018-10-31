import os
import sys
import prctl
import signal
import time
import asyncio
import inspect
import concurrent.futures
import multiprocessing as mp
from influxdb import InfluxDBClient
from typing import Any

from futile.number import ensure_int, ensure_float
from futile.aio import aio_wrap
from futile.log import get_logger
from futile.queues import queue_mget
from futile.process import run_process


_metrics_queue = mp.Queue()
_debug = False
_batch_size = 128
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


def _point_key(p):
    return f"{p['measurement']}-{sorted(p['tags'].items())}"


def _accumulate_counter(points):
    counters = {}
    new_points = []
    for point in points:
        if point["measurement"].endswith(".counter"):
            key = _point_key(point)
            if key not in counters:
                counters[key] = {
                    "fields": {"_count": 0},
                    "time": point["time"],
                    "measurement": point["measurement"],
                    "tags": point["tags"],
                }
            counters[key]["fields"]["_count"] += point["fields"]["_count"]
            counters[key]["time"] = point["time"]
        else:
            new_points.append(point)
    new_points.extend(counters.values())
    return new_points


def _emit_loop():
    sys.stderr.write("metrics starting... batch=%d pid=%d\n" % (_batch_size,
                                                                os.getpid()))
    sys.stderr.flush()
    from futile.signals import handle_exit

    with handle_exit(_exit_emit_loop):
        # TODO drain the queue, when SystemExit receives
        count = 0
        while True:
            try:
                points = queue_mget(_metrics_queue, _batch_size)
                # sys.stderr.write("got %d points\n" % len(points))
                # sys.stderr.flush()
                points = _accumulate_counter(points)
                sys.stderr.write("accumulated %d points\n" % len(points))
                sys.stderr.flush()
                if _debug:
                    count += len(points)
                    sys.stderr.write(
                        "%s got %s point, total_count=%s\n"
                        % (time.time(), len(points), count)
                    )
                    sys.stderr.flush()
                    # sys.stderr.write(str(points) + "\n")
                    # sys.stderr.flush()
                _db.write_points(points, time_precision="ms")
            except Exception as e:
                get_logger("metrics emitter").exception(e)


def init(*, prefix=None, batch_size=128, debug=False, directly=False, **kwargs):

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
            port=int(os.environ.get("INFLUXDB_PORT")),
            # udp_port=int(os.environ.get("INFLUXDB_UDP_PORT")),
            database=os.environ.get("INFLUXDB_DATABASE"),
            # use_udp=True,
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
        timestamp = int(time.time() * 1000)
    # TODO 这里应该再加入一些基础信息到 tags 中, 比如 IP 什么的
    point = dict(measurement=measurement, tags=tags, fields=fields, time=timestamp)
    if _directly:
        _db.write_points([point], time_precision="ms")
    else:
        _metrics_queue.put(point)


def emit_counter(key: str, count: int, tags: dict = None, timestamp=None):
    """
    counter 不能被覆盖, 所以添加一个 _random tag 来作为唯一标识
    """
    if tags is None:
        tags = {}
    tags["_key"] = key
    count = ensure_int(count)
    fields = dict(_count=count)
    _emit(_prefix + ".counter", tags, fields, timestamp=timestamp)


def emit_store(key: str, value: Any, tags: dict = None, timestamp=None):
    if tags is None:
        tags = {}
    tags["_key"] = key
    fields = {"_value": value}
    _emit(_prefix + ".store", tags, fields, timestamp=timestamp)


def emit_timer(key: str, duration: float, tags: dict = None, timestamp=None):
    """
    延迟数据需要统计 pct99 等, 不能彼此覆盖
    """
    if tags is None:
        tags = {}
    tags["_key"] = key
    duration = ensure_float(duration)
    fields = dict(_duration=duration)
    _emit(_prefix + ".timer", tags, fields, timestamp=timestamp)


aemit_counter = aio_wrap(executor=_executor)(emit_counter)
aemit_store = aio_wrap(executor=_executor)(emit_store)
aemit_timer = aio_wrap(executor=_executor)(emit_timer)
