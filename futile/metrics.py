import os
import sys
import prctl
import signal
import time
import asyncio
import inspect
import concurrent.futures
import multiprocessing as mp
import threading
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
_max_timer_seq = 128
_db = None


def _exit_emit_loop():
    time.sleep(0.5)
    sys.stderr.write("metrics exiting...\n")
    sys.stderr.flush()


def _point_key(p):
    return f"{p['measurement']}-{sorted(p['tags'].items())}-{p['time']}"


def _accumulate_points(points):
    counters = {}
    seqs = {}
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
        elif point["measurement"].endswith(".timer"):
            key = _point_key(point)
            if key not in seqs:
                seqs[key] = 0
            if seqs[key] > _max_timer_seq:
                continue
            point["tags"]["_seq"] = seqs[key]
            seqs[key] += 1
            new_points.append(point)
        else:
            new_points.append(point)
    new_points.extend(counters.values())
    return new_points


def _emit_loop():
    sys.stderr.write(
        "metrics starting... batch=%d pid=%d\n" % (_batch_size, os.getpid())
    )
    sys.stderr.flush()
    from futile.signals import handle_exit

    with handle_exit(_exit_emit_loop):
        # TODO drain the queue, when SystemExit receives
        count = 0
        while True:
            try:
                points = queue_mget(_metrics_queue, _batch_size, timeout=10)
                # sys.stderr.write("got %d points\n" % len(points))
                # sys.stderr.flush()
                if not points:
                    continue
                points = _accumulate_points(points)
                if _debug:
                    sys.stderr.write("accumulated %d points\n" % len(points))
                    count += len(points)
                    sys.stderr.write(
                        "%s got %s point, total_count=%s\n"
                        % (time.time(), len(points), count)
                    )
                    sys.stderr.write(str(points) + "\n")
                    sys.stderr.flush()
                _db.write_points(points, time_precision="ms")
            except Exception as e:
                get_logger("metrics emitter").exception(e)


def init(
    *,
    influxdb_host=None,
    influxdb_port=8086,
    influxdb_udp_port=8089,
    influxdb_database=None,
    prefix=None,
    batch_size=128,
    debug=False,
    directly=False,
    use_udp=False,
    **kwargs,
):

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

    _db = InfluxDBClient(
        host=os.getenv("INFLUXDB_HOST", influxdb_host),
        port=int(os.getenv("INFLUXDB_PORT", influxdb_port)),
        udp_port=int(os.getenv("INFLUXDB_UDP_PORT", influxdb_udp_port)),
        database=os.getenv("INFLUXDB_DATABASE", influxdb_database),
        use_udp=use_udp,
    )

    if threading.current_thread() != threading.main_thread():
        get_logger("metrics").error(
            "metrics called NOT from main thread, call metrics.init in main thread!",
        )
        return

    if not _directly:
        run_process(_emit_loop, auto_quit=True)

    get_logger("metrics").info("metrics init successfully")


_tagkv = {}


def define_tagkv(tagk, tagvs):
    _tagkv[tagk] = set(tagvs)


def _emit(measurement, tags, fields, timestamp=None):
    if measurement is None:
        measurement = _prefix
    if tags is None:
        tags = {}
    if fields is None:
        fields = {}
    if timestamp is None:
        timestamp = int(time.time() * 1000)
    # TODO 这里应该再加入一些基础信息到 tags 中, 比如 IP 什么的
    point = dict(measurement=measurement, tags=tags, fields=fields, time=timestamp)
    if _tagkv:
        for tagk, tagv in tags.items():
            if tagv not in _tagkv[tagk]:
                raise ValueError("tag value = %s not in %s", tagv, _tagkv[tagk])
    if _directly:
        _db.write_points([point], time_precision="ms")
    else:
        _metrics_queue.put(point)


def emit(
    *,
    measurement: str = None,
    tags: dict = None,
    fields: dict = None,
    timestamp: int = None,
):
    _emit(measurement, tags, fields, timestamp)


def emit_counter(key: str, count: int, tags: dict = None, timestamp=None):
    """
    counter 不能被覆盖
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


def emit_counter_by_dict(counters, tags=None):
    for k, v in counters.items():
        if not v:
            continue
        emit_counter(k, 1, tags=tags)


aemit_counter = aio_wrap(executor=_executor)(emit_counter)
aemit_store = aio_wrap(executor=_executor)(emit_store)
aemit_timer = aio_wrap(executor=_executor)(emit_timer)
