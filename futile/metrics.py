import os
import json
import sys
import signal
import time
import asyncio
import concurrent.futures
import multiprocessing as mp
import threading
import socket
from influxdb import InfluxDBClient
from typing import Any

from futile.number import ensure_int, ensure_float
from futile.array import chunked
from futile.aio import aio_wrap
from futile.log import get_logger
from futile.queues import queue_mget
from futile.process import run_process


_inited_pid = None
_metrics_queue = mp.Queue()
_debug = False
# this thread should stop running in the forked process
_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=1, thread_name_prefix="metrics"
)
_directly = False
_emitter = None


class MetricsEmitter:
    def __init__(self, influxdb, prefix, *, batch_size=1024, max_timer_seq=128):
        self.pending_timestamp = 0
        self.pending_points = []
        self.batch = []
        self.batch_size = batch_size
        self.prefix = prefix
        self.influxdb = influxdb
        self.tagkv = []
        self.max_timer_seq = max_timer_seq
        self.lock = threading.Lock()
        self.hostname = socket.gethostname()

    def define_tagkv(self, tagk, tagvs):
        self.tagkv[tagk] = set(tagvs)

    def _point_key(self, p):
        return f"{p['measurement']}-{sorted(p['tags'].items())}-{p['time']}"

    def _accumulate_points(self, points):
        counters = {}
        seqs = {}
        new_points = []
        for point in points:
            point_type = point["tags"].get("_type", None)
            if point_type == "counter":
                key = self._point_key(point)
                if key not in counters:
                    counters[key] = {
                        "fields": {"_count": 0},
                        "time": point["time"],
                        "measurement": point["measurement"],
                        "tags": point["tags"],
                    }
                counters[key]["fields"]["_count"] += point["fields"]["_count"]
            elif point_type == "timer":
                key = self._point_key(point)
                if key not in seqs:
                    seqs[key] = 0
                if seqs[key] > self.max_timer_seq:
                    continue
                point["tags"]["_seq"] = seqs[key]
                seqs[key] += 1
                new_points.append(point)
            else:
                new_points.append(point)
        new_points.extend(counters.values())
        return new_points

    def _try_emit(self, point):
        """
        pending 表示当前时间戳内的点
        batch 表示积攒的一个组的点

        如果新的点和当前时间戳不一样了,那就把当前时间戳的点累加后放到 batch
        如果batch 中有足够的点, 打点
        """
        # 如果和当前 pending 时间一致, 继续累加
        if point["time"] == self.pending_timestamp:
            self.pending_points.append(point)
            return []
        if _debug:
            sys.stderr.write(
                "%s got %s raw points" % (time.time(), len(self.pending_points))
            )
            sys.stderr.write(json.dumps(self.pending_points, indent=4) + "\n")
            sys.stderr.flush()
        # 开始输出点, 把当前点重新放到 pending 中
        points = self._accumulate_points(self.pending_points)
        self.pending_timestamp = point["time"]
        self.pending_points = [point]
        self.batch.extend(points)
        if len(self.batch) < self.batch_size:
            return []
        if _debug:
            sys.stderr.write("%s got %s point" % (time.time(), len(self.batch)))
            sys.stderr.write(json.dumps(points, indent=4) + "\n")
            sys.stderr.flush()
        to_send_points = self.batch[:]
        self.batch = []
        return to_send_points

    def close(self):
        if _debug:
            sys.stderr.write(
                "start draining points %s\n" % json.dumps(self.pending_points, indent=4)
            )
            sys.stderr.flush()
        points = self._accumulate_points(self.pending_points)
        if self.batch:
            points.extend(self.batch)
        if _debug:
            sys.stderr.write(
                "final points %s\n" % json.dumps(self.pending_points, indent=4)
            )
            sys.stderr.flush()
        if points:
            for chunk in chunked(self.batch_size, points):
                try:
                    self.influxdb.write_points(chunk, time_precision="ms")
                except Exception as e:
                    sys.stderr.write("%s error writing points" % time.time())
                    sys.stderr.flush()

    def get_point(self, measurement, tags, fields, timestamp=None):
        if measurement is None:
            measurement = self.prefix
        if tags is None:
            tags = {}
        else:
            tags = tags.copy()
        if fields is None:
            fields = {}
        else:
            fields = fields.copy()
        if timestamp is None:
            timestamp = int(time.time() * 1000)
        # TODO 这里应该再加入一些基础信息到 tags 中, 比如 IP 什么的
        tags["hostname"] = self.hostname
        point = dict(measurement=measurement, tags=tags, fields=fields, time=timestamp)
        if self.tagkv:
            for tagk, tagv in tags.items():
                if tagv not in self.tagkv[tagk]:
                    raise ValueError("tag value = %s not in %s", tagv, self.tagkv[tagk])
        return point

    def get_counter_point(
        self,
        key: str = None,
        count: int = 1,
        tags: dict = None,
        measurement: str = None,
        timestamp: int = None,
    ):
        """
        counter 不能被覆盖
        """
        if measurement is None:
            measurement = self.prefix + ".counter"
        if tags is None:
            tags = {}
        else:
            tags = tags.copy()
        if key is not None:
            tags["_key"] = key
        tags["_type"] = "counter"
        count = ensure_int(count)
        fields = dict(_count=count)
        point = self.get_point(measurement, tags, fields, timestamp=timestamp)
        return point

    def get_store_point(
        self,
        key: str = None,
        value: Any = 0,
        tags: dict = None,
        measurement: str = None,
        timestamp=None,
    ):
        if measurement is None:
            measurement = self.prefix + ".store"
        if tags is None:
            tags = {}
        else:
            tags = tags.copy()
        if key is not None:
            tags["_key"] = key
        tags["_type"] = "store"
        fields = {"_value": value}
        point = self.get_point(measurement, tags, fields, timestamp=timestamp)
        return point

    def get_timer_point(
        self,
        key: str = None,
        duration: float = 0,
        tags: dict = None,
        measurement: str = None,
        timestamp=None,
    ):
        """
        延迟数据需要统计 pct99 等, 不能彼此覆盖
        """
        if measurement is None:
            measurement = self.prefix + ".timer"
        if tags is None:
            tags = {}
        else:
            tags = tags.copy()
        if key is not None:
            tags["_key"] = key
        tags["_type"] = "timer"
        duration = ensure_float(duration)
        fields = dict(_duration=duration)
        point = self.get_point(measurement, tags, fields, timestamp=timestamp)
        return point

    def emit(self, point):
        """
        这里可能抛出异常
        """
        with self.lock:
            points = self._try_emit(point)
            if points:
                for chunk in chunked(self.batch_size, points):
                    try:
                        self.influxdb.write_points(chunk, time_precision="ms")
                    except Exception as e:
                        sys.stderr.write("%s error writing points\n" % time.time())
                        sys.stderr.flush()

    def emit_any(self, *args, **kwargs):
        point = self.get_point(*args, **kwargs)
        self.emit(point)

    def emit_counter(self, *args, **kwargs):
        point = self.get_counter_point(*args, **kwargs)
        self.emit(point)

    def emit_store(self, *args, **kwargs):
        point = self.get_store_point(*args, **kwargs)
        self.emit(point)

    def emit_timer(self, *args, **kwargs):
        point = self.get_timer_point(*args, **kwargs)
        self.emit(point)


def _exit_emit_loop():
    sys.stderr.write("metrics exiting...\n")
    sys.stderr.flush()


def _emit_loop():
    sys.stderr.write(
        "metrics starting... batch=%d pid=%d\n" % (_emitter.batch_size, os.getpid())
    )
    sys.stderr.flush()
    from futile.signals import handle_exit

    # TODO drain the queue, when SystemExit receives
    while True:
        try:
            # 读取一个点
            point = _metrics_queue.get()
            _emitter.emit(point)
        except Exception as e:
            get_logger("metrics emitter").exception(e)
    _emitter.close()


def init(
    *,
    influxdb_host=None,
    influxdb_port=8086,
    influxdb_udp_port=8089,
    influxdb_database=None,
    prefix=None,
    batch_size=1024,
    debug=False,
    directly=False,
    use_thread=False,
    use_udp=False,
    timeout=10,
    **kwargs,
):

    if prefix is None:
        raise ValueError("Metric prefix not set")

    global _inited_pid
    if _inited_pid == os.getpid():
        get_logger("metrics").error("metrics already started")
        return

    _inited_pid = os.getpid()

    global _debug
    _debug = debug
    global _directly
    _directly = directly
    global _emitter
    db = InfluxDBClient(
        host=os.environ.get("INFLUXDB_HOST", influxdb_host),
        port=int(os.environ.get("INFLUXDB_PORT", influxdb_port)),
        udp_port=int(os.environ.get("INFLUXDB_UDP_PORT", influxdb_udp_port)),
        database=os.environ.get("INFLUXDB_DATABASE", influxdb_database),
        use_udp=use_udp,
        timeout=timeout,
    )
    _emitter = MetricsEmitter(db, prefix, batch_size=batch_size)

    if not _directly:
        if use_thread:
            thread = threading.Thread(target=_emit_loop)
            thread.daemon = True
            thread.start()
        else:
            if threading.current_thread() != threading.main_thread():
                get_logger("metrics").error("metrics called NOT from main thread")
                return
            run_process(_emit_loop, auto_quit=True)

    get_logger("metrics").info("metrics init successfully")


def emit_any(*args, **kwargs):
    if not _emitter:
        return
    if _directly:
        _emitter.emit_any(*args, **kwargs)
    else:
        _metrics_queue.put(_emitter.get_point(*args, **kwargs))


def emit_counter(*args, **kwargs):
    if not _emitter:
        return
    if _directly:
        _emitter.emit_counter(*args, **kwargs)
    else:
        _metrics_queue.put(_emitter.get_counter_point(*args, **kwargs))


def emit_timer(*args, **kwargs):
    if not _emitter:
        return
    if _directly:
        _emitter.emit_timer(*args, **kwargs)
    else:
        _metrics_queue.put(_emitter.get_timer_point(*args, **kwargs))


def emit_store(*args, **kwargs):
    if not _emitter:
        return
    if _directly:
        _emitter.emit_store(*args, **kwargs)
    else:
        _metrics_queue.put(_emitter.get_store_point(*args, **kwargs))


def close():
    if _directly:
        _emitter.close()


def emit_counter_by_dict(counters, tags=None, timestamp=None):
    for k, v in counters.items():
        if not v:
            continue
        emit_counter(k, 1, tags=tags, timestamp=timestamp)


aemit_counter = aio_wrap(executor=_executor)(emit_counter)
aemit_store = aio_wrap(executor=_executor)(emit_store)
aemit_timer = aio_wrap(executor=_executor)(emit_timer)
