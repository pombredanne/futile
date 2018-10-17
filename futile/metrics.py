import os
import sys
import prctl
import signal
import time
import multiprocessing as mp
from datetime import datetime
from influxdb import InfluxDBClient

from futile.queue import queue_mget


_metrics_queue = mp.Queue()
_debug = False

_batch_size = 10


def _emit():
    if _debug:
        db = InfluxDBClient(database=os.environ.get("INFLUXDB_DATABASE"))
    else:
        db = InfluxDBClient(
            host=os.environ.get("INFLUXDB_HOST"),
            # udp_port=int(os.environ.get("INFLUXDB_UDP_PORT")),
            udp_port=int(os.environ.get("INFLUXDB_UDP_PORT")),
            database=os.environ.get("INFLUXDB_DATABASE"),
            use_udp=True,
        )
    # TODO drain the queue, when SystemExit receives
    while True:
        points = queue_mget(_metrics_queue, _batch_size)
        if _debug:
            sys.stderr.write("%s got %s point, sending\n" % (time.time(), _batch_size))
            sys.stderr.flush()
        sys.stderr.write(str(points)+'\n')
        sys.stderr.flush()
        db.write_points(points, time_precision='s')


def init(prefix, batch_size=10, debug=False):
    global _batch_size
    _batch_size = batch_size
    global _debug
    _debug = debug
    try:
        pid = os.fork()
    except OSError:
        print("unable to fork")
        sys.exit(-1)
    if pid != 0:  # parent
        return
    else:
        prctl.set_pdeathsig(signal.SIGTERM)
        _emit()


def emit(measurement, tags, fields, timestamp=None):
    if timestamp is None:
        timestamp = time.time()
    timestr = datetime.fromtimestamp(timestamp).astimezone().isoformat()
    # 这里应该再加入一些基础信息到 tags 中
    json_body = dict(measurement=measurement, tags=tags, fields=fields, time=timestr)
    _metrics_queue.put(json_body)
