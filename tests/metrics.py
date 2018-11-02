import os
os.putenv("INFLUXDB_HOST", "localhost")
os.putenv("INFLUXDB_PORT", "8086")
os.putenv("INFLUXDB_DATABASE", "crawl")

from futile import metrics
from futile.log import init_log, get_logger
import time

init_log("test metrics")

metrics.init(prefix="test", debug=True, batch_size=10)

l = get_logger("test")
# metrics.emit_counter('doc', 1)

import random
while True:
    for i in range(6):
        metrics.emit_counter('doc', 1)
    for i in range(5):
        metrics.emit_timer('latency', random.random())
    l.info('sending metrics')
    time.sleep(5)
