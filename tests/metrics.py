import os
os.putenv("INFLUXDB_HOST", "localhost")
os.putenv("INFLUXDB_PORT", "8086")
os.putenv("INFLUXDB_DATABASE", "crawl")

from futile import metrics
from futile.log import init_log, get_logger
import time

init_log("test metrics")

metrics.init(prefix="test", debug=True, batch_size=1)

l = get_logger("test")
# metrics.emit_counter('doc', 1)

while True:
    metrics.emit_counter('doc', 1)
    l.info('sending metrics')
    time.sleep(5)
