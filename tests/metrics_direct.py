import os
os.environ["INFLUXDB_HOST"] = "localhost"
os.environ["INFLUXDB_PORT"] = "8086"
os.environ["INFLUXDB_DATABASE"] = "crawl"

from futile import metrics
from futile.log import init_log, get_logger
import time

init_log("test metrics")

metrics.init(prefix="test", debug=True, directly=True, batch_size=1)


import random
while True:
    for i in range(6):
        metrics.emit_counter('doc', 1)  # 模拟打点记录下载文档数量
    for i in range(5):
        metrics.emit_timer('latency', random.random())  # 模拟打点延迟
    for i in range(5):
        metrics.emit_store('cpu', random.random())  # 模拟打点CPU占用
    # 通过字典传入打点计数, 只有真值的才会计数, 也就是下面只有 foo 会计数
    metrics.emit_counter_by_dict({"foo": "bar", "fooz": 0, "barz": None})
    time.sleep(5)
