import os
os.putenv("INFLUXDB_HOST", "localhost")
os.putenv("INFLUXDB_PORT", "8086")
os.putenv("INFLUXDB_DATABASE", "crawl")

from futile import metrics
from futile.log import init_log, get_logger
import time

init_log("test metrics")

# init 函数的参数
# def init(
#     *,
#     influxdb_host=None,  localhost
#     influxdb_port=None,  8086
#     influxdb_udp_port=None,  8089
#     influxdb_database=None,  数据库需要预先创建
#     prefix=None,  # mesurement 前缀
#     batch_size=128,  # 批量打点
#     debug=False,  # debug mode
#     directly=False,  # 是否 fork 出进程打点
#     use_udp=False,  # 使用 udp 打点
#     **kwargs,
# ):
# 或者使用参数传递
metrics.init(prefix="test", debug=True, batch_size=10)

l = get_logger("test")

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
    l.info('sending metrics')
    time.sleep(5)
