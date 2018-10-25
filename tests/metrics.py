from futile import metrics
from futile.log import init_log, get_logger
import time
init_log("test metrics")
print(__name__)

metrics.init(prefix='test', debug=True, batch_size = 1)


l = get_logger('test')

import os

os.fork()
print(os.getpid())


# while True:
#     metrics.emit_store('cpu', 99.0)
#     l.info('sending matrics')
#     time.sleep(5)
