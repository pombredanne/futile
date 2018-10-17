from futile import metrics
import time

metrics.init('crawler', debug=False)


while True:
    metrics.emit('test', tags=dict(ip='10.1.1.1'), fields=dict(count=1.0))
    time.sleep(.1)
