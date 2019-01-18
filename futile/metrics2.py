import os
import statsd

_emitter = None


def fmttags(measurement, tags, base_tags=None):
    """
    >>> fmttags("foo", dict(hello="world", true="false"))
    'foo,hello=world,true=false'
    >>> fmttags("foo", dict(hello="world", true="false"), dict(db="spider"))
    'foo,hello=world,true=false,db=spider'
    """
    if not tags and not base_tags:
        return measurement
    if base_tags:
        tags = {**tags, **base_tags}  # NOTE 不能直接使用 update, 否则会更改 tags
    tagstr = ",".join([f"{k}={v}" for k, v in tags.items()])
    return ",".join([measurement, tagstr])


class MetricsEmitter:
    def __init__(self, host="localhost", port=8125, prefix=None, tags=None):
        """
        Args:
            prefix: global measurement prefix
            tags: global tag dict that will be added to each metric point,
                such as {"db": "spider"}
        """
        self._client = statsd.StatsClient(host, port, prefix)
        self._tags = tags

    def emit_counter(self, key, value, *, tags=None, rate=1):
        key = fmttags(key, tags, self._tags)
        self._client.incr(key, value, rate=rate)

    def emit_timer(self, key, value, *, tags=None, rate=1):
        key = fmttags(key, tags, self._tags)
        self._client.timing(key, value, rate=rate)

    def emit_store(self, key, value, *, tags=None, rate=1, delta=False):
        key = fmttags(key, tags, self._tags)
        self._client.gauge(key, value, rate=rate, delta=delta)


def init(*, host=None, port=None, prefix=None, tags=None):
    global _emitter
    if host is None:
        host = os.getenv("STATSD_HOST") or "localhost"
    if port is None:
        port = os.getenv("STATSD_PORT") or 8125
    _emitter = MetricsEmitter(host, port, prefix, tags)


def emit_counter(key, value, *, tags=None, rate=1):
    if _emitter:
        _emitter.emit_counter(key, value, tags=tags, rate=rate)


def emit_timer(key, value, *, tags=None, rate=1):
    if _emitter:
        _emitter.emit_timer(key, value, tags=tags, rate=rate)


def emit_store(key, value, *, tags=None, rate=1, delta=False):
    if _emitter:
        _emitter.emit_timer(key, value, tags=tags, rate=rate, delta=delta)


class TagStatsClient:
    def __init__(self, statsd_client):
        self._statsd_client = statsd_client

    def __getattr__(self, attr):
        def emit(*args, **kwargs):
            tags = kwargs.pop("tags", {})
            stat = fmttags(args[0], tags)
            method = getattr(self._statsd_client, attr)
            method(stat, *args[1:], **kwargs)

        return emit


if __name__ == "__main__":
    import doctest

    doctest.testmod()
