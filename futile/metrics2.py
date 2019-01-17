def fmttags(measurement, tags):
    """
    >>> fmttags("foo", dict(hello="world", true="false"))
    'foo,hello=world,true=false'
    """
    tagstr = ",".join([f"{k}={v}" for k, v in tags.items()])
    return ",".join([measurement, tagstr])


class TagStatsdClient:
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
