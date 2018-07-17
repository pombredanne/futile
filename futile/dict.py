from collections import OrderedDict, Callable


class OrderedDefaultDict(OrderedDict):
    # Source: http://stackoverflow.com/a/6190500/562769
    def __init__(self, default_factory=None, *a, **kw):
        if (default_factory is not None and
           not isinstance(default_factory, Callable)):
            raise TypeError('first argument must be callable')
        OrderedDict.__init__(self, *a, **kw)
        self.default_factory = default_factory

    def __getitem__(self, key):
        try:
            return OrderedDict.__getitem__(self, key)
        except KeyError:
            return self.__missing__(key)

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = value = self.default_factory()
        return value

    def __reduce__(self):
        if self.default_factory is None:
            args = tuple()
        else:
            args = self.default_factory,
        return type(self), args, None, None, self.items()

    def copy(self):
        return self.__copy__()

    def __copy__(self):
        return type(self)(self.default_factory, self)

    def __deepcopy__(self, memo):
        import copy
        return type(self)(self.default_factory,
                          copy.deepcopy(self.items()))

    def __repr__(self):
        return 'OrderedDefaultDict(%s, %s)' % (self.default_factory,
                                               OrderedDict.__repr__(self))


def dict_transform(src, fields, default=None):
    dst = {}
    for from_key, to_key in fields.items():
        dst[to_key] = src.get(from_key, default)
    return dst


def take_keys(d, keys=None, default=None):
    """
    >>> d = {'foo': 'bar', 'hello': 'world'}
    >>> take_keys(d, ['foo', 'no'], None)
    {'foo': 'bar', 'no': None}
    """
    keys = keys or []
    return {k: d.get(k, default) for k in keys}


def filter_keys(d, keys=None):
    """
    >>> d = {'a': 1, 'b': 2}
    >>> filter_keys(d, keys=['a'])
    {'a': 1}
    """
    keys = set(keys or [])
    return {k: v for k, v in d.items() if k in keys}
