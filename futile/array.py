#!/usr/bin/env python
# coding=utf-8


__all__ = ['chunked', 'chunked_qs', 'compact', 'compact_dict', 'fill', 'find',
           'find_index', 'head', 'first', 'take', 'flatten', 'tail', 'last',
           'unique', 'without', 'count_by', 'all', 'any', 'partition', 'reject',
           'reduce', 'dict_transform', 'split_ranges', 'filter_keys',
           'take_keys', 'take_indices']

import collections
import itertools
import functools
from datetime import timedelta


def chunked(chunk_size, iterable, len_func=len):
    """
    >>> list(chunked(['a', 'b', 'c', 'd'], 2))
    [['a', 'b'], ['c', 'd']]
    >>> list(chunked(['a', 'b', 'c', 'd'], 3))
    [['a', 'b', 'c'], ['d']]
    >>> list(chunked([], 1))
    []
    >>> list(chunked((i for i in range(5)), 2))
    [[0, 1], [2, 3], [4]]
    >>> list(chunked((i for i in range(4)), 2))
    [[0, 1], [2, 3]]
    """
    if hasattr(iterable, '__getitem__'):
        for i in range(0, len_func(iterable), chunk_size):
            yield iterable[i: i + chunk_size]
    else:  # iterable
        chunk = []
        i = 0
        for element in iterable:
            chunk.append(element)
            i += 1
            if i == chunk_size:
                i = 0
                yield chunk
                chunk = []
        if chunk:
            yield chunk  # yield the last uncomplete chunk


def chunked_qs(chunk_size, query_set,):
    return chunked(query_set, chunk_size, lambda qs: qs.count())


def merge_list(obj1, obj2):
    assert isinstance(obj1, list)
    assert isinstance(obj2, list)
    ret = []
    ret.extend(obj1)
    ret.extend(obj2)
    return ret


def merge_dict(obj1, obj2):
    assert isinstance(obj1, dict)
    assert isinstance(obj2, dict)
    ret = {}
    ret.update(obj1)
    ret.update(obj2)
    return ret


def compact(iterable):
    """
    >>> list(compact([0, 1, 2]))
    [1, 2]
    >>> list(compact([1, 2]))
    [1, 2]
    >>> list(compact([0, 1, False, 2, '', 3]))
    [1, 2, 3]
    """
    for el in iterable:
        if el:
            yield el


def compact_dict(dct):
    """
    >>> compact_dict({'a': 'a', 'b': None})
    {'a': 'a'}
    """
    result = {}
    for k, v in dct.items():
        if v:
            result[k] = v
    return result


def fill(sequence, value, start=0, stop=0):
    """
    >>> list(fill([1, 2, 3], '*'))
    ['*', '*', '*']
    >>> list(fill([1, 2, 3], '*', start=1, stop=3))
    [1, '*', '*']
    """
    if stop == 0:
        stop = len(sequence)
    for i in range(len(sequence)):
        if start <= i < stop:
            yield value
        else:
            yield sequence[i]


def find(sequence, key, reverse=False, errors='ignore'):
    """
    find specific element in an array by `key` function

    >>> find([{'a': 'b'}, {'a': 'c'}], key=lambda x: x['a'] == 'b')
    {'a': 'b'}
    """
    if reverse:
        sequence = reversed(sequence)
    for element in sequence:
        try:
            if key(element):
                return element
        except Exception:
            if errors == 'ignore':
                continue
            else:
                raise
    return None


def find_index(iterable, key, reverse=False, errors='ignore'):
    """
    >>> find_index([{'a': 'b'}], key=lambda x: x['a'] == 'b')
    0
    >>> find_index([{'a': 'b'}], key=lambda x: x['a'] == 'c')
    -1
    """
    if reverse:
        iterable = reversed(iterable)
    for i, element in enumerate(iterable):
        try:
            if key(element):
                return i
        except Exception:
            if errors == 'ignore':
                continue
            else:
                raise
    return -1


def head(iterable):
    """
    >>> head([1, 2, 3])
    1
    >>> head([])
    """
    # for django queryset using slicing is significantly faster than iterating to get the first element
    if hasattr(iterable, '__getitem__'):
        try:
            return iterable[0]
        except IndexError:
            return None
    for element in iterable:
        return element
    return None


first = head


def take(iterable, n):
    """
    >>> take([1, 2, 3], 1)
    [1]
    """
    return [element for i, element in enumerate(iterable) if i < n]


def flatten(iterable):
    """
    This should best be implemented with python3 yield from, however, for compatibility...
    >>> list(flatten([1, [2], 3]))
    [1, 2, 3]
    >>> list(flatten([1, [2], [3, [4]]]))
    [1, 2, 3, 4]
    >>> list(flatten(['hello']))
    ['hello']
    """
    result = []
    for element in iterable:
        if hasattr(element, '__iter__') and not isinstance(element, (dict, str, bytes)):
            result.extend(flatten(element))
        else:
            result.append(element)
    return result


def tail(iterable):
    """ return the last element in the iterable
    >>> tail([1, 2, 3])
    3
    """
    for element in reversed(iterable):
        return element


last = tail


def unique(iterable, key=lambda x: x):
    """
    >>> list(unique([1, 2, 3, 1, 2, 3]))
    [1, 2, 3]
    >>> list(unique([1, 2, 3]))
    [1, 2, 3]
    """
    seen = set()
    for element in iterable:
        if key(element) not in seen:
            seen.add(key(element))
            yield element


def without(iterable, values, key=lambda x: x):
    if not isinstance(values, (list, set, tuple)):
        values = [values]
    for element in iterable:
        if key(element) not in values:
            yield element


def count_by(iterable, key=lambda x: x):
    """
    count the iterable by given key function
    >>> count_by([0, 1, 2, 3, 4, 5], key=lambda x : x % 2 == 0)
    defaultdict(<class 'int'>, {True: 3, False: 3})
    """
    result = collections.defaultdict(int)
    for element in iterable:
        computed = key(element)
        result[computed] += 1
    return result


def all(iterable, pred=bool):
    """
    this function adds the pred parameter compared to std all function

    >>> all([-2, 0, 1], pred=lambda x: x > 0)
    False
    """
    for element in iterable:
        if not pred(element):
            return False
    return True


def any(iterable, pred=bool):
    """
    this function adds the pred parameter compared to std any function
    >>> any([-2, 0, 1], pred=lambda x: x > 0)
    True
    """
    for element in iterable:
        if pred(element):
            return True
    return False


def partition(iterable, pred=lambda x: x):
    """
    split values into truthy and false values by `pred`
    >>> partition([-2, -1, 0, 1, 2], pred=lambda x: x < 0)
    ([-2, -1], [0, 1, 2])
    """
    truthy = []
    falsy = []
    for element in iterable:
        if pred(element):
            truthy.append(element)
        else:
            falsy.append(element)
    return truthy, falsy


reject = itertools.filterfalse
reduce = functools.reduce


def split_ranges(start, stop, count=None, step=None):
    """
    split a range into subranges with step, for max count times. the last subrange has the remaining range.
    if count is not supplied, count is caculated automatically
    if step is not not supplied, will divide equally
    >>> from datetime import datetime, timedelta
    >>> start_time = datetime.strptime('2017-01-01', '%Y-%m-%d')
    >>> end_time = datetime.strptime('2017-01-03', '%Y-%m-%d')
    >>> times = split_ranges(start_time, end_time, 24)
    >>> times[1][1] - times[1][0] == timedelta(hours=2)
    True
    >>> times = split_ranges(start_time, end_time, 2)
    >>> times[1][1] - times[1][0] == timedelta(hours=24)
    True
    >>> times[1][0] == datetime(2017, 1, 2, 0, 0, 0)
    True
    >>> split_ranges(0, 100, 2)
    [(0.0, 50.0), (50.0, 100.0)]
    """
    if count is not None and count < 1:
        return []

    if count is None:
        if isinstance(step, timedelta):
            time_range = stop - start
            time_in_ms = time_range.microseconds + 1e6 * (time_range.seconds + 86400 * time_range.days)
            step_in_ms = step.microseconds + 1e6 * (step.seconds + 86400 * step.days)
            count = time_in_ms / step_in_ms
        else:
            count = (stop - start) / step
    if count < 1:
        count = 1

    if step is None:
        step = (stop - start) / count

    return [(start+i*step, start+(i+1)*step) for i in range(int(count))]


def dict_transform(src, fields):
    dst = {}
    for from_key, to_key in fields.items():
        dst[to_key] = src.get(from_key)
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


def take_indices(l, indices=None):
    indices = indices or []
    return [l[idx] for idx in indices if idx < len(l)]


if __name__ == '__main__':
    import doctest
    doctest.testmod()
