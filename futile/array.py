#!/usr/bin/env python
# coding=utf-8


__all__ = [
    "identity",
    "chunked",
    "chunked_qs",
    "compact",
    "compact_dict",
    "fill",
    "find",
    "find_index",
    "head",
    "first",
    "take",
    "flatten",
    "tail",
    "last",
    "unique",
    "without",
    "count_by",
    "all",
    "any",
    "partition",
    "reject",
    "reduce",
    "split_ranges",
    "take_indices",
    "safe_zip",
    "filter_by",
]

import collections
import itertools
import functools
from datetime import timedelta
from typing import Iterable


def identity(obj):
    return obj


def chunked(chunk_size, iterable, len_func=len):
    """
    数组切片，每个切片的大小是 chunk_size

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
    if hasattr(iterable, "__getitem__"):
        for i in range(0, len_func(iterable), chunk_size):
            yield iterable[i : i + chunk_size]
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


def chunked_qs(chunk_size, query_set):
    """
    Django QuerySet 切片
    """
    return chunked(query_set, chunk_size, lambda qs: qs.count())


def merge_dict(obj1: dict, obj2: dict) -> dict:
    """
    合并字典

    >>> d1 = {'a': 1}
    >>> d2 = {'b': 2}
    >>> merge_dict(d1, d2)
    {'a': 1, 'b': 2}
    """
    ret = {}
    ret.update(obj1)
    ret.update(obj2)
    return ret


def compact(iterable: Iterable):
    """
    删除数组中为falsy、0的元素
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


def compact_dict(dct: dict):
    """
    >>> compact_dict({'a': 'a', 'b': None})
    {'a': 'a'}
    """
    result = {}
    for k, v in dct.items():
        if v:
            result[k] = v
    return result


def fill(sequence, value, start=0, stop=None):
    """
    >>> list(fill([1, 2, 3], '*'))
    ['*', '*', '*']
    >>> list(fill([1, 2, 3], '*', start=1, stop=3))
    [1, '*', '*']
    """
    if stop is None:
        stop = len(sequence)
    for i in range(len(sequence)):
        if start <= i < stop:
            yield value
        else:
            yield sequence[i]


def find(sequence, key, *, default=None, reverse=False, errors="ignore"):
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
            if errors == "ignore":
                continue
            else:
                raise
    return default


def find_index(iterable, key, reverse=False, errors="ignore"):
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
            if errors == "ignore":
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
    # for django queryset using slicing is significantly faster than iterating to
    # get the first element
    if hasattr(iterable, "__getitem__"):
        try:
            return iterable[0]
        except IndexError:
            return None
    for element in iterable:
        return element
    return None


first = head


def flatten(iterable):
    """
    >>> list(flatten([1, [2], 3]))
    [1, 2, 3]
    >>> list(flatten([1, [2], [3, [4]]]))
    [1, 2, 3, 4]
    >>> list(flatten(['hello']))
    ['hello']
    """
    for element in iterable:
        if hasattr(element, "__iter__") and not isinstance(element, (dict, str, bytes)):
            yield from flatten(element)
        else:
            yield element


def tail(iterable):
    """ return the last element in the iterable
    >>> tail([1, 2, 3])
    3
    """
    for element in reversed(iterable):
        return element


last = tail


def take(iterable, n):
    """
    >>> list(take(range(5), 1))
    [0]
    """
    for i, e in enumerate(iterable):
        if i < n:
            yield e


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
    """
    >>> list(without([1, 2, 3], 2))
    [1, 3]
    """
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
    split a range into subranges with step, for max count times. the last subrange
    has the remaining range.
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
            time_in_ms = time_range.microseconds + 1e6 * (
                time_range.seconds + 86400 * time_range.days
            )
            step_in_ms = step.microseconds + 1e6 * (step.seconds + 86400 * step.days)
            count = time_in_ms / step_in_ms
        else:
            count = (stop - start) / step
    if count < 1:
        count = 1

    if step is None:
        step = (stop - start) / count

    return [(start + i * step, start + (i + 1) * step) for i in range(int(count))]


def take_indices(l, indices=None, default=None):
    """
    >>> list(take_indices([1, 2, 3], [0, 2, 5]))
    [1, 3, None]
    """
    if indices is None:
        return
    for idx in indices:
        try:
            yield l[idx]
        except IndexError:
            yield default


def group_by_attr(l, attr):
    ret = collections.defaultdict(list)
    for el in l:
        val = getattr(el, attr, None)
        ret[val].append(el)
    return ret


def safe_zip(*lists):
    for l in lists:
        if len(l) != len(lists[0]):
            raise ValueError(
                "zip list lengths are not equal %s" % [len(l) for l in lists]
            )
    return zip(*lists)


def filter_by(l, bools, falsy=False):
    """
    >>> list(filter_by([1,2,3], [True, False, True]))
    [1, 3]
    >>> list(filter_by([1,2,3], [True, False, True], falsy=True))
    [2]
    """
    for a, b in safe_zip(l, bools):
        if not falsy and b or falsy and not b:
            yield a


if __name__ == "__main__":
    import doctest

    doctest.testmod()
