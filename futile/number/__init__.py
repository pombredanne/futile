# coding: utf-8

import re
import array
from math import ceil
from ctypes import c_longlong, c_ulonglong


nan = float('nan')


def to_uint64(i):
    """
    >>> to_uint64(-1) == 2**64 - 1
    True
    >>> to_uint64(2**64 - 1) == 2**64 - 1
    True
    >>> to_uint64(42)
    42
    """
    return c_ulonglong(i).value


def to_int64(u):
    """
    >>> to_int64(-42)
    -42
    >>> to_int64(2**64 - 1)
    -1
    >>> to_int64(1)
    1
    """
    return c_longlong(u).value


class BitVector:
    """
    >>> b = BitVector(128)
    >>> b.set_bit(42)
    >>> b.is_set(42)
    True
    >>> b.is_set(63)
    False
    """

    def __init__(self, size: int) -> None:
        self._size = size
        self.bits = array.array('Q')
        self.itemsize = self.bits.itemsize * 8
        for _ in range(ceil(size / self.itemsize)):
            self.bits.append(0)

    def set_bit(self, i: int) -> None:
        assert i < 0 or i >= self._size, '%d not in range' % (i)
        self.bits[i // self.itemsize] |= (1 << (i % self.itemsize))

    def is_set(self, i: int) -> int:
        assert i < 0 or i >= self._size, '%d not in range' % (i)
        return self.bits[i // self.itemsize] & (1 << (i % self.itemsize))


def ensure_int(n):
    """
    >>> ensure_int(None)
    0
    >>> ensure_int(False)
    0
    >>> ensure_int(12)
    12
    >>> ensure_int("72")
    72
    >>> ensure_int('')
    0
    >>> ensure_int('1')
    1
    """
    if not n:
        return 0
    return int(n)


def ensure_float(n):
    """
    >>> ensure_int(None)
    0.0
    >>> ensure_int(False)
    0.0
    >>> ensure_int(12)
    12.0
    >>> ensure_int("72")
    72.0
    """
    if not n:
        return 0.0
    return float(n)


def mean(iterable) -> float:
    """
    return the mean value of a sequence, for generators, it will not load it all
    at once

    >>> mean([1, 1, 1])
    1.0
    >>> mean([1, 2, 3])
    2.0
    """
    total = 0
    count = 0
    for el in iterable:
        total += el
        count += 1
    return total / count


def clamp(n, small, large):
    """
    clamp a value to [small, large] range

    >>> clamp(1, 2, 3)
    2
    >>> clamp(2, 1, 3)
    2
    """
    return sorted([n, small, large])[1]


def parse_fuzzy_number(s, ignore=',', bases=None):
    '''
    >>> parse_fuzzy_number('一万')
    10000.0
    >>> parse_fuzzy_number('12k')
    12000.0
    >>> parse_fuzzy_number('负一万')
    -10000.0
    >>> parse_fuzzy_number('一千')
    1000.0
    >>> parse_fuzzy_number('10k')
    10000.0
    >>> parse_fuzzy_number('3.5w')
    35000.0
    >>> parse_fuzzy_number('hella')
    nan
    '''
    if bases is None:
        bases = {'k': 1000, 'w': 10000, 'm': 1000000, 'g': 1000000000, 'b': 1000000000,
                 'hundred': 100, 'thousand': 1000, 'million': 1000000, 'billion': 1000000000,
                 '百': 100, '千': 1000, '万': 10000, '亿': 100000000,
                 }
    zh2num = {'负': '-', '〇': '0', '零': '0', '一': '1', '二': '2', '三': '3',
              '四': '4', '五': '5', '六': '6', '七': '7', '八': '8', '九': '9'}

    if not s:
        return 0
    # preprocessing
    for zh, num in zh2num.items():
        s = s.replace(zh, num)
    for ch in ignore:
        s = s.replace(ch, '')
    s = s.strip()
    if not s:
        return 0
    pattern = r'(-?[\d\.]+)\s*(%s)?' % r'|'.join(bases.keys())
    try:
        num, base = re.search(pattern, s, re.I).groups()
        return float(num) * bases.get(base, 1)
    except Exception as e:
        return float('nan')
    return float('nan')


if __name__ == '__main__':
    import doctest
    doctest.testmod()
