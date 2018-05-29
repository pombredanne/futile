# coding: utf-8

import re


def mean(iterable):
    """
    return the mean value of a sequence, for generators, it will not load it all at once

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


def to_int(s, ignore=','):
    '''
    >>> to_int('8,452,558')
    8452558
    >>> to_int('')
    0
    >>> to_int('1')
    1
    >>> to_int('hello')
    nan
    '''
    for ch in ignore:
        s = s.replace(ch, '')
    s = s.strip()
    if not s:
        return 0
    try:
        s = re.search(r'-?\s*\d+', s).group(0)
        return int(s)
    except Exception:
        return float('nan')
    return float('nan')


def to_fuzzy_number(s, ignore=',', bases=None):
    '''
    >>> to_fuzzy_number('一万')
    10000.0
    >>> to_fuzzy_number('12k')
    12000.0
    >>> to_fuzzy_number('负一万')
    -10000.0
    >>> to_fuzzy_number('一千')
    1000.0
    >>> to_fuzzy_number('10k')
    10000.0
    >>> to_fuzzy_number('3.5w')
    35000.0
    >>> to_fuzzy_number('hella')
    nan
    '''
    if bases is None:
        bases = {'k': 1000, 'w': 10000, 'm': 1000000, 'g': 1000000000, 'b': 1000000000,
                 'hundred': 100, 'thousand': 1000, 'million': 1000000, 'billion': 1000000000,
                 '百': 100, '千': 1000, '万': 10000, '亿': 100000000,
            }
    zh2num = {'负': '-', '〇': '0', '零': '0', '一': '1', '二': '2', '三': '3', '四': '4', '五': '5', '六': '6', '七': '7', '八': '8', '九': '9'}

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
