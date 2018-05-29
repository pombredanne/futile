# coding: utf-8

import re
from .array import compact


def expand(raw):
    """
    expand a string with {0..100} to 100 strings

    >>> expand('http://google.com/{1..3}.html')
    ['http://google.com/1.html', 'http://google.com/2.html']
    """
    expanded = [raw]
    pattern = r'\{(\d+\.{2}\d+)\}'
    while True:
        match = re.search(pattern, expanded[0])
        if not match:
            break
        new_expanded = []
        start, end = match.group(1).split('..')
        for i in range(int(start), int(end)):
            new_expanded.extend([re.sub(pattern, str(i), raw, count=1)
                                 for raw in expanded])
        expanded = new_expanded
    return expanded


def ensure_unicode(s, encoding='utf-8', errors='ignore'):
    """convert str(bytes) to unicode(str)
    >>> to_unicode(b'hello') == 'hello'
    True
    >>> to_unicode('你好') == '你好'
    True
    >>> isinstance(to_unicode('你好'), unicode_type)
    True
    """

    if isinstance(s, bytes):
        return s.decode(encoding=encoding, errors=errors)
    return s


def ensure_bytes(s, encoding='utf-8', errors='ignore'):
    """convert unicode(str) to str(bytes)
    >>> to_bytes(b'hello') == b'hello'
    True
    >>> isinstance(to_bytes('你好'), bytes)
    True
    >>> isinstance(to_bytes(123), bytes)
    True
    """
    if isinstance(s, str):
        return s.encode(encoding=encoding, errors=errors)
    return s

def snake_case(s):
    """convert token(s) to snake case
    >>> snake_case('fooBar')
    'foo_bar'
    >>> snake_case('foo_bar')
    'foo_bar'
    >>> snake_case('foo-bar')
    'foo_bar'
    >>> snake_case('FooBar')
    'foo_bar'
    >>> snake_case('Foo-Bar')
    'foo_bar'
    >>> snake_case('foo bar')
    'foo_bar'
    """
    s = to_unicode(s)
    s = re.sub(r'[A-Z]', r'-\g<0>', s, flags=re.UNICODE)  # turing uppercase to seperator with lowercase
    words = compact(re.split(r'\W+', s, flags=re.UNICODE))
    return '_'.join([word.lower() for word in words])


def dash_case(s):
    if s:
        s = snake_case(s).replace('_', '-')
    return s


def pascal_case(s):
    """convert token(s) to PascalCase
    >>> pascal_case('fooBar')
    'FooBar'
    >>> pascal_case('foo_bar')
    'FooBar'
    >>> pascal_case('foo-bar')
    'FooBar'
    >>> pascal_case('FooBar')
    'FooBar'
    >>> pascal_case('Foo-Bar')
    'FooBar'
    >>> pascal_case('foo bar')
    'FooBar'
    """
    s = to_unicode(s)
    s = re.sub(r'[A-Z]', r'-\g<0>', s, flags=re.UNICODE)  # turing uppercase to seperator with lowercase
    s = s.replace('_', '-')
    words = compact(re.split(r'\W+', s, flags=re.UNICODE))
    return ''.join([word.lower().capitalize() for word in words])

def camel_case(s):
    """convert token(s) to camelCase
    >>> camel_case('fooBar')
    'fooBar'
    >>> camel_case('foo_bar')
    'fooBar'
    >>> camel_case('foo-bar')
    'fooBar'
    >>> camel_case('FooBar')
    'fooBar'
    >>> camel_case('Foo-Bar')
    'fooBar'
    >>> camel_case('foo bar')
    'fooBar'
    """
    if s:
        s = pascal_case(s)
        s = s[0].lower() + s[1:]
    return s


def truncate(s, length, ending='…'):
    """truncate string to given length
    >>> truncate('hello', 5)
    'hello'
    >>> truncate('hello', 4)
    'hel…'
    """
    s = to_unicode(s)
    if len(s) > length:
        return s[:length - 1] + ending
    return s


def to_words(s, as_letter='-'):
    """convert given string to words list, hypen(-) and underscore is considered a letter
    >>> to_words('hello, world')
    ['hello', 'world']
    >>> to_words('the quick brown fox jumps over the lazy dog.')
    ['the', 'quick', 'brown', 'fox', 'jumps', 'over', 'the', 'lazy', 'dog']
    """
    s = to_unicode(s)
    return re.findall(r'[\w\-]+', s, flags=re.UNICODE) # NOTE greedy mode


if __name__ == '__main__':
    import doctest
    doctest.testmod()
