# coding: utf-8

import regex as re
import json

try:
    import cchardet as chardet
except ImportError:
    import chardet
from .array import compact


def expand(raw):
    """
    expand a string with {0..100} to 100 strings

    >>> expand('http://google.com/{1..3}.html')
    ['http://google.com/1.html', 'http://google.com/2.html']
    """
    expanded = [raw]
    pattern = r"\{(\d+\.{2}\d+)\}"
    while True:
        match = re.search(pattern, expanded[0])
        if not match:
            break
        new_expanded = []
        start, end = match.group(1).split("..")
        for i in range(int(start), int(end)):
            new_expanded.extend(
                [re.sub(pattern, str(i), raw, count=1) for raw in expanded]
            )
        expanded = new_expanded
    return expanded


def ensure_str(s, encoding="utf-8", use_chardet=False, errors="ignore"):
    """
    >>> ensure_str(b'hello') == 'hello'
    True
    >>> ensure_str('你好') == '你好'
    True
    >>> isinstance(ensure_str('你好'), str)
    True
    """

    if isinstance(s, bytes):
        if use_chardet:
            r = chardet.detect(s[:1024])
            encoding = r["encoding"]
        return s.decode(encoding=encoding, errors=errors)
    if isinstance(s, (int, float)):
        return str(s)
    if isinstance(s, (dict, list)):
        return json.dumps(s)
    if s is None:
        return ""
    return s


def ensure_bytes(s, encoding="utf-8", errors="ignore"):
    """
    >>> ensure_bytes(b'hello') == b'hello'
    True
    >>> isinstance(ensure_bytes('你好'), bytes)
    True
    """
    if isinstance(s, str):
        return s.encode(encoding=encoding, errors=errors)
    return s


def snake_case(s):
    """
    convert token(s) to snake case
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
    s = ensure_str(s)
    # turing uppercase to seperator with lowercase
    s = re.sub(r"[A-Z]", r"-\g<0>", s, flags=re.UNICODE)
    words = compact(re.split(r"\W+", s, flags=re.UNICODE))
    return "_".join([word.lower() for word in words])


def dash_case(s):
    """
    >>> dash_case('fooBar')
    'foo-bar'
    >>> dash_case('foo_bar')
    'foo-bar'
    >>> dash_case('foo-bar')
    'foo-bar'
    >>> dash_case('FooBar')
    'foo-bar'
    >>> dash_case('Foo-Bar')
    'foo-bar'
    >>> dash_case('foo bar')
    'foo-bar'
    """
    if s:
        s = snake_case(s).replace("_", "-")
    return s


slugify = dash_case


def pascal_case(s):
    """
    convert token(s) to PascalCase
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
    s = ensure_str(s)
    # turing uppercase to seperator with lowercase
    s = re.sub(r"[A-Z]", r"-\g<0>", s, flags=re.UNICODE)
    s = s.replace("_", "-")
    words = compact(re.split(r"\W+", s, flags=re.UNICODE))
    return "".join([word.lower().capitalize() for word in words])


def camel_case(s):
    """
    convert token(s) to camelCase
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


def truncate(s, length, ending="…"):
    """
    truncate string to given length

    >>> truncate('hello', 5)
    'hello'
    >>> truncate('hello', 4)
    'hel…'
    """
    s = ensure_str(s)
    if len(s) > length:
        return s[: length - 1] + ending
    return s


def to_words(s, as_letter="-"):
    """
    convert given string to words list, hypen(-) and underscore is considered a letter

    >>> to_words('hello, world')
    ['hello', 'world']
    >>> to_words('the quick brown fox jumps over the lazy dog.')
    ['the', 'quick', 'brown', 'fox', 'jumps', 'over', 'the', 'lazy', 'dog']
    """
    s = ensure_str(s)
    return re.findall(r"[\w\-]+", s, flags=re.UNICODE)  # NOTE greedy mode


def unicode_strip(s) -> str:
    """
    str.strip, but with unicode space characters

    >>> unicode_strip('')
    ''
    >>> unicode_strip(u'　 \x0ba\ufeff ')
    'a'
    """
    if not s:
        return ""
    s = ensure_str(s)
    spaces = "[ \t\n\r\x00-\x1F\x7F\xA0\xAD\u2000-\u200F\u201F\u202F\u3000\uFEFF]+"
    return re.sub(u"^%s|%s$" % (spaces, spaces), "", s)


def render(s: str, **kwargs) -> str:
    from jinja2 import Environment, BaseLoader

    rtemplate = Environment(loader=BaseLoader).from_string(s)
    return rtemplate.render(**kwargs)


if __name__ == "__main__":
    import doctest

    doctest.testmod()
