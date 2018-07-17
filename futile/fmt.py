
def pformat(d, max_v_limit=0):
    u'''
    >>> pformat(dict(a=1, b='abcd'))
    u'{a=1, b=abcd}'
    >>> pformat(['a', 'b', 1])
    u'[a, b, 1]'
    >>> pformat(dict(a=1, b='abcdefg'), max_v_limit=5)
    u'{a=1, b=a..fg}'
    >>> pformat(1)
    u'1'
    >>> pformat([1,2,3], 2)
    u'[1, 2]'
    >>> class C: pass
    >>> o = C()
    >>> o.a = 12345
    >>> pformat(o, max_v_limit=2)
    u'C({a=1..5})'
    '''
    if isinstance(d, (list, tuple)):
        if max_v_limit:
            d = d[:max_v_limit]
        braces = '()' if isinstance(d, tuple) else '[]'
        return u''.join([braces[0],
            u', '.join(pformat(x, max_v_limit=max_v_limit) for x in d),
            braces[1],
            ])
    elif isinstance(d, dict):
        return '{%s}' % u', '.join('%s=%s' % (k, pformat(v, max_v_limit=max_v_limit)) for k, v in d.iteritems())
    elif isinstance(d, (date, datetime)):
        return fmt_datetime(d)
    elif hasattr(d, '__dict__'):
        return u'%s(%s)' % (d.__class__.__name__, pformat(d.__dict__, max_v_limit=max_v_limit))
    else:
        my_shorten = lambda x: (shorten(x, limit=max_v_limit) if max_v_limit else x)
        try:
            s = u'%s' % str_to_unicode(d, strict=False)
        except:
            s = u'%r' % d
        return my_shorten(s)

