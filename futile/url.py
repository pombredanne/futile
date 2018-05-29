#!/usr/bin/env python
# coding: utf-8


__all__ = ['depack_jsonp', 'json_api', 'jsonp_api', 'mget_url_param',
           'get_url_param', 'update_url_param', 'parse_qs_dict',
           'build_url', 'get_domain', 'get_top_domain', 'normalize_url']

import re
import json
import requests
from urllib.parse import quote as urlquote, unquote as urlunquote, urlsplit, \
        urlunsplit, urlencode, parse_qsl


def depack_jsonp(jsonp):
    """
    return json dict str from a jsonp str, if not found, return None

    >>> print(depack_jsonp('json_callback({"a": "b"})'))
    {"a": "b"}
    >>> print(depack_jsonp(''))
    None
    """
    try:
        # by standard, json data should be wrapped in a dict
        return re.search(r'[\w\.&\s]+?\((\{.*?\})\)', jsonp).group(1)
    except Exception:
        return None


def json_api(url, download_func):
    """
    download and return the json data from a json api

    >>> download_func = lambda _: '{"a": "b"}'
    >>> json_api('http://example.com', download_func)
    {'a': 'b'}
    """
    if not download_func:
        download_func = lambda url: requests.get(url).text
    return json.loads(download_func(url))


def jsonp_api(url, download_func):
    """
    download and return the json data from a jsonp api in one step

    >>> download_func = lambda _: 'jsonp({"a": "b"})'
    >>> jsonp_api('http://example.com', download_func)
    {'a': 'b'}
    """
    if not download_func:
        download_func = lambda url: requests.get(url).text
    jsonp = download_func(url)
    if not jsonp:
        return None
    json_data = json.loads(depack_jsonp(jsonp))
    return json_data


def mget_url_param(url, params, default=None):
    """
    >>> mget_url_param('http://foo.bar/search?a=a&b=b&c=c', ['a', 'b', 'd'])
    {'a': 'a', 'b': 'b', 'd': None}
    >>> mget_url_param('http://foo.bar/search?a=alpha&b=beta&c=cella', {'foo': 'a', 'bar': 'b'})
    {'foo': 'alpha', 'bar': 'beta'}
    """
    if isinstance(params, (list, tuple)):
        params = dict(zip(params, params))
    u = urlsplit(url)
    parsed_qs = parse_qs_dict(u.query)
    result = {}
    for key, param in params.items():
        result[key] = parsed_qs.get(param, default)
    return result


def get_url_param(url, param, default=None):
    """
    >>> get_url_param('http://foo.bar?a=a&b=b&c=c', 'a')
    'a'
    """
    result = mget_url_param(url, [param], default)
    return result[param]


def update_url_param(url, params):
    """
    >>> update_url_param('http://google.com/search?q=h&lang=zh', {'q': 'r', 'lang': 'en'})
    'http://google.com/search?q=r&lang=en'
    >>> update_url_param('http://google.com/search', {'q': 'r'})
    'http://google.com/search?q=r'
    """
    u = urlsplit(url)
    parsed_qs = parse_qs_dict(u.query)
    parsed_qs.update(params)
    query_string = urlencode(parsed_qs)
    return urlunsplit([u.scheme, u.netloc, u.path, query_string, u.fragment])


def parse_qs_dict(query_string):
    return dict(parse_qsl(query_string))


def build_url(url, params):
    """
    >>> build_url('http://foo.bar/search', {'q': 'foo'})
    'http://foo.bar/search?q=foo'
    >>> build_url('http://foo.bar/search?q=foo', {'a': 'a'})
    'http://foo.bar/search?q=foo&a=a'
    """
    u = urlsplit(url)
    parsed_qs = parse_qs_dict(u.query)
    parsed_qs.update(params)
    query = urlencode(parsed_qs)
    return urlunsplit([u.scheme, u.netloc, u.path, query, u.fragment])


def get_domain(url):
    u"""
    >>> get_domain('http://user:pass@google.com:8000')
    'google.com'
    >>> get_domain('http://google.com/headline.html')
    'google.com'
    """
    u = urlsplit(url)
    return re.sub(r':.*$', '', re.sub(r'^.*@', '', u.netloc))


def get_top_domain(url):
    u"""
    >>> get_top_domain('http://user:pass@www.google.com:8080')
    'google.com'
    >>> get_top_domain('http://www.sina.com.cn')
    'sina.com.cn'
    >>> get_top_domain('http://bbc.co.uk')
    'bbc.co.uk'
    >>> get_top_domain('http://mail.cs.buaa.edu.cn')
    'buaa.edu.cn'
    >>> get_top_domain('http://t.cn')
    't.cn'
    """
    domain = get_domain(url)
    domain_parts = domain.split('.')
    if len(domain_parts) < 2:
        return domain
    top_domain_parts = 2
    # if a domain's last part is 2 letter long, it must be country name
    if len(domain_parts[-1]) == 2:
        if domain_parts[-1] in ['uk', 'jp']:
            if domain_parts[-2] in ['co', 'ac', 'me', 'gov', 'org', 'net']:
                top_domain_parts = 3
        else:
            if domain_parts[-2] in ['com', 'org', 'net', 'edu', 'gov']:
                top_domain_parts = 3
    return '.'.join(domain_parts[-top_domain_parts:])


def normalize_url(url, remove_query=None, keep_query=None):
    """
    >>> normalize('http://google.com')
    'http://google.com/'
    >>> normalize('HTTP://User:Pass@Google.com:80')
    'http://User:Pass@google.com/'
    >>> normalize('http://google.com/foo/bar/../')
    'http://google.com/foo/'
    >>> normalize('http://google.com/foo')
    'http://google.com/foo'
    >>> normalize('http://google.com/foo/')
    'http://google.com/foo/'
    >>> normalize('httP://google.com/')
    'http://google.com/'
    >>> normalize('google.com/?a=c&a=b')
    'google.com/?a=b&a=c'
    >>> normalize('google.com/?b=a&a=a')
    'google.com/?a=a&b=a'
    >>> normalize('google.com//?b=a&a=a')
    'google.com/?a=a&b=a'
    >>> normalize('google.com/a/b/../?b=a&a=a')
    'google.com/a/?a=a&b=a'
    >>> normalize('google.com/?a=a&spam=shit', remove_query=['spam'])
    'google.com/?a=a'
    """

    u = urlsplit(url)

    # 1. scheme
    scheme = u.scheme.lower()

    # 2. netloc
    hostname = (u.hostname or '').lower()
    if hostname and hostname[-1] == '.':
        hostname = hostname[:-1] # remove ending dot
    if u.port == 80 and scheme == 'http' or u.port == 443 and scheme == 'https':
        port = ''
    else:
        port = u.port
    netloc = hostname
    if u.username:
        netloc = '{}:{}@{}'.format(u.username or '', u.password or '', netloc)
    if port:
        netloc = '{}:{}'.format(netloc, port)

    # 3. path
    fix_quote = lambda s: urlquote(urlunquote(s), safe=":=~+!$,;'@()*[]") # NOTE no /?&#
    path_parts = u.path.split('/')
    new_path_parts = []
    for i, p in enumerate(path_parts):
        if p == '.':
            continue
        elif p == '..' and len(new_path_parts) > 1:
            new_path_parts.pop()
        elif p != '' or i == 0 or i == len(path_parts) - 1:
            new_path_parts.append(p)
    new_path_parts = map(fix_quote, new_path_parts)
    path = '/'.join(new_path_parts)
    if not path:
        path = '/'

    # query
    qsl = parse_qsl(u.query)
    qsl.sort()
    if remove_query:
        qsl = [qs for qs in qsl if qs[0] not in remove_query]
    if keep_query:
        qsl = [qs for qs in qsl if qs[0] in keep_query]
    qsl = [(urlunquote(qs[0]), urlunquote(qs[1])) for qs in qsl]
    query = urlencode(qsl, safe=":=~+!$,;'@()*[]")

    return urlunsplit([scheme, netloc, path, query, u.fragment])

if __name__ == '__main__':
    import doctest
    doctest.testmod()
