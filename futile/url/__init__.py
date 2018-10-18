#!/usr/bin/env python
# coding: utf-8


__all__ = ['depack_jsonp', 'json_api', 'jsonp_api', 'mget_url_param',
           'get_url_param', 'update_url_param', 'parse_qs_dict',
           'build_url', 'get_domain', 'get_main_domain', 'normalize_url']

import re
import os
import json
import requests
from ..file import read_list_from_file
from ..strings import ensure_str
from urllib.parse import quote as urlquote, unquote as urlunquote, urlsplit, \
        urlunsplit, urlencode, parse_qsl

PUBLIC_SUFFIX_FILE = os.path.join(os.path.dirname(__file__), 'public_suffix.dat')
PUBLIC_SUFFIX = set(read_list_from_file(PUBLIC_SUFFIX_FILE, comment='//'))


def depack_jsonp(jsonp):
    """
    return json dict str from a jsonp str, if not found, return None

    >>> print(depack_jsonp('json_callback({"a": "b"})'))
    '{"a": "b"}'
    >>> print(depack_jsonp(''))
    None
    """
    try:
        # by standard, json data should be wrapped in a dict
        return re.search(r'[\w\.&\s]+?\((\{.*?\})\)', jsonp).group(1)
    except Exception:
        return None


def _download(url):
    return requests.get(url).text


def json_api(url, download_func=_download):
    """
    download and return the json data from a json api

    >>> download_func = lambda _: '{"a": "b"}'
    >>> json_api('http://example.com', download_func)
    {'a': 'b'}
    """
    return json.loads(download_func(url))


def jsonp_api(url, download_func=_download):
    """
    download and return the json data from a jsonp api in one step

    >>> download_func = lambda _: 'jsonp({"a": "b"})'
    >>> jsonp_api('http://example.com', download_func)
    {'a': 'b'}
    """
    jsonp = download_func(url)
    if not jsonp:
        return None
    json_data = json.loads(depack_jsonp(jsonp))
    return json_data


def mget_url_param(url: str, params: list, default=None) -> dict:
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


def get_url_param(url: str, param: list, default=None):
    """
    >>> get_url_param('http://foo.bar?a=a&b=b&c=c', 'a')
    'a'
    """
    result = mget_url_param(url, [param], default)
    return result[param]


def update_url_param(url: str, params: dict) -> str:
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


def parse_qs_dict(query_string: str) -> dict:
    query_string = ensure_str(query_string)
    return dict(parse_qsl(query_string))


def build_url(url: str, params: dict) -> str:
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


def get_domain(url: str) -> str:
    """
    >>> get_domain('http://user:pass@google.com:8000')
    'google.com'
    >>> get_domain('http://google.com/headline.html')
    'google.com'
    """
    url = url.lower()
    u = urlsplit(url)
    return re.sub(r':.*$', '', re.sub(r'^.*@', '', u.netloc))


def get_main_domain(url: str) -> str:
    """
    >>> get_main_domain('http://user:pass@www.google.com:8080')
    'google.com'
    >>> get_main_domain('http://www.sina.com.cn')
    'sina.com.cn'
    >>> get_main_domain('http://bbc.co.uk')
    'bbc.co.uk'
    >>> get_main_domain('http://mail.cs.buaa.edu.cn')
    'buaa.edu.cn'
    >>> get_main_domain('http://t.cn')
    't.cn'
    >>> get_main_domain('http://www.gov.cn')  # 这里注册的域名是 www
    'www.gov.cn'
    >>> get_main_domain('http://com.cn')  # 实际上这是不合法的 url，因为 com.cn 是顶级域名
    'com.cn'
    """
    domain = get_domain(url)
    domain_parts = domain.split('.')
    if len(domain_parts) <= 2:
        return domain
    for i in reversed(range(len(domain_parts))):
        possible_suffix = '.'.join(domain_parts[-i:])
        if possible_suffix in PUBLIC_SUFFIX:
            return '.'.join(domain_parts[-i - 1:])
    return domain


def normalize_url(url: str, *, drop_params=None, keep_params=None,
                  drop_fragment=True) -> str:
    """
    >>> normalize_url('http://google.com')
    'http://google.com/'
    >>> normalize_url('HTTPS://User:Pass@Google.com:80')
    'https://User:Pass@google.com/'
    >>> normalize_url('http://google.com/foo/bar/../')
    'http://google.com/foo/'
    >>> normalize_url('http://google.com/foo')
    'http://google.com/foo'
    >>> normalize_url('http://google.com/foo/')
    'http://google.com/foo/'
    >>> normalize_url('httP://google.com/')
    'http://google.com/'
    >>> normalize_url('google.com/?a=c&a=b')
    'http://google.com/?a=b&a=c'
    >>> normalize_url('google.com/?b=a&a=a')
    'http://google.com/?a=a&b=a'
    >>> normalize_url('google.com//?b=a&a=a')
    'http://google.com/?a=a&b=a'
    >>> normalize_url('google.com/a/b/../?b=a&a=a')
    'http://google.com/a/?a=a&b=a'
    >>> normalize_url('google.com/?a=a&spam=shit', remove_params=['spam'])
    'http://google.com/?a=a'
    >>> normalize_url('http://google.com/a/b/c/d/e')
    'http://google.com/a/b/c/d/e'
    >>> normalize_url('http://weaweadsf:asdfasdfasdf')
    'http://weaweadsf:asdfasdfasdf'
    """
    try:
        u = urlsplit(url)

        # 1. scheme
        scheme = u.scheme.lower()
        if not scheme:
            scheme = 'http'

        # 2. netloc
        hostname = (u.hostname or '').lower()
        if hostname and hostname[-1] == '.':
            hostname = hostname[:-1]  # remove ending dot
        # python stdlib may throw exception here when port is not a number
        if u.port == 80 and scheme == 'http' or u.port == 443 and scheme == 'https':
            port = ''
        else:
            port = u.port
        netloc = hostname
        if u.username:
            netloc = f"{u.username or ''}:{u.password or ''}@{netloc}"
        if port:
            netloc = f'{netloc}:{port}'

        # 3. path
        path_parts = u.path.split('/')
        new_path_parts = []
        for i, p in enumerate(path_parts):
            if p == '.':
                continue
            elif p == '..' and len(new_path_parts) > 1:
                new_path_parts.pop()
            elif p != '' or i == 0 or i == len(path_parts) - 1:
                new_path_parts.append(p)

        def fix_quote(s):
            return urlquote(urlunquote(s), safe=":=~+!$,;'@()*[]")  # NOTE no /?&#
        new_path_parts = map(fix_quote, new_path_parts)
        path = '/'.join(new_path_parts)
        if not path:
            path = '/'

        # 4. query
        qsl = parse_qsl(u.query)
        qsl.sort()
        if drop_params is not None:
            qsl = [qs for qs in qsl if qs[0] not in drop_params]
        if keep_params is not None:  # empty means keep nothing at all
            qsl = [qs for qs in qsl if qs[0] in keep_params]
        qsl = [(urlunquote(qs[0]), urlunquote(qs[1])) for qs in qsl]
        query = urlencode(qsl, safe=":=~+!$,;'@()*[]")

        # 5. fragment
        if drop_fragment:
            fragment = ''
        else:
            fragment = u.fragment

        return urlunsplit([scheme, netloc, path, query, fragment])
    except Exception as e:
        raise Exception('url=%s normalize failed' % url)


if __name__ == '__main__':
    import doctest
    doctest.testmod()
