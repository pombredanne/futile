#!/usr/bin/env python
# coding: utf-8


"""
some functions on html parsing
"""

import re
import lxml.html

from futile.encoding import smart_decode


def build_doc(page, url=None, encoding=None):
    """build lxml doc from bytes or unicode"""
    if isinstance(page, bytes):
        if not encoding:
            _, page = smart_decode(page)
        else:
            page = page.decode(encoding)
    # TODO fallback to html5lib parser when failed
    doc = lxml.html.document_fromstring(page)
    doc.resolve_base_href(handle_failures='ignore')
    if url is not None:
        doc.make_links_absolute(url)
    return doc


def mget_re_val(page, res, *, default=""):
    """
    >>> page = 'a=123456'
    >>> mget_re_val(page, {'number': ('\d+', 0)})
    {'number': '123456'}
    """
    ret = {}
    for key, options in res.items():
        if isinstance(options, str):
            pattern = options
            group = 1
        else:
            pattern, group = options
        m = re.search(pattern, page)
        if m:
            ret[key] = m.group(group)
        else:
            ret[key] = default
    return ret


def get_re_val(page, pattern, group=1, default=""):
    """
    >>> page = '123456'
    >>> get_re_val(page, '\d+', 0)
    '123456'
    """
    return mget_re_val(page, {"_": (pattern, group)}, default=default)["_"]


def get_js_variable(page, variable):
    """
    >>> page = '''
    ... var is_follow = "";
    ... var nickname = "Golang语言社区";
    ... var appmsg_type = "9";
    ... var ct = "1531495140";
    ... var publish_time = "2018-07-13" || "";
    ... var user_name = "gh_921b7321b3f3";
    ... '''
    >>> get_js_variable(page, 'nickname')
    'Golang语言社区'
    >>> get_js_variable(page, 'publish_time')
    '2018-07-13'
    """
    pattern = r'var\s+%s\s*=\s*"(.*?)"' % variable
    return get_re_val(page, pattern)


def mget_xpath_val(
    page_or_doc, xpaths, *, url=None, multi=False, encoding=None, default=""
):
    """
    >>> page = '<foo><bar class="hello">hello</bar></foo>'
    >>> mget_xpath_val(page, xpaths={'hello': '//*[@class="hello"]/text()'})
    {'hello': 'hello'}
    """
    if isinstance(page_or_doc, (str, bytes)):
        doc = build_doc(page_or_doc, url=url, encoding=encoding)
    else:
        doc = page_or_doc
    result = {}
    for key, xpath in xpaths.items():
        try:
            if multi:
                result[key] = doc.xpath(xpath)
            else:
                result[key] = doc.xpath(xpath)[0]
        except Exception:  # pylint: disable=broad-except
            result[key] = default
    return result


def get_xpath_val(
    page_or_doc, xpath, *, url=None, multi=False, encoding=None, default=""
):
    """
    >>> page = '<foo><bar class="hello">hello</bar></foo>'
    >>> get_xpath_val(page, xpath='//*[@class="hello"]/text()')
    'hello'
    """
    result = mget_xpath_val(
        page_or_doc,
        {"_": xpath},
        url=url,
        multi=multi,
        encoding=encoding,
        default=default,
    )
    return result["_"]


if __name__ == "__main__":
    import doctest

    doctest.testmod()
