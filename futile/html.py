#!/usr/bin/env python
# coding: utf-8


"""
some functions on html parsing
"""

import re
import lxml.html
try:
    import cchardet as chardet
except ImportError:
    import chardet

from .encoding import try_decode


utf8_parser = lxml.html.HTMLParser(encoding='utf-8', remove_comments=True)  # pylint: disable=invalid-name


def build_doc(page, url=None, encoding=None):
    """build lxml doc from bytes or unicode"""
    if isinstance(page, str):
        decoded_page = page
    else:
        _, decoded_page = try_decode(page)
    # NOTE: we have to do .decode and .encode even for utf-8 pages to remove bad characters
    doc = lxml.html.document_fromstring(decoded_page.encode('utf-8', 'replace'), parser=utf8_parser)
    #doc.resovle_base_href(handle_failures='ignore')
    if url is not None:
        doc.make_links_absolute(url)
    return doc


def mget_re_val(page, res, *, default=""):
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
    return mget_re_val(page, {'_': (pattern, group)}, default=default)['_']


def mget_xpath_val(page_or_doc, xpaths, *, url=None, multi=False, encoding='utf-8',
                   default=""):
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


def get_xpath_val(page_or_doc, xpath, *, url=None, multi=False, encoding='utf-8', default=""):
    """
    >>> page = '<foo><bar class="hello">hello</bar></foo>'
    >>> get_xpath_val(page, xpath='//*[@class="hello"]/text()')
    'hello'
    """
    result = mget_xpath_val(page_or_doc, {'_': xpath},
                            url=url, multi=multi, encoding=encoding, default=default)
    return result['_']


if __name__ == '__main__':
    import doctest
    doctest.testmod()
