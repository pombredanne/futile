# encoding: utf-8


"""
This module is taken from the python-readability lib, which is Apache Licensed
"""


__all__ = ["html_decode"]

import re
import sys
from .strings import ensure_str

try:
    import cchardet as chardet
except ImportError:
    try:
        import chardet
    except ImportError:
        chardet = None


RE_CHARSET = re.compile(br'<meta.*?charset=["\']*(.+?)["\'>]', flags=re.I)
RE_PRAGMA = re.compile(br'<meta.*?content=["\']*;?charset=(.+?)["\'>]', flags=re.I)
RE_XML = re.compile(br'^<\?xml.*?encoding=["\']*(.+?)["\'>]')

CHARSETS = {
    "big5": "big5hkscs",
    "gb2312": "gb18030",
    "ascii": "utf-8",
    "maccyrillic": "cp1251",
    "win1251": "cp1251",
    "win-1251": "cp1251",
    "windows-1251": "cp1251",
}


def fix_charset(encoding):
    """
    Overrides encoding when charset declaration
    or charset determination is a subset of a larger
    charset.  Created because of issues with Chinese websites
    """
    encoding = ensure_str(encoding.lower())
    return CHARSETS.get(encoding, encoding)


def smart_decode(page, hint="utf-8"):
    """
    get the encoding of give page, and the decoded page

    :param lxml.HtmlDocument page
    :return the encoding
    """
    # Regex for XML and HTML Meta charset declaration
    declared_encodings = (
        RE_CHARSET.findall(page) + RE_PRAGMA.findall(page) + RE_XML.findall(page)
    )

    # Try any declared encodings
    for declared_encoding in declared_encodings:
        try:
            encoding = fix_charset(declared_encoding)
            # Now let's decode the page
            decoded_page = page.decode(encoding=encoding)
            # It worked!
            return encoding, decoded_page
        except UnicodeDecodeError:
            pass

    # Fallback to chardet if declared encodings fail
    # Remove all HTML tags, and leave only text for chardet
    text = re.sub(b"(\s*</?[^>]*>)+\s*", b" ", page).strip()
    if chardet:
        res = chardet.detect(text)
        encoding = res["encoding"] or "utf-8"
        encoding = fix_charset(encoding)
        try:
            return encoding, page.decode(encoding=encoding)
        except UnicodeDecodeError:
            pass

    # no way to decode
    return None, ""


if __name__ == "__main__":
    import requests

    r = requests.get(
        "https://s.1688.com/selloffer/offer_search.htm?keywords=%B4%BA%CF%C4%C1%AC%D2%C2%C8%B9"
    )
    import timeit

    def dec():
        return smart_decode(r.content)

    enc, page = dec()
    print(enc, page[:100])


    print(timeit.timeit(dec, number=100))
