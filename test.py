# coding: utf-8


import requests
from futile.html import build_doc
import lxml.html

doc = build_doc(requests.get('http://finance.sina.com.cn/stock/jsy/2017-11-14/doc-ifynstfh7937123.shtml').content)
print(lxml.html.tostring(doc, encoding='unicode', pretty_print=True))
