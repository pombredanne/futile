#!/user/bin/env python
# coding: utf-8


import random


def get_page(url):
    import requests
    try:
        headers = {'User-Agent': get_random_desktop_ua()}
        r = requests.get(url, headers=headers, timeout=10)
        return r.text
    except Exception:
        return None


def get_random_desktop_ua():
    """generate a random user-agent of desktop browser"""
    platforms = ['Windows NT 6.1', 'Macintosh; Intel Mac OS X 10_10_1', 'Windows NT 6.1; WOW64']
    products = [
        {
            'engines': ['AppleWebKit/537.36 (KHTML, like Gecko)'],  # chrome claims to based on webkit, not blink
            'name': 'Chrome',
            'version': ['58.0.3029.{}'.format(i) for i in range(100)],
            'base_product': 'Safari/537.36'
        },
        {
            'engines': ['Gecko/20100101'],  # gecko version number
            'name': 'Firefox',
            'version': ['{}.0'.format(i) for i in range(40, 60)],
            'base_product': ''
        }
    ]

    product = random.choice(products)

    return 'Mozilla/5.0 ({platform}) {engine} {name}/{version} {base_product}'.format(
        platform=random.choice(platforms),
        engine=random.choice(product.get('engines')),
        name=product.get('name'),
        version=random.choice(product.get('version')),
        base_product=product.get('base_product')
    )


def get_random_mobile_ua():
    user_agents = [
        'Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5376e Safari/8536.25',
        'Mozilla/5.0 (Linux; Android 4.4.2; Nexus 4 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.114 Mobile Safari/537.36',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 10_0_2 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) Mobile/14A456 MicroMessenger/6.3.27 NetType/WIFI Language/zh_CN',
        'Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5376e Safari/8536.25',
    ]
    return random.choice(user_agents)


def get_random_bot_ua():
    user_agents = [
        'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
        'Mozilla/5.0 (compatible; Bingbot/2.0; +http://www.bing.com/bingbot.htm)',
        'Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)',
        'DuckDuckBot/1.0; (+http://duckduckgo.com/duckduckbot.html)',
        'Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)',
        'Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)',
        'Sogou web spider/4.0(+http://www.sogou.com/docs/help/webmasters.htm#07)',
    ]
    return random.choice(user_agents)


def is_mobile_ua(ua):
    ua = str(ua)
    return 'Mobile' in ua or 'Android' in ua or 'Mobi' in ua or 'Windows Phone' in ua
