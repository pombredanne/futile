#!/usr/bin/env python
# coding: utf-8


from .conf import Conf
from .redis import get_redis_client


def conf2proxy(conf):
    user = conf.PROXY_USERNAME
    password = conf.PROXY_PASSWORD
    ip = conf.PROXY_IP
    port = conf.PROXY_PORT
    return f'http://{username}:{password}@{ip}:{port}'


def proxy_join(username: str='root',
               password: str='20180606',
               ip: str='127.0.0.1',
               port: str='20017'
               ) -> str:
    return f'http://{username}:{password}@{ip}:{port}'

