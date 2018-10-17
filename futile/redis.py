import os
import redis
from .consul import lookup_service


def make_redis_client(conf=None) -> redis.StrictRedis:
    # addresses = lookup_service('inf.db.redis')
    # ip, port = addresses[0]
    ip = os.environ.get('REDIS_IP')
    port = os.environ.get('REDIS_PORT')
    return redis.StrictRedis(ip, port)


def make_pika_client(conf=None) -> redis.StrictRedis:
    # addresses = lookup_service('inf.db.pika')
    # ip, port = addresses[0]
    ip = os.environ.get('PIKA_IP')
    port = os.environ.get('PIKA_PORT')
    return redis.StrictRedis(ip, port)
