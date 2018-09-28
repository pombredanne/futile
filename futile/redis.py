import redis
from .consul import lookup_service


def get_redis_client(conf):
    return redis.StrictRedis(conf.REDIS_IP, conf.REDIS_PORT)


def make_redis_client() -> redis.StrictRedis:
    addresses = lookup_service('inf.db.redis')
    ip, port = addresses[0]
    return redis.StrictRedis(ip, port)


def make_pika_client() -> redis.StrictRedis:
    addresses = lookup_service('inf.db.pika')
    ip, port = addresses[0]
    return redis.StrictRedis(ip, port)
