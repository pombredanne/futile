import redis
from .consul import lookup_service


def get_redis_client(conf):
    return redis.StrictRedis(conf.REDIS_IP, conf.REDIS_PORT)


def make_redis_client() -> redis.StrictRedis:
    addresses = lookup_service('db.redis')
    ip, port = addresses[0]
    return redis.StrictRedis(ip, port)

