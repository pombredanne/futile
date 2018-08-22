# coding: utf-8


import oss2
import hashlib

from futile.consul import lookup_service, lookup_kv


def make_bucket(bucket_name):
    access_key = lookup_kv(f'oss/{bucket_name}/access_key').decode()
    access_secret = lookup_kv(f'oss/{bucket_name}/access_secret').decode()
    endpoint = lookup_kv(f'oss/{bucket_name}/endpoint').decode()
    auth = oss2.Auth(access_key, access_secret)
    return oss2.Bucket(auth, endpoint, bucket_name)

