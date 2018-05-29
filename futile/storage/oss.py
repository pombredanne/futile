# coding: utf-8


import oss2
import hashlib


def bucket_client(access_key_id, access_key_secret, endpoint, bucket_name):
    return oss2.Bucket(oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_name)
