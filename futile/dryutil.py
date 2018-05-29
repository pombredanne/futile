# coding: utf-8

from .log import get_logger


dry_run = False
L = get_logger('[Dry Util]')


def save(obj):
    if dry_run:
        L.info('saving object %s', obj)
    else:
        obj.save()
