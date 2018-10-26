# coding: utf-8

from .log import get_logger


DRY_RUN = False
L = get_logger('[Dry Util]')


# DEPRECATED
def save(obj, *, dry_run=False):
    """
    保存对象
    """
    if dry_run:
        L.info('saving object %s', obj)
    else:
        obj.save()


# DEPRECATED
def update(obj, *, dry_run=False, **kwargs):
    """
    更新对象的属性
    """
    if dry_run:
        L.info('update object %s with %s', obj, kwargs)
    else:
        for k, v in kwargs.items():
            setattr(obj, k, v)
        obj.save()


# DEPRECATED
def get_or_create(Model, *, dry_run=False, defaults=None, **kwargs):
    if dry_run:
        L.info('get_or_create model %s, defaults=%s, kwargs=%s', Model,
               defaults, kwargs)
        if defaults:
            kwargs.update(defaults)
        obj = Model()
        for k, v in kwargs.items():
            setattr(obj, k, v)
        return obj, False
    else:
        return Model.objects.get_or_create(defaults, **kwargs)


# DEPRECATED
def create(Model, *, dry_run=False, **kwargs):
    if dry_run:
        L.info('create %s, with args %s', Model, kwargs)
    else:
        Model.objects.create(**kwargs)


class DryUtil:

    def __init__(self, dry_run):
        self._dry_run = dry_run

    def save(self, obj):
        """
        保存对象
        """
        if self._dry_run:
            L.info('saving object %s', obj)
        else:
            obj.save()

    def update(self, obj, **kwargs):
        """
        更新对象的属性
        """
        if self._dry_run:
            L.info('update object %s with %s', obj, kwargs)
        else:
            for k, v in kwargs.items():
                setattr(obj, k, v)
            obj.save()

    def get_or_create(self, Model, *, defaults=None, **kwargs):
        if self._dry_run:
            L.info('get_or_create model %s, defaults=%s, kwargs=%s', Model,
                   defaults, kwargs)
            if defaults:
                kwargs.update(defaults)
            obj = Model()
            for k, v in kwargs.items():
                setattr(obj, k, v)
            return obj, False
        else:
            return Model.objects.get_or_create(defaults, **kwargs)

    def update_or_create(self, Model, *, defaults=None, **kwargs):
        if self._dry_run:
            L.info('update_or_create model %s, defaults=%s, kwargs=%s', Model,
                   defaults, kwargs)
            if defaults:
                kwargs.update(defaults)
            obj = Model()
            for k, v in kwargs.items():
                setattr(obj, k, v)
            return obj, False
        else:
            return Model.objects.update_or_create(defaults, **kwargs)

    def create(self, Model, **kwargs):
        if self._dry_run:
            L.info('create %s, with args %s', Model, kwargs)
        else:
            Model.objects.create(**kwargs)
