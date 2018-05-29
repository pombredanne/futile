# coding: utf-8


__all__ = ['keep_run', 'after', 'before', 'throttle', 'rate_limited', 'memoized',
           'no_raise', 'lazyproperty']

import time
import sys
from functools import wraps
import threading
import logging
import pickle


def keep_run(exception_sleep=10):
    """Keep a function running forever, if exception raised, sleep for a while
    Can be used in thread.run"""

    def decorated(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                logging.exception(e)
                if exception_sleep > 0:
                    time.sleep(exception_sleep)
        return wrapped
    return decorated

def after(n):
    def decorate(fn):
        i = [0] # work around for nonlocal in python2
        @wraps(fn)
        def wrapped(*args, **kwargs):
            i[0] += 1
            if i[0] >= n:
                return fn(*args, **kwargs)
        return wrapped
    return decorate

def before(n):
    def decorate(fn):
        i = [0] # work around for nonlocal in python2
        @wraps(fn)
        def wrapped(*args, **kwargs):
            i[0] += 1
            if i[0] < n:
                return fn(*args, **kwargs)
        return wrapped
    return decorate

def throttle(wait=0, error_wait=0):

    class context: # work around for nonlocal in python 2
        cache_time = None
        cache_value = None

    def decorate(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            if context.cache_time and context.cache_time + wait > time.time():
                while True:
                    if context.cache_value:
                        pass

class memoized(object):
    """cache a function's reponse by arguments signature, args and kwargs are both supported,
    deprecated, consider using functools.lru_cache"""
    # TODO, fn and class method?
    def __init__(self, fn):
        self.fn = fn
        self.memo = {}
    def __call__(self, *args, **kwargs):
        key = pickle.dumps(args) + pickle.dumps(kwargs)
        if key not in self.memo:
            logging.debug('miss')
            self.memo[key] = self.fn(*args, **kwargs)
        else:
            logging.debug('hit')
        return self.memo[key]

def no_raise(exceptions=Exception, default=None):
    @wraps
    def decorate(fn):
        def wrapped(*args, **kwargs):
            try:
                return 'success', fn(*args, **kwargs)
            except exceptions:
                return 'failed', default
        return wrapped
    return decorate

def rate_limited(max_qps):
    """the decorated function can be called only `max_qps` per second, otherwise it's blocked."""
    lock = threading.Lock()
    min_interval = 1.0 / max_qps
    get_time = time.perf_counter if sys.version_info.major > 2 else time.clock

    def decorate(fn):
        last_time_called = [get_time()] # NOTE the array is a workaround for nonlocal access
        @wraps(fn)
        def wrapped(*args, **kwargs):
            with lock:
                elapsed = get_time() - last_time_called[0]
                left_to_wait = min_interval - elapsed
                if left_to_wait > 0:
                    time.sleep(left_to_wait)
                ret = fn(*args, **kwargs)
                last_time_called[0] = get_time()
            return ret
        return wrapped
    return decorate


def synchronized(lock):
    """ Synchronization decorator."""

    def wrap(f):
        def locked(*args, **kw):
            lock.acquire()
            try:
                return f(*args, **kw)
            finally:
                lock.release()
        return locked
    return wrap



class lazyproperty(object):
    """
    >>> import math
    >>> class Circle:
    ...     def __inti__(self, radius):
    ...         self.radius = radius
    ...     @lazyproperty
    ...     def area(self):
    ...         ''' only computed on first call'''
    ...         return math.pi * self.radius ** 2
    >>> c = Circle(5)
    """

    def __init__(self, fn):
        self.fn = fn

    def __get__(self, obj, cls):
        if obj is None:
            return self
        else:
            value = self.fn(obj)
        setattr(obj, self.fn.__name__, value)
        return value


if __name__ == '__main__':
    @after(2)
    def hello_after():
        print('hello after 2')

    @before(2)
    def hello_before():
        print('hello before 2')
    print('first time')
    hello_after()
    hello_before()
    print('second time')
    hello_after()
    hello_before()

    print('testing rate_limited decorator')

    @rate_limited(max_qps=10)
    def print_number(num):
        print(int(time.time()), num)
    for i in range(100):
        print_number(i)
