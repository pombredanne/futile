# coding: utf-8


__all__ = [
    "keep_run",
    "after",
    "before",
    "throttle",
    "rate_limited",
    "memoized",
    "no_raise",
    "lazyproperty",
]

import time
import sys
from functools import wraps
import threading
import logging
import pickle
from multiprocessing.pool import Pool

from .timeutil import parse_time_string


def keep_run(exception_sleep=10):
    """
    Keep a function running forever, if exception raised, sleep for a while

    This decorator can be used atop thread.run
    """

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
    """
    run a function only after n times
    """

    def decorate(fn):
        i = 0

        @wraps(fn)
        def wrapped(*args, **kwargs):
            nonlocal i
            i += 1
            if i >= n:
                return fn(*args, **kwargs)

        return wrapped

    return decorate


def before(n):
    """
    run a function only the first n times
    """

    def decorate(fn):
        i = 0

        @wraps(fn)
        def wrapped(*args, **kwargs):
            nonlocal i
            i += 1
            if i < n:
                return fn(*args, **kwargs)

        return wrapped

    return decorate


def throttle(wait=0, error_wait=0):

    cache_time = None  # 存在则表示已经有过缓存了
    cache_value = None

    if isinstance(wait, str):
        wait = parse_time_string(wait)

    if isinstance(error_wait, str):
        error_wait = parse_time_string(error_wait)

    def decorate(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            nonlocal cache_time
            nonlocal cache_value
            if cache_time and time.time() - cache_time < wait and cache_value:
                return cache_value
            cache_time = time.time()
            try:
                cache_value = fn(*args, **kwargs)
            except:
                cache_time = time.time() - wait + error_wait
                raise
            return cache_value
        return wrapped
    return decorate


class memoized:
    """
    Cache a function's reponse by arguments signature, args and kwargs are both
    supported.

    Deprecated, consider using functools.lru_cache
    """

    # TODO, fn and class method?
    def __init__(self, fn):
        self.fn = fn
        self.memo = {}

    def __call__(self, *args, **kwargs):
        key = pickle.dumps(args) + pickle.dumps(kwargs)
        if key not in self.memo:
            logging.debug("miss")
            self.memo[key] = self.fn(*args, **kwargs)
        else:
            logging.debug("hit")
        return self.memo[key]


def no_raise(exceptions=Exception, default=None):
    """
    Instead of raising exceptions, return boolean values to indicate errors.
    """

    @wraps
    def decorate(fn):
        def wrapped(*args, **kwargs):
            try:
                return True, fn(*args, **kwargs)
            except exceptions:
                return False, default

        return wrapped

    return decorate


def rate_limited(max_qps):
    """
    the decorated function can be called only `max_qps` per second, otherwise it's blocked.
    """
    lock = threading.Lock()
    min_interval = 1.0 / max_qps
    get_time = time.perf_counter if sys.version_info.major > 2 else time.clock

    def decorate(fn):
        last_time_called = get_time()

        @wraps(fn)
        def wrapped(*args, **kwargs):
            nonlocal last_time_called
            with lock:
                elapsed = get_time() - last_time_called
                left_to_wait = min_interval - elapsed
                if left_to_wait > 0:
                    time.sleep(left_to_wait)
                ret = fn(*args, **kwargs)
                last_time_called = get_time()
            return ret

        return wrapped

    return decorate


def synchronized(lock=None):
    """
    Synchronization decorator.
    """
    if lock is None:
        lock = threading.Lock()

    def wrap(f):
        def locked(*args, **kw):
            lock.acquire()
            try:
                return f(*args, **kw)
            finally:
                lock.release()

        return locked

    return wrap


class lazyproperty:
    """
    >>> import math
    >>> class Square:
    ...     def __inti__(self, width):
    ...         self.width = width
    ...     @lazyproperty
    ...     def area(self):
    ...         ''' only computed on first call'''
    ...         return self.width ** 2
    >>> c = Square(5)
    >>> c.area
    25
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


def singleton(class_):
    instances = {}
    lock = threading.Lock()

    def get_instance(*args, **kwargs):
        with lock:
            if class_ not in instances:
                instances[class_] = class_(*args, **kwargs)
            return instances[class_]

    return get_instance


def run_in_pool(*args, **kwargs):
    pool = Pool(*args, **kwargs)

    def wrapper(fn):
        def wrapped(*args, **kwargs):
            return pool.apply(fn, args, kwargs)

        return wrapped

    return wrapper


if __name__ == "__main__":

    @after(2)
    def hello_after():
        print("hello after 2")

    @before(2)
    def hello_before():
        print("hello before 2")

    @throttle(wait=1)
    def throttled():
        return time.time()

    while True:
        print(throttled())
        time.sleep(.1)

    print("first time")
    hello_after()
    hello_before()
    print("second time")
    hello_after()
    hello_before()

    print("testing rate_limited decorator")

    @rate_limited(max_qps=10)
    def print_number(num):
        print(int(time.time()), num)

    for i in range(100):
        print_number(i)
