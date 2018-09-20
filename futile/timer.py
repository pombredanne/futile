# coding: utf-8


import time
import logging


class Timer:

    def __init__(self, name, logger=None):
        self._name = name
        self._start_time = time.time()
        self._last_time = self._start_time
        self.L = logger if logger is not None else logging
        self.delays = {}

    def time(self, name):
        now = time.time()
        total_delay = now - self._start_time
        self.total_delay = total_delay
        delay = now - self._last_time
        self.delays[name] = delay
        self.L.info("[%s] total delay = %.3f, %s = %.3f",
                    self._name, total_delay, name, delay)
        self._last_time = now


def timing(fn):
    def timed(*args, **kwargs):
        start_ts = time.time()
        ret = fn(*args, **kwargs)
        end_ts = time.time()
        print("fn: %s execute time: %s" % (fn.__name__, end_ts - start_ts))
        return ret
    return timed
