import time
import logging
from functools import wraps

from . import metrics2 as metrics


class Timer:
    # TODO thread safety
    # TODO support coroutine
    def __init__(
        self,
        task,
        *,
        enable_log=False,
        logger=None,
        send_metrics=False,
        tags=None,
        parent=None,
    ):
        if parent:
            self._task = f"{parent._task}.{task}"
        else:
            self._task = task
        # rechekc in __enter__
        self._start_time = time.time() * 1000
        self._last_time = self._start_time
        self._enable_log = enable_log
        self._logger = logger if logger is not None else logging
        self._send_metrics = send_metrics
        self._tags = {"_task": self._task}
        if tags is not None:
            self._tags.update(tags)
        self._parent = parent

        self.delays = {}

    def __enter__(self):
        self._start_time = time.time() * 1000
        self._last_time = self._start_time
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        status = "success" if exc_type is None else "failed"
        self.close(status=status)

    def time(self, subtask):
        now = time.time() * 1000
        delay = now - self._last_time
        self._last_time = now

        self.delays[subtask] = delay
        if self._enable_log:
            self._logger.info(
                "[timer-%s] total=%.3fms, %s=%.3fms, percent=%.2f%%",
                self._task,
                self.get_total(),
                subtask,
                delay,
                delay / self.get_total() * 100,
            )
        if self._send_metrics:
            tags = {"subtask": subtask, **self._tags}
            metrics.emit_timer("timer", delay, tags=tags)

        return delay

    def close(self, status="success"):
        self._last_time = time.time() * 1000
        if self._enable_log:
            self._logger.info(
                "[timer-%s] total=%.3fms, status=%s",
                self._task,
                self.get_total(),
                status,
            )
        if self._send_metrics:
            tags = {"status": status, "subtask": "_total", **self._tags}
            metrics.emit_timer("timer", self.get_total(), tags=tags)

    def get(self, name):
        return self.delays.get(name, 0)

    def get_total(self):
        return self._last_time - self._start_time


def timing(logger):
    def wrapper(fn):
        @wraps(fn)
        def wrapped(*args, **kw):
            with Timer(
                fn.__name__, logger=logger, send_metrics=True
            ) as _timer:  # noqa: F841
                return fn(*args, **kw)

        wrapped._timing = True
        return wrapped

    return wrapper
