import logging

from functools import wraps

from django.db import close_old_connections
from django.db.utils import OperationalError


def ensure_db_connection(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except OperationalError:
            logging.info("django db disconnected")
            close_old_connections()
            return fn(*args, **kwargs)

    return wrapped


if __name__ == "__main__":

    @ensure_db_connection
    def hello():
        print("hello")
        raise OperationalError

    hello()
