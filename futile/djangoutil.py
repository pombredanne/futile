from functools import wraps

from django.db import close_old_connections
from django.db.utils import OperationalError


def ensure_db_connection(fn):
    @wraps
    def wrapped(*args, **kwargs):
        try:
            fn(*args, **kwargs)
        except OperationalError:
            close_old_connections()
            fn(*args, **kwargs)
    return wrapped
