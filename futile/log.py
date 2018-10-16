import os
import sys
import logging
import logging.handlers
import threading


def setup_thread_excepthook():
    """
    Workaround for `sys.excepthook` thread bug from:
    http://bugs.python.org/issue1230540

    Call once from the main thread before creating any threads.
    """

    init_original = threading.Thread.__init__

    def init(self, *args, **kwargs):

        init_original(self, *args, **kwargs)
        run_original = self.run

        def run_with_except_hook(*args2, **kwargs2):
            try:
                run_original(*args2, **kwargs2)
            except Exception:
                sys.excepthook(*sys.exc_info())

        self.run = run_with_except_hook

    threading.Thread.__init__ = init


class PrefixLogger:
    def __init__(self, prefix=None):
        self._prefix = prefix

    def set_prefix(self, prefix):
        self._prefix = prefix

    def __getattr__(self, attr):
        def log(self, logstr, *args, **kwargs):
            _log = getattr(self._logger, attr)
            _log(f"{self._prefix} {lostr}", *args, **kwargs)

        return log


def get_logger(name, level=logging.DEBUG):
    """
    生成一个logger，日志会交给上层的logger处理
    """
    if isinstance(level, str):
        level = getattr(logging, level.upper())

    logger = logging.getLogger(name)
    logger.propagate = True
    if not logger.handlers:
        handler = logging.NullHandler()
        logger.addHandler(handler)
        # handler 不设置级别
        logger.setLevel(level)
    return logger


def init_log(
    script_name,
    console_level=logging.INFO,
    file_level=logging.INFO,
    additional_handlers=None,
):

    if isinstance(console_level, str):
        console_level = getattr(logging, console_level.upper())

    if isinstance(file_level, str):
        file_level = getattr(logging, file_level.upper())

    root_logger = logging.getLogger("")
    root_logger.handlers = []
    formatter = logging.Formatter(
        "%(asctime)s-[%(name)s]-%(threadName)s-%(levelname)s - %(message)s - %(filename)s:%(lineno)d"
    )

    def exception_hook(type, value, tb):
        import traceback

        root_logger.exception(
            "uncaught error %s", "".join(traceback.format_exception(type, value, tb))
        )

    sys.excepthook = exception_hook
    setup_thread_excepthook()

    # add file logger
    if os.environ.get("DEBUG"):
        home = os.environ.get("HOME")
        log_path = f"{home}/log/{script_name}.log"
        # add console logger
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(console_level)
        root_logger.addHandler(console_handler)
    else:
        log_path = f"/var/log/{script_name}.log"

    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=log_path, when="D", backupCount=7
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(file_level)
    root_logger.addHandler(file_handler)

    if additional_handlers:
        for handler in additional_handlers:
            root_logger.addHandler(handler)
