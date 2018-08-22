import os
import sys
import logging
import logging.handlers


def get_logger(name, level=logging.DEBUG):
    """
    生成一个logger，日志会交给上层的logger处理
    """
    logger = logging.getLogger(name)
    logger.propagate = True
    if not logger.handlers:
        handler = logging.NullHandler()
        logger.addHandler(handler)
        # handler 不设置级别
        logger.setLevel(level)
    return logger


def init_log(script_name,
             console_level=logging.INFO,
             file_level=logging.INFO,
             additional_handlers=None):
    root_logger = logging.getLogger('')
    root_logger.handlers = []
    formatter = logging.Formatter('%(asctime)s-%(name)s-%(threadName)s-%(levelname)s - %(message)s - %(filename)s:%(lineno)d')

    def exception_hook(type, value, tb):
        import traceback
        root_logger.exception('uncaught error %s',
                              ''.join(traceback.format_exception(type, value, tb)))

    sys.excepthook = exception_hook

    # add file logger
    if os.environ.get('DEBUG'):
        home = os.environ.get('HOME')
        log_path = f'{home}/log/{script_name}.log'
        # add console logger
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(console_level)
        root_logger.addHandler(console_handler)
    else:
        log_path = f'/var/log/{script_name}.log'

    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=log_path,
        when='D',
        backupCount=7,
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(file_level)
    root_logger.addHandler(file_handler)

    if additional_handlers:
        for handler in additional_handlers:
            root_logger.addHandler(handler)
