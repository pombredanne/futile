import sys
import logging

def get_logger(name, stdout=True, level=logging.DEBUG):
    """
    生成一个logger，日志会交给上层的logger处理
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        if stdout:
            handler = logging.StreamHandler(sys.stdout)
        else:
            handler = logging.NullHandler()
        formatter = logging.Formatter('%(asctime)s-%(name)s-%(threadName)s-%(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        # handler 不设置级别
        logger.setLevel(level)
    return logger

def setup_log_level(level):
    if isinstance(level, str):
        level = getattr(logging, level.upper())
    logging.basicConfig(level=level)

