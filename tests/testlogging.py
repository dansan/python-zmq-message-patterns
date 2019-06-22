# -*- coding: utf-8 -*-

import sys
import logging

LOG_FORMATS = dict(
    DEBUG='%(asctime)s %(module)s.%(funcName)s:%(lineno)d  %(levelname)s [{pid}] %(message)s',
    INFO='%(asctime)s [{pid}] %(message)s'
)
LOG_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'


def update_formatter(logger, pid):  # type: (logging.Logger, int) -> None
    for handler in logger.handlers:
        if handler.level > logging.DEBUG:
            fmt = LOG_FORMATS['INFO']
        else:
            fmt = LOG_FORMATS['DEBUG']
        fmt = fmt.format(pid=pid)
        handler.setFormatter(logging.Formatter(fmt=fmt, datefmt=LOG_DATETIME_FORMAT))


def setup_logging(name, log_level, pid):  # type: (str, int, int) -> logging.Logger
    logger = logging.getLogger(name)
    if any(map(lambda x: isinstance(x, logging.StreamHandler), logger.handlers)):
        return logger
    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setLevel(log_level)
    logger.setLevel(log_level)
    logger.addHandler(sh)
    update_formatter(logger, pid)
    return logger
