"""This module implements utility classes and functions for logging."""
import logging
from logging.handlers import RotatingFileHandler
from typing import Any

IMPORTANT = 25
logging.addLevelName(IMPORTANT, 'IMPORTANT')


class TermColor(object):
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    ITALICS = '\033[3m'

    @classmethod
    def bold(cls, text):
        return '{}{}{}'.format(cls.BOLD, text, cls.ENDC)

    @classmethod
    def warn(cls, text):
        return '{}{}{}'.format(cls.WARNING, text, cls.ENDC)

    @classmethod
    def success(cls, text):
        return '{}{}{}'.format(cls.OKGREEN, text, cls.ENDC)

    @classmethod
    def error(cls, text):
        return '{}{}{}'.format(cls.FAIL, text, cls.ENDC)

    @classmethod
    def emphasis(cls, text):
        return '{}{}{}'.format(cls.BOLD, text, cls.ENDC)

    @classmethod
    def debug(cls, text):
        return '{}{}{}'.format(cls.OKBLUE, text, cls.ENDC)

    @classmethod
    def normal(cls, text):
        return text

    @classmethod
    def underline(cls, text):
        return '{}{}{}'.format(cls.UNDERLINE, text, cls.ENDC)

    @classmethod
    def italics(cls, text):
        return '{}{}{}'.format(cls.ITALICS, text, cls.ENDC)


class NullHandler(logging.Handler):
    """A do-nothing log handler."""

    def emit(self, record):
        pass


class ColoredConsoleHandler(logging.StreamHandler):
    def format(self, record):
        msg = super(ColoredConsoleHandler, self).format(record)
        if record.levelno == logging.WARNING:
            msg = TermColor.warn(msg)
        elif record.levelno > logging.WARNING:
            msg = TermColor.error(msg)
        elif record.levelno == logging.DEBUG:
            msg = TermColor.debug(msg)
        return msg


def get_module_logger(name):
    """
    A factory which creates loggers with the given name and returns it.
    """
    name = name.split('.')[-1]
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(NullHandler())
    return logger


def log_to_console(level=logging.DEBUG):
    """
    Add a log handler which logs to the console.
    """

    console = ColoredConsoleHandler()
    console.setLevel(level)
    if level == logging.DEBUG:
        formatter = logging.Formatter('%(asctime)s [%(name)s] %(message)s', '%b/%d %H:%M:%S')
    else:
        formatter = logging.Formatter('%(asctime)s %(message)s', '%b/%d %H:%M:%S')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


def log_to_file(filename, level=logging.DEBUG):
    """
    Add a log handler which logs to the console.
    """
    logfile = RotatingFileHandler(filename, maxBytes=1000000, backupCount=10)
    logfile.setLevel(level)
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(message)s', '%b/%d %H:%M:%S')
    logfile.setFormatter(formatter)
    logging.getLogger('').addHandler(logfile)


def type_name(var: Any) -> str:
    """
    Get a type name from a variable.
    :param var: any object
    :return: string type
    """
    return type(var).__name__


def log_call(method, args, kwargs, simple: bool = False):
    """
    Generate a function call string for logging
    :param method: method name
    :param args: positional arguments
    :param kwargs: keyword arguments
    :param simple: if true, do not show detailed signature
    :return: string for example "log_call('check', 1, 2, 3, a=1, b=2, c=3)"
    """

    repr_func = repr if not simple else type_name
    arg_strs = []
    arg_strs.extend([repr_func(arg) for arg in args])
    arg_strs.extend([f'{k}={repr_func(v)}' for i, (k, v) in enumerate(kwargs.items()) if i < 5])

    return f'{method}({", ".join(arg_strs)})'
