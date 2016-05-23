import datetime
import functools
import logging

from .get_logger import get_logger

log = get_logger(__name__, level=logging.DEBUG)


class stopwatch(object):
    """
    Easily measure elapsed time
    """
    def __init__(self, label=None):
        self.start_time = datetime.datetime.now()
        self.label = label

    def __repr__(self):
        stop = datetime.datetime.now()
        r = str(stop - self.start_time)
        if self.label:
            return 'exit: %s %s' % (self.label, r)
        return r

    def __enter__(self):
        log.debug('enter ' + self.label)

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, exc_val, exc_tb):
        log.debug(self)

    def __call__(self, f):
        @functools.wraps(f)
        def decorated(*args, **kwargs):
            self.start_time = datetime.datetime.now()
            if self.label is None:
                self.label = 'function: %s' % f.__name__
            with self:
                return f(*args, **kwargs)
        return decorated
