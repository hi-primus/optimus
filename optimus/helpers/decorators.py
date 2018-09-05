import timeit
import logging
from functools import wraps


def add_method(cls):
    """
    Attach a function to a class as an attribute
    :param cls: Class in which the function will be attached
    :return:
    """

    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)

        setattr(cls, func.__name__, wrapper)
        return func

    return decorator


def time_it(method):
    def timed(*args, **kw):
        start_time = timeit.default_timer()
        f = method(*args, **kw)
        _time = round(timeit.default_timer() - start_time, 2)
        logging.info("{name}() executed in {time} sec".format(name=method.__name__, time=_time))
        return f

    return timed


def add_attr(cls, log_time=False):
    """
    Attach a function to another functions as an attribute
    :param cls: class where the function will be attached
    :param log_time: Print the execution time. Verbose must be true on the Optimus constructor
    :type log_time: bool
    :return:
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = timeit.default_timer()
            f = func(*args, **kwargs)
            _time = round(timeit.default_timer() - start_time, 2)
            if log_time:
                logging.info("{name}() executed in {time} sec".format(name=func.__name__, time=_time))
            return f

        setattr(cls, func.__name__, wrapper)
        return func

    return decorator
