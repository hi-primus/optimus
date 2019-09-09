import timeit
from functools import wraps

from optimus.helpers.logger import logger


def time_it(method):
    def timed(*args, **kw):
        start_time = timeit.default_timer()
        f = method(*args, **kw)
        _time = round(timeit.default_timer() - start_time, 2)
        logger.print("{name}() executed in {time} sec".format(name=method.__name__, time=_time))
        return f

    return timed
