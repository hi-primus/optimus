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

def apply_to_categories(func):

    if hasattr(func, "__func__"):
        func = func.__func__
        static = True
    else:
        static = False

    def wrapper(ref, series, *args, **kwargs):

        # if series doesn't have a 'known' property it is a dask series,
        # which already optimizes this kind of operations

        if str(series.dtype) == "category" and hasattr(series, "cat") and not hasattr(series.cat, "known"):

            transformed = series.cat.categories.to_series()
            if static:
                transformed = func(transformed, *args, **kwargs)
            else:
                transformed = func(ref, transformed, *args, **kwargs)
            string_index = series.cat.categories.__class__(ref.to_list(transformed))
            return series.cat.set_categories(string_index, rename=True)
    
        if static:
            return func(series, *args, **kwargs)
        else:
            return func(ref, series, *args, **kwargs)

    return wrapper
