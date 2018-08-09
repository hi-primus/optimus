# Reference https://medium.com/@mgarod/dynamically-add-a-method-to-a-class-in-python-c49204b85bd6
# Decorator to attach a custom functions to a class

from functools import wraps
import time


def add_method(class_):
    """
    Use it as a decorator to add a function to specific class
    :param class_: Class in which the function will be attached
    :return:
    """

    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)

        setattr(class_, func.__name__, wrapper)
        # Note we are not binding func, but wrapper which accepts self but does exactly the same as func
        return func  # returning func means func can still be used normally

    return decorator


def add_attr(class_):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            time1 = time.time()
            ret = func(*args, **kwargs)
            time2 = time.time()
            print('{:s} function took {:.3f} sec'.format(func.__name__, (time2 - time1)))
            return ret

        setattr(class_, func.__name__, wrapper)
        # Note we are not binding func, but wrapper which accepts self but does exactly the same as func
        return func  # returning func means func can still be used normally

    return decorator


def counted(fn):
    """
    Count the numbers of call to a function
    :param fn:
    :return:
    """

    def wrapper(*args, **kwargs):
        wrapper.called += 1
        return fn(*args, **kwargs)

    wrapper.called = 0
    wrapper.__name__ = fn.__name__
    return wrapper
