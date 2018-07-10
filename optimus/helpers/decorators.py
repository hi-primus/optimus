# Reference https://medium.com/@mgarod/dynamically-add-a-method-to-a-class-in-python-c49204b85bd6
# Decorator to attach a custom functions to a class

from functools import wraps


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
            return func(*args, **kwargs)

        setattr(class_, func.__name__, wrapper)
        # Note we are not binding func, but wrapper which accepts self but does exactly the same as func
        return func  # returning func means func can still be used normally

    return decorator