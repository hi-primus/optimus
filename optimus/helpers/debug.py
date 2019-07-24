import inspect
from inspect import getframeinfo, stack


def get_var_name(var):
    """
    Get the var name from the var passed to a function
    :param var:
    :return:
    """
    _locals = inspect.stack()[2][0].f_locals
    for name in _locals:
        if id(var) == id(_locals[name]):
            return name
    return None


def debug(value):
    """
    Print a message with line and file name
    :param value: string to be printed
    :return:
    """

    frame_info = getframeinfo((stack()[1][0]))
    print("{}->{}:{}".format(value, frame_info.filename, frame_info.lineno, ))
