
def is_function(obj):
    """
    Check if a param is a function
    :param obj: object to check for
    :return:
    """
    return hasattr(obj, '__call__')


def is_list(value):
    """
    Check if an object is a list
    :param value:
    :return:
    """
    return isinstance(value, list)


def is_list_of_str_or_int(value):
    """
    Check if an object is a string or an integer
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, (int, str)) for elem in value)


def is_list_of_str_or_num(value):
    """
    Check if an object is string, integer or float
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, (str, int, float)) for elem in value)

def is_list_of_strings(value):
    """
    Check that all elements in a list are strings
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, str) for elem in value)


def is_list_of_numeric(value):
    """
    Check that all elements in a list are int or float
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, (int, float)) for elem in value)


def is_list_of_tuples(value):
    """
    Check that all elements in a list are tuples
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, tuple) for elem in value)


def is_one_element(value):
    """
    Check that a var is a single element
    :param value:
    :return:
    """
    return isinstance(value, (str, int, float, bool))


def is_str_or_int(value):
    """
    Check that a var is a single element
    :param value:
    :return:
    """
    return isinstance(value, (str, int))


def is_numeric(value):
    """
    Check that a var is a single element
    :param value:
    :return:
    """
    return isinstance(value, (int, float))


def is_str(value):
    """
    Check if an object is a string
    :param value:
    :return:
    """
    return isinstance(value, str)