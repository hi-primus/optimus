def unzip(val):
    list_of_tuples = list(zip(*val))
    return [list(t) for t in list_of_tuples]

def val_to_list(val, allow_none=False, convert_tuple=False):
    """
    Convert a single value to a list
    :param val: Value to convert to list
    :param allow_none: Convert the value even if it's None
    :param convert_tuple: Convert to list if it's a tuple
    :return:
    """
    if val is not None or allow_none:
        if not isinstance(val, (list, tuple) if convert_tuple else (list,)):
            val = [val]
        if isinstance(val, tuple):
            val = list(val)
    return val


def one_list_to_val(val, convert_tuple=False):
    """
    Convert a single list element to val
    :param val:
    :return:
    """
    if isinstance(val, (list, tuple) if convert_tuple else (list,)) and len(val) == 1:
        result = val[0]
    else:
        result = val

    return result


def one_tuple_to_val(val):
    """
    Convert a single tuple element to val
    :param val:
    :return:
    """
    if isinstance(val, tuple) and len(val) == 1:
        result = val[0]
    else:
        result = val

    return result
