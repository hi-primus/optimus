def val_to_list(val):
    """
    Convert a single value string or number to a list
    :param val:
    :return:
    """
    if not isinstance(val, list):
        val = [val]

    return val


def one_list_to_val(val):
    """
    Convert a single list element to val
    :param val:
    :return:
    """
    if isinstance(val, list) and len(val) == 1:
        result = val[0]
    else:
        result = val

    return result
