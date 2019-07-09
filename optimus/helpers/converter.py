from optimus.helpers.functions import format_dict


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


def tuple_to_dict(value):
    """
    Convert tuple to dict
    :param value: tuple to be converted
    :return:
    """

    return format_dict(dict((x, y) for x, y in value))