from ast import literal_eval

import dateutil


def val_to_list(val):
    """
    Convert a single value string or number to a list
    :param val:
    :return:
    """
    if val is not None:
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

    return dict((x, y) for x, y in value)


def format_dict(_dict, tidy=True):
    """
    This function format a dict. If the main dict or a deep dict has only on element
     {"col_name":{0.5: 200}} we get 200
    :param _dict: dict to be formatted
    :param tidy:
    :return:
    """

    from optimus.helpers.check import is_dict, is_list_of_one_element, is_dict_of_one_element, is_list
    if tidy is True:
        def _format_dict(_dict):

            if not is_dict(_dict):
                return _dict
            for k, v in _dict.items():
                # If the value is a dict
                if is_dict(v):
                    # and only have one value
                    if len(v) == 1:
                        _dict[k] = next(iter(v.values()))
                else:
                    if len(_dict) == 1:
                        _dict = v
            return _dict

        if is_list_of_one_element(_dict):
            _dict = _dict[0]
        elif is_dict_of_one_element(_dict):
            # if dict_depth(_dict) >4:
            _dict = next(iter(_dict.values()))

        # Some aggregation like min or max return a string column

        def repeat(f, n, _dict):
            if n == 1:  # note 1, not 0
                return f(_dict)
            else:
                return f(repeat(f, n - 1, _dict))  # call f with returned value

        # TODO: Maybe this can be done in a recursive way
        # We apply two passes to the dict so we can process internals dicts and the superiors ones
        return repeat(_format_dict, 2, _dict)
    else:
        # Return the dict from a list
        if is_list(_dict):
            return _dict[0]
        else:
            return _dict


def str_to_boolean(value):
    """
    Check if a str can be converted to boolean
    :param value:
    :return:
    """
    value = value.lower()
    if value == "true" or value == "false":
        return True


def str_to_date(value):
    try:
        dateutil.parser.parse(value)
        return True
    except (ValueError, OverflowError):
        pass


def str_to_array(value):
    """
    Check if value can be parsed to a tuple or and array.
    Because Spark can handle tuples we will try to transform tuples to arrays
    :param value:
    :return:
    """
    try:
        if isinstance(literal_eval((value.encode('ascii', 'ignore')).decode("utf-8")), (list, tuple)):
            return True
    except (ValueError, SyntaxError,):
        pass
