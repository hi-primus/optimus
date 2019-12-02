# This file need to be send to the cluster via .addPyFile to handle the pickle problem
# This is outside the optimus folder on purpose because it cause problem importing optimus when using de udf.

import re
from ast import literal_eval

import fastnumbers
from dateutil.parser import parse as dparse


def str_to_boolean(_value):
    _value = _value.lower()
    if _value == "true" or _value == "false":
        return True
    else:
        return False


def str_to_date(_value):
    try:
        dparse(_value)
        return True
    except (ValueError, OverflowError):
        pass


def str_to_null(_value):
    _value = _value.lower()
    if _value == "null":
        return True
    else:
        return False


def is_null(_value):
    if _value is None:
        return True
    else:
        return False


def str_to_gender(_value):
    _value = _value.lower()
    if _value == "male" or _value == "female":
        return True
    else
        return False


def str_to_data_type(_value, _dtypes):
    """
    Check if value can be parsed to a tuple or and list.
    Because Spark can handle tuples we will try to transform tuples to arrays
    :param _value:
    :return:
    """
    try:
        if isinstance(literal_eval((_value.encode('ascii', 'ignore')).decode("utf-8")), _dtypes):
            return True
    except (ValueError, SyntaxError):
        pass


def str_to_array(_value):
    return Infer.str_to_data_type(_value, (list, tuple))


def str_to_object(_value):
    return Infer.str_to_data_type(_value, (dict, set))


def str_to_url(_value):
    regex = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    if regex.match(_value):
        return True


def str_to_ip(_value):
    regex = re.compile('''\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}''')
    if regex.match(_value):
        return True


def str_to_email(_value):
    try:
        regex = re.compile(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")
        if regex.match(_value):
            return True
    except TypeError:
        pass


def str_to_credit_card(_value):
    # Reference https://www.regular-expressions.info/creditcard.html
    # https://codereview.stackexchange.com/questions/74797/credit-card-checking
    regex = re.compile(r'(4(?:\d{12}|\d{15})'  # Visa
                       r'|5[1-5]\d{14}'  # Mastercard
                       r'|6011\d{12}'  # Discover (incomplete?)
                       r'|7\d{15}'  # What's this?
                       r'|3[47]\d{13}'  # American Express
                       r')$')
    return bool(regex.match(_value))


def str_to_zip_code(_value):
    regex = re.compile(r'^(\d{5})([- ])?(\d{4})?$')
    if regex.match(_value):
        return True
    return False


def str_to_missing(_value):
    return True if _value == "" else False


def str_to_int(_value):
    return True if fastnumbers.isint(_value) else False


def str_to_decimal(_value):
    return True if fastnumbers.isfloat(_value) else False


def str_to_str(_value):
    return True if isinstance(_value, str) else False


DTYPE_FUNC = {"string": str_to_str, "boolean": str_to_boolean, "date": str_to_date,
              "array": str_to_array, "object": str_to_object, "ip": str_to_ip,
              "url": str_to_url, "email": str_to_email, "gender": str_to_gender,
              "credit_card_number": str_to_credit_card, "zip_code": str_to_zip_code}


class Infer(object):
    """
    This functions return True or False if match and specific dataType
    """

    @staticmethod
    def value(value, dtype: str):
        """
        Return if a value can be parsed as an specified dtype
        :param value:
        :param dtype:
        :return:
        """

        return DTYPE_FUNC[dtype](value)

    @staticmethod
    def mismatch(value: tuple, dtypes: dict):
        """
        UDF function.
        For example if we have an string column we also need to pass if it's a credit card or  postal code.
        Count the dataType that match, do not match, nulls and missing.
        :param value: tuple(Column/Row, value)
        :param dtypes: dict {col_name:(dataType, mismatch)}

        :return:
        """
        col_name, value = value

        _data_type = ""
        dtype = dtypes[col_name]

        if DTYPE_FUNC[dtype](value) is True:
            _data_type = dtype
        else:
            if is_null(value) is True:
                _data_type = "null"
            elif str_to_missing(value) is True:
                _data_type = "missing"
            else:
                _data_type = "mismatch"

        result = (col_name, _data_type), 1
        return result

    @staticmethod
    def parse(value, infer: bool, dtypes, str_funcs=None, int_funcs=None, mismatch: dict = None):
        """

        :param value:
        :param infer: If 'True' try to infer in all the dataTypes available. See int_func and str_funcs
        :param dtypes:
        :param str_funcs: Custom string function to infer.
        :param int_funcs: Custom numeric functions to infer.
        :param mismatch: a dict with a column name and a regular expression that match the datatype defined by the user.
        {col_name: regular_expression}
        :return:
        """
        col_name, value = value

        # Try to order the functions from less to more computational expensive
        if int_funcs is None:
            int_funcs = [(str_to_credit_card, "credit_card_number"), (str_to_zip_code, "zip_code")]

        if str_funcs is None:
            str_funcs = [
                (str_to_missing, "missing"), (str_to_boolean, "boolean"), (str_to_date, "date"),
                (str_to_array, "array"), (str_to_object, "object"), (str_to_ip, "ip"),
                (str_to_url, "url"),
                (str_to_email, "email"), (str_to_gender, "gender"), (str_to_null, "null")
            ]

        mismatch_count = 0
        if dtypes[col_name] == "string" and mismatch is not None:
            # Here we can create a list of predefined functions
            regex_list = {"dd/mm/yyyy": r'^([0-2][0-9]|(3)[0-1])(\/)(((0)[0-9])|((1)[0-2]))(\/)\d{4}$',
                          "yyyy-mm-dd": '([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))'
                          }

            if col_name in mismatch:
                predefined = mismatch[col_name]
                if predefined in regex_list:
                    expr = regex_list[predefined]
                else:
                    expr = mismatch[col_name]
                regex = re.compile(expr)
                if regex.match(value):
                    mismatch_count = 0
                else:
                    mismatch_count = 1

        if dtypes[col_name] == "string" and infer is True:

            if isinstance(value, bool):
                _data_type = "boolean"

            elif fastnumbers.isint(value):  # Check if value is integer
                _data_type = "int"
                for func in int_funcs:
                    if func[0](value) is True:
                        _data_type = func[1]
                        break

            elif fastnumbers.isfloat(value):
                _data_type = "decimal"

            elif isinstance(value, str):
                _data_type = "string"
                for func in str_funcs:
                    if func[0](value) is True:
                        _data_type = func[1]
                        break
            else:
                _data_type = "null"

        else:
            _data_type = dtypes[col_name]
            if is_null(value) is True:
                _data_type = "null"
            elif str_to_missing(value) is True:
                _data_type = "missing"
            else:
                if dtypes[col_name].startswith("array"):
                    _data_type = "array"
                else:
                    _data_type = dtypes[col_name]

        if mismatch:
            result = (col_name, _data_type), (1, mismatch_count)
        else:
            result = (col_name, _data_type), 1

        return result
