# This file need to be send to the cluster via .addPyFile to handle the pickle problem
# This is outside the optimus folder on purpose because it cause problem importing optimus when using de udf.
# This can not import any optimus file unless it's imported via addPyFile
import datetime
import math
import os
import re
from ast import literal_eval

import fastnumbers
import pandas as pd
import pendulum
import hidateinfer



# This function return True or False if a string can be converted to any datatype.
from optimus.helpers.constants import CURRENCIES


def is_datetime_str(_value: str):
    try:
        pdi = hidateinfer.infer([_value])
        code_count = pdi.count('%')
        value_code_count = _value.count('%')
        return code_count >= 2 and value_code_count < code_count and code_count >= len(_value) / 7 and any(
            [c in pdi for c in ['/', '-', ':', '%b', '%B']])
    except Exception:
        return False

def str_to_date_format(_value, date_format):
    # Check this https://stackoverflow.com/questions/17134716/convert-dataframe-column-type-from-string-to-datetime-dd-mm-yyyy-format
    try:
        pendulum.from_format(_value, date_format)
        return True
    except ValueError:
        return False


def str_to_null(_value):
    _value = _value.lower()
    if _value == "null":
        return True
    else:
        return False


def is_null(_value):
    return pd.isnull(_value)


def str_to_data_type(_value, _data_types):
    """
    Check if value can be parsed to a tuple or and list.
    Because Spark can handle tuples we will try to transform tuples to arrays
    :param _value:
    :param _data_types:
    :return:
    """
    # return True if isinstance(_value, str) else False
    try:
        if isinstance(literal_eval((_value.encode('ascii', 'ignore')).decode("utf-8")), _data_types):
            return True
    except (ValueError, SyntaxError, AttributeError):
        return False


def is_list_value(value):
    """
    Check if an object is a list
    :param value:
    :return:
    """
    return isinstance(value, list)


def is_list_str(_value):
    return False
    # return str_to_data_type(_value, (list, tuple))


def is_object_str(_value):
    return False
    # return str_to_data_type(_value, (dict, set))


regex_str = r"."

regex_int = r"(^\d+\.[0]+$)|(^\d+$)"  # For cudf 0.14 regex_int = r"^\d+$" # For cudf 0.14
regex_decimal = r"(^\d+\.\d+$)|(^\d+$)"
regex_non_int_decimal = r"^(\d+\.\d+)$"

regex_boolean = r"true|false|0|1"
regex_boolean_compiled = re.compile(regex_boolean)


def is_bool_str(value, compile=False):
    return str_to(str(value), regex_boolean, regex_boolean_compiled, compile)


regex_gender = r"\bmale\b|\bfemale\b"
regex_gender_compiled = re.compile(regex_gender)


def is_gender(value, compile=False):
    return str_to(value, regex_gender, regex_gender_compiled, compile)


regex_full_url = r"^(?:([A-Za-z0-9]*)\://)?(?:(\w+)\.)?([A-Za-z0-9\-\.]+):?(\d+)?(?:/|$)?(\w+\.?\w+)?\??(\w.+)?$"

regex_url = r"([a-zA-Z]+\.)?([a-zA-Z0-9]+\.)+([a-zA-Z]+)\/?"

regex_url_compiled = re.compile(regex_url, re.IGNORECASE)


def str_to_url(value, compile=False):
    return str_to(value, regex_url, regex_url_compiled, compile)


regex_ip = r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"
regex_ip_compiled = re.compile(regex_ip, re.IGNORECASE)


def str_to_ip(value, compile=False):
    return str_to(value, regex_ip, regex_ip_compiled, compile)


# regex_email = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)" # This do not work in CUDF/RE2
regex_email = r"^[^@]+@[^@]+\.[a-zA-Z]{2,}$"
regex_email_compiled = re.compile(regex_email, re.IGNORECASE)


def is_email(value, compile=False):
    return str_to(value, regex_email, regex_email_compiled, compile)


# Reference https://www.regular-expressions.info/creditcard.html
# https://codereview.stackexchange.com/questions/74797/credit-card-checking
regex_credit_card_number = (r'(4(?:\d{12}|\d{15})'  # Visa
                            r'|5[1-5]\d{14}'  # Mastercard
                            r'|6011\d{12}'  # Discover (incomplete?)
                            r'|7\d{15}'  # What's this?
                            r'|3[47]\d{13}'  # American Express
                            r')$')

regex_credit_card_compiled = re.compile(regex_credit_card_number)


def is_credit_card_number(value, compile=False):
    return str_to(value, regex_credit_card_number, regex_credit_card_compiled, compile)


regex_zip_code = r"^(\d{4,5}(?:[- ]\d{4})?)$"
regex_zip_code_compiled = re.compile(regex_zip_code, re.IGNORECASE)


def is_zip_code(value, compile=False):
    return str_to(value, regex_zip_code, regex_zip_code_compiled, compile)


def is_missing(value):
    return value == ""


regex_social_security_number = "^([1-9])(?!\1{2}-\1{2}-\1{4})[1-9]{2}-[1-9]{2}-[1-9]{4}"
regex_social_security_number_compiled = re.compile(regex_social_security_number, re.IGNORECASE)


def is_social_security_number(value, compile=False):
    return str_to(value, regex_social_security_number, regex_social_security_number_compiled, compile)


regex_http_code = "/^[1-5][0-9][0-9]$/"
regex_http_code_compiled = re.compile(regex_http_code, re.IGNORECASE)


def is_http_code(value, compile=False):
    return str_to(value, regex_http_code, regex_http_code_compiled, compile)


# Reference https://stackoverflow.com/questions/8634139/phone-validation-regex
regex_phone_number = r"\+?\(?([0-9]{3})\)?[-.]?\(?([0-9]{3})\)?[-.]?\(?([0-9]{4})\)?"
regex_phone_number_compiled = re.compile(regex_phone_number, re.IGNORECASE)


def is_phone_number(value, compile=False):
    # print("value",value, str_to(value, regex_phone_number, regex_phone_number_compiled, compile))
    return str_to(value, regex_phone_number, regex_phone_number_compiled, compile)


def str_to(value, regex, compiled_regex, compile=False):
    value = str(value)
    if value is None:
        result = False
    else:
        if compile is True:
            regex = compiled_regex
        else:
            regex = regex

        result = bool(re.match(regex, value))
    return result


def str_to_int(_value):
    return True if fastnumbers.isint(_value) else False


def str_to_decimal(_value):
    return True if fastnumbers.isfloat(_value) else False


def str_to_str(_value):
    return True if isinstance(_value, str) else False


regex_currencies = "|".join(list(CURRENCIES.keys()))
regex_currencies_compiled = re.compile(regex_currencies)


def str_to_currency(value, compile=False):
    return str_to(value, regex_boolean, regex_boolean_compiled, compile)


def is_nan(value):
    """
    Check if a value is nan
    :param value:
    :return:
    """
    result = False
    if is_str(value):
        if value.lower() == "nan":
            result = True
    elif math.isnan(value):
        result = True
    return result


def is_none(value):
    """
    Check if a value is none
    :param value:
    :return:
    """
    result = False
    if is_str(value):
        if value.lower() == "none":
            result = True
    elif value is None:
        result = True
    return result


def is_same_class(class1, class2):
    """
    Check if 2 class are the same
    :param class1:
    :param class2:
    :return:
    """
    return class1 == class2


def is_(value, type_):
    """
    Check if a value is instance of a class
    :param value:
    :param type_:
    :return:
    """

    return isinstance(value, type_)


def is_type(type1, type2):
    """
    Check if a value is a specific class
    :param type1:
    :param type2:
    :return:
    """
    return type1 == type2


def is_function(value):
    """
    Check if a param is a function
    :param value: object to check for
    :return:
    """
    return hasattr(value, '__call__')


def is_empty_function(value):
    """
    Returns True if f is an empty function.
    """

    if not is_function(value):
        return False

    def empty_func():
        pass

    def empty_func_with_docstring():
        """
        Empty function with docstring.
        """
        pass

    empty_lambda = lambda: None

    empty_lambda_with_docstring = lambda: None
    empty_lambda_with_docstring.__doc__ = """Empty function with docstring."""

    def constants(f):
        """
        Return a tuple containing all the constants of a function without a docstring
        """
        return tuple(
            x
            for x in f.__code__.co_consts
            if x != f.__doc__
        )

    return (
            value.__code__.co_code == empty_func.__code__.co_code and
            constants(value) == constants(empty_func)
        ) or (
            value.__code__.co_code == empty_func_with_docstring.__code__.co_code and
            constants(value) == constants(empty_func_with_docstring)
        ) or (
            value.__code__.co_code == empty_lambda.__code__.co_code and
            constants(value) == constants(empty_lambda)
        ) or (
            value.__code__.co_code == empty_lambda_with_docstring.__code__.co_code and
            constants(value) == constants(empty_lambda_with_docstring)
        )


def is_list(value, mode=None):
    """
    Check if a string or any not string value is a python list
    :param value:
    :return:
    """
    if mode == "string":
        result = is_list_str(value)
    else:
        result = is_list_value(value)

    return result


def is_object(value):
    """
    Check if a string or any not string value is a python list
    :param value:
    :return:
    """
    return True if is_object_value(value) or is_object_str(value) else False


def is_bool(value):
    if is_bool_value(value):
        return True
    elif is_bool_value(value):
        return True
    return False


def is_list_empty(value):
    """
    Check is a list is empty
    :param value:
    :return:
    """
    return len(value) == 0


def is_dict(value):
    """
    Check if an object is a list
    :param value:
    :return:
    """
    return isinstance(value, dict)


def is_tuple(value):
    """
    Check if an object is a tuple
    :param value:
    :return:
    """
    return isinstance(value, tuple)

def is_list_or_tuple(value):
    """
    Check if an object is a list or a tuple
    :param value:
    :return:
    """
    return isinstance(value, list)

def is_list_of_int(value):
    """
    Check if an object is a list of integers
    :param value:
    :return:
    """
    return isinstance(value, list) and all(isinstance(elem, int) for elem in value)

def is_list_of_dicts(value):
    """
    Check if an object is a list of dictionaries
    :param value:
    :return:
    """
    return isinstance(value, list) and all(isinstance(elem, dict) for elem in value)

def is_list_with_dicts(value):
    """
    Check if an object is a list with some dictionaries
    :param value:
    :return:
    """
    return isinstance(value, list) and any(isinstance(elem, dict) for elem in value)


def is_list_of_float(value):
    """
    Check if an object is a list of floats
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, float) for elem in value)


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


def is_list_of_dask_dataframes(value):
    """
    Check if an object is a Spark DataFrame
    :param value:
    :return:
    """
    from dask.dataframe.core import DataFrame as DaskDataFrame
    return isinstance(value, list) and all(isinstance(elem, DaskDataFrame) for elem in value)


def is_filepath(file_path):
    """
    Check if a value ia a valid file path
    :param file_path:
    :return:
    """
    # the file is there
    if os.path.exists(file_path):
        return True
    # the file does not exists but write privileges are given
    elif os.access(os.path.dirname(file_path), os.W_OK):
        return True
    # can not write there
    else:
        return False


def is_ip(value):
    """
    Check if a value is valid ip
    :param value:
    :return:
    """
    return re.match(regex_ip_compiled, value)


def is_list_of_str(value):
    """
    Check if all elements in a list are strings
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, str) for elem in value)


def is_list_of_numeric(value):
    """
    Check if all elements in a list are int or float
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, (int, float)) for elem in value)


def is_list_of_list(value):
    """
    Check if all elements in a list are tuples
    :param value:
    :return:
    """

    return bool(value) and isinstance(value, list) and all(isinstance(elem, list) for elem in value)


def is_list_of_tuples(value):
    """
    Check if all elements in a list are tuples
    :param value:
    :return:
    """

    return bool(value) and isinstance(value, list) and all(isinstance(elem, tuple) for elem in value)


def is_list_of_one_element(value):
    """
    Check if a var is a single element
    :param value:
    :return:
    """
    if is_list_value(value):
        return len(value) == 1


def is_dict_of_one_element(value):
    """
    Check if a var is a single element
    :param value:
    :return:
    """
    if is_dict(value):
        return len(value) == 1


def is_one_element(value):
    """
    Check if a var is a single element
    :param value:
    :return:
    """
    return isinstance(value, (str, int, float, bool))


def is_num_or_str(value):
    """
    Check if a var is numeric(int, float) or string
    :param value:
    :return:
    """
    return isinstance(value, (int, float, str))


def is_str_or_int(value):
    """
    Check if a var is a single element
    :param value:
    :return:
    """

    return isinstance(value, (str, int))


def is_numeric(value):
    """
    Check if a var is a single element
    :param value:
    :return:
    """
    return isinstance(value, (int, float))

def is_numeric_like(value):
    """
    Check if a var is a single element
    :param value:
    :return:
    """
    return fastnumbers.isfloat(value) or fastnumbers.isintlike(value)


def is_str(value):
    """
    Check if an object is a string
    :param value:
    :return:
    """
    # Seems 20% faster than
    return isinstance(value, str)
    # return True if type("str") == "str" else False


def is_object_value(value):
    """
    Check if an object is a string
    :param value:
    :return:
    """
    return isinstance(value, object)


def is_list_of_dask_futures(value):
    """
    Check if an object is a list of strings
    :param value:
    :return:
    """
    from dask import distributed
    return bool(value) and isinstance(value, list) and all(
        isinstance(elem, distributed.client.Future) for elem in value)


def is_dask_future(value):
    """
    Check if an object is a list of strings
    :param value:
    :return:
    """
    from dask import distributed
    return isinstance(value, distributed.client.Future)


def is_float(value):
    return fastnumbers.isfloat(value, allow_nan=True)


def is_int(value):
    """
    Check if an object is an integer
    :param value:
    :return:
    """
    return isinstance(value, int) and not isinstance(value, bool)


def is_int_like(value):
    """
    Check if an object is an integer
    :param value:
    :return:
    """
    return fastnumbers.isintlike(value)

def is_float_like(value):
    """
    Check if a var is a float
    :param value:
    :return:
    """
    return fastnumbers.isfloat(value)

def is_url(value):
    regex = re.compile(
        r'^(?:http|ftp|hdfs)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    return re.match(regex, value)


def is_float(value):
    """
    Check if an object is an integer
    :param value:
    :return:
    """
    return isinstance(value, float)


def is_bool_value(value):
    return isinstance(value, bool)


def is_datetime(value):
    """
    Check if an object is a datetime
    :param value:
    :return:
    """
    result = False
    if isinstance(value, datetime.datetime):
        result = True
    # else:
    #     result = is_datetime_str(str(value))

    return result


def is_valid_datetime_format(value):
    try:
        now = datetime.datetime.strftime(datetime.datetime.now(tz=datetime.timezone.utc), value)
        datetime.datetime.strptime(now, value)
    except ValueError:
        return False
    except re.error:
        return False

    return True


def is_binary(value):
    """
    Check if an object is a bytearray
    :param value:
    :return:
    """
    return isinstance(value, bytearray)


def is_date(value):
    """
    Check if an object is a date
    :param value:
    :return:
    """

    return isinstance(value, datetime.date)
