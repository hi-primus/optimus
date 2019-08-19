"""
Helpers to check if an object match a date type
"""
import datetime
import os
import re

import math
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from optimus.helpers.converter import one_list_to_val, val_to_list
from optimus.helpers.parser import parse_spark_dtypes
from optimus.helpers.raiseit import RaiseIt


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
    elif is_numeric(value):
        if math.isnan(value):
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


def is_list(value):
    """
    Check if an object is a list
    :param value:
    :return:
    """
    return isinstance(value, list)


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


def is_column(value):
    """
    Check if a object is a column
    :return:
    """
    return isinstance(value, F.Column)


def is_column_a(df, column, dtypes):
    """
    Check if column match a list of data types
    :param df: dataframe
    :param column: column to be compared with
    :param dtypes: types to be checked
    :return:
    """
    column = val_to_list(column)

    if len(column) > 1:
        RaiseIt.length_error(column, 1)

    data_type = tuple(val_to_list(parse_spark_dtypes(dtypes)))
    column = one_list_to_val(column)

    # Filter columns by data type
    return isinstance(df.schema[column].dataType, data_type)


def is_list_of_str(value):
    """
    Check if an object is a list of strings
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, str) for elem in value)


def is_list_of_int(value):
    """
    Check if an object is a list of integers
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, int) for elem in value)


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


def is_list_of_dataframes(value):
    """
    Check if an object is a Spark DataFrame
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, DataFrame) for elem in value)


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
    parts = value.split(".")
    if len(parts) != 4:
        return False
    for item in parts:
        if not 0 <= int(item) <= 255:
            return False
    return True


def is_list_of_strings(value):
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
    if is_list(value):
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


def is_str(value):
    """
    Check if an object is a string
    :param value:
    :return:
    """
    return isinstance(value, str)


def is_int(value):
    """
    Check if an object is an integer
    :param value:
    :return:
    """
    return isinstance(value, int)


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


# TODO: can be confused with is_type
def is_dataframe(value):
    """
    Check if an object is a Spark DataFrame
    :param value:
    :return:
    """
    return isinstance(value, DataFrame)


def is_bool(value):
    return isinstance(value, bool)


def is_datetime(value):
    """
    Check if an object is a datetime
    :param value:
    :return:
    """

    return isinstance(value, datetime.datetime)


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


def has_(value, _type):
    """
    Check if a list has a element of a specific data type
    :param value: list
    :param _type: data type to check
    :return:
    """
    return any(isinstance(elem, _type) for elem in value)
