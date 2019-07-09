import collections
import json
import os
import random

from fastnumbers import isint, isfloat
from pyspark.ml.linalg import DenseVector
from pyspark.sql.types import ArrayType

from optimus.helpers.check import is_str, is_dict_of_one_element, is_dict, is_list, is_, is_bool, is_datetime, \
    is_date, is_binary, \
    str_to_boolean, str_to_date, str_to_array, is_list_of_one_element
from optimus.helpers.converter import one_list_to_val
from optimus.helpers.logger import logger
from optimus.helpers.parser import parse_spark_class_dtypes


def infer(value):
    """
    Infer a Spark data type from a value
    :param value: value to be inferred
    :return: Spark data type
    """
    result = None
    if value is None:
        result = "null"

    elif is_bool(value):
        result = "bool"

    elif isint(value):
        result = "int"

    elif isfloat(value):
        result = "float"

    elif is_list(value):
        result = ArrayType(infer(value[0]))

    elif is_datetime(value):
        result = "datetime"

    elif is_date(value):
        result = "date"

    elif is_binary(value):
        result = "binary"

    elif is_str(value):
        if str_to_boolean(value):
            result = "bool"
        elif str_to_date(value):
            result = "string"  # date
        elif str_to_array(value):
            result = "string"  # array
        else:
            result = "string"

    return parse_spark_class_dtypes(result)


def random_int(n=5):
    """
    Create a random string of ints
    :return:
    """
    return str(random.randint(1, 10 ** n))


def collect_as_list(df):
    return df.rdd.flatMap(lambda x: x).collect()


def collect_as_dict(df):
    """
    Return a dict from a Collect result
    :param df:
    :return:
    """

    dict_result = []

    for x in df.toJSON().collect():
        dict_result.append(json.loads(x, object_pairs_hook=collections.OrderedDict))
    return dict_result


def filter_list(val, index=0):
    """
    Convert a list to None, int, str or a list filtering a specific index
    [] to None
    ['test'] to test

    :param val:
    :param index:
    :return:
    """
    if len(val) == 0:
        return None
    else:
        return one_list_to_val([column[index] for column in val])


def format_dict(val):
    """
    This function format a dict. If the main dict or a deep dict has only on element
     {"col_name":{0.5: 200}} we get 200
    :param val: dict to be formatted
    :return:
    """

    def _format_dict(_val):
        if not is_dict(_val):
            return _val

        for k, v in _val.items():
            if is_dict(v):
                if len(v) == 1:
                    _val[k] = next(iter(v.values()))
            else:
                if len(_val) == 1:
                    _val = v
        return _val

    if is_list_of_one_element(val):
        val = val[0]
    elif is_dict_of_one_element(val):
        val = next(iter(val.values()))

    # Some aggregation like min or max return a string column

    def repeat(f, n, x):
        if n == 1:  # note 1, not 0
            return f(x)
        else:
            return f(repeat(f, n - 1, x))  # call f with returned value

    # TODO: Maybe this can be done in a recursive way
    # We apply two passes to the dict so we can process internals dicts and the superiors ones
    return repeat(_format_dict, 2, val)


def is_pyarrow_installed():
    """
    Check if pyarrow is installed
    :return:
    """
    try:
        import pyarrow
        have_arrow = True
    except ImportError:
        have_arrow = False
    return have_arrow


def check_env_vars(env_vars):
    """
    Check if a environment var exist
    :param env_vars: Environment var name
    :return:
    """

    for env_var in env_vars:
        if env_var in os.environ:
            logger.print(env_var + "=" + os.environ.get(env_var))
        else:
            logger.print(env_var + " is not set")


# Reference https://nvie.com/posts/modifying-deeply-nested-structures/
def traverse(obj, path=None, callback=None):
    """
    Traverse a deep nested python structure
    :param obj: object to traverse
    :param path:
    :param callback: Function used to transform a value
    :return:
    """
    if path is None:
        path = []

    if is_(obj, dict):
        value = {k: traverse(v, path + [k], callback)
                 for k, v in obj.items()}

    elif is_(obj, list):
        value = [traverse(elem, path + [[]], callback)
                 for elem in obj]

    elif is_(obj, tuple):
        value = tuple(traverse(elem, path + [[]], callback)
                      for elem in obj)
    elif is_(obj, DenseVector):
        value = DenseVector([traverse(elem, path + [[]], callback) for elem in obj])
    else:
        value = obj

    if callback is None:  # if a callback is provided, call it to get the new value
        return value
    else:
        return callback(path, value)


def ellipsis(data, length=20):
    """
    Add a "..." if a string y greater than a specific length
    :param data:
    :param length: length taking into account to cut the string
    :return:
    """
    data = str(data)
    return (data[:length] + '..') if len(data) > length else data