import collections
import json
import os
import random
import re
import subprocess
from pathlib import Path

from fastnumbers import isint, isfloat
from pyspark.ml.linalg import DenseVector
from pyspark.sql.types import ArrayType

from optimus import ROOT_DIR
from optimus.helpers.check import is_str, is_list, is_, is_bool, is_datetime, \
    is_date, is_binary

from optimus.helpers.converter import one_list_to_val, str_to_boolean, str_to_date, str_to_array, val_to_list
from optimus.helpers.logger import logger
from optimus.helpers.parser import parse_spark_class_dtypes
from optimus.helpers.raiseit import RaiseIt


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
    from optimus.helpers.columns import parse_columns
    dict_result = []

    # if there is only an element in the dict just return the value
    if len(dict_result) == 1:
        dict_result = next(iter(dict_result.values()))
    else:
        col_names = parse_columns(df, "*")

        # Because asDict can return messed columns names we order
        for row in df.collect():
            _row = row.asDict()
            r = collections.OrderedDict()
            for col in col_names:
                r[col] = _row[col]
            dict_result.append(r)
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


def absolute_path(files, format="posix"):
    """
    User project base folder to construct and absolute path
    :param files: path files
    :param format: posix or uri
    :return:
    """
    files = val_to_list(files)
    if format == "uri":
        result = [Path(ROOT_DIR + file).as_uri() for file in files]
    elif format == "posix":
        result = [Path(ROOT_DIR + file).as_posix() for file in files]
    else:
        RaiseIt.value_error(format, ["posix", "uri"])

    result = one_list_to_val(result)
    return result


def format_path(path, format="posix"):
    """
    Format a path depending fo the operative system
    :param path:
    :param format:
    :return:
    """
    if format == "uri":
        result = Path(path).as_uri()
    elif format == "posix":
        result = Path(path).as_posix()
    return result


def java_version():
    version = subprocess.check_output(['java', '-version'], stderr=subprocess.STDOUT)
    pattern = '\"(\d+\.\d+).*\"'
    print(re.re.search(pattern, version).groups()[0])


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
