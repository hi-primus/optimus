import collections
import os
import random
import re
import subprocess
import sys
from functools import reduce
from pathlib import Path

from fastnumbers import isint, isfloat
from pyspark.ml.linalg import DenseVector
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
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


def collect_as_dict(df, limit=None):
    """
    Return a dict from a Collect result
    :param df:
    :return:
    """
    # # Explore this approach seems faster
    # use_unicode = True
    # from pyspark.serializers import UTF8Deserializer
    # from pyspark.rdd import RDD
    # rdd = df._jdf.toJSON()
    # r = RDD(rdd.toJavaRDD(), df._sc, UTF8Deserializer(use_unicode))
    # if limit is None:
    #     r.collect()
    # else:
    #     r.take(limit)
    # return r
    #
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
    result = None
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
    print(re.search(pattern, version).groups()[0])


def setup_google_colab():
    """
    Check if we are in Google Colab and setup it up
    :return:
    """
    from optimus.helpers.constants import JAVA_PATH_COLAB, SPARK_PATH_COLAB, SPARK_URL, SPARK_FILE

    IN_COLAB = 'google.colab' in sys.modules

    if IN_COLAB:
        if not os.path.isdir(JAVA_PATH_COLAB) or not os.path.isdir(SPARK_PATH_COLAB):
            print("Installing Optimus, Java8 and Spark. It could take 3 min...")
            commands = [
                "apt-get install openjdk-8-jdk-headless -qq > /dev/null",
                "wget -q {SPARK_URL}".format(SPARK_URL=SPARK_URL),
                "tar xf {SPARK_FILE}".format(SPARK_FILE=SPARK_FILE)
            ]

            cmd = " && ".join(commands)

            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
            p_stdout = p.stdout.read().decode("ascii")
            p_stderr = p.stderr.read().decode("ascii")
            print(p_stdout, p_stderr)

        else:
            print("Settings env vars")
            # Always configure the env vars

            os.environ["JAVA_HOME"] = JAVA_PATH_COLAB
            os.environ["SPARK_HOME"] = SPARK_PATH_COLAB


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


def create_buckets(lower_bound, upper_bound, bins):
    """
    Create a dictionary with bins
    :param lower_bound: low range
    :param upper_bound: high range
    :param bins: number of buckets
    :return:
    """
    range_value = (upper_bound - lower_bound) / bins
    low = lower_bound

    buckets = []

    if bins == 1:
        buckets.append({"lower": low, "upper": low + 1, "bucket": 0})
    else:
        for i in range(0, bins):
            high = low + range_value
            buckets.append({"lower": low, "upper": high, "bucket": i})
            low = high

    # ensure that the upper bound is exactly the higher value.
    # Because floating point calculation it can miss the upper bound in the final sum

        buckets[bins - 1]["upper"] = upper_bound
    return buckets


def append(dfs, like="columns"):
    """
    Concat multiple dataframes columns or rows wise
    :param dfs: List of Dataframes
    :param like: concat as columns or rows
    :return:
    """

    # FIX: Because monotonically_increasing_id can create different
    # sequence for different dataframes the result could be wrong.

    if like == "columns":
        temp_dfs = []
        col_temp_name = "id_" + random_int()

        dfs = val_to_list(dfs)
        for df in dfs:
            temp_dfs.append(df.withColumn(col_temp_name, F.monotonically_increasing_id()))

        def _append(df1, df2):
            return df1.join(df2, col_temp_name, "outer")

        df_result = reduce(_append, temp_dfs).drop(col_temp_name)

    elif like == "rows":
        df_result = reduce(DataFrame.union, dfs)
    else:
        RaiseIt.value_error(like, ["columns", "rows"])

    return df_result

def deep_sort(obj):
    """
    Recursively sort list or dict nested lists
    """

    if isinstance(obj, dict):
        _sorted = {}
        for key in sorted(obj):
            _sorted[key] = deep_sort(obj[key])

    elif isinstance(obj, list):
        new_list = []
        for val in obj:
            new_list.append(deep_sort(val))
        _sorted = sorted(new_list)

    else:
        _sorted = obj

    return _sorted
