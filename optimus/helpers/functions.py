import collections
import functools
import ntpath
import os
import random
import re
import subprocess
import sys
import tempfile
from functools import reduce
from pathlib import Path
from urllib.request import Request, urlopen

import numpy as np
import six
from pyspark.ml.linalg import DenseVector
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from optimus import ROOT_DIR
from optimus.helpers.check import is_url
from optimus.helpers.columns import parse_columns
from optimus.helpers.converter import any_dataframe_to_pandas
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_


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
    [(col_name, row_value),(col_name_1, row_value_2),(col_name_3, row_value_3),(col_name_4, row_value_4)]
    :return:
    """

    dict_result = []

    df = any_dataframe_to_pandas(df)

    # if there is only an element in the dict just return the value
    if len(dict_result) == 1:
        dict_result = next(iter(dict_result.values()))
    else:
        col_names = parse_columns(df, "*")

        # Because asDict can return messed columns names we order
        for index, row in df.iterrows():
            # _row = row.asDict()
            r = collections.OrderedDict()
            # for col_name, value in row.iteritems():
            for col_name in col_names:
                r[col_name] = row[col_name]
            dict_result.append(r)
    return dict_result


# def collect_as_dict(df, limit=None):
#     """
#     Return a dict from a Collect result
#     :param df:
#     :return:
#     """
#     # # Explore this approach seems faster
#     # use_unicode = True
#     # from pyspark.serializers import UTF8Deserializer
#     # from pyspark.rdd import RDD
#     # rdd = df._jdf.toJSON()
#     # r = RDD(rdd.toJavaRDD(), df._sc, UTF8Deserializer(use_unicode))
#     # if limit is None:
#     #     r.collect()
#     # else:
#     #     r.take(limit)
#     # return r
#     #
#     from optimus.helpers.columns import parse_columns
#     dict_result = []
#
#     # if there is only an element in the dict just return the value
#     if len(dict_result) == 1:
#         dict_result = next(iter(dict_result.values()))
#     else:
#         col_names = parse_columns(df, "*")
#
#         # Because asDict can return messed columns names we order
#         for row in df.collect():
#             _row = row.asDict()
#             r = collections.OrderedDict()
#             for col in col_names:
#                 r[col] = _row[col]
#             dict_result.append(r)
#     return dict_result


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
    from optimus.helpers.constants import JAVA_PATH_COLAB
    from optimus.engines.spark.constants import SPARK_PATH_COLAB
    from optimus.engines.spark.constants import SPARK_URL
    from optimus.engines.spark.constants import SPARK_FILE

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
    Concat multiple dataFrames columns or rows wise
    :param dfs: List of DataFrames
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


def update_dict(d, u):
    # python 3.8+ compatibility
    try:
        collectionsAbc = collections.abc
    except ModuleNotFoundError:
        collectionsAbc = collections

    for k, v in six.iteritems(u):
        dv = d.get(k, {})
        if not isinstance(dv, collectionsAbc.Mapping):
            d[k] = v
        elif isinstance(v, collectionsAbc.Mapping):
            d[k] = update_dict(dv, v)
        else:
            d[k] = v
    return d


def reduce_mem_usage(df, categorical_threshold=50):
    """
    Change the columns datatypes to reduce the memory usage
    :param df:
    :param categorical_threshold:
    :return:
    """
    # Reference https://www.kaggle.com/arjanso/reducing-dataframe-memory-size-by-65/notebook

    # rows_count = df.rows.count()

    start_mem_usg = df.ext.size()
    print("Memory usage after optimization:", start_mem_usg, " MB")
    NA_list = []  # Keeps track of columns that have missing values filled in.
    columns_dtype = {}

    min_max = df.cols.range("*")
    for col_name in df.columns:
        if df[col_name].dtype == object:  # Exclude strings
            if len(df.cols.value_counts(col_name)[col_name]) <= categorical_threshold:
                columns_dtype[col_name] = "category"
        else:
            # make variables for Int, max and min
            IsInt = False
            # a = df.cols.range(col_name)

            _min = min_max[col_name]["min"]
            _max = min_max[col_name]["max"]

            # Integer does not support NA, therefore, NA needs to be filled
            if not np.isfinite(df[col_name]).all():
                NA_list.append(col_name)
                df = df[col_name].fillna(_max - 1, inplace=True).compute()

                # test if column can be converted to an integer
            asint = df[col_name].fillna(0).astype(np.int64)
            result = (df[col_name] - asint)
            result = result.sum().compute()
            if -0.01 < result < 0.01:
                IsInt = True

            # Make Integer/unsigned Integer datatypes
            if IsInt:
                if _max >= 0:
                    if _min < 255:
                        columns_dtype[col_name] = np.uint8
                    elif _min < 65535:
                        columns_dtype[col_name] = np.uint16
                    elif _min < 4294967295:
                        columns_dtype[col_name] = np.uint32
                    else:
                        columns_dtype[col_name] = np.uint64
                else:
                    if _max > np.iinfo(np.int8).min and _min < np.iinfo(np.int8).max:
                        columns_dtype[col_name] = np.int8
                    elif _max > np.iinfo(np.int16).min and _min < np.iinfo(np.int16).max:
                        columns_dtype[col_name] = np.int16
                    elif _max > np.iinfo(np.int32).min and _min < np.iinfo(np.int32).max:
                        columns_dtype[col_name] = np.int32
                    elif _max > np.iinfo(np.int64).min and _min < np.iinfo(np.int64).max:
                        columns_dtype[col_name] = np.int64
            else:
                columns_dtype[col_name] = np.float32

    df = df.astype(columns_dtype)
    # Print final result
    # print("___MEMORY USAGE AFTER COMPLETION:___")
    # mem_usg = df.memory_usage().sum() / 1024 ** 2
    mem_usg = df.ext.size()

    print("Memory usage before optimization is: ", mem_usg, " MB")
    print(100 * mem_usg / start_mem_usg, "% of the initial size")
    # return props, NA_list
    return df


def downloader(url, file_format):
    """
    Send the request to download a file
    """

    def write_file(response, file, chunk_size=8192):
        """
        Load the data from the http request and save it to disk
        :param response: data returned from the server
        :param file:
        :param chunk_size: size chunk size of the data
        :return:
        """
        total_size = response.headers['Content-Length'].strip() if 'Content-Length' in response.headers else 100
        total_size = int(total_size)
        bytes_so_far = 0

        while 1:
            chunk = response.read(chunk_size)
            bytes_so_far += len(chunk)
            if not chunk:
                break
            file.write(chunk)
            total_size = bytes_so_far if bytes_so_far > total_size else total_size

        return bytes_so_far

    # try to infer the file format using the file extension
    if file_format is None:
        filename, file_format = os.path.splitext(url)
        file_format = file_format.replace('.', '')

    i = url.rfind('/')
    data_name = url[(i + 1):]

    headers = {"User-Agent": "Optimus Data Downloader/1.0"}

    req = Request(url, None, headers)

    logger.print("Downloading %s from %s", data_name, url)

    # It seems that avro need a .avro extension file
    with tempfile.NamedTemporaryFile(suffix="." + file_format, delete=False) as f:
        bytes_downloaded = write_file(urlopen(req), f)
        path = f.name

    if bytes_downloaded > 0:
        logger.print("Downloaded %s bytes", bytes_downloaded)

    logger.print("Creating DataFrame for %s. Please wait...", data_name)

    return path


@functools.lru_cache(maxsize=128)
def prepare_path(path, file_format):
    """
    Helper to return the file to be loaded and the file name.
    This will memoise
    :param path: Path to the file to be loaded
    :param file_format: format file
    :return:
    """
    # print(path)
    file_name = ntpath.basename(path)
    if is_url(path):
        file = downloader(path, file_format)
    else:
        file = path
    return file, file_name


# value = "dd/MM/yyyy hh:mm:ss-sss MA"


def match_date(value):
    """
    Returns True if the string match and specific format
    :param value:
    :return:
    """
    formats = ["d", "dd", "M", "MM", "yy", "yyyy", "h", "hh", "H", "HH", "kk", "k", "m", "mm", "s", "ss", "sss", "/",
               ":", "-", " ", "+", "|", "mi"]
    formats.sort(key=len, reverse=True)

    result = []

    start = 0

    end = len(value)
    found = False

    while start < end:
        found = False
        for f in formats:
            if value.startswith(f, start):
                start = start + len(f)
                result.append(f)
                found = True
                break
        if found is False:
            raise ValueError('{} is not a valid date format'.format(value[start]))
    exprs = []
    for f in result:
        # Separators
        if f in ["/", ":", "-", " ", "|", "+"]:
            exprs.append("\\" + f)
        # elif f == ":":
        #     exprs.append("\\:")
        # elif f == "-":
        #     exprs.append("\\-")
        # elif f == " ":
        #     exprs.append(" ")
        # elif f == "|":
        #     exprs.append("\\|")
        # elif f == "+":
        #     exprs.append("\\+")

        # Day
        # d  -> 1 ... 31
        # dd -> 01 ... 31

        elif f == "d":
            exprs.append("(3[01]|[12][0-9]|0?[1-9])")
        elif f == "dd":
            exprs.append("(3[01]|[12][0-9]|0[1-9])")

            # Month
        # M  -> 1 ... 12
        # MM -> 01 ... 12
        elif f == "M":
            exprs.append("(1[0-2]|0?[1-9])")
        elif f == "MM":
            exprs.append("(1[0-2]|0[1-9])")

        # Year
        # yy   -> 00 ... 99
        # yyyy -> 0000 ... 9999
        elif f == "yy":
            exprs.append("[0-9]{2}")
        elif f == "yyyy":
            exprs.append("[0-9]{4}")

            # Hours
        # h  -> 1,2 ... 12
        # hh -> 01,02 ... 12
        # H  -> 0,1 ... 23
        # HH -> 00,01 ... 23
        # k  -> 1,2 ... 24
        # kk -> 01,02 ... 24
        elif f == "h":
            exprs.append("(1[0-2]|0?[1-9])")
        elif f == "hh":
            exprs.append("(1[0-2]|0[1-9])")
        elif f == "H":
            exprs.append("(0?[0-9]|1[0-9]|2[0-3]|[0-9])")
        elif f == "HH":
            exprs.append("(0[0-9]|1[0-9]|2[0-3]|[0-9])")
        elif f == "k":
            exprs.append("(0?[1-9]|1[0-9]|2[0-4]|[1-9])")
        elif f == "kk":
            exprs.append("(0[1-9]|1[0-9]|2[0-4])")

        # Minutes
        # m  -> 0 ... 59
        # mm -> 00 .. 59
        elif f == "m":
            exprs.append("[1-5]?[0-9]")
        elif f == "mm":
            exprs.append("[0-5][0-9]")

        # Seconds
        # s  -> 0 ... 59
        # ss -> 00 .. 59
        elif f == "s":
            exprs.append("[1-5]?[0-9]")
        elif f == "ss":
            exprs.append("[0-5][0-9]")

        # Milliseconds
        # sss -> 0 ... 999
        elif f == "sss":
            exprs.append("[0-9]{3}")

        # Extras
        # mi -> Meridian indicator (AM am Am) (PM pm Pm) (m M)
        elif f == "mi":
            exprs.append("([AaPp][Mm]|[Mm]).?")

    return "".join(exprs)


# print("^" + match_date(value) + "$")

def ipython_vars(globals_vars, dtype=None):
    """
    Return the list of data frames depending on the type
    :param globals_vars: globals() from the notebook
    :param dtype: 'pandas', 'cudf', 'dask' or 'dask_cudf'
    :return:
    """
    tmp = globals_vars.copy()
    vars = [(k, v, type(v)) for k, v in tmp.items() if
            not k.startswith('_') and k != 'tmp' and k != 'In' and k != 'Out' and not hasattr(v, '__call__')]

    if dtype == "dask_cudf":
        from dask_cudf.core import DataFrame as DaskCUDFDataFrame
        _dtype = DaskCUDFDataFrame
    elif dtype == "cudf":
        from cudf.core import DataFrame as CUDFDataFrame
        _dtype = CUDFDataFrame
    elif dtype == "dask":
        from dask.dataframe.core import DataFrame
        _dtype = DataFrame
    elif dtype == "pandas":
        import pandas as pd
        PandasDataFrame = pd.DataFrame
        _dtype = PandasDataFrame

    return [name for name, instance, aa in vars if is_(instance, _dtype)]
