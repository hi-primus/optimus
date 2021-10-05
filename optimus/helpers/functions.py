import collections
import functools
import glob
import ntpath
from typing import Union
from optimus.helpers.constants import DATE_FORMAT_ITEMS, DATE_FORMAT_ITEMS_MONTH, PYTHON_DATE_TO_FORMAT
import os
import random
import re
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path
from urllib.parse import unquote
from urllib.request import Request, urlopen

import numpy as np
import fastnumbers
import humanize
import pandas as pd
import six
from fastnumbers import isint, isfloat

from optimus import ROOT_DIR
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_dict, is_list_with_dicts, is_url

def _list_variables(ins, namespace=None):
    
    if not namespace:
        try:
            import IPython
            namespace = IPython.get_ipython().user_global_ns
        except:
            pass

    return [obj for obj in (namespace or []) if isinstance(namespace[obj], ins) and not obj.startswith("_")]


def list_engines(namespace=None):
    from optimus.engines.base.engine import BaseEngine
    return _list_variables(BaseEngine, namespace)


def list_dataframes(namespace=None):
    from optimus.engines.base.basedataframe import BaseDataFrame
    return _list_variables(BaseDataFrame, namespace)


def list_clusters(namespace=None):
    from optimus.engines.base.stringclustering import Clusters
    return _list_variables(Clusters, namespace)


def list_connections(namespace=None):
    from optimus.engines.base.dask.io.jdbc import DaskBaseJDBC
    from optimus.engines.base.io.connect import Connection
    return _list_variables((Connection, DaskBaseJDBC), namespace)


def list_file_connections(namespace=None):
    from optimus.engines.base.io.connect import Connection
    return _list_variables(Connection, namespace)


def list_database_connections(namespace=None):
    from optimus.engines.base.dask.io.jdbc import DaskBaseJDBC
    return _list_variables(DaskBaseJDBC, namespace)


def random_int(n=5):
    """
    Create a random string of ints
    :return:
    """
    return str(random.randint(1, 10 ** n))


def collect_as_list(df):
    return df.rdd.flatMap(lambda x: x).collect()


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
    pattern = r'\"(\d+\.\d+).*\"'
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
    except ImportError as e:
        print(e)
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

def transform_date_format(format: str):
    """
    Transform a date format like `yyyy/mm/dd` to a compatible one like `%Y/%m/%d`
    """

    has_time = "H" in format or "h" in format

    items = DATE_FORMAT_ITEMS if has_time else DATE_FORMAT_ITEMS_MONTH

    for i, j in items:
        reg = re.compile(f"(?<![%])(?<!%-){i}")
        format = re.sub(reg, j, format)

    reg = re.compile("(?<![%])(?<!%-)[A-z]")
    return re.sub(reg, lambda a: f"%{a.group()}", format)


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

        # Ensure that the upper bound is exactly the higher value.
        # Because floating point calculation it can miss the upper bound in the final sum

        buckets[bins - 1]["upper"] = upper_bound
    return buckets


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
    """
    Update only the given keys
    :param d:
    :param u:
    :return:
    """
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


def reduce_mem_usage(df, categorical=True, categorical_threshold=50, verbose=False):
    """
    Change the columns datatypes to reduce the memory usage. Also identify
    :param df:
    :param categorical:
    :param categorical_threshold:
    :param verbose:
    :return:
    """

    # Reference https://www.kaggle.com/arjanso/reducing-dataframe-memory-size-by-65/notebook

    start_mem_usg = df.size()
    dfd = df.data
    ints = df.cols.exec_agg(dfd.applymap(isint).sum(), True)
    floats = df.cols.exec_agg(dfd.applymap(isfloat).sum(), True)
    nulls = df.cols.exec_agg(dfd.isnull().sum(), True)
    total_rows = len(dfd)

    columns_dtype = {}
    for x, y in ints.items():

        if ints[x] == nulls[x]:
            dtype = "object"
        elif floats[x] == total_rows:
            dtype = "numerical"
        elif total_rows <= ints[x] + nulls[x]:
            dtype = "numerical"
        else:
            dtype = "object"
        columns_dtype[x] = dtype

    numerical_int = [col for col, dtype in columns_dtype.items() if dtype == "numerical"]
    final = {}

    if len(numerical_int) > 0:
        min_max = df.cols.range(numerical_int)

        import numpy as np
        for col_name in min_max.keys():
            _min = min_max[col_name]["min"]
            _max = min_max[col_name]["max"]
            if _min >= 0:
                if _max < 255:
                    final[col_name] = np.uint8
                elif _max < 65535:
                    final[col_name] = np.uint16
                elif _max < 4294967295:
                    final[col_name] = np.uint32
                else:
                    final[col_name] = np.uint64
            else:
                if _min > np.iinfo(np.int8).min and _max < np.iinfo(np.int8).max:
                    final[col_name] = np.int8
                elif _min > np.iinfo(np.int16).min and _max < np.iinfo(np.int16).max:
                    final[col_name] = np.int16
                elif _min > np.iinfo(np.int32).min and _max < np.iinfo(np.int32).max:
                    final[col_name] = np.int32
                elif _min > np.iinfo(np.int64).min and _max < np.iinfo(np.int64).max:
                    final[col_name] = np.int64
            # print(final[col_name])

    object_int = [col for col, dtype in columns_dtype.items() if dtype == "object"]
    if len(object_int) > 0:
        count_values = df.cols.value_counts(object_int)

    # if categorical is True:
    #     for col_name in object_int:
    #         if len(count_values[col_name]) <= categorical_threshold:
    #             final[col_name] = "category"

    dfd = dfd.astype(final)
    mem_usg = dfd.size

    if verbose is True:
        print("Memory usage after optimization:", humanize.naturalsize(start_mem_usg))
        print("Memory usage before optimization is: ", humanize.naturalsize(mem_usg))
        print(round(100 * mem_usg / start_mem_usg), "% of the initial size")

    return df.new(dfd, meta=df.meta)


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


def unquote_path(path):
    return unquote(path)


@functools.lru_cache(maxsize=128)
def prepare_path(path, file_format=None):
    """d
    Helper to return the file to be loaded and the file name.
    This will memoise
    :param path: Path to the file to be loaded
    :param file_format: format file
    :return:
    """
    r = []
    if is_url(path):
        file = downloader(path, file_format)
        file_name = ntpath.basename(path)
        r = [(file, file_name,)]
    else:
        for file_name in glob.glob(path, recursive=True):
            r.append((file_name, ntpath.basename(file_name),))
    if len(r) == 0:
        raise Exception("File not found")
    return r


def prepare_path_local(path):
    """
    Helper create the folder of the file to be saved
    :param path: Path to the file to be saved
    """
    path = os.path.join(Path().absolute(), path)
    os.makedirs(os.path.dirname(path), exist_ok=True)


def path_is_local(path):
    """
    Check if path is local
    :param path: Path to the file to be saved
    """
    from optimus.helpers.constants import Schemas
    if path.startswith((*Schemas.list(),)):
        return path.startswith(Schemas.FILE.value)
    return True


def prepare_url_schema(path: str, schema="https", force=False):
    """
    Prepare a url and include a schema if neccessary.
    :param path: Url path to prepare.
    :param schema: schema to include if none is found in 'path'.
    :param force: forces the schema passed to 'schema'.
    """

    if not schema.endswith("://"):
        schema += "://"

    import hiurlparser
    parsed_url = hiurlparser.parse_url(path)
    found_schema = parsed_url["protocol"] if parsed_url else None

    if found_schema is not None and not force:
        return path
    elif found_schema is None:
        return schema + path
    else:
        return schema + path[len(found_schema + "://"):]


def month_names(directive="%B"):
    """
    Gets an array with month names
    """
    import datetime
    return [datetime.datetime.strptime(str(n + 1), '%m').strftime(directive) for n in range(12)]

def weekday_names(directive="%B"):
    """
    Gets an array with month names
    """
    import datetime
    return [datetime.datetime.strptime(str(n + 1), '%m').strftime(directive) for n in range(7)]


def match_date(value, error=False):
    """
    Create a regex from a string with a date format like
    `dd/MM/yyyy hh:mm:ss-sss mi` or `%d/%m/%Y`.
    :param value: Value to test
    :return: String regex
    """
    formats = ["d", "dd", "M", "MM", "MMM", "MMMM", "MMMMM", "yy", "yyyy", "w",
               "ww", "www", "wwww", "wwwww", "h", "hh", "H", "HH", "kk", "k",
               "m", "mm", "s", "ss", "sss", "xxx", "/", ":", "-", " ", "+", "|", "a",
               "mi"]
    formats.sort(key=len, reverse=True)

    if "%" in value:
        for i, j in PYTHON_DATE_TO_FORMAT.items():
            value = value.replace(i, j)

    result = []

    start = 0

    end = len(value)
    found = False

    while start < end:
        found = False
        for f in formats:
            ignore_case = ("h" not in f.lower() and "m" not in f.lower())
            _value = value.lower() if ignore_case else value
            if _value.startswith(f, start):
                start = start + len(f)
                result.append(f)
                found = True
                break
        if found is False:
            error_str = f"{value[start]} is not a valid date format. ({value})"
            if error:
                raise ValueError(error_str)
            else:
                logger.warn(error_str)
                return False

    exprs = []
    for f in result:
        # Separators
        if f in ["/", ":", "-", " ", "|", "+", " "]:
            exprs.append("\\" + f)

        # Day
        # d  -> 1 ... 31
        # dd -> 01 ... 31

        elif f == "d":
            exprs.append("((3[0-1])|([1-2][0-9])|(0?[1-9]))")
        elif f == "dd":
            exprs.append("((3[0-1])|([1-2][0-9])|(0[1-9]))")

        # Month
        # M    -> 1 ... 12
        # MM   -> 01 ... 12
        # MMM  -> Jan, Feb, Sep
        # MMMM -> January, February, September
        elif f == "M":
            exprs.append("((1[0-2])|(0?[1-9]))")
        elif f == "MM":
            exprs.append("((1[0-2])|(0[1-9]))")
        elif f == "MMM":
            exprs.append(f"({'|'.join(month_names('%b'))})")
        elif f in ["MMMM", "MMMMM"]:
            exprs.append(f"({'|'.join(month_names('%B'))})")

        # Weekday
        # w    -> 0 ... 6
        # ww   -> 00 ... 06
        # www  -> Mon, Tues, Sat
        # wwww -> Monday, Tuesday, Saturday
        elif f == "w":
            exprs.append("(0?[0-6])")
        elif f == "ww":
            exprs.append("(0[0-6])")
        elif f == "www":
            exprs.append(f"({'|'.join(weekday_names('%a'))})")
        elif f in ["wwww", "wwwww"]:
            exprs.append(f"({'|'.join(weekday_names('%A'))})")

        # Year
        # yy   -> 00 ... 99
        # yyyy -> 0000 ... 9999
        elif f == "yy":
            exprs.append("([0-9]{2})")
        elif f == "yyyy":
            exprs.append("([0-9]{4})")

            # Hours
        # h  -> 1,2 ... 12
        # hh -> 01,02 ... 12
        # H  -> 0,1 ... 23
        # HH -> 00,01 ... 23
        # k  -> 1,2 ... 24
        # kk -> 01,02 ... 24
        elif f == "h":
            exprs.append("((1[0-2])|(0?[1-9]))")
        elif f == "hh":
            exprs.append("((1[0-2])|(0[1-9]))")
        elif f == "H":
            exprs.append("((0?[0-9])|(1[0-9])|(2[0-3])|[0-9])")
        elif f == "HH":
            exprs.append("((0[0-9])|(1[0-9])|(2[0-3])|[0-9])")
        elif f == "k":
            exprs.append("((0?[1-9])|(1[0-9])|(2[0-4])|[1-9])")
        elif f == "kk":
            exprs.append("((0[1-9])|(1[0-9])|(2[0-4]))")

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
            exprs.append("([0-9]{3})")

        # Extras
        # mi or a -> Meridian indicator (AM am Am) (PM pm Pm) (m M)
        elif f in ["mi", "a"]:
            exprs.append(r"([A|P]?.?\s?)M.?")

    return r"(?i)%s" % "".join(exprs)


def df_dicts_equal(df1, df2, decimal: Union[int, bool] = True, assertion=False):
    if decimal is True:
        decimal = 7
    for k in df1:
        try:
            np.testing.assert_almost_equal(df1[k], df2[k], decimal=decimal)
        except AssertionError as e:
            if assertion:
                logger.warn(f"AssertionError on column {k}")
                raise e
            return False
        except Exception:
            if df1[k] != df2[k]:
                if assertion:
                    raise AssertionError(f"Dataframes are not equal on column '{k}'")
                return False
    return True


def results_equal(r1, r2, decimal: Union[int, bool] = True, assertion=False):
    
    if decimal is True:
        decimal = 7

    try:
        
        if hasattr(r1, "__len__") and len(r1) != len(r2):
            raise AssertionError(f"Lengths '{len(r1)}' and '{len(r2)}' do not match ('{r1}', '{r2}')")

        matching = None

        if is_dict(r1):

            if not is_dict(r2):
                raise AssertionError(f"Types '{str(type(r1))}' and '{str(type(r2))}' do not match")
                
            matching = True
            
            for key in r1:
                if not results_equal(r1[key], r2.get(key, None), decimal, assertion):
                    matching = False
                    break

        if is_list_with_dicts(r1):
            for e1, e2 in zip(r1, r2):
                if not results_equal(e1, e2, decimal, assertion):
                    matching = False
                    break

        if matching is not None:
            return matching
        
        try:
            np.testing.assert_almost_equal(r1, r2, decimal=decimal)
        except AssertionError as e:
            raise e
        except Exception:
            if r1 != r2:
                raise AssertionError(f"'{r1}' and '{r2}' do not match")
            else:
                return True

    except Exception as e:
        if assertion:
            raise e
        return False

    return True

# Taken from https://github.com/Kemaweyan/singleton_decorator/
class _SingletonWrapper:
    """
    A singleton wrapper class. Its instances would be created
    for each decorated class.
    """

    def __init__(self, cls):
        self.__wrapped__ = cls
        self._instance = None

    def __call__(self, *args, **kwargs):
        """Returns a single instance of decorated class"""
        if self._instance is None:
            self._instance = self.__wrapped__(*args, **kwargs)
        return self._instance


def singleton(cls):
    """
    A singleton decorator. Returns a wrapper objects. A call on that object
    returns a single instance object of decorated class. Use the __wrapped__
    attribute to access decorated class directly in unit tests
    """
    return _SingletonWrapper(cls)
