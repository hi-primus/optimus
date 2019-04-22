import datetime
import inspect
import json
import os
import pprint
import random
import re

from IPython.display import display, HTML
from fastnumbers import isint, isfloat
from pyspark.ml.linalg import DenseVector
from pyspark.sql.types import ArrayType

from optimus.helpers.checkit import is_list_of_one_element, is_list_of_strings, is_list_of_tuples, \
    is_str, is_dict_of_one_element, is_tuple, is_dict, is_list, is_, is_bool, is_datetime, is_date, is_binary, \
    str_to_boolean, str_to_date, str_to_array
from optimus.helpers.constants import PYTHON_SHORT_TYPES, SPARK_SHORT_DTYPES, SPARK_DTYPES_DICT, \
    SPARK_DTYPES_DICT_OBJECTS
from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt


def infer(value):
    """
    Infer a Spark datatype from a value
    :param value: value to be inferred
    :return: Spark datatype
    """
    result = None
    # print(v)
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

    return get_spark_dtypes_object(result)


def parse_spark_dtypes(value):
    """
    Get a pyspark data type from a string data type representation. for example 'StringType' from 'string'
    :param value:
    :return:
    """

    value = val_to_list(value)

    try:
        data_type = [SPARK_DTYPES_DICT[SPARK_SHORT_DTYPES[v]] for v in value]

    except KeyError:
        data_type = value

    data_type = one_list_to_val(data_type)
    return data_type


def get_spark_dtypes_object(value):
    """
    Get a pyspark data class from a string data type representation. for example 'StringType()' from 'string'
    :param value:
    :return:
    """
    value = val_to_list(value)
    try:
        data_type = [SPARK_DTYPES_DICT_OBJECTS[SPARK_SHORT_DTYPES[v]] for v in value]

    except (KeyError, TypeError):
        data_type = value

    data_type = one_list_to_val(data_type)
    return data_type


def parse_python_dtypes(value):
    """
    Get a spark data type from a string
    :param value:
    :return:
    """
    return PYTHON_SHORT_TYPES[value.lower()]


def random_int(n=5):
    """
    Create a random string of ints
    :return:
    """
    return str(random.randint(1, 10 ** n))


def print_html(html):
    """
    Just a display() helper to print html code
    :param html: html code to be printed
    :return:
    """
    display(HTML(html))


def print_json(value):
    """
    Print a human readable json
    :param value: json to be printed
    :return: json
    """
    pp = pprint.PrettyPrinter(indent=2)
    if is_str(value):
        value = value.replace("'", "\"")
        value = json.loads(value)

    pp.pprint(value)


def collect_as_list(df):
    return df.rdd.flatMap(lambda x: x).collect()


def collect_as_dict(value):
    """
    Return a dict from a Collect result
    :param value:
    :return:
    """

    dict_result = []

    # if there is only an element in the dict just return the value
    if len(dict_result) == 1:
        dict_result = next(iter(dict_result.values()))
    else:
        dict_result = [v.asDict() for v in value]

    return dict_result


def one_list_to_val(val):
    """
    Convert a single list element to val
    :param val:
    :return:
    """
    if is_list_of_one_element(val):
        result = val[0]
    else:
        result = val

    return result


def val_to_list(val):
    """
    Convert a single value string or number to a list
    :param val:
    :return:
    """
    if not is_list(val):
        val = [val]

    return val


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


def repeat(f, n, x):
    if n == 1:  # note 1, not 0
        return f(x)
    else:
        return f(repeat(f, n - 1, x))  # call f with returned value


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

    # TODO: Maybe this can be done in a recursive way
    # We apply two passes to the dict so we can process internals dicts and the superiors ones
    return repeat(_format_dict, 2, val)


def validate_columns_names(df, col_names, index=0):
    """
    Check if a string or list of string are valid dataframe columns
    :param df: Data frame to be analyzed
    :param col_names: columns names to be checked
    :param index:
    :return:
    """

    columns = val_to_list(col_names)

    if is_list_of_tuples(columns):
        columns = [c[index] for c in columns]

    # Remove duplicates in the list
    if is_list_of_strings(columns):
        columns = set(columns)

    check_for_missing_columns(df, columns)

    return True


def check_for_missing_columns(df, col_names):
    """
    Check if the columns you want to select exits in the dataframe
    :param df: Dataframe to be checked
    :param col_names: cols names to
    :return:
    """
    missing_columns = list(set(col_names) - set(df.columns))

    if len(missing_columns) > 0:
        RaiseIt.value_error(missing_columns, df.columns)

    return False


def parse_columns(df, cols_args, get_args=False, is_regex=None, filter_by_column_dtypes=None,
                  accepts_missing_cols=False):
    """
    Return a list of columns and check that columns exists in the dataframe
    Accept '*' as parameter in which case return a list of all columns in the dataframe.
    Also accept a regex.
    If a list of tuples return to list. The first element is the columns name the others element are params.
    This params can me used to create custom transformation functions. You can find and example in cols().cast()
    :param df: Dataframe in which the columns are going to be checked
    :param cols_args: Accepts * as param to return all the string columns in the dataframe
    :param get_args:
    :param is_regex: Use True is col_attrs is a regex
    :param filter_by_column_dtypes:
    :param accepts_missing_cols: if true not check if column exist in the dataframe
    :return: A list of columns string names
    """

    cols = None
    attrs = None

    # ensure that cols_args is a list
    # cols_args = val_to_list(cols_args)

    # if columns value is * get all dataframes columns
    if is_regex is True:
        r = re.compile(cols_args[0])
        cols = list(filter(r.match, df.columns))

    elif cols_args == "*" or cols_args is None:
        cols = df.columns

    # In case we have a list of tuples we use the first element of the tuple is taken as the column name
    # and the rest as params. We can use the param in a custom function as follow
    # def func(attrs): attrs return (1,2) and (3,4)
    #   return attrs[0] + 1
    # df.cols().apply([('col_1',1,2),('cols_2', 3 ,4)], func)

    # Verify if we have a list with tuples
    elif is_tuple(cols_args) or is_list_of_tuples(cols_args):
        cols_args = val_to_list(cols_args)
        # Extract a specific position in the tuple
        cols = [(i[0:1][0]) for i in cols_args]
        attrs = [(i[1:]) for i in cols_args]
    else:
        # if not a list convert to list
        cols = val_to_list(cols_args)
        # Get col name from index
        cols = [c if is_str(c) else df.columns[c] for c in cols]

    # Check for missing columns
    if accepts_missing_cols is False:
        check_for_missing_columns(df, cols)

    # Filter by column data type
    filter_by_column_dtypes = val_to_list(filter_by_column_dtypes)

    if is_list_of_strings(filter_by_column_dtypes):
        # Get columns for every data type
        columns_filtered = filter_col_name_by_dtypes(df, filter_by_column_dtypes)

        # Intersect the columns filtered per datatype from the whole dataframe with the columns passed to the function
        cols = list(set(cols).intersection(columns_filtered))

    # Return cols or cols an params
    if get_args is True:
        params = cols, attrs
    elif get_args is False:
        params = cols
    else:
        RaiseIt.value_error(get_args, ["True", "False"])

    return params


def tuple_to_dict(value):
    """
    Convert tuple to dict
    :param value: tuple to be converted
    :return:
    """

    return format_dict(dict((x, y) for x, y in value))


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


def filter_col_name_by_dtypes(df, data_type):
    """
    Return column names filtered by the column data type
    :param df: Dataframe which columns are going to be filtered
    :param data_type: Datatype used to filter the column.
    :type data_type: str or list
    :return:
    """
    data_type = parse_spark_dtypes(data_type)

    # isinstace require a tuple
    data_type = tuple(val_to_list(data_type))

    # Filter columns by data type
    return [c for c in df.columns if isinstance(df.schema[c].dataType, data_type)]


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


def get_var_name(var):
    """
    Get the var name from the var passed to a function
    :param var:
    :return:
    """
    _locals = inspect.stack()[2][0].f_locals
    for name in _locals:
        if id(var) == id(_locals[name]):
            return name
    return None


def json_converter(obj):
    """
    Custom converter to be uses with json.dumps
    :param obj:
    :return:
    """
    if isinstance(obj, datetime.datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')

    elif isinstance(obj, datetime.date):
        return obj.strftime('%Y-%m-%d')


def json_enconding(obj):
    """
    Encode a json. Used for testing.
    :param obj:
    :return:
    """
    return json.dumps(obj, sort_keys=True, default=json_converter)
