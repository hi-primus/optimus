import collections
import datetime
import inspect
import json
import os
import pprint
import random
import re
from inspect import currentframe, getframeinfo

from IPython.display import display, HTML
from fastnumbers import isint, isfloat
from ordered_set import OrderedSet
from pyspark.ml.linalg import DenseVector
from pyspark.sql.types import ArrayType

from optimus.helpers.checkit import is_list_of_strings, is_list_of_tuples, \
    is_str, is_dict_of_one_element, is_tuple, is_dict, is_list, is_, is_bool, is_datetime, is_date, is_binary, \
    str_to_boolean, str_to_date, str_to_array, is_list_of_one_element
from optimus.helpers.constants import SPARK_SHORT_DTYPES, SPARK_DTYPES_DICT_OBJECTS
from optimus.helpers.convert import val_to_list, one_list_to_val
from optimus.helpers.logger import logger
from optimus.helpers.parser import parse_spark_dtypes
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

    return get_spark_dtypes_object(result)


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


def random_int(n=5):
    """
    Create a random string of ints
    :return:
    """
    return str(random.randint(1, 10 ** n))


def print_html(html):
    """
    Display() helper to print html code
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


def collect_as_dict(df):
    """
    Return a dict from a Collect result
    :param df:
    :return:
    """

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
        columns = OrderedSet(columns)

    check_for_missing_columns(df, columns)

    return True


def check_for_missing_columns(df, col_names):
    """
    Check if the columns you want to select exits in the dataframe
    :param df: Dataframe to be checked
    :param col_names: cols names to
    :return:
    """
    missing_columns = list(OrderedSet(col_names) - OrderedSet(df.schema.names))

    if len(missing_columns) > 0:
        RaiseIt.value_error(missing_columns, df.columns)

    return False


def replace_multiple_characters(string, to_be_replaced, replace_by):
    """
    Replace multiple single characters in s string
    :param string:
    :param to_be_replaced: Character to be replaced
    :param replace_by: character or string that will replace the matched character
    :return:
    """
    # Iterate over the strings to be replaced
    for elem in to_be_replaced:
        # Check if string is in the main string
        if elem in string:
            # Replace the string
            string = string.replace(elem, replace_by)
    return string


def replace_columns_special_characters(df, replace_by="_"):
    """
    Remove special character from Spark column name
    :param df: Spark Dataframe
    :param replace_by: character or string that will replace the matched character
    :return:
    """
    for col_name in df.cols.names():
        df = df.cols.rename(col_name, replace_multiple_characters(col_name, ["."], replace_by))
    return df


def escape_columns(columns):
    """
    Add a backtick to a columns name to prevent the dot in name problem
    :param columns:
    :return:
    """

    escaped_columns = []
    if is_list(columns):
        for col in columns:
            # Check if the column is already escaped
            if col[0] != "`" and col[len(col) - 1] != "`":
                escaped_columns.append("`" + col + "`")
            else:
                escaped_columns.append(col)
    else:
        # Check if the column is already escaped
        if columns[0] != "`" and columns[len(columns) - 1] != "`":
            escaped_columns = "`" + columns + "`"
        else:
            escaped_columns.append(columns)

    return escaped_columns


def get_output_cols(input_cols, output_cols):
    # Construct input and output columns names
    if is_list(input_cols) and is_list(output_cols):
        if len(input_cols) != len(output_cols):
            RaiseIt.length_error(input_cols, output_cols)
    elif is_list(input_cols) and is_str(output_cols):
        if len(input_cols) > 1:
            output_cols = list([i + output_cols for i in input_cols])
        else:
            output_cols = val_to_list(output_cols)
    elif is_str(input_cols) and is_str(output_cols):
        output_cols = val_to_list(output_cols)
    elif output_cols is None:
        output_cols = input_cols

    return output_cols


def parse_columns(df, cols_args, get_args=False, is_regex=None, filter_by_column_dtypes=None,
                  accepts_missing_cols=False, invert=False):
    """
    Return a list of columns and check that columns exists in the dataframe
    Accept '*' as parameter in which case return a list of all columns in the dataframe.
    Also accept a regex.
    If a list of tuples return to list. The first element is the columns name the others element are params.
    This params can be used to create custom transformation functions. You can find and example in cols().cast()
    :param df: Dataframe in which the columns are going to be checked
    :param cols_args: Accepts * as param to return all the string columns in the dataframe
    :param get_args:
    :param is_regex: Use True is col_attrs is a regex
    :param filter_by_column_dtypes: A data type for which a columns list is going be filtered
    :param accepts_missing_cols: if true not check if column exist in the dataframe
    :param invert: Invert the final selection. For example if you want to select not integers

    :return: A list of columns string names
    """

    attrs = None

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
    if filter_by_column_dtypes is not None:
        filter_by_column_dtypes = val_to_list(filter_by_column_dtypes)

    columns_residual = None

    # If necessary filter the columns by data type
    if filter_by_column_dtypes:
        # Get columns for every data type

        columns_filtered = filter_col_name_by_dtypes(df, filter_by_column_dtypes)

        # Intersect the columns filtered per data type from the whole dataframe with the columns passed to the function
        final_columns = list(OrderedSet(cols).intersection(columns_filtered))

        # This columns match filtered data type
        columns_residual = list(OrderedSet(cols) - OrderedSet(columns_filtered))
    else:
        final_columns = cols

    # Return cols or cols an params
    cols_params = []

    if invert:
        final_columns = list(OrderedSet(cols) - OrderedSet(final_columns))

    if get_args is True:
        cols_params = final_columns, attrs
    elif get_args is False:
        cols_params = final_columns
    else:
        RaiseIt.value_error(get_args, ["True", "False"])

    # if columns_residual:
    #     print(",".join(escape_columns(columns_residual)), "column(s) was not processed because is/are not",
    #           ",".join(filter_by_column_dtypes))

    return cols_params


def check_column_numbers(columns, number=0):
    """
    Check if the columns number match number expected
    :param columns:
    :param number: Number of columns to check
    :return:
    """
    count = len(columns)

    if number is "*":
        if not len(columns) >= 1:
            RaiseIt.value_error(len(columns), ["more than 1"])
    elif not len(columns) == number:

        RaiseIt.value_error(count, "Receive {} columns, {} needed".format(number, columns))


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
    Custom converter to be used with json.dumps
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
    return json.dumps(obj, default=json_converter)


def debug(value):
    """
    print a message with line and file name
    :param value: string to be printed
    :return:
    """

    frame_info = getframeinfo(currentframe())
    print("{}->{}:{}".format(value, frame_info.filename, frame_info.lineno, ))
