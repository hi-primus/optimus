from IPython.display import display, HTML
from pyspark.sql import DataFrame

from optimus.helpers.checkit import is_list_of_one_element, is_list_of_strings, is_one_element, is_list_of_tuples, \
    is_list_of_str_or_int, is_str, is_str_or_int
from optimus.helpers.constants import TYPES, SPARK_TYPES, TYPES_SPARK_FUNC
from optimus.helpers.raiseit import RaiseIfNot

import re
import random


def random_name():
    """
    Create a unique filename
    :return:
    """
    return str(random.randint(1, 100))


def parse_spark_dtypes(value):
    """

    :param value:
    :return:
    """
    try:
        data_type = TYPES_SPARK_FUNC[SPARK_TYPES[value.lower()]]
    except TypeError:
        print("Expected {0}, got {1}".format(",".join([k for k in SPARK_TYPES]), value))
    return data_type


def parse_python_dtypes(value):
    """

    :param value:
    :return:
    """
    try:
        data_type = TYPES[value.lower()]
    except TypeError:
        print("Expected {0}, got {1}".format(",".join([k for k in TYPES]), value))
    return data_type


def print_html(html):
    """
    Just a display() helper to print html code
    :param html: html code to be printed
    :return:
    """
    display(HTML(html))


def collect_to_dict(value):
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
        # if there is only an element in the list return only de dict
        if len(dict_result) == 1:
            dict_result = dict_result[0]

    return dict_result


def one_list_to_val(val):
    """
    Convert a single list element to val
    :param val:
    :return:
    """
    if isinstance(val, list) and len(val) == 1:
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
    if isinstance(val, (int, float, str)):
        result = [val]
    else:
        result = val

    return result


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
    :param val: dict to be formated
    :return:
    """

    def _format_dict(_val):
        if not isinstance(_val, dict):
            return _val

        for k, v in _val.items():
            if isinstance(v, dict):
                if len(v) == 1:
                    _val[k] = next(iter(v.values()))
            else:
                if len(_val) == 1:
                    _val = v
        return _val

    # We apply 2 pass to the dict to process internals dicts and the whole dict
    if isinstance(val, dict) and len(val) == 1:
        val = next(iter(val.values()))

    # TODO: Maybe this can be done in a recursive way
    return repeat(_format_dict, 2, val)


def validate_columns_names(df, col_names, index=0):
    """
    Check if a string or list of string are valid dataframe columns
    :param df:
    :param col_names:
    :param index:
    :return:
    """

    columns = val_to_list(col_names)
    print(is_one_element(columns))
    if not is_list_of_one_element(columns):
        columns = [c[index] for c in columns]

    print(columns)
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
        RaiseIfNot.value_error(missing_columns, df.columns)

    return False


def parse_columns(df, cols_args, get_args=False, is_regex=None, filter_by_dtypes=None):
    """
    Return a list of columns and check that columns exists in the dadaframe
    Accept '*' as parameter in which case return a list of all columns in the dataframe.
    Also accept a regex.
    If a list of tuples return to list. The first element is the columns name the others element are params.
    This params can me used to create custom transformation functions. You can find and example in cols().cast()
    :param df: Dataframe in which the columns are going to be checked
    :param cols_args: Accepts * as param to return all the string columns in the dataframe
    :param filter_by_dtypes:
    :param get_args:
    :param is_regex: Use True is col_attrs is a regex
    :return: A list of columns string names
    """

    cols = None
    attrs = None

    # if columns value is * get all dataframes columns
    if cols_args == "*":
        cols = list(map(lambda dtypes: dtypes[0], df.dtypes))

    # In case we have a list of tuples we use the first element of the tuple is taken as the column name
    # and the rest as params. We can use the param in a custom function as follow
    # def func(attrs): attrs return (1,2) and (3,4)
    #   return attrs[0] + 1
    # df.cols().apply([('col_1',1,2),('cols_2', 3 ,4)], func)

    # Verify if we have a list with tuples
    elif is_list_of_tuples(cols_args):
        # Extract a specific position in the tuple
        # columns = [c[index] for c in columns]
        cols = [(i[0:1][0]) for i in cols_args]
        attrs = [(i[1:]) for i in cols_args]

    # if cols are string or int
    elif is_list_of_str_or_int(cols_args):
        cols = [c if is_str(c) else df.columns[c] for c in cols_args]

    elif is_str_or_int(cols_args):
        # Verify if a regex
        if is_regex:
            r = re.compile(cols_args)
            cols = list(filter(r.match, df.columns))
        else:
            cols = val_to_list(cols_args)
    else:

        # Verify that columns are a string or list of string

        assert isinstance(cols_args, (str, list)), "Columns param must be a string or a list"

    check_for_missing_columns(df, cols)

    # Filter columns by data type
    if filter_by_dtypes is not None:
        columns_filtered = parse_python_dtypes(filter_by_dtypes)
        cols = list(set(cols).intersection(filter_col_name_by_type(df, columns_filtered)))

    # Return cols or cols an params
    if get_args is True:
        params = cols, attrs
    else:
        params = cols

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


def filter_col_name_by_type(df, data_type):
    # data_type = parse_spark_dtypes(data_type)
    return [y[0] for y in filter(lambda x: x[1] == data_type, df.dtypes)]
