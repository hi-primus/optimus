from IPython.display import display, HTML
from pyspark.sql import DataFrame

from optimus.helpers.checkit import *
from optimus.helpers.constants import *

import re


def parse_spark_dtypes(value):
    """

    :param value:
    :return:
    """
    try:
        data_type = TYPES_SPARK_FUNC[TYPES[value.lower()]]
    except TypeError:
        print("Expected {0}, got {1}".format(",".join([k for k in TYPES]), value))
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


def validate_columns_names(df, col_names, index=None):
    """
    Check if a string or list of string are valid dataframe columns
    :param df:
    :param col_names:
    :param index:
    :return:
    """

    # assert col_names is None, "Error: columns not defined"

    if index is not None:
        assert isinstance(col_names, list), "Error: col_names must be a list"
        columns = [c[index] for c in col_names]
    else:
        columns = col_names

    # assert len(columns) > 0, "Error: columns param can not be empty"
    assert columns is not None, "Error: columns param is none"

    # Remove duplicated columns
    if isinstance(columns, list):
        columns = set(columns)

    # if str or int convert
    if isinstance(columns, (str, int)):
        columns = val_to_list(columns)

    all_col_names = df.columns

    # Check if the columns you want to select exits in the dataframe
    missing_col_names = [x for x in columns if x not in all_col_names]

    error_message = "The {missing_col_names} columns do not exists in the DataFrame with the following columns " \
                    "{all_col_names}".format(
        missing_col_names=missing_col_names,
        all_col_names=all_col_names)

    assert len(missing_col_names) == 0, error_message

    return True


def parse_columns(df, cols_attrs, get_attrs=False, is_regex=None, filter_by_type=None):
    """
    Return a list of columns and check that columns exists in the dadaframe
    Accept '*' as parameter in which case return a list of all columns in the dataframe.
    Also accept a regex.
    If a list of tuples return to list. The first element is the columns name the others element are params.
    This params can me used to create custom transformation functions. You can find and example in cols().cast()
    :param df: Dataframe in which the columns are going to be checked
    :param cols_attrs: Accepts * as param to return all the string columns in the dataframe
    :param filter_by_type:
    :param get_attrs:
    :param is_regex: Use True is col_attrs is a regex
    :return: A list of columns string names
    """

    cols = None
    attrs = None

    assert isinstance(df, DataFrame), "Error: df is not a Dataframe"

    # Verify that columns are a string or list of string
    assert isinstance(cols_attrs, (str, list)), "Columns param must be a string or a list"

    # if columns value is * get all dataframes columns
    if cols_attrs == "*":
        cols = list(map(lambda t: t[0], df.dtypes))

    # In case we have a list of tuples we use the first element of the tuple is taken as the column name
    # and the rest as params. We can use the param in a custom function as follow
    # def func(attrs): attrs return (1,2) and (3,4)
    #   return attrs[0] + 1
    # df.cols().apply([('col_1',1,2),('cols_2', 3 ,4)], func)

    # Verify if we have a list with tuples
    elif is_list_of_tuples(cols_attrs):
        # Extract a specific position in the tuple
        # columns = [c[index] for c in columns]
        cols = [(i[0:1][0]) for i in cols_attrs]
        attrs = [(i[1:]) for i in cols_attrs]

    # if cols are string or int
    elif is_list_of_str_or_int(cols_attrs):
        cols = [c if isinstance(c, str) else df.columns[c] for c in cols_attrs]

    elif is_str_or_int(cols_attrs):
        # Verify if a regex
        if is_regex:
            r = re.compile(cols_attrs)
            cols = list(filter(r.match, df.columns))
        else:
            cols = val_to_list(cols_attrs)

    # Filter columns by data type
    if filter_by_type is not None:
        filter_by_type = parse_python_dtypes(filter_by_type)
        cols = list(set(cols).intersection(filter_col_name_by_type(df, filter_by_type)))

    # Return cols or cols an params
    if get_attrs:
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
    #data_type = parse_spark_dtypes(data_type)
    return [y[0] for y in filter(lambda x: x[1] == data_type, df.dtypes)]
