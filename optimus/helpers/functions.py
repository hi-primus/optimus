from IPython.display import display, HTML
from pyspark.sql import DataFrame


def isfunction(obj):
    """
    check if a param is a function
    :param obj: object to check for
    :return:
    """
    return hasattr(obj, '__call__')


def is_list_of_strings(lst):
    """
    Check that all elements in a list are strings
    :param lst:
    :return:
    """
    return bool(lst) and isinstance(lst, list) and all(isinstance(elem, str) for elem in lst)


def is_list_of_numeric(lst):
    """
    Check that all elements in a list are int or float
    :param lst:
    :return:
    """
    return bool(lst) and isinstance(lst, list) and all(isinstance(elem, (int, float)) for elem in lst)


def is_list_of_tuples(lst):
    """
    Check that all elements in a list are tuples
    :param lst:
    :return:
    """
    return bool(lst) and isinstance(lst, list) and all(isinstance(elem, tuple) for elem in lst)


def is_one_element(value):
    """
    Check that a var is a single element
    :param value:
    :return:
    """
    return isinstance(value, (str, int))


def print_html(html):
    """
    Just a display() helper to print html code
    :param html: html code to be printed
    :return:
    """
    display(HTML(html))


def collect_to_dict(value):
    """
    Return a dict from a Collector result
    :param value:
    :return:
    """
    return value[0].asDict()


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
    Convert a list to None, int,str or a list filtering a specific index
    :param val:
    :param index:
    :return:
    """
    if len(val) == 0:
        return None
    else:
        return one_list_to_val([column[index] for column in val])


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

    assert len(missing_col_names) == 0, "Error:%s column(s) not exist(s)" % error_message

    return True


def parse_columns(df, cols_attrs, index=None):
    """
    Check that a column list is a valid list of columns.
    Return a list of columns and check that columsn exists in the adtaframe
    Accept '*' as parameter in which case return a list od all columns in the dataframe
    If a list of tuples return to list. The firts is a list of columns names the second is a list of params.
    This params can me used to create custom transformation functions. You can find and example in cols().cast()
    :param df: Dataframe in which the columns are going to be checked
    :param cols_attrs: Accepts * as param to return all the string columns in the dataframe
    :param index: if a tuple get the column from a specific index
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

    # In case we have tuples we use the first element as the column names and the rest as param  whatever
    # custom function we need.
    # def func(attrs): attrs return (1,2) and (3,4)
    #   return attrs[0] + 1
    # df.cols().apply([('col_1',1,2),('cols_2', 3 ,4)], func)

    # Verify if we have a list with tuples
    elif is_list_of_tuples(cols_attrs):
        # Extract a specific position in the tuple
        # columns = [c[index] for c in columns]
        cols = [(i[0:1][0]) for i in cols_attrs]
        attrs = [(i[1:]) for i in cols_attrs]

    elif is_list_of_numeric(cols_attrs) or is_list_of_strings(cols_attrs):
        cols = cols_attrs

    elif is_one_element(cols_attrs):
        cols = val_to_list(cols_attrs)

    # Validate that all the columns exist
    validate_columns_names(df, cols)

    return cols, attrs

