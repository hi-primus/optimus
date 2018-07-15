from IPython.display import display, HTML


def isfunction(obj):
    """
    check if a param is a function
    :param obj: object to check for
    :return:
    """
    return hasattr(obj, '__call__')


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

    if index is not None:
        assert isinstance(col_names, list), "Error: col_names must be a list"
        columns = [c[index] for c in col_names]
    else:
        columns = col_names

    assert len(columns) > 0, "Error: columns param can not be empty"

    assert columns is not None, "Error: columns para is none"

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


def parse_columns(df, columns, index=None):
    """
    Check that a column list is a valis list of columns.
    :param df: Dataframe in which the columns are going to be checked
    :param columns: Acepts * as param to return all the string columns in the dataframe
    :param index: if a tuple get the column from a specific index
    :return: A list of columns string names
    """

    # assert isinstance(df, (Dataframe))

    # Verify that columns are a string or list of string
    assert isinstance(columns, (str, list)), "columns param must be a string or a list"

    # if columns value is * get all dataframes columns
    if columns == "*":
        columns = list(map(lambda t: t[0], df.dtypes))

    # if string convert to list. Because we always return a list
    if isinstance(columns, str):
        columns = [columns]

    # Verify if we have a list
    elif isinstance(columns, list):
        # Verify that we have list inside the tuples
        if all(isinstance(x, tuple) for x in columns):
            # Extract a specific position in the tupple
            columns = [c[index] for c in columns]

    # Validate that all the columns exist
    validate_columns_names(df, columns)

    return columns
