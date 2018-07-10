

def validate_columns_names(df, col_names, index=None):
    """
    Check if a string or list of string are valid dataframe columns
    :param df:
    :param col_names:
    :param index:
    :return:
    """

    if index is not None:
        assert isinstance(col_names, list), "col_names must be a list"
        columns = [c[index] for c in col_names]
    else:
        columns = col_names

    assert len(columns) > 0, "Error: columns can not be empty"

    # Remove duplicated columns
    if isinstance(columns, list):
        columns = set(columns)

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
    :param columns:  Acepts * as param to return all the string columns in the dataframe
    :return: A list of columns string names
    """

    #assert isinstance(df, (Dataframe))

    # Verify that columns are a string or list of string
    assert isinstance(columns, (str, list)), "columns param must be a string or a list"

    # if columns value is * get all dataframes columns
    if columns == "*":
        columns = list(map(lambda t: t[0], self.dtypes))

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


def one_list_to_val(l):
    """
    Convert a single list element to val
    :param l:
    :return:
    """
    if isinstance(l, list) and len(l) == 1:
        result = l[0]
    else:
        result = l

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
