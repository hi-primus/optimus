@classmethod
def assert_type_str_or_list(cls, variable):
    """

    :param variable:
    :param name_arg:
    :return:
    """
    assert isinstance(variable, (str, list)), \
        "Error: Argument must be a string or a list."


def assert_columns_names(df, required_col_names):
    """
    Check if a string or list of string are valid dataframe columns
    :param columns: columns names
    :return:
    """

    assert len(required_col_names) > 0, "Error: columns can be empty"

    # Remove duplicated columns
    if isinstance(required_col_names, list):
        required_col_names = set(required_col_names)

    # if string convert to list. Because we always return a list
    if isinstance(required_col_names, str):
        required_col_names = [required_col_names]

    all_col_names = df.columns

    # Check if the columns you want to select exits in the dataframe
    missing_col_names = [x for x in required_col_names if x not in all_col_names]

    error_message = "The {missing_col_names} columns are not included in the DataFrame with the following columns " \
                    "{all_col_names}".format(
        missing_col_names=missing_col_names,
        all_col_names=all_col_names)

    assert len(r) == 0, "Error:%s column(s) not exist(s)" % r
