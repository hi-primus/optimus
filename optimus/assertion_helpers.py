def assert_type_str_or_list(variable):
    """

    :param variable:
    :param name_arg:
    :return:
    """
    assert isinstance(variable, (str, list)), \
        "Error: Argument must be a string or a list."


def validate_columns_names(df, required_col_names):
    """
    Check if a string or list of string are valid dataframe columns
    :param df:
    :param required_col_names:
    :return:
    """

    assert len(required_col_names) > 0, "Error: columns can be empty"

    # Remove duplicated columns
    if isinstance(required_col_names, list):
        required_col_names = set(required_col_names)

    all_col_names = df.columns

    # Check if the columns you want to select exits in the dataframe
    missing_col_names = [x for x in required_col_names if x not in all_col_names]

    error_message = "The {missing_col_names} columns are not included in the DataFrame with the following columns " \
                    "{all_col_names}".format(
                        missing_col_names=missing_col_names,
                        all_col_names=all_col_names)

    assert len(missing_col_names) == 0, "Error:%s column(s) not exist(s)" % error_message


def validate_columns_names_list(df, required_col_names):
    """
    Given a list of two element extract a list of columns for the first and every element.
    :param df:
    :param required_col_names:
    :return:
    """
    # Asserting columns is string or list:
    assert isinstance(required_col_names, list), \
        "Error: Column argument must be a tuple(s)"

    # Check that the columns are valid
    validate_columns_names(df, columns)

    return columns
