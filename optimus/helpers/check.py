"""
Helpers to check if an object match a date type
"""


# TODO: can be confused with is_type
from optimus.helpers.parser import parse_spark_dtypes
from optimus.helpers.converter import val_to_list, one_list_to_val
from optimus.helpers.raiseit import RaiseIt


def has_(value, _type):
    """
    Check if a list has a element of a specific data type
    :param value: list
    :param _type: data type to check
    :return:
    """
    return any(isinstance(elem, _type) for elem in value)


def is_column_a(df, column, dtypes):
    """
    Check if column match a list of data types
    :param df: dataframe
    :param column: column to be compared with
    :param dtypes: types to be checked
    :return:
    """
    column = val_to_list(column)

    if len(column) > 1:
        RaiseIt.length_error(column, 1)

    data_type = tuple(val_to_list(parse_spark_dtypes(dtypes)))
    column = one_list_to_val(column)

    # Filter columns by data type
    return isinstance(df.schema[column].dataType, data_type)