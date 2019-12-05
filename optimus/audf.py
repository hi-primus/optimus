from pyspark.sql import functions as F

from infer import Infer, is_column, parse_spark_class_dtypes
from optimus.helpers.parser import parse_python_dtypes
from optimus.helpers.converter import one_list_to_val
from optimus.helpers.functions import is_pyarrow_installed
from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt


def abstract_udf(col, func, func_return_type=None, attrs=None, func_type=None):
    """
    Abstract User defined functions. This is a helper function to create udf, pandas udf or a Column Exp
    :param col: Column to created or transformed
    :param func: Function to be applied to the data
    :param attrs: If required attributes to be passed to the function
    :param func_return_type: Required by UDF and Pandas UDF.
    :param func_type: pandas_udf or udf. The function is going to try to use pandas_udf if func_type is not defined
    :return: A function, UDF or Pandas UDF
    """

    if func_return_type is None:
        func_type = "column_exp"
    # By default is going to try to use pandas UDF
    elif func_type is None and is_pyarrow_installed() is True:
        func_type = "pandas_udf"

    types = ["column_exp", "udf", "pandas_udf"]
    if func_type not in types:
        RaiseIt.value_error(func_type, types)

    # It handle if func param is a plain expression or a function returning and expression
    def func_col_exp(col_name, attr):
        return func

    if is_column(func):
        _func = func_col_exp
    else:
        _func = func

    logger.print(
        "Using '{func_type}' to process column '{column}' with function {func_name}".format(func_type=func_type,
                                                                                            column=col,
                                                                                            func_name=_func.__name__))

    df_func = func_factory(func_type, func_return_type)
    return df_func(attrs, _func)(col)


def func_factory(func_type=None, func_return_type=None):
    """
    Return column express, udf or pandas udf function.
    :param func_type: Type of function udf or pandas udf
    :param func_return_type:
    :return:
    """

    func_return_type = parse_spark_class_dtypes(func_return_type)

    def pandas_udf_func(attr=None, func=None):
        # TODO: Get the column type, so is not necessary to pass the return type as param.

        # Apply the function over the whole series
        def apply_to_series(val, attr):
            if attr is None:
                attr = (None,)
            else:
                attr = (attr,)

            return val.apply(func, args=attr)

        def to_serie(value):
            return apply_to_series(value, attr)

        return F.pandas_udf(to_serie, func_return_type)

    def udf_func(attr, func):
        return F.udf(lambda value: func(value, attr), func_return_type)

    def expression_func(attr, func):
        def inner(col_name):
            return func(col_name, attr)

        return inner

    if func_type is "pandas_udf":
        return pandas_udf_func

    elif func_type is "udf":
        return udf_func

    elif func_type is "column_exp":
        return expression_func


def filter_row_by_data_type_audf(col_name, data_type):
    """
    Filter a column using a Spark data type as reference
    :param col_name:
    :param data_type:
    :return:
    """

    data_type = parse_python_dtypes(data_type)
    return abstract_udf(col_name, filter_row_by_data_type, "boolean", data_type)


def filter_row_by_data_type(col_name, data_type=None, get_type=False):
    """
    A Pandas UDF function that returns bool if the value match with the data_type param passed to the function.
    Also can return the data type
    :param col_name: Column to be process
    :param data_type: The data_type to be compared with
    :param get_type: Value to be returned as string or boolean
    :return: True or False
    """

    if data_type is not None:
        data_type = parse_python_dtypes(data_type)

    def pandas_udf_func(v):
        return v.apply(Infer.parse)

    if get_type is True:
        return_data_type = "string"
    else:
        return_data_type = "boolean"

    col_name = one_list_to_val(col_name)
    return F.pandas_udf(pandas_udf_func, return_data_type)(col_name, None, data_type)
