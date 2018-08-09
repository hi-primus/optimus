from functools import reduce
import dateutil.parser

from optimus.helpers.functions import is_pyarrow_installed, parse_python_dtypes, random_name, one_list_to_val
from optimus.helpers.raiseit import RaiseIfNot
from optimus.helpers.checkit import is_data_type

from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def abstract_udf(col, func, func_return_type=None, attrs=None, func_type=None, verbose=True):
    """
    Abstract User defined functions. This is a helper function to create udf, pandas udf or a Column Exp
    :param col: Column to created or transformed
    :param func: Function to be applied to the data
    :param attrs: If required attributes to be passed to the function
    :param func_return_type: Required bu UDF and Pandas UDF. It is necessary to define the return type
    :param func_type: pandas_udf or udf. The function is going to try to use pandas_udf if func_type is not defined
    :return: A function, UDF or Pandas UDF
    """

    # By default is going to try to use pandas UDF
    if func_type is None and is_pyarrow_installed() is True:
        func_type = "pandas_udf"

    types = ["column_exp", "udf", "pandas_udf"]
    if func_type not in types:
        RaiseIfNot.value_error(func_type, types)

    if verbose is True:
        print("Using '{func_type}' to process column '{column}' with function {func_name}"
              .format(func_type=func_type, column=col, func_name=func.__name__))

    df_func = func_factory(func_type, func_return_type)
    return df_func(attrs, func)(col)


def func_factory(func_type=None, func_return_type=None):
    """
    Return column express, udf or pandas udf function.
    :param func_type: Type of function udf or pandas udf
    :param func_return_type:
    :return:
    """

    # if func_return_type is not None:
    # func_return_type = parse_spark_dtypes(func_return_type)

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
    return abstract_udf(col_name, is_data_type, "boolean", data_type)


def concat(dfs, like="columns"):
    """
    Concat multiple dataframes as columns or rows way
    :param dfs:
    :param like: The way dataframes is going to be concat. like columns or rows
    :return:
    """
    # Add increasing Ids, and they should be the same.
    if like == "columns":
        temp_dfs = []
        col_temp_name = "id_" + random_name()
        for df in dfs:
            temp_dfs.append(df.withColumn(col_temp_name, F.monotonically_increasing_id()))

        def _append_df(df1, df2):
            return df1.join(df2, col_temp_name, "outer")

        df_result = reduce(_append_df, temp_dfs).drop(col_temp_name)

    elif like == "rows":
        df_result = reduce(DataFrame.union, dfs)
    else:
        RaiseIfNot.value_error(like, ["columns", "rows"])

    return df_result


def filter_row_by_data_type(col_name, data_type=None, get_type=False):
    """
    A Pandas UDF function that returns bool if the value match with the data_type param passed to the function.
    Also can return the data type
    :param col_name: Column to be process
    :param data_type: The data_type to be compared
    :param get_type:
    :return: True or False
    """
    if data_type is not None:
        data_type = parse_python_dtypes(data_type)

    def pandas_udf_func(v):

        def str_to_int(value):
            """
            Check if a str can be converted to int
            :param value:
            :return:
            """
            try:
                int(value)
                return True

            except ValueError:
                return False

        def str_to_float(value):
            """
            Check if a str can be converted to float
            :param value:
            :return:
            """
            try:
                float(value)
                return True

            except ValueError:
                pass

        def str_to_boolean(value):
            """
            Check if a str can be converted to boolean
            :param value:
            :return:
            """
            value = value.lower()
            if value == "true" or value == "false":
                return True

        def str_to_date(value):
            try:
                dateutil.parser.parse(value)
                return True
            except ValueError:
                pass

        def func(value):
            """
            Check if a value can be casted to a specific
            :param value: value to be checked

            :return:
            """
            if isinstance(value, bool):
                _data_type = "bool"
            # _data_type = data_type
            elif isinstance(value, int):  # Check if value is integer
                _data_type = "int"
            elif isinstance(value, float):
                _data_type = "float"
            # if string we try to parse it to int, float or bool
            elif isinstance(value, str):
                if str_to_int(value):
                    _data_type = "int"
                elif str_to_float(value):
                    _data_type = "float"
                elif str_to_boolean(value):
                    _data_type = "bool"
                elif str_to_date(value):
                    _data_type = "date"
                else:
                    _data_type = "string"
            else:
                _data_type = "null"

            if get_type is False:
                if _data_type == data_type:
                    return True
                else:
                    return False
            else:
                return _data_type

        return v.apply(func)

    if get_type is True:
        a = "string"
    else:
        a = "boolean"

    col_name = one_list_to_val(col_name)
    return F.pandas_udf(pandas_udf_func, a)(col_name)
