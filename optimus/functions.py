from functools import reduce

from optimus.helpers.functions import is_pyarrow_installed, parse_spark_dtypes, parse_python_dtypes, random_name
from optimus.helpers import raiseit as RaiseIfNot

from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def abstract_udf(col, func, func_return_type=None, attrs=None, func_type=None):
    """
    General User defined functions. This is a helper function to create udf, pandas udf or a Column Exp
    :param col:
    :param func:
    :param attrs:
    :param func_return_type:
    :param func_type: pandas_udf or udf. The function is going to try to use pandas_udf if func_type is not defined
    :return:
    """

    # attrs = val_to_list(attrs)

    if func_type != "column_exp":
        if func_type is None and is_pyarrow_installed():
            func_type = "pandas_udf"
        else:
            func_type = "udf"

    df_func = func_factory(func_type, func_return_type)
    return df_func(attrs, func)(col)


def func_factory(func_type=None, func_return_type=None):
    """
    Return column express, udf or pandas udf function.
    :param func_type:
    :param func_return_type:
    :return:
    """
    if func_return_type is not None:
        func_return_type = parse_spark_dtypes(func_return_type)

    def pandas_udf_func(attr=None, func=None):
        # TODO: Get the column type, so is not necessary to pass the return type a param

        # Apply the function over the whole series
        def apply_to_series(val, attr):
            if attr is None:
                attr = (None,)
            else:
                attr = (attr,)

            return val.apply(func, args=attr)

        return F.pandas_udf(lambda value: apply_to_series(value, attr), func_return_type)

    def udf_func(attr, func):
        return F.udf(lambda value: func(value, attr))

    def expression_func(attr, func):
        # TODO: Check if we can the returned value for a col expression
        def inner(col_name):
            return func(col_name, attr)

        return inner

    if func_type is "pandas_udf":
        return pandas_udf_func

    elif func_type is "udf":
        return udf_func

    elif func_type is "column_exp":
        return expression_func


def filter_row_by_data_type(col_name, data_type):
    """
    Filter a column using a Spark data type as reference
    :param col_name:
    :param data_type:
    :return:
    """

    data_type = parse_python_dtypes(data_type)
    return abstract_udf(col_name, is_data_type, "bool", data_type)


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
