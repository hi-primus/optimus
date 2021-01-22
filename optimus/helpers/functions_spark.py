from functools import reduce

from optimus.helpers.core import val_to_list
from optimus.helpers.functions import random_int
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_

def traverse(obj, path=None, callback=None):
    """
    Traverse a deep nested python structure
    :param obj: object to traverse
    :param path:
    :param callback: Function used to transform a value
    :return:
    """
    from pyspark.ml.linalg import DenseVector

    if path is None:
        path = []

    if is_(obj, dict):
        value = {k: traverse(v, path + [k], callback)
                 for k, v in obj.items()}

    elif is_(obj, list):
        value = [traverse(elem, path + [[]], callback)
                 for elem in obj]

    elif is_(obj, tuple):
        value = tuple(traverse(elem, path + [[]], callback)
                      for elem in obj)
    elif is_(obj, DenseVector):
        value = DenseVector([traverse(elem, path + [[]], callback) for elem in obj])
    else:
        value = obj

    if callback is None:  # if a callback is provided, call it to get the new value
        return value
    else:
        return callback(path, value)


def append(dfs, like="columns"):
    """
    Concat multiple dataFrames columns or rows wise
    :param dfs: List of DataFrames
    :param like: concat as columns or rows
    :return:
    """

    # FIX: Because monotonically_increasing_id can create different
    # sequence for different dataframes the result could be wrong.

    if like == "columns":
        temp_dfs = []
        col_temp_name = "id_" + random_int()

        dfs = val_to_list(dfs)
        for df in dfs:
            from pyspark.sql import functions as F
            temp_dfs.append(df.withColumn(col_temp_name, F.monotonically_increasing_id()))

        def _append(df1, df2):
            return df1.join(df2, col_temp_name, "outer")

        df_result = reduce(_append, temp_dfs).drop(col_temp_name)

    elif like == "rows":
        from pyspark.sql import DataFrame
        df_result = reduce(DataFrame.union, dfs)
    else:
        RaiseIt.value_error(like, ["columns", "rows"])

    return df_result