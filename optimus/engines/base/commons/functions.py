import fastnumbers
import numpy as np
import pandas as pd
from dask_ml.impute import SimpleImputer

# From a top point of view we organize Optimus separating the functions in dataframes and dask engines.
# Some functions are commons for pandas and dask.
from optimus.engines.base.ml.contants import STRING_TO_INDEX, INDEX_TO_STRING
from optimus.helpers.constants import Actions
from optimus.helpers.raiseit import RaiseIt


def to_integer(value, *args):
    try:
        # fastnumbers can only handle string or numerics values. Not None, dates or list
        return fastnumbers.fast_int(value, default=0)
    except TypeError:
        return np.nan


def to_float(value, *args):
    # if value is None or isinstance(value, str):
    #     return None
    # else:
    try:
        # fastnumbers can only handle string or numeric values. Not None, dates or list
        return fastnumbers.fast_float(value, default=np.nan)
    except TypeError:
        return np.nan

    # return fastnumbers.fast_float(value, default=np.nan)


def to_datetime(value, format):
    return pd.to_datetime(value, format=format, errors="coerce")


def to_integer_cudf(value, *args):
    import cudf
    series_string = value.astype(str)
    # See https://github.com/rapidsai/cudf/issues/5345
    series = cudf.Series(series_string.str.stoi()).fillna(False)
    series[
        ~cudf.Series(cudf.core.column.string.cpp_is_integer(series_string._column)).fillna(False)] = None
    return series


def to_float_cudf(value, *args):
    import cudf
    series_string = value.astype(str)
    # See https://github.com/rapidsai/cudf/issues/5345
    series = cudf.Series(series_string.str.stof()).fillna(False)
    series[
        ~cudf.Series(cudf.core.column.string.cpp_is_float(series_string._column)).fillna(False)] = None
    return series


def to_datetime_cudf(value, format):
    import cudf
    return cudf.to_datetime(value, format=format, errors="coerce")


def impute(df, input_cols, data_type="continuous", strategy="mean", output_cols=None):
    """

    :param df:
    :param input_cols:
    :param data_type:
    :param strategy:
    # - If "mean", then replace missing values using the mean along
    #   each column. Can only be used with numeric data.
    # - If "median", then replace missing values using the median along
    #   each column. Can only be used with numeric data.
    # - If "most_frequent", then replace missing using the most frequent
    #   value along each column. Can be used with strings or numeric data.
    # - If "constant", then replace missing values with fill_value. Can be
    #   used with strings or numeric data.
    :param output_cols:
    :return:
    """
    imputer = SimpleImputer(strategy=strategy, copy=False)

    def _imputer(value):
        return imputer.fit_transform(value.to_frame())[value.name]

    if data_type == "continuous":
        return df.cols.apply(input_cols, _imputer, output_cols=output_cols, meta_action=Actions.IMPUTE.value,
                             mode="vectorized")
    elif data_type == "categorical":
        # return df.cols.mode()
        raise
    else:
        RaiseIt.value_error(data_type, ["continuous", "categorical"])


def string_to_index(df, input_cols, output_cols=None, le=None, **kwargs):
    """_
    Maps a string column of labels to an ML column of label indices. If the input column is
    numeric, we cast it to string and index the string values.
    :param df: Dataframe to be transformed
    :param input_cols: Columns to be indexed.
    :param output_cols:Column where the output is going to be saved
    :param le: Label encoder library to make the process
    :return: Dataframe with indexed columns.
    """

    def _string_to_index(value):
        # Label encoder can not handle np.nan
        # value[value.isnull()] = 'NaN'
        return le.fit_transform(value.astype(str))

    return df.cols.apply(input_cols, _string_to_index, output_cols=output_cols,
                         meta_action=Actions.STRING_TO_INDEX.value,
                         mode="vectorized", default=STRING_TO_INDEX)


def index_to_string(df, input_cols, output_cols=None, le=None, **kwargs):
    """
    Maps a column of indices back to a new column of corresponding string values. The index-string mapping is
    either from the ML attributes of the input column, or from user-supplied labels (which take precedence over
    ML attributes).
    :param df: Dataframe to be transformed.
    :param input_cols: Columns to be indexed.
    :param output_cols: Column where the output is going to be saved.
    :param le:
    :return: Dataframe with indexed columns.
    """

    def _index_to_string(value):
        return le.inverse_transform(value)

    return df.cols.apply(input_cols, _index_to_string, output_cols=output_cols,
                         meta_action=Actions.INDEX_TO_STRING.value,
                         mode="vectorized", default=INDEX_TO_STRING)

# def _cast_date(value, format="YYYY-MM-DD"):
#     if pd.isnull(value):
#         return np.nan
#     else:
#         try:
#             # return pendulum.parse(value)
#             # return pendulum.from_format(value, format)
#             # return dparse(value)
#
#             return value
#         except:
#             return value

# def _cast_bool(value):
#     if pd.isnull(value):
#         return np.nan
#     else:
#         return bool(value)
#

# cast_func = {'int': _cast_int, 'decimal': _cast_float, "string": _cast_str, 'bool': _cast_bool,
#              'date': _cast_date, "object": _cast_object, "missing": _cast_str}
