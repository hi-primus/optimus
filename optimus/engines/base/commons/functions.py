import re

import fastnumbers
import numpy as np
import pandas as pd
from dask_ml.impute import SimpleImputer

# From a top point of view we organize Optimus separating the functions in dataframes and dask engines.
# Some functions are commons to pandas and dask.
from optimus.engines.base.ml.contants import STRING_TO_INDEX, INDEX_TO_STRING
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.helpers.core import val_to_list
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_str


def to_float_cudf(series):
    import cudf
    series_string = series.astype(str)
    # See https://github.com/rapidsai/cudf/issues/5345
    # series = cudf.Series(series_string.str.stof()).fillna(False)
    series = cudf.Series(cudf.core.column.string.str_cast.stof(series_string._column))
    series[
        ~cudf.Series(cudf.core.column.string.cpp_is_float(series_string._column)).fillna(False)] = None

    # TODO: after using to_float_cudf() the function .round() is not working(for some unclear reason).
    #  I found to fixes apply astype(float) to the return or use str_cast.stod() instead of stof()

    return series.astype(float)


def to_string_cudf(series):
    return series.astype(str)


def to_integer_cudf(series):
    import cudf
    series_string = series.astype(str)
    # See https://github.com/rapidsai/cudf/issues/5345
    # series = cudf.Series(series_string.str.stoi()).fillna(False)
    series = cudf.Series(cudf.core.column.string.str_cast.stoi(series_string._column))
    series[
        ~cudf.Series(cudf.core.column.string.cpp_is_integer(series_string._column)).fillna(False)] = None
    return series

def to_string(value, *args):
    try:
        return value.astype(str)
    except TypeError:
        return np.nan


def to_integer(value, *args):
    try:
        # fastnumbers can only handle string or numeric values. Not None, dates or list
        return fastnumbers.fast_forceint(value, default=0)
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


def to_datetime(value, format):
    return pd.to_datetime(value, format=format, errors="coerce")


def hist(series, bins):
    return np.histogram(series.ext.to_float(), bins=bins)

def to_datetime_cudf(value, format):
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


def find(df, columns, sub, ignore_case=False):
    """
    Find the start and end position for a char or substring
    :param columns:
    :param ignore_case:
    :param sub:
    :return:
    """

    columns = parse_columns(df, columns)
    sub = val_to_list(sub)

    def get_match_positions(_value, _separator):
        result = None
        if is_str(_value):
            # Using re.IGNORECASE in finditer not seems to work
            if ignore_case is True:
                _separator = _separator + [s.lower() for s in _separator]
            regex = re.compile('|'.join(_separator))

            length = [[match.start(), match.end()] for match in
                      regex.finditer(_value)]
            result = length if len(length) > 0 else None
        return result

    for col_name in columns:
        # Categorical columns can not handle a list inside a list as return for example [[1,2],[6,7]].
        # That could happened if we try to split a categorical column
        # df[col_name] = df[col_name].astype("object")
        df[col_name + "__match_positions__"] = df[col_name].astype("object").apply(get_match_positions,
                                                                                   args=(sub,))

    return df
