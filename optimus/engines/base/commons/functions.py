import re

import fastnumbers
import numpy as np
import pandas as pd
from dask_ml.impute import SimpleImputer
from fastnumbers import isintlike, isfloat, isreal, fast_float, fast_int

# From a top point of view we organize Optimus separating the functions in dataframes and dask engines.
# Some functions are commons to pandas and dask.
from optimus.engines.base.ml.contants import STRING_TO_INDEX, INDEX_TO_STRING
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.helpers.core import val_to_list
from optimus.infer import is_str


def is_string(series):
    def _is_string(value):
        if isinstance(value, str):
            return True
        else:
            return False

    return pd.Series(np.vectorize(_is_string)(series.values).flatten())


def is_integer(series):
    return pd.Series(np.vectorize(isintlike)(series).flatten())


def is_float(series):
    return pd.Series(np.vectorize(isfloat)(series).flatten())


def is_numeric(series):
    return pd.Series(np.vectorize(isreal)(series).flatten())


def word_tokenize(series):
    import nltk
    return nltk.word_tokenize(series)


def is_integer_cudf(series):
    return series.str.isinteger()


def is_float_cudf(series):
    return series.str.isfloat()


def is_numeric_cudf(series):
    return series.str.isnumeric()


def is_string_cudf(series):
    return series.str.isalpha()


# Convertion functions
# cuDF
def to_integer_cudf(series):
    import cudf
    return cudf.to_numeric(series, errors="ignore", downcast="integer")


def to_float_cudf(series):
    import cudf
    return cudf.to_numeric(series, errors="ignore", downcast="float")


def to_string_cudf(series):
    return series.astype(str)


# Vaex

def to_integer_vaex(series):
    return fast_int(series, default=np.nan)


def to_float_vaex(series):
    return fast_float(series, default=np.nan)


def to_string_vaex(series):
    return series.astype(str)


# Pandas Dask
def to_integer(value, *args):
    try:
        # fastnumbers can only handle string or numeric values. Not None, dates or list
        return fastnumbers.fast_forceint(value, default=0)
    except TypeError:
        return np.nan


def to_float(series, *args):
    # if value is None or isinstance(value, str):
    #     return None
    # else:
    return pd.Series(np.vectorize(fast_float)(series, default=np.nan).flatten())

    # try:
    #     # fastnumbers can only handle string or numeric values. Not None, dates or list
    #     return fastnumbers.fast_float(value, default=np.nan)
    # except TypeError:
    #     return np.nan


def to_string(value, *args):
    try:
        return value.astype(str)
    except TypeError:
        return np.nan


def to_boolean(value, *args):
    try:
        # fastnumbers can only handle string or numeric values. Not None, dates or list
        return bool(value)
    except TypeError:
        return np.nan


def to_datetime(value, format):
    return pd.to_datetime(value, format=format, errors="coerce")


def hist(series, bins):
    return np.histogram(series.to_float(), bins=bins)


def to_datetime_cudf(value, format):
    import cudf
    return cudf.to_datetime(value, format=format, errors="coerce")


def impute(df, input_cols, data_type="continuous", strategy="mean", fill_value=None, output_cols=None):
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

    imputer = SimpleImputer(strategy=strategy, fill_value=fill_value)

    def _imputer(value):
        return imputer.fit_transform(value.values.reshape(-1, 1))

    if strategy != "most_frequent":
        df = df.cols.to_float(input_cols)

    return df.cols.apply(input_cols, _imputer, output_cols=output_cols, meta_action=Actions.IMPUTE.value,
                         mode="vectorized")


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
    :param df: Dataframe to be transformed.
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

    dfd = df.data

    for col_name in columns:
        # Categorical columns can not handle a list inside a list as return for example [[1,2],[6,7]].
        # That could happened if we try to split a categorical column
        # dfd[col_name] = dfd[col_name].astype("object")
        dfd[col_name + "__match_positions__"] = dfd[col_name].astype("object").apply(get_match_positions,
                                                                                     args=(sub,))

    return df.new(dfd)
