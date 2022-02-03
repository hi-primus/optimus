import re

import numpy as np

# From a top point of view we organize Optimus separating the functions in dataframes and dask engines.
# Some functions are commons to pandas and dask.
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.helpers.core import val_to_list
from optimus.infer import is_str


def word_tokenize(series):
    import nltk
    return nltk.word_tokenize(series)


def hist(series, bins, range):
    return np.histogram(series.to_float(), bins=bins, range=range)


def string_to_index(df, cols, output_cols=None, le=None, **kwargs):
    """_
    Maps a string column of labels to an ML column of label indices. If the input column is
    numeric, we cast it to string and index the string values.
    :param df: Dataframe to be transformed
    :param cols: Columns to be indexed.
    :param output_cols:Column where the output is going to be saved
    :param le: Label encoder library to make the process
    :return: Dataframe with indexed columns.
    """

    def _string_to_index(value):
        # Label encoder can not handle np.nan
        # value[value.isnull()] = 'NaN'
        return le.fit_transform(value.astype(str))

    return df.cols.apply(cols, _string_to_index, output_cols=output_cols,
                         meta_action=Actions.STRING_TO_INDEX.value,
                         mode="vectorized")


def index_to_string(df, cols, output_cols=None, le=None, **kwargs):
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
        return le.inverse_transform(value.astype("int"))

    return df.cols.apply(cols, _index_to_string, output_cols=output_cols,
                         meta_action=Actions.INDEX_TO_STRING.value,
                         mode="vectorized")


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
            regex_str = '|'.join([re.escape(s) for s in _separator])
            if ignore_case:
                regex_str = f"(?i)({regex_str})"
            regex = re.compile(regex_str)

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
