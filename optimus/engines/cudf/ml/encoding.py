from optimus.engines.base.ml.contants import STRING_TO_INDEX, INDEX_TO_STRING
from optimus.helpers.columns import prepare_columns
from optimus.helpers.constants import Actions
from cuml import preprocessing.LabelEncoder

def string_to_index(df, input_cols, output_cols=None, columns=None, **kwargs):
    """
    Maps a string column of labels to an ML column of label indices. If the input column is
    numeric, we cast it to string and index the string values.
    :param df: Dataframe to be transformed
    :param input_cols: Columns to be indexed.
    :param output_cols:Column where the ouput is going to be saved
    :param columns:
    :return: Dataframe with indexed columns.
    """
    df_actual = df

    columns = prepare_columns(df, input_cols, output_cols, default=STRING_TO_INDEX)

    le = LabelEncoder()
    for input_col, output_col in columns:
        df[output_col] = le.fit_transform(df[input_col])

    df = df.meta.preserve(df_actual, Actions.STRING_TO_INDEX.value, output_cols)
    return df


def index_to_string(df, input_cols, output_cols=None, columns=None, **kargs):
    """
    Maps a column of indices back to a new column of corresponding string values. The index-string mapping is
    either from the ML attributes of the input column, or from user-supplied labels (which take precedence over
    ML attributes).
    :param df: Dataframe to be transformed.
    :param input_cols: Columns to be indexed.
    :param output_cols: Column where the output is going to be saved.
    :param columns:
    :return: Dataframe with indexed columns.
    """
    df_actual = df

    columns = prepare_columns(df, input_cols, output_cols, default=INDEX_TO_STRING)
    le = preprocessing.LabelEncoder()
    for input_col, output_col in columns:
        df[output_col] = le.fit_inverse_transform(df[input_col])

    df = df.meta.preserve(df_actual, Actions.INDEX_TO_STRING.value, output_cols)

    return df