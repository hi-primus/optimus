# from dask_ml.preprocessing import DummyEncoder
from sklearn import preprocessing

from optimus.engines.base.ml.contants import STRING_TO_INDEX, INDEX_TO_STRING
from optimus.helpers.columns import parse_columns, name_col, get_output_cols, prepare_columns
from optimus.helpers.constants import Actions
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_, is_str


def n_gram(df, input_col, n=2):
    """
    Converts the input array of strings inside of a Spark DF into an array of n-grams.
    :param df: Pyspark dataframe to analyze
    :param input_col: Column to analyzer.
    :param n: number of elements per n-gram >=1.
    :return: Spark DataFrame with n-grams calculated.
    """

    pass


def string_to_index(df, input_cols, output_cols=None, columns=None, **kargs):
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

    # if columns is None:
    #     input_cols = parse_columns(df, input_cols)
    #     if output_cols is None:
    #         output_cols = [name_col(input_col, STRING_TO_INDEX) for input_col in input_cols]
    #     output_cols = get_output_cols(input_cols, STRING_TO_INDEX, merge = True)
    # else:
    #     input_cols, output_cols = zip(*columns)

    intput_cols, output_cols = prepare_columns(df, input_cols, output_cols)

    print(input_cols, output_cols)
    le = preprocessing.LabelEncoder()

    for input_col, output_col in zip(input_cols, output_cols):
        le.fit(df[input_col])
        df[output_col] = le.transform(df[input_col])

    df = df.preserve_meta(df_actual, Actions.STRING_TO_INDEX.value, output_cols)
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

    if columns is None:
        input_cols = parse_columns(df, input_cols)
        if output_cols is None:
            output_cols = [name_col(input_col, INDEX_TO_STRING) for input_col in input_cols]
        output_cols = get_output_cols(input_cols, output_cols)
    else:
        input_cols, output_cols = zip(*columns)



    df = df.preserve_meta(df_actual, Actions.INDEX_TO_STRING.value, output_cols)

    return df


def one_hot_encoder(df, input_cols, output_col=None, **kargs):
    """
    Maps a column of label indices to a column of binary vectors, with at most a single one-value.
    :param df: Dataframe to be transformed.
    :param input_cols: Columns to be encoded.
    :param output_col: Column where the output is going to be saved.
    :return: Dataframe with encoded columns.
    """

    input_cols = parse_columns(df, input_cols)

    if output_col is None:
        output_col = name_col(input_cols, "one_hot_encoder")

    # de = DummyEncoder()
    # df[output_col] = de.fit_transform(df[input_cols])

    return df


# TODO: Must we use the pipeline version?
def vector_assembler(df, input_cols, output_col=None):
    """
    Combines a given list of columns into a single vector column.
    :param df: Dataframe to be transformed.
    :param input_cols: Columns to be assembled.
    :param output_col: Column where the output is going to be saved.
    :return: Dataframe with assembled column.
    """

    input_cols = parse_columns(df, input_cols)

    if output_col is None:
        output_col = name_col(input_cols, "vector_assembler")


    # df = pipeline.fit(df).transform(df)

    return df


def normalizer(df, input_cols, output_col=None, p=2.0):
    """
    Transforms a dataset of Vector rows, normalizing each Vector to have unit norm. It takes parameter p, which
    specifies the p-norm used for normalization. (p=2) by default.
    :param df: Dataframe to be transformed
    :param input_cols: Columns to be normalized.
    :param output_col: Column where the output is going to be saved.
    :param p:  p-norm used for normalization.
    :return: Dataframe with normalized columns.
    """

    # Check if columns argument must be a string or list datat ype:
    if not is_(input_cols, (str, list)):
        RaiseIt.type_error(input_cols, ["str", "list"])

    if is_str(input_cols):
        input_cols = [input_cols]

    if is_(input_cols, (float, int)):
        RaiseIt.type_error(input_cols, ["float", "int"])

    # Try to create a vector
    if len(input_cols) > 1:
        df = df.cols.cast(input_cols, "vector")

    if output_col is None:
        output_col = name_col(input_cols, "normalizer")

    # TODO https://developer.ibm.com/code/2018/04/10/improve-performance-ml-pipelines-wide-dataframes-apache-spark-2-3/


    # df = pipeline.fit(df).transform(df)

    return df
