# from dask_ml.preprocessing import DummyEncoder

from optimus.helpers.check import is_spark_dataframe
from optimus.helpers.columns import parse_columns, name_col
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_, is_str


def n_gram(df, input_col, n=2):
    """
    Converts the input array of strings inside of a dataframe into an array of n-grams.
    :param df: Dataframe to analyze
    :param input_col: Column to analyzer.
    :param n: number of elements per n-gram >=1.
    :return: Spark DataFrame with n-grams calculated.
    """

    is_spark_dataframe(df)

    tokenizer = feature.Tokenizer().setInputCol(input_col) | feature.StopWordsRemover()
    count = feature.CountVectorizer()
    gram = feature.NGram(n=n) | feature.CountVectorizer()
    tf = tokenizer | (count, gram) | feature.VectorAssembler()
    tfidf = tf | feature.IDF().setOutputCol('features')

    tfidf_model = tfidf.fit(df)
    df_model = tfidf_model.transform(df)
    return df_model, tfidf_model


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

    de = DummyEncoder()
    df[output_col] = de.fit_transform(df[input_cols])

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

    assembler = [VectorAssembler(inputCols=input_cols, outputCol=output_col)]

    pipeline = Pipeline(stages=assembler)
    df = pipeline.fit(df).transform(df)

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
    normal = [Normalizer(inputCol=col_name, outputCol=output_col, p=p) for col_name in
              list(set(input_cols))]

    pipeline = Pipeline(stages=normal)

    df = pipeline.fit(df).transform(df)

    return df
