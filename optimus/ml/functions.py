from pyspark.ml import feature, Pipeline
from pyspark.ml.feature import StringIndexer, IndexToString, OneHotEncoder, VectorAssembler

from optimus.helpers.functions import is_dataframe, parse_columns


def n_gram(df, input_col, n=2):
    """
    Converts the input array of strings inside of a Spark DF into an array of n-grams.
    :param df: Pyspark dataframe to analyze
    :param input_col: Column to analyzer.
    :param n: number of elements per n-gram >=1.
    :return: Spark DataFrame with n-grams calculated.
    """

    assert_spark_df(df)

    tokenizer = feature.Tokenizer().setInputCol(input_col) | feature.StopWordsRemover()
    count = feature.CountVectorizer()
    gram = feature.NGram(n=n) | feature.CountVectorizer()
    tf = tokenizer | (count, gram) | feature.VectorAssembler()
    tfidf = tf | feature.IDF().setOutputCol('features')

    tfidf_model = tfidf.fit(df)
    df_model = tfidf_model.transform(df)
    return df_model, tfidf_model


def string_to_index(df, input_cols):
    """
    Maps a string column of labels to an ML column of label indices. If the input column is
    numeric, we cast it to string and index the string values.
    :param df:
    :param input_cols: Columns to be indexed.
    :return: Dataframe with indexed columns.
    """

    input_cols = parse_columns(df, input_cols)

    indexers = [StringIndexer(inputCol=column, outputCol=column + "_index").fit(df) for column in
                list(set(input_cols))]

    pipeline = Pipeline(stages=indexers)
    df = pipeline.fit(df).transform(df)

    return df


def index_to_string(df, input_cols):
    """
    Maps a column of indices back to a new column of corresponding string values. The index-string mapping is
    either from the ML attributes of the input column, or from user-supplied labels (which take precedence over
    ML attributes).
    :param df:
    :param input_cols: Columns to be indexed.
    :return: Dataframe with indexed columns.
    """

    input_cols = parse_columns(df, input_cols)

    indexers = [IndexToString(inputCol=column, outputCol=column + "_string") for column in
                list(set(input_cols))]

    pipeline = Pipeline(stages=indexers)
    df = pipeline.fit(df).transform(df)

    return df


def one_hot_encoder(df, input_cols):
    """
    Maps a column of label indices to a column of binary vectors, with at most a single one-value.
    :param df:
    :param input_cols: Columns to be encoded.
    :return: Dataframe with encoded columns.
    """

    input_cols = parse_columns(df, input_cols)

    encode = [OneHotEncoder(inputCol=column, outputCol=column + "_encoded") for column in
              list(set(input_cols))]

    pipeline = Pipeline(stages=encode)
    df = pipeline.fit(df).transform(df)

    return df


# TODO: Must we use the pipeline version?
def vector_assembler(df, input_cols):
    """
    Combines a given list of columns into a single vector column.
    :param df:
    :param input_cols: Columns to be assembled.
    :return: Dataframe with assembled column.
    """

    input_cols = parse_columns(df, input_cols)

    assembler = [VectorAssembler(inputCols=input_cols, outputCol="features")]

    pipeline = Pipeline(stages=assembler)
    df = pipeline.fit(df).transform(df)

    return df
