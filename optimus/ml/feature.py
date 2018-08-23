from pyspark.ml import feature, Pipeline
from pyspark.ml.feature import StringIndexer, IndexToString, OneHotEncoder, VectorAssembler, Normalizer
from pyspark.ml.linalg import DenseVector, VectorUDT
from pyspark.sql import functions as F

from optimus.helpers.checkit import is_dataframe, is_
from optimus.helpers.functions import parse_columns


def n_gram(df, input_col, n=2):
    """
    Converts the input array of strings inside of a Spark DF into an array of n-grams.
    :param df: Pyspark dataframe to analyze
    :param input_col: Column to analyzer.
    :param n: number of elements per n-gram >=1.
    :return: Spark DataFrame with n-grams calculated.
    """

    is_dataframe(df)

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
    :param df: Dataframe to be transformed
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
    :param df: Dataframe to be transformed
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
    :param df: Dataframe to be transformed
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
    :param df: Dataframe to be transformed
    :param input_cols: Columns to be assembled.
    :return: Dataframe with assembled column.
    """

    input_cols = parse_columns(df, input_cols)

    assembler = [VectorAssembler(inputCols=input_cols, outputCol="features")]

    pipeline = Pipeline(stages=assembler)
    df = pipeline.fit(df).transform(df)

    return df


def normalizer(df, input_cols, p=2.0):
    """
    Transforms a dataset of Vector rows, normalizing each Vector to have unit norm. It takes parameter p, which
    specifies the p-norm used for normalization. (p=2) by default.
    :param df: Dataframe to be transformed
    :param input_cols: Columns to be normalized.
    :param p:  p-norm used for normalization.
    :return: Dataframe with normalized columns.
    """

    # Check if columns argument must be a string or list datatype:

    assert isinstance(input_cols, (str, list)), \
        "Error: %s argument must be a string or a list." % "input_cols"

    if isinstance(input_cols, str):
        input_cols = [input_cols]

    assert isinstance(p, (float, int)), "Error: p argument must be a numeric value."

    # Convert ArrayType() column to DenseVector
    def arr_to_vec(arr_column):
        """
        :param arr_column: Column name
        :return: Returns DenseVector by converting an ArrayType() column
        """
        return DenseVector(arr_column)

    # User-Defined function
    # TODO: use apply() to use Pyarrow
    udf_arr_to_vec = F.udf(arr_to_vec, VectorUDT())

    # Check for columns which are not DenseVector types and convert them into DenseVector
    for col in input_cols:
        if not is_(df[col], DenseVector):
            df = df.withColumn(col, udf_arr_to_vec(df[col]))

    normal = [Normalizer(inputCol=column, outputCol=column + "_normalized", p=p) for column in
              list(set(input_cols))]

    pipeline = Pipeline(stages=normal)

    df = pipeline.fit(df).transform(df)

    return df
