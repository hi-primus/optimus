
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


def string_to_index(self, input_cols):
    """
    Maps a string column of labels to an ML column of label indices. If the input column is
    numeric, we cast it to string and index the string values.
    :param input_cols: Columns to be indexed.
    :return: Dataframe with indexed columns.
    """

    # Check if columns argument must be a string or list datatype:
    self._assert_type_str_or_list(input_cols, "input_cols")

    if isinstance(input_cols, str):
        input_cols = [input_cols]

    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StringIndexer

    indexers = [StringIndexer(inputCol=column, outputCol=column + "_index").fit(self._df) for column in
                list(set(input_cols))]

    pipeline = Pipeline(stages=indexers)
    self._df = pipeline.fit(self._df).transform(self._df)

    return self


def index_to_string(self, input_cols):
    """
    Maps a column of indices back to a new column of corresponding string values. The index-string mapping is
    either from the ML attributes of the input column, or from user-supplied labels (which take precedence over
    ML attributes).
    :param input_cols: Columns to be indexed.
    :return: Dataframe with indexed columns.
    """

    # Check if columns argument must be a string or list datatype:
    self._assert_type_str_or_list(input_cols, "input_cols")

    if isinstance(input_cols, str):
        input_cols = [input_cols]

    from pyspark.ml import Pipeline
    from pyspark.ml.feature import IndexToString

    indexers = [IndexToString(inputCol=column, outputCol=column + "_string") for column in
                list(set(input_cols))]

    pipeline = Pipeline(stages=indexers)
    self._df = pipeline.fit(self._df).transform(self._df)

    return self


def one_hot_encoder(self, input_cols):
    """
    Maps a column of label indices to a column of binary vectors, with at most a single one-value.
    :param input_cols: Columns to be encoded.
    :return: Dataframe with encoded columns.
    """

    # Check if columns argument must be a string or list datatype:
    self._assert_type_str_or_list(input_cols, "input_cols")

    if isinstance(input_cols, str):
        input_cols = [input_cols]

    from pyspark.ml import Pipeline
    from pyspark.ml.feature import OneHotEncoder

    encode = [OneHotEncoder(inputCol=column, outputCol=column + "_encoded") for column in
              list(set(input_cols))]

    pipeline = Pipeline(stages=encode)
    self._df = pipeline.fit(self._df).transform(self._df)

    return self


# TODO: Must we use the pipeline version?
def vector_assembler(self, input_cols):
    """
    Combines a given list of columns into a single vector column.
    :param input_cols: Columns to be assembled.
    :return: Dataframe with assembled column.
    """

    # Check if columns argument must be a string or list datatype:
    self._assert_type_str_or_list(input_cols, "input_cols")

    if isinstance(input_cols, str):
        input_cols = [input_cols]

    from pyspark.ml import Pipeline

    assembler = [VectorAssembler(inputCols=input_cols, outputCol="features")]

    pipeline = Pipeline(stages=assembler)
    self._df = pipeline.fit(self._df).transform(self._df)

    return self