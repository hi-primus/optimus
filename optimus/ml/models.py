
def logistic_regression_text(df, input_col):
    """
    Runs a logistic regression for input (text) DataFrame.
    :param df: Pyspark dataframe to analyze
    :param input_col: Column to predict
    :return: DataFrame with logistic regression and prediction run.
    """

    assert_spark_df(df)

    pl = feature.Tokenizer().setInputCol(input_col) | feature.CountVectorizer()
    ml = pl | classification.LogisticRegression()
    ml_model = ml.fit(df)
    df_model = ml_model.transform(df)
    return df_model, ml_model


def random_forest(df, columns, input_col):
    """
    Runs a random forest classifier for input DataFrame.
    :param df: Pyspark dataframe to analyze.
    :param columns: List of columns to select for prediction.
    :param input_col: Column to predict.
    :return: DataFrame with random forest and prediction run.
    """

    assert_spark_df(df)

    assert isinstance(columns, list), "Error, columns must be a list"

    assert isinstance(input_col, str), "Error, input column must be a string"

    data = df.select(columns)
    feats = data.columns
    feats.remove(input_col)
    transformer = op.DataFrameTransformer(data)
    transformer.string_to_index(input_cols=input_col)
    transformer.vector_assembler(input_cols=feats)
    model = RandomForestClassifier()
    transformer.rename_col(columns=[(input_col + "_index", "label")])
    rf_model = model.fit(transformer.df)
    df_model = rf_model.transform(transformer.df)
    return df_model, rf_model


def decision_tree(df, columns, input_col):
    """
    Runs a decision tree classifier for input DataFrame.
    :param df: Pyspark dataframe to analyze.
    :param columns: List of columns to select for prediction.
    :param input_col: Column to predict.
    :return: DataFrame with decision tree and prediction run.
    """

    assert_spark_df(df)

    assert isinstance(columns, list), "Error, columns must be a list"

    assert isinstance(input_col, str), "Error, input column must be a string"

    data = df.select(columns)
    feats = data.columns
    feats.remove(input_col)
    transformer = op.DataFrameTransformer(data)
    transformer.string_to_index(input_cols=input_col)
    transformer.vector_assembler(input_cols=feats)
    model = DecisionTreeClassifier()
    transformer.rename_col(columns=[(input_col + "_index", "label")])
    dt_model = model.fit(transformer.df)
    df_model = dt_model.transform(transformer.df)
    return df_model, dt_model


def gbt(df, columns, input_col):
    """
    Runs a gradient boosting tree classifier for input DataFrame.
    :param df: Pyspark dataframe to analyze.
    :param columns: List of columns to select for prediction.
    :param input_col: Column to predict.
    :return: DataFrame with gradient boosting tree and prediction run.
    """

    assert_spark_df(df)

    assert isinstance(columns, list), "Error, columns must be a list"

    assert isinstance(input_col, str), "Error, input column must be a string"

    data = df.select(columns)
    feats = data.columns
    feats.remove(input_col)
    transformer = op.DataFrameTransformer(data)
    transformer.string_to_index(input_cols=input_col)
    transformer.vector_assembler(input_cols=feats)
    model = GBTClassifier()
    transformer.rename_col(columns=[(input_col + "_index", "label")])
    gbt_model = model.fit(transformer.df)
    df_model = gbt_model.transform(transformer.df)
    return df_model, gbt_model
