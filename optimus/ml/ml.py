"""
Code based on pyspark_pipes by Daniel Acuña
"""

import pyspark

from pyspark.ml.param.shared import HasFeaturesCol, HasInputCol, \
    HasInputCols, HasLabelCol, HasPredictionCol, HasOutputCol, Params, \
    HasRawPredictionCol, HasProbabilityCol

from pyspark.ml import feature, classification
from pyspark.ml.classification import RandomForestClassifier, DecisionTreeClassifier, GBTClassifier
from pyspark.ml import Pipeline

import optimus as op

ALLOWED_TYPES = (HasFeaturesCol, HasInputCol, HasInputCols, HasLabelCol,
                 HasPredictionCol, HasOutputCol)


def assert_spark_df(df):
    assert (isinstance(df, pyspark.sql.dataframe.DataFrame))


def set_default_colnames(pipe_stage):
    """
    Add object ids to default columns
    :param pipe_stage: Params object
    :return: modified stage
    """
    # pylint: disable=protected-access
    if isinstance(pipe_stage, HasFeaturesCol) and not pipe_stage.isSet('featuresCol'):
        pipe_stage._setDefault(featuresCol=pipe_stage.uid + '__features')
    if isinstance(pipe_stage, HasRawPredictionCol) and not pipe_stage.isSet('rawPredictionCol'):
        pipe_stage._setDefault(rawPredictionCol=pipe_stage.uid + '__rawPrediction')
    if isinstance(pipe_stage, HasProbabilityCol) and not pipe_stage.isSet('probabilityCol'):
        pipe_stage._setDefault(probabilityCol=pipe_stage.uid + '__probability')
    if isinstance(pipe_stage, HasPredictionCol) and not pipe_stage.isSet('predictionCol'):
        pipe_stage._setDefault(predictionCol=pipe_stage.uid + '__prediction')
    return pipe_stage


def is_instance(obj, typelist):
    """
    Returns true if object `obj` is of any of the types list in `typelist`
    :param obj: object
    :param typelist: list of types
    :return: bool
    """
    return any([isinstance(obj, t) for t in typelist])


def get_pipeline_laststep(pipe):
    """
    Get last stage of pipe that is not a Pipeline
    :param pipe: Params
    :return: Params
    """
    if isinstance(pipe, Pipeline):
        return get_pipeline_laststep(pipe.getStages()[-1])
    return pipe


def get_pipeline_firststep(pipe):
    """
    Get last stage of a pipe that is not a Pipeline
    :param pipe: Params
    :return: Estimator or Transformer
    """
    if isinstance(pipe, Pipeline):
        return get_pipeline_firststep(pipe.getStages()[0])

    return pipe


def left_pipe_function(self, other):
    """
    Infix pipe operator
    :param self: right-hand side object
    :param other: left-hand side object
    :return: Pipeline
    """
    # pylint: disable=too-many-branches

    self = set_default_colnames(self)
    # At the start of the Pipeline we need to put together two things
    if is_instance(other, ALLOWED_TYPES):
        result = Pipeline().setStages([other]) | self
    elif isinstance(other, Pipeline):
        last_step = set_default_colnames(get_pipeline_laststep(other))
        if isinstance(last_step, HasOutputCol):
            # let's generate some random string to represent the column's name
            last_step_output = last_step.getOutputCol()
        elif isinstance(last_step, HasPredictionCol):
            last_step_output = last_step.getPredictionCol()
        else:
            raise Exception("Type of step not supported")

        # should we connect input with output?
        first_step = set_default_colnames(get_pipeline_firststep(self))
        if isinstance(first_step, HasInputCol):
            if not first_step.isSet('inputCol'):
                first_step.setInputCol(last_step_output)
        if isinstance(first_step, HasFeaturesCol):
            if not first_step.isSet('featuresCol'):
                first_step.setFeaturesCol(last_step_output)

        result = Pipeline().setStages(other.getStages() + [self])
    elif isinstance(other, (tuple, list)):
        # check that connecting to one estimator or transformer
        if not isinstance(self, HasInputCols):
            raise Exception("When many to one connection, then receiver "
                            "must accept multiple inputs")

        all_outputs = []
        all_objects = []
        for stage in other:
            if not isinstance(stage, NotBroadcasted):
                last_step = set_default_colnames(get_pipeline_laststep(stage))

                if isinstance(last_step, HasOutputCol):
                    # let's generate some random string to represent the column's name
                    last_step_output = last_step.getOutputCol()
                elif isinstance(last_step, HasPredictionCol):
                    last_step_output = last_step.getPredictionCol()
                else:
                    raise Exception("It must contain output or prediction")

                all_outputs.append(last_step_output)
                all_objects.append(stage)
            else:
                all_objects.append(stage.object)

        # should we connect input with output?
        first_step = set_default_colnames(get_pipeline_firststep(self))
        if not first_step.isSet('inputCols'):
            first_step.setInputCols(all_outputs)

        result = Pipeline().setStages(all_objects + [self])
    else:
        raise Exception("Type of pipeline not supported")

    return result


def right_pipe_function(self, other):
    """
    Prefix pipe operator
    :param self: left-hand side operator
    :param other: right-hand side operator
    :return: Pipeline
    """
    self = set_default_colnames(self)
    if isinstance(other, (list, tuple)) and \
            isinstance(self, tuple(list(ALLOWED_TYPES) + [Pipeline])):
        last_step = set_default_colnames(get_pipeline_laststep(self))

        if isinstance(last_step, HasOutputCol):
            # let's generate some random string to represent the column's name
            last_step_output = last_step.getOutputCol()
        elif isinstance(last_step, HasPredictionCol):
            last_step_output = last_step.getPredictionCol()
        else:
            raise Exception("It must contain output or prediction")

        for stage in other:
            first_step = set_default_colnames(get_pipeline_firststep(stage))

            if isinstance(first_step, HasInputCol):
                # let's generate some random string to represent the column's name
                if not first_step.isSet('inputCol'):
                    first_step.setInputCol(last_step_output)
            elif isinstance(first_step, HasFeaturesCol):
                if not first_step.isSet('featuresCol'):
                    first_step.setFeaturesCol(last_step_output)
            else:
                raise Exception("A step didn't allow inputs")

        result = [NotBroadcasted(self)] + list(other)
    else:
        result = left_pipe_function(other, self)
    return result


class NotBroadcasted:
    """
    Wraps a Params object so that it doesn't broadcast it as part of a many to one
    pipe transformation
    """

    # pylint: disable=too-few-public-methods
    def __init__(self, params_object):
        self.object = params_object


def patch():
    """
    Monkey patches the Params class
    :return: None
    """
    Params.__or__ = right_pipe_function
    Params.__ror__ = left_pipe_function


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


def sql(self, sql_expression):
    """
    Implements the transformations which are defined by SQL statement. Currently we only support
    SQL syntax like "SELECT ... FROM __THIS__ ..." where "__THIS__" represents the
    underlying table of the input dataframe.
    :param sql_expression: SQL expression.
    :return: Dataframe with columns changed by SQL statement.
    """

    self._assert_type_str(sql_expression, "sql_expression")

    from pyspark.ml.feature import SQLTransformer

    sql_trans = SQLTransformer(statement=sql_expression)

    self._df = sql_trans.transform(self._df)

    return self


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


def normalizer(self, input_cols, p=2.0):
    """
    Transforms a dataset of Vector rows, normalizing each Vector to have unit norm. It takes parameter p, which
    specifies the p-norm used for normalization. (p=2) by default.
    :param input_cols: Columns to be normalized.
    :param p:  p-norm used for normalization.
    :return: Dataframe with normalized columns.
    """

    # Check if columns argument must be a string or list datatype:
    self._assert_type_str_or_list(input_cols, "input_cols")

    if isinstance(input_cols, str):
        input_cols = [input_cols]

    assert isinstance(p, (float, int)), "Error: p argument must be a numeric value."

    from pyspark.ml import Pipeline
    from pyspark.ml.feature import Normalizer
    from pyspark.ml.linalg import DenseVector, VectorUDT

    # Convert ArrayType() column to DenseVector
    def arr_to_vec(arr_column):
        """
        :param arr_column: Column name
        :return: Returns DenseVector by converting an ArrayType() column
        """
        return DenseVector(arr_column)

    # User-Defined function
    udf_arr_to_vec = udf(arr_to_vec, VectorUDT())

    # Check for columns which are not DenseVector types and convert them into DenseVector
    for col in input_cols:
        if not isinstance(self._df[col], DenseVector):
            self._df = self._df.withColumn(col, udf_arr_to_vec(self._df[col]))

    normal = [Normalizer(inputCol=column, outputCol=column + "_normalized", p=p) for column in
              list(set(input_cols))]

    pipeline = Pipeline(stages=normal)
    self._df = pipeline.fit(self._df).transform(self._df)

    return self


def undo_vec_assembler(self, column, feature_names):
    """This function unpack a column of list arrays into different columns.
    +-------------------+-------+
    |           features|columna|
    +-------------------+-------+
    |[11, 2, 1, 1, 1, 1]|   hola|
    | [0, 1, 1, 1, 1, 1]|  salut|
    |[31, 1, 1, 1, 1, 1]|  hello|
    +-------------------+-------+
                  |
                  |
                  V
    +-------+---+---+-----+----+----+---+
    |columna|one|two|three|four|five|six|
    +-------+---+---+-----+----+----+---+
    |   hola| 11|  2|    1|   1|   1|  1|
    |  salut|  0|  1|    1|   1|   1|  1|
    |  hello| 31|  1|    1|   1|   1|  1|
    +-------+---+---+-----+----+----+---+
    """
    # Check if column argument a string datatype:
    self._assert_type_str(column, "column")

    assert (column in self._df.columns), "Error: column specified does not exist in dataFrame."

    assert (isinstance(feature_names, list)), "Error: feature_names must be a list of strings."
    # Function to extract value from list column:
    func = udf(lambda x, index: x[index])

    exprs = []

    # Recursive function:
    def exprs_func(column, exprs, feature_names, index):
        if index == 0:
            return [func(col(column), lit(index)).alias(feature_names[index])]
        else:
            return exprs_func(column, exprs, feature_names, index - 1) + [
                func(col(column), lit(index)).alias(feature_names[index])]

    self._df = self._df.select(
        [x for x in self._df.columns] + [*exprs_func(column, exprs, feature_names, len(feature_names) - 1)]).drop(
        column)
    self._add_transformation()  # checkpoint in case

    return self


def scale_vec_col(self, columns, name_output_col):
    """
    This function groups the columns specified and put them in a list array in one column, then a scale
    process is made. The scaling proccedure is spark scaling default (see the example
    bellow).

    +---------+----------+
    |Price    |AreaLiving|
    +---------+----------+
    |1261706.9|16        |
    |1263607.9|16        |
    |1109960.0|19        |
    |978277.0 |19        |
    |885000.0 |19        |
    +---------+----------+

                |
                |
                |
                V
    +----------------------------------------+
    |['Price', 'AreaLiving']                 |
    +----------------------------------------+
    |[0.1673858972637624,0.5]                |
    |[0.08966137157852398,0.3611111111111111]|
    |[0.11587093205757598,0.3888888888888889]|
    |[0.1139820728616421,0.3888888888888889] |
    |[0.12260126542983639,0.4722222222222222]|
    +----------------------------------------+
    only showing top 5 rows

    """

    # Check if columns argument must be a string or list datatype:
    self._assert_type_str_or_list(columns, "columns")

    # Check if columns to be process are in dataframe
    self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

    # Check if name_output_col argument a string datatype:
    self._assert_type_str(name_output_col, "nameOutpuCol")

    # Model to use vectorAssember:
    vec_assembler = VectorAssembler(inputCols=columns, outputCol="features_assembler")
    # Model for scaling feature column:
    mm_scaler = MinMaxScaler(inputCol="features_assembler", outputCol=name_output_col)
    # Dataframe with feature_assembler column
    temp_df = vec_assembler.transform(self._df)
    # Fitting scaler model with transformed dataframe
    model = mm_scaler.fit(temp_df)

    exprs = list(filter(lambda x: x not in columns, self._df.columns))

    exprs.extend([name_output_col])

    self._df = model.transform(temp_df).select(*exprs)
    self._add_transformation()  # checkpoint in case

    return self


def impute_missing(self, columns, out_cols, strategy):
    """
    Imputes missing data from specified columns using the mean or median.
    :param columns: List of columns to be analyze.
    :param out_cols: List of output columns with missing values imputed.
    :param strategy: String that specifies the way of computing missing data. Can be "mean" or "median"
    :return: Transformer object (DF with columns that has the imputed values).
    """

    # Check if columns to be process are in dataframe
    self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

    assert isinstance(columns, list), "Error: columns argument must be a list"

    assert isinstance(out_cols, list), "Error: out_cols argument must be a list"

    # Check if columns argument a string datatype:
    self._assert_type_str(strategy, "strategy")

    assert (strategy == "mean" or strategy == "median"), "Error: strategy has to be 'mean' or 'median'."

    def impute(cols):
        imputer = Imputer(inputCols=cols, outputCols=out_cols)
        model = imputer.setStrategy(strategy).fit(self._df)
        self._df = model.transform(self._df)

    impute(columns)

    return self
