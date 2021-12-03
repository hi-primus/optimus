from pyspark.ml import feature, Pipeline
from pyspark.ml.feature import OneHotEncoder, VectorAssembler, Normalizer, IndexToString
import databricks.koalas as ks
from optimus.engines.base.meta import Meta
from optimus.engines.base.ml.constants import INDEX_TO_STRING
from optimus.engines.base.ml.encoding import BaseEncoding
from optimus.helpers.check import is_spark_dataframe
from optimus.helpers.columns import parse_columns, name_col, get_output_cols
from optimus.helpers.constants import Actions
from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_, is_str


class Encoding(BaseEncoding):
    def n_gram(df, input_col, n=2):
        """
        Converts the input array of strings inside of a Spark DF into an array of n-grams.
        :param df: Pyspark dataframe to analyze
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

        indexers = [IndexToString(inputCol=input_col, outputCol=output_col, **kargs) for input_col, output_col
                    in zip(list(set(input_cols)), list(set(output_cols)))]
        pipeline = Pipeline(stages=indexers)
        df = pipeline.fit(df).transform(df)

        df.meta = Meta.action(df.meta, df_actual, Actions.INDEX_TO_STRING.value, output_cols)

        return df

    def one_hot_encoder(self, cols="*", prefix=None, drop=True, **kwargs):
        """
        Maps a column of label indices to a column of binary vectors, with at most a single one-value.
        :param cols: Columns to be encoded.
        :param prefix: Prefix of the columns where the output is going to be saved.
        :param drop:
        :return: Dataframe with encoded columns.
        """
        df = self.root

        df = df.new(ks.concat([df.data, ks.get_dummies(df[cols].cols.to_string().data, prefix=prefix)], axis=1))

        if drop:
            df = df.cols.drop(cols)

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
