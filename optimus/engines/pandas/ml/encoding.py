import pandas as pd

from optimus.engines.base.ml.encoding import BaseEncoding
from optimus.helpers.columns import parse_columns, name_col
from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_, is_str


class Encoding(BaseEncoding):

    def n_gram(self, input_col, n=2):
        """
        Converts the input array of strings inside of DF into an array of n-grams.
        :param input_col: Column to analyzer.
        :param n: number of elements per n-gram >=1.
        :return:
        """

        pass

    def one_hot_encoder(self, cols="*", prefix=None, drop=True, **kwargs):
        """
        Maps a column of label indices to a column of binary vectors, with at most a single one-value.
        :param cols: Columns to be encoded.
        :param prefix: Prefix of the columns where the output is going to be saved.
        :param drop:
        :return: Dataframe with encoded columns.
        """
        df = self.root

        df = df.new(pd.concat([df.data, pd.get_dummies(df[cols].cols.to_string().data, prefix=prefix)], axis=1))

        if drop:
            df = df.cols.drop(cols)

        return df

    # TODO: Must we use the pipeline version?
    def vector_assembler(self, input_cols, output_col=None):
        """
        Combines a given list of columns into a single vector column.
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
