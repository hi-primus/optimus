from optimus.helpers.types import *


class BaseEncoding:
    def __init__(self, root: 'DataFrameType'):
        self.root = root

    def n_gram(self, col, n=2):
        """
        Converts the input array of strings inside of DF into an array of n-grams.
        :param col: Column to analyze.
        :param n: number of elements per n-gram >=1.
        :return:
        """
        raise NotImplementedError('Not implemented yet')

    def one_hot_encoder(self, cols, prefix=None, drop=True, **kwargs):
        """
        Maps a column of label indices to a column of binary vectors, with at most a single one-value.
        :param cols: Columns to be encoded.
        :param prefix: Column where the output is going to be saved.
        :param drop:
        :return: Dataframe with encoded columns.
        """
        raise NotImplementedError('Not implemented yet')

    def vector_assembler(self, cols, output_col=None):
        """
        Combines a given list of columns into a single vector column.
        :param cols: Columns to be assembled.
        :param output_col: Column where the output is going to be saved.
        :return: Dataframe with assembled column.
        """
        raise NotImplementedError('Not implemented yet')

    def normalizer(df, cols, output_col=None, p=2.0):
        """
        Transforms a dataset of Vector rows, normalizing each Vector to have unit norm. It takes parameter p, which
        specifies the p-norm used for normalization. (p=2) by default.
        :param cols: Columns to be normalized.
        :param output_col: Column where the output is going to be saved.
        :param p:  p-norm used for normalization.
        :return: Dataframe with normalized columns.
        """
        raise NotImplementedError('Not implemented yet')
