from sklearn import preprocessing

import polars as pl
from optimus.engines.base.commons.functions import string_to_index, index_to_string, find
from optimus.engines.base.dataframe.columns import DataFrameBaseColumns
from optimus.engines.base.polars.columns import PolarsBaseColumns

DataFrame = pl.DataFrame


class Cols(PolarsBaseColumns, DataFrameBaseColumns):
    def __init__(self, df):
        super().__init__(df)

    def _series_to_pandas(self, series):
        return series

    def _select(self, cols):
        return self.root.data.select(cols)

    def _names(self):
        return list(self.root.data.columns)

    def find(self, cols="*", sub=None, ignore_case=False):
        """
        Find the start and end position for a char or substring
        :param cols:
        :param ignore_case:
        :param sub:
        :return:
        """
        df = self.root
        return find(df, cols, sub, ignore_case)

    def to_timestamp(self, cols="*", date_format=None, output_cols=None):
        raise NotImplementedError('Not implemented yet')

    def astype(self, cols="*", output_cols=None, *args, **kwargs):
        raise NotImplementedError('Not implemented yet')

    # NLP
    def stem_words(self, input_col):
        raise NotImplementedError('Not implemented yet')

    def string_to_index(self, cols="*", output_cols=None):
        df = self.root
        df.le = df.le or preprocessing.LabelEncoder()
        df = string_to_index(df, cols, output_cols, df.le)

        return df

    def index_to_string(self, cols="*", output_cols=None):
        df = self.root
        df.le = df.le or preprocessing.LabelEncoder()
        df = index_to_string(df, cols, output_cols, df.le)

        return df

    def _data_type(self):
        # We need to collect the data to get the columns data types.
        return [(col_name, dtype) for col_name, dtype in
                zip(self.root.cols.names(), [t.__name__.lower() for t in self.root.data.fetch(10).dtypes])]
