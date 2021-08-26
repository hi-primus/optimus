import numpy as np
import pandas as pd
from sklearn import preprocessing

from optimus.engines.base.commons.functions import string_to_index, index_to_string, find
from optimus.engines.base.pandas.columns import PandasBaseColumns
from optimus.engines.base.dataframe.columns import DataFrameBaseColumns

DataFrame = pd.DataFrame


class Cols(PandasBaseColumns, DataFrameBaseColumns):
    def __init__(self, df):
        super().__init__(df)

    def _series_to_pandas(self, series):
        return series

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

    def count_by_data_types(self, cols="*", data_type=None):
        df = self.root
        result = {}
        df_len = len(df.data)
        for col_name, na_count in df.cols.count_na(cols, tidy=False)["count_na"].items():

            mismatches_count = df.cols.match_data_type(
                col_name, data_type).data.value_counts().to_dict().get(False)
            mismatches_count = 0 if mismatches_count is None else mismatches_count
            result[col_name] = {"match": df_len - na_count, "missing": na_count,
                                "mismatch": mismatches_count - na_count}
        return result

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
