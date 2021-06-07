import numpy as np
import pandas as pd
from sklearn import preprocessing

from optimus.engines.base.commons.functions import impute, string_to_index, index_to_string, find
from optimus.engines.base.dataframe.columns import DataFrameBaseColumns
from optimus.engines.base.columns import BaseColumns

DataFrame = pd.DataFrame


class Cols(DataFrameBaseColumns, BaseColumns):
    def __init__(self, df):
        super().__init__(df)

    @property
    def _pd(self):
        return pd

    def _series_to_pandas(self, series):
        return series

    def find(self, columns, sub, ignore_case=False):
        """
        Find the start and end position for a char or substring
        :param columns:
        :param ignore_case:
        :param sub:
        :return:
        """
        df = self.root
        return find(df, columns, sub, ignore_case)

    @staticmethod
    def to_timestamp(input_cols, date_format=None, output_cols=None):
        pass

    def impute(self, input_cols, data_type="continuous", strategy="mean", fill_value=None, output_cols=None):
        df = self.root
        return impute(df, input_cols, data_type=data_type, strategy=strategy, fill_value=fill_value, output_cols=None)

    @staticmethod
    def astype(*args, **kwargs):
        pass

    # NLP
    @staticmethod
    def stem_words(input_col):
        df = self

    def heatmap(self, col_x, col_y, bins_x=10, bins_y=10):
        df = self.root.data
        heatmap, xedges, yedges = np.histogram2d(df[col_x].values, df[col_y].values, bins=[bins_x, bins_y])
        extent = [xedges[0], xedges[-1], yedges[0], yedges[-1]]
        return {"x": {"name": col_x, "values": heatmap.T.tolist()}, "y": {"name": col_y, "values": extent}}

    def count_by_dtypes(self, columns, dtype):
        df = self.root
        result = {}
        df_len = len(df.data)
        for col_name, na_count in df.cols.count_na(columns, tidy=False)["count_na"].items():
            # for i, j in df.constants.DTYPES_DICT.items():
            #     if j == df[col_name].dtype.type:
            #         _dtype = df.constants.SHORT_DTYPES[i]

            # _dtype = df.cols.dtypes(col_name)[col_name]

            mismatches_count = df.cols.is_match(col_name, dtype).value_counts().to_dict().get(False)
            mismatches_count = 0 if mismatches_count is None else mismatches_count
            result[col_name] = {"match": df_len - na_count, "missing": na_count,
                                "mismatch": mismatches_count - na_count}
        return result

    @staticmethod
    def qcut(columns, num_buckets, handle_invalid="skip"):
        pass

    def string_to_index(self, input_cols="*", output_cols=None, columns=None):
        df = self.root
        le = preprocessing.LabelEncoder()
        df = string_to_index(df, input_cols, output_cols, le)

        return df

    def index_to_string(self, input_cols=None, output_cols=None, columns=None):
        df = self.root
        le = preprocessing.LabelEncoder()
        df = index_to_string(df, input_cols, output_cols, le)

        return df
