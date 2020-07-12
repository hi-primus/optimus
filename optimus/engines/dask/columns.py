from dask.dataframe.core import DataFrame
from dask_ml import preprocessing

from optimus.engines.base.commons.functions import to_integer, to_float, impute, string_to_index, index_to_string
from optimus.engines.base.dask.columns import DaskBaseColumns
from optimus.helpers.constants import Actions


def cols(self: DataFrame):
    class Cols(DaskBaseColumns):
        def __init__(self, df):
            super(DaskBaseColumns, self).__init__(df)

        def string_to_index(self, input_cols=None, output_cols=None, columns=None):
            df = self.df
            le = preprocessing.LabelEncoder()
            return string_to_index(df, input_cols, output_cols, le)

        def index_to_string(self, input_cols=None, output_cols=None, columns=None):
            df = self.df
            le = preprocessing.LabelEncoder()
            return index_to_string(df, input_cols, output_cols, le)

        def to_float(self, input_cols="*", output_cols=None):
            df = self.df

            return df.cols.apply(input_cols, to_float, output_cols=output_cols, meta_action=Actions.TO_FLOAT.value,
                                 mode="map")

        def to_integer(self, input_cols, output_cols=None):
            df = self.df
            return df.cols.apply(input_cols, to_integer, output_cols=output_cols, meta_action=Actions.TO_FLOAT.value,
                                 mode="map")

        def to_string(self, input_cols, output_cols=None):
            df = self.df

            return df.cols.apply(input_cols, str, output_cols=output_cols, meta_action=Actions.TO_FLOAT.value,
                                 mode="map")

        def impute(self, input_cols, data_type="continuous", strategy="mean", output_cols=None):
            df = self.df
            return impute(df, input_cols, data_type="continuous", strategy="mean", output_cols=None)

    return Cols(self)


DataFrame.cols = property(cols)
