import fastnumbers
import numpy as np
from dask.dataframe.core import DataFrame

from optimus.engines.base.commons.functions import to_integer, to_float
from optimus.engines.base.dask.columns import DaskBaseColumns
from optimus.helpers.constants import Actions


def cols(self: DataFrame):
    class Cols(DaskBaseColumns):
        def __init__(self, df):
            super(DaskBaseColumns, self).__init__(df)

        def to_float(self, input_cols, output_cols=None):

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

    return Cols(self)


DataFrame.cols = property(cols)
