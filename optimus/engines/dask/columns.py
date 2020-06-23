import dask.dataframe as dd
import pandas as pd
from dask import delayed
from dask.dataframe.core import DataFrame

from optimus.engines.base.dask.columns import DaskBaseColumns
from optimus.helpers.columns import parse_columns


# This implementation works for Dask
def cols(self: DataFrame):
    class Cols(DaskBaseColumns):
        def __init__(self, df):
            super(DaskBaseColumns, self).__init__(df)



    return Cols(self)


DataFrame.cols = property(cols)
