from cudf.core import DataFrame
from cudf.core import Series

from optimus.engines.base.dataframe.extension import DataFrameBaseExt, SeriesBaseExt


def ext(self: DataFrame):
    class Ext(DataFrameBaseExt):
        _name = None

        def __init__(self, df):
            super(DataFrameBaseExt, self).__init__(df)

    return Ext(self)


def ext_series(self: Series):

    class Ext(SeriesBaseExt):

        def __init__(self, series):
            super(Ext, self).__init__(series)

            # self.series = series

    return Ext(self)


DataFrame.ext = property(ext)
Series.ext = property(ext_series)
