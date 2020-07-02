import pandas as pd

from optimus.engines.base.commons.functions import to_float, to_integer, to_datetime
from optimus.engines.base.dataframe.extension import DataFrameBaseExt, SeriesBaseExt

DataFrame = pd.DataFrame
Series = pd.Series


def ext(self: DataFrame):
    class Ext(DataFrameBaseExt):

        def __init__(self, df):
            super().__init__(df)

    return Ext(self)


def ext_series(self: Series):
    class Ext(SeriesBaseExt):

        def __init__(self, series):
            super(Ext, self).__init__(series)
            self.series = series

        def to_float(self):
            series = self.series
            return series.map(to_float)

        def to_integer(self):
            series = self.series
            return series.map(to_integer)

        def to_datetime(self, format):
            series = self.series
            return to_datetime(series, format)

    return Ext(self)


DataFrame.ext = property(ext)
Series.ext = property(ext_series)
