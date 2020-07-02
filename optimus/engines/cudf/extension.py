from cudf.core import DataFrame
from cudf.core import Series

from optimus.engines.base.commons.functions import to_float_cudf, to_integer_cudf, to_datetime_cudf
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
            self.series = series

        def to_float(self):
            series = self.series
            return to_float_cudf(series)

        def to_integer(self):
            series = self.series
            return to_integer_cudf(series)

        def to_datetime(self, format):
            series = self.series
            return to_datetime_cudf(series, format)

    return Ext(self)


DataFrame.ext = property(ext)
Series.ext = property(ext_series)
