from datatable import dt, as_type
from optimus.engines.base.dataframe.functions import DataFrameBaseFunctions
from optimus.engines.base.datatable.functions import DatatableBaseFunctions


class DatatableFunctions(DatatableBaseFunctions, DataFrameBaseFunctions):
    _engine = dt

    @classmethod
    def _to_float(cls, series):
        return series[:, as_type(series, float)]

    @classmethod
    def _to_integer(cls, series, default=0):
        return series[:, as_type(series, int)]

    @staticmethod
    def dask_to_compatible(dfd):
        from optimus.helpers.converter import dask_dataframe_to_cudf
        return dask_dataframe_to_cudf(dfd)

    def count_zeros(self, series, *args):
        # Cudf can not handle null so we fill it with non zero values.
        non_zero_value = 1
        return (self.to_float(series).fillna(non_zero_value).values == 0).sum()

    def kurtosis(self, series):
        return series.kurt()[0, 0]

    def skew(self, series):
        return series.skew()[0, 0]

    def exp(self, series):
        return series.exp()[0, 0]

    def sqrt(self, series):
        return series[:, dt.math.sqrt(series)]

    def unique_values(self, series, *args):
        # Cudf can not handle null so we fill it with non zero values.
        return self.to_string(series).unique()

    def std(self, series):
        return series[:, dt.sd(series)][0, 0]

    def min(self, series):
        return series[:, dt.min(series)][0, 0]

    def max(self, series):
        return series[:, dt.max(series)][0, 0]

    # def mod(self, other):
    #     
    #     return cudf.mod(self.to_float(series), other)

    def radians(self, series):
        return series[:, dt.math.deg2rad(series)]

    def degrees(self, series):
        return series[:, dt.math.rad2deg(series)]

    def ln(self, series):
        return series[:, dt.math.log(series)]

    def log(self, series, base=10):
        return series[:, dt.math.log10(series)]

    def ceil(self, series):
        return self.to_float(series).ceil()

    def floor(self, series):
        return self.to_float(series).floor()

    def sin(self, series):
        return series[:, dt.math.sin(series)]

    def cos(self, series):
        return series[:, dt.math.cos(series)]

    def tan(self, series):
        return series[:, dt.math.tan(series)]

    def asin(self, series):
        return series[:, dt.math.asin(series)]

    def acos(self, series):
        return series[:, dt.math.acos(series)]

    def atan(self, series):
        return series[:, dt.math.atan(series)]

    def sinh(self, series):
        return series[:, dt.math.sinh(series)]

    def cosh(self, series):
        return series[:, dt.math.cosh(series)]

    def tanh(self, series):
        return series[:, dt.math.tanh(series)]

    def asinh(self, series):
        return series[:, dt.math.asinh(series)]

    def acosh(self, series):
        return series[:, dt.math.acosh(series)]

    def atanh(self, series):
        return series[:, dt.math.atanh(series)]

    def cut(self, bins):
        raise NotImplementedError("Not implemented yet https://github.com/rapidsai/cudf/issues/5589")

    def normalize_chars(self, series):
        if not series.isnull().all():
            return self.to_string_accessor(series).normalize_characters()
        else:
            return series

    def format_date(self, series, current_format=None, output_format=None):

        # Some formats are no supported yet. https://github.com/rapidsai/cudf/issues/5991
        return cudf.to_datetime(series, format=current_format, errors="coerce").dt.strftime(output_format)

    def time_between(self, date_format=None):

        raise NotImplementedError("Not implemented yet see https://github.com/rapidsai/cudf/issues/1041")
        # return cudf.to_datetime(series).astype('str', format=date_format) - datetime.now().date()
