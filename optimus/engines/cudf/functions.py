# DataFrame = pd.DataFrame

import cudf

from optimus.engines.base.cudf.functions import CUDFBaseFunctions
from optimus.engines.base.dataframe.functions import DataFrameBaseFunctions


class CUDFFunctions(CUDFBaseFunctions, DataFrameBaseFunctions):

    _engine = cudf

    @staticmethod
    def dask_to_compatible(dfd):
        from optimus.helpers.converter import dask_dataframe_to_cudf
        return dask_dataframe_to_cudf(dfd)

    def count_zeros(self, series, *args):
        # Cudf can not handle null so we fill it with non zero values.
        non_zero_value = 1
        return (self.to_float(series).fillna(non_zero_value).values == 0).sum()

    def kurtosis(self, series):
        return self.to_float(series).kurt()

    def skew(self, series):
        return self.to_float(series).skew()

    def exp(self, series):
        return cudf.exp(self.to_float(series))

    def sqrt(self, series):
        return self.to_float(series).sqrt()

    def unique_values(self, series, *args):
        # Cudf can not handle null so we fill it with non zero values.
        return self.to_string(series).unique()

    # def mod(self, other):
    #     
    #     return cudf.mod(self.to_float(series), other)

    def radians(self, series):
        return cudf.radians(self.to_float(series))

    def degrees(self, series):
        return cudf.degrees(self.to_float(series))

    def ln(self, series):
        return self.to_float(series).log()

    def log(self, series, base=10):
        return cudf.log(self.to_float(series)) / cudf.log(base)

    def ceil(self, series):
        return self.to_float(series).ceil()

    def floor(self, series):
        return self.to_float(series).floor()

    def sin(self, series):
        return self.to_float(series).sin()

    def cos(self, series):
        return self.to_float(series).cos()

    def tan(self, series):
        return self.to_float(series).tan()

    def asin(self, series):
        return self.to_float(series).asin()

    def acos(self, series):
        return self.to_float(series).acos()

    def atan(self, series):
        return self.to_float(series).atan()

    def sinh(self, series):
        return 1 / 2 * (cudf.exp(series) - cudf.exp(-series))

    def cosh(self, series):
        return 1 / 2 * (cudf.exp(series) + cudf.exp(-series))

    def tanh(self, series):
        return self.sinh() / self.cosh()

    def asinh(self, series):
        return 1 / self.sinh()

    def acosh(self, series):
        return 1 / self.cosh()

    def atanh(self, series):
        return 1 / self.tanh()

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
