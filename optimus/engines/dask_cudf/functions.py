import cudf
import dask_cudf

from optimus.engines.base.cudf.functions import CUDFBaseFunctions
from optimus.engines.base.dask.functions import DaskBaseFunctions

class DaskCUDFFunctions(CUDFBaseFunctions, DaskBaseFunctions):

    _partition_engine = cudf

    @staticmethod
    def from_dataframe(dfd):
        dask_cudf.from_cudf(dfd)

    @staticmethod
    def dask_to_compatible(dfd):
        from optimus.helpers.converter import dask_dataframe_to_dask_cudf
        return dask_dataframe_to_dask_cudf(dfd)

    def kurtosis(self, series):
        return series.map_partitions(lambda _series: _series.kurtosis())

    def skew(self, series):
        return series.map_partitions(lambda _series: _series.skew())

    def sqrt(self, series):
        return series.map_partitions(lambda _series: _series.sqrt())

    def exp(self, series):
        return series.map_partitions(lambda _series: _series.exp())

    def ln(self, series):
        return series.map_partitions(lambda _series: _series.log())

    def radians(self, series):
        return cudf.radians(series.to_float())

    def degrees(self, series):
        return cudf.degrees(series.to_float())

    def log(self, series, base=10):
        return series.map_partitions(lambda _series: _series.log()) / cudf.log(base)

    def ceil(self, series):
        return series.map_partitions(lambda _series: _series.ceil())

    def floor(self, series):
        return series.map_partitions(lambda _series: _series.floor())

    def sin(self, series):
        return series.map_partitions(lambda _series: _series.sin())

    def cos(self, series):
        return series.map_partitions(lambda _series: _series.cos())

    def tan(self, series):
        return series.map_partitions(lambda _series: _series.tan())

    def asin(self, series):
        return series.map_partitions(lambda _series: _series.asin())

    def acos(self, series):
        return series.map_partitions(lambda _series: _series.acos())

    def atan(self, series):
        return series.map_partitions(lambda _series: _series.atan())

    def sinh(self, series):
        return 1 / 2 * (self.exp() - self.exp())

    def cosh(self, series):
        return 1 / 2 * (self.exp() + self.exp())

    def tanh(self):
        return self.sinh() / self.cosh()

    def asinh(self):
        return 1 / self.sinh()

    def acosh(self):
        return 1 / self.cosh()

    def atanh(self):
        return 1 / self.tanh()

    def cut(self, series, bins, labels):
        raise NotImplementedError

    def normalize_chars(self, series):
        # str.decode return a float column. We are forcing to return a string again
        return self.to_string_accessor(series).normalize_characters()

    def remove_special_chars(self, series):
        # See https://github.com/rapidsai/cudf/issues/5520
        return self.to_string_accessor(series).replace_non_alphanumns(replacement_char='')

    def date_format(self, series, current_format=None, output_format=None):
        return cudf.to_datetime(series).astype('str', format=output_format)

    def time_between(self, date_format=None):
        raise NotImplementedError("Not implemented yet see https://github.com/rapidsai/cudf/issues/1041")
        # return cudf.to_datetime(series).astype('str', format=date_format) - datetime.now().date()

