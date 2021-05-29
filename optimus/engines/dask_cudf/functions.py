# These function can return and Column Expression or a list of columns expression
# Must return None if the data type can not be handle

# from dask_cudf.core import DataFrame as DaskCUDFDataFrame

import random
import string

import cudf
import dask

from optimus.engines.base.commons.functions import to_float_cudf, to_integer_cudf
from optimus.engines.base.functions import Functions
from optimus.engines.base.dask.functions import DaskBaseFunctions
from optimus.helpers.core import val_to_list
import dask.dataframe as dd

import numpy as np

class DaskCUDFFunctions(DaskBaseFunctions, Functions):
    def _to_float_partition(self, series):
        return to_float_cudf(series)

    def _to_integer_partition(self, series):
        return to_integer_cudf(series)

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

    def years_between(self, date_format=None):
        raise NotImplementedError("Not implemented yet see https://github.com/rapidsai/cudf/issues/1041")
        # return cudf.to_datetime(series).astype('str', format=date_format) - datetime.now().date()

