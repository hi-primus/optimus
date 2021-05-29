# This functions must handle one or multiple columns
# Must return None if the data type can not be handle


from datetime import datetime, timedelta

import dask
import dask.array as da
import pandas as pd
from dask.array import stats

from optimus.engines.base.commons.functions import to_float, to_integer, to_boolean, to_datetime, word_tokenize
from optimus.engines.base.functions import Functions
from optimus.engines.base.dask.functions import DaskBaseFunctions
from optimus.helpers.core import val_to_list


class DaskFunctions(DaskBaseFunctions, Functions):

    @property
    def constants(self):
        from optimus.engines.base.dask.constants import constants
        return constants(self)

    def delayed(self, func):
        def wrapper(*args, **kwargs):
            return dask.delayed(func)(*args, **kwargs)

        return wrapper

    def to_float(self, series):
        return to_float(series)

    def to_integer(self, series):
        return to_integer(series)

    def to_boolean(self, series):
        return to_boolean(series)

    def to_string(self, series):
        if str(series.dtype) in self.constants.STRING_TYPES:
            return series
        return series.astype(str)

    def word_tokenize(self, value):
        return word_tokenize(value)

    def kurtosis(self, series):
        return stats.kurtosis(self._to_float(series))

    def skew(self, series):
        return stats.skew(self._to_float(series))

    def exp(self, series):
        return da.exp(self._to_float(series))

    def sqrt(self, series):
        return da.sqrt(self._to_float(series))

    def unique(self, series, *args):
        # print("args",args)
        # Cudf can not handle null so we fill it with non zero values.
        return self.to_string(series).unique()

    def radians(self, series):
        return da.radians(self._to_float(series))

    def degrees(self, series):
        return da.degrees(self._to_float(series))

    def ln(self, series):
        return da.log(self._to_float(series))

    def log(self, series, base=10):
        return da.log(self._to_float(series)) / da.log(base)

    def ceil(self, series):
        return da.ceil(self._to_float(series))

    def floor(self, series):
        return da.floor(self._to_float(series))

    def sin(self, series):
        return da.sin(self._to_float(series))

    def cos(self, series):
        return da.cos(self._to_float(series))

    def tan(self, series):
        return da.tan(self._to_float(series))

    def asin(self, series):
        return da.arcsin(self._to_float(series))

    def acos(self, series):
        return da.arccos(self._to_float(series))

    def atan(self, series):
        return da.arctan(self._to_float(series))

    def sinh(self, series):
        return da.arcsinh(self._to_float(series))

    def cosh(self, series):
        return da.cosh(self._to_float(series))

    def tanh(self, series):
        return da.tanh(self._to_float(series))

    def asinh(self, series):
        return da.arcsinh(self._to_float(series))

    def acosh(self, series):
        return da.arccosh(self._to_float(series))

    def atanh(self, series):
        return da.arctanh(self._to_float(series))

    def normalize_chars(self, series):
        # str.decode return a float column. We are forcing to return a string again
        return series.str.normalize("NFKD").str.encode('ascii', errors='ignore').str.decode('utf8').astype(str)

    def date_format(self, series, current_format=None, output_format=None):
        return pd.to_datetime(series, format=current_format, errors="coerce").dt.strftime(output_format)

    def years_between(self, series, date_format=None):
        return (pd.to_datetime(series, format=date_format,
                               errors="coerce").dt.date - datetime.now().date()) / timedelta(days=365)

    def to_datetime(self, series, format):
        return to_datetime(series, format)
