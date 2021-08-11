# This functions must handle one or multiple columns
# Must return None if the data type can not be handle


from datetime import datetime

import dask
import dask.dataframe as dd
import dask.array as da
import pandas as pd
from dask.array import stats

from optimus.engines.base.commons.functions import word_tokenize
from optimus.engines.base.pandas.functions import PandasBaseFunctions
from optimus.engines.base.dask.functions import DaskBaseFunctions


class DaskFunctions(PandasBaseFunctions, DaskBaseFunctions):
    
    @property
    def _partition_engine(self):
        return pd

    def from_dataframe(self, dfd):
        return dask.dataframe.from_pandas(dfd, npartitions=self.n_partitions)

    def delayed(self, func):
        def wrapper(*args, **kwargs):
            return dask.delayed(func)(*args, **kwargs)

        return wrapper

    def word_tokenize(self, series):
        return self.to_string(series).map(word_tokenize, na_action=None)

    def kurtosis(self, series):
        return stats.kurtosis(self.to_float(series))

    def skew(self, series):
        return stats.skew(self.to_float(series))

    def exp(self, series):
        return dd.from_pandas(da.exp(self.to_float(series)), series.npartitions, None)

    def sqrt(self, series):
        return dd.from_pandas(da.sqrt(self.to_float(series)), series.npartitions, None)

    def reciprocal(self, series):
        return dd.from_pandas(da.reciprocal(self.to_float(series)), series.npartitions, None)

    def unique_values(self, series, *args):
        # print("args",args)
        # Cudf can not handle null so we fill it with non zero values.
        return self.to_string(series).unique()

    def radians(self, series):
        return dd.from_pandas(da.radians(self.to_float(series)), series.npartitions, None)

    def degrees(self, series):
        return dd.from_pandas(da.degrees(self.to_float(series)), series.npartitions, None)

    def ln(self, series):
        return dd.from_pandas(da.log(self.to_float(series)), series.npartitions, None)

    def log(self, series, base=10):
        return dd.from_pandas(da.log(self.to_float(series)) / da.log(base), series.npartitions, None)

    def ceil(self, series):
        return dd.from_pandas(da.ceil(self.to_float(series)), series.npartitions, None)

    def floor(self, series):
        return dd.from_pandas(da.floor(self.to_float(series)), series.npartitions, None)

    def sin(self, series):
        return dd.from_pandas(da.sin(self.to_float(series)), series.npartitions, None)

    def cos(self, series):
        return dd.from_pandas(da.cos(self.to_float(series)), series.npartitions, None)

    def tan(self, series):
        return dd.from_pandas(da.tan(self.to_float(series)), series.npartitions, None)

    def asin(self, series):
        return dd.from_pandas(da.arcsin(self.to_float(series)), series.npartitions, None)

    def acos(self, series):
        return dd.from_pandas(da.arccos(self.to_float(series)), series.npartitions, None)

    def atan(self, series):
        return dd.from_pandas(da.arctan(self.to_float(series)), series.npartitions, None)

    def sinh(self, series):
        return dd.from_pandas(da.arcsinh(self.to_float(series)), series.npartitions, None)

    def cosh(self, series):
        return dd.from_pandas(da.cosh(self.to_float(series)), series.npartitions, None)

    def tanh(self, series):
        return dd.from_pandas(da.tanh(self.to_float(series)), series.npartitions, None)

    def asinh(self, series):
        return dd.from_pandas(da.arcsinh(self.to_float(series)), series.npartitions, None)

    def acosh(self, series):
        return dd.from_pandas(da.arccosh(self.to_float(series)), series.npartitions, None)

    def atanh(self, series):
        return dd.from_pandas(da.arctanh(self.to_float(series)), series.npartitions, None)

    def normalize_chars(self, series):
        # str.decode return a float column. We are forcing to return a string again
        return series.str.normalize("NFKD").str.encode('ascii', errors='ignore').str.decode('utf8').astype(str)

    def format_date(self, series, current_format=None, output_format=None):
        return dd.from_pandas(pd.to_datetime(series, format=current_format,
                              errors="coerce").dt.strftime(output_format), series.npartitions, None)

    def days_between(self, series, date_format=None):
        return dd.from_pandas(pd.to_datetime(series, format=date_format,
                              errors="coerce").dt.date - datetime.now().date(), series.npartitions, None)
