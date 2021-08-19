# This functions must handle one or multiple columns
# Must return None if the data type can not be handle


from optimus.infer import is_list_or_tuple

import dask
import dask.dataframe as dd
import dask.array as da
import pandas as pd

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

    def kurtosis(self, series):
        return self.to_float(series.dropna()).kurtosis()

    def skew(self, series):
        return self.to_float(series.dropna()).skew()

    def exp(self, series):
        return da.exp(self.to_float(series))

    def sqrt(self, series):
        return da.sqrt(self.to_float(series))

    def reciprocal(self, series):
        return da.reciprocal(self.to_float(series))

    def unique_values(self, series, *args):
        # print("args",args)
        # Cudf can not handle null so we fill it with non zero values.
        return self.to_string(series).unique()

    def radians(self, series):
        return da.radians(self.to_float(series))

    def degrees(self, series):
        return da.degrees(self.to_float(series))

    def ln(self, series):
        return da.log(self.to_float(series))

    def log(self, series, base=10):
        return da.log(self.to_float(series)) / da.log(base)

    def ceil(self, series):
        return da.ceil(self.to_float(series))

    def floor(self, series):
        return da.floor(self.to_float(series))

    def sin(self, series):
        return da.sin(self.to_float(series))

    def cos(self, series):
        return da.cos(self.to_float(series))

    def tan(self, series):
        return da.tan(self.to_float(series))

    def asin(self, series):
        return da.arcsin(self.to_float(series))

    def acos(self, series):
        return da.arccos(self.to_float(series))

    def atan(self, series):
        return da.arctan(self.to_float(series))

    def sinh(self, series):
        return da.arcsinh(self.to_float(series))

    def cosh(self, series):
        return da.cosh(self.to_float(series))

    def tanh(self, series):
        return da.tanh(self.to_float(series))

    def asinh(self, series):
        return da.arcsinh(self.to_float(series))

    def acosh(self, series):
        return da.arccosh(self.to_float(series))

    def atanh(self, series):
        return da.arctanh(self.to_float(series))

    def normalize_chars(self, series):
        # str.decode return a float column. We are forcing to return a string again
        return series.str.normalize("NFKD").str.encode('ascii', errors='ignore').str.decode('utf8').astype(str)

    def format_date(self, series, current_format=None, output_format=None):
        return dd.to_datetime(series, format=current_format, errors="coerce").dt.strftime(output_format)

    def days_between(self, series, value=None, date_format=None):

        value_date_format = date_format

        if is_list_or_tuple(date_format) and len(date_format) == 2:
            date_format, value_date_format = date_format

        if is_list_or_tuple(value) and len(value) == 2:
            value, value_date_format = value

        series = dd.to_datetime(series, format=date_format, errors="coerce", unit='ns').astype('int64')
        dfd = series.to_frame()
        dfd.columns = ['A']
        if value is None:
            dfd['B'] = pd.Timestamp.now()
        else:
            value = dd.to_datetime(value, format=value_date_format, errors="coerce", unit='ns')
            dfd['B'] = value

        return dd.to_timedelta(dfd['B'].astype('int64') - dfd['A']).dt.days
