# This functions must handle one or multiple columns
# Must return None if the data type can not be handle


from datetime import datetime, timedelta

import dask.array as da
import pandas as pd
from dask.array import stats

# DataFrame = pd.DataFrame
from optimus.engines.base.commons.functions import to_float, to_integer, to_string
from optimus.engines.base.functions import Functions
from optimus.helpers.core import val_to_list


class DaskFunctions(Functions):
    def __init__(self, parent):
        super(DaskFunctions, self).__init__(parent)

    def to_float(self, series, *args):
        return to_float(series)

    def to_integer(self, series, *args):
        return to_integer(series)

    def to_string(self, series, *args):
        return to_string(series)

    def count_zeros(self, *args):
        series = self.series
        return int((series.ext.to_float().values == 0).sum())

    def kurtosis(self, series):
        return stats.kurtosis(series.ext.to_float())

    def skew(self, series):
        return stats.skew(series.ext.to_float())

    def exp(self, series):
        return da.exp(series.ext.to_float())

    def sqrt(self, series):
        print("series",type(series),series)
        # s = self.parent.new(series).cols.min()
        return self.parent.new(series).cols.to_float().data
        # return

    def unique(self, series, *args):
        # print("args",args)
        # Cudf can not handle null so we fill it with non zero values.
        return series.astype(str).unique().ext.to_dict(index=False)

    # def mod(self, other):
    #     series = self.series
    #     return da.mod(series.ext.to_float(), other)

    # def pow(self, other):
    #     series = self.series
    #     return da.power(series.ext.to_float(), other)

    def radians(self, series):
        return da.radians(series.ext.to_float())

    def degrees(self, series):
        return da.degrees(series.ext.to_float())

    def ln(self, series):
        return da.log(series.ext.to_float())

    def log(self, series):
        return da.log10(series.ext.to_float())

    def ceil(self, series):
        return da.ceil(series.ext.to_float())

    def sin(self, series):
        return da.sin(series.ext.to_float())

    def cos(self, series):
        return da.cos(series.ext.to_float())

    def tan(self, series):
        return da.tan(series.ext.to_float())

    def asin(self, series):
        return da.arcsin(series.ext.to_float())

    def acos(self, series):
        return da.arccos(series.ext.to_float())

    def atan(self, series):
        return da.arctan(series.ext.to_float())

    def sinh(self, series):
        return da.arcsinh(series.ext.to_float())

    def cosh(self, series):
        return da.cosh(series.ext.to_float())

    def tanh(self, series):
        return da.tanh(series.ext.to_float())

    def asinh(self, series):
        return da.arcsinh(series.ext.to_float())

    def acosh(self, series):
        return da.arccosh(series.ext.to_float())

    def atanh(self, series):
        return da.arctanh(series.ext.to_float())

    def clip(self, series, lower_bound, upper_bound):
        return series.ext.to_float().clip(lower_bound, upper_bound)

    def cut(self, series, bins):
        return series.ext.to_float(series).cut(bins, include_lowest=True, labels=list(range(bins)))

    def to_datetime(self, series, format):
        return pd.to_datetime(series, format=format, errors="coerce")

    def remove_special_chars(self, series):
        return series.astype(str).str.replace('[^A-Za-z0-9]+', '')

    def remove_accents(self, series):
        # str.decode return a float column. We are forcing to return a string again
        return series.str.normalize("NFKD").str.encode('ascii', errors='ignore').str.decode('utf8').astype(str)

    def date_format(self, series, current_format=None, output_format=None):
        return pd.to_datetime(series, format=current_format, errors="coerce").dt.strftime(output_format)

    def years_between(self, series, date_format=None):
        return (pd.to_datetime(series, format=date_format,
                               errors="coerce").dt.date - datetime.now().date()) / timedelta(days=365)

    def to_datetime(self, series, format):
        return pd.to_datetime(series, format=format, errors="coerce")

    def replace_chars(self, series, search, replace_by):
        # if ignore_case is True:
        #     # Cudf do not accept re.compile as argument for replace
        #     # regex = re.compile(str_regex, re.IGNORECASE)
        #     regex = str_regex
        # else:
        #     regex = str_regex
        replace_by = val_to_list(replace_by)
        for i, j in zip(search, replace_by):
            series = series.astype(str).str.replace(i, j)
        return series
