# DataFrame = pd.DataFrame
from datetime import datetime

import ibis
import numpy as np
import pandas as pd

from optimus.engines.base.functions import BaseFunctions
from optimus.helpers.core import val_to_list


class IbisFunctions(BaseFunctions):
    _engine = ibis

    def _series_dtype(self, series):
        return series.type().to_pandas().name

    def min(self, series, numeric=False, string=False):
        """
        Get the minimum value of a series
        """

        return series.min()

    def max(self, series, numeric=False, string=False):
        """
        Get the maximum value of a series
        """
        return series.max()


    def to_float(self, series, *args):
        return series.cast("float64")

    def to_string(self, series):
        return series.cast("string")

    def to_string_accessor(self, series):
        return self.to_string(series)

    def title(self, series, *args):
        raise NotImplementedError("Not implemented yet")

    #
    # def replace(self, series, *args):
    #     return series.cast("string").upper()

    def count_zeros(self, *args):
        series = self.series
        return int((series.to_float().values == 0).sum())

    def kurtosis(self, series, *args):
        return self.to_float(series).kurt()

    def skew(self, series, *args):
        return self.to_float(series).skew()

    def exp(self, series, *args):
        return self.to_float(series).exp()

    def sqrt(self, series, *args):
        return self.to_float(series).sqrt()

    def radians(self):
        series = self.series
        return np.radians(series.to_float())

    def degrees(self):
        series = self.series
        return np.degrees(series.to_float())

    def ln(self, series, *args):
        return self.to_float(series).log()

    def log(self, series, base=10):
        return self.to_float(series).log() / np.log(base)

    def ceil(self, series, *args):
        return self.to_float(series).ceil()

    def sin(self):
        series = self.series
        return np.sin(series.to_float())

    def cos(self, series, *args):
        return self.to_float(series).cos()

    def tan(self):
        series = self.series
        return np.tan(series.to_float())

    def asin(self):
        series = self.series
        return np.arcsin(series.to_float())

    def acos(self):
        series = self.series
        return np.arccos(series.to_float())

    def atan(self):
        series = self.series
        return np.arctan(series.to_float())

    def sinh(self):
        series = self.series
        return np.arcsinh(series.to_float())

    def cosh(self):
        series = self.series
        return np.cosh(series.to_float())

    def tanh(self):
        series = self.series
        return np.tanh(series.to_float())

    def asinh(self):
        series = self.series
        return np.arcsinh(series.to_float())

    def acosh(self):
        series = self.series
        return np.arccosh(series.to_float())

    def atanh(self):
        series = self.series
        return np.arctanh(series.to_float())

    def clip(self, lower_bound, upper_bound):
        series = self.series
        return series.clip(lower_bound, upper_bound)

    def cut(self, bins):
        series = self.series
        return series.to_float(series).cut(bins, include_lowest=True, labels=list(range(bins)))

    def replace_chars(self, search, replace_by, ignore_case):
        series = self.series
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

    def normalize_chars(self):
        series = self.series
        return series.str.normalize("NFKD").str.encode('ascii', errors='ignore').str.decode('utf8')

    def format_date(self, current_format=None, output_format=None):
        series = self.series
        return pd.to_datetime(series, format=current_format, errors="coerce").dt.strftime(output_format)

    def time_between(self, date_format=None):
        series = self.series
        return (pd.to_datetime(series, format=date_format,
                               errors="coerce").dt.date - datetime.now().date())
