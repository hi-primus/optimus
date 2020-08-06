# DataFrame = pd.DataFrame
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pandas import Series

from optimus.engines.base.functions import Functions


def functions(self):
    class PandasFunctions(Functions):
        def __init__(self, df):
            super(PandasFunctions, self).__init__(df)

        def count_zeros(self, *args):
            series = self.series
            return int((series.ext.to_float().values == 0).sum())

        def kurtosis(self):
            series = self.series
            return series.kurtosis(series.ext.to_float())

        def skew(self):
            series = self.series
            return series.skew(series.ext.to_float())

        def exp(self):
            series = self.series
            return np.exp(series.ext.to_float())

        def sqrt(self):
            series = self.series
            return np.sqrt(series.ext.to_float())

        def radians(self):
            series = self.series
            return np.radians(series.ext.to_float())

        def degrees(self):
            series = self.series
            return np.degrees(series.ext.to_float())

        def ln(self):
            series = self.series
            return np.log(series.ext.to_float())

        def log(self):
            series = self.series
            return np.log10(series.ext.to_float())

        def ceil(self):
            series = self.series
            return np.ceil(series.ext.to_float())

        def sin(self):
            series = self.series
            return np.sin(series.ext.to_float())

        def cos(self):
            series = self.series
            return np.cos(series.ext.to_float())

        def tan(self):
            series = self.series
            return np.tan(series.ext.to_float())

        def asin(self):
            series = self.series
            return np.arcsin(series.ext.to_float())

        def acos(self):
            series = self.series
            return np.arccos(series.ext.to_float())

        def atan(self):
            series = self.series
            return np.arctan(series.ext.to_float())

        def sinh(self):
            series = self.series
            return np.arcsinh(series.ext.to_float())

        def cosh(self):
            series = self.series
            return np.cosh(series.ext.to_float())

        def tanh(self):
            series = self.series
            return np.tanh(series.ext.to_float())

        def asinh(self):
            series = self.series
            return np.arcsinh(series.ext.to_float())

        def acosh(self):
            series = self.series
            return np.arccosh(series.ext.to_float())

        def atanh(self):
            series = self.series
            return np.arctanh(series.ext.to_float())

        def clip(self, lower_bound, upper_bound):
            series = self.series
            return series.clip(lower_bound, upper_bound)

        def cut(self, bins):
            series = self.series
            return series.ext.to_float(series).cut(bins, include_lowest=True, labels=list(range(bins)))

        def replace_string(self, search, replace_by):
            series = self.series
            # if ignore_case is True:
            #     # Cudf do not accept re.compile as argument for replace
            #     # regex = re.compile(str_regex, re.IGNORECASE)
            #     regex = str_regex
            # else:
            #     regex = str_regex

            for i, j in zip(search, replace_by):
                series = series.astype(str).str.replace(i, j)
            return series

        def remove_special_chars(self):
            series = self.series
            return series.astype(str).str.replace('[^A-Za-z0-9]+', '')

        def remove_accents(self):
            series = self.series
            return series.str.normalize("NFKD").str.encode('ascii', errors='ignore').str.decode('utf8')

        def date_format(self, current_format=None, output_format=None):
            series = self.series
            return pd.to_datetime(series, format=current_format, errors="coerce").dt.strftime(output_format)

        def years_between(self, date_format=None):
            series = self.series
            return (pd.to_datetime(series, format=date_format,
                                   errors="coerce").dt.date - datetime.now().date()) / timedelta(days=365)

    return PandasFunctions(self)


Series.functions = property(functions)
