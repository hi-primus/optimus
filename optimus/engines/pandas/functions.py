# DataFrame = pd.DataFrame
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from optimus.engines.base.commons.functions import to_string, to_integer, to_float
from optimus.engines.base.functions import Functions
from optimus.helpers.core import val_to_list


class PandasFunctions(Functions):
    # def __init__(self, ):
    #     super(PandasFunctions, self).__init__(df)

    def _to_float(self, value):
        return value.map(to_float)

    def to_float(self, series):
        return to_float(series)

    def _to_integer(self, value):
        return value.map(to_integer)

    def to_integer(self, series):
        return to_integer(series)

    def to_string(self, series):
        return to_string(series)

    def count_zeros(self, series, *args):
        return int((series.to_float().values == 0).sum())

    def kurtosis(self, series):
        return series.kurtosis(self._to_float(series))

    def skew(self, series):
        return series.skew(self._to_float(series))

    def exp(self, series):
        return np.exp(self._to_float(series))

    def sqrt(self, series):
        return np.sqrt(self._to_float(series))

    def radians(self, series):
        return np.radians(self._to_float(series))

    def degrees(self, series):
        return np.degrees(self._to_float(series))

    def ln(self, series):
        return np.log(self._to_float(series))

    def log(self, series):
        return np.log10(self._to_float(series))

    def ceil(self, series):
        return np.ceil(self._to_float(series))

    def sin(self, series):
        return np.sin(self._to_float(series))

    def cos(self, series):
        return np.cos(self._to_float(series))

    def tan(self, series):
        return np.tan(self._to_float(series))

    def asin(self, series):
        return np.arcsin(self._to_float(series))

    def acos(self, series):
        return np.arccos(self._to_float(series))

    def atan(self, series):
        return np.arctan(self._to_float(series))

    def sinh(self, series):
        return np.arcsinh(self._to_float(series))

    def cosh(self, series):
        return np.cosh(self._to_float(series))

    def tanh(self, series):
        return np.tanh(self._to_float(series))

    def asinh(self, series):
        return np.arcsinh(self._to_float(series))

    def acosh(self, series):
        return np.arccosh(self._to_float(series))

    def atanh(self, series):
        return np.arctanh(self._to_float(series))

    def clip(self, series, lower_bound, upper_bound):
        return series.clip(lower_bound, upper_bound)

    def cut(self, series, bins):
        return series.to_float(series).cut(bins, include_lowest=True, labels=list(range(bins)))

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

    def remove_special_chars(self, series):
        return series.astype(str).str.replace('[^A-Za-z0-9]+', '')

    def remove_accents(self, series):
        return series.str.normalize("NFKD").str.encode('ascii', errors='ignore').str.decode('utf8')

    def date_format(self, series, current_format=None, output_format=None):
        return pd.to_datetime(series, format=current_format, errors="coerce").dt.strftime(output_format)

    def years_between(self, series, date_format=None):
        return (pd.to_datetime(series, format=date_format,
                               errors="coerce").dt.date - datetime.now().date()) / timedelta(days=365)
