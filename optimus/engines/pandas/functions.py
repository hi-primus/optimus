# DataFrame = pd.DataFrame
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import re
from optimus.engines.base.commons.functions import to_string, to_integer, to_float, to_boolean, word_tokenize
from optimus.engines.base.functions import Functions


class PandasFunctions(Functions):

    def _to_float(self, value):
        return value.map(to_float)

    def to_float(self, series):
        return to_float(series)

    def to_string(self, series):
        return to_float(series)

    def _to_integer(self, value):
        return value.map(to_integer)

    def to_integer(self, series):
        return to_integer(series)

    def to_boolean(self, series):
        return to_boolean(series)

    def to_string(self, series):
        return to_string(series)

    def word_tokenize(self, value):
        return word_tokenize(value)

    def count_zeros(self, series, *args):
        return int((self.to_float(series).values == 0).sum())

    def kurtosis(self, series):
        return self.to_float(series).kurtosis()

    def skew(self, series):
        return self.to_float(series).skew()

    def exp(self, series):
        return np.exp(self.to_float(series))

    def sqrt(self, series):
        return np.sqrt(self.to_float(series))

    def reciprocal(self, series):
        return np.reciprocal(self.to_float(series))

    def radians(self, series):
        return np.radians(self.to_float(series))

    def degrees(self, series):
        return np.degrees(self.to_float(series))

    def ln(self, series):
        return np.log(self.to_float(series))

    def log(self, series, base=10):
        return np.log(self.to_float(series)) / np.log(base)

    def sin(self, series):
        return np.sin(self.to_float(series))

    def cos(self, series):
        return np.cos(self.to_float(series))

    def tan(self, series):
        return np.tan(self.to_float(series))

    def asin(self, series):
        return np.arcsin(self.to_float(series))

    def acos(self, series):
        return np.arccos(self.to_float(series))

    def atan(self, series):
        return np.arctan(self.to_float(series))

    def sinh(self, series):
        return np.arcsinh(self.to_float(series))

    def cosh(self, series):
        return np.cosh(self.to_float(series))

    def tanh(self, series):
        return np.tanh(self.to_float(series))

    def asinh(self, series):
        return np.arcsinh(self.to_float(series))

    def acosh(self, series):
        return np.arccosh(self.to_float(series))

    def atanh(self, series):
        return np.arctanh(self.to_float(series))

    def floor(self, series):
        return np.floor(self.to_float(series))

    def ceil(self, series):
        return np.ceil(self.to_float(series))

    def replace_chars(self, series, search, replace_by):
        search = list(map(re.escape, search))
        return series.replace(search, replace_by, regex=True)

    def normalize_chars(self, series):
        return series.str.normalize("NFKD").str.encode('ascii', errors='ignore').str.decode('utf8')

    def date_format(self, series, current_format=None, output_format=None):
        return pd.to_datetime(series, format=current_format, errors="coerce").dt.strftime(output_format).reset_index(
            drop=True)

    def years_between(self, series, date_format=None):
        return (pd.to_datetime(series, format=date_format,
                               errors="coerce").dt.date - datetime.now().date()) / timedelta(days=365)
