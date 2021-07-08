import numpy as np

from optimus.engines.base.commons.functions import to_float, to_float_vaex
from optimus.engines.base.functions import Functions


class VaexFunctions(Functions):

    @staticmethod
    def count_zeros(series, *args):
        pass

    @staticmethod
    def kurtosis(series):
        pass

    @staticmethod
    def skew(series):
        pass

    def word_tokenize(self, series):
        pass

    @staticmethod
    def sqrt(series):
        pass

    @staticmethod
    def radians(series):
        pass

    @staticmethod
    def degrees(series):
        pass

    @staticmethod
    def ln(series):
        pass

    @staticmethod
    def log(series, base):
        pass

    @staticmethod
    def ceil(series):
        pass

    def to_float(self, series):
        return to_float_vaex

    def _to_float(self, series):
        return series.apply(to_float_vaex)

    def sin(self, series):
        return np.sin(self._to_float(series))

    @staticmethod
    def cos(series):
        pass

    @staticmethod
    def tan(series):
        pass

    @staticmethod
    def asin(series):
        pass

    @staticmethod
    def acos(series):
        pass

    @staticmethod
    def atan(series):
        pass

    @staticmethod
    def sinh(series):
        pass

    @staticmethod
    def cosh(series):
        pass

    @staticmethod
    def tanh(series):
        pass

    @staticmethod
    def asinh(series):
        pass

    @staticmethod
    def acosh(series):
        pass

    @staticmethod
    def atanh(series):
        pass

    @staticmethod
    def replace_chars(series, search, replace_by):
        pass

    @staticmethod
    def date_format(self, current_format=None, output_format=None):
        pass

    @staticmethod
    def days_between(self, date_format=None):
        pass

    def slice(self, series, start, stop, step):
        # Step is not handle by Vaex for version 4.0
        if step:
            print("step param is not implemented in Vaex")
        return self.to_string_accessor(series).slice(start, stop)
