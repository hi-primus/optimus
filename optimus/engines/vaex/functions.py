import vaex
import numpy as np
from fastnumbers import fast_float, fast_int, isintlike

from optimus.engines.base.functions import BaseFunctions


class VaexFunctions(BaseFunctions):

    _engine = vaex

    def is_integer(self, series):
        # if str(series.dtype) in self.constants.DATETIME_INTERNAL_TYPES:
        #     return False
        # if str(series.dtype) in self.constants.INT_INTERNAL_TYPES:
        #     return True
        return np.vectorize(isintlike)(series).flatten()

    @staticmethod
    def count_zeros(series, *args):
        pass

    @staticmethod
    def kurtosis(series):
        pass

    @staticmethod
    def skew(series):
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
        def to_float_vaex(series):
            return fast_float(series, default=np.nan)
        return series.apply(to_float_vaex)

    def to_integer(self, series):
        def to_integer_vaex(series):
            return fast_int(series, default=np.nan)
        return series.apply(to_integer_vaex)

    def to_string(self, series):
        def to_string_vaex(series):
            return series.astype(str)
        return series.apply(to_string_vaex)

    def sin(self, series):
        return np.sin(self.to_float(series))

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
    def replace_chars(series, search, replace_by, ignore_case):
        pass

    @staticmethod
    def format_date(self, current_format=None, output_format=None):
        pass

    @staticmethod
    def time_between(self, date_format=None):
        pass

    def slice(self, series, start, stop, step):
        # Step is not handle by Vaex for version 4.0
        if step:
            print("step param is not implemented in Vaex")
        return self.to_string_accessor(series).slice(start, stop)
