import numpy as np
import vaex

from optimus.engines.base.functions import BaseFunctions
from optimus.engines.base.vaex.functions import VaexBaseFunctions


class VaexFunctions(VaexBaseFunctions, BaseFunctions):
    _engine = vaex

    def is_integer(self, series):
        # def func(x):
        #     return isintlike(x, on_fail=on_fail)
        #
        # def on_fail(x):
        #     return np.nan

        # if str(series.dtype) in self.constants.DATETIME_INTERNAL_TYPES:
        #     return False
        # if str(series.dtype) in self.constants.INT_INTERNAL_TYPES:
        #     return True
        return self.to_string(series).str.match(r"^\d+$")

    def is_float(self, series):

        # if str(series.dtype) in self.constants.DATETIME_INTERNAL_TYPES:
        #     return False
        # if str(series.dtype) in self.constants.INT_INTERNAL_TYPES:
        #     return True
        return self.to_string(series).str.match(r"^\d+\.\d+$")

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

    def _to_float(self, series):
        def to_float_vaex(value):
            # Faster than fastnumbers
            try:
                value = float(value)
            except ValueError:
                value = np.nan
            return value

        return series.apply(to_float_vaex)

    def _to_integer(self, series):
        def to_integer_vaex(value):
            # Faster than fastnumbers
            try:
                value = int(value)
            except ValueError:
                value = np.nan
            return value

        return series.apply(to_integer_vaex)

    # def to_string(self, series):
    #     if not str(series.dtype) in self.constants.STRING_INTERNAL_TYPES:
    #         return series.astype(str)
    #     else:
    #         return series

    # def to_string(self, series):
    #     return series.astype(str)

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
