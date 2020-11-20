# DataFrame = pd.DataFrame

import cudf

from optimus.engines.base.commons.functions import to_float_cudf, to_integer_cudf
from optimus.engines.base.functions import Functions


class CUDFFunctions(Functions):
    def __init__(self, df):
        super(CUDFFunctions, self).__init__(df)

    def to_float(self, series):
        return to_float_cudf(series)

    def to_integer(self, series):
        return to_integer_cudf(series)

    def count_zeros(self, series, *args):
        # Cudf can not handle null so we fill it with non zero values.
        non_zero_value = 1
        return (self.to_float(series).fillna(non_zero_value).values == 0).sum()

    def kurtosis(self, series):
        return self.to_float(series).kurt()

    def skew(self, series):
        return self.to_float(series).skew()

    def exp(self, series):
        return cudf.exp(self.to_float(series))

    def sqrt(self, series):
        return self.to_float(series).sqrt()

    def unique(self, series, *args):
        # Cudf can not handle null so we fill it with non zero values.
        return series.astype(str).unique()

    # def mod(self, other):
    #     
    #     return cudf.mod(self.to_float(series), other)

    def radians(self, series):
        return cudf.radians(self.to_float(series))

    def degrees(self, series):
        return cudf.degrees(self.to_float(series))

    def ln(self, series):
        return cudf.log(self.to_float(series))

    def log(self, series):
        return self.to_float(series).log() / cudf.log(10)

    def ceil(self, series):
        return self.to_float(series).ceil()

    def floor(self, series):
        return self.to_float(series).ceil()

    def sin(self, series):
        return self.to_float(series).sin()

    def cos(self, series):
        return self.to_float(series).cos()

    def tan(self, series):
        return self.to_float(series).tan()

    def asin(self, series):
        return self.to_float(series).asin()

    def acos(self, series):
        return self.to_float(series).acos()

    def atan(self, series):
        return self.to_float(series).atan()

    def sinh(self, series):
        return 1 / 2 * (cudf.exp(series) - cudf.exp(-series))

    def cosh(self, series):
        return 1 / 2 * (cudf.exp(series) + cudf.exp(-series))

    def tanh(self, series):
        return self.sinh() / self.cosh()

    def asinh(self, series):
        return 1 / self.sinh()

    def acosh(self, series):
        return 1 / self.cosh()

    def atanh(self, series):
        return 1 / self.tanh()

    def clip(self, series, lower_bound, upper_bound):
        return self.to_float(series).clip(float(lower_bound), float(upper_bound))

    def cut(self, bins):
        raise NotImplementedError("Not implemented yet https://github.com/rapidsai/cudf/issues/5589")

    def replace_chars(self, series, search, replace_by):
        return series.astype(str).str.replace(search, replace_by)

    def remove_special_chars(self, series):
        return series.astype(str).str.filter_alphanum()

    def remove_accents(self, series):
        if not series.isnull().all():
            return series.astype(str).str.normalize_characters()
        else:
            return series

    def date_format(self, series, current_format=None, output_format=None):

        # Some formats are no supported yet. https://github.com/rapidsai/cudf/issues/5991
        return cudf.to_datetime(series, format=current_format, errors="coerce").dt.strftime(output_format)

    def years_between(self, date_format=None):

        raise NotImplementedError("Not implemented yet see https://github.com/rapidsai/cudf/issues/1041")
        # return cudf.to_datetime(series).astype('str', format=date_format) - datetime.now().date()
