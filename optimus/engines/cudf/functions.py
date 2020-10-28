# DataFrame = pd.DataFrame

import cudf
from cudf import Series

from optimus.engines.base.functions import Functions


def functions(self):
    class CUDFFunctions(Functions):
        def __init__(self, df):
            super(CUDFFunctions, self).__init__(df)



        def count_zeros(self, *args):
            # Cudf can not handle null so we fill it with non zero values.
            series = self.series
            non_zero_value = 1
            return (series.ext.to_float().fillna(non_zero_value).values == 0).sum()

        def kurtosis(self):
            series = self.series
            return series.ext.to_float().kurt()

        def skew(self):
            series = self.series
            return series.ext.to_float().skew()

        def exp(self):
            series = self.series
            return cudf.exp(series.ext.to_float())

        def sqrt(self):
            series = self.series
            return series.ext.to_float().sqrt()

        def unique(self, *args):
            series = self.series
            # Cudf can not handle null so we fill it with non zero values.
            return series.astype(str).unique()

        # def mod(self, other):
        #     series = self.series
        #     return cudf.mod(series.ext.to_float(), other)

        def radians(self):
            series = self.series
            return cudf.radians(series.ext.to_float())

        def degrees(self):
            series = self.series
            return cudf.degrees(series.ext.to_float())

        def ln(self):
            series = self.series
            return cudf.log(series.ext.to_float())

        def log(self):
            series = self.series
            return series.ext.to_float().log() / cudf.log(10)

        def ceil(self):
            series = self.series
            return series.ext.to_float().ceil()

        def floor(self):
            series = self.series
            return series.ext.to_float().ceil()

        def sin(self):
            series = self.series
            return series.ext.to_float().sin()

        def cos(self):
            series = self.series
            return series.ext.to_float().cos()

        def tan(self):
            series = self.series
            return series.ext.to_float().tan()

        def asin(self):
            series = self.series
            return series.ext.to_float().asin()

        def acos(self):
            series = self.series
            return series.ext.to_float().acos()

        def atan(self):
            series = self.series
            return series.ext.to_float().atan()

        def sinh(self):
            series = self.series
            return 1 / 2 * (cudf.exp(series) - cudf.exp(-series))

        def cosh(self):
            series = self.series
            return 1 / 2 * (cudf.exp(series) + cudf.exp(-series))

        def tanh(self):
            return self.sinh() / self.cosh()

        def asinh(self):
            return 1 / self.sinh()

        def acosh(self):
            return 1 / self.cosh()

        def atanh(self):
            return 1 / self.tanh()

        def clip(self, lower_bound, upper_bound):
            series = self.series
            return series.ext.to_float().clip(float(lower_bound), float(upper_bound))

        def cut(self, bins):
            raise NotImplementedError("Not implemented yet https://github.com/rapidsai/cudf/issues/5589")

        def replace_string(self, search, replace_by):
            series = self.series
            return series.astype(str).str.replace(search, replace_by)

        def remove_special_chars(self):
            series = self.series
            return series.astype(str).str.filter_alphanum()

        def remove_accents(self):
            series = self.series
            if not series.isnull().all():
                return series.astype(str).str.normalize_characters()
            else:
                return series

        def date_format(self, current_format=None, output_format=None):
            series = self.series
            return cudf.to_datetime(series).astype('str', format=output_format)

        def years_between(self, date_format=None):
            series = self.series
            raise NotImplementedError("Not implemented yet see https://github.com/rapidsai/cudf/issues/1041")
            # return cudf.to_datetime(series).astype('str', format=date_format) - datetime.now().date()

    return CUDFFunctions(self)


Series.functions = property(functions)
