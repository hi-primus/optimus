# DataFrame = pd.DataFrame

import cudf

from optimus.engines.base.functions import Functions


def functions(self):
    class CUDFFunctions(Functions):
        def __init__(self, df):
            super(CUDFFunctions, self).__init__(df)

        def kurtosis(self):
            series = self.series
            return cudf.kurtosis(series.ext.to_float())

        def skew(self):
            series = self.series
            return cudf.skew(series.ext.to_float())

        def exp(self):
            series = self.series
            return cudf.exp(series.ext.to_float())

        def sqrt(self):
            series = self.series
            return cudf.sqrt(series.ext.to_float())

        # def mod(self, other):
        #     series = self.series
        #     return cudf.mod(series.ext.to_float(), other)

        # def pow(self, other):
        #     series = self.series
        #     return cudf.power(series.ext.to_float(), other)

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
            return cudf.log10(series.ext.to_float())

        def ceil(self):
            series = self.series
            return cudf.ceil(series.ext.to_float())

        def sin(self):
            series = self.series
            return cudf.sin(series.ext.to_float())

        def cos(self):
            series = self.series
            return cudf.cos(series.ext.to_float())

        def tan(self):
            series = self.series
            return cudf.tan(series.ext.to_float())

        def asin(self):
            series = self.series
            return cudf.arcsin(series.ext.to_float())

        def acos(self):
            series = self.series
            return cudf.arccos(series.ext.to_float())

        def atan(self):
            series = self.series
            return cudf.arctan(series.ext.to_float())

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
            raise NotImplementedError("Not implemented yet https://github.com/rapidsai/cudf/pull/5222")

        def remove_special_chars(self):
            series = self.series
            # See https://github.com/rapidsai/cudf/issues/5520
            return series.astype(str).str.replace_non_alphanumns(replacement_char='')

        def date_format(self, current_format=None, output_format=None):
            series = self.series
            return cudf.to_datetime(series).astype('str', format=output_format)

        def years_between(self, date_format=None):
            series = self.series
            raise NotImplementedError("Not implemented yet see https://github.com/rapidsai/cudf/issues/1041")
            # return cudf.to_datetime(series).astype('str', format=date_format) - datetime.now().date()

    return CUDFFunctions(self)


Series.functions = property(functions)
