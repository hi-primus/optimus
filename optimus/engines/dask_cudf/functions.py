# These function can return and Column Expression or a list of columns expression
# Must return None if the data type can not be handle

# from dask_cudf.core import DataFrame as DaskCUDFDataFrame


import cudf

from optimus.engines.base.functions import Functions
from dask_cudf import Series


import random
import string
def get_random_string(length):
    # Random string with the combination of lower and upper case
    letters = string.ascii_letters
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str


def create_apply_row(df, input_cols, output_cols, func):
    # Create dict input cols
    input_temp_names = [get_random_string(8) for _ in range(len(input_cols))]

    _output_cols = ({output_col: np.float64 for output_col in output_cols})
    _input_cols = (dict(zip(input_cols, input_temp_names)))

    input_values = [x + "_value_" for x in input_cols]

    if len(input_temp_names) == 1:
        _enumerate = f"""enumerate({",".join(input_temp_names)})"""
    else:
        _enumerate = f"""enumerate(zip({",".join(input_temp_names)}))"""

    _func = (f"""
def __func({",".join(input_temp_names)},{",".join(output_cols)}):
    for i,({",".join(input_values)}) in {_enumerate}:
        {output_cols[0]}[i]={func}            
    """)
    exec(_func, globals())

    return df.apply_rows(__func, incols=_input_cols, outcols=_output_cols)


import numpy as np
import math


def create_func(_df, input_cols, output_cols, func, args=None):
    #     return create_apply_row(_df, input_cols, output_cols,func(float(f"""{output_cols[0]}_value_"""),{str(*args)}))
    if args is not None:
        args = str(*args)
        _func = f"""{func}(float({input_cols[0]}_value_),{args})"""
    else:
        _func = f"""{func}(float({input_cols[0]}_value_))"""

    return create_apply_row(_df, input_cols, output_cols, _func)

def functions(self):
    class DASKCUDFFunctions(Functions):
        def __init__(self, df):
            super(DASKCUDFFunctions, self).__init__(df)

        def kurtosis(self):
            series = self.series
            raise NotImplementedError("Not implemented yet")

        def skew(self):
            series = self.series
            raise NotImplementedError("Not implemented yet")

        def exp(self):
            series = self.series
            return cudf.exp(series.ext.to_float())

            # return cudf.pow(1 / series.ext.to_float())

        def pow(_df, input_cols, output_cols, n):
            return create_func(_df, input_cols, output_cols, "math.pow", args=(n,))

        def sqrt(self, input_cols, output_cols):
            series = self.series
            return create_func(series, input_cols, output_cols, "math.sqrt")

        def log(_df, input_cols, output_cols):
            return create_func(_df, input_cols, output_cols, "math.log")


        def sqrt(self):
            series = self.series

            # TODO: WIP apply the function only to
            # def func(x, out):
            #     for i, billingId in enumerate(x):
            #         out[i] = math.sqrt(float(billingId))
            #
            # df.apply_rows(func,
            #                incols={'price': 'x'},
            #                outcols={'out': np.float64}).compute()

            return cudf.pow(1 / series.ext.to_float())

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
            return 1 / 2 * (cudf.exp(series) - cudf.exp(series))

        def cosh(self):
            series = self.series
            return 1 / 2 * (cudf.exp(series) + cudf.exp(series))

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
            raise NotImplementedError("Not implemented yet see https://github.com/rapidsai/cudf/issues/1041")
            # return cudf.to_datetime(series).astype('str', format=date_format) - datetime.now().date()

    return DASKCUDFFunctions(self)


Series.functions = property(functions)
