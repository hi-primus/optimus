# These function can return and Column Expression or a list of columns expression
# Must return None if the data type can not be handle

# from dask_cudf.core import DataFrame as DaskCUDFDataFrame


import random
import string

import cudf

from optimus.engines.base.commons.functions import to_float_cudf, to_integer_cudf
from optimus.engines.base.functions import Functions
from optimus.helpers.core import val_to_list


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


def create_func(_df, input_cols, output_cols, func, args=None):
    #     return create_apply_row(_df, input_cols, output_cols,func(float(f"""{output_cols[0]}_value_"""),{str(*args)}))
    if args is not None:
        args = str(*args)
        _func = f"""{func}(float({input_cols[0]}_value_),{args})"""
    else:
        _func = f"""{func}(float({input_cols[0]}_value_))"""

    return create_apply_row(_df, input_cols, output_cols, _func)


class DaskCUDFFunctions(Functions):

    def _to_float(self, series, *args):
        return series.map_partitions(to_float_cudf, meta=float)

    def _to_integer(self, series, *args):
        return series.map_partitions(to_integer_cudf, meta=int)

    def to_float(self, series):
        return to_float_cudf(series)

    def to_integer(self, series):
        return to_integer_cudf(series)

    def to_string(self, series):
        print(series.dtype)
        return series.astype(str)

    def count_zeros(self, *args):
        series = self.series
        return int((series.to_float().values == 0).sum())

    def kurtosis(self):
        series = self.series
        return series.map_partitions(lambda _series: _series.kurtosis())

    def skew(self, series):
        # series = self.series
        return series.map_partitions(lambda _series: _series.skew())

    def sqrt(self):
        series = self.series
        return series.map_partitions(lambda _series: _series.sqrt())

    def exp(self):
        series = self.series
        return series.map_partitions(lambda _series: _series.exp())

    def log(self):
        series = self.series
        return series.map_partitions(lambda _series: _series.log() / cudf.log(10))

    def radians(self):
        series = self.series
        return cudf.radians(series.to_float())

    def degrees(self):
        series = self.series
        return cudf.degrees(series.to_float())

    def ln(self):
        series = self.series
        return series.map_partitions(lambda _series: _series.log())

    def ceil(self):
        series = self.series
        return series.map_partitions(lambda _series: _series.ceil())

    def floor(self):
        series = self.series
        return series.map_partitions(lambda _series: _series.floor())

    def sin(self):
        series = self.series
        return series.map_partitions(lambda _series: _series.sin())

    def cos(self):
        series = self.series
        return series.map_partitions(lambda _series: _series.cos())

    def tan(self):
        series = self.series
        return series.map_partitions(lambda _series: _series.tan())

    def asin(self):
        series = self.series
        return series.map_partitions(lambda _series: _series.asin())

    def acos(self):
        series = self.series
        return series.map_partitions(lambda _series: _series.acos())

    def atan(self):
        series = self.series
        return series.map_partitions(lambda _series: _series.atan())

    def sinh(self):
        series = self.series
        return 1 / 2 * (self.exp() - self.exp())

    def cosh(self):
        series = self.series
        return 1 / 2 * (self.exp() + self.exp())

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

    def cut(self, bins):
        series = self.series
        return series.to_float(series).cut(bins, include_lowest=True, labels=list(range(bins)))

    # def remove_special_chars(self):
    #     series = self.series
    #     return series.astype(str).str.replace('[^A-Za-z0-9]+', '')

    def remove_accents(self, series):
        # str.decode return a float column. We are forcing to return a string again
        return series.astype(str).str.normalize_characters()

    def remove_special_chars(self, series):
        # See https://github.com/rapidsai/cudf/issues/5520
        return series.astype(str).str.replace_non_alphanumns(replacement_char='')

    def date_format(self, series, current_format=None, output_format=None):
        return cudf.to_datetime(series).astype('str', format=output_format)

    def years_between(self, date_format=None):
        raise NotImplementedError("Not implemented yet see https://github.com/rapidsai/cudf/issues/1041")
        # return cudf.to_datetime(series).astype('str', format=date_format) - datetime.now().date()

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
