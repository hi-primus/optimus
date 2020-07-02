import fastnumbers
import numpy as np
import pandas as pd


# From a top point of view we organize Optimus separating the functions in dataframes and dask engines.
# Some functions are commons for pandas and dask.

def to_integer(value, *args):
    return fastnumbers.fast_int(value, default=0)


def to_float(value, *args):
    # fastnumbers.fast_float(x) if x is not None else None
    return fastnumbers.fast_float(value, default=np.nan)


def to_datetime(value, format):
    return pd.to_datetime(value, format=format, errors="coerce")


def to_datetime_cudf(value, format):
    return pd.to_datetime(value, format=format, errors="coerce")


def to_integer_cudf(value, *args):
    import cudf
    series_string = value.astype(str)
    # See https://github.com/rapidsai/cudf/issues/5345
    series = cudf.Series(series_string.str.stoi()).fillna(False)
    series[
        ~cudf.Series(cudf.core.column.string.cpp_is_integer(series_string._column)).fillna(False)] = None
    return series


def to_float_cudf(value, *args):
    import cudf
    series_string = value.astype(str)
    # See https://github.com/rapidsai/cudf/issues/5345
    series = cudf.Series(series_string.str.stof()).fillna(False)
    series[
        ~cudf.Series(cudf.core.column.string.cpp_is_float(series_string._column)).fillna(False)] = None
    return series

# def _cast_date(value, format="YYYY-MM-DD"):
#     if pd.isnull(value):
#         return np.nan
#     else:
#         try:
#             # return pendulum.parse(value)
#             # return pendulum.from_format(value, format)
#             # return dparse(value)
#
#             return value
#         except:
#             return value

# def _cast_bool(value):
#     if pd.isnull(value):
#         return np.nan
#     else:
#         return bool(value)
#

# cast_func = {'int': _cast_int, 'decimal': _cast_float, "string": _cast_str, 'bool': _cast_bool,
#              'date': _cast_date, "object": _cast_object, "missing": _cast_str}
