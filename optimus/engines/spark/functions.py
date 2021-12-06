import databricks.koalas as ks
import numpy as np
# These function can return a Column Expression or a list of columns expression
# Must return None if the data type can not be handle
import pandas as pd
from pyspark.sql import functions as F

from optimus.engines.base.commons.functions import word_tokenize
from optimus.engines.base.dataframe.functions import DataFrameBaseFunctions
from optimus.engines.base.pandas.functions import PandasBaseFunctions
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.raiseit import RaiseIt


class SparkFunctions(PandasBaseFunctions, DataFrameBaseFunctions):
    _engine = ks

    def _to_float(self, series):
        """
        Converts a series values to floats
        """
        return series.astype("float")

    def _to_integer(self, series, default=0):
        """
        Converts a series values to integers
        """
        return series.astype("integer")

    def _to_string(self, series):
        """
        Converts a series values to strings
        """
        return series.astype("str")

    def _to_boolean(self, series):
        """
        Converts a series values to bool
        """
        return series.astype("bool")

    def hist(col_name, df, buckets, min_max=None, dtype=None):
        """
        Create a columns expression to calculate a column histogram
        :param col_name:
        :param df:
        :param buckets:
        :param min_max: Min and max value necessary to calculate the buckets
        :param dtype: Column datatype to calculate the related histogram. Int, String and Dates return different histograms
        :return:
        """
        PYSPARK_NUMERIC_TYPES = ["byte", "short", "big", "int", "double", "float"]
        def is_column_a(df, column, dtypes):
            """
            Check if column match a list of data types
            :param df: dataframe
            :param column: column to be compared with
            :param dtypes: types to be checked
            :return:
            """
            column = val_to_list(column)

            if len(column) > 1:
                RaiseIt.length_error(column, 1)

            data_type = tuple(val_to_list(parse_spark_dtypes(dtypes)))
            column = one_list_to_val(column)

            # Filter columns by data type
            return isinstance(df.schema[column].dataType, data_type)

        def create_exprs(_input_col, _buckets, _func):
            def count_exprs(_exprs):
                return F.sum(F.when(_exprs, 1).otherwise(0))

            _exprs = []
            for i, b in enumerate(_buckets):
                lower = b["lower"]
                upper = b["upper"]

                if is_numeric(lower):
                    lower = round(lower, 2)

                if is_numeric(upper):
                    upper = round(upper, 2)

                if len(_buckets) == 1:
                    count = count_exprs(
                        (_func(_input_col) == lower))
                else:
                    if i == len(_buckets):
                        count = count_exprs(
                            (_func(_input_col) > lower) & (_func(_input_col) <= upper))
                    else:
                        count = count_exprs(
                            (_func(_input_col) >= lower) & (_func(_input_col) < upper))

                info = F.create_map(F.lit("count"), count.cast("int"), F.lit("lower"), F.lit(lower), F.lit("upper"),
                                    F.lit(upper)).alias(
                    "hist_agg" + "_" + _input_col + "_" + str(b["bucket"]))
                _exprs.append(info)
            _exprs = F.array(*_exprs).alias("hist" + _input_col)
            return _exprs

        def hist_numeric(_min_max, _buckets):
            if _min_max is None:
                _min_max = df.agg(F.min(col_name).alias("min"), F.max(col_name).alias("max")).to_dict()[0]

            if _min_max["min"] is not None and _min_max["max"] is not None:
                _buckets = create_buckets(_min_max["min"], _min_max["max"], _buckets)
                _exprs = create_exprs(col_name, _buckets, F.col)
            else:
                _exprs = None

            return _exprs

        def hist_string(_buckets):
            _buckets = create_buckets(0, 50, _buckets)
            func = F.length
            return create_exprs(col_name, _buckets, func)

        def hist_date():
            now = datetime.datetime.now()
            current_year = now.year
            oldest_year = 1950

            # Year
            _buckets = create_buckets(oldest_year, current_year, current_year - oldest_year)
            func = F.year
            year = create_exprs(col_name, _buckets, func)

            # Month
            _buckets = create_buckets(1, 12, 11)
            func = F.month
            month = create_exprs(col_name, _buckets, func)

            # Day
            _buckets = create_buckets(1, 31, 31)
            func = F.dayofweek
            day = create_exprs(col_name, _buckets, func)

            # Hour
            _buckets = create_buckets(0, 23, 23)
            func = F.hour
            hour = create_exprs(col_name, _buckets, func)

            # Min
            _buckets = create_buckets(0, 60, 60)
            func = F.minute
            minutes = create_exprs(col_name, _buckets, func)

            # Second
            _buckets = create_buckets(0, 60, 60)
            func = F.second
            second = create_exprs(col_name, _buckets, func)

            exprs = F.create_map(F.lit("years"), year, F.lit("months"), month, F.lit("weekdays"), day,
                                 F.lit("hours"), hour, F.lit("minutes"), minutes, F.lit("seconds"), second)

            return exprs

        if dtype is not None:
            col_dtype = dtype[col_name]["dtype"]
            if col_dtype == "int" or col_dtype == "decimal":
                exprs = hist_numeric(min_max, buckets)
            elif col_dtype == "string":
                exprs = hist_string(buckets)
            elif col_dtype == "date":
                exprs = hist_date()
            else:
                exprs = None
        else:
            if is_column_a(df, col_name, PYSPARK_NUMERIC_TYPES):
                exprs = hist_numeric(min_max, buckets)

            elif is_column_a(df, col_name, "str"):
                exprs = hist_string(buckets)

            elif is_column_a(df, col_name, "date") or is_column_a(df, col_name, "timestamp"):
                exprs = hist_date()
            else:
                exprs = None

        return exprs

    def create_exprs(_input_col, _buckets, _func):
        def count_exprs(_exprs):
            return F.sum(F.when(_exprs, 1).otherwise(0))

        _exprs = []
        for i, b in enumerate(_buckets):
            lower = b["lower"]
            upper = b["upper"]

            if is_numeric(lower):
                lower = round(lower, 2)

            if is_numeric(upper):
                upper = round(upper, 2)

            if len(_buckets) == 1:
                count = count_exprs(
                    (_func(_input_col) == lower))
            else:
                if i == len(_buckets):
                    count = count_exprs(
                        (_func(_input_col) > lower) & (_func(_input_col) <= upper))
                else:
                    count = count_exprs(
                        (_func(_input_col) >= lower) & (_func(_input_col) < upper))

            info = F.create_map(F.lit("count"), count.cast("int"), F.lit("lower"), F.lit(lower), F.lit("upper"),
                                F.lit(upper)).alias(
                "hist_agg" + "_" + _input_col + "_" + str(b["bucket"]))
            _exprs.append(info)
        _exprs = F.array(*_exprs).alias("hist" + _input_col)
        return _exprs

    @staticmethod
    def dask_to_compatible(dfd):
        from optimus.helpers.converter import dask_dataframe_to_pandas
        return ks.from_pandas(dask_dataframe_to_pandas(dfd))

    @staticmethod
    def new_df(*args, **kwargs):
        return ks.from_pandas(pd.DataFrame(*args, **kwargs))

    @staticmethod
    def df_concat(df_list):
        return ks.concat(df_list, axis=0, ignore_index=True)

    def word_tokenize(self, series):
        return self.to_string(series).map(word_tokenize, na_action=None)

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

    def normalize_chars(self, series):
        return series.str.normalize("NFKD").str.encode('ascii', errors='ignore').str.decode('utf8')

    def format_date(self, series, current_format=None, output_format=None):
        return ks.to_datetime(series, format=current_format,
                              errors="coerce").dt.strftime(output_format).reset_index(drop=True)

    def time_between(self, series, value=None, date_format=None):

        value_date_format = date_format

        if is_list_or_tuple(date_format) and len(date_format) == 2:
            date_format, value_date_format = date_format

        if is_list_or_tuple(value) and len(value) == 2:
            value, value_date_format = value

        date = pd.to_datetime(series, format=date_format, errors="coerce")
        value = pd.Timestamp.now() if value is None else pd.to_datetime(value, format=value_date_format,
                                                                        errors="coerce")

        return (value - date)
