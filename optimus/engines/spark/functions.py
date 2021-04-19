import datetime
import itertools

from pyspark.sql import functions as F

from optimus.engines.base.functions import Functions
from optimus.helpers.check import is_column_a
from optimus.helpers.core import val_to_list
from optimus.helpers.functions import create_buckets
# These function can return a Column Expression or a list of columns expression
# Must return None if the data type can not be handle
from optimus.infer import is_numeric, regex_full_url


class SparkFunctions(Functions):

    @staticmethod
    def replace_chars(col, search, replace_by):
        return F.col(col).cast("float")

    @staticmethod
    def replace_words(col, search, replace_by):
        return F.col(col).cast("float")

    @staticmethod
    def replace_full(col, search, replace_by):
        _search = val_to_list(search)

        if _input_col != output_col:
            _df = _df.cols.copy(_input_col, _output_col)

        return _df.replace(_search, _replace_by, _output_col)

    def to_string(self, col_name):
        return F.col(col_name).cast("string")

    def to_string_accessor(self, series):
        return self.to_string(series)

    def to_float(self, col_name):
        return F.col(col_name).cast("float")

    def _to_float(self, col_name):
        return F.col(col_name).cast("float")

    def to_integer(self, col_name):
        return F.col(col_name).cast("int")

    @staticmethod
    def lower(col_name):
        return F.lower(F.col(col_name))

    @staticmethod
    def upper(col_name):
        return F.upper(F.col(col_name))

    @staticmethod
    def title(col):
        pass

    @staticmethod
    def pad(series, width, side, fillchar=""):
        pass

    @staticmethod
    def extract(series, regex):
        pass

    @staticmethod
    def slice(series, start, stop, step):
        pass

    @staticmethod
    def proper(col):
        pass

    @staticmethod
    def trim(col):
        pass

    @staticmethod
    def count_zeros(series, *args):
        pass

    @staticmethod
    def skew(col):
        pass

    @staticmethod
    def clip(series, lower_bound, upper_bound):
        pass

    @staticmethod
    def cut(series, bins):
        pass

    @staticmethod
    def sqrt(col):
        pass

    @staticmethod
    def radians(col):
        pass

    @staticmethod
    def degrees(col):
        pass

    @staticmethod
    def ln(col):
        pass

    @staticmethod
    def log(col, base):
        pass

    @staticmethod
    def ceil(col):
        pass

    def sin(self, series):
        return F.sin(self._to_float(series))

    def cos(self, series):
        return F.cos(self._to_float(series))

    def tan(self, series):
        return F.tan(self._to_float(series))

    def asin(self, series):
        return F.asin(self._to_float(series))

    def acos(self, series):
        return F.acos(self._to_float(series))

    def atan(self, series):
        return F.atan(self._to_float(series))

    def sinh(self, series):
        return F.sinh(self._to_float(series))

    def cosh(self, series):
        return F.cosh(self._to_float(series))

    def tanh(self, series):
        return F.tanh(self._to_float(series))

    def asinh(self, series):
        return F.log10(series + F.sqrt(F.pow(series, F.lit(2)) + F.lit(1)))

    def acosh(self, series):
        return F.log10(series + F.sqrt(F.pow(series, F.lit(2)) - F.lit(1)))

    def atanh(self, series):
        return 1 / 2 * F.log10(F.lit(1) + series / F.lit(1) - series)

    @staticmethod
    def replace_chars(series, search, replace_by):
        pass

    @staticmethod
    def date_format(self, current_format=None, output_format=None):
        pass

    @staticmethod
    def years_between(self, date_format=None):
        pass

    @staticmethod
    def min(col_name, *args):
        return F.min(col_name)

    @staticmethod
    def max(col_name, *args):
        return F.max(col_name)

    # @staticmethod
    # def mode(col_name):
    #     return F.mode(col_name)

    @staticmethod
    def stddev(col_name, *args):
        return F.stddev(col_name)

    @staticmethod
    def kurtosis(col_name, *args):
        return F.kurtosis(col_name)

    @staticmethod
    def mean(col_name, *args):
        return F.mean(col_name)

    @staticmethod
    def skewness(col_name, *args):
        return F.skewness(col_name)

    @staticmethod
    def sum(col_name, *args):
        return F.sum(col_name)

    @staticmethod
    def variance(col_name, *args):
        return F.variance(col_name)

    @staticmethod
    def match_nulls_strings(col_name):
        return F.isnan(col_name) | F.isnull(col_name) | ((F.lower(F.col(col_name))) == "nan")

    @staticmethod
    def match_nulls_integers(col_name):
        return F.isnan(col_name) | F.isnull(col_name)

    @staticmethod
    def match_nan(col_name):
        return F.isnan(col_name)

    @staticmethod
    def match_null(col_name):
        return F.isnull(col_name)

    @staticmethod
    def na_agg_integer(col_name):
        return F.count(F.when(SparkFunctions.match_nulls_integers(col_name), col_name))

    @staticmethod
    def na_agg(col_name):
        return F.count(F.when(SparkFunctions.match_null(col_name), col_name))

    @staticmethod
    def zeros_agg(col_name):
        return F.count(F.when(F.col(col_name) == 0, col_name))

    @staticmethod
    def count_uniques_agg(col_name, estimate=True, df=None):
        if estimate is True:
            result = F.approx_count_distinct(col_name)
        else:
            result = F.countDistinct(col_name)
        return result

    @staticmethod
    def range_agg(col_name):
        return F.create_map(F.lit("min"), F.min(col_name), F.lit("max"), F.max(col_name))

    @staticmethod
    def hist_agg(col_name, buckets, min_max, df=None):
        """
        Create a columns expression to calculate a column histogram
        :param col_name:
        :param df:
        :param buckets:
        :param min_max: Min and max vaule neccesary to calculate the buckets
        :param dtype: Column datatype to calculate the related histogram. Int, String and Dates return different histograms

        :return:
        """

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
                _min_max = \
                    df.data.agg(F.min(col_name).alias("min"), F.max(col_name).alias("max")).toPandas().reset_index(
                        drop=True).to_dict("records")[0]

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


        dtype = df.cols.dtypes(col_name)
        if dtype is not None:
            col_dtype = dtype[col_name]
            if col_dtype == "int" or col_dtype == "decimal":
                exprs = hist_numeric(min_max, buckets)
            elif col_dtype == "string":
                exprs = hist_string(buckets)
            elif col_dtype == "date":
                exprs = hist_date()
            else:
                exprs = None
        else:
            if is_column_a(df, col_name, df.constants.NUMERIC_TYPES):
                exprs = hist_numeric(min_max, buckets)

            elif is_column_a(df, col_name, "str"):
                exprs = hist_string(buckets)

            elif is_column_a(df, col_name, "date") or is_column_a(df, col_name, "timestamp"):
                exprs = hist_date()
            else:
                exprs = None

        return exprs

    # def to_string(self, col):
    #     return F.col(col).cast("str")

    def count_na(self, col_name, df=None):
        # print("col_name",col_name, self)
        # If type column is Struct parse to String. isnan/isNull can not handle Structure/Boolean
        # if is_column_a(df, col_name, ["struct", "boolean"]):
        #     df = df.cols.cast(col_name, "string")

        # Select the nan/null rows depending of the columns data type
        # df = self.root

        # If numeric
        if is_column_a(df, col_name, df.constants.NUMERIC_TYPES):
            expr = F.count(F.when(SparkFunctions.match_nulls_integers(col_name), col_name))
        # If string. Include 'nan' string
        elif is_column_a(df, col_name, df.constants.STRING_TYPES):
            expr = F.count(
                F.when(SparkFunctions.match_nulls_strings(col_name), col_name))
            # print("Including 'nan' as Null in processing string type column '{}'".format(col_name))
        else:
            expr = F.count(F.when(SparkFunctions.match_null(col_name), col_name))

        return expr

    @staticmethod
    def percentile(col_name, df, values, relative_error):
        """
        Return the percentile of a spark
        :param col_name:  '*', list of columns names or a single column name.
        :param df:
        :param values: list of percentiles to be calculated
        :param relative_error:  If set to zero, the exact percentiles are computed, which could be very expensive. 0 to 1 accepted
        :return: percentiles per columns
        """

        # Make sure values are double
        if values is None:
            values = [0.05, 0.25, 0.5, 0.75, 0.95]

        values = val_to_list(values)
        values = list(map(str, values))

        if is_column_a(df, col_name, df.constants.NUMERIC_TYPES):
            # Get percentiles

            p = F.expr("percentile_approx(`{COLUMN}`, array({VALUES}), {ERROR})".format(COLUMN=col_name,
                                                                                        VALUES=" , ".join(values),
                                                                                        ERROR=relative_error))

            # Zip the arrays
            expr = [[F.lit(v), p.getItem(i)] for i, v in enumerate(values)]
            expr = F.create_map(*list(itertools.chain(*expr)))

        else:
            expr = None
        # print(expr)
        return expr

