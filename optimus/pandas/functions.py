# These function can return and Column Expression or a list of columns expression
# Must return None if the data type can not be handle
import pandas as pd


def functions(self):
    class Functions:
        @staticmethod
        def min():
            return min

        @staticmethod
        def max():
            return max

        @staticmethod
        def stddev():
            return 'std'

        @staticmethod
        def kurtosis():
            return 'kurtosis'

        @staticmethod
        def mean():
            return 'mean'

        @staticmethod
        def skewness():
            return 'skewness'

        @staticmethod
        def sum():
            return sum

        @staticmethod
        def variance():
            return 'variance'

        # @staticmethod
        # def match_nulls_strings(col_name):
        #     return F.isnan(col_name) | F.isnull(col_name) | ((F.lower(F.col(col_name))) == "nan")
        #
        # @staticmethod
        # def match_nulls_integers(col_name):
        #     return F.isnan(col_name) | F.isnull(col_name)
        #
        # @staticmethod
        # def match_nan(col_name):
        #     return F.isnan(col_name)
        #
        # @staticmethod
        # def match_null(col_name):
        #     return F.isnull(col_name)
        #
        # @staticmethod
        # def na_agg_integer(col_name):
        #     return F.count(F.when(Functions.match_nulls_integers(col_name), col_name))
        #
        # @staticmethod
        # def na_agg(col_name):
        #     return F.count(F.when(Functions.match_null(col_name), col_name))
        #
        @staticmethod
        def zeros_agg(col_name):
            return (col_name.values == 0).sum()



        @staticmethod
        def count_uniques_agg(col_name: pd.DataFrame, estimate=True):
            """

            :param col_name:
            :param estimate:
            :return:
            """

            return col_name.value_counts().ext.to_dict()

        # @staticmethod
        # def range_agg(col_name):
        #     return F.create_map(F.lit("min"), F.min(col_name), F.lit("max"), F.max(col_name))
        #
        # @staticmethod
        # def hist_agg(col_name, df, buckets, min_max=None, dtype=None):
        #     """
        #     Create a columns expression to calculate a column histogram
        #     :param col_name:
        #     :param df:
        #     :param buckets:
        #     :param min_max: Min and max vaule neccesary to calculate the buckets
        #     :param dtype: Column datatype to calculate the related histogram. Int, String and Dates return different histograms
        #
        #     :return:
        #     """
        #
        #     def create_exprs(_input_col, _buckets, _func):
        #         def count_exprs(_exprs):
        #             return F.sum(F.when(_exprs, 1).otherwise(0))
        #
        #         _exprs = []
        #         for i, b in enumerate(_buckets):
        #             lower = b["lower"]
        #             upper = b["upper"]
        #
        #             if is_numeric(lower):
        #                 lower = round(lower, 2)
        #
        #             if is_numeric(upper):
        #                 upper = round(upper, 2)
        #
        #             if len(_buckets) == 1:
        #                 count = count_exprs(
        #                     (_func(_input_col) == lower))
        #             else:
        #                 if i == len(_buckets):
        #                     count = count_exprs(
        #                         (_func(_input_col) > lower) & (_func(_input_col) <= upper))
        #                 else:
        #                     count = count_exprs(
        #                         (_func(_input_col) >= lower) & (_func(_input_col) < upper))
        #
        #             info = F.create_map(F.lit("count"), count.cast("int"), F.lit("lower"), F.lit(lower), F.lit("upper"),
        #                                 F.lit(upper)).alias(
        #                 "hist_agg" + "_" + _input_col + "_" + str(b["bucket"]))
        #             _exprs.append(info)
        #         _exprs = F.array(*_exprs).alias("hist" + _input_col)
        #         return _exprs
        #
        #     def hist_numeric(_min_max, _buckets):
        #         if _min_max is None:
        #             _min_max = df.agg(F.min(col_name).alias("min"), F.max(col_name).alias("max")).ext.to_dict()[0]
        #
        #         if _min_max["min"] is not None and _min_max["max"] is not None:
        #             _buckets = create_buckets(_min_max["min"], _min_max["max"], _buckets)
        #             _exprs = create_exprs(col_name, _buckets, F.col)
        #         else:
        #             _exprs = None
        #
        #         return _exprs
        #
        #     def hist_string(_buckets):
        #         _buckets = create_buckets(0, 50, _buckets)
        #         func = F.length
        #         return create_exprs(col_name, _buckets, func)
        #
        #     def hist_date():
        #         now = datetime.datetime.now()
        #         current_year = now.year
        #         oldest_year = 1950
        #
        #         # Year
        #         _buckets = create_buckets(oldest_year, current_year, current_year - oldest_year)
        #         func = F.year
        #         year = create_exprs(col_name, _buckets, func)
        #
        #         # Month
        #         _buckets = create_buckets(1, 12, 11)
        #         func = F.month
        #         month = create_exprs(col_name, _buckets, func)
        #
        #         # Day
        #         _buckets = create_buckets(1, 31, 31)
        #         func = F.dayofweek
        #         day = create_exprs(col_name, _buckets, func)
        #
        #         # Hour
        #         _buckets = create_buckets(0, 23, 23)
        #         func = F.hour
        #         hour = create_exprs(col_name, _buckets, func)
        #
        #         # Min
        #         _buckets = create_buckets(0, 60, 60)
        #         func = F.minute
        #         minutes = create_exprs(col_name, _buckets, func)
        #
        #         # Second
        #         _buckets = create_buckets(0, 60, 60)
        #         func = F.second
        #         second = create_exprs(col_name, _buckets, func)
        #
        #         exprs = F.create_map(F.lit("years"), year, F.lit("months"), month, F.lit("weekdays"), day,
        #                              F.lit("hours"), hour, F.lit("minutes"), minutes, F.lit("seconds"), second)
        #
        #         return exprs
        #
        #     if dtype is not None:
        #         col_dtype = dtype[col_name]["dtype"]
        #         if col_dtype == "int" or col_dtype == "decimal":
        #             exprs = hist_numeric(min_max, buckets)
        #         elif col_dtype == "string":
        #             exprs = hist_string(buckets)
        #         elif col_dtype == "date":
        #             exprs = hist_date()
        #         else:
        #             exprs = None
        #     else:
        #         if is_column_a(df, col_name, PYSPARK_NUMERIC_TYPES):
        #             exprs = hist_numeric(min_max, buckets)
        #
        #         elif is_column_a(df, col_name, "str"):
        #             exprs = hist_string(buckets)
        #
        #         elif is_column_a(df, col_name, "date") or is_column_a(df, col_name, "timestamp"):
        #             exprs = hist_date()
        #         else:
        #             exprs = None
        #
        #     return exprs
        #
        # @staticmethod
        # def count_na_agg(col_name, df):
        #     # If type column is Struct parse to String. isnan/isNull can not handle Structure/Boolean
        #     # if is_column_a(df, col_name, ["struct", "boolean"]):
        #     #     df = df.cols.cast(col_name, "string")
        #
        #     # Select the nan/null rows depending of the columns data type
        #     # If numeric
        #     if is_column_a(df, col_name, PYSPARK_NUMERIC_TYPES):
        #         expr = F.count(F.when(Functions.match_nulls_integers(col_name), col_name))
        #     # If string. Include 'nan' string
        #     elif is_column_a(df, col_name, PYSPARK_STRING_TYPES):
        #         expr = F.count(
        #             F.when(Functions.match_nulls_strings(col_name), col_name))
        #         # print("Including 'nan' as Null in processing string type column '{}'".format(col_name))
        #     else:
        #         expr = F.count(F.when(Functions.match_null(col_name), col_name))
        #
        #     return expr
        #
        @staticmethod
        def percentile_agg(col_name: pd.Series, values=None):
            if values is None:
                values = [0.05, 0.25, 0.5, 0.75, 0.95]

            return col_name.quantile(values).ext.to_dict()

    return Functions()


pd.DataFrame.functions = property(functions)
