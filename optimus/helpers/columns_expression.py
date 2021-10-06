import datetime
import itertools

from pyspark.sql import functions as F

from optimus.infer import is_numeric
from optimus.infer_spark import PYSPARK_NUMERIC_TYPES, PYSPARK_STRING_TYPES
from optimus.helpers.check import is_column_a
from optimus.helpers.core import val_to_list
from optimus.helpers.functions import create_buckets


# These function can return and Column Expression or a list of columns expression
# Must return None if the data type can not be handle


def match_nulls_strings(col_name):
    return F.isnan(col_name) | F.isnull(col_name) | ((F.lower(F.col(col_name))) == "nan")


def match_nulls_integers(col_name):
    return F.isnan(col_name) | F.isnull(col_name)


def match_nan(col_name):
    return F.isnan(col_name)


def match_null(col_name):
    return F.isnull(col_name)


def na_agg_integer(col_name):
    return F.count(F.when(match_nulls_integers(col_name), col_name))


def na_agg(col_name):
    return F.count(F.when(match_null(col_name), col_name))


def zeros_agg(col_name):
    return F.count(F.when(F.col(col_name) == 0, col_name))


def count_uniques_agg(col_name, estimate=False):
    if estimate is True:
        result = F.approx_count_distinct(col_name)
    else:
        result = F.countDistinct(col_name)
    return result


def range_agg(col_name):
    return F.create_map(F.lit("min"), F.min(col_name), F.lit("max"), F.max(col_name))


def hist_agg(col_name, df, buckets, min_max=None, data_type=None):
    """
    Create a columns expression to calculate a column histogram
    :param col_name:
    :param df:
    :param buckets:
    :param min_max: Min and max vaule neccesary to calculate the buckets
    :param data_type: Column datatype to calculate the related histogram. Int, String and Dates return different histograms

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

    if data_type is not None:
        col_data_type = data_type[col_name]["data_type"]
        if col_data_type == "int" or col_data_type == "float":
            exprs = hist_numeric(min_max, buckets)
        elif col_data_type == "string":
            exprs = hist_string(buckets)
        elif col_data_type == "date":
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


def count_na_agg(col_name, df):
    # If type column is Struct parse to String. isnan/isNull can not handle Structure/Boolean
    # if is_column_a(df, col_name, ["struct", "boolean"]):
    #     df = df.cols.cast(col_name, "string")

    # Select the nan/null rows depending of the columns data type
    # If numeric
    if is_column_a(df, col_name, PYSPARK_NUMERIC_TYPES):
        expr = F.count(F.when(match_nulls_integers(col_name), col_name))
    # If string. Include 'nan' string
    elif is_column_a(df, col_name, PYSPARK_STRING_TYPES):
        expr = F.count(
            F.when(match_nulls_strings(col_name), col_name))
        # print("Including 'nan' as Null in processing string type column '{}'".format(col_name))
    else:
        expr = F.count(F.when(match_null(col_name), col_name))

    return expr


def percentile_(col_name, df, values, relative_error):
    """
    Return the percentile of a dataframe
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

    if is_column_a(df, col_name, PYSPARK_NUMERIC_TYPES):
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
