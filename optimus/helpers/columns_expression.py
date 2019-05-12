from pyspark.sql import functions as F


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
