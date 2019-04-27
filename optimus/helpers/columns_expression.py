from pyspark.sql import functions as F


def match_nulls_strings(col_name):
    return F.isnan(col_name) | F.isnull(col_name) | ((F.lower(F.col(col_name))) == "nan")


def match_nulls_integers(col_name):
    return F.isnan(col_name) | F.isnull(col_name)


def match_nan(col_name):
    return F.isnan(col_name)


def match_null(col_name):
    return F.isnull(col_name)
