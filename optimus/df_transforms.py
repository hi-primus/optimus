from pyspark.sql.functions import lower, col, trim, regexp_replace
from functools import reduce


def reorder_columns(*colNames):
    def inner(df):
        return df.select(*colNames)
    return inner


def lower_case(colName):
    def inner(df):
        return df.withColumn(colName, lower(col(colName)))
    return inner


def trim_col(colName):
    def inner(df):
        return df.withColumn(colName, trim(col(colName)))
    return inner


def __remove_chars(col_name, removed_chars):
    def inner(df):
        regexp = "|".join('\{0}'.format(i) for i in removed_chars)
        return df.withColumn(col_name, regexp_replace(col_name, regexp, ""))
    return inner


def remove_chars(col_names, removed_chars):
    def inner(df):
        return reduce(
            lambda memo_df, col_name: __remove_chars(col_name, removed_chars)(memo_df),
            col_names,
            df
        )
    return inner

