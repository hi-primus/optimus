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


def remove_chars(colName, chars):
    def inner(df):
        regexp = "|".join('\{0}'.format(i) for i in chars)
        return df.withColumn(colName, regexp_replace(colName, regexp, ""))
    return inner

def multi_remove_chars(colNames, chars):
    def inner(df):
        return reduce(
            lambda memo_df, col_name: remove_chars(col_name, chars)(memo_df),
            colNames,
            df
        )
    return inner

