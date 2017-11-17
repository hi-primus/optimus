from pyspark.sql.functions import lower, col, trim, regexp_replace


def lower_case(colName):
    def inner(df):
        return df.withColumn(colName, lower(col(colName)))
    return inner


def trim_col(colName):
    def inner(df):
        return df.withColumn(colName, trim(col(colName)))
    return inner


def remove_special_chars(colName):
    def inner(df):
        special_chars = ["!", "&", "."]
        regexp = "|".join(['\{0}'.format(i) for i in special_chars])
        return df.withColumn(colName, regexp_replace(colName, regexp, ""))
    return inner
