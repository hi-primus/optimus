# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.1.1
#   kernel_info:
#     name: python3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# Hi, this notebook will show you almost all the columns operation availables in Optimus. For row operation, IO, ML and DL please go to the examples folder in the repo

# %load_ext autoreload
# %autoreload 2

import sys

sys.path.append("..")

# ## Columns Operations
#
# In this notebook you can see a detailed overview ok all the columns operations available in Optimus. You can access the operation via df.cols.

from optimus import Optimus

# Create optimus
op = Optimus()

# ## Create dataframe

# +
from pyspark.sql.types import StringType, IntegerType, ArrayType

df = op.create.df(
    [
        ("words", "str", True),
        ("num", "int", True),
        ("animals", "str", True),
        ("thing", StringType(), True),
        ("two strings", StringType(), True),
        ("filter", StringType(), True),
        ("num 2", "string", True),
        ("col_array", ArrayType(StringType()), True),
        ("col_int", ArrayType(IntegerType()), True)

    ]
    ,
    [
        ("  I like     fish  ", 1, "dog", "housé", "cat-car", "a", "1", ["baby", "sorry"], [1, 2, 3]),
        ("    zombies", 2, "cat", "tv", "dog-tv", "b", "2", ["baby 1", "sorry 1"], [3, 4]),
        ("simpsons   cat lady", 2, "frog", "table", "eagle-tv-plus", "1", "3", ["baby 2", "sorry 2"], [5, 6, 7]),
        (None, 3, "eagle", "glass", "lion-pc", "c", "4", ["baby 3", "sorry 3"], [7, 8])
    ])

df.table()
# -

# ## Create Columns
# ### Spark
# * You can not create multiple columns at the same time
# * You need to use the lit function. lit???
#
# ### Pandas
# * Assing function seems to do the job https://stackoverflow.com/questions/12555323/adding-new-column-to-existing-dataframe-in-python-pandas
#

# ### Create a column with a constant value

df = df.cols.append("new_col_1", 1)
df.table()

# ### Create multiple columns with a constant value

# +
from pyspark.sql.functions import *

df.cols.append([
    ("new_col_2", 2.22),
    ("new_col_3", lit(3))
]).table()
# -

# ### Create multiple columns with a constant string, a new column with existing columns value and an array

# +

df.cols.append([
    ("new_col_4", "test"),
    ("new_col_5", df['num'] * 2),
    ("new_col_6", [1, 2, 3])
]).table()
# -

# ## Select columns
# ### Spark
# * You can not select columns by string and index at the same time
#
# ### Pandas
# * You can not select columns by string and index at the same time

df.table()
columns = ["words", 1, "animals", 3]
df.cols.select(columns).table()

# ### Select columns with a Regex

df.cols.select("n.*", regex=True).table()

# ### Select all the columns of type string

df.cols.select("*", data_type="str").table()

# ## Rename Column
# ### Spark
# You can not rename multiple columns using Spark Vanilla API
#
#
# ### Pandas
# * Almost the same behavior https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.rename.html

df.cols.rename('num', 'number').table()

# ### Rename multiple columns and uppercase all the columns

df.cols.rename([('num', 'number'), ("animals", "gods")], str.upper).table()

# ### Convert to lower case

df.cols.rename(str.lower).table()

# ### Convert to uppercase

df.cols.rename(str.upper).table()

# ## Cast a columns
#
# This is a opinionated way to handle column casting. 
# One of the first thing that every data cleaning process need to acomplish is define a data dictionary.
# Because of that we prefer to create a tuple like this:
#
# df.cols().cast(
# [("words","str"),
# ("num","int"),
# ("animals","float"),
# ("thing","str")]
# )
#
# ### Spark
# * Can not cast multiple columns
#
# ### Pandas
#
# with astype()
# https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.astype.html

df.cols.cast([("num", "string"), ("num 2", "integer")]).dtypes

# ### Cast a column to string

df.cols.cast("num", "string").dtypes

# ### Cast all columns to string

df.cols.cast("*", "string").dtypes

# ### Cast a column to Vectors

# +
from pyspark.ml.linalg import Vectors

df.cols.cast("col_int", Vectors)
# -

# ## Keep columns
# ### Spark
# * You can you df.select() to get the columns you want
#
# ### Pandas
# * Via drop()
#

from pyspark.sql.functions import *

df.withColumn("num", col("num").cast(StringType()))

df.table()
df.cols.keep("num").table()

# ## Move columns
# ### Spark
# Do not exist in spark
#
# ### Pandas
# Do not exist in pandas

df.cols.move("words", "after", "thing").table()

# ## Sorting Columns
# ### Spark
# You can not sort columns using Spark Vanilla API 
#
# ### Pandas
# df.reindex_axis(sorted(df.columns), axis=1)

# ### Sort in Alphabetical order

df.cols.sort().table()

# ### Sort in Reverse Alphabetical order

df.cols.sort(order="desc").table()

# ## Drop columns
# ### Spark 
# * You can not delete multiple colums
#
# ### Pandas
# * Almost the same as pandas
# https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.drop.html

# ###  Drop one columns

df2 = df.cols.drop("num")
df2.table()

# ### Drop multiple columns

df2 = df.cols.drop(["num", "words"])
df2.table()

df.table()

# ## Chaining
#
# .cols y .rows attributes are used to organize and encapsulate `optimus` functionality apart from Apache Spark Dataframe API.
#
# At the same time it can be helpfull when you look at the code because every line is self explained.
#
# The past transformations were done step by step, but this can be achieved by chaining all operations into one line of code, like the cell below. This way is much more efficient and scalable because it uses all optimization issues from the lazy evaluation approach.

df.table()
df \
    .cols.rename([('num', 'number')]) \
    .cols.drop(["number", "words"]) \
    .withColumn("new_col_2", lit("spongebob")) \
    .cols.append("new_col_1", 1) \
    .cols.sort(order="desc") \
    .rows.drop(df["num 2"] == 3) \
    .table()

# ## Unnest  Columns
#
# With unnest you can convert one column into multiple ones. it can hadle string, array and vectors
#
# ### Spark
# Can split strings with split()
#
# ### Pandas
# via str.split()

df.table()
df.cols.unnest("two strings", "-") \
    .table()

# ### Only get the first element

df.cols.unnest("two strings", "-", index=1).table()

# ### Unnest array of string

df \
    .cols.unnest("col_array") \
    .table()

# ### Unnest and array of ints

df \
    .cols.unnest(["col_int"]) \
    .table()

# ### Spits in 3 parts

df \
    .cols.unnest(["two strings"], splits=3, separator="-") \
    .table()

# ### Unnest a Vector

# +
from pyspark.ml.linalg import Vectors

df1 = op.sc.parallelize([
    ("assert", Vectors.dense([1, 2, 3])),
    ("require", Vectors.sparse(3, {1: 2}))
]).toDF()
# -

df1 \
    .cols.unnest(["vector"]) \
    .table()

df = df.cols.append("new_col_1", 1)

# ## Impute

# ### Fill missing data

df_fill = op.spark.createDataFrame([(1.0, float("nan"), "1"),
                                    (2.0, float("nan"), "nan"),
                                    (float("nan"), 3.0, None),
                                    (4.0, 4.0, "2"),
                                    (5.0, 5.0, "2")
                                    ], ["a", "b", "c"]
                                   )

df_fill.table()

df_fill.cols.impute(["a", "b"], "continuous", "median").table()

df_fill.cols.impute(["c"], "categorical").table()

# ## Get columns by type
# ### Spark
# Not implemented in Spark Vanilla
#
# ### Pandas

df.cols.select_by_dtypes("int").table()

# ## Apply custom function
#
# Spark have few ways to transform data rdd, Columns Expression, UDF and Pandas UDF. apply() and apply_expr() try to make a consistent way to call this expression without knowing the implementation details.
#
# ### Spark
# You need to declare a UDF Spark function
#
# ### Pandas
# Almost the same behavior that Optimus

df.table()


# ### Create a function that only apply to string value in column filter
#
# Sometimes there are columns with for example with numbers even when are supposed to be only of words or letters. 
#
# In order to solve this problem, apply_by_dtypes() function can be used. 
#
# In the next example we replace a number in a string column with "new string"

# +
def func(val, attr):
    return attr


df.cols.apply_by_dtypes("filter", func, "string", "new string", data_type="integer").table()


# -

# ### Create a UDF function that sum a values(32 in this case) to two columns

# +
def func(val, attr):
    return val + attr


df.cols.apply(["num", "new_col_1"], func, "int", 32, "udf").table()


# -

# ### Create a Pandas UDF function that sum a values(32 in this case) to two columns

# +
def func(val, attr):
    return val + attr


df.cols.apply(["num", "new_col_1"], func, "int", 10).table()
# -

# ### Select row where column "filter" is "integer"

# +
from optimus.functions import filter_row_by_data_type as fbdt

df.rows.select(fbdt("filter", "integer")).table()
# -

# ### Create an abstract dataframe to filter a rows where the value of column "num"> 1

# +
from optimus.functions import abstract_udf as audf


def func(val, attr):
    return val > 1


df.rows.select(audf("num", func, "boolean")).table()
# -

# ### Create an abstract dataframe (Pandas UDF) to pass two arguments to a function a apply a sum operation

# +
from optimus.functions import abstract_udf as audf


def func(val, attr):
    return val + attr[0] + attr[1]


df.withColumn("num_sum", audf("num", func, "int", [10, 20])).table()


# -

# ### Apply a column expression to when the value of "num" or "num 2" is grater than 2

# +


def func(col_name, attr):
    return F.when(F.col(col_name) > 2, 10).otherwise(1)


df.cols.apply_expr(["num", "num 2"], func).table()
# -

# ### Convert to uppercase

# +
from pyspark.sql import functions as F


def func(col_name, attr):
    return F.upper(F.col(col_name))


df.cols.apply_expr(["two strings", "animals"], func).table()


# -

# ### Using apply with a condition

# +
def func(val, attr):
    return 10


col = "num"

df.cols.apply(col, func, "int", when=df["num"] > 1).table()

df.cols.apply(col, func, "int", when=fbdt(col, "int")).table()
# -

# ## Count Nulls

# +
import numpy as np

df_null = op.spark.createDataFrame(
    [(1, 1, None), (1, 2, float(5)), (1, 3, np.nan), (1, 4, None), (1, 5, float(10)), (1, 6, float('nan')),
     (1, 6, float('nan'))],
    ('session', "timestamp1", "id2"))
# -

df_null.table()

df_null.cols.count_na("id2")

df_null.cols.count_na("*")

# ## Count uniques
# ### Spark
#
# ### Pandas
#

df.cols.count_uniques("*")

# ## Unique
# ### Spark
# An abstraction of distinct to be use in multiple columns at the same time
#
# ### Pandas
# Similar behavior than pandas

df.table()

df_distinct = op.create.df(
    [
        ("words", "str", True),
        ("num", "int", True)
    ],
    [
        ("  I like     fish  ", 1),
        ("    zombies", 2),
        ("simpsons   cat lady", 2),
        (None, 3),
        (None, 0)
    ])

df_distinct.cols.unique("num").table()

# ## Count Zeros

df_zeros = df_distinct
df_zeros.table()
df_zeros.cols.count_zeros("*")

# ## Column Data Types

df.cols.dtypes('*')

# ## Replace

df.table()

# ### Replace "dog","cat" in column "animals" by the word "animals"

df.cols.replace("animals", ["dog", "cat"], "animals").table()

# ### Replace "dog-tv", "cat", "eagle", "fish" in columns "two strings","animals" by the string "animals"

df.cols.replace(["two strings", "animals"], ["dog-tv", "cat", "eagle", "fish"], "animals").table()

# ### Replace "dog" by  "dog_1" and "cat" by "cat_1" in columns "animals"

df.cols.replace("animals", [("dog", "dog_1"), ("cat", "cat_1")]).table()

# ### Replace in column "animals", "dog" by "pet" 

df.cols.replace("animals", "dog", "animal").table()

# ### Replace a,b,c by % in all columns

df.cols.replace("*", ["a", "b", "c"], "%").table()

# ### Replace 3 and 2 by 10 in a numeric columns

df.cols.replace('num', ["3", 2], 10).table()

# ### Replace 3 by 6 and 2 by 12 in a numeric columns

df.cols.replace('num', [("3", 6), (2, 12)]).table()

# ### Replace as words

df.cols.replace("animals", "dog", "animal", search_by="words").table()

df.cols.replace("animals", [("dog", "dog_1"), ("cat", "cat_1")], "words").table()

df.cols.replace("animals", ["dog", "cat"], "animals", "words").table()

# ### Use Regex

df.cols.replace_regex('*', '.*[Cc]at.*', 'cat_1').table()

# ## Nest

# ### Merge two columns in a column vector
# #### Match the string as a word not as a substring

df.cols.nest(["num", "new_col_1"], output_col="col_nested", shape="vector").table()

# ### Merge two columns in a string columns

df.cols.nest(["animals", "two strings"], output_col="col_nested", shape="string").table()

# ### Merge three columns in an array

df.cols.nest(["animals", "two strings", "num 2"], "col_nested", shape="array").table()

# ## Histograms

df = op.load.url("https://raw.githubusercontent.com/ironmussa/Optimus/master/examples/data/foo.csv")

df.table()

df.cols.hist("price", 10)

df.cols.frequency("billingId")

# ## Statistics

# ### Quantile Statistics

print(df.cols.min("billingId"))
print(df.cols.percentile(['billingId', 'price'], [0.05, 0.25, 0.5, 0.75, 0.95]))
print(df.cols.max("billingId"))
print(df.cols.median(["billingId", "price"]))
print(df.cols.range(["billingId", "price"]))
print(df.cols.std(["billingId", "price"]))

print(df.cols.min("*"))

# + {"active": ""}
# ### Descriptive Statistics
# -

print(df.cols.kurt("billingId"))
print(df.cols.mean("billingId"))
print(df.cols.skewness("billingId"))
print(df.cols.sum("billingId"))
print(df.cols.variance("billingId"))
print(df.cols.mad("billingId"))

# ### Calculate Median Absolute deviation

df.cols.mad("price")

df.cols.mad("price", more=True)

# ### Calculate precentiles

print(df.cols.percentile(['price'], [0.05, 0.25, 0.5, 0.75, 0.95]))

# ### Calculate Mode

print(df.cols.mode(["price", "billingId"]))

# ## String Operations

df.table()

df \
    .cols.trim("lastName") \
    .cols.lower("lastName") \
    .cols.upper(["product", "firstName"]) \
    .cols.reverse("firstName") \
    .table()

# ### Calculate the interquartile range

df.cols.iqr("price")

df.cols.iqr("price", more=True)

# ### Calculate  Zscore

df.cols.z_score("price").table()

# ## Cleaning and Date Operations Operations

df.cols.years_between("birth", "yyyyMMdd", ).table()

df.cols.remove("*", ["&", "%"]).table()

df.cols.remove_accents("lastName").table()

df.cols.remove_special_chars("lastName").table()

df.cols.clip("billingId", 100, 200).table()

df_abs = op.create.df(
    [
        ("words", "str", True),
        ("num", "int", True),
        ("animals", "str", True),
        ("thing", StringType(), True),
        ("two strings", StringType(), True),
        ("filter", StringType(), True),
        ("num 2", "string", True),
        ("col_array", ArrayType(StringType()), True),
        ("col_int", ArrayType(IntegerType()), True)

    ]
    ,
    [
        ("  I like     fish  ", -1, "dog", "housé", "cat-car", "a", "-1", ["baby", "sorry"], [1, 2, 3]),
        ("    zombies", -2, "cat", "tv", "dog-tv", "b", "-2", ["baby 1", "sorry 1"], [3, 4]),
        ("simpsons   cat lady", -2, "frog", "table", "eagle-tv-plus", "1", "3", ["baby 2", "sorry 2"], [5, 6, 7]),
        (None, 3, "eagle", "glass", "lion-pc", "c", "4", ["baby 3", "sorry 3"], [7, 8])
    ])

df_abs.cols.abs(["num", "num 2"]).table()

df.cols.qcut("billingId", 5).table()
