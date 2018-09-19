# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext_format_version: '1.2'
#   jupytext_formats: ipynb,py
#   kernel_info:
#     name: python3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
#   language_info:
#     codemirror_mode:
#       name: ipython
#       version: 3
#     file_extension: .py
#     mimetype: text/x-python
#     name: python
#     nbconvert_exporter: python
#     pygments_lexer: ipython3
#     version: 3.6.5
#   nteract:
#     version: 0.11.7
# ---

# # Reference
#
# https://databricks.com/blog/2015/08/12/from-pandas-to-apache-sparks-dataframe.html
#
# https://pandas.pydata.org/pandas-docs/stable/10min.html

# %load_ext autoreload
# %autoreload 2

import sys
sys.path.append("..")

# ## Install Optimus 
#
# from command line:
#
# `pip install optimuspyspark`
#
# from a notebook you can use:
#
# `!pip install optimuspyspark`

# ## Import optimus and start it

from optimus import Optimus
op= Optimus(master="local", verbose=True)

# ## Dataframe creation
#
# Create a dataframe to passing a list of values for columns and rows. Unlike pandas you need to specify the column names.
#

# * https://en.wikipedia.org/wiki/Optimus_Prime
# * https://en.wikipedia.org/wiki/Bumblebee_(Transformers)
# * https://en.wikipedia.org/wiki/Ironhide
# * https://en.wikipedia.org/wiki/Jazz_(Transformers)
# * https://en.wikipedia.org/wiki/Megatron

df = op.create.df(
    [
        "names",
        "height(ft)",
        "function",
        "rank",
        "weight(t)",
        "japanese name",
        "last position",
        "attributes"
    ],
    [
        
        ("Optim'us", 28.0, "Leader", 10, 4.3, ["Inochi", "Convoy"], "19.442735,-99.201111",[8.5344, 4300]),
        ("bumbl#ebéé  ", 17.5, "Espionage", 7, 2, ["Bumble","Goldback"], "10.642707,-71.612534",[5.334, 2000]),
        ("ironhide&", 26.0, "Security", 7, 4, ["Roadbuster"], "37.789563,-122.400356",[7.9248, 4000]),
        ("Jazz",13.0, "First Lieutenant", 8, 1.8, ["Meister"], "33.670666,-117.841553",[3.9624, 1800]),
        ("Megatron",None, "None", None, 5.7, ["Megatron"], None,[None,5700]),
        ("Metroplex_)^$",300 , "Battle Station", 8, None, ["Metroflex"],None,[91.44, None]),
        
    ]).h_repartition(1)
df.table()

# Creating a dataframe by passing a list of tuples specifyng the column data type. You can specify as data type an string or a Spark Datatypes. https://spark.apache.org/docs/2.3.1/api/java/org/apache/spark/sql/types/package-summary.html
#
# Also you can use some Optimus predefined types:
# * "str" = StringType() 
# * "int" = IntegerType() 
# * "float" = FloatType()
# * "bool" = BoleanType()

df = op.create.df(
    [
        ("names", "str"),
        ("height", "float"),
        ("function", "str"),
        ("rank", "int"),
    ],
    [
        ("bumbl#ebéé  ", 17.5, "Espionage", 7),
        ("Optim'us", 28.0, "Leader", 10),
        ("ironhide&", 26.0, "Security", 7),
        ("Jazz",13.0, "First Lieutenant", 8),
        ("Megatron",None, "None", None),
        
    ])
df.table()

# Creating a dataframe and specify if the column accepts null values

df = op.create.df(
    [
        ("names", "str", True),
        ("height", "float", True),
        ("function", "str", True),
        ("rank", "int", True),
    ],
    [
        ("bumbl#ebéé  ", 17.5, "Espionage", 7),
        ("Optim'us", 28.0, "Leader", 10),
        ("ironhide&", 26.0, "Security", 7),
        ("Jazz",13.0, "First Lieutenant", 8),
        ("Megatron",None, "None", None),
        
    ])
df.table()

# Creating a Daframe using a pandas dataframe

# +
import pandas as pd
import numpy as np

data = [("bumbl#ebéé  ", 17.5, "Espionage", 7),
         ("Optim'us", 28.0, "Leader", 10),
         ("ironhide&", 26.0, "Security", 7)]
labels = ["names", "height", "function", "rank"]

# Create pandas dataframe
pdf = pd.DataFrame.from_records(data, columns=labels)

df = op.create.df(pdf = pdf)
df.table()
# -

# ## Viewing data
# Here is how to View the first 10 elements in a dataframe.

df.table(10)

# ## Partitions
# Partition are the way Spark divide the data in your local computer or cluster to better optimize how it will be processed.It can greatly impact the Spark performance.
#
# Take 5 minutes to read this article:
# https://www.dezyre.com/article/how-data-partitioning-in-spark-helps-achieve-more-parallelism/297
#
# ## Lazy operations
# Lorem ipsum 
#
# https://stackoverflow.com/questions/38027877/spark-transformation-why-its-lazy-and-what-is-the-advantage
#
# ## Inmutability
# Lorem ipsum
#
# ## Spark Architecture
# Lorem ipsum

# ## Columns and Rows
#
# Optimus organized operations in columns and rows. This is a little different of how pandas works in which all operations are aroud the pandas class. We think this approach can better help you to access and transform data. For a deep dive about the designing decision please read:
#
# https://towardsdatascience.com/announcing-optimus-v2-agile-data-science-workflows-made-easy-c127a12d9e13

# Sort by cols names

df.cols.sort().table()

# Sort by rows rank value

df.rows.sort("rank").table()

df.describe().table()

# ## Selection
#
# Unlike Pandas, Spark DataFrames don't support random row access. So methods like `loc` in pandas are not available.
#
# Also Pandas don't handle indexes. So methods like `iloc` are not available.

# Select an show an specific column

df.cols.select("names").table()

# Select rows from a Dataframe where a the condition is meet

df.rows.select(df["rank"]>7).table()

# Select rows by specific values on it

df.rows.is_in("rank",[7, 10]).table()

# Create and unique id for every row.

df.create_id().table()

# Create wew columns

df.cols.append("Affiliation","Autobot").table()

# ## Missing Data

# + {"inputHidden": false, "outputHidden": false}
df.rows.drop_na("*",how='any').table()
# -

# Filling missing data.

df.cols.fill_na("*","N//A").table()

# To get the boolean mask where values are nan.

df.cols.is_na("*").table()

# # Operations

# ## Stats

# + {"inputHidden": false, "outputHidden": false}
df.cols.mean("height(ft)")

# + {"inputHidden": false, "outputHidden": false}
df.cols.mean("*")
# -

# ### Apply

# + {"inputHidden": false, "outputHidden": false}
def func(value, args):
    return value + 1

df.cols.apply("height(ft)",func,"float").table()
# -

# ### Histogramming

df.cols.count_uniques("*")

# ### String Methods

# + {"inputHidden": false, "outputHidden": false}
df\
    .cols.lower("names")\
    .cols.upper("function").table()
# -

# ## Merge

# ### Concat
#
# Optimus provides and intuitive way to concat Dataframes by columns or rows.

# +
df_new = op.create.df(
    [
        "class"
    ],
    [
        ("Autobot"),
        ("Autobot"),
        ("Autobot"),
        ("Autobot"),
        ("Decepticons"),
        
        
    ]).h_repartition(1)

op.concat([df,df_new], "columns").table()

# +
df_new = op.create.df(
    [
        "names",
        "height",
        "function",
        "rank",
    ],
    [
        ("Grimlock", 22.9, "Dinobot Commander", 9),               
    ]).h_repartition(1)

op.concat([df,df_new], "rows").table()


# + {"inputHidden": false, "outputHidden": false}
Operations like `join` and `group` are handle using Spark directly

# + {"inputHidden": false, "outputHidden": false}
import pandas as pd

pdf = pd.DataFrame({'A': {0: 'a', 1: 'b', 2: 'c'},
                   'B': {0: 1, 1: 3, 2: 5},
                   'C': {0: 2, 1: 4, 2: 6}})

sdf = op.create.df(pdf=pdf)
sdf.table()
sdf.melt(id_vars=['A'], value_vars=['B', 'C']).table()

# + {"inputHidden": false, "outputHidden": false}
pd.DataFrame({'A': {0: 'a', 1: 'b', 2: 'c'},
                            'B': {0: 1, 1: 3, 2: 5},
                            'C': {0: 2, 1: 4, 2: 6}})

# + {"inputHidden": false, "outputHidden": false}
df = op.create.df(
   [("A","str"), ("B","int"), ("C","int")],
[
    ("a",1,2),
    ("b",3,4),
    ("c",5,6),        
])
df.melt(id_vars=['A'], value_vars=['B', 'C']).table()

[[('A', StringType, True), ('B', StringType, True), ('C', StringType, True)],
 [('a', '1', '2'), ('b', '3', '4'), ('c', '5', '6')]]

# + {"inputHidden": false, "outputHidden": false}
a = [("A","str"), ("B","int"), ("C","int")],
[
    ("a",1,2),
    ("b",3,4),
    ("c",5,6),        
]

# + {"inputHidden": false, "outputHidden": false}
#df.to_json()
df.table()

# + {"inputHidden": false, "outputHidden": false}
value = df.collect()
[tuple(v.asDict().values()) for v in value]


# + {"inputHidden": false, "outputHidden": false}
df.cols.names()

# + {"inputHidden": false, "outputHidden": false}
df.export()

# + {"inputHidden": false, "outputHidden": false}
source_df = op.create.df(
    ["A", "B", "C"],
    [
        ("a", 1, 2),
        ("b", 3, 4),
        ("c", 5, 6),
    ])


actual_df = source_df.melt(id_vars=['A'], value_vars=['B', 'C']).table()

expected_df = op.create.df([('A', StringType, True), ('B', StringType, True), ('C', StringType, True)],
                           [('a', '1', '2'), ('b', '3', '4'), ('c', '5', '6')])

assert (expected_df.collect() == actual_df.collect())

# + {"inputHidden": false, "outputHidden": false}
df.schema

# + {"inputHidden": false, "outputHidden": false}
eval("StringType()")

# + {"inputHidden": false, "outputHidden": false, "scrolled": false}
op.profiler.run(df, "*",infer=False)
# -


