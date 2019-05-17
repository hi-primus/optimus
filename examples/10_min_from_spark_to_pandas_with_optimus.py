# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py
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

op = Optimus(master="local")

# ## Dataframe creation
#
# Create a dataframe to passing a list of values for columns and rows. Unlike pandas you need to specify the column names.
#

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

        ("Optim'us", 28.0, "Leader", 10, 4.3, ["Inochi", "Convoy"], "19.442735,-99.201111", [8.5344, 4300.0]),
        ("bumbl#ebéé  ", 17.5, "Espionage", 7, 2.0, ["Bumble", "Goldback"], "10.642707,-71.612534", [5.334, 2000.0]),
        ("ironhide&", 26.0, "Security", 7, 4.0, ["Roadbuster"], "37.789563,-122.400356", [7.9248, 4000.0]),
        ("Jazz", 13.0, "First Lieutenant", 8, 1.8, ["Meister"], "33.670666,-117.841553", [3.9624, 1800.0]),
        ("Megatron", None, "None", None, 5.7, ["Megatron"], None, [None, 5700.0]),
        ("Metroplex_)^$", 300.0, "Battle Station", 8, None, ["Metroflex"], None, [91.44, None]),

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
        ("Jazz", 13.0, "First Lieutenant", 8),
        ("Megatron", None, "None", None),

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
        ("Jazz", 13.0, "First Lieutenant", 8),
        ("Megatron", None, "None", None),

    ])
df.table()

# Creating a Daframe using a pandas dataframe

# +
import pandas as pd

data = [("bumbl#ebéé  ", 17.5, "Espionage", 7),
        ("Optim'us", 28.0, "Leader", 10),
        ("ironhide&", 26.0, "Security", 7)]
labels = ["names", "height", "function", "rank"]

# Create pandas dataframe
pdf = pd.DataFrame.from_records(data, columns=labels)

df = op.create.df(pdf=pdf)
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

df.rows.select(df["rank"] > 7).table()

# Select rows by specific values on it

df.rows.is_in("rank", [7, 10]).table()

# Create and unique id for every row.

df.create_id().table()

# Create wew columns

df.cols.append("Affiliation", "Autobot").table()

# ## Missing Data

# + {"inputHidden": false, "outputHidden": false}
df.rows.drop_na("*", how='any').table()
# -

# Filling missing data.

df.cols.fill_na("*", "N//A").table()

# To get the boolean mask where values are nan.

df.cols.is_na("*").table()

# # Operations

# ## Stats

# + {"inputHidden": false, "outputHidden": false}
df.cols.mean("height")

# + {"inputHidden": false, "outputHidden": false}
df.cols.mean("*")


# -

# ### Apply

# + {"inputHidden": false, "outputHidden": false}
def func(value, args):
    return value + 1


df.cols.apply("height", func, "float").table()
# -

# ### Histogramming

df.cols.count_uniques("*")

# ### String Methods

# + {"inputHidden": false, "outputHidden": false}
df \
    .cols.lower("names") \
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

op.concat([df, df_new], "columns").table()

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

op.concat([df, df_new], "rows").table()

# + {"inputHidden": false, "outputHidden": false}
# Operations like `join` and `group` are handle using Spark directly

# + {"inputHidden": false, "outputHidden": false}
df_melt = df.melt(id_vars=["names"], value_vars=["height", "function", "rank"])
df.table()
# -

df_melt.pivot("names", "variable", "value").table()

# ## Ploting

df.plot.hist("height", 10)

df.plot.frequency("*", 10)

# ## Getting Data In/Out

# + {"inputHidden": false, "outputHidden": false}
df.cols.names()

# + {"inputHidden": false, "outputHidden": false}
df.export()

# + {"inputHidden": false, "outputHidden": false}
df.schema
# -

df.table()

# + {"inputHidden": false, "outputHidden": false}
op.profiler.run(df, "height", infer=True)
# -
df_csv = op.load.url("https://raw.githubusercontent.com/ironmussa/Optimus/master/examples/data/foo.csv").limit(5)
df_csv.table()

df_json = op.load.url("https://raw.githubusercontent.com/ironmussa/Optimus/master/examples/data/foo.json",
                      "json").limit(5)
df_json.table()

df_csv.save.csv("test.csv")

df.table()

# ## Enrichment

# +
import requests


def func_request(params):
    # You can use here whatever header or auth info you need to send. 
    # For more information see the requests library
    url = "https://jsonplaceholder.typicode.com/todos/" + str(params["rank"])

    return requests.get(url)


def func_response(response):
    # Here you can parse de response
    return response["title"]


df_result = op.enrich(df, func_request=func_request, func_response=func_response)
# -

df_result.table()
