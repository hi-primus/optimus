# Optimus

[![Logo Optimus](https://raw.githubusercontent.com/hi-primus/optimus/develop-22.2/images/optimus-logo.png)](https://hi-optimus.com)

[![Tests](https://github.com/hi-primus/optimus/actions/workflows/main.yml/badge.svg)](https://github.com/hi-primus/optimus/actions/workflows/main.yml)
[![Docker image updated](https://github.com/hi-primus/optimus/actions/workflows/docker.yml/badge.svg)](https://hub.docker.com/r/hiprimus/optimus)
[![PyPI Latest Release](https://img.shields.io/pypi/v/pyoptimus.svg)](https://pypi.org/project/pyoptimus/) 
[![GitHub release](https://img.shields.io/github/release/hi-primus/optimus.svg?include_prereleases)](https://github.com/hi-primus/optimus/releases)
[![CalVer](https://img.shields.io/badge/calver-YY.MM.MICRO-22bfda.svg)](http://calver.org)

[![Downloads](https://pepy.tech/badge/pyoptimus)](https://pepy.tech/project/pyoptimus)
[![Downloads](https://pepy.tech/badge/pyoptimus/month)](https://pepy.tech/project/pyoptimus/month)
[![Downloads](https://pepy.tech/badge/pyoptimus/week)](https://pepy.tech/project/pyoptimus/week)
[![Mentioned in Awesome Data Science](https://awesome.re/mentioned-badge.svg)](https://github.com/bulutyazilim/awesome-datascience) 
[![Slack](https://img.shields.io/badge/chat-slack-red.svg?logo=slack&color=36c5f0)](https://communityinviter.com/apps/hi-bumblebee/welcome)

# Overview

Optimus is an opinionated python library to easily load, process, plot and create ML models that run over pandas, Dask, cuDF, dask-cuDF, Vaex or Spark. 

Some amazing things Optimus can do for you:
* Process using a simple API, making it easy to use for newcomers.
* More than 100 functions to handle strings, process dates, urls and emails.
* Easily plot data from any size.
* Out of box functions to explore and fix data quality. 
* Use the same code to process your data in your laptop or in a remote cluster of GPUs.

[See Documentation](https://docs.hi-optimus.com/en/latest/)

## Try Optimus
To launch a live notebook server to test optimus using binder or Colab, click on one of the following badges:

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/hi-primus/optimus/develop-22.2?filepath=https%3A%2F%2Fraw.githubusercontent.com%2Fhi-primus%2Foptimus%2Fdevelop-22.2%2Fexamples%2F10_min_to_optimus.ipynb)
[![Colab](https://img.shields.io/badge/launch-colab-yellow.svg?logo=googlecolab&color=e6a210)](https://colab.research.google.com/github/hi-primus/optimus/blob/master/examples/10_min_to_optimus_colab.ipynb)

## Installation (pip): 
In your terminal just type:
```
pip install pyoptimus
```

By default Optimus install Pandas as the default engine, to install other engines you can use the following commands:

| Engine    | Command                                |
|-----------|----------------------------------------|
| Dask      | ```pip install pyoptimus[dask]```      |
| cuDF      | ```pip install pyoptimus[cudf]```      |
| Dask-cuDF | ```pip install pyoptimus[dask-cudf]``` |
| Vaex      | ```pip install pyoptimus[vaex]```      |
| Spark     | ```pip install pyoptimus[spark]```     |

To install from the repo: 
```
pip install git+https://github.com/hi-primus/optimus.git@develop-22.2
```

To install other engines: 
```
pip install git+https://github.com/hi-primus/optimus.git@develop-22.2#egg=pyoptimus[dask]
```



### Requirements
* Python 3.7 or 3.8

## Examples

You can go to [10 minutes to Optimus](https://github.com/hi-primus/optimus/blob/develop-22.2/examples/10_min_to_optimus.ipynb) where you can find the basics to start working in a notebook.

Also you can go to the [Examples](https://github.com/hi-primus/optimus/tree/develop-22.2/examples/examples.md) section and find specific notebooks about data cleaning, data munging, profiling, data enrichment and how to create ML and DL models.

Here's a handy [Cheat Sheet](https://htmlpreview.github.io/?https://github.com/hi-primus/optimus/blob/develop-22.2/docs/cheatsheet/optimus_cheat_sheet.html) with the most common Optimus' operations.

## Start Optimus

Start Optimus using ```"pandas"```, ```"dask"```, ```"cudf"```,```"dask_cudf"```,```"vaex"``` or ```"spark"```.

```python
from optimus import Optimus
op = Optimus("pandas")
```

## Loading data

Now Optimus can load data in csv, json, parquet, avro and excel formats from a local file or from a URL.

```python
#csv
df = op.load.csv("../examples/data/foo.csv")

#json
df = op.load.json("../examples/data/foo.json")

# using a url
df = op.load.json("https://raw.githubusercontent.com/hi-primus/optimus/develop-22.2/examples/data/foo.json")

# parquet
df = op.load.parquet("../examples/data/foo.parquet")

# ...or anything else
df = op.load.file("../examples/data/titanic3.xls")
```

Also, you can load data from Oracle, Redshift, MySQL and Postgres databases.

## Saving Data

```python
#csv
df.save.csv("data/foo.csv")

# json
df.save.json("data/foo.json")

# parquet
df.save.parquet("data/foo.parquet")
```

You can also save data to oracle, redshift, mysql and postgres.

## Create dataframes

Also, you can create a dataframe from scratch
```python
df = op.create.dataframe({
    'A': ['a', 'b', 'c', 'd'],
    'B': [1, 3, 5, 7],
    'C': [2, 4, 6, None],
    'D': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10']
})
```

Using `display` you have a beautiful way to show your data with extra information like column number, column data type and marked white spaces.

```python
display(df)
```
![](https://github.com/hi-primus/optimus/tree/develop-22.2/readme/images/table.png)

## Cleaning and Processing
 
Optimus was created to make data cleaning a breeze. The API was designed to be super easy to newcomers and very familiar for people that comes from Pandas.
Optimus expands the standard DataFrame functionality adding `.rows` and `.cols` accessors.

For example you can load data from a url, transform and apply some predefined cleaning functions:

```python
new_df = df\
    .rows.sort("rank", "desc")\
    .cols.lower(["names", "function"])\
    .cols.date_format("date arrival", "yyyy/MM/dd", "dd-MM-YYYY")\
    .cols.years_between("date arrival", "dd-MM-YYYY", output_cols="from arrival")\
    .cols.normalize_chars("names")\
    .cols.remove_special_chars("names")\
    .rows.drop(df["rank"]>8)\
    .cols.rename("*", str.lower)\
    .cols.trim("*")\
    .cols.unnest("japanese name", output_cols="other names")\
    .cols.unnest("last position seen", separator=",", output_cols="pos")\
    .cols.drop(["last position seen", "japanese name", "date arrival", "cybertronian", "nulltype"])
```

# Need help? 🛠️

## Feedback

Feedback is what drive Optimus future, so please take a couple of minutes to help shape the Optimus' Roadmap:  http://bit.ly/optimus_survey 

Also if you want to a suggestion or feature request use https://github.com/hi-primus/optimus/issues

## Troubleshooting

If you have issues, see our [Troubleshooting Guide](https://github.com/hi-primus/optimus/tree/develop-22.2/troubleshooting.md)

# Contributing to Optimus 💡

Contributions go far beyond pull requests and commits. We are very happy to receive any kind of contributions  
including: 
 
* [Documentation](https://docs.hi-optimus.com/en/latest/) updates, enhancements, designs, or bugfixes. 
* Spelling or grammar fixes. 
* README.md corrections or redesigns. 
* Adding unit, or functional [tests](https://github.com/hi-primus/optimus/tree/develop-22.2/tests)  
* Triaging GitHub issues -- especially determining whether an issue still persists or is reproducible.
* [Blogging, speaking about, or creating tutorials](https://hioptimus.com/category/blog/) about Optimus and its many features. 
* Helping others on our official chats
 
# Backers and Sponsors

Become a [backer](https://opencollective.com/optimus#backer) or a [sponsor](https://opencollective.com/optimus#sponsor) and get your image on our README on Github with a link to your site. 

[![OpenCollective](https://opencollective.com/optimus/backers/badge.svg)](#backers) [![OpenCollective](https://opencollective.com/optimus/sponsors/badge.svg)](#sponsors)
