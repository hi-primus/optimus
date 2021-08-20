# Optimus

[![Logo Optimus](https://raw.githubusercontent.com/hi-primus/optimus/develop-21.9/images/optimus-logo.png)](https://hi-optimus.com)

![Tests](https://github.com/hi-primus/optimus/actions/workflows/main.yml/badge.svg)
![![Docker image updated](https://hub.docker.com/r/hiprimus/optimus)](https://github.com/hi-primus/optimus/actions/workflows/docker.yml/badge.svg)
[![PyPI Latest Release](https://img.shields.io/pypi/v/pyoptimus.svg)](https://pypi.org/project/pyoptimus/) 
[![GitHub release](https://img.shields.io/github/release/hi-primus/optimus.svg?include_prereleases)](https://github.com/hi-primus/optimus/releases)
[![CalVer](https://img.shields.io/badge/calver-YY.MM.MICRO-22bfda.svg)](http://calver.org)

[![Downloads](https://pepy.tech/badge/pyoptimus)](https://pepy.tech/project/pyoptimus)
[![Downloads](https://pepy.tech/badge/pyoptimus/month)](https://pepy.tech/project/pyoptimus/month)
[![Downloads](https://pepy.tech/badge/pyoptimus/week)](https://pepy.tech/project/pyoptimus/week)
[![Mentioned in Awesome Data Science](https://awesome.re/mentioned-badge.svg)](https://github.com/bulutyazilim/awesome-datascience) 
[![Slack](https://img.shields.io/badge/chat-slack-red.svg?logo=slack&color=36c5f0)](https://communityinviter.com/apps/hi-bumblebee/welcome)

# Get started 🏃

## Try Optimus

To launch a live notebook server to test optimus using binder or Colab, click on one of the following badges:

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/hi-primus/optimus/develop-21.9)
[![Colab](https://img.shields.io/badge/launch-colab-yellow.svg?logo=googlecolab&color=e6a210)](https://colab.research.google.com/github/hi-primus/optimus/blob/master/examples/10_min_to_optimus.ipynb)

## Installation (pip): 
 
In your terminal just type  ```pip install pyoptimus```

### Requirements
* Python 3.7 or 3.8

## Examples

You can go to the 10 minutes to Optimus [notebook](https://github.com/hi-primus/optimus/blob/develop-21.9/examples/10_min_to_optimus.ipynb) where you can find the basic to start working.

Also you can go to [Examples](https://github.com/hi-primus/optimus/tree/develop-21.9/examples/examples.md) and found specific notebooks about data cleaning, data munging, profiling, data enrichment and how to create ML and DL models.

Besides check the [Cheat Sheet](https://htmlpreview.github.io/?https://github.com/hi-primus/optimus/blob/develop-21.9/docs/cheatsheet/optimus_cheat_sheet.html)

## Start Optimus

Start Optimus using ```"pandas"```, ```"dask"```, ```"cudf"``` or ```"dask_cudf"```.

```python
from optimus import Optimus
op = Optimus("pandas")
```

## Loading data

Now Optimus can load data in csv, json, parquet, avro, excel from a local file or URL.

```python
#csv
df = op.load.csv("../examples/data/foo.csv")

#json
df = op.load.json("../examples/data/foo.json")

# using a url
df = op.load.json("https://raw.githubusercontent.com/hi-primus/optimus/develop-21.9/examples/data/foo.json")

# parquet
df = op.load.parquet("../examples/data/foo.parquet")

# ...or anything else
df = op.load.file("../examples/data/titanic3.xls")
```

Also, you can load data from oracle, redshift, mysql and postgres.

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
![](https://github.com/hi-primus/optimus/tree/develop-21.9/readme/images/table.png)

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

If you have issues, see our [Troubleshooting Guide](https://github.com/hi-primus/optimus/tree/develop-21.9/troubleshooting.md)

# Contributing to Optimus 💡

Contributions go far beyond pull requests and commits. We are very happy to receive any kind of contributions  
including: 
 
* [Documentation](https://github.com/hi-primus/optimus/tree/develop-21.9/docs/source) updates, enhancements, designs, or   bugfixes. 
* Spelling or grammar fixes. 
* README.md corrections or redesigns. 
* Adding unit, or functional [tests](https://github.com/hi-primus/optimus/tree/develop-21.9/tests)  
* Triaging GitHub issues -- especially determining whether an issue still persists or is reproducible.
* [Blogging, speaking about, or creating tutorials](https://hioptimus.com/category/blog/) about Optimus and its many features. 
* Helping others on our official chats
 
# Backers and Sponsors

Become a [backer](https://opencollective.com/optimus#backer) or a [sponsor](https://opencollective.com/optimus#sponsor) and get your image on our README on Github with a link to your site. 

[![OpenCollective](https://opencollective.com/optimus/backers/badge.svg)](#backers) [![OpenCollective](https://opencollective.com/optimus/sponsors/badge.svg)](#sponsors)
