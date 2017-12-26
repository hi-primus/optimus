[![Logo Optimus](https://github.com/ironmussa/Optimus/blob/master/images/logoOptimus.png)](https://hioptimus.com)

[![PyPI version](https://badge.fury.io/py/optimuspyspark.svg)](https://badge.fury.io/py/optimuspyspark) [![Build Status](https://travis-ci.org/ironmussa/Optimus.svg?branch=master)](https://travis-ci.org/ironmussa/Optimus) [![Documentation Status](https://readthedocs.org/projects/optimus-ironmussa/badge/?version=latest)](http://optimus-ironmussa.readthedocs.io/en/latest/?badge=latest)
 [![built_by iron](https://img.shields.io/badge/built_by-iron-FF69A4.svg)](http://ironmussa.com) [![Updates](https://pyup.io/repos/github/ironmussa/Optimus/shield.svg)](https://pyup.io/repos/github/ironmussa/Optimus/)
 [![GitHub release](https://img.shields.io/github/release/ironmussa/optimus.svg)](https://github.com/ironmussa/Optimus/) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/e01572e2af5640fcbcdd58e7408f3ea0)](https://www.codacy.com/app/favio.vazquezp/Optimus?utm_source=github.com&utm_medium=referral&utm_content=ironmussa/Optimus&utm_campaign=badger) [![StackShare](https://img.shields.io/badge/tech-stack-0690fa.svg?style=flat)](https://stackshare.io/iron-mussa/devops)

[![Platforms](https://img.shields.io/badge/platform-Linux%20%7C%20Mac%20OS%20%7C%20Windows-blue.svg)](https://spark.apache.org/docs/2.2.0/#downloading) [![Dependency Status](https://gemnasium.com/badges/github.com/ironmussa/Optimus.svg)](https://gemnasium.com/github.com/ironmussa/Optimus) [![Quality Gate](https://sonarqube.com/api/badges/gate?key=ironmussa-optimus:optimus)](https://sonarqube.com/dashboard/index/ironmussa-optimus:optimus)  [![Code Health](https://landscape.io/github/ironmussa/Optimus/develop/landscape.svg?style=flat)](https://landscape.io/github/ironmussa/Optimus/develop) [![Coverage Status](https://coveralls.io/repos/github/ironmussa/Optimus/badge.svg?branch=master)](https://coveralls.io/github/ironmussa/Optimus?branch=master) [![Mentioned in Awesome Data Science](https://awesome.re/mentioned-badge.svg)](https://github.com/bulutyazilim/awesome-datascience)



[![Join the chat at https://gitter.im/optimuspyspark/Lobby](https://badges.gitter.im/optimuspyspark/Lobby.svg)](https://gitter.im/optimuspyspark/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Optimus is the missing framework for cleaning and pre-processing data in a distributed fashion. It uses all the power of 
Apache Spark (optimized via Catalyst) to do so. It implements several handy tools for data wrangling and munging that will 
make your life much easier. The first obvious advantage over any other public data cleaning library or framework is that 
it will work on your laptop or your big cluster, and second, it is amazingly easy to install, use and understand.

Click on Optimus to enter in our Website:

<a href="https://hioptimus.com"><img src="https://github.com/ironmussa/Optimus/blob/master/images/robotOptimus.png" alt="Click Me!" border="0" height="220"></a>

And if you want to see some cool information and tutorials for Optimus check out our blog:

https://hioptimus.com/category/blog/

## Click below for the official documentation

[![Documentation](https://media.readthedocs.com/corporate/img/header-logo.png)](http://docs.hioptimus.com/en/latest/)

## Survey 

Please take a couple of minutes to help shape the Optimus' Roadmap:

### https://optimusdata.typeform.com/to/aEnYRY

## Installation (pip):

In your terminal just type:

```
pip install optimuspyspark
```

## Requirements
* Apache Spark 2.2.0
* Python>=3.6

## Basic Usage

DataFrameTransformer class receives a dataFrame as an argument. This
class has all methods listed above.

**Note:** Every possible transformation make changes over this dataFrame and
overwrites it.

### Transformer.clear_accents(columns)

This function deletes accents in strings dataFrames, it does not
eliminate main character, but only deletes special tildes.

``clear_accents`` method receives column names (``column``) as argument.
``columns`` must be a string or a list of column names.

E.g:

Building a dummy dataFrame:

```python

    # Importing sql types
    from pyspark.sql.types import StringType, IntegerType, StructType, StructField
    # Importing optimus
    import optimus as op

    # Building a simple dataframe:
    schema = StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("population", IntegerType(), True)])

    countries = ['Colombia', 'US@A', 'Brazil', 'Spain']
    cities = ['Bogotá', 'New York', '   São Paulo   ', '~Madrid']
    population = [37800000,19795791,12341418,6489162]

    # Dataframe:
    df = op.spark.createDataFrame(list(zip(cities, countries, population)), schema=schema)

    df.show()
```

New DF:

```
+---------------+--------+----------+
|           city| country|population|
+---------------+--------+----------+
|         Bogotá|Colombia|  37800000|
+---------------+--------+----------+
|       New York|    US@A|  19795791|
+---------------+--------+----------+
|   São Paulo   |  Brazil|  12341418|
+---------------+--------+----------+
|        ~Madrid|   Spain|   6489162|
+---------------+--------+----------+
```

```python

    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.show()

    # Clear accents:
    transformer.clear_accents(columns='*')

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.show()
```

Original dataFrame:
```
+---------------+--------+----------+
|           city| country|population|
+---------------+--------+----------+
|         Bogotá|Colombia|  37800000|
+---------------+--------+----------+
|       New York|    US@A|  19795791|
+---------------+--------+----------+
|   São Paulo   |  Brazil|  12341418|
+---------------+--------+----------+
|        ~Madrid|   Spain|   6489162|
+---------------+--------+----------+
```
New dataFrame:
```
+---------------+--------+----------+
|           city| country|population|
+---------------+--------+----------+
|         Bogota|Colombia|  37800000|
+---------------+--------+----------+
|       New York|    US@A|  19795791|
+---------------+--------+----------+
|   Sao Paulo   |  Brazil|  12341418|
+---------------+--------+----------+
|        ~Madrid|   Spain|   6489162|
+---------------+--------+----------+
```

### Transformer.remove_special_chars(columns)

This method remove special characters (i.e. !"#$%&/()=?) in columns of
dataFrames.

``remove_special_chars`` method receives ``columns`` as input. ``columns``
must be a string or a list of strings.

E.g:

```python

    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.show()

    # Remove special characters:
    transformer.remove_special_chars(columns=['city', 'country'])

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.show()
```

Original dataFrame:
```
+---------------+--------+----------+
|           city| country|population|
+---------------+--------+----------+
|         Bogotá|Colombia|  37800000|
+---------------+--------+----------+
|       New York|    US@A|  19795791|
+---------------+--------+----------+
|   São Paulo   |  Brazil|  12341418|
+---------------+--------+----------+
|        ~Madrid|   Spain|   6489162|
+---------------+--------+----------+
```
New dataFrame:
```
+---------------+--------+----------+
|           city| country|population|
+---------------+--------+----------+
|         Bogotá|Colombia|  37800000|
+---------------+--------+----------+
|       New York|     USA|  19795791|
+---------------+--------+----------+
|   São Paulo   |  Brazil|  12341418|
+---------------+--------+----------+
|         Madrid|   Spain|   6489162|
+---------------+--------+----------+
```

### Transformer.replace_na(value, columns=None)

This method replace nulls with specified value.

``columns`` argument is an optional list of column names to consider. Columns specified in subset that do not have
matching data type are ignored. For example, if value is a string, and subset contains a non-string column,
then the non-string column is simply ignored. If `columns == "*"` then it will choose all columns.

``value`` argument is the value to replace nulls with. If the value is a dict, then subset is ignored and value
must be a mapping from column name (string) to replacement value. The replacement value must be an int, long,
float, or string.

Let's download a sample data using our amazing `read_url` function.


```python
    # Import optimus
    import optimus as op
    # Instance of Utilities class
    tools = op.Utilities()
    # Reading df from web
    url = "https://raw.githubusercontent.com/ironmussa/Optimus/master/examples/impute_data.csv"
    df = tools.read_url(path=url)
```

If we examine this DF we see that there are some missing values.
```
+---+---+
|  a|  b|
+---+---+
|1.0|NaN|
+---+---+
|2.0|NaN|
+---+---+
|NaN|3.0|
+---+---+
|4.0|4.0|
+---+---+
|5.0|5.0|
+---+---+
```
Remember that we have the `impute_missing` function that lets you choose to use the mean or the median of the columns in
which the missing values are located for your imputation. But with `replace_na` you can say replace the nulls in one,
or all columns in the dataframe with a specific value. For this example we will replace NA with 0's.

```python

    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)
    # Replace NA with 0's
    transformer.replace_na(0.0, columns="*")
    # Show DF
    transformer.show()
```
```
+---+---+
|  a|  b|
+---+---+
|1.0|0.0|
+---+---+
|2.0|0.0|
+---+---+
|0.0|3.0|
+---+---+
|4.0|4.0|
+---+---+
|5.0|5.0|
+---+---+
```

## Contributing to Optimus (based on Webpack)

Contributions go far beyond pull requests and commits. We are very happy to receive any kind of contributions 
including:

* [Documentation](https://github.com/ironmussa/Optimus/tree/master/docs/source) updates, enhancements, designs, or 
bugfixes.
* Spelling or grammar fixes.
* README.md corrections or redesigns.
* Adding unit, or functional [tests](https://github.com/ironmussa/Optimus/tree/master/tests) 
* Triaging GitHub issues -- especially determining whether an issue still persists or is reproducible.
* [Searching #optimusdata on twitter](https://twitter.com/search?q=optimusdata) and helping someone else who needs help.
* [Blogging, speaking about, or creating tutorials](https://hioptimus.com/category/blog/) 
about Optimus and its many features.
* Helping others in our optimus [gitter channel](https://gitter.im/optimuspyspark/Lobby).


## Backers
[[Become a backer](https://opencollective.com/optimus#backer)] and get your image on our README on Github with a link to your site.
[![OpenCollective](https://opencollective.com/optimus/backers/badge.svg)](#backers) 


## Sponsors
[[Become a sponsor](https://opencollective.com/optimus#backer)] and get your image on our README on Github with a link to your site.
[![OpenCollective](https://opencollective.com/optimus/sponsors/badge.svg)](#sponsors)

## Optimus for Spark 1.6.x

Optimus main stable branch will 
work now for Spark 2.2.0 The 1.6.x version is now under maintenance, 
the last tag release for this Spark version is the 0.4.0. We strongly 
suggest that you use the >2.x version of the framework because the 
new improvements and features will be added now on this version.

## Contributors: 

 - Project Manager: [Argenis León](https://github.com/argenisleon) 
 - Original developers: [Andrea Rosales](https://github.com/andrearosr), [Hugo Reyes](https://github.com/hugounavez), [Alberto Bonsanto](https://github.com/Bonsanto)
 - Principal developer and maintainer: [Favio Vázquez](https://github.com/faviovazquez)
 
[![Logo Data](https://www.bbvadata.com/wp-content/uploads/2016/07/bbvada_logo.png)](https://www.bbvadata.com)
 
## License:

Apache 2.0 © [Iron](https://github.com/ironmussa)

[![Logo Iron](https://ironmussa.com/wp-content/uploads/2017/08/iron-svg-2.png)](https://ironmussa.com)

<a href="https://twitter.com/optimus_data"><img src="https://www.shareicon.net/data/256x256/2015/09/01/94063_circle_512x512.png" alt="Optimus twitter" border="0" height="60"></a>
