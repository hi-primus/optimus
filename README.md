
[![Logo Optimus](images/logoOptimus.png)](https://hioptimus.com) 


[![PyPI version](https://badge.fury.io/py/optimuspyspark.svg)](https://badge.fury.io/py/optimuspyspark) [![Build Status](https://travis-ci.org/ironmussa/Optimus.svg?branch=master)](https://travis-ci.org/ironmussa/Optimus) [![Documentation Status](https://readthedocs.org/projects/optimus-ironmussa/badge/?version=latest)](http://optimus-ironmussa.readthedocs.io/en/latest/?badge=latest)  [![built_by iron](https://img.shields.io/badge/built_by-iron-FF69A4.svg)](http://ironmussa.com) [![Updates](https://pyup.io/repos/github/ironmussa/Optimus/shield.svg)](https://pyup.io/repos/github/ironmussa/Optimus/)  [![GitHub release](https://img.shields.io/github/release/ironmussa/optimus.svg)](https://github.com/ironmussa/Optimus/) 
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/02b3ba0fe2b64d6297c6b8320f8b15a7)](https://www.codacy.com/app/argenisleon/Optimus?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=ironmussa/Optimus&amp;utm_campaign=Badge_Grade)
[![Coverage Status](https://coveralls.io/repos/github/ironmussa/Optimus/badge.svg?branch=master)](https://coveralls.io/github/ironmussa/Optimus?branch=master) [![Mentioned in Awesome Data Science](https://awesome.re/mentioned-badge.svg)](https://github.com/bulutyazilim/awesome-datascience)  [![Join the chat at https://gitter.im/optimuspyspark/Lobby](https://badges.gitter.im/optimuspyspark/Lobby.svg)](https://gitter.im/optimuspyspark/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)  


To launch a live notebook server to test optimus using binder or Colab, click on one of the following badges:

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/ironmussa/Optimus/master)
[![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/ironmussa/Optimus/blob/master/examples/10_min_from_spark_to_pandas_with_optimus.ipynb)

Optimus is the missing framework to profile, clean, process and do ML in a distributed fashion using Apache Spark(PySpark).

## Installation (pip):  
  
In your terminal just type  `pip install optimuspyspark`

### Requirements
* Apache Spark>= 2.4.0  
* Python>=3.6  

## Examples 

You can go to the 10 minutes to Optimus [notebook](https://github.com/ironmussa/Optimus/blob/master/examples/10_min_from_spark_to_pandas_with_optimus.ipynb) where you can find the basic to start working. 

Also you can go to the [examples](examples/) folder to found specific notebooks about data cleaning, data munging, profiling, data enrichment and how to create ML and DL models.

Besides check the [Cheat Sheet](https://htmlpreview.github.io/?https://github.com/ironmussa/Optimus/blob/master/docs/cheatsheet/optimus_cheat_sheet.html) 


## Documentation
  
[![Documentation](https://media.readthedocs.com/corporate/img/header-logo.png)](http://docs.hioptimus.com/en/latest/)  
  
## Feedback 
Feedback is what drive Optimus future, so please take a couple of minutes to help shape the Optimus' Roadmap:  https://optimusdata.typeform.com/to/aEnYRY  

Also if you want to a suggestion or feature request use https://github.com/ironmussa/optimus/issues
 
## Start Optimus

```python
from optimus import Optimus
op= Optimus()
```

You also can use an already created Spark session:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('optimus').getOrCreate()
op= Optimus(spark)
```

## Loading data
Now Optimus can load data in csv, json, parquet, avro, excel from a local file or URL.

```python
#csv
df = op.load.csv("examples/data/foo.csv")

#json
# Use a local file
df = op.load.json("examples/data/foo.json")
# Use a url
df = op.load.json("https://raw.githubusercontent.com/ironmussa/Optimus/master/examples/data/foo.json")

# parquet
df = op.load.parquet("examples/data/foo.parquet")

# avro
# df = op.load.avro("examples/data/foo.avro").table(5)

# excel 
df = op.load.excel("examples/data/titanic3.xls")
```

Also you can create a dataframe from scratch
```python
from pyspark.sql.types import *
from datetime import date, datetime

df = op.create.df(
    [
        ("names", "str", True), 
        ("height(ft)","int", True), 
        ("function", "str", True), 
        ("rank", "int", True), 
        ("age","int",True),
        ("weight(t)","float",True),
        ("japanese name", ArrayType(StringType()), True),
        ("last position seen", "str", True),
        ("date arrival", "str", True),
        ("last date seen", "str", True),
        ("attributes", ArrayType(FloatType()), True),
        ("DateType"),
        ("Tiemstamp"),
        ("Cybertronian", "bool", True), 
        ("NullType", "null", True),
    ],
    [
        ("Optim'us", 28, "Leader", 10, 5000000, 4.3, ["Inochi", "Convoy"], "19.442735,-99.201111", "1980/04/10",
         "2016/09/10", [8.5344, 4300.0], date(2016, 9, 10), datetime(2014, 6, 24), True,
         None),
        ("bumbl#ebéé  ", 17, "Espionage", 7, 5000000, 2.0, ["Bumble", "Goldback"], "10.642707,-71.612534", "1980/04/10",
         "2015/08/10", [5.334, 2000.0], date(2015, 8, 10), datetime(2014, 6, 24), True,
         None),
        ("ironhide&", 26, "Security", 7, 5000000, 4.0, ["Roadbuster"], "37.789563,-122.400356", "1980/04/10",
         "2014/07/10", [7.9248, 4000.0], date(2014, 6, 24), datetime(2014, 6, 24), True,
         None),
        ("Jazz", 13, "First Lieutenant", 8, 5000000, 1.80, ["Meister"], "33.670666,-117.841553", "1980/04/10",
         "2013/06/10", [3.9624, 1800.0], date(2013, 6, 24), datetime(2014, 6, 24), True, None),
        ("Megatron", None, "None", 10, 5000000, 5.70, ["Megatron"], None, "1980/04/10", "2012/05/10", [None, 5700.0],
         date(2012, 5, 10), datetime(2014, 6, 24), True, None),
        ("Metroplex_)^$", 300, "Battle Station", 8, 5000000, None, ["Metroflex"], None, "1980/04/10", "2011/04/10",
         [91.44, None], date(2011, 4, 10), datetime(2014, 6, 24), True, None),

    ], infer_schema = True).h_repartition(1)
```

With .table() you hace a beautifull way to show your data. You have extra informacion like column number, column data type and marked white spaces 


```python
df.table()
```
![](images/table.png)


## Cleaning and Processing
  
Optimus V2 was created to make data cleaning a breeze. The API was designed to be super easy to newcomers and very familiar for people that comes from Pandas.
Optimus expand the Spark DataFrame functionality adding .rows and .cols attributes.

For example you can load data from a url, transform and apply some predefined cleaning functions:

```python
# This is a custom function
def func(value, arg):
    return "this was a number"
    
new_df = df\
    .rows.sort("rank","desc")\
    .withColumn('new_age', df.age)\
    .cols.lower(["names","function"])\
    .cols.date_transform("date arrival", "yyyy/MM/dd", "dd-MM-YYYY")\
    .cols.years_between("date arrival", "dd-MM-YYYY", output_cols = "from arrival")\
    .cols.remove_accents("names")\
    .cols.remove_special_chars("names")\
    .rows.drop(df["rank"]>8)\
    .cols.rename(str.lower)\
    .cols.trim("*")\
    .cols.unnest("japanese name", output_cols="other names")\
    .cols.unnest("last position seen",separator=",", output_cols="pos")\
    .cols.drop(["last position seen", "japanese name","date arrival", "cybertronian", "nulltype"])

# .cols.apply_by_dtypes("product",func=func, func_return_type="string", data_type="integer")\
# .cols.replace("product","taaaccoo","taco")\
# .cols.replace("product",["piza","pizzza"],"pizza")\
```

You transform this

```python
df.table()
```
![](images/table1.png)

Into this

```python
new_df.table()
```
![](images/table2.png)

Note that you can use Optimus functions and Spark functions(`.WithColumn()`) and all the df function availables in a Spark Dataframe at the same time. To know about all the Optimus functionality please go to this [notebooks](examples/)

### Custom functions
Spark have multiple ways to transform your data like rdd, Column Expression ,udf and pandas udf. In Optimus we create the `apply()` and `apply_expr` which handle all the implementation complexity.

Here you apply a function to the "billingid" column. Sum 1 and 2 to the current column value. All powered by Pandas UDF
```python
def func(value, args):
    return value + args[0] + args[1]

df.cols.apply("height(ft)",func,"int", [1,2]).table()
```
![](images/table3.png)

If you want to apply a Column Expression use `apply_expr()` like this. In this case we pasa an argument 10 to divide the actual column value

```python
from pyspark.sql import functions as F

def func(col_name, args):
    return F.col(col_name)/20

df.cols.apply("height(ft)", func=func, args=20).table()
```
![](images/table4.png)

You can change the table output back to ascii if you which

```python
op.output("ascii")
```

To return to HTML just:

```python
op.output("html")
```

## Data profiling

Optimus comes with a powerful and unique data profiler. Besides basic and advance stats like min, max, kurtosis, mad etc, 
it also let you know what type of data has every column. For example if a string column have string, integer, float, bool, date Optimus can give you an unique overview about your data. 
Just run `df.profile("*")` to profile all the columns. For more info about the profiler please go to this [notebook](examples/new-api-profiler.ipynb).

Let's load a "big" dataset

```python
df = op.load.csv("https://raw.githubusercontent.com/ironmussa/Optimus/master/examples/data/Meteorite_Landings.csv").h_repartition()
```

### Numeric

```python
op.profiler.run(df, "mass (g)", infer=False)
```

![](images/profiler_numeric.png)

```python
op.profiler.run(df, "name", infer=False)
```

![](images/profiler.png)

For dates data types Optimus can give you extra data
```python
op.profiler.run(df, "year", infer=True)
```

![](images/profiler1.png)

## Plots
Besides histograms, frequency plots you also have scatter plots and box plots. All powered by Apache by pyspark

```python
df = op.load.excel("examples/data/titanic3.xls")
df = df.rows.drop_na(["age","fare"])
```

```python
df.plot.hist("fare", output="image", path="images/hist.png")
```

```python
df.plot.frequency("age", output="image", path="images/frequency.png")
```

```python
df.plot.scatter(["fare", "age"], buckets=30, output="image", path="images/scatter.png")
```

```python
df.plot.box("age", output="image", path="images/box.png")
```
```python
df.plot.correlation(["age","fare","survived"])
```
### Using other plotting libraries


Optimus has a tiny API so you can use any plotting library. For example, you can use df.cols.scatter(), df.cols.frequency(), df.cols.boxplot() or df.cols.hist() to output a JSON that you can process to adapt the data to any plotting library.


## Outliers


### Get the ouliers using iqr

```python
df.outliers.iqr("age").select().table()
```
![](images/table5.png)

### Remove the outliers using iqr

```python
df.outliers.iqr("age").drop().table()
```
![](images/table6.png)

### You can also use z_score, modified_z_score or mad




```python
df.outliers.z_score("age", threshold=2).drop()
df.outliers.modified_z_score("age", threshold = 2).drop()
df.outliers.mad("age", threshold = 2).drop()
```

## Database connection
Optimus have handy tools to connect to databases and extract informacion. Optimus can handle Redshift, postgres and mysql




```
#Put your db credentials here
db =  op.connect(
    db_type="redshift",
    url="iron.******.us-east-1.redshift.amazonaws.com",
    database= "******",
    user= "******",
    password = "******",
    port="5439")
    
#Show all tables names
db.tables()

#Show a summary of every table
db.table.show("*",20)

#Get a table as dataframe
db.table_to_df("tablename")
```


## Data enrichment

You can connect to any external API to enrich your data using Optimus. Optimus use MongoDB to download the data and then merge it with the Spark Dataframe. You need to install MongoDB

Let's load a tiny dataset we can enrich

```python
df = op.load.json("https://raw.githubusercontent.com/ironmussa/Optimus/master/examples/data/foo.json")
```

```python
import requests

def func_request(params):
    # You can use here whatever header or auth info you need to send. 
    # For more information see the requests library
    
    url= "https://jsonplaceholder.typicode.com/todos/" + str(params["id"])
    return requests.get(url)

def func_response(response):
    # Here you can parse de response
    return response["title"]


e = op.enrich()

df_result = e.run(df, func_request, func_response, calls= 60, period = 60, max_tries = 8)
```

```python
df_result.table()
```
![](images/table7.png)

## Machine Learning 

Machine Learning is one of the last steps, and the goal for most Data Science WorkFlows.

Apache Spark created a library called MLlib where they coded great algorithms for Machine Learning. Now
with the ML library we can take advantage of the Dataframe API and its optimization to create easily
Machine Learning Pipelines.

Even though this task is not extremely hard, is not easy. The way most Machine Learning models work on Spark
are not straightforward, and they need lots feature engineering to work. That's why we created the feature engineering
section inside Optimus.


One of the best "tree" models for machine learning is Random Forest. What about creating a RF model with just
one line? With Optimus is really easy.

```python
df_cancer =op.load.csv("https://raw.githubusercontent.com/ironmussa/Optimus/master/tests/data_cancer.csv")
```

```python
columns = ['diagnosis', 'radius_mean', 'texture_mean', 'perimeter_mean', 'area_mean', 'smoothness_mean',
           'compactness_mean', 'concavity_mean', 'concave points_mean', 'symmetry_mean',
           'fractal_dimension_mean']
df_predict, rf_model = op.ml.random_forest(df_cancer, columns, "diagnosis")
```

This will create a DataFrame with the predictions of the Random Forest model.

So lets see the prediction compared with the actual label:


```python
df_predict.cols.select(["label","prediction"]).table()
```
![](images/table8.png)

The rf_model variable contains the Random Forest model for analysis.
 
## Contributing to Optimus
Contributions go far beyond pull requests and commits. We are very happy to receive any kind of contributions   
including:  
  
* [Documentation](https://github.com/ironmussa/Optimus/tree/master/docs/source) updates, enhancements, designs, or   bugfixes.  
* Spelling or grammar fixes.  
* README.md corrections or redesigns.  
* Adding unit, or functional [tests](https://github.com/ironmussa/Optimus/tree/master/tests)   
* Triaging GitHub issues -- especially determining whether an issue still persists or is reproducible.  
* [Searching #optimusdata on twitter](https://twitter.com/search?q=optimusdata) and helping someone else who needs help.  
* [Blogging, speaking about, or creating tutorials](https://hioptimus.com/category/blog/)   about Optimus and its many features.  
* Helping others in our optimus [gitter channel](https://gitter.im/optimuspyspark/Lobby).    
  
## Backers  
[[Become a backer](https://opencollective.com/optimus#backer)] and get your image on our README on Github with a link to your site.  
[![OpenCollective](https://opencollective.com/optimus/backers/badge.svg)](#backers)   


## Sponsors  
[[Become a sponsor](https://opencollective.com/optimus#backer)] and get your image on our README on Github with a link to your site.  
[![OpenCollective](https://opencollective.com/optimus/sponsors/badge.svg)](#sponsors)  
  
## Optimus for Spark 1.6.x  
  
Optimus main stable branch will work now for Spark 2.3.1 The 1.6.x version is now under maintenance, the last tag release for this Spark version is the 0.4.0. We strongly suggest that you use the >2.x version of the framework because the new improvements and features will be added now on this version.
## Core Team
Argenis Leon and Favio Vazquez

## Contributors:
Here is the amazing people that make Optimus possible:
  
[![0](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/images/0)](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/links/0)[![1](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/images/1)](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/links/1)[![2](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/images/2)](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/links/2)[![3](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/images/3)](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/links/3)[![4](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/images/4)](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/links/4)[![5](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/images/5)](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/links/5)[![6](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/images/6)](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/links/6)[![7](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/images/7)](https://sourcerer.io/fame/FavioVazquez/ironmussa/Optimus/links/7)    
   
## License:  
  
Apache 2.0 © [Iron](https://github.com/ironmussa)  
  
[![Logo Iron](https://iron-ai.com/wp-content/uploads/2017/08/iron-svg-2.png)](https://ironmussa.com)  
  
<a href="https://twitter.com/optimus_data"><img src="https://www.shareicon.net/data/256x256/2015/09/01/94063_circle_512x512.png" alt="Optimus twitter" border="0" height="60"></a>


