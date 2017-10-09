Optimus (By Iron_)
=======

.. image:: images/logoOptimus.png


.. _Iron: https://github.com/ironmussa

Description
------------

Optimus is the missing framework for cleaning and pre-processing data in a distributed fashion. It uses all the power of `Apache Spark`_ (optimized via Catalyst_) to do it. It implements several handy tools for data wrangling and munging that will make your life much easier. The first obvious advantage over any other public data cleaning library or framerwork is that it will work on your laptop or your big cluster, and second, it is amazingly easy to install, use and understand.

.. _Apache Spark: https://spark.apache.

.. _Catalyst: https://static.javadoc.io/org.apache.spark/spark-catalyst_2.10/1.0.1/index.html#org.apache.spark.sql.catalyst.package

Requirements
------------

-  Apache Spark 2.2.0
-  Python 3.5

Installation
-------------

In your terminal just type:

.. code:: bash

  pip install optimuspyspark


DataFrameTransformer
--------------------

DataFrameTransformer is a powerful and flexible library to make
dataFrame transformations in Apache Spark (pySpark).

This library contains several transformation functions based in spark
original modules but with some features added to facilitate its use.

Since functions in this library are mounted in the Spark SQL Context, it
offers not only the high performance of original Spark SQL functions but
also an easier usability.

DataFrameProfiler class
-----------------------

This class makes a profile for a given dataframe and its different general features.
Based on spark-df-profiling by Julio Soto.

Initially it is a good idea to see a general view of the DataFrame to be analyzed.

Lets assume you have the following dataset, called foo.csv, in your current directory:

+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| id | firstName            | lastName    | billingId | product    | price | birth      | dummyCol |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 1  | Luis                 | Alvarez$$%! | 123       | Cake       | 10    | 1980/07/07 | never    |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 2  | André                | Ampère      | 423       | piza       | 8     | 1950/07/08 | gonna    |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 3  | NiELS                | Böhr//((%%  | 551       | pizza      | 8     | 1990/07/09 | give     |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 4  | PAUL                 | dirac$      | 521       | pizza      | 8     | 1954/07/10 | you      |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 5  | Albert               | Einstein    | 634       | pizza      | 8     | 1990/07/11 | up       |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 6  | Galileo              | GALiLEI     | 672       | arepa      | 5     | 1930/08/12 | never    |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 7  | CaRL                 | Ga%%%uss    | 323       | taco       | 3     | 1970/07/13 | gonna    |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 8  | David                | H$$$ilbert  | 624       | taaaccoo   | 3     | 1950/07/14 | let      |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 9  | Johannes             | KEPLER      | 735       | taco       | 3     | 1920/04/22 | you      |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 10 | JaMES                | M$$ax%%well | 875       | taco       | 3     | 1923/03/12 | down     |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 11 | Isaac                | Newton      | 992       | pasta      | 9     | 1999/02/15 | never    |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 12 | Emmy%%               | Nöether$    | 234       | pasta      | 9     | 1993/12/08 | gonna    |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 13 | Max!!!               | Planck!!!   | 111       | hamburguer | 4     | 1994/01/04 | run      |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 14 | Fred                 | Hoy&&&le    | 553       | pizzza     | 8     | 1997/06/27 | around   |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 15 | (((   Heinrich ))))) | Hertz       | 116       | pizza      | 8     | 1956/11/30 | and      |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 16 | William              | Gilbert###  | 886       | BEER       | 2     | 1958/03/26 | desert   |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 17 | Marie                | CURIE       | 912       | Rice       | 1     | 2000/03/22 | you      |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 18 | Arthur               | COM%%%pton  | 812       | 110790     | 5     | 1899/01/01 | #        |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 19 | JAMES                | Chadwick    | 467       | null       | 10    | 1921/05/03 | #        |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+

.. code:: python

    # Import optimus
    import optimus as op
    #Import os module for system tools
    import os

    # Reading dataframe. os.getcwd() returns de current directory of the notebook
    # 'file:///' is a prefix that specifies the type of file system used, in this
    # case, local file system (hard drive of the pc) is used.
    filePath = "file:///" + os.getcwd() + "/foo.csv"

    df = tools.read_csv(path=filePath,
                                sep=',')

    # Instance of profiler class
    profiler = op.DataFrameProfiler(df)
    profiler.profiler()

This overview presents basic information about the DataFrame, like number of variable it has,
how many are missing values and in which column, the types of each varaible, also some statistical information
that describes the variable plus a frecuency plot. table that specifies the existing datatypes in each column
dataFrame and other features. Also, for this particular case, the table of dataType is shown in order to visualize
a sample of column content.

DataFrameAnalyzer class
-----------------------

DataFrameAnalyzer class analyze dataType of rows in each columns of
dataFrames.

**DataFrameAnalyzer methods**

-  DataFrameAnalyzer.column_analyze(column_list, plots=True, values_bar=True, print_type=False, num_bars=10)
-  DataFrameAnalyzer.plot_hist(df_one_col, hist_dict, type_hist, num_bars=20, values_bar=True)
-  DataFrameAnalyzer.get_categorical_hist(df_one_col, num_bars)
-  DataFrameAnalyzer.get_numerical_hist(df_one_col, num_bars)
-  DataFrameAnalyzer.unique_values_col(column)
-  DataFrameAnalyzer.write_json(json_cols, path_to_json_file)
-  DataFrameAnalyzer.get_frequency(columns, sort_by_count=True)

Lets assume you have the following dataset, called foo.csv, in your current directory:

+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| id | firstName            | lastName    | billingId | product    | price | birth      | dummyCol |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 1  | Luis                 | Alvarez$$%! | 123       | Cake       | 10    | 1980/07/07 | never    |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 2  | André                | Ampère      | 423       | piza       | 8     | 1950/07/08 | gonna    |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 3  | NiELS                | Böhr//((%%  | 551       | pizza      | 8     | 1990/07/09 | give     |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 4  | PAUL                 | dirac$      | 521       | pizza      | 8     | 1954/07/10 | you      |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 5  | Albert               | Einstein    | 634       | pizza      | 8     | 1990/07/11 | up       |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 6  | Galileo              | GALiLEI     | 672       | arepa      | 5     | 1930/08/12 | never    |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 7  | CaRL                 | Ga%%%uss    | 323       | taco       | 3     | 1970/07/13 | gonna    |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 8  | David                | H$$$ilbert  | 624       | taaaccoo   | 3     | 1950/07/14 | let      |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 9  | Johannes             | KEPLER      | 735       | taco       | 3     | 1920/04/22 | you      |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 10 | JaMES                | M$$ax%%well | 875       | taco       | 3     | 1923/03/12 | down     |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 11 | Isaac                | Newton      | 992       | pasta      | 9     | 1999/02/15 | never    |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 12 | Emmy%%               | Nöether$    | 234       | pasta      | 9     | 1993/12/08 | gonna    |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 13 | Max!!!               | Planck!!!   | 111       | hamburguer | 4     | 1994/01/04 | run      |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 14 | Fred                 | Hoy&&&le    | 553       | pizzza     | 8     | 1997/06/27 | around   |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 15 | (((   Heinrich ))))) | Hertz       | 116       | pizza      | 8     | 1956/11/30 | and      |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 16 | William              | Gilbert###  | 886       | BEER       | 2     | 1958/03/26 | desert   |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 17 | Marie                | CURIE       | 912       | Rice       | 1     | 2000/03/22 | you      |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 18 | Arthur               | COM%%%pton  | 812       | 110790     | 5     | 1899/01/01 | #        |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+
| 19 | JAMES                | Chadwick    | 467       | null       | 10    | 1921/05/03 | #        |
+----+----------------------+-------------+-----------+------------+-------+------------+----------+

The following code shows how to instantiate the class to analyze a dataFrame:

.. code:: python

    # Import optimus
    import optimus as op
    # Instance of Utilities class
    tools = op.Utilities()

    # Reading dataframe. os.getcwd() returns de current directory of the notebook
    # 'file:///' is a prefix that specifies the type of file system used, in this
    # case, local file system (hard drive of the pc) is used.
    filePath = "file:///" + os.getcwd() + "/foo.csv"
  
    df = tools.read_csv(path=filePath, sep=',')

    analyzer = op.DataFrameAnalyzer(df=df,pathFile=filePath)

Methods
--------

Analyzer.column_analyze(column_list, plots=True, values_bar=True, print_type=False, num_bars=10)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This function counts the number of registers in a column that are numbers (integers, floats) and the number of string registers.

Input:

``column_list``: A list or a string column name.

``plots``: Can be True or False. If true it will output the predefined plots.

``values_bar (optional)``: Can be True or False. If it is True, frequency values are placed over each bar.

``print_type (optional)``: Can be one of the following strings: 'integer', 'string', 'float'. Depending of what string
is provided, a list of distinct values of that type is printed.

``num_bars``: number of bars printed in histogram

The method outputs a list containing the number of the different datatypes [nulls, strings, integers, floats].

Example: 

.. code:: python

  analyzer.column_analyze("*", plots=False, values_bar=True, print_type=False, num_bars=10)
  
+-----------+----------+------------+----------------------+
|           |          |            | Column name: id      |
+-----------+----------+------------+----------------------+
|           |          |            | Column datatype: int |
+-----------+----------+------------+----------------------+
| Datatype  | Quantity | Percentage |                      |
+-----------+----------+------------+----------------------+
| None      | 0        | 0.00 %     |                      |
+-----------+----------+------------+----------------------+
| Empty str | 0        | 0.00 %     |                      |
+-----------+----------+------------+----------------------+
| String    | 0        | 0.00 %     |                      |
+-----------+----------+------------+----------------------+
| Integer   | 19       | 100.00 %   |                      |
+-----------+----------+------------+----------------------+
| Float     | 0        | 0.00 %     |                      |
+-----------+----------+------------+----------------------+

Min value:  1

Max value:  19

end of __analyze 4.059180021286011

+-----------+----------+------------+-------------------------+
|           |          |            | Column name: firstName  |
+-----------+----------+------------+-------------------------+
|           |          |            | Column datatype: string |
+-----------+----------+------------+-------------------------+
| Datatype  | Quantity | Percentage |                         |
+-----------+----------+------------+-------------------------+
| None      | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+
| Empty str | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+
| String    | 19       | 100.00 %   |                         |
+-----------+----------+------------+-------------------------+
| Integer   | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+
| Float     | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+

end of __analyze 1.1431787014007568

+-----------+----------+------------+-------------------------+
|           |          |            | Column name: lastName   |
+-----------+----------+------------+-------------------------+
|           |          |            | Column datatype: string |
+-----------+----------+------------+-------------------------+
| Datatype  | Quantity | Percentage |                         |
+-----------+----------+------------+-------------------------+
| None      | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+
| Empty str | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+
| String    | 19       | 100.00 %   |                         |
+-----------+----------+------------+-------------------------+
| Integer   | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+
| Float     | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+

end of __analyze 0.9663524627685547

+-----------+----------+------------+------------------------+
|           |          |            | Column name: billingId |
+-----------+----------+------------+------------------------+
|           |          |            | Column datatype: int   |
+-----------+----------+------------+------------------------+
| Datatype  | Quantity | Percentage |                        |
+-----------+----------+------------+------------------------+
| None      | 0        | 0.00 %     |                        |
+-----------+----------+------------+------------------------+
| Empty str | 0        | 0.00 %     |                        |
+-----------+----------+------------+------------------------+
| String    | 0        | 0.00 %     |                        |
+-----------+----------+------------+------------------------+
| Integer   | 19       | 100.00 %   |                        |
+-----------+----------+------------+------------------------+
| Float     | 0        | 0.00 %     |                        |
+-----------+----------+------------+------------------------+

Min value:  111

Max value:  992

end of __analyze 4.292513847351074

+-----------+----------+------------+-------------------------+
|           |          |            | Column name: product    |
+-----------+----------+------------+-------------------------+
|           |          |            | Column datatype: string |
+-----------+----------+------------+-------------------------+
| Datatype  | Quantity | Percentage |                         |
+-----------+----------+------------+-------------------------+
| None      | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+
| Empty str | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+
| String    | 18       | 94.74 %    |                         |
+-----------+----------+------------+-------------------------+
| Integer   | 1        | 5.26 %     |                         |
+-----------+----------+------------+-------------------------+
| Float     | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+

end of __analyze 1.180891990661621

+-----------+----------+------------+------------------------+
|           |          |            | Column name: price    |
+-----------+----------+------------+------------------------+
|           |          |            | Column datatype: int   |
+-----------+----------+------------+------------------------+
| Datatype  | Quantity | Percentage |                        |
+-----------+----------+------------+------------------------+
| None      | 0        | 0.00 %     |                        |
+-----------+----------+------------+------------------------+
| Empty str | 0        | 0.00 %     |                        |
+-----------+----------+------------+------------------------+
| String    | 0        | 0.00 %     |                        |
+-----------+----------+------------+------------------------+
| Integer   | 19       | 100.00 %   |                        |
+-----------+----------+------------+------------------------+
| Float     | 0        | 0.00 %     |                        |
+-----------+----------+------------+------------------------+

Min value:  1

Max value:  10

end of __analyze 4.364053964614868

+-----------+----------+------------+-------------------------+
|           |          |            | Column name: birth      |
+-----------+----------+------------+-------------------------+
|           |          |            | Column datatype: string |
+-----------+----------+------------+-------------------------+
| Datatype  | Quantity | Percentage |                         |
+-----------+----------+------------+-------------------------+
| None      | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+
| Empty str | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+
| String    | 19       | 100.00 %   |                         |
+-----------+----------+------------+-------------------------+
| Integer   | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+
| Float     | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+

end of __analyze 0.9144570827484131
  
+-----------+----------+------------+-------------------------+
|           |          |            | Column name: dummyCol   |
+-----------+----------+------------+-------------------------+
|           |          |            | Column datatype: string |
+-----------+----------+------------+-------------------------+
| Datatype  | Quantity | Percentage |                         |
+-----------+----------+------------+-------------------------+
| None      | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+
| Empty str | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+
| String    | 19       | 100.00 %   |                         |
+-----------+----------+------------+-------------------------+
| Integer   | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+
| Float     | 0        | 0.00 %     |                         |
+-----------+----------+------------+-------------------------+

end of __analyze 0.9651758670806885

Total execution time:  17.98968768119812

+-----------+------------------+---------------------+
|           |                  | General Description |
+-----------+------------------+---------------------+
| Features  | Name or Quantity |                     |
+-----------+------------------+---------------------+
| File Name | foo.csv          |                     |
+-----------+------------------+---------------------+
| Columns   | 8                |                     |
+-----------+------------------+---------------------+
| Rows      | 19               |                     |
+-----------+------------------+---------------------+

Analyzer.get_categorical_hist(df_one_col, num_bars)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This function analyzes a dataframe of a single column (only string type columns) and returns a dictionary with bins and values of frequency.

Input:

``df_one_col``:One column dataFrame.

``num_bars``: Number of bars or histogram bins.

The method outputs a dictionary with bins and values of frequency for only type strings colmuns.

Example:

Lets say we want to plot a histogram of frecuencies for the ``product`` column. We first need to obtain the dictionary of the frecuencies for each one. This is what this function does for categorical data. Remember that if you run the ``columnAnalyze()`` method with ``plots = True`` this is done for you.

.. code:: python 

  productDf = analyzer.get_data_frame.select("product") #or df.select("product")
  hist_dictPro = analyzer.get_categorical_hist(df_one_col=productDf, num_bars=10)
  print(hist_dictPro)

.. code:: python
    
    #Output
    """[{'cont': 4, 'value': 'pizza'}, {'cont': 3, 'value': 'taco'}, {'cont': 2, 'value': 'pasta'}, {'cont': 1, 'value':         'hamburguer'}, {'cont': 1, 'value': 'BEER'}, {'cont': 1, 'value': 'Rice'}, {'cont': 1, 'value': 'piza'}, {'cont': 1,         'value': 'Cake'}, {'cont': 1, 'value': 'arepa'}, {'cont': 1, 'value': '110790'}]"""

Now that we have the dictionary we just need to call ``plot_hist()``.

Analyzer.get_numerical_hist(df_one_col, num_bars)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This function analyzes a dataframe of a single column (only numerical columns) and returns a dictionary with bins and values of frequency.

Input:

``df_one_col``:One column dataFrame.

``num_bars``: Number of bars or histogram bins.

The method outputs a dictionary with bins and values of frequency for only numerical colmuns.

Example:

Lets say we want to plot a histogram of frecuencies for the ``price`` column. We first need to obtain the dictionary of the frecuencies for each one. This is what this function does for numerical data. Remember that if you run the ``columnAnalyze()`` method with ``plots = True`` this is done for you.

.. code:: python

  priceDf = analyzer.get_data_frame.select("price") #or df.select("price")
  hist_dictPri = analyzer.get_numerical_hist(df_one_col=priceDf, num_bars=10)
  print(hist_dictPri)
  
.. code:: python

  #Output
  """[{'cont': 2, 'value': 9.55}, {'cont': 2, 'value': 8.649999999999999}, {'cont': 6, 'value': 7.749999999999999}, {'cont':   2, 'value': 5.05}, {'cont': 1, 'value': 4.1499999999999995}, {'cont': 4, 'value': 3.25}, {'cont': 1, 'value':               2.3499999999999996}, {'cont': 1, 'value': 1.45}]"""


Analyzer.plot_hist(df_one_col, hist_dict, type_hist, num_bars=20, values_bar=True)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This function builds the histogram (bins) of a categorical or numerical column dataframe.

Input: 

``df_one_col``: A dataFrame of one column.

``hist_dict``: Python dictionary with histogram values.

``type_hist``: type of histogram to be generated, numerical or categorical.

``num_bars``: Number of bars in histogram.

``values_bar``: If values_bar is True, values of frequency are plotted over bars.
        
The method outputs a plot of the histogram for a categorical or numerical column.

Example:

.. code:: python

  # For a categorical DF
  analyzer.plot_hist(df_one_col=productDf,hist_dict= hist_dictPro, type_hist='categorical')
  
.. image:: images/productHist.png

.. code:: python

  # For a numerical DF
  analyzer.plot_hist(df_one_col=priceDf,hist_dict= hist_dictPri, type_hist='categorical')
  
.. image:: images/priceHist.png

Analyzer.unique_values_col(column)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This function counts the number of values that are unique and also the total number of values. Then, returns the values obtained.

Input:

``column``: Name of column dataFrame, this argument must be string type.

The method outputs a dictionary of values counted, as an example: ``{'unique': 10, 'total': 15}``.

Example:

.. code:: python

  print(analyzer.unique_values_col("product"))
  print(analyzer.unique_values_col("price"))
  
.. code:: python 

  #Output
  {'unique': 13, 'total': 19} 
  {'unique': 8, 'total': 19}

Analyzer.write_json(json_cols, path_to_json_file)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This functions outputs a JSON for the DataFrame in the specified path.

Input:

``json_cols``: Dictionary that represents the dataframe.

``path_to_json_file``: Specified path to write the returned JSON.

The method outputs the dataFrame as a JSON. To use it in a simple way first run 

.. code:: python

  json_cols = analyzer.column_analyze(column_list="*", print_type=False, plots=False)

And you will have the desired dictionary to pass to the write_json function.

Example:

.. code:: python

  analyzer.write_json(json_cols=json_cols, path_to_json_file= os.getcwd() + "/foo.json")

Analyzer.get_frequency(self, columns, sort_by_count=True)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This function gets the frequencies for values inside the specified columns.

Input:

``columns``: String or List of columns to analyze

``sort_by_count``: Boolean if true the counts will be sort desc.

The method outputs a Spark Dataframe with counts per existing values in each column.

Tu use it, first lets create a sample DataFrame:

.. code:: python

    import random
    import optimus as op
    from pyspark.sql.types import StringType, StructType, IntegerType, FloatType, DoubleType, StructField

    schema = StructType(
            [
            StructField("strings", StringType(), True),
            StructField("integers", IntegerType(), True),
            StructField("integers2", IntegerType(), True),
            StructField("floats",  FloatType(), True),
            StructField("double",  DoubleType(), True)
            ]
    )

    size = 200
    # Generating strings column:
    foods = ['    pizza!       ', 'pizza', 'PIZZA;', 'pizza', 'pízza¡', 'Pizza', 'Piz;za']
    foods = [foods[random.randint(0,6)] for count in range(size)]
    # Generating integer column:
    num_col_1 = [random.randint(0,9) for number in range(size)]
    # Generating integer column:
    num_col_2 = [random.randint(0,9) for number in range(size)]
    # Generating integer column:
    num_col_3 = [random.random() for number in range(size)]
    # Generating integer column:
    num_col_4 = [random.random() for number in range(size)]

    # Building DataFrame
    df = op.spark.createDataFrame(list(zip(foods, num_col_1, num_col_2, num_col_3, num_col_4)),schema=schema)

    # Instantiate Analyzer
    analyzer = op.DataFrameAnalyzer(df)

    # Get frequency DataFrame
    df_counts = analyzer.get_frequency(["strings", "integers"], True)

And you will get (note that these are random generated values):

+-----------------+-----+
|          strings|count|
+-----------------+-----+
|            pizza|   48|
+-----------------+-----+
|           Piz;za|   38|
+-----------------+-----+
|            Pizza|   37|
+-----------------+-----+
|           pízza¡|   29|
+-----------------+-----+
|    pizza!       |   25|
+-----------------+-----+
|           PIZZA;|   23|
+-----------------+-----+

+--------+-----+
|integers|count|
+--------+-----+
|       8|   31|
+--------+-----+
|       5|   24|
+--------+-----+
|       1|   24|
+--------+-----+
|       9|   20|
+--------+-----+
|       6|   20|
+--------+-----+
|       2|   19|
+--------+-----+
|       3|   19|
+--------+-----+
|       0|   17|
+--------+-----+
|       4|   14|
+--------+-----+
|       7|   12|
+--------+-----+

DataFrameTransformer class
--------------------------

-  DataFrameTransformer(df)

**DataFrameTransformer methods**

* **Column operations**:

  - DataFrameTransformer.drop_col(columns)
  - DataFrameTransformer.replace_col(search, changeTo, columns)
  - DataFrameTransformer.keep_col(columns)
  - DataFrameTransformer.rename_col(column, newName)
  - DataFrameTransformer.move_col(column, ref_col, position)

* **Row operations** :

  - DataFrameTransformer.dropRow(columns)
  - DataFrameTransformer.delete_row(func)

* **String operations**:

  - DataFrameTransformer.trim_col(columns)
  - DataFrameTransformer.clear_accents(columns)
  - DataFrameTransformer.lookup(column, list_str, str_to_replace)
  - DataFrameTransformer.remove_special_chars(columns)
  - DataFrameTransformer.date_transform(column, dateFormat)

* **General operation function**: 

  - DataFrameTransformer.set_col(columns, func, dataType)

* **Others**:
  - DataFrameTransformer.count_items(col_id, col_search, new_col_feature, search_string)
  - DataFrameTransformer.age_calculate(column)

DataFrameTransformer class receives a dataFrame as an argument. This
class has all methods listed above.

Note: Every possible transformation make changes over this dataFrame and
overwrites it.

The following code shows how to instantiate the class to transform a
dataFrame:

.. code:: python

    # Importing sql types
    from pyspark.sql.types import StringType, IntegerType, StructType, StructField
    # Importing optimus
    import optimus as op

    # Building a simple dataframe:
    schema = StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("population", IntegerType(), True)])

    countries = ['Japan', 'USA', 'France', 'Spain']
    cities = ['Tokyo', 'New York', '   Paris   ', 'Madrid']
    population = [37800000,19795791,12341418,6489162]

    # Dataframe:
    df = op.spark.createDataFrame(list(zip(cities, countries, population)), schema=schema)

    # DataFrameTransformer Instantiation:
    transformer = op.DataFrameTransformer(df)

    transformer.show()
    
Output:
 
 +-----------+-------+----------+
 |       city|country|population|
 +-----------+-------+----------+
 |      Tokyo|  Japan|  37800000|
 +-----------+-------+----------+
 |   New York|    USA|  19795791|
 +-----------+-------+----------+
 |   Paris   | France|  12341418|
 +-----------+-------+----------+
 |     Madrid|  Spain|   6489162|
 +-----------+-------+----------+
 
Methods
-------

Transformer.trim_col(columns)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This methods cut left and right extra spaces in column strings provided
by user.

``columns`` argument is expected to be a string o a list of column names.

If a string ``"*"`` is provided, the method will do the trimming
operation in whole dataframe.

**Example:**

.. code:: python

    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.show()

    # Triming string blank spaces:
    transformer.trim_col("*")

    # Printing trimmed dataFrame:
    print('Trimmed dataFrame:')
    transformer.show()

Original dataFrame:

+-----------+-------+----------+
|       city|country|population|
+-----------+-------+----------+
|      Tokyo|  Japan|  37800000|
+-----------+-------+----------+
|   New York|    USA|  19795791|
+-----------+-------+----------+
|   Paris   | France|  12341418|
+-----------+-------+----------+
|     Madrid|  Spain|   6489162|
+-----------+-------+----------+

Trimmed dataFrame:

+--------+-------+----------+
|    city|country|population|
+--------+-------+----------+
|   Tokyo|  Japan|  37800000|
+--------+-------+----------+
|New York|    USA|  19795791|
+--------+-------+----------+
|   Paris| France|  12341418|
+--------+-------+----------+
|  Madrid|  Spain|   6489162|
+--------+-------+----------+

Transformer.drop_col(columns)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method eliminate the list of columns provided by user.

``columns`` argument is expected to be a string or a list of columns
names.

**Example:**

.. code:: python

    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.show()

    # drop column specified:
    transformer.drop_col("country")

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.show()


Original dataFrame:

+-----------+-------+----------+
|       city|country|population|
+-----------+-------+----------+
|      Tokyo|  Japan|  37800000|
+-----------+-------+----------+
|   New York|    USA|  19795791|
+-----------+-------+----------+
|   Paris   | France|  12341418|
+-----------+-------+----------+
|     Madrid|  Spain|   6489162|
+-----------+-------+----------+

New dataFrame:

+-----------+----------+
|       city|population|
+-----------+----------+
|      Tokyo|  37800000|
+-----------+----------+
|   New York|  19795791|
+-----------+----------+
|   Paris   |  12341418|
+-----------+----------+
|     Madrid|   6489162|
+-----------+----------+

Transformer.keep_col(columns)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method keep only columns specified by user with ``columns``
argument in DataFrame.

``columns`` argument is expected to be a string or a list of columns names.

**Example:**

.. code:: python

    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.show()

    # Keep columns specified by user:
    transformer.keep_col(['city', 'population'])

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.show()

Original dataFrame:

+-----------+-------+----------+
|       city|country|population|
+-----------+-------+----------+
|      Tokyo|  Japan|  37800000|
+-----------+-------+----------+
|   New York|    USA|  19795791|
+-----------+-------+----------+
|   Paris   | France|  12341418|
+-----------+-------+----------+
|     Madrid|  Spain|   6489162|
+-----------+-------+----------+

New dataFrame:
    
+-----------+----------+
|       city|population|
+-----------+----------+
|      Tokyo|  37800000|
+-----------+----------+
|   New York|  19795791|
+-----------+----------+
|   Paris   |  12341418|
+-----------+----------+
|     Madrid|   6489162|
+-----------+----------+

Transformer.replace_col(search, changeTo, columns)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method search the ``search`` value argument in the DataFrame
columns specified in ``columns`` to replace it for ``changeTo`` value.

``search`` and ``changeTo`` are expected to be numbers and same dataType
('integer', 'string', etc) each other. ``columns`` argument is expected
to be a string or list of string column names.

If ``columns = '*'`` is provided, searching and replacing action is made
in all columns of DataFrame that have same dataType of ``search`` and
``changeTo``.

**Example:**

.. code:: python

    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.show()

    # Replace values in columns specified by user:
    transformer.replace_col(search='Tokyo', changeTo='Maracaibo', columns='city')

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.show()

Original dataFrame:

+-----------+-------+----------+
|       city|country|population|
+-----------+-------+----------+
|      Tokyo|  Japan|  37800000|
+-----------+-------+----------+
|   New York|    USA|  19795791|
+-----------+-------+----------+
|   Paris   | France|  12341418|
+-----------+-------+----------+
|     Madrid|  Spain|   6489162|
+-----------+-------+----------+

New dataFrame:

+-----------+-------+----------+
|       city|country|population|
+-----------+-------+----------+
|  Maracaibo|  Japan|  37800000|
+-----------+-------+----------+
|   New York|    USA|  19795791|
+-----------+-------+----------+
|   Paris   | France|  12341418|
+-----------+-------+----------+
|     Madrid|  Spain|   6489162|
+-----------+-------+----------+

Transformer.delete_row(func)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method deletes rows in columns according to condition provided by
user.

``delete_row`` method receives a function ``func`` as an input parameter.

``func`` is required to be a ``lambda`` function, which is a native
python feature.

**Example 1:**

.. code:: python


    # Importing sql functions
    from pyspark.sql.functions import col

    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.show()

    # Replace values in columns specified by user:
    func = lambda pop: (pop > 6500000) & (pop <= 30000000)
    transformer.delete_row(func(col('population')))

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.show()

Original dataFrame:

+-----------+-------+----------+
|       city|country|population|
+-----------+-------+----------+
|      Tokyo|  Japan|  37800000|
+-----------+-------+----------+
|   New York|    USA|  19795791|
+-----------+-------+----------+
|   Paris   | France|  12341418|
+-----------+-------+----------+
|     Madrid|  Spain|   6489162|
+-----------+-------+----------+

New dataFrame:

+-----------+-------+----------+
|       city|country|population|
+-----------+-------+----------+
|   New York|    USA|  19795791|
+-----------+-------+----------+
|   Paris   | France|  12341418|
+-----------+-------+----------+

**Example 2:**

.. code:: python


    # Importing sql functions
    from pyspark.sql.functions import col

    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.show()

    # Delect rows where Tokyo isn't found in city
    # column or France isn't found in country column:
    func = lambda city, country: (city == 'Tokyo')  | (country == 'France')
    transformer.delete_row(func(col('city'), col('country')))

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.show()

Original dataFrame:

+-----------+-------+----------+
|       city|country|population|
+-----------+-------+----------+
|      Tokyo|  Japan|  37800000|
+-----------+-------+----------+
|   New York|    USA|  19795791|
+-----------+-------+----------+
|   Paris   | France|  12341418|
+-----------+-------+----------+
|     Madrid|  Spain|   6489162|
+-----------+-------+----------+

New dataFrame:
    
+-----------+-------+----------+
|       city|country|population|
+-----------+-------+----------+
|      Tokyo|  Japan|  37800000|
+-----------+-------+----------+
|   Paris   | France|  12341418|
+-----------+-------+----------+

Transformer.set_col(columns, func, dataType)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method can be used to make math operations or string manipulations
in row of dataFrame columns.

The method receives a list of columns (or a single column) of dataFrame
in ``columns`` argument. A ``lambda`` function default called ``func``
and a string which describe the ``dataType`` that ``func`` function
should return.

``columns`` argument is expected to be a string or a list of columns
names and ``dataType`` a string indicating one of the following options:
``'integer', 'string', 'double','float'``.

It is a requirement for this method that the dataType provided must be
the same to dataType of ``columns``. On the other hand, if user writes
``columns == '*'`` the method makes operations in ``func`` if only if
columns have same dataType that ``dataType`` argument.

Here some examples:

**Example: 1**

.. code:: python

    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.show()

    print (' Replacing a number if value in cell is greater than 5:')

    # Replacing a number:   
    func = lambda cell: (cell * 2) if (cell > 14000000 ) else cell
    transformer.set_col(['population'], func, 'integer')

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.show()

Original dataFrame:

+-----------+-------+----------+
|       city|country|population|
+-----------+-------+----------+
|      Tokyo|  Japan|  37800000|
+-----------+-------+----------+
|   New York|    USA|  19795791|
+-----------+-------+----------+
|   Paris   | France|  12341418|
+-----------+-------+----------+
|     Madrid|  Spain|   6489162|
+-----------+-------+----------+

Replacing a number if value in cell is greater than 14000000:

New dataFrame:

+-----------+-------+----------+
|       city|country|population|
+-----------+-------+----------+
|      Tokyo|  Japan|  75600000|
+-----------+-------+----------+
|   New York|    USA|  39591582|
+-----------+-------+----------+
|   Paris   | France|  12341418|
+-----------+-------+----------+
|     Madrid|  Spain|   6489162|
+-----------+-------+----------+

**Example 2:**

.. code:: python

    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.show()

    # Capital letters:
    func = lambda cell: cell.upper()
    transformer.set_col(['city'], func, 'string')

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.show()

Original dataFrame:

+-----------+-------+----------+
|       city|country|population|
+-----------+-------+----------+
|      Tokyo|  Japan|  37800000|
+-----------+-------+----------+
|   New York|    USA|  19795791|
+-----------+-------+----------+
|   Paris   | France|  12341418|
+-----------+-------+----------+
|     Madrid|  Spain|   6489162|
+-----------+-------+----------+

New dataFrame:

+-----------+-------+----------+
|       city|country|population|
+-----------+-------+----------+
|      TOKYO|  Japan|  37800000|
+-----------+-------+----------+
|   NEW YORK|    USA|  19795791|
+-----------+-------+----------+
|   PARIS   | France|  12341418|
+-----------+-------+----------+
|     MADRID|  Spain|   6489162|
+-----------+-------+----------+

Transformer.clear_accents(columns)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This function deletes accents in strings dataFrames, it does not
eliminate main character, but only deletes special tildes.

``clear_accents`` method receives column names (``column``) as argument.
``columns`` must be a string or a list of column names.

E.g:

Building a dummy dataFrame:

.. code:: python

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

New DF:

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

.. code:: python

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

Original dataFrame:

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

New dataFrame:

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

DataFrameTransformer.remove_special_chars(columns)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method remove special characters (i.e. !"#$%&/()=?) in columns of
dataFrames.

``remove_special_chars`` method receives ``columns`` as input. ``columns``
must be a string or a list of strings.

E.g:

.. code:: python


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

Original dataFrame:

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

New dataFrame:

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

DataFrameTransformer.rename_col(columns)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method changes name of column specified by ``columns`` argument.
``columns`` Is a List of tuples. Each tuple has de following form: (oldColumnName, newColumnName).

E.g:

.. code:: python

    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.show()

    names = [('city', 'villes')]
    # Changing name of columns:
    transformer.rename_col(names)

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.show()

Original dataFrame:

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

New dataFrame:

+---------------+--------+----------+
|         villes| country|population|
+---------------+--------+----------+
|         Bogotá|Colombia|  37800000|
+---------------+--------+----------+
|       New York|    US@A|  19795791|
+---------------+--------+----------+
|   São Paulo   |  Brazil|  12341418|
+---------------+--------+----------+
|        ~Madrid|   Spain|   6489162|
+---------------+--------+----------+

DataFrameTransformer.lookup(column, list_str, str_to_replace)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method search a list of strings specified in ``list_str`` argument
among rows in column dataFrame and replace them for ``str_to_replace``.

``lookup`` can only be runned in StringType columns.

E.g:

Building a dummy dataFrame:

.. code:: python


    # Importing sql types
    from pyspark.sql.types import StringType, IntegerType, StructType, StructField
    # Importing optimus
    import optimus as op

    # Building a simple dataframe:
    schema = StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("population", IntegerType(), True)])

    countries = ['Venezuela', 'Venezuela', 'Brazil', 'Spain']
    cities = ['Caracas', 'Ccs', '   São Paulo   ', '~Madrid']
    population = [37800000,19795791,12341418,6489162]

    # Dataframe:
    df = op.spark.createDataFrame(list(zip(cities, countries, population)), schema=schema)

    df.show()

New DF:

+---------------+---------+----------+
|           city|  country|population|
+---------------+---------+----------+
|        Caracas|Venezuela|  37800000|
+---------------+---------+----------+
|            Ccs|Venezuela|  19795791|
+---------------+---------+----------+
|   São Paulo   |   Brazil|  12341418|
+---------------+---------+----------+
|        ~Madrid|    Spain|   6489162|
+---------------+---------+----------+

.. code:: python


    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.show()

    # Capital letters:
    transformer.lookup('city', "Caracas", ['Caracas', 'Ccs'])

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.show()

Original dataFrame:

+---------------+---------+----------+
|           city|  country|population|
+---------------+---------+----------+
|        Caracas|Venezuela|  37800000|
+---------------+---------+----------+
|            Ccs|Venezuela|  19795791|
+---------------+---------+----------+
|   São Paulo   |   Brazil|  12341418|
+---------------+---------+----------+
|        ~Madrid|    Spain|   6489162|
+---------------+---------+----------+

New dataFrame:

+---------------+---------+----------+
|           city|  country|population|
+---------------+---------+----------+
|        Caracas|Venezuela|  37800000|
+---------------+---------+----------+
|        Caracas|Venezuela|  19795791|
+---------------+---------+----------+
|   São Paulo   |   Brazil|  12341418|
+---------------+---------+----------+
|        ~Madrid|    Spain|   6489162|
+---------------+---------+----------+

DataFrameTransformer.move_col(column, ref_col, position)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This function move a column from one position to another according to
the reference column ``ref_col`` and ``position`` argument.

``position`` argument must be the following string: 'after' or 'before'.
If ``position = 'after'`` then, ``column`` is placed just ``after`` the
reference column ``ref_col`` provided by user.

E.g:

.. code:: python


    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.show()

    # Capital letters:
    transformer.move_col('city', 'country', position='after')

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.show()

Original dataFrame:

+---------------+---------+----------+
|           city|  country|population|
+---------------+---------+----------+
|        Caracas|Venezuela|  37800000|
+---------------+---------+----------+
|            Ccs|Venezuela|  19795791|
+---------------+---------+----------+
|   São Paulo   |   Brazil|  12341418|
+---------------+---------+----------+
|        ~Madrid|    Spain|   6489162|
+---------------+---------+----------+

New dataFrame:

+---------+---------------+----------+
|  country|           city|population|
+---------+---------------+----------+
|Venezuela|        Caracas|  37800000|
+---------+---------------+----------+
|Venezuela|            Ccs|  19795791|
+---------+---------------+----------+
|   Brazil|   São Paulo   |  12341418|
+---------+---------------+----------+
|    Spain|        ~Madrid|   6489162|
+---------+---------------+----------+

DataFrameTransformer.count_items(col_id, col_search, new_col_feature, search_string):
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This function can be used to split a feature with some extra information
in order to make a new column feature.

See the example bellow to more explanations:

.. code:: python



    # Importing sql types
    from pyspark.sql.types import StringType, IntegerType, StructType, StructField
    # Importing optimus
    import optimus as op

    # Building a simple dataframe:
    schema = StructType([
            StructField("bill_id", IntegerType(), True),
            StructField("foods", StringType(), True)])

    id_ = [1, 2, 2, 3, 3, 3, 3, 4, 4]
    foods = ['Pizza', 'Pizza', 'Beer', 'Hamburger', 'Beer', 'Beer', 'Beer', 'Pizza', 'Beer']


    # Dataframe:
    df = op.spark.createDataFrame(list(zip(id_, foods)), schema=schema)

    df.show()

New DF:

+-------+---------+
|bill id|    foods|
+-------+---------+
|      1|    Pizza|
+-------+---------+
|      2|    Pizza|
+-------+---------+
|      2|     Beer|
+-------+---------+
|      3|Hamburger|
+-------+---------+
|      3|     Beer|
+-------+---------+
|      3|     Beer|
+-------+---------+
|      3|     Beer|
+-------+---------+
|      4|    Pizza|
+-------+---------+
|      4|     Beer|
+-------+---------+

.. code:: python

    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.show()

    # Transformation:
    transformer.count_items(col_id="bill_id",col_search="foods",new_col_feature="beer_count",search_string="Beer")

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.show()

Original dataFrame:

+-------+---------+
|bill id|    foods|
+-------+---------+
|      1|    Pizza|
+-------+---------+
|      2|    Pizza|
+-------+---------+
|      2|     Beer|
+-------+---------+
|      3|Hamburger|
+-------+---------+
|      3|     Beer|
+-------+---------+
|      3|     Beer|
+-------+---------+
|      3|     Beer|
+-------+---------+
|      4|    Pizza|
+-------+---------+
|      4|     Beer|
+-------+---------+

New dataFrame:

+-------+----------+
|bill_id|beer_count|
+-------+----------+
|      3|         3|
+-------+----------+
|      4|         1|
+-------+----------+
|      2|         1|
+-------+----------+

DataFrameTransformer.date_transform(column, current_format, output_format)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method changes date format in ``column`` from ``current_format`` to
``output_format``.

The column of dataFrame is expected to be StringType or DateType.

``date_transform`` returns column name.

E.g.

date_transform(self, column, current_format, output_format)

.. code:: python


    # Importing sql types
    from pyspark.sql.types import StringType, IntegerType, StructType, StructField
    # Importing optimus
    import optimus as op

    # Building a simple dataframe:
    schema = StructType([
            StructField("city", StringType(), True),
            StructField("dates", StringType(), True),
            StructField("population", IntegerType(), True)])

    dates = ['1991/02/25', '1998/05/10', '1993/03/15', '1992/07/17']
    cities = ['Caracas', 'Ccs', '   São Paulo   ', '~Madrid']
    population = [37800000,19795791,12341418,6489162]

    # Dataframe:
    df = op.spark.createDataFrame(list(zip(cities, dates, population)), schema=schema)

    df.show()

New DF:

+---------------+----------+----------+
|           city|     dates|population|
+---------------+----------+----------+
|        Caracas|1991/02/25|  37800000|
+---------------+----------+----------+
|            Ccs|1998/05/10|  19795791|
+---------------+----------+----------+
|   São Paulo   |1993/03/15|  12341418|
+---------------+----------+----------+
|        ~Madrid|1992/07/17|   6489162|
+---------------+----------+----------+

.. code:: python


    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.show()

    # Tranform string date format:
    transformer.date_transform(columns="dates",
                              current_format="yyyy/mm/dd",
                              output_format="dd-mm-yyyy")

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.show()

Original dataFrame:

+---------------+----------+----------+
|           city|     dates|population|
+---------------+----------+----------+
|        Caracas|1991/02/25|  37800000|
+---------------+----------+----------+
|            Ccs|1998/05/10|  19795791|
+---------------+----------+----------+
|   São Paulo   |1993/03/15|  12341418|
+---------------+----------+----------+
|        ~Madrid|1992/07/17|   6489162|
+---------------+----------+----------+

New dataFrame:

+---------------+----------+----------+
|           city|     dates|population|
+---------------+----------+----------+
|        Caracas|25-02-1991|  37800000|
+---------------+----------+----------+
|            Ccs|10-05-1998|  19795791|
+---------------+----------+----------+
|   São Paulo   |15-03-1993|  12341418|
+---------------+----------+----------+
|        ~Madrid|17-07-1992|   6489162|
+---------------+----------+----------+

DataFrameTransformer.to_csv(path_name, header=True, mode="overwrite", sep=",", *args, **kargs)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This functions writes a Spark dataframe as a CSV in the specified path.

This method require the ``path_name`` to be specified by the user with the name and path of the file to
be saved.

With the ``mode`` Specifies the behavior of the save operation when data already exists:

    - "append": Append contents of this DataFrame to existing data.
    - "overwrite" (default case): Overwrite existing data.
    - "ignore": Silently ignore this operation if data already exists.
    - "error": Throw an exception if data already exists.

And with the ``sep`` argument you can set the single character as a separator for each field and value. If None is set,
it uses the default value (",").

You can also pass all the options that Spark allows as **kargs to the function.

E.g.

.. code:: python

    # Importing sql types
    from pyspark.sql.types import StringType, IntegerType, StructType, StructField
    # Importing optimus
    import optimus as op

    # Building a simple dataframe:
    schema = StructType([
            StructField("bill_id", IntegerType(), True),
            StructField("foods", StringType(), True)])

    id_ = [1, 2, 2, 3, 3, 3, 3, 4, 4]
    foods = ['Pizza', 'Pizza', 'Beer', 'Hamburger', 'Beer', 'Beer', 'Beer', 'Pizza', 'Beer']


    # Dataframe:
    df = op.spark.createDataFrame(list(zip(id_, foods)), schema=schema)

    df.show()

DF:

+-------+---------+
|bill id|    foods|
+-------+---------+
|      1|    Pizza|
+-------+---------+
|      2|    Pizza|
+-------+---------+
|      2|     Beer|
+-------+---------+
|      3|Hamburger|
+-------+---------+
|      3|     Beer|
+-------+---------+
|      3|     Beer|
+-------+---------+
|      3|     Beer|
+-------+---------+
|      4|    Pizza|
+-------+---------+
|      4|     Beer|
+-------+---------+

Now lets write this DF as a CSV

.. code:: python

    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)

    # Write DF as CSV
    transformer.to_csv("test.csv")

This will create a folder with the name "test.csv" in the current path, and inside it will be te CSV with the
concept. But with the ``read_csv`` function you can just pass the name "test.csv" and Optimus will understand. 
    
Library mantained by `Favio Vazquez`_
-------
.. _Favio Vazquez: https://github.com/faviovazquez
