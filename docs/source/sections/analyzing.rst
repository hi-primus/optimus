Analyzing your Data Frame
===========================

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
~~~~~~~~~

Analyzer.column_analyze(column_list, plots=True, values_bar=True, print_type=False, num_bars=10)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This function counts the number of registers in a column that are numbers (integers, floats) and the number of string registers.

Input:

``column_list``: A list or a string column name.

``plots``: Can be True or False. If true it will output the predefined plots.

``values_bar (optional)``: Can be True or False. If it is True, frequency values are placed over each bar.

``print_type (optional)``: Can be one of the following strings: 'integer', 'string', 'float'. Depending of what string
is provided, a list of distinct values of that type is printed.

``num_bars``: number of bars printed in histogram

The method outputs a list containing the number of the different datatypes [nulls, strings, integers, floats].