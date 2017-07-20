Optimus (By Iron_)
=======


.. _Iron: https://github.com/ironmussa

Description
------------

Optimus is the missing library for cleaning and pre-processing data in a distributed fashion. It uses all the power of `Apache Spark`_ (optimized via Catalyst_) to do it. It implements several handy tools for data wrangling and munging that will make your life much easier. The first obvious advantage over any other public data cleaning library is that it will work on your laptop or your big cluster, and second, it is amazingly easy to install, use and understand.

.. _Apache Spark: https://spark.apache.

.. _Catalyst: https://static.javadoc.io/org.apache.spark/spark-catalyst_2.10/1.0.1/index.html#org.apache.spark.sql.catalyst.package

The following schema shows the structure class organization of the whole
library:

* Optimus

  - DataFrameTransformer
  - DataFrameAnalyzer
  
* Utilities

Requirements
------------

-  Apache Spark 1.6
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

Optimus
-------

Optimus is a class that wraps DataFrameAnalyzer and DataFrameTransformer
classes.

DataFrameAnalyzer
-----------------

DataFrameAnalyzer class analyze dataType of rows in each columns of
dataFrames.

-  columnAnalize
-  plotHist
-  getCategoricalHist
-  getNumericalHist
-  uniqueValuesCol
-  writeJson

DataFrameTransformer class
--------------------------

-  DataFrameTransformer(df)

**DataFrameTransformer methods**

* **Column operations**:

  - [DataFrameTransformer.dropCol(columns)](#dataframetransformer-class)
  - DataFrameTransformer.replaceCol(search, changeTo, columns)]
  - DataFrameTransformer.keepCol(columns)
  - DataFrameTransformer.renameCol(column, newName)</li>
  - DataFrameTransformer.moveCol(column, refCol, position)

* **Row operations** :

  - DataFrameTransformer.dropRow(columns)
  - DataFrameTransformer.deleteRow(func)

* **String operations**:

  - DataFrameTransformer.trimCol(columns)
  - DataFrameTransformer.clearAccents(columns)
  - DataFrameTransformer.lookup(column, listStr, StrToReplace)
  - DataFrameTransformer.removeSpecialChars(columns)
  - DataFrameTransformer.dateTransform(column, dateFormat)

* **General operation function**: 

  - DataFrameTransformer.setCol(columns, func, dataType)

* **Others**:
  - DataFrameTransformer.explodeTable(coldId, col, newColFeature)
  - DataFrameTransformer.ageCalculate(column)

DataFrameTransformer class receives a dataFrame as an argument. This
class has all methods listed aboved.

Note: Every possible transformation make changes over this dataFrame and
overwrites it.

The following code shows how to instanciate the class to transform a
dataFrame:

.. code:: python

    # Importing sql types
    from pyspark.sql.types import StringType, IntegerType, StructType, StructField
    # Importing DataFrameTransformer library
    from optimus.DfTransf import DataFrameTransformer

    # Building a simple dataframe:
    schema = StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("population", IntegerType(), True)])

    countries = ['Japan', 'USA', 'France', 'Spain']
    cities = ['Tokyo', 'New York', '   Paris   ', 'Madrid']
    population = [37800000,19795791,12341418,6489162]

    # Dataframe:
    df = sqlContext.createDataFrame(list(zip(cities, countries, population)), schema=schema)

    # DataFrameTransformer Instanciation:
    transformer = DataFrameTransformer(df)

    transformer.getDataframe().show()

.. code:: python

    #+-----------+-------+----------+
    #|       city|country|population|
    #+-----------+-------+----------+
    #|      Tokyo|  Japan|  37800000|
    #|   New York|    USA|  19795791|
    #|   Paris   | France|  12341418|
    #|     Madrid|  Spain|   6489162|
    #+-----------+-------+----------+

Methods
-------

Transformer.trimCol(columns)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This methods cut left and right extra spaces in column strings provided
by user.

``columns`` argument is expected to be a string o a list of column names
.

If a string ``"*"`` is provided, the method will do the trimming
operation in whole dataframe.

**Example:**

.. code:: python

    # Instantiation of DataTransformer class:
    transformer = DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.getDataframe().show()

    # Triming string blank spaces:
    transformer.trimCol("*")

    # Printing trimmed dataFrame:
    print('Trimmed dataFrame:')
    transformer.getDataframe().show()

.. code:: python

    Original dataFrame:
    #+-----------+-------+----------+
    #|       city|country|population|
    #+-----------+-------+----------+
    #|      Tokyo|  Japan|  37800000|
    #|   New York|    USA|  19795791|
    #|   Paris   | France|  12341418|
    #|     Madrid|  Spain|   6489162|
    #+-----------+-------+----------+

    Trimmed dataFrame:
    #+--------+-------+----------+
    #|    city|country|population|
    #+--------+-------+----------+
    #|   Tokyo|  Japan|  37800000|
    #|New York|    USA|  19795791|
    #|   Paris| France|  12341418|
    #|  Madrid|  Spain|   6489162|
    #+--------+-------+----------+

Transformer.dropCol(columns)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method eliminate the list of columns provided by user.

``columns`` argument is expected to be a string or a list of columns
names.

**Example:**

.. code:: python

    # Instantiation of DataTransformer class:
    transformer = DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.getDataframe().show()

    # drop column specified:
    transformer.dropCol("country")

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.getDataframe().show()

.. code:: python

    Original dataFrame:
    #+-----------+-------+----------+
    #|       city|country|population|
    #+-----------+-------+----------+
    #|      Tokyo|  Japan|  37800000|
    #|   New York|    USA|  19795791|
    #|   Paris   | France|  12341418|
    #|     Madrid|  Spain|   6489162|
    #+-----------+-------+----------+

    New dataFrame:
    #+-----------+----------+
    #|       city|population|
    #+-----------+----------+
    #|      Tokyo|  37800000|
    #|   New York|  19795791|
    #|   Paris   |  12341418|
    #|     Madrid|   6489162|
    #+-----------+----------+

Transformer.keepCol(columns)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method keep only columns specified by user with ``columns``
argument in DataFrame.

``columns`` argument is expected to be a string or a list of columns
names.

**Example:**

.. code:: python

    # Instanciation of DataTransformer class:
    transformer = DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.getDataframe().show()

    # Keep columns specified by user:
    transformer.keepCol(['city', 'population'])

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.getDataframe().show()

.. code:: python

    Original dataFrame:
    #+-----------+-------+----------+
    #|       city|country|population|
    #+-----------+-------+----------+
    #|      Tokyo|  Japan|  37800000|
    #|   New York|    USA|  19795791|
    #|   Paris   | France|  12341418|
    #|     Madrid|  Spain|   6489162|
    #+-----------+-------+----------+

    New dataFrame:
    #+-----------+----------+
    #|       city|population|
    #+-----------+----------+
    #|      Tokyo|  37800000|
    #|   New York|  19795791|
    #|   Paris   |  12341418|
    #|     Madrid|   6489162|
    #+-----------+----------+

Transformer.replaceCol(search, changeTo, columns)
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

    # Instanciation of DataTransformer class:
    transformer = DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.getDataframe().show()

    # Replace values in columns specified by user:
    transformer.replaceCol(search='Tokyo', changeTo='Maracaibo', columns='city')

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.getDataframe().show()

.. code:: python

    Original dataFrame:
    #+-----------+-------+----------+
    #|       city|country|population|
    #+-----------+-------+----------+
    #|      Tokyo|  Japan|  37800000|
    #|   New York|    USA|  19795791|
    #|   Paris   | France|  12341418|
    #|     Madrid|  Spain|   6489162|
    #+-----------+-------+----------+

    New dataFrame:
    #+-----------+-------+----------+
    #|       city|country|population|
    #+-----------+-------+----------+
    #|  Maracaibo|  Japan|  37800000|
    #|   New York|    USA|  19795791|
    #|   Paris   | France|  12341418|
    #|     Madrid|  Spain|   6489162|
    #+-----------+-------+----------+

Transformer.deleteRow(func)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method deletes rows in columns according to condition provided by
user.

``deleteRow`` method receives a function ``func`` as an input parameter.

``func`` is required to be a ``lambda`` function, which is a native
python feature.

**Example 1:**

.. code:: python


    # Importing sql functions
    from pyspark.sql.functions import col

    # Instanciation of DataTransformer class:
    transformer = DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.getDataframe().show()

    # Replace values in columns specified by user:
    func = lambda pop: (pop > 6500000) & (pop <= 30000000)
    transformer.deleteRow(func(col('population')))

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.getDataframe().show()

.. code:: python


    Original dataFrame:
    #+-----------+-------+----------+
    #|       city|country|population|
    #+-----------+-------+----------+
    #|      Tokyo|  Japan|  37800000|
    #|   New York|    USA|  19795791|
    #|   Paris   | France|  12341418|
    #|     Madrid|  Spain|   6489162|
    #+-----------+-------+----------+

    New dataFrame:
    #+-----------+-------+----------+
    #|       city|country|population|
    #+-----------+-------+----------+
    #|   New York|    USA|  19795791|
    #|   Paris   | France|  12341418|
    #+-----------+-------+----------+

**Example 2:**

.. code:: python


    # Importing sql functions
    from pyspark.sql.functions import col

    # Instanciation of DataTransformer class:
    transformer = DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.getDataframe().show()

    # Delect rows where Tokyo isn't found in city
    # column or France isn't found in country column:
    func = lambda city, country: (city == 'Tokyo')  | (country == 'France')
    transformer.deleteRow(func(col('city'), col('country')))

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.getDataframe().show()

.. code:: python


    Original dataFrame:
    #+-----------+-------+----------+
    #|       city|country|population|
    #+-----------+-------+----------+
    #|      Tokyo|  Japan|  37800000|
    #|   New York|    USA|  19795791|
    #|   Paris   | France|  12341418|
    #|     Madrid|  Spain|   6489162|
    #+-----------+-------+----------+

    New dataFrame:
    #+-----------+-------+----------+
    #|       city|country|population|
    #+-----------+-------+----------+
    #|      Tokyo|  Japan|  37800000|
    #|   Paris   | France|  12341418|
    #+-----------+-------+----------+

Transformer.setCol(columns, func, dataType)
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

    # Instanciation of DataTransformer class:
    transformer = DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.getDataframe().show()

    print (' Replacing a number if value in cell is greater than 5:')

    # Replacing a number:   
    func = lambda cell: (cell * 2) if (cell > 14000000 ) else cell
    transformer.setCol(['population'], func, 'integer')

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.getDataframe().show()

.. code:: python

    Original dataFrame:
    #+-----------+-------+----------+
    #|       city|country|population|
    #+-----------+-------+----------+
    #|      Tokyo|  Japan|  37800000|
    #|   New York|    USA|  19795791|
    #|   Paris   | France|  12341418|
    #|     Madrid|  Spain|   6489162|
    #+-----------+-------+----------+

    Replacing a number if value in cell is greater than 14000000:
    New dataFrame:
    #+-----------+-------+----------+
    #|       city|country|population|
    #+-----------+-------+----------+
    #|      Tokyo|  Japan|  75600000|
    #|   New York|    USA|  39591582|
    #|   Paris   | France|  12341418|
    #|     Madrid|  Spain|   6489162|
    #+-----------+-------+----------+

**Example 2:**

.. code:: python

    # Instanciation of DataTransformer class:
    transformer = DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.getDataframe().show()

    # Capital letters:
    func = lambda cell: cell.upper()
    transformer.setCol(['city'], func, 'string')

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.getDataframe().show()

.. code:: python

    Original dataFrame:
    #+-----------+-------+----------+
    #|       city|country|population|
    #+-----------+-------+----------+
    #|      Tokyo|  Japan|  37800000|
    #|   New York|    USA|  19795791|
    #|   Paris   | France|  12341418|
    #|     Madrid|  Spain|   6489162|
    #+-----------+-------+----------+

    New dataFrame:
    #+-----------+-------+----------+
    #|       city|country|population|
    #+-----------+-------+----------+
    #|      TOKYO|  Japan|  37800000|
    #|   NEW YORK|    USA|  19795791|
    #|   PARIS   | France|  12341418|
    #|     MADRID|  Spain|   6489162|
    #+-----------+-------+----------+

Transformer.clearAccents(columns)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This function deletes accents in strings dataFrames, it does not
eliminate main character, but only deletes special tildes.

``clearAccents`` method receives column names (``column``) as argument.
``columns`` must be a string or a list of column names.

E.g:

Building a dummy dataFrame:

.. code:: python

    # Importing sql types
    from pyspark.sql.types import StringType, IntegerType, StructType, StructField
    # Importing DataFrameTransformer library
    from optimus.DfTransf import DataFrameTransformer

    # Building a simple dataframe:
    schema = StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("population", IntegerType(), True)])

    countries = ['Colombia', 'US@A', 'Brazil', 'Spain']
    cities = ['Bogotá', 'New York', '   São Paulo   ', '~Madrid']
    population = [37800000,19795791,12341418,6489162]

    # Dataframe:
    df = sqlContext.createDataFrame(list(zip(cities, countries, population)), schema=schema)

    df.show()

.. code:: python

    #+---------------+--------+----------+
    #|           city| country|population|
    #+---------------+--------+----------+
    #|         Bogotá|Colombia|  37800000|
    #|       New York|    US@A|  19795791|
    #|   São Paulo   |  Brazil|  12341418|
    #|        ~Madrid|   Spain|   6489162|
    #+---------------+--------+----------+

.. code:: python

    # Instanciation of DataTransformer class:
    transformer = DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.getDataframe().show()

    # Clear accents:
    transformer.clearAccents(columns='*')

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.getDataframe().show()

.. code:: python


    Original dataFrame:
    #+---------------+--------+----------+
    #|           city| country|population|
    #+---------------+--------+----------+
    #|         Bogotá|Colombia|  37800000|
    #|       New York|    US@A|  19795791|
    #|   São Paulo   |  Brazil|  12341418|
    #|        ~Madrid|   Spain|   6489162|
    #+---------------+--------+----------+

    New dataFrame:
    #+---------------+--------+----------+
    #|           city| country|population|
    #+---------------+--------+----------+
    #|         Bogota|Colombia|  37800000|
    #|       New York|    US@A|  19795791|
    #|   Sao Paulo   |  Brazil|  12341418|
    #|        ~Madrid|   Spain|   6489162|
    #+---------------+--------+----------+

DataFrameTransformer.removeSpecialChars(columns)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method remove special characters (i.e. !"#$%&/()=?) in columns of
dataFrames.

``removeSpecialChars`` method receives ``columns`` as input. ``columns``
must be a string or a list of strings.

E.g:

.. code:: python


    # Instanciation of DataTransformer class:
    transformer = DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.getDataframe().show()

    # Remove special characters:
    transformer.removeSpecialChars(columns=['city', 'country'])

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.getDataframe().show()

.. code:: python


    Original dataFrame:
    #+---------------+--------+----------+
    #|           city| country|population|
    #+---------------+--------+----------+
    #|         Bogotá|Colombia|  37800000|
    #|       New York|    US@A|  19795791|
    #|   São Paulo   |  Brazil|  12341418|
    #|        ~Madrid|   Spain|   6489162|
    #+---------------+--------+----------+

    New dataFrame:
    #+---------------+--------+----------+
    #|           city| country|population|
    #+---------------+--------+----------+
    #|         Bogotá|Colombia|  37800000|
    #|       New York|     USA|  19795791|
    #|   São Paulo   |  Brazil|  12341418|
    #|         Madrid|   Spain|   6489162|
    #+---------------+--------+----------+

DataFrameTransformer.renameCol(column, newName)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method changes name of column specified by ``column`` argument.
``newName`` is the name to be set in column dataFrame.

E.g:

.. code:: python

    # Instanciation of DataTransformer class:
    transformer = DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.getDataframe().show()

    names = [('city', 'villes')]
    # Changing name of columns:
    transformer.renameCol(names)

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.getDataframe().show()

.. code:: python


    Original dataFrame:
    #+---------------+--------+----------+
    #|           city| country|population|
    #+---------------+--------+----------+
    #|         Bogotá|Colombia|  37800000|
    #|       New York|    US@A|  19795791|
    #|   São Paulo   |  Brazil|  12341418|
    #|        ~Madrid|   Spain|   6489162|
    #+---------------+--------+----------+

    New dataFrame:
    #+---------------+--------+----------+
    #|         villes| country|population|
    #+---------------+--------+----------+
    #|         Bogotá|Colombia|  37800000|
    #|       New York|    US@A|  19795791|
    #|   São Paulo   |  Brazil|  12341418|
    #|        ~Madrid|   Spain|   6489162|
    #+---------------+--------+----------+

DataFrameTransformer.lookup(column, listStr, StrToReplace)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method search a list of strings specified in ``listStr`` argument
among rows in column dataFrame and replace them for ``StrToReplace``.

``lookup`` can only be runned in StringType columns.

E.g:

Building a dummy dataFrame:

.. code:: python


    # Importing sql types
    from pyspark.sql.types import StringType, IntegerType, StructType, StructField
    # Importing DataFrameTransformer library
    from optimus.DfTransf import DataFrameTransformer

    # Building a simple dataframe:
    schema = StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("population", IntegerType(), True)])

    countries = ['Venezuela', 'Venezuela', 'Brazil', 'Spain']
    cities = ['Caracas', 'Ccs', '   São Paulo   ', '~Madrid']
    population = [37800000,19795791,12341418,6489162]

    # Dataframe:
    df = sqlContext.createDataFrame(list(zip(cities, countries, population)), schema=schema)

    df.show()

.. code:: python


    #+---------------+---------+----------+
    #|           city|  country|population|
    #+---------------+---------+----------+
    #|        Caracas|Venezuela|  37800000|
    #|            Ccs|Venezuela|  19795791|
    #|   São Paulo   |   Brazil|  12341418|
    #|        ~Madrid|    Spain|   6489162|
    #+---------------+---------+----------+

.. code:: python


    # Instanciation of DataTransformer class:
    transformer = DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.getDataframe().show()

    # Capital letters:
    transformer.lookup('city', ['Caracas', 'Ccs'], 'Caracas')

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.getDataframe().show()

.. code:: python


    Original dataFrame:
    #+---------------+---------+----------+
    #|           city|  country|population|
    #+---------------+---------+----------+
    #|        Caracas|Venezuela|  37800000|
    #|            Ccs|Venezuela|  19795791|
    #|   São Paulo   |   Brazil|  12341418|
    #|        ~Madrid|    Spain|   6489162|
    #+---------------+---------+----------+

    New dataFrame:
    #+---------------+---------+----------+
    #|           city|  country|population|
    #+---------------+---------+----------+
    #|        Caracas|Venezuela|  37800000|
    #|        Caracas|Venezuela|  19795791|
    #|   São Paulo   |   Brazil|  12341418|
    #|        ~Madrid|    Spain|   6489162|
    #+---------------+---------+----------+

DataFrameTransformer.moveCol(column, refCol, position)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This function move a column from one position to another according to
the reference column ``refCol`` and ``position`` argument.

``position`` argument must be the following string: 'after' or 'before'.
If ``position = 'after'`` then, ``column`` is placed just ``after`` the
reference column ``refCol`` provided by user.

E.g:

.. code:: python


    # Instanciation of DataTransformer class:
    transformer = DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.getDataframe().show()

    # Capital letters:
    transformer.moveCol('city', 'country', position='after')

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.getDataframe().show()

.. code:: python

    Original dataFrame:
    #+---------------+---------+----------+
    #|           city|  country|population|
    #+---------------+---------+----------+
    #|        Caracas|Venezuela|  37800000|
    #|            Ccs|Venezuela|  19795791|
    #|   São Paulo   |   Brazil|  12341418|
    #|        ~Madrid|    Spain|   6489162|
    #+---------------+---------+----------+

    New dataFrame:
    #+---------+---------------+----------+
    #|  country|           city|population|
    #+---------+---------------+----------+
    #|Venezuela|        Caracas|  37800000|
    #|Venezuela|            Ccs|  19795791|
    #|   Brazil|   São Paulo   |  12341418|
    #|    Spain|        ~Madrid|   6489162|
    #+---------+---------------+----------+

DataFrameTransformer.explodeTable(coldId, col, newColFeature)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This function can be used to split a feature with some extra information
in order to make a new column feature.

See the example bellow to more explanations:

.. code:: python



    # Importing sql types
    from pyspark.sql.types import StringType, IntegerType, StructType, StructField
    # Importing DataFrameTransformer library
    from optimus.DfTransf import DataFrameTransformer

    # Building a simple dataframe:
    schema = StructType([
            StructField("bill id", IntegerType(), True),
            StructField("foods", StringType(), True)])

    id_ = [1, 2, 2, 3, 3, 3, 3, 4, 4]
    foods = ['Pizza', 'Pizza', 'Beer', 'Hamburger', 'Beer', 'Beer', 'Beer', 'Pizza', 'Beer']


    # Dataframe:
    df = sqlContext.createDataFrame(list(zip(id_, foods)), schema=schema)

    df.show()

.. code:: python


    #+-------+---------+
    #|bill id|    foods|
    #+-------+---------+
    #|      1|    Pizza|
    #|      2|    Pizza|
    #|      2|     Beer|
    #|      3|Hamburger|
    #|      3|     Beer|
    #|      3|     Beer|
    #|      3|     Beer|
    #|      4|    Pizza|
    #|      4|     Beer|
    #+-------+---------+

.. code:: python

    # Instanciation of DataTransformer class:
    transformer = DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.getDataframe().show()

    # Transformation:
    transformer.explodeTable('bill id', 'foods', 'Beer')

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.getDataframe().show()

.. code:: python

    Original dataFrame:
    #+-------+---------+
    #|bill id|    foods|
    #+-------+---------+
    #|      1|    Pizza|
    #|      2|    Pizza|
    #|      2|     Beer|
    #|      3|Hamburger|
    #|      3|     Beer|
    #|      3|     Beer|
    #|      3|     Beer|
    #|      4|    Pizza|
    #|      4|     Beer|
    #+-------+---------+

    New dataFrame:
    #+-------+---------+----+
    #|bill id|    foods|Beer|
    #+-------+---------+----+
    #|      1|    Pizza|   0|
    #|      2|    Pizza|   1|
    #|      3|Hamburger|   3|
    #|      4|    Pizza|   1|
    #+-------+---------+----+

DataFrameTransformer.dateTransform(column, currentFormat, outputFormat)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method changes date format in ``column`` from ``currentFormat`` to
``outputFormat``.

The column of dataFrame is expected to be StringType or DateType.

``dateTransform`` returns column name.

E.g.

dateTransform(self, column, currentFormat, outputFormat)

.. code:: python


    # Importing sql types
    from pyspark.sql.types import StringType, IntegerType, StructType, StructField
    # Importing DataFrameTransformer library
    from optimus.DfTransf import DataFrameTransformer

    # Building a simple dataframe:
    schema = StructType([
            StructField("city", StringType(), True),
            StructField("dates", StringType(), True),
            StructField("population", IntegerType(), True)])

    countries = ['1991/02/25', '1998/05/10', '1993/03/15', '1992/07/17']
    cities = ['Caracas', 'Ccs', '   São Paulo   ', '~Madrid']
    population = [37800000,19795791,12341418,6489162]

    # Dataframe:
    df = sqlContext.createDataFrame(list(zip(cities, countries, population)), schema=schema)

    df.show()

.. code:: python


    #+---------------+----------+----------+
    #|           city|     dates|population|
    #+---------------+----------+----------+
    #|        Caracas|1991/02/25|  37800000|
    #|            Ccs|1998/05/10|  19795791|
    #|   São Paulo   |1993/03/15|  12341418|
    #|        ~Madrid|1992/07/17|   6489162|
    #+---------------+----------+----------+

.. code:: python


    # Instanciation of DataTransformer class:
    transformer = DataFrameTransformer(df)

    # Printing of original dataFrame:
    print('Original dataFrame:')
    transformer.getDataframe().show()

    # Tranform string date format:
    transformer.dateTransform(columns="dates",
                              currentFormat="yyyy/mm/dd",
                              outputFormat="dd-mm-yyyy")

    # Printing new dataFrame:
    print('New dataFrame:')
    transformer.getDataframe().show()

.. code:: python


    Original dataFrame:
    #+---------------+----------+----------+
    #|           city|     dates|population|
    #+---------------+----------+----------+
    #|        Caracas|1991/02/25|  37800000|
    #|            Ccs|1998/05/10|  19795791|
    #|   São Paulo   |1993/03/15|  12341418|
    #|        ~Madrid|1992/07/17|   6489162|
    #+---------------+----------+----------+

    New dataFrame:
    #+---------------+----------+----------+
    #|           city|     dates|population|
    #+---------------+----------+----------+
    #|        Caracas|25-02-1991|  37800000|
    #|            Ccs|10-05-1998|  19795791|
    #|   São Paulo   |15-03-1993|  12341418|
    #|        ~Madrid|17-07-1992|   6489162|
    #+---------------+----------+----------+
    
Library mantained by `Favio Vazquez`_
-------
.. _Favio Vazquez: https://github.com/faviovazquez
