Transforming your Data
=======================

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

Transformer.trim_col(columns)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

Transformer.set_col(columns, func, data_type)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method can be used to make math operations or string manipulations
in row of dataFrame columns.

The method receives a list of columns (or a single column) of dataFrame
in ``columns`` argument. A ``lambda`` function default called ``func``
and a string which describe the ``data_type`` that ``func`` function
should return.

``columns`` argument is expected to be a string or a list of columns
names and ``dataType`` a string indicating one of the following options:
``'integer', 'string', 'double','float'``.

It is a requirement for this method that the dataType provided must be
the same to dataType of ``columns``. On the other hand, if user writes
``columns == '*'`` the method makes operations in ``func`` if only if
columns have same dataType that ``data_type`` argument.

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

DataFrameTransformer.replace_na(value, columns=None)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method replace nulls with specified value.

``columns`` argument is an optional list of column names to consider. Columns specified in subset that do not have
matching data type are ignored. For example, if value is a string, and subset contains a non-string column,
then the non-string column is simply ignored. If `columns == "*"` then it will choose all columns.

``value`` argument is the value to replace nulls with. If the value is a dict, then subset is ignored and value
must be a mapping from column name (string) to replacement value. The replacement value must be an int, long,
float, or string.

Let's download a sample data using our amazing `read_url` function.


.. code:: python
    # Import optimus
    import optimus as op
    # Instance of Utilities class
    tools = op.Utilities()
    # Reading df from web
    url = "https://raw.githubusercontent.com/ironmussa/Optimus/master/examples/impute_data.csv"
    df = tools.read_dataset_url(path=url)

If we examine this DF we see that there are some missing values.

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

Remember that we have the `impute_missing` function that lets you choose to use the mean or the median of the columns in
which the missing values are located for your imputation. But with `replace_na` you can say replace the nulls in one,
or all columns in the dataframe with a specific value. For this example we will replace NA with 0's.

.. code:: python

    # Instantiation of DataTransformer class:
    transformer = op.DataFrameTransformer(df)
    # Replace NA with 0's
    transformer.replace_na(0.0, columns="*")
    # Show DF
    transformer.show()

+---+---+
|  a|  b|
+---+---+
|0.0|0.0|
+---+---+
|0.0|0.0|
+---+---+
|0.0|3.0|
+---+---+
|4.0|4.0|
+---+---+
|5.0|5.0|
+---+---+

And that's it!